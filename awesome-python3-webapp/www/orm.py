import asyncio, logging
import aiomysql


def log(sql, args=()):
    logging.info('SQL: %s' % sql)


# 创建一个全局的连接池，每个HTTP请求都从池子中获得数据库连接
# 连接池由全局变量__pool存储，缺省状况下将编码设置为utf-8，自动提交事务
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf-8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )


# 单独封装select，其他insert，update，delete一并封装，理由如下：
# 使用Cursor对象执行insert、update、delete语句时候，执行结果由rowcount返回影响的行数，就可以拿到执行结果
# 使用Cursor对象执行select语句时，通过fetchall()可以拿到结果集。结果集是一个list，每个元素都是一个tuple，对应一行记录
async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    async with __pool.get() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
        logging.info('row returned:%s' % len(rs))
        return rs


async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affected


# 用于输出元类中创建sql_insert语句中的占位符
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)


# 定义Field类，负责保存(数据库)表的字段名和字段类型
class Field(object):

    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        # 返回表名字 字段名 和字段类型
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


# 定义数据库中五个存储类型
class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)


# 布尔类型不可以作为主键
class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)


class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):

    def __int__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


# 定义model的元类
# 所有的元类都继承自type
# ModelMetaclass元类定义了所有Model基类（继承ModelMetaclass）的子类实现操作

# -*-ModelMetaclass的工作主要是为一个数据库表映射成一个封装的类作准备：
# 读取具体子类(user)的映射信息
# 创造类的时候，排除对model类的修改
# 在当前类中查找所有的类属行(attrs)，如果找到Field属性，就将其保存到__mappings__的dict中，同时从类属性中删除Field(放置实例属性遮住类的同名属性)
# 将数据库表明保存到__table__中。

# 完成上述工作就可以在model中定义各种数据库的操作方法
# metaclass是类的模板，必须从’type‘类型派生
class ModelMetaclass(type):

    # __new__控制__init__的执行，所以在其执行之前，
    # cls代表__init__的类，此参数在实例化时候有python解释器自动提供，例如下文的User和model
    # bases: 代表继承父类的集合
    # attrs: 类的方法集合
    def __new__(cls, name, bases, attrs):
        # 排除model，是因为要排除对model类的修改
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名称，如果存在表名，则返回表名，否则返回name
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table:%s)' % (name, tableName))
        # 获取Field所有主键名和Field
        mappings = dict()  # 保存映射关系
        fields = []  # 保存除主键外的属性名字
        primaryKey = None
        # k表示字段名
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键，当第一次主键存在primarykey被赋值，如果后来再出现主键的话就会发生错误
                    if primaryKey:
                        raise Exception('Duplicate primary key for field: %s' % k)  # 一个表只能有一个主键，当再出现一个主键的时候就报错
                    primaryKey = k  # 该列设为列表的主键，主键仅能被设置一次
                else:
                    fields.append(k)  # 保存除主键外的属性
        if not primaryKey:  # 如果主键不存在也将会报错，在这个表中没有找到主键，一个表有且仅有一个主键
            raise Exception('Primary key not found.')
        # w下面位字段从属性中删除Field属性
        for k in mappings.keys():
            attrs.pop(k)  # 从类属性中删除Field属性否则，容易造成运行是发生错误（实例的属性会覆盖类的同名属性）
        # 保存除主键外的属性为''列表的形式
        # 除主键外的其他属性变成'id'，'name'这种形式
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))  # 转换为sql语法
        # 创建共Model类使用的属性
        attrs['__mappings__'] = mappings  # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey
        attrs['__fields__'] = fields  # 除主键外的属性名
        attrs['__select__'] = 'select `%s`, `%s` from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)  # 查询列的名字，也看一下在Field定义上有没有定义名字，默认None
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


# 定义ORM所有映射的基类：Model
# Model类的任意子类可以映射一个数据库表
# Model类可以看做是对所有数据库表操作的基本定义的映射

# 基于字典查询形式
# Model从dict继承，拥有字典的所有功能，同时实现特殊方法__getattr__和__setattr__,能够实现属性操作
# 实现数据库操作的所有方法，定义为class方法，所有继承自model都具有数据库操作方法
class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        # 默认内置函数实现，注意这里None的用处,是为了当user没有赋值数据时，返回None，调用于update
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        # 第三个参数None，可以在没有返回数值时，返回None，调用于save
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s:%s' % (key, str(value)))
                setattr(self, key, value)
        return value

    @classmethod
    # 当前类方法有cls传入，从而可以用cls做一些相关处理，并且有子类继承时候，调用该类方法
    async def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invaild limit value:%s' % str(limit))
        # 返回的rs是一个元素是tuple的list
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]  # 每条记录对应的类实例，**r是关键字参数，构成了一个cls类的列表

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    @classmethod
    async def find(cls, pk):
        ' find object by primary key '
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        # 返回一条记录，以dict的形式返回，因为cls的夫类继承了dict类
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging._warn('failed to insert record: affected rows:%s' % rows)

    async def update(self):
        args = list(map(self.getValue, self.__field__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging._warn('failed to update by primary key: affected rows:%s' % rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging._warn('failed to remove by primary key: affected rows:%s' % rows)

















































# import asyncio, logging
#
# import aiomysql
#
#
# def log(sql, args=()):
#     logging.info('SQL: %s' % sql)
#
# # 创建一个全局的连接池，每个HTTP请求都从池子中获得数据库连接
# # 连接池由全局变量__pool存储，缺省状况下将编码设置为utf-8，自动提交事务
# async def create_pool(loop, **kw):
#     logging.info('create database connection pool...')
#     global __pool
#     __pool = await aiomysql.create_pool(
#         host=kw.get('host', 'localhost'),
#         port=kw.get('port', 3306),
#         user=kw['user'],
#         password=kw['password'],
#         db=kw['db'],
#         charset=kw.get('charset', 'utf8'),
#         autocommit=kw.get('autocommit', True),
#         maxsize=kw.get('maxsize', 10),
#         minsize=kw.get('minsize', 1),
#         loop=loop
#     )
#
#
# # 单独封装select，其他insert，update，delete一并封装，理由如下：
# # 使用Cursor对象执行insert、update、delete语句时候，执行结果由rowcount返回影响的行数，就可以拿到执行结果
# # 使用Cursor对象执行select语句时，通过fetchall()可以拿到结果集。结果集是一个list，每个元素都是一个tuple，对应一行记录
# async def select(sql, args, size=None):
#     log(sql, args)
#     global __pool
#     async with __pool.get() as conn:
#         async with conn.cursor(aiomysql.DictCursor) as cur:
#             await cur.execute(sql.replace('?', '%s'), args or ())
#             if size:
#                 rs = await cur.fetchmany(size)
#             else:
#                 rs = await cur.fetchall()
#         logging.info('rows returned: %s' % len(rs))
#         return rs
#
# async def execute(sql, args, autocommit=True):
#     log(sql)
#     async with __pool.get() as conn:
#         if not autocommit:
#             await conn.begin()
#         try:
#             async with conn.cursor(aiomysql.DictCursor) as cur:
#                 await cur.execute(sql.replace('?', '%s'), args)
#                 affected = cur.rowcount
#             if not autocommit:
#                 await conn.commit()
#         except BaseException as e:
#             if not autocommit:
#                 await conn.rollback()
#             raise
#         return affected
#
#
# # 用于输出元类中创建sql_insert语句中的占位符
# def create_args_string(num):
#     L = []
#     for n in range(num):
#         L.append('?')
#     return ', '.join(L)
#
# class Field(object):
#
#     def __init__(self, name, column_type, primary_key, default):
#         self.name = name
#         self.column_type = column_type
#         self.primary_key = primary_key
#         self.default = default
#
#     def __str__(self):
#         return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)
#
# class StringField(Field):
#
#     def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
#         super().__init__(name, ddl, primary_key, default)
#
# class BooleanField(Field):
#
#     def __init__(self, name=None, default=False):
#         super().__init__(name, 'boolean', False, default)
#
# class IntegerField(Field):
#
#     def __init__(self, name=None, primary_key=False, default=0):
#         super().__init__(name, 'bigint', primary_key, default)
#
# class FloatField(Field):
#
#     def __init__(self, name=None, primary_key=False, default=0.0):
#         super().__init__(name, 'real', primary_key, default)
#
# class TextField(Field):
#
#     def __init__(self, name=None, default=None):
#         super().__init__(name, 'text', False, default)
#
# class ModelMetaclass(type):
#
#     def __new__(cls, name, bases, attrs):
#         if name=='Model':
#             return type.__new__(cls, name, bases, attrs)
#         tableName = attrs.get('__table__', None) or name
#         logging.info('found model: %s (table: %s)' % (name, tableName))
#         mappings = dict()
#         fields = []
#         primaryKey = None
#         for k, v in attrs.items():
#             if isinstance(v, Field):
#                 logging.info('  found mapping: %s ==> %s' % (k, v))
#                 mappings[k] = v
#                 if v.primary_key:
#                     # 找到主键:
#                     if primaryKey:
#                         raise Exception('Duplicate primary key for field: %s' % k)
#                     primaryKey = k
#                 else:
#                     fields.append(k)
#         if not primaryKey:
#             raise Exception('Primary key not found.')
#         for k in mappings.keys():
#             attrs.pop(k)
#         escaped_fields = list(map(lambda f: '`%s`' % f, fields))
#         attrs['__mappings__'] = mappings # 保存属性和列的映射关系
#         attrs['__table__'] = tableName
#         attrs['__primary_key__'] = primaryKey # 主键属性名
#         attrs['__fields__'] = fields # 除主键外的属性名
#         attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
#         attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
#         attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
#         attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
#         return type.__new__(cls, name, bases, attrs)
#
# class Model(dict, metaclass=ModelMetaclass):
#
#     def __init__(self, **kw):
#         super(Model, self).__init__(**kw)
#
#     def __getattr__(self, key):
#         try:
#             return self[key]
#         except KeyError:
#             raise AttributeError(r"'Model' object has no attribute '%s'" % key)
#
#     def __setattr__(self, key, value):
#         self[key] = value
#
#     def getValue(self, key):
#         return getattr(self, key, None)
#
#     def getValueOrDefault(self, key):
#         value = getattr(self, key, None)
#         if value is None:
#             field = self.__mappings__[key]
#             if field.default is not None:
#                 value = field.default() if callable(field.default) else field.default
#                 logging.debug('using default value for %s: %s' % (key, str(value)))
#                 setattr(self, key, value)
#         return value
#
#     @classmethod
#     async def findAll(cls, where=None, args=None, **kw):
#         ' find objects by where clause. '
#         sql = [cls.__select__]
#         if where:
#             sql.append('where')
#             sql.append(where)
#         if args is None:
#             args = []
#         orderBy = kw.get('orderBy', None)
#         if orderBy:
#             sql.append('order by')
#             sql.append(orderBy)
#         limit = kw.get('limit', None)
#         if limit is not None:
#             sql.append('limit')
#             if isinstance(limit, int):
#                 sql.append('?')
#                 args.append(limit)
#             elif isinstance(limit, tuple) and len(limit) == 2:
#                 sql.append('?, ?')
#                 args.extend(limit)
#             else:
#                 raise ValueError('Invalid limit value: %s' % str(limit))
#         rs = await select(' '.join(sql), args)
#         return [cls(**r) for r in rs]
#
#     @classmethod
#     async def findNumber(cls, selectField, where=None, args=None):
#         ' find number by select and where. '
#         sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
#         if where:
#             sql.append('where')
#             sql.append(where)
#         rs = await select(' '.join(sql), args, 1)
#         if len(rs) == 0:
#             return None
#         return rs[0]['_num_']
#
#     @classmethod
#     async def find(cls, pk):
#         ' find object by primary key. '
#         rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
#         if len(rs) == 0:
#             return None
#         return cls(**rs[0])
#
#     async def save(self):
#         args = list(map(self.getValueOrDefault, self.__fields__))
#         args.append(self.getValueOrDefault(self.__primary_key__))
#         rows = await execute(self.__insert__, args)
#         if rows != 1:
#             logging.warn('failed to insert record: affected rows: %s' % rows)
#
#     async def update(self):
#         args = list(map(self.getValue, self.__fields__))
#         args.append(self.getValue(self.__primary_key__))
#         rows = await execute(self.__update__, args)
#         if rows != 1:
#             logging.warn('failed to update by primary key: affected rows: %s' % rows)
#
#     async def remove(self):
#         args = [self.getValue(self.__primary_key__)]
#         rows = await execute(self.__delete__, args)
#         if rows != 1:
#             logging.warn('failed to remove by primary key: affected rows: %s' % rows)