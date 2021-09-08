#!/usr/bin/env python
# encoding: utf-8
import pymssql
from core.logics.db.db_driver import *


class SqlServerData:
    """
    随机生成测试数据模块
    """
    DEFAULT_PLACEHOLDER = '%s'

    @staticmethod
    def tinyint_data(precision, scale, unsigned, **kwargs):
        return random_int(0, 255), SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def smallint_data(precision, scale, unsigned, **kwargs):
        return random_int(-32768, 32767), SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def int_data(precision, scale, unsigned, **kwargs):
        return random_int(-2147483648, 2147483647), SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def bigint_data(precision, scale, unsigned, **kwargs):
        return random_int(-9223372036854775808, 9223372036854775807), SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def float_data(precision, scale, unsigned, **kwargs):
        return random_decimal(5, 2, -1000, 1000, unsigned), SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def real_data(precision, scale, unsigned, **kwargs):
        return random_decimal(5, 2, -1000, 1000, unsigned), SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def decimal_data(precision, scale, unsigned, **kwargs):
        if unsigned and scale is None:
            return random_decimal(precision, 2, 0, 1000, unsigned), SqlServerData.DEFAULT_PLACEHOLDER
        elif not unsigned and scale is None:
            return random_decimal(precision, 2, -1000, 1000, unsigned), SqlServerData.DEFAULT_PLACEHOLDER
        elif unsigned and scale is not None:
            int_part = int(precision) - int(scale)
            if int_part != 0:
                maximum = int('9' * int_part)
            else:
                maximum = 1
            return random_decimal(precision, scale, 0, maximum, unsigned), SqlServerData.DEFAULT_PLACEHOLDER
        else:
            int_part = int(precision) - int(scale)
            if int_part != 0:
                maximum = int('9' * int_part)
            else:
                maximum = 1
            return random_decimal(precision, scale, -maximum, maximum, unsigned), SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def numeric_data(precision, scale, unsigned, **kwargs):
        return SqlServerData.decimal_data(precision, scale, unsigned, **kwargs)

    @staticmethod
    def money_data(precision, scale, unsigned, **kwargs):
        return SqlServerData.decimal_data(int(precision)-1, scale, unsigned, **kwargs)

    @staticmethod
    def smallmoney_data(precision, scale, unsigned, **kwargs):
        return SqlServerData.decimal_data(int(precision)-1, scale, unsigned, **kwargs)

    @staticmethod
    def char_data(precision, scale, unsigned, **kwargs):
        return random_str_which_cn_nbytes(precision), SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def varchar_data(precision, scale, unsigned, **kwargs):
        if precision and int(precision) == -1:
            precision = 50
        return random_str_which_cn_nbytes(precision), SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def nchar_data(precision, scale, unsigned, **kwargs):
        return random_str(precision), SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def nvarchar_data(precision, scale, unsigned, **kwargs):
        if precision and int(precision) == -1:
            precision = 50
        return random_str(precision), SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def binary_data(precision, scale, unsigned, **kwargs):
        return random_bytes(precision), 'CONVERT(binary({0}), %s)'.format(precision)

    @staticmethod
    def varbinary_data(precision, scale, unsigned, **kwargs):
        if precision and int(precision) == -1:
            return random_bytes(50), 'CONVERT(varbinary(max), %s)'.format(precision)
        else:
            return random_bytes(precision), 'CONVERT(varbinary({0}), %s)'.format(precision)

    @staticmethod
    def bit_data(precision, scale, unsigned, **kwargs):
        return random_bit(2), SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def text_data(precision, scale, unsigned, **kwargs):
        return random_str(100), SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def ntext_data(precision, scale, unsigned, **kwargs):
        return random_str(100), SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def date_data(precision, scale, unsigned, **kwargs):
        return random_date(), SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def time_data(precision, scale, unsigned, **kwargs):
        return random_time(), SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def datetime_data(precision, scale, unsigned, **kwargs):
        maximum = int('9' * 3)
        time_without_f = random_timestamp('%Y-%m-%d %H:%M:%S')
        time_f = str(random_int(0, maximum))
        return time_without_f + '.' + time_f, SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def datetime2_data(precision, scale, unsigned, **kwargs):
        maximum = int('9' * 7)
        time_without_f = random_timestamp('%Y-%m-%d %H:%M:%S')
        time_f = str(random_int(0, maximum))
        return time_without_f + '.' + time_f, SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def timestamp_data(precision, scale, unsigned, **kwargs):
        return None, SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def datetimeoffset_data(precision, scale, unsigned, **kwargs):
        return SqlServerData.datetime2_data(precision, scale, unsigned, **kwargs)

    @staticmethod
    def smalldatetime_data(precision, scale, unsigned, **kwargs):
        return random_date(), SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def image_data(precision, scale, unsigned, **kwargs):
        return random_bytes(100), 'CONVERT(binary({0}), %s)'.format(100)

    @staticmethod
    def uniqueidentifier_data(precision, scale, unsigned, **kwargs):
        return random_uuid(), SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def xml_data(precision, scale, unsigned, **kwargs):
        return random_xml(), SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def geometry_data(precision, scale, unsigned, **kwargs):
        point = random_point()
        geom_type = "POINT(%s %s)" % (point[0], point[1])
        return geom_type, SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def geography_data(precision, scale, unsigned, **kwargs):
        point = random_point()
        geom_type = "POINT(%s %s)" % (point[0], point[1])
        return geom_type, SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def sysname_data(precision, scale, unsigned, **kwargs):
        return None, SqlServerData.DEFAULT_PLACEHOLDER

    @staticmethod
    def sql_variant_data(precision, scale, unsigned, **kwargs):
        return random_str_which_cn_nbytes(50), SqlServerData.DEFAULT_PLACEHOLDER


class SqlServerDB(DBDriver):
    ALIAS = 'SQLSERVER'

    # 数据库内置关键字，用于检索并设置默认值
    RESERVED_KEYWORD = ['sysdatetime()']

    query = {
        'get_table_schema':
            '''
select COLUMN_NAME,COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE,
CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION
from information_schema.columns where TABLE_NAME = '{0}' order by ORDINAL_POSITION''',
        'get_primary_key':
            """
select COL_NAME(object_id('{0}'),c.colid)
from sysobjects a,sysindexes b,sysindexkeys c
where a.name=b.name and b.id=c.id and b.indid=c.indid
and a.xtype='PK' and a.parent_obj=object_id('{0}')
and c.id=object_id('{0}')""",
        'get_auto_increment':
            "select name from sys.columns where is_identity = 1 and OBJECT_NAME(OBJECT_ID)='{0}'",
        'enable_identity_insert':
            "SET IDENTITY_INSERT {0} ON",
        'get_ddl':
            """
DECLARE @table_name SYSNAME
SELECT @table_name = 'dbo.{0}'
DECLARE
	  @object_name SYSNAME
	, @object_id INT
SELECT
	  @object_name = '[' + s.name + '].[' + o.name + ']'
	, @object_id = o.[object_id]
FROM sys.objects o WITH (NOWAIT)
JOIN sys.schemas s WITH (NOWAIT) ON o.[schema_id] = s.[schema_id]
WHERE s.name + '.' + o.name = @table_name
	AND o.[type] = 'U'
	AND o.is_ms_shipped = 0

DECLARE @SQL NVARCHAR(MAX) = ''

;WITH index_column AS
(
	SELECT
		  ic.[object_id]
		, ic.index_id
		, ic.is_descending_key
		, ic.is_included_column
		, c.name
	FROM sys.index_columns ic WITH (NOWAIT)
	JOIN sys.columns c WITH (NOWAIT) ON ic.[object_id] = c.[object_id] AND ic.column_id = c.column_id
	WHERE ic.[object_id] = @object_id
),
fk_columns AS
(
	 SELECT
		  k.constraint_object_id
		, cname = c.name
		, rcname = rc.name
	FROM sys.foreign_key_columns k WITH (NOWAIT)
	JOIN sys.columns rc WITH (NOWAIT) ON rc.[object_id] = k.referenced_object_id AND rc.column_id = k.referenced_column_id
	JOIN sys.columns c WITH (NOWAIT) ON c.[object_id] = k.parent_object_id AND c.column_id = k.parent_column_id
	WHERE k.parent_object_id = @object_id
)
SELECT @SQL = STUFF((
	SELECT c.name + ' ' +
		CASE WHEN c.is_computed = 1
			THEN 'AS ' + cc.[definition]
			ELSE tp.name +
				CASE WHEN tp.name IN ('varchar', 'char', 'varbinary', 'binary')
					   THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR(5)) END + ')'
					 WHEN tp.name IN ('nvarchar', 'nchar')
					   THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + ')'
					 WHEN tp.name IN ('datetime2', 'time2', 'datetimeoffset')
					   THEN '(' + CAST(c.scale AS VARCHAR(5)) + ')'
					 WHEN tp.name IN ('decimal','numeric')
					   THEN '(' + CAST(c.[precision] AS VARCHAR(5)) + ',' + CAST(c.scale AS VARCHAR(5)) + ')'
					ELSE ''
				END + char(10)
		END
	FROM sys.columns c WITH (NOWAIT)
	JOIN sys.types tp WITH (NOWAIT) ON c.user_type_id = tp.user_type_id
	LEFT JOIN sys.computed_columns cc WITH (NOWAIT) ON c.[object_id] = cc.[object_id] AND c.column_id = cc.column_id
	LEFT JOIN sys.default_constraints dc WITH (NOWAIT) ON c.default_object_id != 0 AND c.[object_id] = dc.parent_object_id AND c.column_id = dc.parent_column_id
	LEFT JOIN sys.identity_columns ic WITH (NOWAIT) ON c.is_identity = 1 AND c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
	WHERE c.[object_id] = @object_id
	ORDER BY c.column_id
	FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 0, '')


PRINT @SQL; SELECT @SQL;""",
        'get_all_data':
            "select * from {0}",
        'delete_table':
            "drop table \"{0}\"",
        'create_table': '''
create table "{table_name}"
(
{columns}
)''',
        'create_column':
            "{name} {column_type}{default}{not_null}{auto_inc}",
        'primary_key':
            "constraint {table_name}_pk primary key nonclustered ({column_names})",
        'rename_table':
            "exec sp_rename 'dbo.{0}', {1}, 'OBJECT'",
        'rename_column':
            "exec sp_rename 'dbo.{0}.{1}', {2}, 'COLUMN'",
        'delete_column':
            "alter table {0} drop column {1}",
        'add_column':
            "alter table {table_name} add {name} {column_type}{default}{not_null}",
        'update_column':
            "alter table {table_name} alter column {name} {column_type}{not_null}",
        'get_constraint':
            """
select name
from sysobjects t
where id=(select cdefault from syscolumns where id=object_id('{0}') and name='{1}')""",
        'delete_constraint':
            "alter table {0} drop constraint {1}",
        'add_default':
            "alter table {0} add {1} for {2}",
        'add_primary_key':
            "alter table {0} add constraint {0}_pk primary key nonclustered ({1});",
        'delete_primary_key':
            "alter table {0} drop constraint {0}_pk",
        'insert_data':
            "insert into {table_name} ({column_name}) values ({column_data})",
        'get_condition':
            "select top {count} {column_name} from {table_name} order by {column_name} desc",
        'get_condition_customize':
            "select * from {table_name} {condition}",
        'update_data':
            "update {table_name} set {table_columns} {condition}",
        'delete_data':
            "delete {table_name} {condition}",
        'table_exist':
            "select * from information_schema.TABLES where TABLE_NAME = '{0}'"
    }

    def __init__(self, db_config: dict, table_name):
        self.host = db_config["ip"]
        self.port = db_config["port"]
        self.user = db_config["username"]
        self.password = db_config["password"]
        self.database = db_config["database"]
        self.charset = 'utf8'
        self.table_name = table_name
        self.db = self.__connect()

        # 是否手动提交变化，不开启则为自动提交，开启后可进行事务模式测试
        self.manual_commit = False

    def __connect(self):
        print('连接Sql Server数据库 host: {0}, port: {1}, user: {2}, passwd: {3}, db: {4}, charset: {5}'.format(
            self.host, self.port, self.user, self.password, self.database, self.charset))
        db = pymssql.connect(host=self.host + ":" + str(self.port),
                             user=self.user, password=self.password, database=self.database, charset=self.charset)
        return db

    def __del__(self):
        try:
            self.db.close()
        except:
            pass

    @db_call
    def execute(self, sql, fetch_all=True, sql_params=None, handle_exception=False):
        if sql_params is None:
            sql_params = []
        cursor = self.db.cursor()
        print('SQL: {0}'.format(sql))
        for param in sql_params:
            print('\t' + str(param))
        try:
            cursor.execute(sql, sql_params)
            if fetch_all:
                result = cursor.fetchall()
                print('RET: {0}'.format(result))
                return result
        except Exception as e:
            print(e)
            # 是否处理异常
            if not handle_exception:
                raise e
        finally:
            cursor.close()
            # 是否手动提交变化，不开启则为自动提交，开启后可进行事务模式测试
            if not self.manual_commit:
                self.db.commit()

    @db_step('获取Sql Server表结构')
    def get_table_info(self):
        print('表名: {0}'.format(self.table_name))
        columns = []
        pks = []
        pk_info = [x[0] for x in self.execute(self.sql('get_primary_key').format(self.table_name))]
        auto_inc_info = [x[0] for x in self.execute(self.sql('get_auto_increment').format(self.table_name))]
        ddl = self.execute(self.sql('get_ddl').format(self.table_name))[0][0]
        column_type_info = {}
        for line in ddl.split('\n'):
            if not line:
                continue
            sp = line.split(' ')
            column_name = sp[0]
            column_type = ' '.join(sp[1:])
            column_type_info[column_name] = column_type.strip()
        types = self.execute(self.sql('get_table_schema').format(self.table_name))
        for each in types:
            name = each[0].lower()
            default = each[1]
            not_null = each[2] == 'NO'
            data_type = each[3]
            column_type = column_type_info[each[0]]
            pk = each[0] in pk_info
            auto_inc = each[0] in auto_inc_info
            unsigned = None
            # 基于数据类型，精度、标度获取位置不同
            if each[4] is not None:
                pre = each[4]
                sca = None
            elif each[6] is not None:
                pre = each[6]
                sca = each[7]
            elif each[8] is not None:
                pre = each[8]
                sca = None
            else:
                pre = None
                sca = None
            column = DBColumn(name, data_type, column_type, default, not_null, auto_inc, pre, sca, unsigned)
            columns.append(column)
            if pk:
                pks.append(column)
        table_info = DBTable(self.ALIAS, self.table_name, columns, pks)
        return table_info

    @db_step('获取Sql Server表所有数据')
    def get_table_data(self):
        print('表名: {0}'.format(self.table_name))
        results = self.execute(self.sql('get_all_data').format(self.table_name))
        return results

    @db_step('删除Sql Server表')
    def delete_table(self, raise_error=True):
        print('表名: {0}'.format(self.table_name))
        handle_exception = not raise_error
        self.execute(self.sql('delete_table').format(self.table_name), False,
                     handle_exception=handle_exception)

    def _format_default_value(self, default):
        if default is None:
            default = ''
        else:
            default_value = "'{0}'".format(default) if default not in self.RESERVED_KEYWORD else default
            default = " default {0}".format(default_value)
        return default

    @db_step('创建Sql Server表')
    def create_table(self, table_info: DBTable):
        print('表名: {0}'.format(self.table_name))
        # 检查DBTable对象是否为当前数据库
        if table_info.db.upper() != self.ALIAS:
            raise DBNotMatchingException('当前DB实例为：{0}, 与所要操作的数据库：{1}, 不匹配'.format(
                self.ALIAS, table_info.db))

        columns = []
        for column in table_info.columns:
            name = column.name
            column_type = column.column_type
            default = self._format_default_value(column.default)
            not_null = ' not null' if column.not_null else ' null'
            if column.auto_inc:
                auto_inc = ' identity'
                not_null = ''
            else:
                auto_inc = ''
            columns.append('\t' + self.sql('create_column').format(
                name=name, column_type=column_type, not_null=not_null, default=default, auto_inc=auto_inc))
        if table_info.primary_keys:
            columns.append('\t' + self.sql('primary_key').format(
                table_name=self.table_name,
                column_names=', '.join([each.name for each in table_info.primary_keys])
            ))
        table_basic = self.sql('create_table').format(table_name=self.table_name, columns=',\n'.join(columns))
        self.execute(table_basic, False)

    @db_step('重命名Sql Server表')
    def rename_table(self, new_table_name: str):
        print('表名: {0}, 重命名为：{1}'.format(self.table_name, new_table_name))
        self.execute(self.sql('rename_table').format(self.table_name, new_table_name), False)
        self.table_name = new_table_name

    @db_step('增加Sql Server表字段')
    def add_column(self, column_info: DBColumn):
        print('表名: {0}, 新增字段：{1}'.format(self.table_name, column_info.name))
        assert column_info.name, '新增字段需提供字段名称'
        assert column_info.column_type, '新增字段需提供字段类型'
        default = self._format_default_value(column_info.default)
        not_null = ' not null' if column_info.not_null else ' null'
        self.execute(self.sql('add_column').format(table_name=self.table_name, name=column_info.name,
                                                   column_type=column_info.column_type, not_null=not_null,
                                                   default=default), False)

    @db_step('更新Sql Server表字段')
    def update_column(self, column_info: DBColumn):
        print('表名: {0}, 修改字段：{1}'.format(self.table_name, column_info.name))
        assert column_info.name, '新增字段需提供字段名称'
        assert column_info.column_type, '新增字段需提供字段类型'
        default = self._format_default_value(column_info.default)
        not_null = ' not null' if column_info.not_null else ' null'

        # 获取并删除默认值
        constraint_info = self.execute(self.sql('get_constraint').format(self.table_name, column_info.name))
        if len(constraint_info) > 0:
            self.execute(self.sql('delete_constraint').format(self.table_name, constraint_info[0][0]), False)

        # 更新字段类型
        self.execute(self.sql('update_column').format(table_name=self.table_name, name=column_info.name,
                                                      column_type=column_info.column_type, not_null=not_null), False)

        # 更新默认值
        if default:
            self.execute(self.sql('add_default').format(self.table_name, default ,column_info.name,), False)

    @db_step('删除Sql Server表字段')
    def delete_column(self, column_name: str):
        print('表名: {0}, 字段名：{1}'.format(self.table_name, column_name))
        # 获取并删除默认值
        constraint_info = self.execute(self.sql('get_constraint').format(self.table_name, column_name))
        if len(constraint_info) > 0:
            self.execute(self.sql('delete_constraint').format(self.table_name, constraint_info[0][0]), False)

        self.execute(self.sql('delete_column').format(self.table_name, column_name), False)

    @db_step('重命名Sql Server表字段')
    def rename_column(self, old_column_name: str, new_column_name: str):
        print('表名: {0}, 字段名：{1}, 重命名为：{2}'.format(self.table_name, old_column_name, new_column_name))
        self.execute(self.sql('rename_column').format(self.table_name, old_column_name, new_column_name), False)

    @db_step('增加Sql Server表主键')
    def add_primary_key(self, keys: list):
        print('表名: {0}, 增加主键：{1}'.format(self.table_name, keys))
        self.execute(self.sql('add_primary_key').format(self.table_name, ', '.join(keys)), False)

    @db_step('删除Sql Server表主键')
    def delete_primary_key(self):
        print('表名: {0}, 删除主键'.format(self.table_name))
        self.execute(self.sql('delete_primary_key').format(self.table_name), False)

    @db_step('插入Sql Server表数据')
    def insert_data(self, count: int):
        print('表名: {0}, 插入数据'.format(self.table_name))
        # 获取DBTable对象
        table_info = self.get_table_info()

        # 插入多行数据，可根据self.manual_commit设置事务提交模式
        for _ in range(count):
            values = []
            placeholders = []
            column_names = []
            for column in table_info.columns:
                # 自增字段将不赋值
                if column.auto_inc:
                    continue
                column_names.append(column.name)
                # 如果字段名为SORTED_AND_UNIQUE_COLUMN_NAME，将会随机生成唯一值
                if column.name == SORTED_AND_UNIQUE_COLUMN_NAME:
                    values.append(random_sorted_and_unique_num())
                    placeholders.append(SqlServerData.DEFAULT_PLACEHOLDER)
                # 如果字段允许为空，则有概率插入None
                elif not column.not_null and random_int(0, 9) == 0:
                    values.append(None)
                    placeholders.append(SqlServerData.DEFAULT_PLACEHOLDER)
                # 基于数据类型随机生成符合类型、精度、标度的数据
                else:
                    random_value, placeholder = getattr(SqlServerData, column.data_type + '_data')(
                        column.precision, column.scale, column.unsigned, column_type=column.column_type)
                    if type(random_value) is list:
                        values.extend(random_value)
                    else:
                        values.append(random_value)
                    placeholders.append(placeholder)
            self.execute(
                self.sql('insert_data').format(table_name=self.table_name,
                                               column_name=', '.join(column_names),
                                               column_data=', '.join(placeholders)),
                False, sql_params=tuple(values))

    @db_step('手动插入Sql Server表数据')
    def manual_insert_data(self, column_names: list, placeholders: list, values: list):
        print('表名: {0}, 手动插入数据'.format(self.table_name))
        assert len(column_names) == len(placeholders), '列数量与预替换符数量不匹配'
        self.execute(self.sql('insert_data').format(table_name=self.table_name,
                                                    column_name=', '.join(column_names),
                                                    column_data=', '.join(placeholders)),
                     False, sql_params=tuple(values))

    def _generate_matching_condition(self, table_info: DBTable, count_or_condition: int):
        conditions = []
        auto_inc_and_pk = list(filter(lambda x: x.auto_inc, table_info.primary_keys))
        pks = [each for each in table_info.primary_keys]
        if type(count_or_condition) == int:
            # 有自增主键，选择自增主键作为查找依据
            if len(auto_inc_and_pk) > 0:
                column_name = auto_inc_and_pk[0].name
            # 有主键，选择第一个主键作为查找依据
            elif len(pks) > 0:
                column_name = pks[0].name
            # 无主键，选择第一个字段作为查找依据
            else:
                column_name = table_info.columns[0].name
            columns_value = self.execute(self.sql('get_condition').
                                         format(table_name=self.table_name,
                                                column_name=column_name,
                                                count=count_or_condition))
            assert len(columns_value) == count_or_condition, \
                '当前表不存在：{0}行数据，可进行操作'.format(count_or_condition)
            for column_value in columns_value:
                conditions.append("where {0} = '{1}'".format(column_name, column_value[0]))
        # 其他需用户指定查找条件
        else:
            columns_value = self.execute(self.sql('get_condition_customize').
                                         format(table_name=self.table_name, condition=count_or_condition))
            assert len(columns_value) > 0, \
                '当前表不存在满足条件的数据可进行操作，条件：{0}'.format(count_or_condition)
            conditions.append(count_or_condition)
        return conditions

    @db_step('更新Sql Server表数据')
    def update_data(self, count_or_condition):
        print('表名: {0}, 更新数据'.format(self.table_name))
        # 获取DBTable对象
        table_info = self.get_table_info()

        # 获得满足查找条件的数据查找对象
        conditions = self._generate_matching_condition(table_info, count_or_condition)

        # 更新多行数据，可根据self.manual_commit设置事务提交模式
        for condition in conditions:
            values = []
            placeholders = []
            for column in table_info.columns:
                # 字段为SORTED_AND_UNIQUE_COLUMN_NAME，timestamp，自增字段，或主键字段，将不会修改改数据
                if column.name == SORTED_AND_UNIQUE_COLUMN_NAME or \
                        column.auto_inc or \
                        column.name in [pk.name for pk in table_info.primary_keys] or \
                        column.data_type == 'timestamp':
                    continue
                # 如果字段允许为空，则有概率插入None
                if not column.not_null and random_int(0, 9) == 0:
                    values.append(None)
                    placeholders.append("{0}={1}".format(column.name, SqlServerData.DEFAULT_PLACEHOLDER))
                    continue
                # 基于数据类型随机生成符合类型、精度、标度的数据
                random_value, placeholder = getattr(SqlServerData, column.data_type + '_data')(
                    column.precision, column.scale, column.unsigned, column_type=column.column_type)
                if type(random_value) is list:
                    values.extend(random_value)
                else:
                    values.append(random_value)
                placeholders.append("{0}={1}".format(column.name, placeholder))
            self.execute(
                self.sql('update_data').format(table_name=self.table_name,
                                               table_columns=', '.join(placeholders),
                                               condition=condition),
                False, sql_params=tuple(values))

    @db_step('手动更新Sql Server表数据')
    def manual_update_data(self, count_or_condition, column_names: list, placeholders: list, values: list):
        print('表名: {0}, 手动更新数据'.format(self.table_name))
        assert len(column_names) == len(placeholders), '列数量与预替换符数量不匹配'
        table_columns = ['{0}={1}'.format(name, placeholders[index]) for index, name in enumerate(column_names)]
        # 获取DBTable对象
        table_info = self.get_table_info()

        # 获得满足查找条件的数据查找对象
        conditions = self._generate_matching_condition(table_info, count_or_condition)

        # 更新多行数据，可根据self.manual_commit设置事务提交模式
        for condition in conditions:
            self.execute(self.sql('update_data').format(table_name=self.table_name,
                                                        table_columns=', '.join(table_columns),
                                                        condition=condition),
                         False, sql_params=tuple(values))

    @db_step('删除Sql Server表数据')
    def delete_data(self, count_or_condition):
        print('表名: {0}, 删除数据'.format(self.table_name))
        # 获取DBTable对象
        table_info = self.get_table_info()

        # 获得满足查找条件的数据查找对象
        conditions = self._generate_matching_condition(table_info, count_or_condition)

        # 删除多行数据，可根据self.manual_commit设置事务提交模式
        for condition in conditions:
            self.execute(self.sql('delete_data').format(table_name=self.table_name, condition=condition), False)

    @db_step('判断Sql Server表是否存在')
    def is_table_exist(self):
        print('判断表名: {0}, 是否存在'.format(self.table_name))
        ret = self.execute(self.sql('table_exist').format(self.table_name))
        return len(ret) == 1

    @db_step('判断Sql Server列是否存在')
    def is_column_exist(self, column_info: DBColumn):
        print('判断表名: {0}, 列名：{1}，是否存在'.format(self.table_name, column_info.name))
        return self.get_table_info().is_column_exist(column_info)
