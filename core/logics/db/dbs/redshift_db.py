#!/usr/bin/env python
# encoding: utf-8
import psycopg2
from core.logics.db.db_driver import *


class RedshiftData:
    """
    随机生成测试数据模块
    """
    DEFAULT_PLACEHOLDER = '%s'

    @staticmethod
    def int2_data(precision, scale, unsigned, **kwargs):
        return random_int(-32768, 32767), RedshiftData.DEFAULT_PLACEHOLDER

    @staticmethod
    def int4_data(precision, scale, unsigned, **kwargs):
        return random_int(-2147483648, 2147483647), RedshiftData.DEFAULT_PLACEHOLDER

    @staticmethod
    def int8_data(precision, scale, unsigned, **kwargs):
        return random_int(-9223372036854775808, 9223372036854775807), RedshiftData.DEFAULT_PLACEHOLDER

    @staticmethod
    def float4_data(precision, scale, unsigned, **kwargs):
        return random_decimal(5, 2, -1000, 1000, unsigned), RedshiftData.DEFAULT_PLACEHOLDER

    @staticmethod
    def float8_data(precision, scale, unsigned, **kwargs):
        return random_decimal(5, 2, -1000, 1000, unsigned), RedshiftData.DEFAULT_PLACEHOLDER

    @staticmethod
    def numeric_data(precision, scale, unsigned, **kwargs):
        return random_decimal(5, 2, -1000, 1000, unsigned), RedshiftData.DEFAULT_PLACEHOLDER

    @staticmethod
    def varchar_data(precision, scale, unsigned, **kwargs):
        if precision is None or precision > 50:
            precision = 50
        return random_str_which_cn_nbytes(precision), RedshiftData.DEFAULT_PLACEHOLDER

    @staticmethod
    def bpchar_data(precision, scale, unsigned, **kwargs):
        if precision is None or precision > 50:
            precision = 50
        return random_str_without_cn_punctuation(precision), RedshiftData.DEFAULT_PLACEHOLDER

    @staticmethod
    def text_data(precision, scale, unsigned, **kwargs):
        return random_str(100), RedshiftData.DEFAULT_PLACEHOLDER

    @staticmethod
    def real_data(precision, scale, unsigned, **kwargs):
        return random_decimal(5, 2, -1000, 1000, unsigned), RedshiftData.DEFAULT_PLACEHOLDER

    @staticmethod
    def date_data(precision, scale, unsigned, **kwargs):
        return random_date(), RedshiftData.DEFAULT_PLACEHOLDER

    @staticmethod
    def time_data(precision, scale, unsigned, **kwargs):
        if precision is None or precision == 0:
            precision = 6
        maximum = int('9' * precision)
        time_without_f = random_timestamp('%Y-%m-%d %H:%M:%S')
        time_f = str(random_int(0, maximum))
        return time_without_f + '.' + time_f, RedshiftData.DEFAULT_PLACEHOLDER

    @staticmethod
    def timetz_data(precision, scale, unsigned, **kwargs):
        if precision is None or precision == 0:
            precision = 6
        return RedshiftData.time_data(precision, scale, unsigned, **kwargs)

    @staticmethod
    def timestamp_data(precision, scale, unsigned, **kwargs):
        return RedshiftData.time_data(precision, scale, unsigned, **kwargs)

    @staticmethod
    def timestamptz_data(precision, scale, unsigned, **kwargs):
        return RedshiftData.time_data(precision, scale, unsigned, **kwargs)

    @staticmethod
    def bool_data(precision, scale, unsigned, **kwargs):
        return random_int(0, 1) == 1, "%s::bool"


class RedshiftDB(DBDriver):
    ALIAS = 'REDSHIFT'

    # 数据库内置关键字，用于检索并设置默认值
    RESERVED_KEYWORD = []
    RESERVED_KEYWORD_CONTAINS = ['NULL::']
    
    query = {
        'get_table_schema':
            '''
select column_name,column_default,is_nullable,data_type, udt_name,
character_maximum_length,
numeric_precision,numeric_scale,datetime_precision
from information_schema.columns where TABLE_NAME = '{0}' order by cast(dtd_identifier as int)''',
        'get_primary_key':
            """
select pg_attribute.attname AS colname from pg_constraint
inner join pg_class ON pg_constraint.conrelid = pg_class.oid
inner join pg_attribute ON pg_attribute.attrelid = pg_class.oid
and pg_attribute.attnum = pg_constraint.conkey [ 1 ]
inner join pg_type ON pg_type.oid = pg_attribute.atttypid
where pg_class.relname = '{0}' AND pg_constraint.contype = 'p'""",
        'get_ddl': '''
SELECT
    a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) as type
  FROM pg_class c,
   pg_attribute a,
   pg_type t
   WHERE c.relname = '{0}'
   AND a.attnum > 0
   AND a.attrelid = c.oid
   AND a.atttypid = t.oid
 ORDER BY a.attnum''',
        'get_all_data':
            "select * from \"{0}\"",
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
            "constraint {table_name}_pk primary key ({column_names})",
        'rename_table':
            "alter table {0} rename to {1}",
        'rename_column':
            "alter table {0} rename {1} to {2}",
        'delete_column':
            "alter table {0} drop column {1}",
        'add_column':
            "alter table {table_name} add {name} {column_type}{default}{not_null}",
        'update_column':
            "alter table {table_name} alter column {name} type {column_type}",
        'add_column_default':
            "alter table {table_name} alter column {name} set {default}",
        'drop_column_default':
            "alter table {table_name} alter column {name} drop default",
        'add_column_not_null':
            "alter table {table_name} alter column {name} set not null",
        'drop_column_not_null':
            "alter table {table_name} alter column {name} drop not null",
        'add_primary_key':
            "alter table {0} add constraint {0}_pk primary key ({1});",
        'delete_primary_key':
            "alter table {0} drop constraint {0}_pk",
        'insert_data':
            "insert into {table_name} ({column_name}) values ({column_data})",
        'get_condition':
            "select {column_name} from {table_name} order by {column_name} desc limit {count}",
        'get_condition_customize':
            "select * from {table_name} {condition}",
        'update_data':
            "update {table_name} set {table_columns} {condition}",
        'delete_data':
            "delete from {table_name} {condition}",
        'table_exist':
            "select * from information_schema.TABLES where TABLE_NAME = '{0}'"
    }

    def __init__(self, db_config: dict, table_name):
        self.host = db_config["host"]
        self.port = db_config["port"]
        self.user = db_config["username"]
        self.password = db_config["password"]
        self.database = db_config["database"]
        self.table_name = table_name
        self.db = self.__connect()

        # 是否手动提交变化，不开启则为自动提交，开启后可进行事务模式测试
        self.manual_commit = False

    def __del__(self):
        try:
            self.db.close()
        except:
            pass

    def __connect(self):
        print('连接Redshift数据库 host: {0}, port: {1}, user: {2}, passwd: {3}, db: {4}'.format(
            self.host, self.port, self.user, self.password, self.database))
        db = psycopg2.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                              database=self.database)
        return db

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

    @db_step('获取Redshift表结构')
    def get_table_info(self):
        print('表名: {0}'.format(self.table_name))
        columns = []
        pks = []
        pk_info = [x[0] for x in self.execute(self.sql('get_primary_key').format(self.table_name))]
        types = self.execute(self.sql('get_table_schema').format(self.table_name))
        ddl = self.execute(self.sql('get_ddl').format(self.table_name))
        column_type_info = {}
        for line in ddl:
            column_name = line[0]
            column_type = line[1]
            column_type_info[column_name] = column_type.strip()
        for each in types:
            name = each[0].lower()
            default = each[1]
            not_null = each[2] == 'NO'
            data_type = each[4]
            column_type = column_type_info[each[0]]
            pk = each[0] in pk_info
            # 基于数据类型，精度、标度获取位置不同
            if default is not None:
                if default.find('identity') != -1:
                    auto_inc = True
                    default = None
                else:
                    auto_inc = False
            else:
                auto_inc = False
            unsigned = None
            if each[5] is not None:
                pre = each[5]
                sac = None
            elif each[6] is not None:
                pre = each[6]
                sac = each[7]
            elif each[8] is not None:
                pre = each[8]
                sac = None
            else:
                pre = None
                sac = None
            column = DBColumn(name, data_type, column_type, default, not_null, auto_inc, pre, sac, unsigned)
            columns.append(column)
            if pk:
                pks.append(column)
        table_info = DBTable(self.ALIAS, self.table_name, columns, pks)
        return table_info

    @db_step('获取Redshift表所有数据')
    def get_table_data(self):
        print('表名: {0}'.format(self.table_name))
        results = self.execute(self.sql('get_all_data').format(self.table_name))
        return results

    @db_step('删除Redshift表')
    def delete_table(self, raise_error=True):
        print('表名: {0}'.format(self.table_name))
        handle_exception = not raise_error
        self.execute(self.sql('delete_table').format(self.table_name), False,
                     handle_exception=handle_exception)

    def _format_default_value(self, default):
        if default is None:
            default = ''
        elif default in self.RESERVED_KEYWORD:
            default_value = default
            default = " default {0}".format(default_value)
        else:
            has_keyword = list(filter(lambda x: default.startswith(x), self.RESERVED_KEYWORD_CONTAINS))
            if has_keyword:
                default_value = default
            else:
                default_value = "'{0}'".format(default)
            default = " default {0}".format(default_value)
        return default

    @db_step('创建Redshift表')
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
                auto_inc = ' IDENTITY(0,1)'
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

    @db_step('重命名Redshift表')
    def rename_table(self, new_table_name: str):
        print('表名: {0}, 重命名为：{1}'.format(self.table_name, new_table_name))
        self.execute(self.sql('rename_table').format(self.table_name, new_table_name), False)
        self.table_name = new_table_name

    @db_step('增加Redshift表字段')
    def add_column(self, column_info: DBColumn):
        print('表名: {0}, 新增字段：{1}'.format(self.table_name, column_info.name))
        assert column_info.name, '新增字段需提供字段名称'
        assert column_info.column_type, '新增字段需提供字段类型'
        default = self._format_default_value(column_info.default)
        not_null = ' not null' if column_info.not_null else ' null'
        self.execute(self.sql('add_column').format(table_name=self.table_name, name=column_info.name,
                                                   column_type=column_info.column_type, not_null=not_null,
                                                   default=default), False)

    @db_step('更新Redshift表字段')
    def update_column(self, column_info: DBColumn):
        print('表名: {0}, 修改字段：{1}'.format(self.table_name, column_info.name))
        assert column_info.name, '新增字段需提供字段名称'
        assert column_info.column_type, '新增字段需提供字段类型'
        default = self._format_default_value(column_info.default)
        # 更新字段类型
        self.db.autocommit = True
        self.execute(self.sql('update_column').format(table_name=self.table_name, name=column_info.name,
                                                      column_type=column_info.column_type), False)
        # 更新字段默认值
        if not default:
            self.execute(self.sql('drop_column_default').format(
                table_name=self.table_name, name=column_info.name), False)
        else:
            self.execute(self.sql('add_column_default').format(
                table_name=self.table_name, name=column_info.name,
                default=default), False)

        # 更新字段非空属性
        if column_info.not_null:
            self.execute(self.sql('add_column_not_null').format(
                table_name=self.table_name, name=column_info.name), False)
        else:
            self.execute(self.sql('drop_column_not_null').format(
                table_name=self.table_name, name=column_info.name), False)
        self.db.autocommit = False

    @db_step('删除Redshift表字段')
    def delete_column(self, column_name: str):
        print('表名: {0}, 字段名：{1}'.format(self.table_name, column_name))
        self.execute(self.sql('delete_column').format(self.table_name, column_name), False)

    @db_step('重命名Redshift表字段')
    def rename_column(self, old_column_name: str, new_column_name: str):
        print('表名: {0}, 字段名：{1}, 重命名为：{2}'.format(self.table_name, old_column_name, new_column_name))
        self.execute(self.sql('rename_column').format(self.table_name, old_column_name, new_column_name), False)

    @db_step('增加Redshift表主键')
    def add_primary_key(self, keys: list):
        print('表名: {0}, 增加主键：{1}'.format(self.table_name, keys))
        self.execute(self.sql('add_primary_key').format(self.table_name, ', '.join(keys)), False)

    @db_step('删除Redshift表主键')
    def delete_primary_key(self):
        print('表名: {0}, 删除主键'.format(self.table_name))
        self.execute(self.sql('delete_primary_key').format(self.table_name), False)

    @db_step('插入Redshift表数据')
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
                    placeholders.append(RedshiftData.DEFAULT_PLACEHOLDER)
                # 如果字段允许为空，则有概率插入None
                elif not column.not_null and random_int(0, 9) == 0:
                    values.append(None)
                    placeholders.append(RedshiftData.DEFAULT_PLACEHOLDER)
                # 基于数据类型随机生成符合类型、精度、标度的数据
                else:
                    random_value, placeholder = getattr(RedshiftData, column.data_type + '_data')(
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

    @db_step('手动插入Redshift表数据')
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

    @db_step('更新Redshift表数据')
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
                    placeholders.append("{0}={1}".format(column.name, RedshiftData.DEFAULT_PLACEHOLDER))
                    continue
                # 基于数据类型随机生成符合类型、精度、标度的数据
                random_value, placeholder = getattr(RedshiftData, column.data_type + '_data')(
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

    @db_step('手动更新Redshift表数据')
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

    @db_step('删除Redshift表数据')
    def delete_data(self, count_or_condition):
        print('表名: {0}, 删除数据'.format(self.table_name))
        # 获取DBTable对象
        table_info = self.get_table_info()

        # 获得满足查找条件的数据查找对象
        conditions = self._generate_matching_condition(table_info, count_or_condition)

        # 删除多行数据，可根据self.manual_commit设置事务提交模式
        for condition in conditions:
            self.execute(self.sql('delete_data').format(table_name=self.table_name, condition=condition), False)

    @db_step('判断Redshift表是否存在')
    def is_table_exist(self):
        print('判断表名: {0}, 是否存在'.format(self.table_name))
        ret = self.execute(self.sql('table_exist').format(self.table_name))
        return len(ret) == 1

    @db_step('判断Redshift列是否存在')
    def is_column_exist(self, column_info: DBColumn):
        print('判断表名: {0}, 列名：{1}，是否存在'.format(self.table_name, column_info.name))
        return self.get_table_info().is_column_exist(column_info)
