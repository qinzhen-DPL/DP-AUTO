#!/usr/bin/env python
# encoding: utf-8
import ibm_db
from core.logics.db.db_driver import *


class DB2Data:
    """
    随机生成测试数据模块
    """
    DEFAULT_PLACEHOLDER = '?'

    @staticmethod
    def smallint_data(precision, scale, unsigned, **kwargs):
        return random_int(-32768, 32767), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def integer_data(precision, scale, unsigned, **kwargs):
        return random_int(-2147483648, 2147483647), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def bigint_data(precision, scale, unsigned, **kwargs):
        return random_int(-9223372036854775808, 9223372036854775807), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def double_data(precision, scale, unsigned, **kwargs):
        return random_decimal(8, 4, -9999, 9999, unsigned), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def real_data(precision, scale, unsigned, **kwargs):
        return random_decimal(4, 2, -100, 100, unsigned), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def decimal_data(precision, scale, unsigned, **kwargs):
        int_part = int(precision) - int(scale)
        if int_part != 0:
            maximum = int('9' * int_part)
        else:
            maximum = 1
        return random_decimal(precision, scale, -maximum, maximum, unsigned), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def character_data(precision, scale, unsigned, **kwargs):
        if precision > 100:
            precision = 100
        return random_str_which_cn_nbytes(precision), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def varchar_data(precision, scale, unsigned, **kwargs):
        if precision > 100:
            precision = 100
        return random_str_which_cn_nbytes(precision), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def long_varchar_data(precision, scale, unsigned, **kwargs):
        return random_str_which_cn_nbytes(200), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def character_for_bit_data_data(precision, scale, unsigned, **kwargs):
        if precision > 100:
            precision = 100
        return random_bytes(precision), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def varchar_for_bit_data_data(precision, scale, unsigned, **kwargs):
        if precision > 100:
            precision = 100
        return random_bytes(precision), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def long_varchar_for_bit_data_data(precision, scale, unsigned, **kwargs):
        return random_bytes(200), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def blob_data(precision, scale, unsigned, **kwargs):
        if precision > 100:
            precision = 100
        return random_bytes(precision), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def clob_data(precision, scale, unsigned, **kwargs):
        if precision > 100:
            precision = 100
        return random_str_which_cn_nbytes(precision), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def date_data(precision, scale, unsigned, **kwargs):
        return random_date(), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def time_data(precision, scale, unsigned, **kwargs):
        maximum = 0 if scale == 0 else int('9' * scale)
        time_without_f = random_timestamp('%Y-%m-%d %H:%M:%S')
        time_f = str(random_int(0, maximum))
        return time_without_f + '.' + time_f, DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def timestamp_data(precision, scale, unsigned, **kwargs):
        return DB2Data.time_data(precision, scale, unsigned, **kwargs)

    @staticmethod
    def xml_data(precision, scale, unsigned, **kwargs):
        return random_xml(), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def graphic_data(precision, scale, unsigned, **kwargs):
        if precision > 100:
            precision = 100
        return random_str(precision), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def vargraphic_data(precision, scale, unsigned, **kwargs):
        if precision > 100:
            precision = 100
        return random_str(precision), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def long_vargraphic_data(precision, scale, unsigned, **kwargs):
        return random_str(200), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def decfloat_data(precision, scale, unsigned, **kwargs):
        return random_decimal(8, 2, -1000000, 1000000, unsigned), DB2Data.DEFAULT_PLACEHOLDER

    @staticmethod
    def dbclob_data(precision, scale, unsigned, **kwargs):
        if precision > 100:
            precision = 100
        return random_str_which_cn_nbytes(precision), DB2Data.DEFAULT_PLACEHOLDER


class DB2DB(DBDriver):
    ALIAS = 'DB2'

    # 数据库内置关键字，用于检索并设置默认值
    RESERVED_KEYWORD = []

    query = {
        'get_table_schema':
            '''
SELECT
COLNAME, DEFAULT, NULLS, TYPENAME, COLLATIONNAME,
LENGTH, SCALE,
KEYSEQ, IDENTITY
FROM syscat.COLUMNS t WHERE tabname='{1}' and tabschema = '{0}' order by COLNO''',
        'get_all_data':
            "select * from {0}.\"{1}\"",
        'delete_table':
            "drop table {0}.\"{1}\"",
        'create_table': '''
create table {schema}."{table_name}"
(
{columns}
)''',
        'create_column':
            "{name} {column_type}{default}{not_null}{auto_inc}",
        'primary_key':
            "constraint {table_name}_pk primary key ({column_names})",
        'rename_table':
            'rename table {0}."{1}" to "{2}"',
        'rename_column':
            'alter table {0}."{1}" rename column {2} to {3}',
        'delete_column':
            'alter table {0}."{1}" drop column {2}',
        'add_column':
            'alter table {schema}."{table_name}" add column {name} {column_type}{default}{not_null}',
        'update_column':
            'alter table {schema}."{table_name}" alter column {name} set data type {column_type}',
        'add_column_default':
            'alter table {schema}."{table_name}" alter column {name} set {default}',
        'drop_column_default':
            'alter table {schema}."{table_name}" alter column {name} drop default',
        'add_column_not_null':
            'alter table {schema}."{table_name}" alter column {name} set not null',
        'drop_column_not_null':
            'alter table {schema}."{table_name}" alter column {name} drop not null',
        'reorg_table':
            "call sysproc.admin_cmd('reorg table {0}.\"{1}\"')",
        'add_primary_key':
            'alter table {0}."{1}" add constraint {1}_pk primary key ({2})',
        'delete_primary_key':
            'alter table {0}."{1}" drop constraint {1}_pk',
        'insert_data':
            'insert into {schema}."{table_name}" ({column_name}) values ({column_data})',
        'get_condition':
            'select {column_name} from {schema}."{table_name}" order by {column_name} desc fetch first {count} rows only',
        'get_condition_customize':
            'select * from {schema}."{table_name}" {condition}',
        'update_data':
            'update {schema}."{table_name}" set {table_columns} {condition}',
        'delete_data':
            'delete from {schema}."{table_name}" {condition}',
        'table_exist':
            "SELECT * FROM syscat.TABLES t WHERE tabname='{0}' and tabschema = '{1}'"
    }

    def __init__(self, db_config: dict, table_name):
        self.host = db_config["ip"]
        self.port = db_config["jdbc-port"]
        self.user = db_config["username"]
        self.password = db_config["password"]
        self.database = db_config["database"]
        self.schema = db_config["schema"]
        self.table_name = table_name
        self.db = self.__connect()

        # 是否手动提交变化，不开启则为自动提交，开启后可进行事务模式测试
        self.manual_commit = False

    def __del__(self):
        try:
            ibm_db.close(self.db)
        except:
            pass

    def __connect(self):
        print('连接DB2数据库 host: {0}, port: {1}, user: {2}, passwd: {3}, db: {4}'.format(
            self.host, self.port, self.user, self.password, self.database))
        db = ibm_db.connect("DATABASE={0};HOSTNAME={1};PORT={2};PROTOCOL=TCPIP;UID={3};PWD={4};".format(
            self.database, self.host, self.port, self.user, self.password), "", "")
        return db

    @db_call
    def execute(self, sql, fetch_all=True, sql_params=None, handle_exception=False):
        print('SQL: {0}'.format(sql))
        try:
            if sql_params:
                for param in sql_params:
                    print('\t' + str(param))
                pre_sql = ibm_db.prepare(self.db, sql)
                stmt = ibm_db.execute(pre_sql, sql_params)
            else:
                stmt = ibm_db.exec_immediate(self.db, sql)
            if fetch_all:
                ret = ibm_db.fetch_tuple(stmt)
                result = []
                while ret:
                    result.append(ret)
                    ret = ibm_db.fetch_tuple(stmt)
                print('RET: {0}'.format(result))
                return result
        except Exception as e:
            # 是否处理异常
            print(e)
            if not handle_exception:
                raise e
        finally:
            # 是否手动提交变化，不开启则为自动提交，开启后可进行事务模式测试
            if not self.manual_commit:
                ibm_db.commit(self.db)

    @db_step('获取DB2表结构')
    def get_table_info(self):
        print('表名: {0}'.format(self.table_name))
        types = self.execute(self.sql('get_table_schema').format(self.schema, self.table_name))
        columns = []
        pks = []
        for each in types:
            name = each[0].lower()
            default = each[1]
            not_null = each[2] == 'N'
            data_type = each[3]
            collation_name = each[4]
            pk = each[7] is not None
            auto_inc = each[8] == 'Y'
            unsigned = None
            pre = each[5]
            sca = each[6]
            # 基于数据类型，精度、标度获取位置不同
            if data_type == 'DECIMAL':
                column_type = '{0}({1},{2})'.format(data_type, pre, sca)
            elif data_type == 'DBCLOB' or data_type == 'CLOB' or data_type == 'BLOB' or \
                    data_type == 'GRAPHIC' or data_type == 'VARGRAPHIC':
                column_type = '{0}({1})'.format(data_type, pre)
            elif data_type == 'TIMESTAMP':
                column_type = '{0}({1})'.format(data_type, sca)
            elif data_type == 'CHARACTER' and collation_name == 'IDENTITY':
                column_type = 'CHAR({0}) FOR BIT DATA'.format(pre)
                data_type += ' FOR BIT DATA'
            elif data_type == 'CHARACTER' and collation_name == 'UNIQUE':
                column_type = 'CHAR({0})'.format(pre)
            elif data_type == 'VARCHAR' and collation_name == 'IDENTITY':
                column_type = 'VARCHAR({0}) FOR BIT DATA'.format(pre)
                data_type += ' FOR BIT DATA'
            elif data_type == 'VARCHAR' and collation_name == 'UNIQUE':
                column_type = 'VARCHAR({0})'.format(pre)
            elif data_type == 'LONG VARCHAR' and collation_name == 'IDENTITY':
                column_type = 'LONG VARCHAR FOR BIT DATA'
                data_type += ' FOR BIT DATA'
            elif data_type == 'LONG VARCHAR' and collation_name == 'UNIQUE':
                column_type = 'LONG VARCHAR'
            else:
                column_type = data_type
            data_type = data_type.lower().replace(' ', '_')
            column = DBColumn(name, data_type, column_type, default, not_null, auto_inc, pre, sca, unsigned)
            columns.append(column)
            if pk:
                pks.append(column)
        table_info = DBTable(self.ALIAS, self.table_name, columns, pks)
        return table_info

    @db_step('获取DB2表所有数据')
    def get_table_data(self):
        print('表名: {0}'.format(self.table_name))
        results = self.execute(self.sql('get_all_data').format(self.schema, self.table_name))
        return results

    @db_step('删除DB2表')
    def delete_table(self, raise_error=True):
        print('表名: {0}'.format(self.table_name))
        handle_exception = not raise_error
        self.execute(self.sql('delete_table').format(self.schema, self.table_name), False,
                     handle_exception=handle_exception)

    def _format_default_value(self, default):
        if default is None:
            default = ''
        else:
            default_value = "'{0}'".format(default) if default not in self.RESERVED_KEYWORD else default
            default = " default {0}".format(default_value)
        return default

    @db_step('创建DB2表')
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
            not_null = ' not null' if column.not_null else ''
            if column.auto_inc:
                auto_inc = ' generated always as identity'
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
        table_basic = self.sql('create_table').format(schema=self.schema, table_name=self.table_name,
                                                      columns=',\n'.join(columns))
        self.execute(table_basic, False)

    @db_step('重命名DB2表')
    def rename_table(self, new_table_name: str):
        print('表名: {0}, 重命名为：{1}'.format(self.table_name, new_table_name))
        self.execute(self.sql('rename_table').format(self.schema, self.table_name, new_table_name), False)
        self.table_name = new_table_name

    @db_step('增加DB2表字段')
    def add_column(self, column_info: DBColumn):
        print('表名: {0}, 新增字段：{1}'.format(self.table_name, column_info.name))
        assert column_info.name, '新增字段需提供字段名称'
        assert column_info.column_type, '新增字段需提供字段类型'
        default = self._format_default_value(column_info.default)
        not_null = ' not null' if column_info.not_null else ' null'
        self.execute(self.sql('add_column').format(schema=self.schema, table_name=self.table_name,
                                                   name=column_info.name, column_type=column_info.column_type,
                                                   not_null=not_null, default=default), False)

        # 同步数据库表信息，否则更新无效
        self.execute(self.sql('reorg_table').format(self.schema, self.table_name), False)

    @db_step('更新DB2表字段')
    def update_column(self, column_info: DBColumn):
        print('表名: {0}, 修改字段：{1}'.format(self.table_name, column_info.name))
        assert column_info.name, '新增字段需提供字段名称'
        assert column_info.column_type, '新增字段需提供字段类型'
        default = self._format_default_value(column_info.default)
        # 更新字段类型
        self.execute(self.sql('update_column').format(schema=self.schema, table_name=self.table_name,
                                                      name=column_info.name,
                                                      column_type=column_info.column_type), False)
        # 更新字段默认值
        if not default:
            self.execute(self.sql('drop_column_default').format(
                schema=self.schema, table_name=self.table_name, name=column_info.name), False)
        else:
            self.execute(self.sql('add_column_default').format(
                schema=self.schema, table_name=self.table_name, name=column_info.name,
                default=default), False)

        # 更新字段非空属性
        if column_info.not_null:
            self.execute(self.sql('add_column_not_null').format(
                schema=self.schema, table_name=self.table_name, name=column_info.name), False)
        else:
            self.execute(self.sql('drop_column_not_null').format(
                schema=self.schema, table_name=self.table_name, name=column_info.name), False)

        # 同步数据库表信息，否则更新无效
        self.execute(self.sql('reorg_table').format(self.schema, self.table_name), False)

    @db_step('删除DB2表字段')
    def delete_column(self, column_name: str):
        print('表名: {0}, 字段名：{1}'.format(self.table_name, column_name))
        self.execute(self.sql('delete_column').format(self.schema, self.table_name, column_name), False)

        # 同步数据库表信息，否则更新无效
        self.execute(self.sql('reorg_table').format(self.schema, self.table_name), False)

    @db_step('重命名DB2表字段')
    def rename_column(self, old_column_name: str, new_column_name: str):
        print('表名: {0}, 字段名：{1}, 重命名为：{2}'.format(self.table_name, old_column_name, new_column_name))
        self.execute(self.sql('rename_column').format(self.schema, self.table_name,
                                                      old_column_name, new_column_name), False)

        # 同步数据库表信息，否则更新无效
        self.execute(self.sql('reorg_table').format(self.schema, self.table_name), False)

    @db_step('增加DB2表主键')
    def add_primary_key(self, keys: list):
        print('表名: {0}, 增加主键：{1}'.format(self.table_name, keys))
        self.execute(self.sql('add_primary_key').format(self.schema, self.table_name, ', '.join(keys)), False)

        # 同步数据库表信息，否则更新无效
        self.execute(self.sql('reorg_table').format(self.schema, self.table_name), False)

    @db_step('删除DB2表主键')
    def delete_primary_key(self):
        print('表名: {0}, 删除主键'.format(self.table_name))
        self.execute(self.sql('delete_primary_key').format(self.schema, self.table_name), False)

        # 同步数据库表信息，否则更新无效
        self.execute(self.sql('reorg_table').format(self.schema, self.table_name), False)

    @db_step('插入DB2表数据')
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
                    placeholders.append(DB2Data.DEFAULT_PLACEHOLDER)
                # 如果字段允许为空，则有概率插入None
                elif not column.not_null and random_int(0, 9) == 0:
                    values.append(None)
                    placeholders.append(DB2Data.DEFAULT_PLACEHOLDER)
                # 基于数据类型随机生成符合类型、精度、标度的数据
                else:
                    random_value, placeholder = getattr(DB2Data, column.data_type + '_data')(
                        column.precision, column.scale, column.unsigned, column_type=column.column_type)
                    if type(random_value) is list:
                        values.extend(random_value)
                    else:
                        values.append(random_value)
                    placeholders.append(placeholder)
            self.execute(
                self.sql('insert_data').format(schema=self.schema, table_name=self.table_name,
                                               column_name=', '.join(column_names),
                                               column_data=', '.join(placeholders)),
                False, sql_params=tuple(values))

    @db_step('手动插入DB2表数据')
    def manual_insert_data(self, column_names: list, placeholders: list, values: list):
        print('表名: {0}, 手动插入数据'.format(self.table_name))
        assert len(column_names) == len(placeholders), '列数量与预替换符数量不匹配'
        self.execute(self.sql('insert_data').format(schema=self.schema, table_name=self.table_name,
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
                                         format(schema=self.schema, table_name=self.table_name,
                                                column_name=column_name,
                                                count=count_or_condition))
            assert len(columns_value) == count_or_condition, \
                '当前表不存在：{0}行数据，可进行操作'.format(count_or_condition)
            for column_value in columns_value:
                conditions.append("where {0} = '{1}'".format(column_name, column_value[0]))
        # 其他需用户指定查找条件
        else:
            columns_value = self.execute(self.sql('get_condition_customize').
                                         format(schema=self.schema, table_name=self.table_name,
                                                condition=count_or_condition))
            assert len(columns_value) > 0, \
                '当前表不存在满足条件的数据可进行操作，条件：{0}'.format(count_or_condition)
            conditions.append(count_or_condition)
        return conditions

    @db_step('更新DB2表数据')
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
                # 字段为SORTED_AND_UNIQUE_COLUMN_NAME，自增字段，或主键字段，将不会修改改数据
                if column.name == SORTED_AND_UNIQUE_COLUMN_NAME or \
                        column.auto_inc or \
                        column.name in [pk.name for pk in table_info.primary_keys]:
                    continue
                # 如果字段允许为空，则有概率插入None
                if not column.not_null and random_int(0, 9) == 0:
                    values.append(None)
                    placeholders.append("{0}={1}".format(column.name, DB2Data.DEFAULT_PLACEHOLDER))
                    continue
                # 基于数据类型随机生成符合类型、精度、标度的数据
                random_value, placeholder = getattr(DB2Data, column.data_type + '_data')(
                    column.precision, column.scale, column.unsigned, column_type=column.column_type)
                if type(random_value) is list:
                    values.extend(random_value)
                else:
                    values.append(random_value)
                placeholders.append("{0}={1}".format(column.name, placeholder))
            self.execute(
                self.sql('update_data').format(schema=self.schema, table_name=self.table_name,
                                               table_columns=', '.join(placeholders),
                                               condition=condition),
                False, sql_params=tuple(values))

    @db_step('手动更新DB2表数据')
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
            self.execute(self.sql('update_data').format(schema=self.schema, table_name=self.table_name,
                                                        table_columns=', '.join(table_columns),
                                                        condition=condition),
                         False, sql_params=tuple(values))

    @db_step('删除DB2表数据')
    def delete_data(self, count_or_condition):
        print('表名: {0}, 删除数据'.format(self.table_name))
        # 获取DBTable对象
        table_info = self.get_table_info()

        # 获得满足查找条件的数据查找对象
        conditions = self._generate_matching_condition(table_info, count_or_condition)

        # 删除多行数据，可根据self.manual_commit设置事务提交模式
        for condition in conditions:
            self.execute(self.sql('delete_data').format(schema=self.schema, table_name=self.table_name,
                                                        condition=condition), False)

    @db_step('判断DB2表是否存在')
    def is_table_exist(self):
        print('判断表名: {0}, 是否存在'.format(self.table_name))
        ret = self.execute(self.sql('table_exist').format(self.table_name, self.schema))
        return len(ret) == 1

    @db_step('判断DB2列是否存在')
    def is_column_exist(self, column_info: DBColumn):
        print('判断表名: {0}, 列名：{1}，是否存在'.format(self.table_name, column_info.name))
        return self.get_table_info().is_column_exist(column_info)
