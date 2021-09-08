#!/usr/bin/env python
# encoding: utf-8
import pymysql
from core.logics.db.db_driver import *


class MySqlData:
    """
    随机生成测试数据模块
    """
    DEFAULT_PLACEHOLDER = '%s'

    @staticmethod
    def tinyint_data(precision, scale, unsigned, **kwargs):
        if unsigned:
            return random_int(0, 255), MySqlData.DEFAULT_PLACEHOLDER
        else:
            return random_int(-128, 127), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def smallint_data(precision, scale, unsigned, **kwargs):
        if unsigned:
            return random_int(0, 65535), MySqlData.DEFAULT_PLACEHOLDER
        else:
            return random_int(-32768, 32767), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def mediumint_data(precision, scale, unsigned, **kwargs):
        if unsigned:
            return random_int(0, 16777215), MySqlData.DEFAULT_PLACEHOLDER
        else:
            return random_int(-8388608, 8388607), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def int_data(precision, scale, unsigned, **kwargs):
        if unsigned:
            return random_int(0, 4294967295), MySqlData.DEFAULT_PLACEHOLDER
        else:
            return random_int(-2147483648, 2147483647), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def bigint_data(precision, scale, unsigned, **kwargs):
        if unsigned:
            return random_int(0, 18446744073709551615), MySqlData.DEFAULT_PLACEHOLDER
        else:
            return random_int(-9223372036854775808, 9223372036854775807), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def float_data(precision, scale, unsigned, **kwargs):
        if unsigned and scale is None:
            return random_decimal(precision, 2, 0, 1000, unsigned), MySqlData.DEFAULT_PLACEHOLDER
        elif not unsigned and scale is None:
            return random_decimal(precision, 2, -1000, 1000, unsigned), MySqlData.DEFAULT_PLACEHOLDER
        elif unsigned and scale is not None:
            int_part = int(precision) - int(scale)
            if int_part != 0:
                maximum = int('9' * int_part)
            else:
                maximum = 1
            return random_decimal(precision, scale, 0, maximum, unsigned), MySqlData.DEFAULT_PLACEHOLDER
        else:
            int_part = int(precision) - int(scale)
            if int_part != 0:
                maximum = int('9' * int_part)
            else:
                maximum = 1
            return random_decimal(precision, scale, -maximum, maximum, unsigned), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def double_data(precision, scale, unsigned, **kwargs):
        if unsigned and scale is None:
            return random_decimal(precision, 2, 0, 1000, unsigned), MySqlData.DEFAULT_PLACEHOLDER
        elif not unsigned and scale is None:
            return random_decimal(precision, 2, -1000, 1000, unsigned), MySqlData.DEFAULT_PLACEHOLDER
        elif unsigned and scale is not None:
            int_part = int(precision) - int(scale)
            if int_part != 0:
                maximum = int('9' * int_part)
            else:
                maximum = 1
            return random_decimal(precision, scale, 0, maximum, unsigned), MySqlData.DEFAULT_PLACEHOLDER
        else:
            int_part = int(precision) - int(scale)
            if int_part != 0:
                maximum = int('9' * int_part)
            else:
                maximum = 1
            return random_decimal(precision, scale, -maximum, maximum, unsigned), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def decimal_data(precision, scale, unsigned, **kwargs):
        if unsigned and scale is None:
            return random_decimal(precision, 2, 0, 1000, unsigned), MySqlData.DEFAULT_PLACEHOLDER
        elif not unsigned and scale is None:
            return random_decimal(precision, 2, -1000, 1000, unsigned), MySqlData.DEFAULT_PLACEHOLDER
        elif unsigned and scale is not None:
            int_part = int(precision) - int(scale)
            if int_part != 0:
                maximum = int('9' * int_part)
            else:
                maximum = 1
            return random_decimal(precision, scale, 0, maximum, unsigned), MySqlData.DEFAULT_PLACEHOLDER
        else:
            int_part = int(precision) - int(scale)
            if int_part != 0:
                maximum = int('9' * int_part)
            else:
                maximum = 1
            return random_decimal(precision, scale, -maximum, maximum, unsigned), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def char_data(precision, scale, unsigned, **kwargs):
        return random_str(precision), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def varchar_data(precision, scale, unsigned, **kwargs):
        return random_str(precision), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def tinyblob_data(precision, scale, unsigned, **kwargs):
        return pymysql.Binary(random_bytes(precision)), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def blob_data(precision, scale, unsigned, **kwargs):
        return pymysql.Binary(random_bytes(100)), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def mediumblob_data(precision, scale, unsigned, **kwargs):
        return pymysql.Binary(random_bytes(100)), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def longblob_data(precision, scale, unsigned, **kwargs):
        return pymysql.Binary(random_bytes(100)), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def tinytext_data(precision, scale, unsigned, **kwargs):
        return random_str(100), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def text_data(precision, scale, unsigned, **kwargs):
        return random_str(100), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def mediumtext_data(precision, scale, unsigned, **kwargs):
        return random_str(100), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def longtext_data(precision, scale, unsigned, **kwargs):
        return random_str(100), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def date_data(precision, scale, unsigned, **kwargs):
        return random_date(), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def time_data(precision, scale, unsigned, **kwargs):
        return random_time(), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def year_data(precision, scale, unsigned, **kwargs):
        return random_year(), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def datetime_data(precision, scale, unsigned, **kwargs):
        return random_datetime(), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def timestamp_data(precision, scale, unsigned, **kwargs):
        return random_timestamp(), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def bit_data(precision, scale, unsigned, **kwargs):
        return random_bit(precision), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def binary_data(precision, scale, unsigned, **kwargs):
        if precision <= 100:
            return pymysql.Binary(random_bytes(precision)), MySqlData.DEFAULT_PLACEHOLDER
        else:
            return pymysql.Binary(random_bytes(100)), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def varbinary_data(precision, scale, unsigned, **kwargs):
        if precision <= 100:
            return pymysql.Binary(random_bytes(precision)), MySqlData.DEFAULT_PLACEHOLDER
        else:
            return pymysql.Binary(random_bytes(100)), MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def point_data(precision, scale, unsigned, **kwargs):
        # return random_point(), "ST_GeomFromText('POINT(%s %s)')"
        return None, MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def multipoint_data(precision, scale, unsigned, **kwargs):
        # return random_multipoint(), "ST_GeomFromText('MULTIPOINT((%s %s), (%s %s))')"
        return None, MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def linestring_data(precision, scale, unsigned, **kwargs):
        # return random_linestring(), "ST_GeomFromText('LINESTRING(%s %s, %s %s)')"
        return None, MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def multilinestring_data(precision, scale, unsigned, **kwargs):
        # return random_multilinestring(), "ST_GeomFromText('MULTILINESTRING((%s %s, %s %s), (%s %s, %s %s))')"
        return None, MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def polygon_data(precision, scale, unsigned, **kwargs):
        # return random_polygon(), "ST_GeomFromText('POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))')"
        return None, MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def multipolygon_data(precision, scale, unsigned, **kwargs):
        # return random_multipolygon(), "ST_GeomFromText('MULTIPOLYGON(((%s %s, %s %s, %s %s, %s %s, %s %s)),((%s %s, %s %s, %s %s, %s %s, %s %s)))')"
        return None, MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def geometry_data(precision, scale, unsigned, **kwargs):
        # return MySqlData.point_data(precision, scale, unsigned)
        return None, MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def geometrycollection_data(precision, scale, unsigned, **kwargs):
        # return MySqlData.multipoint_data(precision, scale, unsigned)
        return None, MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def enum_data(precision, scale, unsigned, **kwargs):
        if precision == 0:
            return '', MySqlData.DEFAULT_PLACEHOLDER
        else:
            search_result = re.search(r'enum\((.*)\)', kwargs['column_type'])
            if search_result:
                sp = search_result.group(1).split(',')
                return sp[random_int(0, len(sp) - 1)][1:-1], MySqlData.DEFAULT_PLACEHOLDER
            else:
                return '', MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def set_data(precision, scale, unsigned, **kwargs):
        if precision == 0:
            return '', MySqlData.DEFAULT_PLACEHOLDER
        else:
            search_result = re.search(r'set\((.*)\)', kwargs['column_type'])
            if search_result:
                sp = search_result.group(1).split(',')
                return sp[random_int(0, len(sp) - 1)][1:-1], MySqlData.DEFAULT_PLACEHOLDER
            else:
                return '', MySqlData.DEFAULT_PLACEHOLDER

    @staticmethod
    def json_data(precision, scale, unsigned, **kwargs):
        return random_json(), MySqlData.DEFAULT_PLACEHOLDER


class MySqlDB(DBDriver):
    ALIAS = 'MYSQL'

    # 数据库内置关键字，用于检索并设置默认值
    RESERVED_KEYWORD = ['CURRENT_TIMESTAMP']

    query = {
        'get_table_schema':
            '''
select 
COLUMN_NAME,COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE,COLUMN_TYPE,
CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,
COLUMN_KEY, EXTRA 
from information_schema.columns 
where TABLE_NAME = '{0}' order by ORDINAL_POSITION;''',
        'get_column_type':
            "select COLUMN_TYPE from information_schema.columns where TABLE_NAME = '{0}' and COLUMN_NAME = '{1}'",
        'get_all_data':
            "select * from {0}",
        'delete_table':
            "drop table {0}",
        'create_table': '''
create table {table_name}
(
{columns}
) charset = {encoding}''',
        'primary_key':
            "constraint {table_name}_pk primary key ({column_names})",
        'rename_table':
            "rename table {0} to {1}",
        'rename_column':
            "alter table {0} change column {1} {2} {3}",
        'delete_column':
            "alter table {0} drop column {1}",
        'create_column':
            "{name} {column_type}{default}{not_null}{auto_inc}",
        'add_column':
            "alter table {table_name} add column {name} {column_type}{default}{not_null}",
        'update_column':
            "alter table {table_name} modify {name} {column_type}{default}{not_null}",
        'add_primary_key':
            "alter table {0} add constraint {0}_pk primary key {0} ({1});",
        'delete_primary_key':
            "alter table {0} drop primary key",
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
            "show tables like '{0}'"
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

    def __del__(self):
        try:
            self.db.close()
        except:
            pass

    def __connect(self):
        print('连接MySql数据库 host: {0}, port: {1}, user: {2}, passwd: {3}, db: {4}, charset: {5}'.format(
            self.host, self.port, self.user, self.password, self.database, self.charset))
        db = pymysql.connect(host=self.host, port=self.port, user=self.user,
                             passwd=self.password, db=self.database, charset=self.charset)
        db.ping()
        return db

    def _format_default_value(self, default):
        if default is None:
            default = ''
        else:
            default_value = "'{0}'".format(default) if default not in self.RESERVED_KEYWORD else default
            default = " default {0}".format(default_value)
        return default

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

    @db_step('获取MySQL表结构')
    def get_table_info(self):
        print('表名: {0}'.format(self.table_name))
        types = self.execute(self.sql('get_table_schema').format(self.table_name))
        columns = []
        pks = []
        for each in types:
            name = each[0].lower()
            default = each[1]
            not_null = each[2] == 'NO'
            data_type = each[3]
            column_type = each[4]
            pk = each[10] == 'PRI'
            auto_inc = each[11] == 'auto_increment'
            unsigned = column_type.find('unsigned') != -1
            # 基于数据类型，精度、标度获取位置不同
            if each[5] is not None:
                pre = each[5]
                sca = None
            elif each[7] is not None:
                pre = each[7]
                sca = each[8]
            elif each[9] is not None:
                pre = each[9]
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

    @db_step('获取MySQL表所有数据')
    def get_table_data(self):
        print('表名: {0}'.format(self.table_name))
        results = self.execute(self.sql('get_all_data').format(self.table_name))
        return results

    @db_step('创建MySQL表')
    def create_table(self, table_info: DBTable):
        print('表名: {0}'.format(self.table_name))
        # 检查DBTable对象是否为当前数据库
        if table_info.db.upper() != self.ALIAS:
            raise DBNotMatchingException('当前DB实例为：{0}, 与所要操作的数据库：{1}, 不匹配'.format(
                self.ALIAS, table_info.db))

        # 拼接DDL
        columns = []
        for column in table_info.columns:
            name = column.name
            column_type = column.column_type
            default = self._format_default_value(column.default)
            not_null = ' not null' if column.not_null else ' null'
            if column.auto_inc:
                auto_inc = ' auto_increment'
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
        table_basic = self.sql('create_table').format(table_name=self.table_name, columns=',\n'.join(columns),
                                                      encoding=self.charset)
        self.execute(table_basic, False)

    @db_step('重命名MySQL表')
    def rename_table(self, new_table_name: str):
        print('表名: {0}, 重命名为：{1}'.format(self.table_name, new_table_name))
        self.execute(self.sql('rename_table').format(self.table_name, new_table_name), False)
        self.table_name = new_table_name

    @db_step('删除MySQL表')
    def delete_table(self, raise_error=True):
        print('表名: {0}'.format(self.table_name))
        handle_exception = not raise_error
        self.execute(self.sql('delete_table').format(self.table_name), False,
                     handle_exception=handle_exception)

    @db_step('增加MySQL表字段')
    def add_column(self, column_info: DBColumn):
        print('表名: {0}, 新增字段：{1}'.format(self.table_name, column_info.name))
        assert column_info.name, '新增字段需提供字段名称'
        assert column_info.column_type, '新增字段需提供字段类型'
        default = self._format_default_value(column_info.default)
        not_null = ' not null' if column_info.not_null else ' null'
        self.execute(self.sql('add_column').format(table_name=self.table_name, name=column_info.name,
                                                   column_type=column_info.column_type, not_null=not_null,
                                                   default=default), False)

    @db_step('更新MySQL表字段')
    def update_column(self, column_info: DBColumn):
        print('表名: {0}, 修改字段：{1}'.format(self.table_name, column_info.name))
        assert column_info.name, '新增字段需提供字段名称'
        assert column_info.column_type, '新增字段需提供字段类型'
        default = self._format_default_value(column_info.default)
        not_null = ' not null' if column_info.not_null else ' null'
        self.execute(self.sql('update_column').format(table_name=self.table_name, name=column_info.name,
                                                      column_type=column_info.column_type, not_null=not_null,
                                                      default=default), False)

    @db_step('删除MySQL表字段')
    def delete_column(self, column_name: str):
        print('表名: {0}, 字段名：{1}'.format(self.table_name, column_name))
        self.execute(self.sql('delete_column').format(self.table_name, column_name), False)

    @db_step('重命名MySQL表字段')
    def rename_column(self, old_column_name: str, new_column_name: str):
        print('表名: {0}, 字段名：{1}, 重命名为：{2}'.format(self.table_name, old_column_name, new_column_name))
        column_type = self.execute(self.sql('get_column_type').format(self.table_name, old_column_name))
        assert len(column_type) == 1, '无法正确获取表名: {0}, 字段名：{1}的字段信息'.format(self.table_name, old_column_name)
        self.execute(self.sql('rename_column').
                     format(self.table_name, old_column_name, new_column_name, column_type[0][0]), False)

    @db_step('增加MySQL表主键')
    def add_primary_key(self, keys: list):
        print('表名: {0}, 增加主键：{1}'.format(self.table_name, keys))
        self.execute(self.sql('add_primary_key').format(self.table_name, ', '.join(keys)), False)

    @db_step('删除MySQL表主键')
    def delete_primary_key(self):
        print('表名: {0}, 删除主键'.format(self.table_name))
        self.execute(self.sql('delete_primary_key').format(self.table_name), False)

    @db_step('插入MySQL表数据')
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
                    placeholders.append(MySqlData.DEFAULT_PLACEHOLDER)
                # 如果字段允许为空，则有概率插入None
                elif not column.not_null and random_int(0, 9) == 0:
                    values.append(None)
                    placeholders.append(MySqlData.DEFAULT_PLACEHOLDER)
                # 基于数据类型随机生成符合类型、精度、标度的数据
                else:
                    random_value, placeholder = getattr(MySqlData, column.data_type + '_data')(
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
                False, sql_params=values)

    @db_step('手动插入MySQL表数据')
    def manual_insert_data(self, column_names: list, placeholders: list, values: list):
        print('表名: {0}, 手动插入数据'.format(self.table_name))
        assert len(column_names) == len(placeholders), '列数量与预替换符数量不匹配'
        self.execute(self.sql('insert_data').format(table_name=self.table_name,
                                                    column_name=', '.join(column_names),
                                                    column_data=', '.join(placeholders)),
                     False, sql_params=values)

    def _generate_matching_condition(self, table_info: DBTable, count_or_condition):
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

    @db_step('更新MySQL表数据')
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
                    placeholders.append("{0}={1}".format(column.name, MySqlData.DEFAULT_PLACEHOLDER))
                    continue
                # 基于数据类型随机生成符合类型、精度、标度的数据
                random_value, placeholder = getattr(MySqlData, column.data_type + '_data')(
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
                False, sql_params=values)

    @db_step('手动更新MySQL表数据')
    def manual_update_data(self, count_or_condition, column_names: list, placeholders: list, values: list):
        print('表名: {0}, 手动更新数据'.format(self.table_name))
        assert len(column_names) == len(placeholders), '列数量与预替换符数量不匹配'
        table_columns = ['{0}={1}'.format(name, placeholders[index]) for index, name in enumerate(column_names)]
        # 获取DBTable对象
        table_info = self.get_table_info()

        # 获得满足查找条件的数据查找对象
        conditions = self._generate_matching_condition(table_info, count_or_condition)
        for condition in conditions:
            self.execute(self.sql('update_data').format(table_name=self.table_name,
                                                        table_columns=', '.join(table_columns),
                                                        condition=condition), False, sql_params=values)

    @db_step('删除MySQL表数据')
    def delete_data(self, count_or_condition):
        print('表名: {0}, 删除数据'.format(self.table_name))
        # 获取DBTable对象
        table_info = self.get_table_info()

        # 获得满足查找条件的数据查找对象
        conditions = self._generate_matching_condition(table_info, count_or_condition)

        # 删除多行数据，可根据self.manual_commit设置事务提交模式
        for condition in conditions:
            self.execute(self.sql('delete_data').format(table_name=self.table_name, condition=condition), False)

    @db_step('判断MySQL表是否存在')
    def is_table_exist(self):
        print('判断表名: {0}, 是否存在'.format(self.table_name))
        ret = self.execute(self.sql('table_exist').format(self.table_name))
        return len(ret) == 1

    @db_step('判断MySQL列是否存在')
    def is_column_exist(self, column_info: DBColumn):
        print('判断表名: {0}, 列名：{1}，是否存在'.format(self.table_name, column_info.name))
        return self.get_table_info().is_column_exist(column_info)
