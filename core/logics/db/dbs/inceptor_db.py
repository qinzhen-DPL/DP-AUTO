#!/usr/bin/env python
# encoding: utf-8
import collections
import avro.schema
from enum import Enum
from pyhive import hive
from core.logics.db.db_driver import *


class InceptorData:
    """
    随机生成测试数据模块
    """
    DEFAULT_PLACEHOLDER = '%s'

    @staticmethod
    def bigint_data(precision, scale, unsigned, **kwargs):
        if unsigned:
            return random_int(0, 18446744073709551615), InceptorData.DEFAULT_PLACEHOLDER
        else:
            return random_int(-9223372036854775808, 9223372036854775807), InceptorData.DEFAULT_PLACEHOLDER

    @staticmethod
    def int_data(precision, scale, unsigned, **kwargs):
        if unsigned:
            return random_int(0, 4294967295), InceptorData.DEFAULT_PLACEHOLDER
        else:
            return random_int(-2147483648, 2147483647), InceptorData.DEFAULT_PLACEHOLDER

    @staticmethod
    def decimal_data(precision, scale, unsigned, **kwargs):
        if unsigned and scale is None:
            return random_decimal(precision, 2, 0, 1000, unsigned), InceptorData.DEFAULT_PLACEHOLDER
        elif not unsigned and scale is None:
            return random_decimal(precision, 2, -1000, 1000, unsigned), InceptorData.DEFAULT_PLACEHOLDER
        elif unsigned and scale is not None:
            int_part = int(precision) - int(scale)
            if int_part != 0:
                maximum = int('9' * int_part)
            else:
                maximum = 1
            return random_decimal(precision, scale, 0, maximum, unsigned), InceptorData.DEFAULT_PLACEHOLDER
        else:
            int_part = int(precision) - int(scale)
            if int_part != 0:
                maximum = int('9' * int_part)
            else:
                maximum = 1
            return random_decimal(precision, scale, -maximum, maximum, unsigned), InceptorData.DEFAULT_PLACEHOLDER

    @staticmethod
    def string_data(precision, scale, unsigned, **kwargs):
        return random_str_without_punctuation(20), "{0}".format(InceptorData.DEFAULT_PLACEHOLDER)


class InceptorFileType(Enum):
    csv = 0
    avro = 1
    parquet = 2
    orc = 3


class InceptorDB(DBDriver):
    ALIAS = 'INCEPTOR'

    # Avro schema字段信息
    AVRO_SCHEMA_NAME = 'parquetRecord'
    AVRO_SCHEMA_TYPE = 'record'

    query = {
        'get_table_schema':
            "desc {table_name}",
        'get_all_data':
            "select * from {0}",
        'delete_table':
            "drop table {0}",
        'create_table': '''
create table {table_name}
(
{columns}
)
{settings}
stored as {file_type}
location '{location}'
{tblproperties}
    ''',
        'create_external_table': '''
create external table {table_name}
(
{columns}
)
{settings}
stored as {file_type}
location '{location}'
{tblproperties}
        ''',
        'set-buckets':
            'clustered by({column_name}) into {buckets_num} buckets',
        'set-tblproperties':
            "TBLPROPERTIES({0})",
        'create_column':
            "{name} {column_type}",
        'primary_key':
            "primary key ({column_names}) DISABLE NOVALIDATE",
        'rename_table':
            "alter table {0} rename to {1}",
        'add_column':
            "alter table {0} add columns ({1} {2})",
        'update_column':
            "alter table {0} change {1} {1} {2}",
        'rename_column':
            "alter table {0} change {1} {2} {3}",
        'delete_column':
            "alter table {0} replace columns ({1})",
        'csv_format':
            '''row format serde 'org.apache.hadoop.hive.ql.io.csv.serde.CSVSerde'
with serdeproperties
(
    'field.delim' = '{0}',
    'quote.delim'     = '{1}',
    'escape.delim'    = '{2}',
    'line.delim'    = '{3}',
    'serialization.line.encoding' = '{4}'
)''',
        'avro_format': "row format serde 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'",
        'insert_data':
            "insert into table {table_name} ({column_name}) values {column_data}",
        'load_data':
            "load data inpath '{0}' {1} into table {2}",
        'insert_data_from_other_table':
            "insert {overwrite} table {table_name} select * from {other_table_name}",
        'update_data':
            "update {table_name} set {table_columns} {condition}",
        'delete_data':
            "delete from {table_name} {condition}",
        'get_condition':
            "select {column_name} from {table_name} order by {column_name} desc limit {count}",
        'get_condition_customize':
            "select * from {table_name} {condition}",
        'switch_to_external':
            "alter table {table_name} set tblproperties('EXTERNAL'='{external}')",
        'table_exist':
            """select * from TBLS where DB_ID =
(select DB_ID from DBS where NAME = '{0}') and TBL_NAME = '{1}'"""
    }

    def __init__(self, db_config: dict, table_name, file_type: InceptorFileType):
        self.host = db_config["ip"]
        self.port = db_config["port"]
        self.user = db_config["username"]
        self.password = db_config["password"]
        self.database = db_config["database"]
        self.root = db_config["root"]
        self.table_name = table_name
        self.file_type = file_type

        # csv 类型的默认文件分隔符
        self.separator_char = config.DEFAULT_SEPARATOR
        self.quote_char = config.DEFAULT_QUOTA
        self.escape_char = config.DEFAULT_ESCAPE
        self.line_char = config.DEFAULT_LINE
        self.encoding = 'utf8'

        # 连接服务器
        self.db = self.__connect()

        # 是否手动提交变化，不开启则为自动提交，开启后可进行事务模式测试
        self.manual_commit = False

        # 外表的数据文件默认路径，可以在创建表之前手工修改指定到已知路径
        self.hdfs_external_file_location = None

        # 设置分桶信息
        self.buckets = None
        self.clustered = None

    def __connect(self):
        print('连接Inceptor host: {0}, port: {1}, username: {2}, password: {3}, database: {4}'.format(
            self.host, self.port, self.user, self.password, self.database))
        db = hive.Connection(host=self.host, port=self.port, username=self.user,
                             password=self.password, auth='LDAP', database=self.database)
        return db

    def __del__(self):
        try:
            self.db.close()
        except:
            pass

    def _get_avro_schema(self, table_info):
        # 通过table_info转换为avro schema
        schema_json = collections.OrderedDict()
        schema_json['name'] = self.AVRO_SCHEMA_NAME
        schema_json['type'] = self.AVRO_SCHEMA_TYPE
        schema_json['fields'] = []
        for column in table_info.columns:
            field = {'name': column.name, 'type': []}
            field['type'].append(column.column_type)
            if not column.not_null:
                field['type'].append('null')
            if column.default is not None:
                field['default'] = column.default
            schema_json['fields'].append(field)
        schema = avro.schema.parse(json.dumps(schema_json))
        return schema

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

    @db_step('获取Inceptor表结构')
    def get_table_info(self):
        print('表名: {0}'.format(self.table_name))
        types = self.execute(self.sql('get_table_schema').format(table_name=self.table_name))
        columns = []
        pks = []
        for each in types:
            name = each[0].lower()
            default = None
            not_null = False
            column_type = each[1]
            pk = None
            auto_inc = False
            unsigned = None
            # 基于数据类型，精度、标度获取位置不同
            search_result = re.search(r'decimal\((.*),(.*)\)', column_type)
            if search_result:
                pre = search_result.group(1)
                sca = search_result.group(2)
                data_type = 'decimal'
            else:
                data_type = column_type
                pre = None
                sca = None
            column = DBColumn(name, data_type, column_type, default, not_null, auto_inc, pre, sca, unsigned)
            columns.append(column)
            if pk:
                pks.append(column)
        table_info = DBTable(self.ALIAS, self.table_name, columns, pks)
        return table_info

    @db_step('获取Inceptor表所有数据')
    def get_table_data(self):
        print('表名: {0}'.format(self.table_name))
        results = self.execute(self.sql('get_all_data').format(self.table_name))
        return results

    @db_step('删除Inceptor表')
    def delete_table(self, raise_error=True):
        print('表名: {0}'.format(self.table_name))
        handle_exception = not raise_error
        self.execute(self.sql('delete_table').format(self.table_name), False,
                     handle_exception=handle_exception)

    @db_step('创建Inceptor表')
    def create_table(self, table_info: DBTable, external_table=True):
        print('表名: {0}'.format(self.table_name))
        # 检查DBTable对象是否为当前数据库
        if table_info.db.upper() != self.ALIAS:
            raise DBNotMatchingException('当前DB实例为：{0}, 与所要操作的数据库：{1}, 不匹配'.format(
                self.ALIAS, table_info.db))

        # 拼接DDL
        columns = []
        settings = []
        for column in table_info.columns:
            name = column.name
            column_type = column.column_type
            columns.append('\t' + self.sql('create_column').format(name=name, column_type=column_type))

        # 设置分桶
        if (self.buckets is not None) and (self.clustered is not None):
            settings.append(self.sql('set-buckets').format(column_name=self.clustered, buckets_num=self.buckets))

        if (self.buckets is not None) and \
                (self.clustered is not None) and \
                (self.file_type == InceptorFileType.orc) and \
                (self.hdfs_external_file_location is None):
            tblproperties = self.sql('set-tblproperties').format("'transactional'='true'")
        else:
            tblproperties = ''

        # 设置文件存储类型
        if self.file_type == InceptorFileType.csv:
            file_type = "CSVFILE"
        elif self.file_type == InceptorFileType.avro:
            file_type = "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'\n OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'"
            avro_schema = self._get_avro_schema(table_info)
            tblproperties = self.sql('set-tblproperties').format("'avro.schema.literal'='{0}'".format(avro_schema))
        else:
            file_type = self.file_type.name.upper()

        # 分隔符设置
        if self.file_type == InceptorFileType.csv:
            settings.append(self.sql('csv_format').format(self.separator_char, self.quote_char,
                                                          self.escape_char, self.line_char, self.encoding))
        elif self.file_type == InceptorFileType.avro:
            settings.append(self.sql('avro_format'))

        # 拼接DDL
        if self.hdfs_external_file_location is not None:
            table_basic = self.sql('create_external_table').format(table_name=self.table_name,
                                                                   columns=',\n'.join(columns),
                                                                   file_type=file_type,
                                                                   settings='\n'.join(settings),
                                                                   location=self.hdfs_external_file_location,
                                                                   tblproperties=tblproperties)
        else:
            table_basic = self.sql('create_table').format(table_name=self.table_name, columns=',\n'.join(columns),
                                                          file_type=file_type, settings='\n'.join(settings),
                                                          location=self.root + '/' + self.table_name,
                                                          tblproperties=tblproperties)
        self.execute(table_basic, False)

    @db_step('改变Inceptor表为外表')
    def switch_to_external(self, external=True):
        print('表名: {0}'.format(self.table_name))
        self.execute(self.sql('switch_to_external').format(table_name=self.table_name, external=str(external).upper())
                     , False)

    @db_step('重命名Inceptor表')
    def rename_table(self, new_table_name: str):
        print('表名: {0}, 重命名为：{1}'.format(self.table_name, new_table_name))
        self.execute(self.sql('rename_table').format(self.table_name, new_table_name), False)
        self.table_name = new_table_name

    @db_step('增加Inceptor表字段')
    def add_column(self, column_info: DBColumn):
        print('表名: {0}, 新增字段：{1}'.format(self.table_name, column_info.name))
        assert column_info.name, '新增字段需提供字段名称'
        assert column_info.column_type, '新增字段需提供字段类型'
        self.execute(self.sql('add_column').format(self.table_name, column_info.name, column_info.column_type),
                     False)

    @db_step('更新Inceptor表字段')
    def update_column(self, column_info: DBColumn):
        print('表名: {0}, 修改字段：{1}'.format(self.table_name, column_info.name))
        assert column_info.name, '新增字段需提供字段名称'
        assert column_info.column_type, '新增字段需提供字段类型'
        self.execute(self.sql('update_column').format(self.table_name, column_info.name,
                                                      column_info.column_type), False)

    @db_step('删除Inceptor表字段')
    def delete_column(self, column_name: str):
        print('表名: {0}, 字段名：{1}'.format(self.table_name, column_name))
        table_info = self.get_table_info()
        old_column = list(filter(lambda x: x.name == column_name.lower(), table_info.columns))
        assert len(old_column) == 1, '无法正确获取表名: {0}, 字段名：{1}的字段信息'.format(self.table_name, column_name)

        rest_columns = list(filter(lambda x: x.name != column_name.lower(), table_info.columns))
        rest_column_types = []
        for each in rest_columns:
            rest_column_types.append('{0} {1}'.format(each.name, each.column_type))
        self.execute(self.sql('delete_column').format(self.table_name, ', '.join(rest_column_types)), False)

    @db_step('重命名Inceptor表字段')
    def rename_column(self, old_column_name: str, new_column_name: str):
        print('表名: {0}, 字段名：{1}, 重命名为：{2}'.format(self.table_name, old_column_name, new_column_name))
        table_info = self.get_table_info()
        old_column = list(filter(lambda x: x.name == old_column_name.lower(), table_info.columns))
        assert len(old_column) == 1, '无法正确获取表名: {0}, 字段名：{1}的字段信息'.format(self.table_name, old_column_name)
        self.execute(self.sql('rename_column').
                     format(self.table_name, old_column_name, new_column_name, old_column[0].column_type), False)

    @db_step('增加Inceptor表主键')
    def add_primary_key(self, keys: list):
        raise NotImplementedError

    @db_step('删除Inceptor表主键')
    def delete_primary_key(self):
        raise NotImplementedError

    @db_step('插入Inceptor表数据')
    def insert_data(self, count: int):
        print('表名: {0}, 插入数据'.format(self.table_name))
        # 获取DBTable对象
        table_info = self.get_table_info()

        # 插入多行数据，可根据self.manual_commit设置事务提交模式
        values = []
        placeholders = []
        column_names = []
        for column in table_info.columns:
            column_names.append(column.name)
        for _ in range(count):
            line_placeholders = []
            line_values = []
            for column in table_info.columns:
                # 如果字段名为SORTED_AND_UNIQUE_COLUMN_NAME，将会随机生成唯一值
                if column.name == SORTED_AND_UNIQUE_COLUMN_NAME:
                    line_values.append(random_sorted_and_unique_num())
                    line_placeholders.append(InceptorData.DEFAULT_PLACEHOLDER)
                # 基于数据类型随机生成符合类型、精度、标度的数据
                else:
                    random_value, placeholder = getattr(InceptorData, column.data_type + '_data')(
                        column.precision, column.scale, column.unsigned, column_type=column.column_type)
                    if type(random_value) is list:
                        line_values.extend(random_value)
                    else:
                        line_values.append(random_value)
                        line_placeholders.append(placeholder)
            placeholders.append('({0})'.format(', '.join(line_placeholders)))
            values.extend(line_values)
            print('DATA[{0}]'.format(_))
            for data in line_values:
                print('\t' + str(data))
        self.execute(
            self.sql('insert_data').format(table_name=self.table_name, column_name=', '.join(column_names),
                                           column_data=', '.join(placeholders)),
            False, sql_params=values)

    @db_step('手动插入Inceptor表数据')
    def manual_insert_data(self, column_names: list, placeholders: list, values: list):
        print('表名: {0}, 手动插入数据'.format(self.table_name))
        assert len(column_names) == len(placeholders), '列数量与预替换符数量不匹配'
        self.execute(self.sql('insert_data').format(table_name=self.table_name,
                                                    column_name=', '.join(column_names),
                                                    column_data="({0})".format(', '.join(placeholders))),
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

    @db_step('更新Inceptor表数据')
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
                    placeholders.append("{0}={1}".format(column.name, InceptorData.DEFAULT_PLACEHOLDER))
                    continue
                # 基于数据类型随机生成符合类型、精度、标度的数据
                random_value, placeholder = getattr(InceptorData, column.data_type + '_data')(
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

    @db_step('手动更新Inceptor表数据')
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

    @db_step('删除Inceptor表数据')
    def delete_data(self, count_or_condition):
        print('表名: {0}, 删除数据'.format(self.table_name))
        # 获取DBTable对象
        table_info = self.get_table_info()

        # 获得满足查找条件的数据查找对象
        conditions = self._generate_matching_condition(table_info, count_or_condition)

        # 删除多行数据，可根据self.manual_commit设置事务提交模式
        for condition in conditions:
            self.execute(self.sql('delete_data').format(table_name=self.table_name, condition=condition), False)

    @db_step('导入Inceptor表数据')
    def load_data(self, hdfs_location, overwrite=True):
        print('表名: {0}, 导入数据：{1}, 是否覆盖：{2}'.format(self.table_name, hdfs_location, overwrite))
        overwrite_str = 'overwrite' if overwrite else ''
        self.execute(self.sql('load_data').format(hdfs_location, overwrite_str, self.table_name), False)

    @db_step('导入Inceptor表数据')
    def insert_data_from_other_table(self, other_table, overwrite=True):
        print('表名: {0}, 导入已存在表数据：{1}, 是否覆盖：{2}'.format(self.table_name, other_table, overwrite))
        overwrite_str = 'overwrite' if overwrite else 'into'
        self.execute(self.sql('insert_data_from_other_table').format(
            overwrite=overwrite_str, table_name=self.table_name, other_table_name=other_table), False)

    @db_step('判断Inceptor表是否存在')
    def is_table_exist(self):
        print('判断表名: {0}, 是否存在'.format(self.table_name))
        try:
            self.execute(self.sql('get_table_schema').format(table_name=self.table_name))
        except Exception as e:
            if str(e).find('Table not found') != -1:
                return False
            raise e
        return True

    @db_step('判断Hive列是否存在')
    def is_column_exist(self, column_info: DBColumn):
        print('判断表名: {0}, 列名：{1}，是否存在'.format(self.table_name, column_info.name))
        return self.get_table_info().is_column_exist(column_info)
