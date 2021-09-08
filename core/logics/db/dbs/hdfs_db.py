#!/usr/bin/env python
# encoding: utf-8
from decimal import *
import collections
import avro.schema
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
from enum import Enum
from hdfs.client import InsecureClient
from core.logics.db.db_driver import *
from hdfs.ext.avro import AvroReader


class HDFSData:
    """
    随机生成测试数据模块
    """

    @staticmethod
    def int_data(precision, scale, unsigned, **kwargs):
        if unsigned:
            return int(random_int(0, 4294967295))
        else:
            return int(random_int(-2147483648, 2147483647))

    @staticmethod
    def long_data(precision, scale, unsigned, **kwargs):
        if unsigned:
            return int(random_int(0, 18446744073709551615))
        else:
            return int(random_int(-9223372036854775808, 9223372036854775807))

    @staticmethod
    def string_data(precision, scale, unsigned, **kwargs):
        return random_str_without_punctuation(20)

    @staticmethod
    def boolean_data(precision, scale, unsigned, **kwargs):
        if random_int(0, 1) == 0:
            return False
        else:
            return True

    @staticmethod
    def decimal_data(precision, scale, unsigned, **kwargs):
        if precision is None:
            return float(random_decimal(5, 2, -1000, 1000, unsigned))
        else:
            return Decimal(random_decimal(precision, scale, -1000, 1000, unsigned))

    @staticmethod
    def float_data(precision, scale, unsigned, **kwargs):
        return float(random_decimal(8, 2, -1000000, 1000000, unsigned))

    @staticmethod
    def double_data(precision, scale, unsigned, **kwargs):
        return float(random_decimal(8, 4, -9999, 9999, unsigned))

    @staticmethod
    def bytes_data(precision, scale, unsigned, **kwargs):
        return random_bytes(20)


class HDFSFileType(Enum):
    csv = 0
    avro = 1


class HDFSDB(DBDriver):
    ALIAS = 'HDFS'

    # HDFS文件所需的Schema关联文件后缀
    SCHEMA_EXT = '.yml'

    # Avro schema字段信息
    AVRO_SCHEMA_NAME = 'parquetRecord'
    AVRO_SCHEMA_TYPE = 'record'

    def __init__(self, db_config: dict, file_name, file_type: HDFSFileType, encoding='utf8'):
        self.host = db_config["hdfs_web"]
        self.root = db_config["root"]
        self.user = db_config["username"]
        self.table_name = file_name
        self.file_type = file_type
        self.table_name = file_name
        self.encoding = encoding

        # HDFS-csv需要schema文件用于描述对应数据的结构、字段信息
        # 虽然csv并没有强行数据要求限制，但为了拓展下游库数据转换、高级清洗等测试逻辑
        # 需要限制csv中某一字段仅会生成属于一种类型的数据，所以需要额外存放、提供schema信息
        # 作为上游库时且通过框架创建表时将会自动创建该schema文件
        # 可通过该文件获取字段信息，用于生成随机数据
        self.table_schema_name = file_name + self.SCHEMA_EXT
        # 如作为下游库或没有该schema信息文件，需显示将self.table_info赋值为DBTable对象
        self.table_info = None

        # 所有临时文件的生成、下载路径，测试过程后会自动删除该文件
        # 但如出现特殊情况文件未删除，建议将self.download_path配置到TestBase.report_path路径
        self.download_path = ''

        self.db = self.__connect()
        # 默认使用的行、列分隔符，可在初始化后被重写
        self.line_end = config.DEFAULT_LINEEND
        self.separator = config.DEFAULT_SEPARATOR

    def __connect(self):
        print('连接HDFS host: {0}, root: {1}, user: {2}'.format(self.host, self.root, self.user))
        db = InsecureClient(self.host, user=self.user)
        return db

    def __create_file(self, file_path, data):
        with open(file_path, 'w', encoding=self.encoding) as f:
            for line in data:
                f.write(self.separator.join(['"{0}"'.format(str(x) if x is not None else '') for x in line])
                        + self.line_end)

    @db_call
    def __fetch_all_data(self, file_name):
        # 下载文件、读取文件信息、删除文件
        file_path = os.path.join(self.download_path, file_name)
        if os.path.exists(file_path):
            os.remove(file_path)
        self.db.download(self.root + '/' + file_name, file_path)

        # 解析文件
        results = []
        if self.file_type == HDFSFileType.csv:
            # 以分行符分行
            with open(file_path, 'r', encoding=self.encoding) as f:
                result = f.readlines()
            result = ''.join(result).split(self.line_end)
            for line in result:
                if not line.strip():
                    continue
                # 以分隔符分列
                row_list = line.split(self.separator)
                # 去除信息双引号
                results.append([i[1:-1] for i in row_list])
            print('RET: {0}'.format(results))
        else:
            # 读取avro文件并转为OrderedDict，保证数据、字段有序
            with DataFileReader(open(file_path, "rb"), DatumReader()) as reader:
                schema = json.loads(reader.schema)
                for msg in reader:
                    each = collections.OrderedDict()
                    for column in schema['fields']:
                        name = column['name'].lower()
                        each[name] = msg[name]
                    results.append(list(each.values()))
        os.remove(file_path)
        return results

    @db_call
    def __upload_file(self, local_file_path, file_name):
        self.db.upload(self.root + '/' + file_name, local_file_path, overwrite=True)

    @db_call
    def __delete(self, file_name, raise_error=True):
        try:
            self.db.delete(self.root + '/' + file_name)
        except Exception as e:
            print(e)
            if raise_error:
                raise e

    @db_call
    def __upload_data_to_server(self, table_data, table_info):
        # csv将会把数据上传到服务器
        if self.file_type == HDFSFileType.csv:
            for data in table_data:
                print(str(data))
            table_file_path = os.path.join(self.download_path, self.table_name)
            self.__create_file(table_file_path, table_data)
            self.__upload_file(table_file_path, self.table_name)
            os.remove(table_file_path)
        # avro将会把数据与字段信息整合成后上传到服务器
        else:
            schema = self._get_avro_schema(table_info)
            table_file_path = os.path.join(self.download_path, self.table_name)
            with DataFileWriter(open(table_file_path, "wb"), DatumWriter(), schema) as writer:
                for data in table_data:
                    print(str(data))
                    data_dict = collections.OrderedDict()
                    for index, column in enumerate(table_info.columns):
                        data_dict[column.name] = data[index]
                    writer.append(data_dict)
            self.__upload_file(table_file_path, self.table_name)
            os.remove(table_file_path)

    @db_step('获取HDFS文件结构')
    def get_table_info(self):
        print('文件名: {0}'.format(self.table_name))
        # 如果提供了self.table_info，则直接返回
        if self.table_info is not None:
            return self.table_info

        # csv 则通过schema文件获取
        if self.file_type == HDFSFileType.csv:
            with self.db.read(self.root + '/' + self.table_schema_name, encoding=self.encoding) as reader:
                table_info = read_yml_ddl_file(None, reader)
            for column in table_info.columns:
                column.data_type = column.column_type
            return table_info
        # avro 则读取avro头部schema信息
        else:
            with AvroReader(self.db, self.root + '/' + self.table_name) as reader:
                schema = reader.schema
            columns = []
            for column in schema['fields']:
                name = column['name'].lower()
                default = column['default'] if 'default' in column.keys() else None
                not_null = 'null' not in column['type']
                data_type = list(filter(lambda x: x != 'null', column['type']))[0]
                column_type = data_type
                auto_inc = False
                unsigned = None
                if type(data_type) == dict:
                    pre = data_type['precision']
                    sca = data_type['scale']
                    data_type = data_type['logicalType']
                else:
                    pre = None
                    sca = None
                column = DBColumn(name, data_type, column_type, default, not_null, auto_inc, pre, sca, unsigned)
                columns.append(column)
            table_info = DBTable(self.ALIAS, self.table_name, columns, [])
            return table_info

    @db_step('获取HDFS文件所有数据')
    def get_table_data(self):
        print('文件名: {0}'.format(self.table_name))
        results = self.__fetch_all_data(self.table_name)
        return results

    @db_step('删除HDFS文件')
    def delete_table(self, raise_error=True):
        print('文件名: {0}'.format(self.table_name))
        self.__delete(self.table_name, raise_error)
        if self.file_type == HDFSFileType.csv and self.table_info is None:
            self.__delete(self.table_schema_name, raise_error)

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

    @db_step('创建HDFS表')
    def create_table(self, table_info: DBTable):
        print('表名: {0}, Schema名: {1}'.format(self.table_name, self.table_schema_name))
        # 检查DBTable对象是否为当前数据库
        if table_info.db.upper() != self.ALIAS:
            raise DBNotMatchingException('当前DB实例为：{0}, 与所要操作的数据库：{1}, 不匹配'.format(
                self.ALIAS, table_info.db))

        # csv创建数据以及schema文件
        if self.file_type == HDFSFileType.csv:
            table_file_path = os.path.join(self.download_path, self.table_name)
            schema_file_path = os.path.join(self.download_path, self.table_schema_name)
            self.__create_file(table_file_path, [])
            write_yml_ddl_file(table_info, schema_file_path)
            self.__upload_file(table_file_path, self.table_name)
            self.__upload_file(schema_file_path, self.table_schema_name)
            os.remove(table_file_path)
            os.remove(schema_file_path)
        # avro创建avro文件且数据为空
        else:
            self.__upload_data_to_server([], table_info)

    @db_step('重命名HDFS表')
    def rename_table(self, new_table_name: str):
        print('表名: {0}, 重命名为：{1}'.format(self.table_name, new_table_name))
        self.db.rename(self.root + '/' + self.table_name, self.root + '/' + new_table_name)
        if self.file_type == HDFSFileType.csv:
            self.db.rename(self.root + '/' + self.table_schema_name, self.root + '/' + new_table_name + self.SCHEMA_EXT)
        self.table_name = new_table_name
        self.table_schema_name = new_table_name + self.SCHEMA_EXT

    @db_step('增加HDFS表字段')
    def add_column(self, column_info: DBColumn):
        raise NotImplementedError

    @db_step('更新HDFS表字段')
    def update_column(self, column_info: DBColumn):
        raise NotImplementedError

    @db_step('删除HDFS表字段')
    def delete_column(self, column_name: str):
        raise NotImplementedError

    @db_step('重命名HDFS表字段')
    def rename_column(self, old_column_name: str, new_column_name: str):
        raise NotImplementedError

    @db_step('增加HDFS表主键')
    def add_primary_key(self, keys: list):
        raise NotImplementedError

    @db_step('删除HDFS表主键')
    def delete_primary_key(self):
        raise NotImplementedError

    @db_step('插入HDFS表数据')
    def insert_data(self, count: int):
        print('表名: {0}, 插入数据'.format(self.table_name))
        # 获取DBTable对象
        table_info = self.get_table_info()

        # 获取当前所有数据
        table_data = self.get_table_data()

        # 插入多行数据
        for _ in range(count):
            values = []
            for index, column in enumerate(table_info.columns):
                # 自增字段将基于最大数据id
                if self.file_type == HDFSFileType.csv:
                    function_name = column.column_type + '_data'
                    if column.auto_inc:
                        if len(table_data) > 0:
                            last_id = table_data[-1][index]
                        else:
                            last_id = -1
                        values.append(int(last_id) + 1)
                else:
                    function_name = column.data_type + '_data'
                # 如果字段名为SORTED_AND_UNIQUE_COLUMN_NAME，将会随机生成唯一值
                if column.name == SORTED_AND_UNIQUE_COLUMN_NAME:
                    values.append(random_sorted_and_unique_num())
                # 如果字段允许为空，则有概率插入None
                elif not column.not_null and random_int(0, 9) == 0:
                    values.append(None)
                # 基于数据类型随机生成符合类型、精度、标度的数据
                else:
                    random_value = getattr(HDFSData, function_name)(
                        column.precision, column.scale, column.unsigned, column_type=column.column_type)
                    values.append(random_value)
            table_data.append(values)
        self.__upload_data_to_server(table_data, table_info)

    @db_step('手动插入HDFS表数据')
    def manual_insert_data(self, column_names: list, data_values: list):
        print('表名: {0}, 手动插入数据'.format(self.table_name))
        assert len(column_names) == len(data_values), '列数量与列名数量不匹配'
        # 获取DBTable对象
        table_info = self.get_table_info()

        # 获取当前所有数据
        table_data = self.get_table_data()

        values = []
        for index, column in enumerate(table_info.columns):
            # 自增字段将基于最大数据id
            if column.auto_inc:
                if len(table_data) > 0:
                    last_id = table_data[-1][index]
                else:
                    last_id = -1
                values.append(int(last_id) + 1)
            # 如果字段名为SORTED_AND_UNIQUE_COLUMN_NAME，将会随机生成唯一值
            elif column.name == SORTED_AND_UNIQUE_COLUMN_NAME:
                values.append(random_sorted_and_unique_num())
            # 插入用户指定数据
            elif column.name in [x.lower() for x in column_names]:
                index = [x.lower() for x in column_names].index(column.name)
                values.append(data_values[index])
            # 未指定则插入None
            else:
                values.append(None)
        table_data.append(values)
        self.__upload_data_to_server(table_data, table_info)

    @db_step('更新HDFS表数据')
    def update_data(self, count):
        print('表名: {0}, 更新数据'.format(self.table_name))
        # 获取DBTable对象
        table_info = self.get_table_info()

        # 获取当前所有数据
        table_data = self.get_table_data()
        assert len(table_data) >= count, '修改数据超出当前文件上限'

        # 更新多行数据
        for position in range(count):
            values = []
            current_line_index = len(table_data) - position - 1
            for index, column in enumerate(table_info.columns):
                if self.file_type == HDFSFileType.csv:
                    function_name = column.column_type + '_data'
                else:
                    function_name = column.data_type + '_data'
                # 字段为SORTED_AND_UNIQUE_COLUMN_NAME，自增字段，将不会修改改数据
                if column.name == SORTED_AND_UNIQUE_COLUMN_NAME or \
                        column.auto_inc:
                    values.append(table_data[current_line_index][index])
                # 如果字段允许为空，则有概率插入None
                elif not column.not_null and random_int(0, 9) == 0:
                    values.append(None)
                # 基于数据类型随机生成符合类型、精度、标度的数据
                else:
                    random_value = getattr(HDFSData, function_name)(
                        column.precision, column.scale, column.unsigned, column_type=column.column_type)
                    values.append(random_value)
            table_data[current_line_index] = values
        self.__upload_data_to_server(table_data, table_info)

    @db_step('手动更新HDFS表数据')
    def manual_update_data(self, count, column_names: list, data_values: list):
        print('表名: {0}, 手动更新数据'.format(self.table_name))
        assert len(column_names) == len(data_values), '列数量与列名数量不匹配'
        # 获取DBTable对象
        table_info = self.get_table_info()

        # 获取当前所有数据
        table_data = self.get_table_data()
        assert len(table_data) >= count, '修改数据超出当前文件上限'

        # 更新多行数据
        for position in range(count):
            values = []
            current_line_index = len(table_data) - position - 1
            for index, column in enumerate(table_info.columns):
                # 字段为SORTED_AND_UNIQUE_COLUMN_NAME，自增字段，将不会修改改数据
                if column.name == SORTED_AND_UNIQUE_COLUMN_NAME or \
                        column.auto_inc:
                    values.append(table_data[current_line_index][index])
                # 插入用户指定数据
                elif column.name in [x.lower() for x in column_names]:
                    index = [x.lower() for x in column_names].index(column.name)
                    values.append(data_values[index])
                # 未指定则插入None
                else:
                    values.append(None)
            table_data[current_line_index] = values
        self.__upload_data_to_server(table_data, table_info)

    @db_step('删除HDFS表数据')
    def delete_data(self, count):
        print('表名: {0}, 删除数据'.format(self.table_name))
        # 获取DBTable对象
        table_info = self.get_table_info()

        # 获取当前所有数据
        table_data = self.get_table_data()
        assert len(table_data) >= count, '修改数据超出当前文件上限'

        # 删除多行数据
        table_data = table_data[0:len(table_data) - count]
        self.__upload_data_to_server(table_data, table_info)

    @db_step('判断HDFS表是否存在')
    def is_table_exist(self):
        print('判断表名: {0}, 是否存在'.format(self.table_name))
        file_path = os.path.join(self.download_path, self.table_name)
        if os.path.exists(file_path):
            os.remove(file_path)
        try:
            self.db.download(self.root + '/' + self.table_name, file_path)
            os.remove(file_path)
        except Exception as e:
            if str(e).find('File does not exist') != -1:
                return False
            raise e
        return True

    @db_step('判断HDFS列是否存在')
    def is_column_exist(self, column_info: DBColumn):
        print('判断表名: {0}, 列名：{1}，是否存在'.format(self.table_name, column_info.name))
        return self.get_table_info().is_column_exist(column_info)
