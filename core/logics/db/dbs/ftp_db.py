#!/usr/bin/env python
# encoding: utf-8
import collections
from enum import Enum
from core.logics.other.dp_ftp import FTP
from core.logics.db.db_driver import *


class FtpData:
    """
    随机生成测试数据模块
    """
    @staticmethod
    def int_data(precision, scale, unsigned, **kwargs):
        if unsigned:
            return random_int(0, 4294967295)
        else:
            return random_int(-2147483648, 2147483647)

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
        return float(random_decimal(5, 2, -1000, 1000, unsigned))


class FtpFileType(Enum):
    csv = 0
    json = 1


class FtpDB(DBDriver):
    ALIAS = 'FTP'

    # FTP文件所需的Schema关联文件后缀
    SCHEMA_EXT = '.yml'

    def __init__(self, db_config: dict, file_name, file_type: FtpFileType, encoding='utf8'):
        self.host = db_config["ip"]
        self.port = db_config["port"]
        self.user = db_config["username"]
        self.password = db_config["password"]
        self.root = db_config["root"]
        self.encoding = encoding
        self.file_type = file_type
        self.table_name = file_name
        # FTP-json/csv均需要schema文件用于描述对应数据的结构、字段信息
        # 虽然csv/json并没有强行数据要求限制，但为了拓展下游库数据转换、高级清洗等测试逻辑
        # 需要限制csv/json中某一字段仅会生成属于一种类型的数据，所以需要额外存放、提供schema信息
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

    def __del__(self):
        try:
            self.db.close()
        except:
            pass

    def __connect(self):
        print('连接FTP host: {0}, port: {1}, user: {2}, passwd: {3}, root: {4}'.format(
            self.host, self.port, self.user, self.password, self.root))
        db = FTP()
        db.set_debuglevel(1)
        db.set_pasv(True)
        db.connect(host=self.host, port=self.port)
        db.login(user=self.user, passwd=self.password)
        db.encoding = self.encoding
        return db

    def __create_file(self, file_path, data):
        """
        通过给定数据，在本地生成文件
        """
        with open(file_path, 'w', encoding=self.encoding) as f:
            for line in data:
                f.write(self.separator.join(['"{0}"'.format(str(x) if x is not None else '') for x in line])
                        + self.line_end)

    def __create_json_file(self, file_path, data):
        """
        通过给定数据，在本地生成json文件
        """
        with open(file_path, 'w', encoding=self.encoding) as f:
            for line in data:
                f.write(json.dumps(line, ensure_ascii=False) + '\n')

    @db_call
    def __fetch_all_data(self, file_name):
        # 下载文件、读取文件信息、删除文件
        file_path = os.path.join(self.download_path, file_name)
        if os.path.exists(file_path):
            os.remove(file_path)
        with open(file_path, "wb") as f:
            self.db.retrbinary('RETR {0}'.format(self.root + '/' + file_name), f.write)
        with open(file_path, 'r', encoding=self.encoding) as f:
            result = f.readlines()
        os.remove(file_path)

        # 解析文件
        results = []
        if self.file_type == FtpFileType.csv:
            # 以分行符分行
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
            for msg in result:
                result_dict = format_json(msg, encoding=self.encoding, lower_key=True)
                results.append(list(result_dict.values()))
        return results

    @db_call
    def __get_table_info(self):
        # 下载文件、读取文件信息、删除文件
        file_path = os.path.join(self.download_path, self.table_schema_name)
        if os.path.exists(file_path):
            os.remove(file_path)
        with open(file_path, "wb") as f:
            self.db.retrbinary('RETR {0}'.format(self.root + '/' + self.table_schema_name), f.write)
        table_info = read_yml_ddl_file(file_path)
        os.remove(file_path)
        return table_info

    @db_call
    def __delete(self, file_name, raise_error=True):
        try:
            self.db.cwd('.')
            self.db.delete(self.root + '/' + file_name)
        except Exception as e:
            print(e)
            if raise_error:
                raise e

    @db_call
    def __upload_file(self, local_file_path, file_name):
        with open(local_file_path, "rb") as f:
            self.db.storbinary("STOR {0}".format(self.root + '/' + file_name), f)

    @db_call
    def __rename(self, from_name, to_name):
        self.db.rename(from_name, to_name)

    @db_call
    def __upload_data_to_server(self, table_data, table_info):
        # csv将会把数据上传到服务器
        if self.file_type == FtpFileType.csv:
            for data in table_data:
                print(str(data))
            table_file_path = os.path.join(self.download_path, self.table_name)
            self.__create_file(table_file_path, table_data)
            self.__upload_file(table_file_path, self.table_name)
            os.remove(table_file_path)
        # json将会把数据与字段信息整合成dict后上传到服务器
        else:
            table_file_path = os.path.join(self.download_path, self.table_name)
            table_dict_data = []
            for data in table_data:
                print(str(data))
                data_dict = collections.OrderedDict()
                for index, column in enumerate(table_info.columns):
                    data_dict[column.name] = data[index]
                table_dict_data.append(data_dict)
            self.__create_json_file(table_file_path, table_dict_data)
            self.__upload_file(table_file_path, self.table_name)
            os.remove(table_file_path)

    @db_step('获取FTP文件结构')
    def get_table_info(self):
        print('文件名: {0}'.format(self.table_name))
        # 如果提供了self.table_info，则直接返回
        if self.table_info is not None:
            return self.table_info
        # 未提供，则通过schema文件获取
        table_info = self.__get_table_info()
        for column in table_info.columns:
            # csv默认数据类型为string，用于数据比较
            if self.file_type == FtpFileType.csv:
                column.data_type = 'string'
            # json数据类型转换
            else:
                if column.column_type.lower() in ['int', 'decimal']:
                    column.data_type = 'number'
                else:
                    column.data_type = column.column_type.lower()
        return table_info

    @db_step('获取FTP文件所有数据')
    def get_table_data(self):
        print('文件名: {0}'.format(self.table_name))
        return self.__fetch_all_data(self.table_name)

    @db_step('删除FTP文件')
    def delete_table(self, raise_error=True):
        print('文件名: {0}'.format(self.table_name))
        self.__delete(self.table_name, raise_error)
        if self.table_info is None:
            self.__delete(self.table_schema_name, raise_error)

    @db_step('创建FTP表')
    def create_table(self, table_info: DBTable):
        print('表名: {0}, Schema名: {1}'.format(self.table_name, self.table_schema_name))
        # 检查DBTable对象是否为当前数据库
        if table_info.db.upper() != self.ALIAS:
            raise DBNotMatchingException('当前DB实例为：{0}, 与所要操作的数据库：{1}, 不匹配'.format(
                self.ALIAS, table_info.db))

        # 下载文件、读取文件信息、删除文件
        table_file_path = os.path.join(self.download_path, self.table_name)
        schema_file_path = os.path.join(self.download_path, self.table_schema_name)
        self.__create_file(table_file_path, [])
        write_yml_ddl_file(table_info, schema_file_path)
        self.__upload_file(table_file_path, self.table_name)
        self.__upload_file(schema_file_path, self.table_schema_name)
        os.remove(table_file_path)
        os.remove(schema_file_path)

    @db_step('重命名FTP表')
    def rename_table(self, new_table_name: str):
        print('表名: {0}, 重命名为：{1}'.format(self.table_name, new_table_name))
        self.__rename(self.root + '/' + self.table_name, self.root + '/' + new_table_name)
        self.__rename(self.root + '/' + self.table_schema_name, self.root + '/' + new_table_name + self.SCHEMA_EXT)
        self.table_name = new_table_name
        self.table_schema_name = new_table_name + self.SCHEMA_EXT

    @db_step('增加FTP表字段')
    def add_column(self, column_info: DBColumn):
        raise NotImplementedError

    @db_step('更新FTP表字段')
    def update_column(self, column_info: DBColumn):
        raise NotImplementedError

    @db_step('删除FTP表字段')
    def delete_column(self, column_name: str):
        raise NotImplementedError

    @db_step('重命名FTP表字段')
    def rename_column(self, old_column_name: str, new_column_name: str):
        raise NotImplementedError

    @db_step('增加FTP表主键')
    def add_primary_key(self, keys: list):
        raise NotImplementedError

    @db_step('删除FTP表主键')
    def delete_primary_key(self):
        raise NotImplementedError

    @db_step('插入FTP表数据')
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
                if column.auto_inc:
                    if len(table_data) > 0:
                        last_id = table_data[-1][index]
                    else:
                        last_id = -1
                    values.append(int(last_id) + 1)
                # 如果字段名为SORTED_AND_UNIQUE_COLUMN_NAME，将会随机生成唯一值
                elif column.name == SORTED_AND_UNIQUE_COLUMN_NAME:
                    values.append(random_sorted_and_unique_num())
                # 如果字段允许为空，则有概率插入None
                elif not column.not_null and random_int(0, 9) == 0:
                    values.append(None)
                # 基于数据类型随机生成符合类型、精度、标度的数据
                else:
                    random_value = getattr(FtpData, column.column_type + '_data')(
                        column.precision, column.scale, column.unsigned, column_type=column.column_type)
                    values.append(random_value)
            table_data.append(values)
        self.__upload_data_to_server(table_data, table_info)

    @db_step('手动插入FTP表数据')
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

    @db_step('更新FTP表数据')
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
                # 字段为SORTED_AND_UNIQUE_COLUMN_NAME，自增字段，将不会修改改数据
                if column.name == SORTED_AND_UNIQUE_COLUMN_NAME or \
                        column.auto_inc:
                    values.append(table_data[current_line_index][index])
                # 如果字段允许为空，则有概率插入None
                elif not column.not_null and random_int(0, 9) == 0:
                    values.append(None)
                # 基于数据类型随机生成符合类型、精度、标度的数据
                else:
                    random_value = getattr(FtpData, column.column_type + '_data')(
                        column.precision, column.scale, column.unsigned, column_type=column.column_type)
                    values.append(random_value)
            table_data[current_line_index] = values
        self.__upload_data_to_server(table_data, table_info)

    @db_step('手动更新FTP表数据')
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

    @db_step('删除FTP表数据')
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

    @db_step('判断FTP是否存在')
    def is_table_exist(self):
        print('判断表名: {0}, 是否存在'.format(self.table_name))
        file_path = os.path.join(self.download_path, self.table_name)
        if os.path.exists(file_path):
            os.remove(file_path)
        try:
            with open(file_path, "wb") as f:
                self.db.retrbinary('RETR {0}'.format(self.root + '/' + self.table_name), f.write)
            os.remove(file_path)
        except Exception as e:
            if str(e).find('550 Failed to open file') != -1:
                return False
            raise e
        return True

    @db_step('判断FTP列是否存在')
    def is_column_exist(self, column_info: DBColumn):
        print('判断表名: {0}, 列名：{1}，是否存在'.format(self.table_name, column_info.name))
        return self.get_table_info().is_column_exist(column_info)
