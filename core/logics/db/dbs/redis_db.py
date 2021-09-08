#!/usr/bin/env python
# encoding: utf-8
import redis
import re
from core.logics.db.db_driver import *
from core.utils.common import format_json


class RedisDB(DBDriver):
    ALIAS = 'REDIS'

    def __init__(self, db_config: dict, key):
        self.host = db_config["ip"]
        self.port = db_config["port"]
        self.database = db_config["database"]
        self.table_name = key

        # 如作为下游库或没有该schema信息文件，需显示将self.table_info赋值为DBTable对象
        self.table_info = None

        self.db = self.__connect()

    def __del__(self):
        try:
            self.db.close()
        except:
            pass

    def __connect(self):
        print('连接Redis host: {0}, port: {1}, database: {2}'.format(self.host, self.port, self.database))
        db = redis.StrictRedis(host=self.host, port=self.port, db=self.database)
        return db

    @db_call
    def __fetch_all(self):
        print('获取Redis所有数据，键值: {0}'.format(self.table_name))
        result = []
        all_keys = [key for key in self.db.keys() if key.decode('utf-8').startswith(self.table_name)]
        # 因为数据中可能存在各种特殊字符，导致json.loads失败
        # 下面代码对有可能出现的字符进行处理，尽力保证原数据的情况下转为dict
        for key in all_keys:
            # 处理每一行数据，使用\n分割进行单独处理
            msg = self.db.get(key).decode('utf-8')
            lines = msg.split('\n')
            new_lines = []
            for line in lines:
                # 替换\t
                line = line.replace('\t', '\\t')
                # 匹配是否为k:v格式，如匹配则将v值中的双引号去除
                search_result = re.search(r'^  "([^"]*)": "(.*)",', line)
                if search_result:
                    col_name = search_result.group(1)
                    col_value = search_result.group(2)
                    line = '  "' + col_name + '": "' + col_value.replace('"', '\'') + '",'
                new_lines.append(line)
            msg = ''.join(new_lines)
            result_dict = json.loads(msg)
            result_dict = {k.lower(): v for k, v in result_dict.items()}
            line = [result_dict[column.name.lower()] for column in self.table_info.columns]
            result.append(line)
        print('RET: {0}'.format(result))
        return result

    @db_step('获取Redis结构')
    def get_table_info(self):
        print('键值: {0}'.format(self.table_name))
        # 如果提供了self.table_info，则直接返回
        if self.table_info is not None:
            return self.table_info

        all_keys = [key for key in self.db.keys() if key.decode('utf-8').startswith(self.table_name)]
        if not all_keys:
            raise Exception('至少需要一条记录, 才可以获得相应的schema')
        msg = self.db.get(all_keys[0]).decode('utf-8')  # 以第一条记录为样本, 提取相应的column name, 以及列的数量信息
        msg_json = format_json(msg)
        columns = []
        for each in msg_json.keys():
            name = each.lower()
            default = None
            not_null = False
            data_type = 'STRING'
            auto_inc = False
            unsigned = False
            pre = None
            sca = None
            column_type = None
            column = DBColumn(name, data_type, column_type, default, not_null, auto_inc, pre, sca, unsigned)
            columns.append(column)
        table_info = DBTable(self.ALIAS, self.table_name, columns, [])
        return table_info

    @db_step('获取Redis所有数据')
    def get_table_data(self):
        print('键值: {0}'.format(self.table_name))
        results = self.__fetch_all()
        return results

    @db_step('删除Redis所有键值')
    def delete_table(self, raise_error=True):
        print('键值: {0}'.format(self.table_name))
        try:
            all_keys = [key for key in self.db.keys() if key.decode('utf-8').startswith(self.table_name)]
            for key in all_keys:
                self.db.delete(key)
        except Exception as e:
            print(e)
            if raise_error:
                raise e

    def create_table(self, table_info: DBTable):
        raise NotImplementedError

    def rename_table(self, new_table_name: str):
        raise NotImplementedError

    def add_column(self, column_info: DBColumn):
        raise NotImplementedError

    def update_column(self, column_info: DBColumn):
        raise NotImplementedError

    def delete_column(self, column_name: str):
        raise NotImplementedError

    def rename_column(self, old_column_name: str, new_column_name: str):
        raise NotImplementedError

    def add_primary_key(self, keys: list):
        raise NotImplementedError

    def delete_primary_key(self):
        raise NotImplementedError

    def insert_data(self, count: int):
        raise NotImplementedError

    def manual_insert_data(self, *args, **kwargs):
        raise NotImplementedError

    def update_data(self, count_or_condition):
        raise NotImplementedError

    def manual_update_data(self, *args, **kwargs):
        raise NotImplementedError

    def delete_data(self, count_or_condition):
        raise NotImplementedError

    def is_table_exist(self):
        raise NotImplementedError

    def is_column_exist(self, column_info: DBColumn):
        raise NotImplementedError
