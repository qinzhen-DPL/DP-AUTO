#!/usr/bin/env python
# encoding: utf-8
import os
import subprocess
from core.logics.db.db_driver import *
from core.utils.common import format_json


class SequoiaDB(DBDriver):
    ALIAS = 'SEQUOIADB'

    ENDING = 'SDB_SUCCESS'

    query = {
        'get_table_schema':
            "get_table_schema",
        'get_all_data':
            "get_all_data",
        'delete_table':
            "delete_table",
    }

    def __init__(self, db_config: dict, collection):
        self.host = db_config["ip"]
        self.port = db_config["port"]
        self.user = db_config["username"]
        self.password = db_config["password"]
        self.database = db_config["database"]
        self.table_name = collection

        # 如作为下游库或没有该schema信息文件，需显示将self.table_info赋值为DBTable对象
        self.table_info = None

        self.__connect()

    def __connect(self):
        print('连接SequoiaDB host: {0}, port: {1}, user: {2}, password: {3}, collection_space: {4}'.format(
            self.host, self.port, self.user, self.password, self.database))

    @db_call
    def execute(self, sql, collection):
        """
        因巨杉python-driver系统兼容性问题，替换使用java-driver进行调用
        """
        print('SQL: {0}'.format(sql))
        cmd = 'java -jar {jar_path} {cmd} {ip} {port} {user} {passwd} {collection_space} {collection}'.format(
            jar_path=os.path.abspath(os.path.join('lib', 'SDB.jar')),
            cmd=sql,
            ip=self.host, port=self.port, user=self.user, passwd=self.password,
            collection_space=self.database, collection=collection
        )
        print('CMD: {0}'.format(cmd))
        popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = popen.communicate()
        ret = stdout.split('\n')
        print('STDOUT: {0}'.format(ret))
        print('STDERR: {0}'.format(stderr))
        if stderr:
            raise SequoiaDBOperationException('SequoiaDB调用器出现错误，请查看日志中的异常')
        return ret

    @db_step('获取SequoiaDB结构')
    def get_table_info(self):
        print('表名: {0}'.format(self.table_name))
        # 如果提供了self.table_info，则直接返回
        if self.table_info is not None:
            return self.table_info

        results = self.execute(self.sql('get_all_data'), self.table_name)
        if not results:
            raise Exception('至少需要一条记录, 才可以获得相应的schema')
        results_json = format_json(results[0])

        for a_key in list(results_json.keys()):
            if a_key.startswith("_"):
                results_json.pop(a_key)  # 去掉第一个特殊字符
                break
        columns = []
        for each in results_json.keys():
            name = each
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

    @db_step('获取SequoiaDB所有数据')
    def get_table_data(self):
        print('Collection: {0}'.format(self.table_name))
        results = self.execute(self.sql('get_all_data'), self.table_name)
        result = []
        for msg in results:
            if not msg.strip():
                continue
            result_dict = json.loads(msg)
            result_dict = {k.lower(): v for k, v in result_dict.items()}
            line = []
            for column in self.table_info.columns:
                item = result_dict[column.name.lower()]
                if isinstance(item, dict):
                    item = item[list(item.keys())[0]]
                line.append(item)
            result.append(line)
        return result

    @db_step('获取SequoiaDB所有数据')
    def delete_table(self, raise_error=True):
        print('Collection: {0}'.format(self.table_name))
        self.execute(self.sql('delete_table'), self.table_name)

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
