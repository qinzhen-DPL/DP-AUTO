#!/usr/bin/env python
# encoding: utf-8
from core.testCases.testbase import *


class HiveOperation(TestBase):
    """Hive操作演示"""

    @classmethod
    def setUpClass(cls):
        # 创建context对象，存储测试过程中的上下文信息
        cls.context = Context()

        # 创建数据操作实例
        cls.ddl_file = os.path.join(cls.data_path, 'DDL', 'example', 'hive.yml')
        cls.export_ddl_file = os.path.join(cls.report_path, 'hive-csv.yml')
        cls.table_name = 'demo_automation_001'
        cls.db = create_db_instance_with_configuration(db_name='hive_csv',
                                                       table_name=cls.table_name,
                                                       configuration=config.HIVE_SINK)

    def test_001_create_table(self):
        """创建内表"""
        self.db.delete_table(raise_error=False)
        self.db.create_table(read_yml_ddl_file(self.ddl_file))
        write_yml_ddl_file(self.db.get_table_info(), self.export_ddl_file)

    @skip_depends_on('test_001_create_internal_table')
    def test_002_data_operation(self):
        """插入、修改、删除数据"""
        self.db.insert_data(10)
        self.db.manual_insert_data(['c1', 'c2'], ['%s', '%s'], ['text', 1])
        all_data = self.db.get_table_data()
        for each in all_data:
            print(each)
        assert len(all_data) == 11

    @skip_depends_on('test_001_create_internal_table')
    def test_003_delete_table(self):
        """删除表"""
        self.db.delete_table(raise_error=False)
        assert self.db.is_table_exist() is False
