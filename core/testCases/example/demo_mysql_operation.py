#!/usr/bin/env python
# encoding: utf-8
from core.testCases.testbase import *


class MySqlOperation(TestBase):
    """MySql操作演示"""

    @classmethod
    def setUpClass(cls):
        # 创建context对象，存储测试过程中的上下文信息
        cls.context = Context()

        # 创建数据操作实例
        cls.ddl_file = os.path.join(cls.data_path, 'DDL', 'example', 'mysql.yml')
        cls.export_ddl_file = os.path.join(cls.report_path, 'mysql.yml')
        cls.table_name = 'demo_automation_001'
        cls.db = create_db_instance_with_configuration(db_name='mysql',
                                                       table_name=cls.table_name,
                                                       configuration=config.MYSQL_SINK)

    def test_001_create_table(self):
        """创建数据表"""
        self.db.delete_table(raise_error=False)
        self.db.create_table(read_yml_ddl_file(self.ddl_file))
        write_yml_ddl_file(self.db.get_table_info(), self.export_ddl_file)

    @skip_depends_on('test_001_create_table')
    def test_002_column_operation(self):
        """增加、修改、删除字段"""
        self.db.add_column(DBColumn(name='new_column_1', column_type='int', default='1', not_null=True))
        self.db.add_column(DBColumn(name='new_column_2', column_type='int', default='1', not_null=True))
        self.db.update_column(DBColumn(name='new_column_2', column_type='char(20)', default='default', not_null=True))
        self.db.update_column(DBColumn(name='new_column_2', column_type='char(20)', default=None, not_null=False))
        self.db.rename_column('new_column_2', 'new_column_3')
        self.db.delete_column('new_column_1')
        assert self.db.is_column_exist(DBColumn(name='new_column_1')) is False

    @skip_depends_on('test_002_column_operation')
    def test_003_table_operation(self):
        """重命名表"""
        self.db.rename_table(self.table_name + "_new")

    @skip_depends_on('test_003_table_operation')
    def test_004_data_operation(self):
        """插入、修改、删除数据"""
        self.db.insert_data(10)
        self.db.update_data(5)
        self.db.delete_data(1)
        self.db.manual_insert_data(['c1', 'c2'], ['%s', '%s'], [1, 1])
        self.db.manual_update_data(1, ['c1', 'c2'], ['%s', '%s'], [2, 2])
        self.db.manual_update_data('where id = 1', ['c1', 'c2'], ['%s', '%s'], [3, 3])
        all_data = self.db.get_table_data()
        for each in all_data:
            print(each)
        assert len(all_data) == 10

    @skip_depends_on('test_001_create_table')
    def test_005_delete_table(self):
        """删除表"""
        self.db.delete_table(raise_error=False)
        assert self.db.is_table_exist() is False
