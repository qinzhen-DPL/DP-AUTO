#!/usr/bin/env python
# encoding: utf-8
from core.testCases.steps_for_reuse.node_steps import *
from core.testCases.steps_for_reuse.link_steps import *
from core.testCases.steps_for_reuse.task_steps import *
from core.testCases.steps_for_reuse.db_steps import *
from core.testCases.steps_for_reuse.teardown_steps import *


@param.via_excel('TestData.xlsx', 'postgresql',
                 skip_and_write_back_columns=['link_id', 'task_id', 'source_table_name', 'sink_table_name'],
                 clear_history=False)
class TestMySqlTasks(TestBase):

    @classmethod
    def setUpClass(cls):
        # 创建session对象，存储测试过程中的接口上下文信息
        cls.api_session = create_session()

        # 创建context对象，存储测试过程中的上下文信息
        cls.context = Context()

        # 创建接口抽象实例
        cls.login_api = LoginApi(cls.api_session)
        cls.node_api = NodeApi(cls.api_session)
        cls.link_api = LinkApi(cls.api_session)
        cls.task_api = TaskApi(cls.api_session)

        # 设置任务、链路、表明的时间戳缩写，全局统一
        cls.now = naming_now()
        cls.ddl_files = [os.path.join(cls.data_path, 'DDL', 'regression', 'postgresql_all_types_columns.yml')]

    def setParameters(self, case_name, source_db_type, sink_db_type):
        self.case_name = str(case_name)
        self.source_db_type = source_db_type
        self.sink_db_type = sink_db_type
        self.is_real = True
        self.source_name, self.source_type = naming_node(source_db_type, is_source=True)
        self.sink_name, self.sink_type = naming_node(sink_db_type, is_source=False)
        self.link_task_name = naming_link_and_task(source_db_type, sink_db_type, is_real=self.is_real, now=self.now)
        self.source_table_names = [naming_source_table('all_types_columns', self.now)]

    def test_001_create_source_db(self):
        """创建测试表，并插入数据"""
        create_source_table(self.ddl_files, self.source_table_names, self.source_db_type)

    @skip_depends_on('test_001_create_source_db')
    def test_002_create_task(self):
        """创建任务"""
        self.login_api.login()

        self.context.source_node_id, self.context.sink_node_id, self.context.source_tables, self.context.sink_tables = \
            init_single_source_sink_node_basic_info(self.node_api, self.source_table_names, self.ddl_files,
                                                    self.source_db_type, self.source_name, self.source_type,
                                                    self.sink_db_type, self.sink_name, self.sink_type,
                                                    self.is_real, self.now)

        self.context.link_id, self.context.link_mappings = \
            create_single_basic_link(self.link_api, self.link_task_name,
                                     self.source_type, self.context.source_tables, self.context.source_node_id,
                                     self.sink_type, self.context.sink_tables, self.context.sink_node_id,
                                     self.is_real, self.sink_db_type)

        self.context.task_id = create_single_basic_task(self.task_api, self.link_task_name,
                                                        self.context.link_mappings, self.context.link_id,
                                                        self.context.source_node_id, self.source_type,
                                                        self.context.sink_node_id, self.sink_type, self.is_real)

    @skip_depends_on('test_002_create_task')
    def test_003_compare_db(self):
        """比较全量数据同步结果"""
        schema_fail_count, data_fail_count = \
            compare_db_data_and_schema(self.node_api, self.link_api, self.context.link_id, self.context.sink_node_id,
                                       self.context.source_tables, self.source_db_type, self.source_type,
                                       self.context.sink_tables, self.sink_db_type, self.sink_type,
                                       order_by='id', unique_by='id')
        assert schema_fail_count + data_fail_count == 0, '数据库比较异常，请查看打印信息进行分析'

    @skip_depends_on('test_002_create_task')
    def test_004_delete(self):
        """清除数据"""
        delete_table_link_task(self.task_api, self.link_api, self.context.task_id, self.context.link_id,
                               self.context.source_tables, self.context.sink_tables,
                               self.source_db_type, self.sink_db_type)
