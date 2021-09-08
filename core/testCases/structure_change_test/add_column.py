#!/usr/bin/env python
# encoding: utf-8
from core.testCases.testbase import *


class AddColumnTasks(TestBase):

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

        # 绑定测试信息数据
        cls.source_db_type = 'mysql'    # 工厂能识别的数据库名称，详见工厂函数
        cls.sink_db_type = 'mysql'      # 工厂能识别的数据库名称，详见工厂函数
        cls.is_real = True  # 实时模式还是定时模式
        cls.source_name, cls.source_type = naming_node(cls.source_db_type, is_source=True)  # 获取上游库命名及类型
        cls.sink_name, cls.sink_type = naming_node(cls.sink_db_type, is_source=False)       # 获取下游库命名及类型
        cls.link_task_name = naming_link_and_task(cls.source_db_type, cls.sink_db_type,
                                                  is_real=cls.is_real, now=cls.now)         # 给任务链路创建唯一名称
        cls.source_table_name = naming_source_table('all_types_columns', cls.now)           # 为创建的上游表命名

    def test_001_create_source_db(self):
        """创建测试表，并插入数据"""
        source = create_db_instance(self.source_db_type, self.source_table_name, is_source=True)
        # 读取建表信息文件，并创建上游表
        ddl_file = os.path.join(self.data_path, 'DDL', 'regression', 'mysql_all_types_columns.yml')
        source.create_table(read_yml_ddl_file(ddl_file))
        # 为上游表插入10条随机测试数据
        source.insert_data(10)

    @skip_depends_on('test_001_create_source_db')
    def test_002_create_task(self):
        """创建任务"""
        self.login_api.login()

        # 获取测试用源端与目的端节点，并校验该节点存在
        nodes = {node['name']: node['id'] for node in self.node_api.get_node_list()}
        source_node_id = nodes[self.source_name]
        sink_node_id = nodes[self.sink_name]

        # 获取源端数据库测试用表，如不存在则终止测试
        self.node_api.refresh_node(source_node_id)
        source_table = self.node_api.get_node_existing_entity_tables(source_node_id, self.source_type, True,
                                                                     [self.source_table_name])
        assert len(source_table) == 1, '源端没有可测试表存在'

        # 根据源端测试表，为目的端写入表命名
        sink_table_name = naming_sink_table(self.source_table_name, self.source_db_type, self.sink_db_type,
                                            is_real=self.is_real, now=self.now)
        sink_table = [sink_table_name, None]

        # 创建、更新、激活链路
        link_id = self.link_api.create_link(self.link_task_name)
        payload = LinkData.default_link_data(src_id=source_node_id, sink_id=sink_node_id,
                                             source_type=self.source_type, sink_type=self.sink_type,
                                             is_real=self.is_real, source_tables=source_table,
                                             sink_tables=[sink_table], filed_mappings={}, detail_db_type=None)
        self.link_api.update_link(link_id, payload)
        self.link_api.create_sink_tables(link_id)
        self.link_api.active_link(link_id)

        # 配置策略 - 新增字段
        relation_id = self.link_api.get_link_info_by_id(link_id)['sinkNodes'][0]['relationId']
        payload = LinkData.sink_node_policy_config(node_id=sink_node_id, relation_id=relation_id,
                                                   entity_field_added='CREATE_MAPPING')
        self.link_api.update_link(link_id, payload)

        # 创建、更新、激活任务
        link_mappings = self.link_api.get_link_mapping_info(link_id)
        task_id = self.task_api.create_task(self.link_task_name)
        payload = TaskData.basic_increment_task(src_id=source_node_id, sink_id=sink_node_id,
                                                source_type=self.source_type, sink_type=self.sink_type,
                                                link_id=link_id, link_mappings=link_mappings, is_real=self.is_real)
        self.task_api.update_task(task_id, payload)
        self.task_api.active_task(task_id)

        # 设置下游传递数据
        self.context.source_table = self.source_table_name
        self.context.sink_table = sink_table_name
        self.context.link_id = link_id
        self.context.task_id = task_id

    @skip_depends_on('test_002_create_task')
    def test_003_compare_db(self):
        """比较全量数据同步结果"""
        # 生成源端库、目的端库操作实例对象
        source = create_db_instance(self.source_db_type, self.context.source_table, True)
        sink = create_db_instance(self.sink_db_type, self.context.sink_table, False)

        # 校验对比表结构以及所有数据字段
        db = DBOperator(source, sink, TestBase.data_path)
        source_db_data, sink_db_data = db.get_table_schema_and_data(order_by='id', unique_by='id')
        fail_count = db.compare_schema(source_db_data, sink_db_data)
        fail_count += db.compare_data(source_db_data, sink_db_data)
        print(fail_count)

    @skip_depends_on('test_003_compare_db')
    def test_004_add_column(self):
        """上游表新增字段"""
        source = create_db_instance(self.source_db_type, self.context.source_table, is_source=True)
        source.add_column(DBColumn(name='test', column_type='int', default=None, not_null=False))
        source.insert_data(5)

    @skip_depends_on('test_004_add_column')
    def test_005_compare_db(self):
        """比较全量数据同步结果"""
        # 生成源端库、目的端库操作实例对象
        source = create_db_instance(self.source_db_type, self.context.source_table, True)
        sink = create_db_instance(self.sink_db_type, self.context.sink_table, False)

        # 校验对比表结构以及所有数据字段
        db = DBOperator(source, sink, TestBase.data_path)
        source_db_data, sink_db_data = db.get_table_schema_and_data(order_by='id', unique_by='id')
        fail_count = db.compare_schema(source_db_data, sink_db_data)
        fail_count += db.compare_data(source_db_data, sink_db_data)

    # @skip_depends_on('test_002_create_task')
    # def test_006_delete(self):
    #     """清除数据"""
    #     self.task_api.force_delete_task_by_id(self.context.task_id)
    #     self.link_api.force_delete_link_by_id(self.context.link_id)
    #     source = create_db_instance(self.source_db_type, self.context.source_table, is_source=True)
    #     sink = create_db_instance(self.sink_db_type, self.context.sink_table, is_source=False)
    #     source.delete_table(raise_error=False)
    #     sink.delete_table(raise_error=False)
