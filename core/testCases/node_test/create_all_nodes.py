#!/usr/bin/env python
# encoding: utf-8
from core.testCases.testbase import *


@param.via_excel('TestData.xlsx', 'node', skip_and_write_back_columns=['node_id', 'node_name'], clear_history=False)
class TestCreateAllNodes(TestBase):

    @classmethod
    def setUpClass(cls):
        # 创建session对象，存储测试过程中的接口上下文信息
        cls.api_session = create_session()

        # 创建context对象，存储测试过程中的上下文信息
        cls.context = Context()

        # 创建接口抽象实例
        cls.login_api = LoginApi(cls.api_session)
        cls.node_api = NodeApi(cls.api_session)

    # def writeBack(self):
    #     return self.context.node_id, self.context.node_name

    def setParameters(self, case_name, db_type, source_or_sink):
        self.case_name = str(case_name)
        self.is_source = source_or_sink.lower() == 'source'
        self.node_name, self.node_type = naming_node(db_type, self.is_source, '')
        self.context.node_name = self.node_name

    def test_001_login_and_clean_data(self):
        """登录系统并清空历史数据"""
        self.login_api.login()
        self.node_api.check_state_and_delete_node_by_names([self.node_name])

    @skip_depends_on('test_001_login_and_clean_data')
    def test_002_create_node(self):
        """创建、激活节点"""
        self.context.node_id = self.node_api.create_node(self.node_name, self.node_type, self.is_source)
        self.node_api.init_node(self.context.node_id, self.node_type, self.is_source)
        self.node_api.wait_node_init_success(self.context.node_id)

    # @skip_depends_on('test_002_create_node')
    # def test_003_delete_node(self):
    #     """暂停、删除节点"""
    #     self.node_api.suspend_node(self.context.node_id)
    #     self.node_api.delete_node(self.context.node_id)
