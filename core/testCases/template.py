#!/usr/bin/env python
# encoding: utf-8
from core.testCases.testbase import *


@param.via_excel('EXCEL_NAME', 'SHEET_NAME', skip_and_write_back_columns=['COLUMNS'], clear_history=True)
class Template(TestBase):
    """
    使用DDT执行该测试类，测试数据中的每一行将会在执行过程中，动态生成多个测试类，并加载到unittest中
    加载的数据会基于excel的行序号顺序执行

    每个测试类请继承TestBase, TestBase中存储了全局可访问的变量及对象:
    所有测试类的基类，可以通过使用以下类属性或别名，快速访问内置变量
    settings: 所有框架级别的配置
    config: 所有脚本级别的配置
    param: 数据驱动装饰器
    execution_context: 获取/设置上下文变量，共享于当前测试
    每次testrun都会初始化以下变量report_path，data_path，system_logger

    在使用IDE直接运行测试时（不使用runner框架）：
    report_path: 指向report文件根目录
    data_path: 指向assets/TestData目录
    system_logger: None

    在使用runner运行测试时：
    report_path: 本次测试执行报告文件夹的绝对路径
    data_path: 测试数据文件夹的绝对路径
    system_logger: 系统级别日志
    """

    @classmethod
    def setUpClass(cls):
        """
        在该类创建时执行一次且只会执行一次
        如果使用DDT则会基于每行数据动态创建一个新的测试类，既setUpClass会在每个测试数据执行一次

        可在该函数中设置全局数据，该数据可在不同测试步骤之间进行传递，不会被框架重写
        """
        pass

    @classmethod
    def tearDownClass(cls):
        """
        在该类所有测试步骤执行结束后执行一次且只会执行一次
        """
        pass

    def setUp(self):
        """
        在每个测试脚本运行前执行一次
        """
        pass

    def tearDown(self):
        """
        在每个测试脚本运行后执行一次
        """
        pass

    def setParameters(self, *args):
        """
        如果使DDT则会将每行数据中的每列的数据值，动态传递到该函数中
        请保证有效列数量跟该函数接受的参数匹配，如不匹配则会抛出异常

        可在该函数中设置数据初始化逻辑，该函数会在每个测试脚本之前之前执行一次
        如需要使用全局唯一数据，请在setUpClass中设置

        在setUp执行之前运行
        """
        pass

    def writeBack(self):
        """
        如果使DDT则会将改函数的返回值（list or tuple），动态回写到数据文件中
        请保证返回的每个数据为str类型，并且返回的列表或元祖大小与回写的数据量一致

        在tearDownClass执行之前运行
        """
        pass

    def test_001(self):
        """
        具体的测试步骤
        """
        pass

    @skip_depends_on('test_001')
    def test_002(self):
        """
        具体的测试步骤
        可基于上一个测试步骤的结果判断是否跳过
        """
        pass

    @skip_if_any_failure
    def test_003(self):
        """
        具体的测试步骤
        只要在测试过程中有任何的错误，都将跳过执行
        """
        pass
