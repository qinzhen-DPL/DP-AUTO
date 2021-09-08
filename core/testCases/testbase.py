#!/usr/bin/env python
# encoding: utf-8
import os
import unittest
# api常用库
from core.logics.api.api_driver import create_session, serialize_dpo
from core.logics.api.apis.login_api import LoginApi
from core.logics.api.apis.node_api import NodeApi, NodeType, NodeData
from core.logics.api.apis.link_api import LinkApi, LinkData
from core.logics.api.apis.task_api import TaskApi, TaskData

# db常用库
from core.logics.db.db_driver import *
from core.logics.db.db_factory import create_db_instance, create_db_instance_with_configuration
from core.logics.db.db_operator import DBOperator
from core.logics.db.excepted_condition import *
from core.logics.db.db_compare_rule import DBSchemaCompareRule, DBValueCompareRule

# 测试常用库
from core.logics.other.naming import *

# 系统常用库
from runner import DIR_SYMBOL, TEST_DATA
from core import settings
from core import config
from core.operators import param
from functools import wraps


def skip_if_any_failure(test_func):
    """
    前置脚本有任何异常，终止后续脚本运行
    """
    @wraps(test_func)
    def inner_func(self):
        failures = [fail[0] for fail in self._outcome.result.failures]
        errors = [error[0] for error in self._outcome.result.errors]
        skipped = [error[0] for error in self._outcome.result.skipped]
        if (len(failures) != 0) or (len(errors) != 0) or (len(skipped) != 0):
            test = unittest.skipIf(True, "前置脚本失败，跳过该测试脚本")(test_func)
        else:
            test = test_func
        return test(self)
    return inner_func


def skip_depends_on(depend):
    """
    指定前置脚本失败时，终止后续脚本运行
    """
    def wrapper_func(test_func):
        @wraps(test_func)
        def inner_func(self):
            str_class = "%s.%s" % (self.__class__.__module__, self.__class__.__qualname__)
            filter_name = "%s (%s)" % (depend, str_class)

            if depend == test_func.__name__:
                raise ValueError("{} 不能依赖自身".format(depend))
            failures = [str(fail[0]) for fail in self._outcome.result.failures]
            errors = [str(error[0]) for error in self._outcome.result.errors]
            skipped = [str(error[0]) for error in self._outcome.result.skipped]
            flag = (filter_name in failures) or (filter_name in errors) or (filter_name in skipped)
            if flag:
                test = unittest.skipIf(True, "前置脚本失败，跳过该测试脚本")(test_func)
            else:
                test = test_func
            return test(self)
        return inner_func
    return wrapper_func


def get_workspace(str_dir):
    """
    获取当前工作空间的路径，
    :param str_dir: 原始路径
    :return: 格式化后的路径
    """
    # 使用DIR_SYMBOL 表示在当前工作空间为基准，查找相对路径
    if str_dir.startswith(DIR_SYMBOL):
        relative_dir = str_dir.replace(DIR_SYMBOL, '')
        return os.path.join(os.path.split(os.path.realpath(__file__))[0], '..', '..', relative_dir)
    # 使用用户指定的路径
    else:
        return str_dir


class Context:
    """
    用于获取/设置上下文变量，可在步骤/脚本/套件间传递
    """
    pass


class TestBase(unittest.TestCase):
    """
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
    # 请勿在子类中重写以下属性值，在runner加载时动态设置
    system_logger = None
    report_path = os.path.abspath(get_workspace(settings.REPORT))
    data_path = os.path.abspath(os.path.join(get_workspace(settings.ASSETS), TEST_DATA))

    # 获取/设置上下文变量，共享于当前测试
    execution_context = Context()
