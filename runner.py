#!/usr/bin/env python
# encoding: utf-8
import os
import time
import datetime
import unittest
import logging
import argparse
import ruamel.yaml
import traceback
import shutil
from core import settings
from core.operators.notifier import Notifier
from core.operators.html_test_runner import HTMLTestReport
from core.exceptions.framework_related_exception import *

USAGE = '''
DataPipeline Runner 用法
通过指定以下参数执行自动化回归测试
'''
CASE_USAGE = '执行指定的测试脚本'
PLAN_USAGE = '执行指定的测试计划'
NOTIFIER_USAGE = '通知回归结果'
DIR_SYMBOL = '${CWD}/'
TEST_PLAN = 'TestPlan'
TEST_DATA = 'TestData'
LATEST_REPORT_FOLDER = 'LATEST'

# 退出状态码
STEP_PASS = 0
REGRESSION_FAIL = 2
CLEANUP_FAIL = 4
NOTIFY_FAIL = 8


def get_workspace(str_dir):
    """
    获取当前工作空间的路径，
    :param str_dir: 原始路径
    :return: 格式化后的路径
    """
    # 使用DIR_SYMBOL 表示在当前工作空间为基准，查找相对路径
    if str_dir.startswith(DIR_SYMBOL):
        relative_dir = str_dir.replace(DIR_SYMBOL, '')
        return os.path.join(os.path.split(os.path.realpath(__file__))[0], relative_dir)
    # 使用用户指定的路径
    else:
        return str_dir


class DPLogger:
    """
    请勿在其他类中生成该Logger对象，该Logger用作框架全局系统生产日志
    所有脚本中需要记录的日志，请使用print()
    框架执行过程中将会把控制台输出信息重定向到报告以及脚本日志文件中
    """

    def __init__(self, log_file_path):
        """
        用于初始化自动化框架系统级别Logging，请勿手动调用该函数或尝试初始化该类型
        """
        self.log_file = log_file_path
        self.logger = logging.getLogger('DP')
        self._init_logging('DataPipeline Automation Framework')

    def _init_logging(self, welcome_str):
        """
        用于初始化自动化框架系统级别Logging，请勿手动调用该函数或尝试初始化该类型
        :param welcome_str: welcome_str
        """
        # Logging全局配置信息，如需定制请修改打印配置
        self.logger.setLevel(logging.INFO)
        formatter_fh = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
        formatter_ch = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')

        # Logging到文件的配置
        fh = logging.FileHandler(self.log_file, mode='w', encoding='UTF-8')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter_fh)
        self.logger.addHandler(fh)

        # Logging 控制台的配置
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter_ch)
        self.logger.addHandler(ch)

        # 打印框架信息
        welcome_str_len = len(welcome_str)
        self.logger.info('*' * (welcome_str_len + 12))
        self.logger.info('*{0}*'.format(' ' * (welcome_str_len + 10)))
        self.logger.info('*     {0}     *'.format(welcome_str))
        self.logger.info('*{0}*'.format(' ' * (welcome_str_len + 10)))
        self.logger.info('*' * (welcome_str_len + 12))


class TestRunner:
    """
    本类为测试框架的入口执行类
    定义了自动化测试执行的生命周期:
    1.  初始化测试信息，以及读取所有测试脚本信息
    2.  创建unittest runner，并依次执行所有测试用例/测试套件
    3.  在回归测试同时，实时获取执行结果，并在最终生成测试报告
        Pass -> 代表脚本执行无误
        Failure -> 代表脚本执行中有断言错误或与产品有关校验错误
        Exception -> 代表脚本执行中有异常
    4.  拷贝测试生成的数据及文件，并复制到LATEST文件夹
    5.  调用notifier, 例如：将结果通过邮件发送
    """

    def __init__(self):
        """
        初始化测试信息，并读取所有测试脚本信息
        """
        # 初始化测试空间及文件夹
        self.result = None
        self.start_time = datetime.datetime.now()
        self.report_path, self.asset_path = self._init_workspace()
        # 注意：测试类、测试脚本文件存放路径为: core.testCases
        self.case_path = os.path.join(os.path.split(os.path.realpath(__file__))[0], 'core', 'testCases')
        self.plan_path = os.path.join(self.asset_path, TEST_PLAN)
        self.data_path = os.path.join(self.asset_path, TEST_DATA)

        # 初始化并获取用户CLI传递的参数
        self.parser = self._prepare_cli()
        self.args = self._parse_arguments()

        # 加载所有测试脚本
        self.cases = self._load_executable_cases()

    def __del__(self):
        """
        runner执行结束后被调用，打印执行时间等信息
        """
        stop = datetime.datetime.now()
        seconds = (stop - self.start_time).seconds
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        self.logger.info('')
        self.logger.info('DP 执行结束: {0}'.format(stop))
        self.logger.info('DP 执行时间: %02d:%02d:%02d' % (h, m, s))

    def _init_workspace(self):
        """
        检查并创建测试根目录，并初始化系统级别logging
        :return: 报告目录文件位置, 资产文件位置
        """
        workspace_dir = str(time.strftime('%Y_%m_%d_%H%M%S', self.start_time.timetuple()))
        report_base_dir = os.path.abspath(os.path.join(get_workspace(settings.REPORT), workspace_dir))
        os.makedirs(report_base_dir, exist_ok=True)
        assets_base_dir = os.path.abspath(get_workspace(settings.ASSETS))
        log_file = os.path.abspath(os.path.join(report_base_dir, 'runner.log'))
        self.html_file = os.path.abspath(os.path.join(report_base_dir, 'report.html'))
        self.logger = DPLogger(log_file).logger
        self.logger.info('DP 开始执行: {0}'.format(self.start_time))
        self.logger.info('DP 结果报告路径: {0}'.format(report_base_dir))
        self.logger.info('DP 资产文件路径: {0}'.format(assets_base_dir))
        self.logger.info('DP 日志文件路径: {0}'.format(log_file))
        if not os.path.exists(assets_base_dir):
            raise AssetNotFoundException('资产文件路径不存在，地址: {0}'.format(assets_base_dir), self.logger)
        return report_base_dir, assets_base_dir

    def _prepare_cli(self):
        """
        配置runner支持的构建参数
        -c, --case: 执行指定的测试脚本
        -p, --plan: 执行指定的测试用例
        -n, --notify: 执行结束后发送结果通知
        :return: argument parser
        """
        self.logger.info('')
        self.logger.info('DP Runner 开始运行!!!')
        parser = argparse.ArgumentParser(prog='operators',
                                         formatter_class=argparse.RawDescriptionHelpFormatter,
                                         description=USAGE)
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('-c', '--case', nargs='+', help=CASE_USAGE)
        group.add_argument('-p', '--plan', nargs='+', help=PLAN_USAGE)
        parser.add_argument('-n', '--notify', nargs='+', help=NOTIFIER_USAGE)
        return parser

    def _parse_arguments(self):
        """
        获取用户传递的执行参数
        :return: argument list
        """
        self.logger.info('检查执行参数...')
        args = self.parser.parse_args()
        self.logger.info('执行参数: {0}'.format(args))

        # 如果需要结果通知，检查该通知方式系统当前是否支持
        if args.notify:
            available_notifier = [x.ALIAS for x in Notifier.__subclasses__()]
            for notifier in args.notify:
                if str(notifier).lower() not in available_notifier:
                    raise NoSuchNotifierException('不支持该通知方式: {0}'.format(notifier), self.logger)
        return args

    def _load_executable_cases(self):
        """
        加载所有需要执行的脚本
        """
        self.logger.info('加载测试脚本...')
        if self.args.case:
            return self._load_cases(self.args.case)
        elif self.args.plan:
            return self._load_plan(self.args.plan)

    def _load_cases(self, cases):
        """
        分析测试脚本，并添加到unittest中
        :param cases: cases list
        :return: 包含所有测试脚本的test_suite
        """
        test_suite = unittest.TestSuite()
        discover_case_count = 0
        for case in cases:
            if not case.endswith('.py'):
                case = case + '.py'
            # 依次通过测试用例相对于testCase文件的目录进行查找测试脚本
            seek_folder = os.path.abspath(os.path.join(self.case_path, case, '..'))
            discover = unittest.defaultTestLoader.discover(seek_folder,
                                                           pattern=case.split('/')[-1],   # case名字，不带文件夹符号
                                                           top_level_dir=seek_folder)
            cass_count = discover.countTestCases()
            discover_case_count += cass_count
            # 测试文件中有效的测试用例不能为0
            if cass_count == 0:
                raise TestCaseNotFoundException('未找到测试脚本: {0}'.format(case), self.logger)
            test_suite.addTest(discover)
            self.logger.info('测试模块: {0}, 总共: {1} 测试脚本'.format(case, cass_count))
        self.logger.info('总共：{0} 模块, 总共：{1} 测试脚本'.format(len(cases), discover_case_count))
        return test_suite

    def _load_plan(self, plans):
        """
        分析测试计划文件，并添加到unittest中
        :param plans: plan list
        :return: 包含所有测试脚本的test_suite
        """
        cases = []
        for plan in plans:
            if not plan.endswith('.yml'):
                plan = plan + '.yml'
            plan_file = os.path.abspath(os.path.join(self.plan_path, plan))
            try:
                with open(plan_file, 'r', encoding='utf-8') as fs:
                    data = ruamel.yaml.round_trip_load(fs)
                cases.extend(data['Cases'])
                self.logger.info('测试计划: {0}, 总共: {1} 模块'.format(plan, len(data['Cases'])))
            except Exception as e:
                raise TestCaseNotFoundException('测试计划文件异常，文件: {0}, 异常: {1}, 堆栈信息: {2}'.
                                                format(plan_file, e, traceback.format_exc()), self.logger)
        return self._load_cases(cases)

    def do_regression(self):
        """
        创建HTMLTestReport对象，并执行给定的所有测试脚本/套件
        :return: 执行成功返回0，否则返回2
        """
        try:
            self.logger.info('')
            self.logger.info('开始运行回归测试...')
            with open(self.html_file, 'wb') as fp:
                # 传递runner对象的logger, report_path, data_path到HTMLTestReport中
                # 使测试脚本执行线程可以访问该对象
                # logger: 为系统级别日志
                # report_path: 当前测试执行的报告文件的绝对路径
                # data_path: 测试资产文件的绝对路径
                runner = HTMLTestReport(logger=self.logger, report_path=self.report_path, data_path=self.data_path,
                                        stream=fp, verbosity=2,
                                        title='DataPipeline 自动化测试报告',
                                        description='')
                self.result = {'detail': runner.run(self.cases), 'startTime': self.start_time}
                if self.result['detail'].error_count != 0 or self.result['detail'].failure_count != 0:
                    step_pass = False
                else:
                    step_pass = True
        except Exception as ex:
            self.logger.error(str(ex))
            self.logger.error(traceback.format_exc())
            step_pass = False
        return STEP_PASS if step_pass else REGRESSION_FAIL

    def do_cleanup(self):
        """
        复制所有测试结果文件，并拷贝到LATEST文件夹
        :return: 执行成功返回0，否则返回4
        """
        try:
            self.logger.info('')
            self.logger.info('开始执行结果清理步骤...')
            destination = os.path.abspath(os.path.join(get_workspace(settings.REPORT), LATEST_REPORT_FOLDER))
            self.logger.info('复制所有测试结果文件，并拷贝到LATEST文件夹...')
            if os.path.exists(destination):
                shutil.rmtree(destination)
            shutil.copytree(self.report_path, destination)
            self.logger.info('测试结果文件路径: {0}'.format(destination))
            step_pass = True
        except Exception as ex:
            self.logger.error(str(ex))
            self.logger.error(traceback.format_exc())
            step_pass = False
        return STEP_PASS if step_pass else CLEANUP_FAIL

    def do_notify(self):
        """
        发送结果通知
        :return: 执行成功返回0，否则返回8
        """
        if not self.args.notify:
            return STEP_PASS
        try:
            self.logger.info('')
            self.logger.info('开始发送测试报告通知...')
            # 检查当前系统是否支持该notifier
            for notifier in self.args.notify:
                known_notifier = list(filter(lambda x: x.ALIAS == notifier.lower(), Notifier.__subclasses__()))
                if len(known_notifier) != 1:
                    raise NoSuchNotifierException('不支持该通知方式: {0}'.format(notifier), self.logger)
                module = getattr(getattr(__import__('core'), 'operators'), 'notifier')
                clazz = getattr(module, known_notifier[0].__name__)
                notifier = clazz(self.logger, self.result, self.report_path)
                notifier.inform()
            step_pass = True
        except Exception as ex:
            self.logger.error(str(ex))
            self.logger.error(traceback.format_exc())
            step_pass = False
        return STEP_PASS if step_pass else NOTIFY_FAIL

    def execute_cli(self):
        """
        执行回归测试，并发送测试结果
        :return: 退出状态码
        """
        exit_code = self.do_regression()
        exit_code += self.do_cleanup()
        exit_code += self.do_notify()
        return exit_code


if __name__ == '__main__':
    """
    exit 0: 测试执行正确，没有异常
    exit 1: Runner初始化失败，或其他框架未捕获异常 
    exit 2: 回归测试失败 
    exit 4: 结果清理步骤失败  
    exit 8: 结果通知失败
    """
    exit(TestRunner().execute_cli())
