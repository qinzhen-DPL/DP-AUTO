# 测试脚本/测试套件/测试计划

按照不同测试场景划分并存储于：core/testCases

## 测试脚本

测试脚本需以：test_数字索引_测试对象描述，以使unittest框架能够按顺序依次执行：

    def test_001_test_short_description(self):
        pass

测试失败：表示脚本中存在断言错误

    def test_002_fail(self):
        assert False, 'fail'

测试异常：表示脚本中存在未捕获异常

    def test_003_error(self):
        raise Exception('error')

测试跳过：表示该脚本不会被执行

    @skip_depends_on('test_003_error')
    def test_004_skip(self):
        pass


## 测试套件

测试套件类需继承TestBase，其提供了如下变量/别名可全局访问
- settings: 所有框架级别的配置
- config: 所有脚本级别的配置
- param: 数据驱动装饰器
- report_path: 本次测试执行报告文件夹的绝对路径
- data_path: 测试数据文件夹的绝对路径
- system_logger: 系统级别日志,
- execution_context: 获取/设置上下文变量，共享于当前测试


    from core.testCases.testbase import *
   
    class Demo(TestBase):

参数化构建测试套件，需执行参数化读取过程param.via_excel，并创建setParameters函数进行数据绑定/转义

    @param.via_excel('TestData.xlsx', 'demo')
    class Demo(TestBase):

    def setParameters(self, case_name, param_1, param_2):
        self.case_name = str(case_name)
        self.param_1 = str(param_1)
        self.param_2 = str(param_2)


参数化构建测试套件，并回写测试数据结果

    @param.via_excel('TestData.xlsx', 'demo', skip_and_write_back_columns=['column_id'], clear_history=True)
    class Demo(TestBase):

    def setParameters(self, case_name, param_1, param_2):
        self.case_name = str(case_name)
        self.param_1 = str(param_1)
        self.param_2 = str(param_2)
        
    def writeBack(self):
        return self.column_id

## 测试计划

测试计划模板:

	Information:
    Description:
        - 测试计划的描述
    Cases:
        - demo
        - 所有测试用例的路径，相对于core/testCase
