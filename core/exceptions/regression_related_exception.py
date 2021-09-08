#!/usr/bin/env python
# encoding: utf-8


class RegressionException(Exception):
    """回归测试级别的异常基类"""

    def __init__(self, msg='', logger=None):
        # 需要打印的异常信息
        self.message = msg

        # 如有配置logger则调用logger.error输出异常
        if logger:
            logger.error(msg)

        Exception.__init__(self, msg)

    def __repr__(self):
        return self.message

    __str__ = __repr__


class ApiPathNotFoundException(RegressionException):
    """API路径为找到"""
    pass


class DBSQLNotFoundException(RegressionException):
    """SQL未找到"""
    pass


class ApiDataNotFoundException(RegressionException):
    """API数据为找到"""
    pass


class ApiPathException(RegressionException):
    """API路径拼接错误"""
    pass


class ApiDataException(RegressionException):
    """API数据拼接错误"""
    pass


class ApiException(RegressionException):
    """API执行出错"""
    pass


class ApiSessionNotFoundException(RegressionException):
    """API Session未定义"""
    pass


class ParameterDefinitionException(RegressionException):
    """参数定义错误"""
    pass


class TimeoutException(RegressionException):
    """超时错误"""
    pass


class DBDefaultMappingException(RegressionException):
    """数据库默认映射读取或解析错误"""
    pass


class NotSupportedFtpFileFormatException(RegressionException):
    """解析FTP下载文件类型错误"""
    pass


class DBCouldNotUseAsSourceException(RegressionException):
    """不允许作为源端数据库错误"""
    pass


class DBCouldNotUseAsSinkException(RegressionException):
    """不允许作为下游数据库错误"""
    pass


class DBNotSupportedException(RegressionException):
    """数据库系统不支持错误"""
    pass


class UniqueByNotInTableColumnException(RegressionException):
    """表去重字段不存在错误"""
    pass


class OrderByNotInTableColumnException(RegressionException):
    """表排序字段不存在错误"""
    pass


class FiledMappingNotFoundException(RegressionException):
    """字段映射关系不存在错误"""
    pass


class SequoiaDBOperationException(RegressionException):
    """SequoiaDB调用器出现错误"""
    pass


class WriteBackException(RegressionException):
    """写入测试结果时出现错误"""
    pass


class DBNotMatchingException(RegressionException):
    """数据库表对象与当前DB实例不匹配"""
    pass


class PrimaryKeysSameException(RegressionException):
    """主键不能包含相同的列"""
    pass


class PrimaryKeysNotExistException(RegressionException):
    """主键不存在"""
    pass


class SortedAndUniqueOverLoadException(RegressionException):
    """有序且唯一字段超出上限"""
    pass


class IntervalYearToMonthNotSupportException(RegressionException):
    """INTERVAL YEAR TO MONTH类型cx_oracle不支持"""
    pass


class NoWaitConditionException(RegressionException):
    """没有提供等待条件"""
    pass
