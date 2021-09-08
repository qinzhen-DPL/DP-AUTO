#!/usr/bin/env python
# encoding: utf-8


class FrameworkException(Exception):
    """框架级别的异常基类"""

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


class NoSuchNotifierException(FrameworkException):
    """结果通知类当前框架不支持"""
    pass


class AssetNotFoundException(FrameworkException):
    """资产文件未找到"""
    pass


class TestCaseNotFoundException(FrameworkException):
    """测试脚本未找到"""
    pass
