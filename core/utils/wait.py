#!/usr/bin/env python
# encoding: utf-8
import time
import datetime
from core import settings
from core.exceptions.regression_related_exception import TimeoutException


def calculate_time_duration(start):
    stop = datetime.datetime.now()
    seconds = (stop - start).seconds
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return h, m, s


class Wait:
    """
    轮询等待
    """

    def __init__(self, timeout, wait_interval=None):
        """
        :param timeout: 等待超时时间
        :param wait_interval: 轮询等待间隔时间
        """
        self.timeout = timeout
        self.wait_interval = wait_interval if wait_interval else settings.WAIT_INTERVAL
        self.start_time = time.time()

    def until(self, method, message=''):
        """
        循环调用等待步骤，基于步骤返回的结果判断是否继续执行等待
        如等待后已超时则终止等待，并抛出异常
        对于等待步骤并不做异常捕获处理，请在步骤中配置异常捕获逻辑
        如遇异常将直接中断等待
        """
        end_time = time.time() + self.timeout
        while True:
            value = method()
            if value:
                return value
            time.sleep(self.wait_interval)
            if time.time() > end_time:
                break
        raise TimeoutException(message)
