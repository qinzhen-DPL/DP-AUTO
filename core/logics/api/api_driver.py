#!/usr/bin/env python
# encoding: utf-8
import time
import json
import datetime
import requests
import urllib3
from core import config
from core.utils.wait import Wait
from core.exceptions.regression_related_exception import *


API_LOGGING_START = '-' * 10 + 'API-START' + '-' * 10
API_LOGGING_END = '-' * 10 + 'API-END' + '-' * 10


def create_session():
    """
    创建requests库的session对象，用于维护接口上下文请求状态
    """
    return requests.session()


def serialize_dpo(dpo):
    """
    用于序列化接口参数对象，转换为标准json，作为请求payload
    :param dpo: 接口参数对象实例
    :return: json/dict
    """
    def dp(obj):
        new_kv = {}
        for k, v in obj.__dict__.items():
            # 对象为空时，不拼接
            if v is None:
                continue
            # 对象为列表或字典时，并且值不为空时，不拼接
            elif (isinstance(v, list) or isinstance(v, dict)) and not v:
                continue
            else:
               new_kv[k] = v
        return new_kv
    return json.loads(json.dumps(dpo, default=dp))


def api_step(step_name):
    """
    接口步骤装饰器，用于打印接口行为
    """
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            print('[API]: ' + step_name)
            return func(*args, **kwargs)
        return inner_wrapper
    return wrapper


def api_call(func):
    """
    接口调用装饰器，用于打印接口行为
    """
    def inner_wrapper(*args, **kwargs):
        print(API_LOGGING_START)
        start_time = datetime.datetime.now()
        has_error = None
        ret = None
        try:
            ret = func(*args, **kwargs)
        except Exception as e:
            has_error = e
        stop_time = datetime.datetime.now()
        ms = (stop_time - start_time).microseconds / 1000
        print('Request time: {0}ms'.format(str(ms)))
        print(API_LOGGING_END)
        if has_error:
            raise has_error
        return ret
    return inner_wrapper


class ApiDriver:

    def __init__(self, session):
        """
        实例化ApiDriver并存储Session对象
        """
        # 目前仅支持传递session传递APIDriver，用于保持所有接口上下文行为
        if not isinstance(session, requests.sessions.Session):
            raise ApiSessionNotFoundException('Api session 初始化失败，请检查传递的session对象是否正确')
        else:
            self.session = session
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _format_json(self, ori):
        """
        格式化json，用于打印json内容
        """
        return json.dumps(ori, ensure_ascii=False, sort_keys=True, indent=4)

    def url(self, what):
        """
        获取实现类API的url路径
        """
        if what not in self.path_dictionary.keys():
            raise ApiPathNotFoundException('无法找到ApiObject该实现类中的URL路径. {0}'.format(what))
        return self.path_dictionary[what]

    def data(self, key):
        """
        获取实现类API的data
        """
        if key not in self.data_dictionary.keys():
            raise ApiDataNotFoundException('无法找到ApiObject该实现类中的data. {0}'.format(key))
        return self.data_dictionary[key]

    def combine_url(self, *args):
        """
        合并请求URL地址信息
        """
        args_len = len(args)
        if args_len < 2:
            raise ApiPathException('待合并的API URL路径，至少需要两个参数')
        url = args[0]
        for path in args[1:]:
            if str(url).endswith('/') and str(path).startswith('/'):
                url += path[1:]
            elif str(url).endswith('/') or str(path).startswith('/'):
                url += path
            else:
                url += '/' + path
        return url

    def combine_data(self, *args):
        """
        合并请求Data信息
        """
        args_len = len(args)
        if args_len < 2:
            raise ApiDataException('待合并的API Data，至少需要两个参数')
        base_data = args[0]
        combined_data = tuple(args[1:])
        if type(base_data) == dict:
            base_data = (str(base_data)) % combined_data
            base_data = json.loads(base_data.replace('\'', '"'))
        else:
            base_data = str(base_data).format(combined_data)
        return base_data

    def log_response(self, res, need_log=True):
        """
        打印全部响应信息
        """
        if need_log:
            print('\nResponse Url: {0}'.format(res.url))
            print('Response Status code: {0}'.format(res.status_code))
            text = res.text
            try:
                text = self._format_json(json.loads(text))
            except:
                pass
            print('Response Text: {0}'.format(text))
            print('Response Reason: {0}'.format(res.reason))

    @api_call
    def get(self, url, need_log=True, params=None, **kwargs):
        print('Get: {0}'.format(url))
        if need_log:
            print('Params: {0}'.format(params))
            print('Arguments: {0}'.format(kwargs))
        res = self.session.get(url, params=params, **kwargs)
        self.log_response(res, need_log)
        return res

    @api_call
    def post(self, url, need_log=True, data=None, json=None, **kwargs):
        print('Post: {0}'.format(url))
        if need_log:
            print('Data: {0}'.format(data))
            print('Json: {0}'.format(self._format_json(json)))
            print('Arguments: {0}'.format(kwargs))
        res = self.session.post(url, data=data, json=json, **kwargs)
        self.log_response(res, need_log)
        return res

    @api_call
    def put(self, url, need_log=True, data=None, json=None, **kwargs):
        print('Put: {0}'.format(url))
        if need_log:
            print('Data: {0}'.format(data))
            print('Json: {0}'.format(self._format_json(json)))
            print('Arguments: {0}'.format(kwargs))
        res = self.session.put(url, data=data, json=json, **kwargs)
        self.log_response(res, need_log)
        return res

    @api_call
    def delete(self, url, need_log=True, **kwargs):
        print('Delete: {0}'.format(url))
        if need_log:
            print('Arguments: {0}'.format(kwargs))
        res = self.session.delete(url, **kwargs)
        self.log_response(res, need_log)
        return res

    @api_call
    def head(self, url, need_log=True, **kwargs):
        print('Head: {0}'.format(url))
        print('Arguments: {0}'.format(kwargs))
        res = self.session.head(url, **kwargs)
        self.log_response(res, need_log)
        return res

    @api_call
    def options(self, url, need_log=True, **kwargs):
        print('Options: {0}'.format(url))
        print('Arguments: {0}'.format(kwargs))
        res = self.session.options(url, **kwargs)
        self.log_response(res, need_log)
        return res

    def raise_for_status(self, response):
        """
        自动检查返回体结果，判断是否有异常
        """
        http_error_msg = ''
        if isinstance(response.reason, bytes):
            try:
                reason = response.reason.decode('utf-8')
            except UnicodeDecodeError:
                reason = response.reason.decode('iso-8859-1')
        else:
            reason = response.reason

        # 客户端异常
        if 400 <= response.status_code < 500:
            http_error_msg = u'%s 客户端异常: %s, URL: %s, 异常信息: %s' % \
                             (response.status_code, reason, response.url, response.text)

        # 服务器异常
        elif 500 <= response.status_code < 600:
            http_error_msg = u'%s 服务器异常: %s, URL: %s, 异常信息: %s' % \
                             (response.status_code, reason, response.url, response.text)

        if http_error_msg:
            print(http_error_msg)
            raise ApiException(http_error_msg)
