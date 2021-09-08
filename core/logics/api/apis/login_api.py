#!/usr/bin/env python
# encoding: utf-8
from core.logics.api.api_driver import *


class LoginApi(ApiDriver):

    path_dictionary = {
        'login': '/v3/users/login',
        'check': '/v3/product/auth',
    }

    data_dictionary = {
        'login': {"login": "%s", "password": "%s"},
    }

    def __init__(self, session):
        ApiDriver.__init__(self, session)

    @api_step(step_name='通过用户名、密码登录系统')
    def login(self, user_name=None, password=None):
        url = self.combine_url(config.API_BASE_URL, self.url('login'))
        # 不设置用户名密码时，使用系统配置账号
        if not user_name:
            user_name = config.USERNAME
        if not password:
            password = config.PASSWORD
        data = self.combine_data(self.data('login'), user_name, password)
        res = self.post(url, json=data, verify=True)
        self.raise_for_status(res)
        token = res.json()['data']['token']
        print("Token: {0}".format(token))
        # 为接口session设置全局header，记录登录状态
        self.session.headers.update({'Authorization': token})
        return token

    @api_step(step_name='检查token是否过期')
    def check_token(self):
        url = self.combine_url(config.API_BASE_URL, self.url('check'))
        res = self.get(url, verify=True)
        self.raise_for_status(res)
