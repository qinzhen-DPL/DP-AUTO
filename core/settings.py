#!/usr/bin/env python
# encoding: utf-8
"""
DataPipeline 框架相关配置信息
"""

# 使用${CWD}/to/report/path, 表示当前工作空间为基准的路径
REPORT = '${CWD}/report'
ASSETS = '${CWD}/assets'

# 邮件相关信息
EMAIL_NOTIFIER_ACCOUNT = 'sj89118@126.com'
EMAIL_NOTIFIER_USERNAME = 'sj89118@126.com'
EMAIL_NOTIFIER_PASSWORD = 'SJsj89118'
EMAIL_TO_LIST = ['jie.sheng@dilatoit.com']
EMAIL_CC_LIST = []
EMAIL_BCC_LIST = []
SMTP_SERVER = 'smtp.126.com'
SMTP_PORT = 25

# 系统等待间隔时间
WAIT_INTERVAL = 1

# 等待数据库数据同步超时时间
DB_SYNC_WAIT_TIMEOUT = 90
