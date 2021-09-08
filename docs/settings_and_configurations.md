# 系统设置和配置

在运行任何测试脚本之前，需要更新 **"2个"** 配置文件

- core/settings.py
- core/config.py

## Core - Settings

此配置文件为自动化测试框架相关的配置信息，下面介绍一些主要的系统配置

### DataPipeline 框架相关配置信息

|配置项|类型/默认值|描述|
|---|---|---|
|REPORT|str: ${CWD}/report|测试报告存储的根目录，使用${CWD}/to/report/path, 表示当前工作空间为基准的路径|
|ASSETS|str: ${CWD}/assets|测试资产文件的根目录，使用${CWD}/to/report/path, 表示当前工作空间为基准的路径|
|WAIT_INTERVAL|int: 1|轮询等待的最小间隔时间|
|DB_SYNC_WAIT_TIMEOUT|int: 90|等待数据库数据同步超时时间|

### 邮件通知相关配置信息

Email notifier related, only support exchange

|配置项|类型|
|---|---|
|EMAIL_NOTIFIER_ACCOUNT|str|
|EMAIL_NOTIFIER_USERNAME|str|
|EMAIL_NOTIFIER_PASSWORD|str|
|EMAIL_TO_LIST|list|
|EMAIL_CC_LIST|list|
|EMAIL_BCC_LIST|list|

## Core - Config

此配置文件为自动化测试相关的配置信息，下面介绍一些主要的系统配置

|配置项|类型|描述|
|---|---|---|
|API_BASE_URL|str|DataPipeline产品URL|
|USERNAME|str|登录用户名|
|PASSWORD|str|登录密码|
