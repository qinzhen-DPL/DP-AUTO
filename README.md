# DataPipeline Test Automation Framework 

### 集成UI、API、DB、MQ的测试自动化框架

- **一体化**
	- 使用一套框架完成对于UI、API、DB、MQ测试脚本的编写以及验证
	
- **易于使用，扩展性强**  
	- 对于初学者：数据驱动模式开发，快速编排测试计划  
	- 对于测开工程师：基于开源框架/工具，提供友好、易懂的API语法，毫不费力构建测试脚本  
	- 对于测试专家：无限测试功能、类库扩展
  
- **轻松地与现有系统集成**  
	- 内置的CLI可以轻松快速地与现有系统集成

## 组件及别名介绍

- ***DP:*** 核心自动化测试框架   
- ***Runner:*** 命令行（CLI）程序  
- ***YAML:*** 测试计划

## 技术栈

- Python 3.7  
- unittest: 单元测试（ATDD）驱动框架
- selenium: 桌面自动化底层库   
- request/urllib3: 接口自动化调用库    
- ParametrizedTestCase/openpyxl: 数据驱动模块
- HTMLTestReport: 测试运行器及报告模块
- yaml: 测试计划存储文件类型
- PyMySQL/psycopg2/cx-Oracle/pymssql/ibm-db: 关系型数据库操作库
- redis/redis-py-cluster: redis操作库
- kafka-python/confluent-kafka: kakfa, kafka-avro操作库
- PyHive/thrift/sasl: hive/inceptor操作库
- hdfs: hdfs操作库
- SDB.jar: sequoiadb操作库

## 文件夹结构介绍

- ***assets:*** 测试资产文件，包括：测试计划文件，测试数据文件，等其他数据文件  
- ***bin:*** 可执行/二进制文件
- ***config:*** 框架及脚本配置文件
- ***core:*** DP + Runner，框架核心
- ***doc:*** MarkDown文档
- ***lib:*** 三方库及编译后文件
- ***report:*** 测试报告

## 系统检查及运行手册

	# 检查python  
	doctor.bat  
	  
	# 检查并安装python依赖文件  
	install.bat  
	  
	# 检查CLI程序，并查看帮助文档  
	help.bat
	  
	# 按照测试计划执行脚本
	py -3 runner.py -p 测试计划名称（多个之间加空格）
	# 按照测试脚本执行脚本
	py -3 runner.py -c 测试脚本名称（多个之间加空格）

## 下一步
- [配置开发环境](docs/setup_development_environment.md)
- [系统设置和配置](docs/settings_and_configurations.md)
- [对象模块](docs/object_module.md)
- [测试脚本/测试套件/测试计划](docs/test_case_suite_plan.md)
- [命令行程序](docs/runner_cli.md)
- [测试报告](docs/reporting.md)
- [结果通知](docs/notification.md)
