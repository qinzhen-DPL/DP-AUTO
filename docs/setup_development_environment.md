# 配置开发环境

## 使用虚拟python环境

	python -m venv ./venv  
	source venv/bin/activate

## 使用本地python环境

请确保已安装Python3，并运行下面的命令来检查python是否已经在当前环境中

	py -3 -version
	
## 安装python依赖

	pip install -r requirements.txt

## 冻结python依赖

如果需要添加更多的pip依赖，请确保将它们添加到requirements.txt文件中

	pip freeze > requirements.txt


## pyhive安装说明
linux系统：需要提前安装gcc

	yum install gcc
	
	pip install -r requirements.txt
	
windows系统：

	pip install sasl-0.2.1-cp37-cp37m-win_amd64.whl
	
	pip install -r requirements.txt
	
	
## Oracle Client libraries安装说明

### linux系统：
下载Oracle Instant Client，选择Basic Package (ZIP)版本下载
- [x86-64 64-bit](https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html)
- [x86 32-bit](https://www.oracle.com/database/technologies/instant-client/linux-x86-32-downloads.html)
- [ARM (aarch64) 64-bit](https://www.oracle.com/database/technologies/instant-client/linux-arm-aarch64-downloads.html)

解压成功后，运行：

	sudo yum install libaio
	
安装成功后，在导入cx_Oracle代码位置修改instantclient路径配置

	import cx_Oracle
    cx_Oracle.init_oracle_client(config_dir="/home/your_username/oracle/your_config_dir")
或将以上路径配置在系统环境变量中
	
### windows系统：
下载Oracle Instant Client，选择Basic Package (ZIP)版本下载
- [x86-64 64-bit](https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html)
- [x86 32-bit](https://www.oracle.com/database/technologies/instant-client/linux-x86-32-downloads.html)

解压成功后，在导入cx_Oracle代码位置修改instantclient路径配置

	import cx_Oracle
    cx_Oracle.init_oracle_client(lib_dir=r"C:\oracle\instantclient_19_11")
或将以上路径配置在系统环境变量中

	
	
### Mac系统：
下载Oracle Instant Client，[Basic 64-bit DMG](https://www.oracle.com/database/technologies/instant-client/macos-intel-x86-downloads.html)
双击DMG加载成功后，运行：

	/Volumes/instantclient-basic-macos.x64-19.8.0.0.0dbru/install_ic.sh

运行成功后，在导入cx_Oracle代码位置修改instantclient路径配置

    import cx_Oracle
    cx_Oracle.init_oracle_client(lib_dir="/Users/your_username/Downloads/instantclient_19_8")
    
    
## ibm-db离线安装
如因网络问题无法正常下载安装ibm-db，可通过以下方式进行离线安装：

下载依赖库[macos64_odbc_cli.tar.gz](https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/macos64_odbc_cli.tar.gz)
下载ibm-db库[ibm_db-3.0.2.tar.gz](https://pypi.org/project/ibm-db/#files)

解压ibm-db库到文件夹…/ibm_db-3.0.2
解压依赖库到上述文件夹…/ibm_db-3.0.2，此处一定要注意license不要替换

在终端进入对应的文件夹并输入命令：

    python setup.py install
    
## 运行机host修改
运行测试前需将本地host映射添加一下信息：

- 182.92.3.156	shengjie            # kafka
- 47.94.4.151	hdfs                    # hdfs 
- 47.94.4.151 iz2zeaejd6v32vcy01u8l8z # hadoop-web
- 123.57.195.172	tdh01
- 123.57.194.97	tdh02
- 47.93.225.227	tdh03
- 123.57.60.144	cdh01
- 182.92.68.157	cdh02
- 101.200.125.43	cdh03