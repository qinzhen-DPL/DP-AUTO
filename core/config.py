#!/usr/bin/env python
# encoding: utf-8
"""
DataPipeline 测试相关配置信息
"""

API_BASE_URL = 'http://59.110.14.109'
USERNAME = 'admin'
PASSWORD = '123456'

# mysql
MYSQL_SOURCE = {
        'type': 'MYSQL',
        'version': 'MYSQL_5_7',
        'host': '192.168.1.217',
        'ip': '60.205.183.90',
        'username': 'autodb',
        'password': '123456',
        'database': 'autodb',
        'port': 3306
    }
MYSQL_SINK = {
        'type': 'MYSQL',
        'version': 'MYSQL_5_7',
        'host': '192.168.1.238',
        'ip': '47.95.195.197',
        'username': 'autodb',
        'password': '123456',
        'database': 'autodb',
        'port': 3306
    }

# sql server
SQL_SERVER_SOURCE = {
        'type': 'SQL_SERVER',
        'version': 'SQL_SERVER_2012',
        'host': '192.168.1.216',
        'ip': '60.205.186.83',
        'username': 'dp_auto',
        'password': '123456',
        'database': 'dp_auto',
        'schema': 'dbo',
        'port': 1433
    }
SQL_SERVER_SINK = {
        'type': 'SQL_SERVER',
        'version': 'SQL_SERVER_2012',
        'host': '192.168.1.29',
        'ip': '47.94.108.241',
        'username': 'dp_auto',
        'password': '123456',
        'database': 'dp_auto',
        'schema': 'dbo',
        'port': 1433
    }

# oracle
ORACLE_SOURCE = {
        'type': 'ORACLE',
        'version': 'ORACLE_10G',
        'host': '192.168.1.53',
        'ip': '47.94.83.111',
        'username': 'AAUTO_DB',
        'password': '123456',
        'database': 'orcl',
        'schema': 'AAUTO_DB',
        'port': 1521
    }
ORACLE_SINK = {
        'type': 'ORACLE',
        'version': 'ORACLE_10G',
        'host': '192.168.1.17',
        'ip': '39.107.93.243',
        'username': 'DP_AUTO',
        'password': '123456',
        'database': 'orcl',
        'schema': 'DP_AUTO',
        'port': 1521
    }

# PostgreSql server
POSTGRESQL_SOURCE = {
        'type': 'POSTGRES',
        'version': 'POSTGRES_10_4',
        'host': '192.168.1.23',
        'ip': '47.94.107.125',
        'username': 'autodb',
        'password': '123456',
        'database': 'autodb',
        'schema': 'public',
        'port': 5432
    }
POSTGRESQL_SINK = {
        'type': 'POSTGRES',
        'version': 'POSTGRES_10_4',
        'host': '192.168.1.20',
        'ip': '182.92.174.102',
        'username': 'dp_sink',
        'password': '123456',
        'database': 'dp_sink',
        'schema': 'public',
        'port': 5411
    }

# DB2
DB2_SOURCE = {
        'type': 'DB2',
        'version': 'DB2_11',
        'host': '192.168.1.86',
        'ip': '123.57.152.71',
        "access_username": "admin",
        "sourceHost": "192.168.1.86",
        "sourcePort": "10901",
        "sourceUser": "DB2INST1",
        "sourceDb": "DP_TEST",
        "access_password": "123456",
        "sourcePass": "123456",
        "targetHost": "192.168.1.86",
        "targetPort": "11701",
        "zkHost": "shengjie",
        "zkPort": "8889",
        "targetPass": "123456",
        "kafkaHosts": [
                "shengjie:9999"
        ],
        "schemaRegistryHost": "shengjie",
        "schemaRegistryPort": 8081,
        'username': 'DB2INST1',
        'password': '123456',
        'database': 'DP_TEST',
        'schema': 'FJ',
        'port': 10101,
        'jdbc-port': 50000
    }

DB2_SINK = {
        'type': 'DB2',
        'version': 'DB2_11',
        'host': '192.168.1.86',
        'ip': '123.57.152.71',
        'username': 'DB2INST1',
        'password': '123456',
        'database': 'DP_TEST',
        'schema': 'FJ',
        'jdbc-port': 50000
    }


# TiDB
TiDB_SOURCE = {
        'type': 'TIDB',
        'version': 'TIDB_2',
        'host': '192.168.1.41',
        'ip': '39.106.214.41',
        'username': 'dp_test',
        'password': '123456',
        'database': 'test',
        'port': 4000
    }
TiDB_SINK = {
        'type': 'TIDB',
        'version': 'TIDB_2',
        'host': '192.168.1.67',
        'ip': '39.106.103.109',
        'username': 'dp_auto',
        'password': '123456',
        'database': 'dp_auto',
        'port': 4000
    }

# HashData
HASHDATA_SOURCE = {
        'type': 'HASHDATA',
        'version': 'HASHDATA_2_5_X',
        'host': '192.168.1.47',
        'ip': '182.92.73.147',
        'username': 'dp_test',
        'password': '123456',
        'database': 'dp_test',
        'schema': 'public',
        'port': 5432
    }

# OpenGauss
OPENGAUSS_SINK = {
        'type': 'OPENGAUSS',
        'version': 'OPENGAUSS_1',
        'host': '192.168.1.59',
        'ip': '123.56.162.91',
        'username': 'dp_test',
        'password': 'Datapipeline123',
        'database': 'dp_test',
        'schema': 'dp_test',
        'port': 15400
    }

# Redshift
REDSHIFT_SINK = {
        'type': 'REDSHIFT',
        'version': 'AMAZON_REDSHIFT',
        'host': 'data-pipeline.cye55uthbqll.cn-north-1.redshift.amazonaws.com.cn',
        'username': 'datapipeline',
        'password': 'Datapipeline123',
        'database': 'dev',
        'schema': 'public',
        'port': 5439,
        's3_directory': 'dp_auto',
        's3_bucket': 'wangshaodata',
        's3_accessId': 'AKIAOWSRYE4XCKMOZU2A',
        's3_accessKey': 'wrgzpZqvdEeCQaYYZQjYX2UJWn8IWKdSfhgPR6er'
    }

# GreenPlum
GREENPLUM_SINK = {
        'type': 'GREENPLUM',
        'version': 'GREENPLUM_6',
        'host': '192.168.1.68',
        'ip': '47.93.239.84',
        'username': 'dp_auto',
        'password': '123456',
        'database': 'dp_auto',
        'schema': 'public',
        'port': 5432
    }

# Kafka
KAFKA_SOURCE = {
        'type': 'KAFKA',
        'version': 'KAFKA_1',
        'host': '192.168.1.56:9093',
        'schema_registry': 'http://192.168.1.56:8082'
    }

KAFKA_SINK = {
        'type': 'KAFKA',
        'version': 'KAFKA_1',
        'host': '192.168.1.56:9093',
        'schema_registry': 'http://192.168.1.56:8082'
    }

# FTP
FTP_SOURCE = {
        'type': 'FTP',
        'version': 'FTP_3_0_2',
        'host': '192.168.1.236',
        'ip': '39.106.20.66',
        'port': 21,
        'username': 'nftp',
        'password': 'datapipeline123',
        'root': '/source/auto',
    }

FTP_SINK = {
        'type': 'FTP',
        'version': 'FTP_3_0_2',
        'host': '192.168.1.236',
        'ip': '39.106.20.66',
        'port': 21,
        'username': 'nftp',
        'password': 'datapipeline123',
        'root': '/sink/auto',
    }

# Redis
REDIS_SINK = {
        'type': 'REDIS',
        'version': 'REDIS_6_0',
        'host': ["192.168.1.50:6379"],
        'port': 6379,
        'database': 0,
        'ip': '182.92.3.156'
    }

REDIS_CLUSTER_SINK = {
        'type': 'REDIS',
        'version': 'REDIS_5_0',
        'host': [
                "192.168.1.14:7000",
                "192.168.1.14:7001",
                "192.168.1.14:7002",
                "192.168.1.14:7003",
                "192.168.1.14:7004"],
        'port': 7000,
        'ip': '101.200.239.239',
        'startup_nodes': [
                {"host": "192.168.1.14", "port": "7000"},
                {"host": "192.168.1.14", "port": "7001"},
                {"host": "192.168.1.14", "port": "7002"},
                {"host": "192.168.1.14", "port": "7003"},
                {"host": "192.168.1.14", "port": "7004"}
        ],
        'password': '123456',
    }

# SequoiaDB
SEQUOIADB_SINK = {
        'type': 'SEQUOIA_DB',
        'version': 'SEQUOIA_DB_2_8_7',
        'host': '192.168.1.15:11810',
        'ip': '47.95.13.83',
        'port': 11810,
        'username': 'dp_test',
        'password': '123456',
        'database': 'dp_auto',
}

# Hive
HIVE_SOURCE = {
        'type': 'HIVE',
        'version': 'HIVE_2',
        'ip': '101.200.125.43',
        'port': 10000,
        'root': '/test',
        'username': 'hdfs',
        'database': 'fengjun',
        'config_file': ['hive/core-site.xml', 'hive/hdfs-site.xml', 'hive/hive-site.xml'],
        'mysql_ip': 'cdh01',
        'mysql_username': 'root',
        'mysql_password': '123456',
        'mysql_port': 3306,
        'mysql_database': 'hive'
    }

HIVE_SINK = {
        'type': 'HIVE',
        'version': 'HIVE_2',
        'ip': '101.200.125.43',
        'port': 10000,
        'root': '/test',
        'username': 'hdfs',
        'database': 'fengjun',
        'config_file': ['hive/core-site.xml', 'hive/hdfs-site.xml', 'hive/hive-site.xml'],
        'mysql_ip': 'cdh01',
        'mysql_username': 'root',
        'mysql_password': '123456',
        'mysql_port': 3306,
        'mysql_database': 'hive'
    }

# Inceptor
INCEPTOR_SOURCE = {
        'type': 'INCEPTOR',
        'version': 'INCEPTOR_6_2_2',
        'ip': '123.57.195.172',
        'port': 10000,
        'root': '/user/zyt',
        'hdfs_user': 'zyt',
        'username': 'zyt',
        'password': '123456',
        'database': 'default',
        'config_file': ['inceptor/core-site.xml', 'inceptor/hdfs-site.xml', 'inceptor/hive-site.xml'],
        'krb5_conf': 'inceptor/krb5.conf',
        'hive_keytab': 'inceptor/zyt.keytab',
        'principal': 'zyt@TDH'
    }

INCEPTOR_SINK = {
        'type': 'INCEPTOR',
        'version': 'INCEPTOR_6_2_2',
        'ip': '123.57.195.172',
        'port': 10000,
        'root': '/user/zyt',
        'hdfs_user': 'zyt',
        'username': 'zyt',
        'password': '123456',
        'database': 'default',
        'config_file': ['inceptor/core-site.xml', 'inceptor/hdfs-site.xml', 'inceptor/hive-site.xml'],
        'krb5_conf': 'inceptor/krb5.conf',
        'hive_keytab': 'inceptor/zyt.keytab',
        'principal': 'zyt@TDH'
    }

# HDFS
HDFS_SOURCE = {
        'type': 'HDFS',
        'version': 'HDFS_2_7',
        'hdfs_web': 'http://hdfs:50070',
        'root': '/user/zyt',
        'username': 'root',
        'config_file': ['hdfs/core-site.xml', 'hdfs/hdfs-site.xml']
    }

HDFS_SINK = {
        'type': 'HDFS',
        'version': 'HDFS_2_7',
        'hdfs_web': 'http://hdfs:50070',
        'root': '/data/hive/qinzhentest1',
        'username': 'root',
        'config_file': ['hdfs/core-site.xml', 'hdfs/hdfs-site.xml']
    }

DB_SUPPORTED_TABLES = ["All_types_columns", "DROP", "FK_table", "Long_columns", "Nokey_table", "PK_table",
                       "Reserved_columns", "Special_columns", "UPK_table", "Z_VIEW"]

ALL_TYPES_COLUMNS = ["id", "col1", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10", "col11", "col12", "col13", "col14", "col15", "col16", "col17", "col18", "col19", "col20", "col21", "col22", "col23", "col24", "col25", "col26", "col27", "col28", "col29", "col30", "col31", "col32", "col33", "col34", "col35", "col36", "col37", "col38", "col39", "col40", "col41"]
KAFKA_COLUMNS = ["key", "value"]

SEPARATOR = '~'
LINEEND = '`'
DEFAULT_SEPARATOR = ','
DEFAULT_LINEEND = '\n'

DEFAULT_LINE = '\\n'
DEFAULT_QUOTA = '\\"'
DEFAULT_ESCAPE = '\\\\'
