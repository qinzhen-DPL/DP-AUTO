#!/usr/bin/env python
# encoding: utf-8
import re
import datetime
from core.logics.api.apis.node_api import NodeType
from core.exceptions.regression_related_exception import DBNotSupportedException


def naming_now():
    """
    基于现在时间生成缩写
    """
    now = datetime.datetime.now()
    return now.strftime('%y%m%d%H%M%S')


def naming_db(db_type: str):
    """
    基于数据库类型生成缩写
    """
    if db_type.lower() in ['mysql']:
        node_name = 'mysql'
        node_type = NodeType.mysql
    elif db_type.lower() in ['oracle', 'orcl']:
        node_name = 'orcl'
        node_type = NodeType.oracle
    elif db_type.lower() in ['sqlserver', 'sql_server', 'sql server', 'sql']:
        node_name = 'sql'
        node_type = NodeType.sql_server
    elif db_type.lower() in ['postgresql', 'postgres', 'pg']:
        node_name = 'pg'
        node_type = NodeType.postgresql
    elif db_type.lower() in ['db2']:
        node_name = 'db2'
        node_type = NodeType.db2
    elif db_type.lower() in ['redshift', 'resh']:
        node_name = 'resh'
        node_type = NodeType.redshift
    elif db_type.lower() in ['greenplum', 'gp']:
        node_name = 'gp'
        node_type = NodeType.greenplum
    elif db_type.lower() in ['tidb']:
        node_name = 'tidb'
        node_type = NodeType.tidb
    elif db_type.lower() in ['hashdata']:
        node_name = 'hash'
        node_type = NodeType.hasddata
    elif db_type.lower() in ['opengauss', 'gauss']:
        node_name = 'gauss'
        node_type = NodeType.opengauss
    elif db_type.lower() in ['sequoiadb', 'sqdb']:
        node_name = 'sqdb'
        node_type = NodeType.sequoiadb
    elif db_type.lower() in ['inceptor', 'inpt', 'inceptor_csv', 'inceptor_avro', 'inceptor_parquet', 'inceptor_orc',
                             'inpt_csv', 'inpt_avro', 'inpt_parquet', 'inpt_orc']:
        node_name = 'inpt'
        node_type = NodeType.inceptor
    elif db_type.lower() in ['kafka', 'kafka_json', 'kafka_avro']:
        node_name = 'kafka'
        node_type = NodeType.kafka
    elif db_type.lower() in ['hive', 'hive_csv', 'hive_avro', 'hive_parquet', 'hive_orc']:
        node_name = 'hive'
        node_type = NodeType.hive
    elif db_type.lower() in ['ftp', 'ftp_csv', 'ftp_json']:
        node_name = 'ftp'
        node_type = NodeType.ftp
    elif db_type.lower() in ['hdfs', 'hdfs_csv', 'hdfs_avro']:
        node_name = 'hdfs'
        node_type = NodeType.hdfs
    elif db_type.lower() in ['redis']:
        node_name = 'redis'
        node_type = NodeType.redis
    else:
        raise DBNotSupportedException('该数据库系统现不支持，数据库名：{0}'.format(db_type))
    return node_name, node_type


def naming_node(db_type: str, is_source: bool, suffix=None):
    """
    基于节点类型，和后缀信息生成缩写
    """
    naming = '{db}_{source_or_sink}{suffix}'
    node_name, node_type = naming_db(db_type)
    source_or_sink = 'source' if is_source else 'sink'
    suffix_str = '_' + suffix if suffix else ''
    naming = naming.format(db=node_name, source_or_sink=source_or_sink, suffix=suffix_str)
    print('节点名称：{0}'.format(naming))
    return naming, node_type


def naming_link_and_task(source_db_type: str, sink_db_type: str, is_real, now):
    """
    基于源端、目的端数据库类型、测试类型生成缩写
    """
    naming = '{source_db}_{sink_db}_{task_type}_{now}'
    source_node_name, source_node_type = naming_db(source_db_type)
    sink_node_name, sink_node_type = naming_db(sink_db_type)
    task_type = 'real' if is_real else 'cron'
    naming = naming.format(source_db=source_node_name, sink_db=sink_node_name, task_type=task_type, now=now)
    print('线路任务名称：{0}'.format(naming))
    return naming


def naming_sink_table(table_name: str, source_db_type: str, sink_db_type: str, is_real, now):
    """
    基于表名、源端、目的端数据库类型、测试类型生成缩写
    """
    naming = '{table_name}_{source_db}_{sink_db}_{task_type}_{now}'
    table_name = table_name.lower()
    search_result = re.search(r'src_(.*)_(.*)', table_name)
    if table_name.find('all_types_columns') != -1:
        table_name = 'altype'
    elif table_name.find('drop') != -1:
        table_name = 'drop'
    elif table_name.find('fk_table') != -1:
        table_name = 'fktab'
    elif table_name.find('long_columns') != -1:
        table_name = 'long'
    elif table_name.find('nokey_table') != -1:
        table_name = 'nokey'
    elif table_name.find('pk_table') != -1:
        table_name = 'pktab'
    elif table_name.find('reserved_columns') != -1:
        table_name = 'resvd'
    elif table_name.find('special_columns') != -1:
        table_name = 'spec'
    elif table_name.find('upk_table') != -1:
        table_name = 'upk'
    elif table_name.find('z_view') != -1:
        table_name = 'view'
    elif search_result:
        table_name = search_result.group(1)
    source_node_name, source_node_type = naming_db(source_db_type)
    sink_node_name, sink_node_type = naming_db(sink_db_type)
    task_type = 'real' if is_real else 'cron'
    naming = naming.format(table_name=table_name, source_db=source_node_name, sink_db=sink_node_name,
                           task_type=task_type, now=now)
    print('下游表名称：{0}'.format(naming))
    return naming


def naming_source_table(table_name: str, now):
    """
    随机生成上游测试表名称
    """
    naming = 'src_{table_name}_{now}'
    table_name = table_name.lower()
    naming = naming.format(table_name=table_name, now=now)
    print('上游表名称：{0}'.format(naming))
    return naming
