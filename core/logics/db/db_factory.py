#!/usr/bin/env python
# encoding: utf-8
from core import config
from core.exceptions.regression_related_exception import *
from core.logics.db.dbs.mysql_db import MySqlDB
from core.logics.db.dbs.oracle_db import OracleDB
from core.logics.db.dbs.sql_server_db import SqlServerDB
from core.logics.db.dbs.postgresql_db import PostgreSqlDB
from core.logics.db.dbs.db2_db import DB2DB
from core.logics.db.dbs.tidb_db import TidbDB
from core.logics.db.dbs.opengauss_db import OpenGaussDB
from core.logics.db.dbs.redshift_db import RedshiftDB
from core.logics.db.dbs.greenplum_db import GreenPlumDB
from core.logics.db.dbs.kafka_db import KafkaDB, KafkaType
from core.logics.db.dbs.hive_db import HiveDB, HiveFileType
from core.logics.db.dbs.ftp_db import FtpDB, FtpFileType
from core.logics.db.dbs.hdfs_db import HDFSDB, HDFSFileType
from core.logics.db.dbs.redis_db import RedisDB
from core.logics.db.dbs.inceptor_db import InceptorDB, InceptorFileType
from core.logics.db.dbs.hashdata_db import HashDataDB
from core.logics.db.dbs.sequoia_db import SequoiaDB


def _get_is_source_or_sink(source_or_sink):
    return str(source_or_sink).lower() == 'source'


def create_db_instance_with_configuration(db_name: str, table_name: str, configuration: dict, **kwargs):
    """
    简单工厂，基于数据库名生成对应数据库操作对象
    对于已存在于工厂中的数据库的特殊修改，可添加新的别名用于生产该对象
    :param db_name: DB名
    :param table_name: 测试表名
    :param configuration: 连接所需的配置信息
    :return: DBDriver子类实例
    """
    # mysql
    if db_name.lower() == 'mysql':
        return MySqlDB(configuration, table_name)

    # oracle
    if db_name.lower() == 'oracle':
        return OracleDB(configuration, table_name)

    # sql_server
    if db_name.lower() == 'sqlserver':
        return SqlServerDB(configuration, table_name)

    # postgresql
    if db_name.lower() == 'postgresql':
        return PostgreSqlDB(configuration, table_name)

    # DB2
    if db_name.lower() == 'db2':
        return DB2DB(configuration, table_name)

    # tidb
    if db_name.lower() == 'tidb':
        return TidbDB(configuration, table_name)

    # opengauss
    if db_name.lower() == 'opengauss':
        return OpenGaussDB(configuration, table_name)

    # redshift
    if db_name.lower() == 'redshift':
        return RedshiftDB(configuration, table_name)

    # greenplum
    if db_name.lower() == 'greenplum':
        return GreenPlumDB(configuration, table_name)

    # hashdata
    if db_name.lower() == 'hashdata':
        return HashDataDB(configuration, table_name)

    # ftp
    if db_name.lower() == 'ftp_csv':
        return FtpDB(configuration, table_name, FtpFileType.csv)
    if db_name.lower() == 'ftp_json':
        return FtpDB(configuration, table_name, FtpFileType.json)

    # kafka
    if db_name.lower() == 'kafka_json':
        return KafkaDB(configuration, table_name, KafkaType.json)
    if db_name.lower() == 'kafka_avro':
        return KafkaDB(configuration, table_name, KafkaType.avro)

    # hdfs
    if db_name.lower() == 'hdfs_csv':
        return HDFSDB(configuration, table_name, HDFSFileType.csv)
    if db_name.lower() == 'hdfs_avro':
        return HDFSDB(configuration, table_name, HDFSFileType.avro)

    # hive
    if db_name.lower() == 'hive_csv':
        return HiveDB(configuration, table_name, HiveFileType.csv)
    if db_name.lower() == 'hive_avro':
        return HiveDB(configuration, table_name, HiveFileType.avro)
    if db_name.lower() == 'hive_parquet':
        return HiveDB(configuration, table_name, HiveFileType.parquet)
    if db_name.lower() == 'hive_orc':
        return HiveDB(configuration, table_name, HiveFileType.orc)

    # inceptor
    if db_name.lower() == 'inceptor_csv':
        return InceptorDB(configuration, table_name, InceptorFileType.csv)
    if db_name.lower() == 'inceptor_avro':
        return InceptorDB(configuration, table_name, InceptorFileType.avro)
    if db_name.lower() == 'inceptor_parquet':
        return InceptorDB(configuration, table_name, InceptorFileType.parquet)
    if db_name.lower() == 'inceptor_orc':
        return InceptorDB(configuration, table_name, InceptorFileType.orc)

    # redis
    if db_name.lower() == 'redis':
        return RedisDB(configuration, table_name)

    # sequoiadb
    if db_name.lower() == 'sequoiadb':
        return SequoiaDB(configuration, table_name)

    raise DBNotSupportedException('该数据库暂暂不支持，数据库：{0}'.format(db_name))


def create_db_instance(db_name: str, table_name: str, is_source: bool, **kwargs):
    """
    简单工厂，基于数据库名生成对应数据库操作对象
    对于已存在于工厂中的数据库的特殊修改，可添加新的别名用于生产该对象
    :param db_name: DB名
    :param table_name: 测试表名
    :param is_source: 源端表或目的端表
    :return: DBDriver子类实例
    """
    # mysql
    if db_name.lower() == 'mysql' and is_source:
        return MySqlDB(config.MYSQL_SOURCE, table_name)
    if db_name.lower() == 'mysql' and not is_source:
        return MySqlDB(config.MYSQL_SINK, table_name)

    # oracle
    if db_name.lower() == 'oracle' and is_source:
        return OracleDB(config.ORACLE_SOURCE, table_name)
    if db_name.lower() == 'oracle' and not is_source:
        return OracleDB(config.ORACLE_SINK, table_name)

    # sql_server
    if db_name.lower() == 'sqlserver' and is_source:
        return SqlServerDB(config.SQL_SERVER_SOURCE, table_name)
    if db_name.lower() == 'sqlserver' and not is_source:
        return SqlServerDB(config.SQL_SERVER_SINK, table_name)

    # postgresql
    if db_name.lower() == 'postgresql' and is_source:
        return PostgreSqlDB(config.POSTGRESQL_SOURCE, table_name)
    if db_name.lower() == 'postgresql' and not is_source:
        return PostgreSqlDB(config.POSTGRESQL_SINK, table_name)

    # DB2
    if db_name.lower() == 'db2' and is_source:
        return DB2DB(config.DB2_SOURCE, table_name)
    if db_name.lower() == 'db2' and not is_source:
        return DB2DB(config.DB2_SINK, table_name)

    # tidb
    if db_name.lower() == 'tidb' and is_source:
        return TidbDB(config.TiDB_SOURCE, table_name)
    if db_name.lower() == 'tidb' and not is_source:
        return TidbDB(config.TiDB_SINK, table_name)

    # opengauss
    if db_name.lower() == 'opengauss' and is_source:
        raise DBCouldNotUseAsSourceException('OpenGauss不允许作为源端数据库')
    if db_name.lower() == 'opengauss' and not is_source:
        return OpenGaussDB(config.OPENGAUSS_SINK, table_name)

    # redshift
    if db_name.lower() == 'redshift' and is_source:
        raise DBCouldNotUseAsSourceException('Redshift不允许作为源端数据库')
    if db_name.lower() == 'redshift' and not is_source:
        return RedshiftDB(config.REDSHIFT_SINK, table_name)

    # greenplum
    if db_name.lower() == 'greenplum' and is_source:
        raise DBCouldNotUseAsSourceException('GreenPlum不允许作为源端数据库')
    if db_name.lower() == 'greenplum' and not is_source:
        return GreenPlumDB(config.GREENPLUM_SINK, table_name)

    # hashdata
    if db_name.lower() == 'hashdata' and is_source:
        return HashDataDB(config.HASHDATA_SOURCE, table_name)
    if db_name.lower() == 'hashdata' and not is_source:
        raise DBCouldNotUseAsSourceException('HashData不允许作为目标数据库')

    # ftp
    if db_name.lower() == 'ftp_csv' and is_source:
        return FtpDB(config.FTP_SOURCE, table_name, FtpFileType.csv)
    if db_name.lower() == 'ftp_csv' and not is_source:
        return FtpDB(config.FTP_SINK, table_name, FtpFileType.csv)
    if db_name.lower() == 'ftp_json' and is_source:
        return FtpDB(config.FTP_SOURCE, table_name, FtpFileType.json)
    if db_name.lower() == 'ftp_json' and not is_source:
        return FtpDB(config.FTP_SINK, table_name, FtpFileType.json)

    # kafka
    if db_name.lower() == 'kafka_json' and is_source:
        return KafkaDB(config.KAFKA_SOURCE, table_name, KafkaType.json)
    if db_name.lower() == 'kafka_json' and not is_source:
        return KafkaDB(config.KAFKA_SINK, table_name, KafkaType.json)
    if db_name.lower() == 'kafka_avro' and is_source:
        return KafkaDB(config.KAFKA_SOURCE, table_name, KafkaType.avro)
    if db_name.lower() == 'kafka_avro' and not is_source:
        return KafkaDB(config.KAFKA_SINK, table_name, KafkaType.avro)

    # hdfs
    if db_name.lower() == 'hdfs_csv' and is_source:
        return HDFSDB(config.HDFS_SOURCE, table_name, HDFSFileType.csv)
    if db_name.lower() == 'hdfs_csv' and not is_source:
        return HDFSDB(config.HDFS_SINK, table_name, HDFSFileType.csv)
    if db_name.lower() == 'hdfs_avro' and is_source:
        return HDFSDB(config.HDFS_SOURCE, table_name, HDFSFileType.avro)
    if db_name.lower() == 'hdfs_avro' and not is_source:
        return HDFSDB(config.HDFS_SINK, table_name, HDFSFileType.avro)

    # hive
    if db_name.lower() == 'hive_csv' and is_source:
        return HiveDB(config.HIVE_SOURCE, table_name, HiveFileType.csv)
    if db_name.lower() == 'hive_csv' and not is_source:
        return HiveDB(config.HIVE_SINK, table_name, HiveFileType.csv)
    if db_name.lower() == 'hive_avro' and is_source:
        return HiveDB(config.HIVE_SOURCE, table_name, HiveFileType.avro)
    if db_name.lower() == 'hive_avro' and not is_source:
        return HiveDB(config.HIVE_SINK, table_name, HiveFileType.avro)
    if db_name.lower() == 'hive_parquet' and is_source:
        return HiveDB(config.HIVE_SOURCE, table_name, HiveFileType.parquet)
    if db_name.lower() == 'hive_parquet' and not is_source:
        return HiveDB(config.HIVE_SINK, table_name, HiveFileType.parquet)
    if db_name.lower() == 'hive_orc' and is_source:
        return HiveDB(config.HIVE_SOURCE, table_name, HiveFileType.orc)
    if db_name.lower() == 'hive_orc' and not is_source:
        return HiveDB(config.HIVE_SINK, table_name, HiveFileType.orc)

    # inceptor
    if db_name.lower() == 'inceptor_csv' and is_source:
        return InceptorDB(config.INCEPTOR_SOURCE, table_name, InceptorFileType.csv)
    if db_name.lower() == 'inceptor_csv' and not is_source:
        return InceptorDB(config.INCEPTOR_SINK, table_name, InceptorFileType.csv)
    if db_name.lower() == 'inceptor_avro' and is_source:
        return InceptorDB(config.INCEPTOR_SOURCE, table_name, InceptorFileType.avro)
    if db_name.lower() == 'inceptor_avro' and not is_source:
        return InceptorDB(config.INCEPTOR_SINK, table_name, InceptorFileType.avro)
    if db_name.lower() == 'inceptor_parquet' and is_source:
        return InceptorDB(config.INCEPTOR_SOURCE, table_name, InceptorFileType.parquet)
    if db_name.lower() == 'inceptor_parquet' and not is_source:
        return InceptorDB(config.INCEPTOR_SINK, table_name, InceptorFileType.parquet)
    if db_name.lower() == 'inceptor_orc' and is_source:
        return InceptorDB(config.INCEPTOR_SOURCE, table_name, InceptorFileType.orc)
    if db_name.lower() == 'inceptor_orc' and not is_source:
        return InceptorDB(config.INCEPTOR_SINK, table_name, InceptorFileType.orc)

    # redis
    if db_name.lower() == 'redis' and is_source:
        raise DBCouldNotUseAsSourceException('Redis不允许作为源端数据库')
    if db_name.lower() == 'redis' and not is_source:
        return RedisDB(config.REDIS_SINK, table_name)

    # sequoiadb
    if db_name.lower() == 'sequoiadb' and is_source:
        raise DBCouldNotUseAsSourceException('SequoiaDB不允许作为源端数据库')
    if db_name.lower() == 'sequoiadb' and not is_source:
        return SequoiaDB(config.SEQUOIADB_SINK, table_name)

    raise DBNotSupportedException('该数据库暂暂不支持，数据库：{0}'.format(db_name))
