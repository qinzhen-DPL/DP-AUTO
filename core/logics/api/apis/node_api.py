#!/usr/bin/env python
# encoding: utf-8
import os
from core.logics.api.api_driver import *
from enum import Enum


class NodeType(Enum):
    mysql = 'create_mysql_node_data'
    sql_server = 'create_sql_server_node_data'
    oracle = 'create_oracle_node_data'
    postgresql = 'create_postgresql_node_data'
    db2 = 'create_db2_node_data'
    tidb = 'create_tidb_node_data'
    hasddata = 'create_hashdata_node_data'
    redshift = 'create_redshift_node_data'
    greenplum = 'create_greenplum_node_data'
    opengauss = 'create_opengauss_node_data'
    kafka = 'create_kafka_node_data'
    hive = 'create_hive_node_data'
    inceptor = 'create_inceptor_node_data'
    ftp = 'create_ftp_node_data'
    hdfs = 'create_hdfs_node_data'
    redis = 'create_redis_node_data'
    sequoiadb = 'create_sequoiadb_node_data'


class NodeApi(ApiDriver):

    path_dictionary = {
        'create_node': '/v3/data-nodes',
        'upload_file': '/v3/files?type={0}',
        'init_node': '/v3/data-nodes/{0}/init',
        'active_node': '/v3/data-nodes/{0}/active',
        'suspend_node': '/v3/data-nodes/{0}/suspend',
        'delete_node': '/v3/data-nodes/{0}',
        'list_node': '/v3/data-nodes/list',
        'node_entities': '/v3/entity/node/{0}/entities?refresh=true&includeDeleted=true',
        'create_node_entity': '/v3/entity',
        'node_info': '/v3/data-nodes/{0}?includeExtraConfig=true',
        'refresh_node': '/v3/entity/refresh?nodeId={0}'
    }

    data_dictionary = {
    }

    def __init__(self, session):
        ApiDriver.__init__(self, session)

    @api_step(step_name='上传文件')
    def update_file(self, file_path, file_type):
        print('上传文件: {0}, type: {1}'.format(file_path, file_type))
        url = self.combine_url(config.API_BASE_URL, self.url('upload_file').format(file_type))
        files = {'files': open(os.path.join('core', 'logics', 'api', 'config_file', file_path), 'rb')}
        res = self.post(url, files=files, verify=True)
        self.raise_for_status(res)
        file_id = res.json()['data'][0]['fileId']
        print('File id: {0}'.format(file_id))
        return file_id

    @api_step(step_name='上传节点文件')
    def update_node_file(self, file_path, file_type, node_id):
        print('上传文件: {0}, type: {1}'.format(file_path, file_type))
        url = self.combine_url(config.API_BASE_URL, self.url('upload_file').format(file_type))
        files = {'files': open(file_path, 'rb'), 'entityId': node_id, 'entityType': 'DATA_NODE'}
        res = self.post(url, files=files, verify=True)
        self.raise_for_status(res)
        file_id = res.json()['data'][0]['fileId']
        print('File id: {0}'.format(file_id))
        return file_id

    @api_step(step_name='创建节点')
    def create_node(self, node_name, node_type: NodeType, is_source: bool):
        print('Node name: {0}, type: {1}'.format(node_name, node_type.name))
        url = self.combine_url(config.API_BASE_URL, self.url('create_node'))
        data = getattr(NodeData, node_type.value)(is_source)
        data["name"] = node_name

        # 上传文件
        if node_type == node_type.hdfs:
            file_ids = []
            config_dict = config.HDFS_SOURCE if is_source else config.HDFS_SINK
            for config_file in config_dict['config_file']:
                file_ids.append(self.update_file(config_file, 'HDFS'))
            data["basicConfig"]['configFileIds'] = file_ids
        elif node_type == node_type.hive:
            file_ids = []
            config_dict = config.HIVE_SOURCE if is_source else config.HIVE_SINK
            for config_file in config_dict['config_file']:
                file_ids.append(self.update_file(config_file, 'HDFS'))
            data["basicConfig"]['hiveMetaStoreClientConfig']['configFileIds'] = file_ids
        elif node_type == node_type.inceptor:
            file_ids = []
            config_dict = config.INCEPTOR_SOURCE if is_source else config.INCEPTOR_SINK
            for config_file in config_dict['config_file']:
                file_ids.append(self.update_file(config_file, 'HDFS'))
            data["basicConfig"]['hiveMetaStoreClientConfig']['configFileIds'] = file_ids
            krb5_file_id = self.update_file(config_dict['krb5_conf'], 'HBASE')
            key_tab_file_id = self.update_file(config_dict['hive_keytab'], 'HDFS')
            data["basicConfig"]['hiveMetaStoreClientConfig']['securityConfig']['kerberos']['confFileId'] = krb5_file_id
            data["basicConfig"]['hiveMetaStoreClientConfig']['securityConfig']['kerberos']['keytabFileId'] = key_tab_file_id

        res = self.post(url, json=data, verify=True)
        self.raise_for_status(res)
        node_id = res.json()['data']['id']
        node_state = res.json()['data']['state']
        assert node_state == 'ACTIVE', '新建节点不为ACTIVE状态'
        print('Node name: {0}, ID: {1}, state: {2}'.format(node_name, node_id, node_state))

        return node_id

    @api_step(step_name='检查并初始化节点')
    def init_node(self, node_id, node_type, is_source):
        url = self.combine_url(config.API_BASE_URL, self.url('init_node').format(str(node_id)))
        # TODO: 考虑将初始化类型作为参数，目前将源端以目的端的初始化信息固定
        if node_type == NodeType.oracle or node_type == NodeType.tidb:
            url = url + '?modes=JDBC_READ&modes=JDBC_WRITE'
        elif node_type == NodeType.db2 and is_source:
            url = url + '?modes=IBM_CDC'
        elif node_type == NodeType.db2 and not is_source:
            url = url + '?modes=JDBC_READ&modes=JDBC_WRITE'
        elif node_type == NodeType.hive or node_type == NodeType.inceptor:
            url = url + '?modes=SCAN&modes=COPY'
        res = self.post(url, verify=True)
        self.raise_for_status(res)

    @api_step(step_name='刷新节点')
    def refresh_node(self, node_id):
        url = self.combine_url(config.API_BASE_URL, self.url('refresh_node').format(str(node_id)))
        res = self.post(url, verify=True)
        self.raise_for_status(res)

    @api_step(step_name='等待节点初始化成功')
    def wait_node_init_success(self, node_id, timeout=10):
        url = self.combine_url(config.API_BASE_URL, self.url('node_info').format(str(node_id)))

        class NodeInitSuccess:
            def __init__(self, instance):
                self.instance = instance

            def __call__(self):
                res = self.instance.get(url, verify=True)
                self.instance.raise_for_status(res)
                if '连接成功' not in res.json()['data']['connectState']:
                    print('节点连接验证结果失败，等待后将会再次检验')
                    return False
                else:
                    return True
        Wait(timeout).until(NodeInitSuccess(self),
                            "已等待{0}秒，node id: {1} 该节点连接验证结果仍失败".format(timeout, node_id))

    @api_step(step_name='激活节点')
    def active_node(self, node_id):
        url = self.combine_url(config.API_BASE_URL, self.url('active_node').format(str(node_id)))
        res = self.post(url, verify=True)
        self.raise_for_status(res)

    @api_step(step_name='暂停节点')
    def suspend_node(self, node_id):
        url = self.combine_url(config.API_BASE_URL, self.url('suspend_node').format(str(node_id)))
        res = self.post(url, verify=True)
        self.raise_for_status(res)

    @api_step(step_name='删除节点')
    def delete_node(self, node_id):
        url = self.combine_url(config.API_BASE_URL, self.url('delete_node').format(str(node_id)))
        res = self.delete(url, verify=True)
        self.raise_for_status(res)

    @api_step(step_name='获取节点全部实体对象')
    def get_node_entities(self, node_id):
        url = self.combine_url(config.API_BASE_URL, self.url('node_entities').format(str(node_id)))
        res = self.get(url, verify=True)
        self.raise_for_status(res)
        entities = res.json()['data']['items']
        return entities

    @api_step(step_name='创建节点实体对象')
    def create_node_entity(self, node_id, node_type: NodeType, file_type, table_name, headers):
        url = self.combine_url(config.API_BASE_URL, self.url('create_node_entity'))
        if node_type == node_type.ftp and file_type == 'csv':
            data = NodeData.create_ftp_entity_data()
            data['dataNodeEntities'][0]['nodeId'] = node_id
            data['dataNodeEntities'][0]['entityInfo']['name'] = table_name
            data['dataNodeEntities'][0]['entityBasicConfig']['fileConfig']['headers'] = headers
        elif node_type == node_type.hdfs and file_type == 'csv':
            data = NodeData.create_hdfs_entity_data()
            data['dataNodeEntities'][0]['nodeId'] = node_id
            data['dataNodeEntities'][0]['entityInfo']['name'] = table_name
            data['dataNodeEntities'][0]['entityBasicConfig']['fileConfig']['headers'] = headers
        elif node_type == node_type.ftp and file_type == 'json':
            data = NodeData.create_ftp_json_entity_data()
            data['dataNodeEntities'][0]['nodeId'] = node_id
            data['dataNodeEntities'][0]['entityInfo']['name'] = table_name
            data['dataNodeEntities'][0]['entityBasicConfig']['fileConfig']['sampleDataFileId'] = headers
        elif node_type == node_type.hdfs and file_type == 'avro':
            data = NodeData.create_hdfs_avro_entity_data()
            data['dataNodeEntities'][0]['nodeId'] = node_id
            data['dataNodeEntities'][0]['entityInfo']['name'] = table_name
            data['dataNodeEntities'][0]['entityBasicConfig']['fileConfig']['schemaDefinitionFileId'] = headers
        res = self.put(url, json=data, verify=True)
        self.raise_for_status(res)

    @api_step(step_name='更新Kakfa topic序列化类型')
    def update_kafka_topic_type(self, entity_id, node_id):
        url = self.combine_url(config.API_BASE_URL, self.url('create_node_entity'))
        data = NodeData.set_kafka_avro_entity_data(entity_id, node_id)
        res = self.put(url, json=data, verify=True)
        self.raise_for_status(res)

    @api_step(step_name='获取并筛选节点全部实体表')
    def get_node_existing_entity_tables(self, node_id, db_type: NodeType, is_source, supported_tables=None):
        """
        获取数据库所有有效的测试库
        :param node_id: 数据库节点id
        :param db_type: 数据库类型
        :param is_source: 源端或目的端
        :param supported_tables: 所有需要返回的表名，如果为空则返回所有config.DB_SUPPORTED_TABLES中存在的表
        :return: [(表名, 表id)]
        """
        supported_tables = supported_tables if supported_tables else config.DB_SUPPORTED_TABLES
        node_entities = self.get_node_entities(node_id)

        if db_type == NodeType.mysql:
            config_dict = config.MYSQL_SOURCE if is_source else config.MYSQL_SINK
            table_mapping = {entity['name']: entity['id'] for entity in node_entities
                             if entity['schema'] == config_dict['database']}
        elif db_type == NodeType.oracle:
            config_dict = config.ORACLE_SOURCE if is_source else config.ORACLE_SINK
            table_mapping = {entity['name']: entity['id'] for entity in node_entities
                             if entity['schema'] == config_dict['schema'] and
                             entity['database'] == config_dict['database']}
        elif db_type == NodeType.sql_server:
            config_dict = config.SQL_SERVER_SOURCE if is_source else config.SQL_SERVER_SINK
            table_mapping = {entity['name']: entity['id'] for entity in node_entities
                             if entity['schema'] == config_dict['schema'] and
                             entity['database'] == config_dict['database']}
        elif db_type == NodeType.postgresql:
            config_dict = config.POSTGRESQL_SOURCE if is_source else config.POSTGRESQL_SINK
            table_mapping = {entity['name']: entity['id'] for entity in node_entities
                             if entity['schema'] == config_dict['schema'] and
                             entity['database'] == config_dict['database']}
        elif db_type == NodeType.db2:
            config_dict = config.DB2_SOURCE if is_source else config.DB2_SINK
            table_mapping = {entity['name']: entity['id'] for entity in node_entities
                             if entity['schema'] == config_dict['schema'] and
                             entity['database'] == config_dict['database']}
        elif db_type == NodeType.tidb:
            config_dict = config.TiDB_SOURCE if is_source else config.TiDB_SINK
            table_mapping = {entity['name']: entity['id'] for entity in node_entities
                             if entity['schema'] == config_dict['database']}
        elif db_type == NodeType.hasddata:
            table_mapping = {entity['name']: entity['id'] for entity in node_entities
                             if entity['schema'] == config.HASHDATA_SOURCE['schema'] and
                             entity['database'] == config.HASHDATA_SOURCE['database']}
        elif db_type == NodeType.opengauss:
            table_mapping = {entity['name']: entity['id'] for entity in node_entities
                             if entity['schema'] == config.OPENGAUSS_SINK['schema'] and
                             entity['database'] == config.OPENGAUSS_SINK['database']}
        elif db_type == NodeType.redshift:
            table_mapping = {entity['name']: entity['id'] for entity in node_entities
                             if entity['schema'] == config.REDSHIFT_SINK['schema'] and
                             entity['database'] == config.REDSHIFT_SINK['database']}
        elif db_type == NodeType.greenplum:
            table_mapping = {entity['name']: entity['id'] for entity in node_entities
                             if entity['schema'] == config.GREENPLUM_SINK['schema'] and
                             entity['database'] == config.GREENPLUM_SINK['database']}
        elif db_type == NodeType.hive:
            config_dict = config.HIVE_SOURCE if is_source else config.HIVE_SINK
            table_mapping = {entity['name']: entity['id'] for entity in node_entities
                             if entity['database'] == config_dict['database']}
            return [[table, table_mapping[table]] for table in [x.lower() for x in supported_tables]
                    if table in table_mapping.keys()]
        elif db_type == NodeType.inceptor:
            config_dict = config.INCEPTOR_SOURCE if is_source else config.INCEPTOR_SINK
            table_mapping = {entity['name']: entity['id'] for entity in node_entities
                             if entity['database'] == config_dict['database']}
            return [[table, table_mapping[table]] for table in [x.lower() for x in supported_tables]
                    if table in table_mapping.keys()]
        elif db_type == NodeType.kafka or \
                db_type == NodeType.ftp or \
                db_type == NodeType.hdfs:
            table_mapping = {entity['name']: entity['id'] for entity in node_entities}
        elif db_type == NodeType.redis:
            return [[entity['name'], entity['id']] for entity in node_entities]
        elif db_type == NodeType.sequoiadb:
            table_mapping = {entity['name']: entity['id'] for entity in node_entities
                             if entity['database'] == config.SEQUOIADB_SINK['database']}
        else:
            raise DBNotSupportedException('该数据库不支持，数据库：{0}'.format(db_type.name))

        return [[table, table_mapping[table]] for table in supported_tables
                if table in table_mapping.keys()]

    @api_step(step_name='获取所有节点列表')
    def get_node_list(self):
        url = self.combine_url(config.API_BASE_URL, self.url('list_node'))
        res = self.get(url, verify=True)
        self.raise_for_status(res)
        nodes = res.json()['data']['items']
        return nodes

    @api_step(step_name='通过节点名称获取节点信息')
    def get_node_info_by_name(self, node_name):
        nodes = self.get_node_list()
        filter_nodes = [node for node in nodes if node['name'] == node_name]
        if len(filter_nodes) == 1:
            return filter_nodes[0]
        return None

    @api_step(step_name='通过节点ID获取节点信息')
    def get_node_info_by_id(self, node_id):
        url = self.combine_url(config.API_BASE_URL, self.url('node_info').format(str(node_id)))
        res = self.get(url, verify=True)
        self.raise_for_status(res)
        return res.json()['data']

    @api_step(step_name='通过节点ID，暂停并删除节点')
    def check_state_and_delete_node_by_ids(self, node_ids: list):
        for node_id in node_ids:
            node_info = self.get_node_info_by_id(node_id)
            if node_info['state'] == 'ACTIVE':
                self.suspend_node(node_id)
            self.delete_node(node_id)

    @api_step(step_name='通过节点名称，暂停并删除节点')
    def check_state_and_delete_node_by_names(self, node_names: list):
        node_ids = []
        for node_name in node_names:
            node_info = self.get_node_info_by_name(node_name)
            if not node_info:
                print('无法找到该节点，名称: {0}'.format(node_name))
                continue
            node_ids.append(node_info['id'])
        if node_ids:
            self.check_state_and_delete_node_by_ids(node_ids)

    @api_step(step_name='删除所有节点')
    def delete_all_nodes(self):
        nodes = self.get_node_list()
        node_ids = [node['id'] for node in nodes]
        self.check_state_and_delete_node_by_ids(node_ids)


class NodeData:

    @staticmethod
    def create_mysql_node_data(is_source):
        config_dict = config.MYSQL_SOURCE if is_source else config.MYSQL_SINK
        return {
            "description": '',
            "type": config_dict['type'],
            "version": config_dict['version'],
            "basicConfig": {
                "host": config_dict['host'],
                "username": config_dict['username'],
                "password": config_dict['password'],
                "database": config_dict['database'],
                "port": config_dict['port'],
            }
        }

    @staticmethod
    def create_sql_server_node_data(is_source):
        config_dict = config.SQL_SERVER_SOURCE if is_source else config.SQL_SERVER_SINK
        return {
            "description": '',
            "type": config_dict['type'],
            "version": config_dict['version'],
            "basicConfig": {
                "host": config_dict['host'],
                "username": config_dict['username'],
                "password": config_dict['password'],
                "database": config_dict['database'],
                "schema": 'dbo',
                "port": config_dict['port'],
            }
        }

    @staticmethod
    def create_oracle_node_data(is_source):
        config_dict = config.ORACLE_SOURCE if is_source else config.ORACLE_SINK
        return {
            "description": '',
            "type": config_dict['type'],
            "version": config_dict['version'],
            "basicConfig": {
                "host": config_dict['host'],
                "username": config_dict['username'],
                "password": config_dict['password'],
                "database": config_dict['database'],
                "port": config_dict['port'],
            }
        }

    @staticmethod
    def create_postgresql_node_data(is_source):
        config_dict = config.POSTGRESQL_SOURCE if is_source else config.POSTGRESQL_SINK
        return {
            "description": '',
            "type": config_dict['type'],
            "version": config_dict['version'],
            "basicConfig": {
                "host": config_dict['host'],
                "username": config_dict['username'],
                "password": config_dict['password'],
                "database": config_dict['database'],
                "port": config_dict['port'],
            }
        }

    @staticmethod
    def create_db2_node_data(is_source):
        if is_source:
            return {
                        "description": "",
                        "type": config.DB2_SOURCE['type'],
                        "version": config.DB2_SOURCE['version'],
                        "basicConfig": {
                            "ibmCdcNodeConfig": {
                                "host": config.DB2_SOURCE['host'],
                                "port": config.DB2_SOURCE['port'],
                                "username": config.DB2_SOURCE['access_username'],
                                "sourceHost": config.DB2_SOURCE['sourceHost'],
                                "sourcePort": config.DB2_SOURCE['sourcePort'],
                                "sourceUser": config.DB2_SOURCE['sourceUser'],
                                "sourceDb": config.DB2_SOURCE['sourceDb'],
                                "password": config.DB2_SOURCE['access_password'],
                                "sourcePass": config.DB2_SOURCE['sourcePass'],
                                "targetHost": config.DB2_SOURCE['targetHost'],
                                "targetPort": config.DB2_SOURCE['targetPort'],
                                "zkHost": config.DB2_SOURCE['zkHost'],
                                "zkPort": config.DB2_SOURCE['zkPort'],
                                "targetPass": config.DB2_SOURCE['targetPass'],
                                "kafkaHosts": config.DB2_SOURCE['kafkaHosts'],
                                "schemaRegistryHost": config.DB2_SOURCE['schemaRegistryHost'],
                                "schemaRegistryPort": config.DB2_SOURCE['schemaRegistryPort']
                            }
                        }
                    }
        else:
            return {
                        "description": "",
                        "type": config.DB2_SINK['type'],
                        "version": config.DB2_SINK['version'],
                        "basicConfig": {
                            "host": config.DB2_SINK['host'],
                            "username": config.DB2_SINK['username'],
                            "password": config.DB2_SINK['password'],
                            "database": config.DB2_SINK['database'],
                            "port": config.DB2_SINK['jdbc-port']
                        }
                    }

    @staticmethod
    def create_tidb_node_data(is_source):
        config_dict = config.TiDB_SOURCE if is_source else config.TiDB_SINK
        return {
            "description": '',
            "type": config_dict['type'],
            "version": config_dict['version'],
            "basicConfig": {
                "host": config_dict['host'],
                "username": config_dict['username'],
                "password": config_dict['password'],
                "database": config_dict['database'],
                "port": config_dict['port'],
            }
        }

    @staticmethod
    def create_hashdata_node_data(is_source):
        if not is_source:
            raise DBCouldNotUseAsSourceException('HashData不能作为目标数据库')
        config_dict = config.HASHDATA_SOURCE
        return {
            "description": '',
            "type": config_dict['type'],
            "version": config_dict['version'],
            "basicConfig": {
                "host": config_dict['host'],
                "username": config_dict['username'],
                "password": config_dict['password'],
                "database": config_dict['database'],
                "port": config_dict['port'],
            }
        }

    @staticmethod
    def create_redshift_node_data(is_source):
        if is_source:
            raise DBCouldNotUseAsSinkException('Redshift不能作为源端数据库')
        config_dict = config.REDSHIFT_SINK
        return {
            "description": '',
            "type": config_dict['type'],
            "version": config_dict['version'],
            "basicConfig": {
                "s3": {
                    "authType": "ACCESS_KEY",
                    "bucket": config_dict['s3_bucket'],
                    "region": "cn-north-1",
                    "accessId": config_dict['s3_accessId'],
                    "accessKey": config_dict['s3_accessKey'],
                    "directory": config_dict['s3_directory']
                },
                "host": config_dict['host'],
                "username": config_dict['username'],
                "password": config_dict['password'],
                "database": config_dict['database'],
                "port": config_dict['port'],
                "schema": "public"
            }
        }

    @staticmethod
    def create_greenplum_node_data(is_source):
        if is_source:
            raise DBCouldNotUseAsSinkException('GreenPlum不能作为源端数据库')
        config_dict = config.GREENPLUM_SINK
        return {
            "description": '',
            "type": config_dict['type'],
            "version": config_dict['version'],
            "basicConfig": {
                "host": config_dict['host'],
                "username": config_dict['username'],
                "password": config_dict['password'],
                "database": config_dict['database'],
                "port": config_dict['port'],
                "schema": 'public',
            }
        }

    @staticmethod
    def create_opengauss_node_data(is_source):
        if is_source:
            raise DBCouldNotUseAsSinkException('OpenGauss不能作为源端数据库')
        config_dict = config.OPENGAUSS_SINK
        return {
                    "description": "",
                    "type": config_dict['type'],
                    "version": config_dict['version'],
                    "basicConfig": {
                        "host": config_dict['host'],
                        "username": config_dict['username'],
                        "password": config_dict['password'],
                        "database": config_dict['database'],
                        "port": config_dict['port']
                    }
                }

    @staticmethod
    def create_kafka_node_data(is_source):
        config_dict = config.KAFKA_SOURCE if is_source else config.KAFKA_SINK
        return {
            "description": '',
            "type": config_dict['type'],
            "version": config_dict['version'],
            "basicConfig": {
                "host": config_dict['host'],
                "schemaRegistryHost": config_dict['schema_registry'],
                "securityConfig": {
                    "authType": "SIMPLE"
                }
            },
            "policyConfig": {
                "defaultConfig": {
                    "keySerializer": "CONFLUENT_KAFKA_STRING",
                    "valueSerializer": "CONFLUENT_KAFKA_STRING"
                }
            }
        }

    @staticmethod
    def create_ftp_node_data(is_source):
        config_dict = config.FTP_SOURCE if is_source else config.FTP_SINK
        return {
            "description": '',
            "type": config_dict['type'],
            "version": config_dict['version'],
            "basicConfig": {
                "host": config_dict['host'],
                "port": config_dict['port'],
                "username": config_dict['username'],
                "password": config_dict['password'],
                "protocolMode": "PASSIVE_MODE",
                "securityConfig": {
                    "authType": "SIMPLE"
                },
                "directory": config_dict['root']
            }
        }

    @staticmethod
    def create_redis_node_data(is_source):
        if is_source:
            raise DBCouldNotUseAsSinkException('Redis不能作为源端数据库')
        config_dict = config.REDIS_SINK
        return {
                    "description": "",
                    "type": config_dict['type'],
                    "version": config_dict['version'],
                    "basicConfig": {
                        "hosts": config_dict['host'],
                        "deployMode": "SINGLE"
                    }
                }

    @staticmethod
    def create_redis_cluster_node_data(is_source):
        if is_source:
            raise DBCouldNotUseAsSinkException('Redis不能作为源端数据库')
        config_dict = config.REDIS_SINK
        return {
            "description": "",
            "type": config_dict['type'],
            "version": config_dict['version'],
            "basicConfig": {
                "hosts": config_dict['host'],
                "deployMode": "CLUSTER",
                "password": config_dict['password']
            }
        }

    @staticmethod
    def create_sequoiadb_node_data(is_source):
        if is_source:
            raise DBCouldNotUseAsSinkException('SequoiaDB不能作为源端数据库')
        config_dict = config.SEQUOIADB_SINK
        return {
            "description": '',
            "type": config_dict['type'],
            "version": config_dict['version'],
            "basicConfig": {
                "host": config_dict['host'],
                "username": config_dict['username'],
                "password": config_dict['password'],
                "database": ""
            }
        }

    @staticmethod
    def create_hdfs_node_data(is_source):
        config_dict = config.HDFS_SOURCE if is_source else config.HDFS_SINK
        return {
                    "description": "",
                    "type": config_dict['type'],
                    "version": config_dict['version'],
                    "basicConfig": {
                        "directory": config_dict['root'],
                        "user": config_dict['username'],
                        "securityConfig": {
                            "authType": "SIMPLE"
                        },
                    }
                }

    @staticmethod
    def create_hive_node_data(is_source):
        config_dict = config.HIVE_SOURCE if is_source else config.HIVE_SINK
        return {
                    "description": "",
                    "type": config_dict['type'],
                    "version": config_dict['version'],
                    "basicConfig": {
                        "hiveMetaStoreClientConfig": {
                            "directory": config_dict['root'],
                            "user": config_dict['username'],
                            "securityConfig": {
                                "authType": "SIMPLE"
                            },
                            "database": ""
                        }
                    }
                }

    @staticmethod
    def create_inceptor_node_data(is_source):
        config_dict = config.INCEPTOR_SOURCE if is_source else config.INCEPTOR_SINK
        return {
            "description": "",
            "type": config_dict['type'],
            "version": config_dict['version'],
            "basicConfig": {
                "hiveMetaStoreClientConfig": {
                    "directory": config_dict['root'],
                    "user": config_dict['hdfs_user'],
                    "securityConfig": {
                        "authType": "KERBEROS",
                        "kerberos": {
                            "confFileId": None,
                            "principal": config_dict['principal'],
                            "keytabFileId": None
                        }
                    },
                    "database": ""
                }
            }
        }

    @staticmethod
    def create_ftp_entity_data():
        return {
                "dataNodeEntities": [
                    {
                        "delete": False,
                        "entityInfo": {
                            "name": ""
                        },
                        "entityBasicConfig": {
                            "fileConfig": {
                                "fileType": "CSV",
                                "separator": ",",
                                "lineEnd": "\\n",
                                "headers": [],
                                "escapeChar": "\\\\",
                                "quoteChar": "\\\"",
                                "skipBlankLine": False,
                                "charset": "utf8"
                            }
                        },
                    }
                ]
            }

    @staticmethod
    def create_ftp_json_entity_data():
        return {
            "dataNodeEntities": [
                {
                    "delete": False,
                    "entityInfo": {
                        "name": ""
                    },
                    "entityBasicConfig": {
                        "fileConfig": {
                            "sampleDataFileId": None,
                            "charset": "utf-8",
                            "fileType": "JSON"
                        }
                    },
                }
            ]
        }

    @staticmethod
    def create_hdfs_entity_data():
        return {
            "dataNodeEntities": [
                {
                    "delete": False,
                    "entityInfo": {
                        "name": ""
                    },
                    "entityBasicConfig": {
                        "fileConfig": {
                            "fileType": "CSV",
                            "separator": ",",
                            "lineEnd": "\\n",
                            "headers": [],
                            "escapeChar": "\\\\",
                            "quoteChar": "\\\"",
                            "skipBlankLine": False,
                            "charset": "utf8"
                        }
                    },
                }
            ]
        }

    @staticmethod
    def create_hdfs_avro_entity_data():
        return {
            "dataNodeEntities": [
                {
                    "delete": False,
                    "entityInfo": {
                        "name": ""
                    },
                    "entityBasicConfig": {
                        "fileConfig": {
                            "schemaDefinitionFileId": None,
                            "codecType": "UNCOMPRESSED",
                            "fileType": "AVRO"
                        }
                    },
                }
            ]
        }

    @staticmethod
    def set_kafka_avro_entity_data(entity_id, node_id):
        return {
            "dataNodeEntities": [
                {
                    "entityBasicConfig": {
                        "keySerializer": "CONFLUENT_KAFKA_AVRO",
                        "valueSerializer": "CONFLUENT_KAFKA_AVRO"
                    },
                    "id": entity_id,
                    "nodeId": node_id
                }
            ]
        }
