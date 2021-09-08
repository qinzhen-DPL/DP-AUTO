#!/usr/bin/env python
# encoding: utf-8
from core.logics.api.apis.node_api import NodeType
from core.logics.api.api_driver import *


class LinkApi(ApiDriver):
    path_dictionary = {
        'create_link': '/v3/data-links',
        'update_link': '/v3/data-links/{0}',
        'active_link': '/v3/data-links/{0}/active',
        'suspend_link': '/v3/data-links/{0}/suspend',
        'delete_link': '/v3/data-links/{0}',
        'list_link': '/v3/data-links/list',
        'link_info': '/v3/data-links/{0}?includeExtraConfig=true&includeNode=true',
        'create_sink_table': '/v3/entity/create-sink-entities?linkId={0}&execute=true',
        'link_mapping_info': '/v3/entity/mappings/link/{0}',
        'filed_mapping_info': '/v3/entity/field-mappings?linkId={0}&sinkNodeId={1}&srcEntityId={2}',
        'file_filed_mapping_info': '/v3/entity/field-mappings?fileType={3}&linkId={0}&sinkNodeId={1}&srcEntityId={2}',
        'all_filed_mapping_info': '/v3/entity/field-mappings?linkId={0}&sinkEntityId={1}&sinkNodeId={2}&srcEntityId={3}',
        'code_engine': '/v3/entity/code-engine'
    }

    data_dictionary = {
        'create_link': {"name": "%s", "description": ""},
    }

    def __init__(self, session):
        ApiDriver.__init__(self, session)

    @api_step(step_name='创建链路')
    def create_link(self, link_name):
        print('Link name: {0}'.format(link_name))
        url = self.combine_url(config.API_BASE_URL, self.url('create_link'))
        data = self.combine_data(self.data('create_link'), link_name)
        res = self.post(url, json=data, verify=True)
        self.raise_for_status(res)
        link_id = res.json()['data']['id']
        link_state = res.json()['data']['state']
        print('Link name: {0}, ID: {1}, state: {2}'.format(link_name, link_id, link_state))
        return link_id

    @api_step(step_name='更新链路')
    def update_link(self, link_id, data):
        url = self.combine_url(config.API_BASE_URL, self.url('update_link').format(str(link_id)))
        res = self.put(url, json=data, verify=True)
        self.raise_for_status(res)

    @api_step(step_name='激活链路')
    def active_link(self, link_id):
        url = self.combine_url(config.API_BASE_URL, self.url('active_link').format(str(link_id)))
        res = self.post(url, verify=True)
        self.raise_for_status(res)

    @api_step(step_name='暂停链路')
    def suspend_link(self, link_id):
        url = self.combine_url(config.API_BASE_URL, self.url('suspend_link').format(str(link_id)))
        res = self.post(url, verify=True)
        self.raise_for_status(res)

    @api_step(step_name='删除链路')
    def delete_link(self, link_id):
        url = self.combine_url(config.API_BASE_URL, self.url('delete_link').format(str(link_id)))
        res = self.delete(url, verify=True)
        self.raise_for_status(res)

    @api_step(step_name='获取全部链路信息')
    def get_link_list(self):
        url = self.combine_url(config.API_BASE_URL, self.url('list_link'))
        res = self.get(url, verify=True)
        self.raise_for_status(res)
        links = res.json()['data']['items']
        return links

    @api_step(step_name='通过链路名称获取该链路信息')
    def get_link_info_by_name(self, link_name):
        links = self.get_link_list()
        filter_links = [link for link in links if link['name'] == link_name]
        if len(filter_links) == 1:
            return filter_links[0]
        return None

    @api_step(step_name='通过链路ID获取该链路信息')
    def get_link_info_by_id(self, link_id, raise_exception=True):
        url = self.combine_url(config.API_BASE_URL, self.url('link_info').format(str(link_id)))
        res = self.get(url, verify=True)
        if raise_exception:
            self.raise_for_status(res)
            return res.json()['data']
        else:
            return res

    @api_step(step_name='获取链路映射信息')
    def get_link_mapping_info(self, link_id):
        url = self.combine_url(config.API_BASE_URL, self.url('link_mapping_info').format(str(link_id)))
        res = self.get(url, verify=True)
        self.raise_for_status(res)
        return res.json()['data']['items']

    @api_step(step_name='获取链路表字段映射信息')
    def get_filed_mapping_info(self, link_id, source_table_id, sink_node_id, file_type=None):
        if file_type:
            url = self.combine_url(config.API_BASE_URL, self.url('file_filed_mapping_info').
                                   format(str(link_id), str(sink_node_id), str(source_table_id), file_type))
        else:
            url = self.combine_url(config.API_BASE_URL, self.url('filed_mapping_info').
                                   format(str(link_id), str(sink_node_id), str(source_table_id)))
        res = self.get(url, verify=True)
        self.raise_for_status(res)
        return res.json()['data']

    @api_step(step_name='获取链路表字段映射信息')
    def get_filed_mapping_column_info(self, link_id, source_table_id, sink_table_id,
                                      sink_node_id, is_source: bool):
        url = self.combine_url(config.API_BASE_URL, self.url('all_filed_mapping_info').
                               format(str(link_id), str(sink_table_id), str(sink_node_id), str(source_table_id)))
        res = self.get(url, verify=True)
        self.raise_for_status(res)
        from core.logics.db.db_driver import DBColumn, DBTable
        columns = []
        if is_source:
            for mapping in res.json()['data']['mappings']:
                columns.append(DBColumn(name=mapping['srcFieldName'].lower(),
                                        data_type=mapping.get('srcFieldType', None),
                                        not_null=mapping.get('srcFieldNullable', None),
                                        precision=mapping.get('srcFieldPrecision', None),
                                        scale=mapping.get('srcFieldScale', None)))
            return DBTable(None, None, columns, None)
        else:
            for mapping in res.json()['data']['mappings']:
                columns.append(DBColumn(name=mapping['sinkFieldName'].lower(),
                                        data_type=mapping.get('sinkFieldType', None),
                                        not_null=mapping.get('sinkFieldNullable', None),
                                        precision=mapping.get('sinkFieldPrecision', None),
                                        scale=mapping.get('sinkFieldScale', None)))
            return DBTable(None, None, columns, None)

    @api_step(step_name='获取链路表字段映射ID')
    def get_filed_mapping_id(self, link_id, source_table_id, sink_table_id, sink_node_id):
        url = self.combine_url(config.API_BASE_URL, self.url('all_filed_mapping_info').
                               format(str(link_id), str(sink_table_id), str(sink_node_id), str(source_table_id)))
        res = self.get(url, verify=True)
        self.raise_for_status(res)
        return res.json()['data']['mappings'][0]['dataLinkEntityMappingId']

    @api_step(step_name='通过链路ID删除链路')
    def force_delete_link_by_id(self, link_id, wait_timeout=30):
        link_info = self.get_link_info_by_id(link_id)
        if link_info['state'] == 'ACTIVE' or link_info['state'] == 'ACTIVATING':
            self.suspend_link(link_id)

        class IsSuspend:
            def __init__(self, instance):
                self.instance = instance

            def __call__(self):
                status = self.instance.get_link_info_by_id(link_id)
                print('当前状态: {0}'.format(status['state']))
                return status['state'] == 'SUSPEND'
        Wait(wait_timeout, wait_interval=3).until(
            IsSuspend(self), "已等待{0}秒，链路仍不能暂停，任务ID: {1}".format(wait_timeout, link_id))
        self.delete_link(link_id)

        class IsDeleted:
            def __init__(self, instance):
                self.instance = instance

            def __call__(self):
                ret = self.instance.get_link_info_by_id(link_id, raise_exception=False)
                return ret.status_code == 404
        Wait(wait_timeout, wait_interval=3).until(
            IsDeleted(self), "已等待{0}秒，链路仍不能删除，任务ID: {1}".format(wait_timeout, link_id))

    @api_step(step_name='通过链路名称删除链路')
    def force_delete_link_by_name(self, link_name, wait_timeout=10):
        link_info = self.get_link_info_by_name(link_name)
        assert link_info, '无法找到该链路，名称: {0}'.format(link_name)
        self.force_delete_link_by_id(link_info['id'], wait_timeout)

    @api_step(step_name='创建Sink数据库表')
    def create_sink_tables(self, link_id):
        url = self.combine_url(config.API_BASE_URL, self.url('create_sink_table').format(str(link_id)))
        res = self.post(url, data={}, verify=True)
        self.raise_for_status(res)

    @api_step(step_name='删除所有链路')
    def delete_all_links(self, wait_timeout=10):
        links = self.get_link_list()
        for link in links:
            self.force_delete_link_by_id(link['id'], wait_timeout)

    @api_step(step_name='添加高级清洗')
    def add_code_engine(self, data):
        url = self.combine_url(config.API_BASE_URL, self.url('code_engine'))
        res = self.post(url, json=data, verify=True)
        self.raise_for_status(res)


class LinkData:

    @staticmethod
    def kafka_avro_code_engine(link_id, link_entity_mapping_id):
        return {
                "linkId": link_id,
                "linkEntityMappingId": link_entity_mapping_id,
                "code": "import com.datapipeline.clients.codeengine.CustomizedCodeEngineException;\nimport com.datapipeline.clients.connector.record.DpRecordMeta;\nimport com.datapipeline.clients.connector.schema.base.DpRecordKey;\nimport com.datapipeline.clients.connector.schema.base.SinkColumn;\nimport com.datapipeline.clients.connector.schema.base.SinkSchema;\nimport com.datapipeline.clients.connector.schema.base.SourceFieldMeta;\nimport com.datapipeline.clients.record.DpRecordConstants;\nimport com.datapipeline.internal.bean.node.column.DataType;\nimport com.datapipeline.internal.bean.node.column.kafka.KafkaType;\nimport java.nio.ByteBuffer;\nimport java.util.HashMap;\nimport java.util.List;\nimport java.util.Map;\nimport java.util.Objects;\nimport org.apache.kafka.connect.data.Schema;\nimport org.apache.kafka.connect.data.Schema.Type;\nimport org.apache.kafka.connect.data.SchemaBuilder;\nimport org.apache.kafka.connect.data.Struct;\nimport org.codehaus.jettison.json.JSONObject;\n\n/**\n* 详情请见产品手册:\n*/\npublic class AvroCodeEngine {\n\n    private List < String > valueSyncColumnNames;\n    private List < String > keySyncColumnNames;\n\n    private Schema valueSchema;\n    private Schema keySchema;\n\n    public Struct getKeyStruct(DpRecordKey dpRecordKey, SinkSchema sinkSchema, JSONObject dataJson, List < SourceFieldMeta > sourceFieldMetaList) {\n        List < String > pkNames = dpRecordKey.getPrimaryKeyNames();\n\n        Map < String,\n        SourceFieldMeta > sourceMetaMap = new HashMap();\n        for (SourceFieldMeta sourceFieldMeta: sourceFieldMetaList) {\n            sourceMetaMap.put(sourceFieldMeta.getName(), sourceFieldMeta);\n        }\n        if (!Objects.equals(keySyncColumnNames, sinkSchema.getSyncColumnNames())) {\n            keySyncColumnNames = sinkSchema.getSyncColumnNames();\n            SchemaBuilder keySchemaBuilder = SchemaBuilder.struct().name(\"key\");\n            for (SinkColumn sinkColumn: sinkSchema.getSyncColumns()) {\n                SourceFieldMeta sourceFieldMeta = sourceMetaMap.get(sinkColumn.getName());\n                if (pkNames.contains(sinkColumn.getName())) {\n                    keySchemaBuilder.field(sinkColumn.getName(), convertDefinitionToSchema(sinkColumn.getDefinition().getType(), sourceFieldMeta));\n                }\n            }\n            keySchema = keySchemaBuilder.build();\n        }\n        Struct struct = new Struct(keySchema);\n\n        for (SinkColumn sinkColumn: sinkSchema.getSyncColumns()) {\n            String columnName = sinkColumn.getName();\n            if (pkNames.contains(columnName)) {\n\n                struct.put(columnName, convertValue(keySchema.field(columnName).schema(), dataJson.opt(columnName)));\n            }\n        }\n        return struct;\n\n    }\n\n    public Map < String,\n    Object > process(Map < String, Object > record, DpRecordMeta meta) {\n        // 系统会向 dml 字段写入标识：insert、update、delete。\n        // 如果是定时读取模式，存量 dml 字段会标识为 insert，增量 dml 字段会标识为 update，\n        // 如果是实时读取模式，存量 dml 字段会标识为 insert，增量 dml 字段会标识为 update 和 delete。\n        Map < String,\n        String > customParams = meta.getCustomParams();\n        if (customParams != null) {\n            record.put(\"enttyp\", customParams.get(\"ENTTYP\"));\n        }\n        record.put(\"op_\", meta.getType());\n\n        // 保存该脚本后，请在目的地表结构添加新字段：dml，并把字段类型改为字符串类型（例如：varchar）\n        return record;\n    }\n\n    public Struct getValueStruct(SinkSchema sinkSchema, JSONObject dataJson, List < SourceFieldMeta > sourceFieldMetaList) throws CustomizedCodeEngineException {\n        if (!Objects.equals(valueSyncColumnNames, sinkSchema.getSyncColumnNames())) {\n            valueSyncColumnNames = sinkSchema.getSyncColumnNames();\n            SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct().name(\"DBMessage\").field(\"op_\", Schema.OPTIONAL_STRING_SCHEMA);\n\n            Map < String,\n            SourceFieldMeta > sourceMetaMap = new HashMap();\n            if (sourceFieldMetaList != null) {\n                for (SourceFieldMeta sourceFieldMeta: sourceFieldMetaList) {\n                    sourceMetaMap.put(sourceFieldMeta.getName(), sourceFieldMeta);\n                }\n            }\n            for (SinkColumn sinkColumn: sinkSchema.getSyncColumns()) {\n                SourceFieldMeta sourceFieldMeta = sourceMetaMap.get(sinkColumn.getName());\n                if (sourceFieldMeta != null) {\n                    valueSchemaBuilder.field(sinkColumn.getName(), convertDefinitionToSchema(sinkColumn.getDefinition().getType(), sourceFieldMeta));\n                }\n\n            }\n            valueSchema = valueSchemaBuilder.build();\n        }\n        Struct struct = new Struct(valueSchema);\n        struct.put(\"op_\", dataJson.optString(\"op_\"));\n        for (SinkColumn sinkColumn: sinkSchema.getSyncColumns()) {\n            String columnName = sinkColumn.getName();\n            struct.put(columnName, convertValue(valueSchema.field(columnName).schema(), dataJson.opt(columnName)));\n        }\n\n        return struct;\n    }\n\n    private Schema convertDefinitionToSchema(DataType dataType, SourceFieldMeta sourceFieldMeta) {\n        Map < String,\n        String > map = new HashMap < >();\n        if (sourceFieldMeta != null) {\n            if (sourceFieldMeta.getScale() != null) {\n                map.put(DpRecordConstants.DATA_SCALE, String.valueOf(sourceFieldMeta.getScale()));\n            }\n            if (sourceFieldMeta.getPrecision() != null) {\n                map.put(DpRecordConstants.DATA_LENGTH, String.valueOf(sourceFieldMeta.getPrecision()));\n            }\n            if (sourceFieldMeta.getSourceType() != null) {\n                map.put(DpRecordConstants.SOURCE_TYPE, String.valueOf(sourceFieldMeta.getSourceType()));\n            }\n        }\n        KafkaType kafkaType = (KafkaType) dataType;\n        if (kafkaType == KafkaType.INT8) {\n            return SchemaBuilder.int8().optional().parameters(map).build();\n        }\n        if (kafkaType == KafkaType.INT16) {\n            return SchemaBuilder.int16().optional().parameters(map).build();\n        }\n        if (kafkaType == KafkaType.INT32) {\n            return SchemaBuilder.int32().optional().parameters(map).build();\n        }\n        if (kafkaType == KafkaType.INT64) {\n            return SchemaBuilder.int64().optional().parameters(map).build();\n        }\n        if (kafkaType == KafkaType.FLOAT32) {\n            return SchemaBuilder.float32().optional().parameters(map).build();\n        }\n        if (kafkaType == KafkaType.FLOAT64) {\n            return SchemaBuilder.float64().optional().parameters(map).build();\n        }\n        if (kafkaType == KafkaType.BOOLEAN) {\n            return SchemaBuilder.bool().optional().parameters(map).build();\n        }\n        if (kafkaType == KafkaType.STRING) {\n            return SchemaBuilder.string().optional().parameters(map).build();\n        }\n        if (kafkaType == KafkaType.BYTES) {\n            return SchemaBuilder.bytes().optional().parameters(map).build();\n        }\n        throw new IllegalArgumentException(String.format(\"data type %s not support\", dataType.toString()));\n\n    }\n\n    private Object convertValue(Schema schema, Object data) {\n        if (data == JSONObject.NULL || data == null) {\n            return null;\n        }\n        Type type = schema.type();\n        if (type == Type.INT8) {\n            if (data instanceof Byte) {\n                return data;\n            }\n            // convert data to Byte\n        }\n        if (type == Type.INT16) {\n            if (data instanceof Short) {\n                return data;\n            } else {\n                return Short.valueOf(data.toString());\n            }\n        }\n        if (type == Type.INT32) {\n            if (data instanceof Integer) {\n                return data;\n            } else {\n                return Integer.valueOf(data.toString());\n            }\n        }\n        if (type == Type.INT64) {\n            if (data instanceof Long) {\n                return data;\n            } else {\n                return Long.valueOf(data.toString());\n            }\n        }\n        if (type == Type.FLOAT32) {\n            if (data instanceof Float) {\n                return data;\n            } else {\n                return Float.valueOf(data.toString());\n            }\n        }\n        if (type == Type.FLOAT64) {\n            if (data instanceof Double) {\n                return data;\n            } else {\n                return Double.valueOf(data.toString());\n            }\n        }\n        if (type == Type.BOOLEAN) {\n            if (data instanceof Boolean) {\n                return data;\n            } else {\n                return Boolean.valueOf(data.toString());\n            }\n        }\n        if (type == Type.STRING) {\n            return data.toString();\n        }\n        if (type == Type.BYTES) {\n            if (data instanceof ByteBuffer || data instanceof byte[]) {\n                return data;\n            } else {\n                return data.toString().getBytes();\n            }\n        }\n        // convert data to ByteBuffer or byte[]\n        if (type == Type.STRUCT || type == Type.MAP || type == Type.ARRAY) {\n            // Customized handling.\n            return data;\n        }\n        throw new IllegalArgumentException(String.format(\"data type %s not support\", schema.type().toString()));\n    }\n\n}",
                "enable": True,
                "language": "JAVA"
            }

    @staticmethod
    def jdbc_read_data(node_id):
        return {
                    "node": node_id,
                    "basicConfig": {
                        "mode": "JDBC_READ",
                        "fullMode": "JDBC_READ",
                        "@class": "com.datapipeline.internal.bean.DataNodeRelationSrcBasicConfig"
                    }
                }

    @staticmethod
    def binlog_read_data(node_id):
        return {
            "node": node_id,
            "basicConfig": {
                "mode": "BINLOG",
                "fullMode": "JDBC_READ",
                "@class": "com.datapipeline.internal.bean.DataNodeRelationSrcBasicConfig"
            }
        }

    @staticmethod
    def logminer_read_data(node_id):
        return {
            "node": node_id,
            "basicConfig": {
                "mode": "LOG_MINER",
                "fullMode": "JDBC_READ",
                "@class": "com.datapipeline.internal.bean.DataNodeRelationSrcBasicConfig"
            }
        }

    @staticmethod
    def wal2json_read_data(node_id):
        return {
            "node": node_id,
            "basicConfig": {
                "mode": "WAL2JSON",
                "fullMode": "JDBC_READ",
                "@class": "com.datapipeline.internal.bean.DataNodeRelationSrcBasicConfig"
            }
        }

    @staticmethod
    def jdbc_write_data(node_id):
        return {
                    "node": node_id,
                    "basicConfig": {
                        "mode": "JDBC_WRITE",
                        "@class": "com.datapipeline.internal.bean.DataNodeRelationSinkBasicConfig"
                    }
                }

    @staticmethod
    def scan_read_data(node_id):
        return {
                    "node": node_id,
                    "basicConfig": {
                        "mode": "SCAN",
                        "fullMode": "SCAN",
                        "@class": "com.datapipeline.internal.bean.DataNodeRelationSrcBasicConfig"
                    }
                }

    @staticmethod
    def copy_write_data(node_id):
        return {
                    "node": node_id,
                    "basicConfig": {
                        "mode": "COPY",
                        "@class": "com.datapipeline.internal.bean.DataNodeRelationSinkBasicConfig"
                    }
                }

    @staticmethod
    def kafka_source_data(node_id):
        return {
                    "node": node_id,
                    "basicConfig": {
                        "mode": "KAFKA_CLIENT",
                        "@class": "com.datapipeline.internal.bean.DataNodeRelationSrcBasicConfig"
                    }
                }

    @staticmethod
    def kafka_sink_data(node_id):
        return {
                    "node": node_id,
                    "basicConfig": {
                        "mode": "KAFKA_CLIENT",
                        "@class": "com.datapipeline.internal.bean.DataNodeRelationSinkBasicConfig"
                    }
                }

    @staticmethod
    def redis_sink_data(node_id):
        return {
                    "node": node_id,
                    "basicConfig": {
                        "mode": "REDIS_CLIENT",
                        "@class": "com.datapipeline.internal.bean.DataNodeRelationSinkBasicConfig"
                    }
                }

    @staticmethod
    def sequoia_sink_data(node_id):
        return {
                    "node": node_id,
                    "basicConfig": {
                        "mode": "BULK_INSERT",
                        "@class": "com.datapipeline.internal.bean.DataNodeRelationSinkBasicConfig"
                    }
                }

    @staticmethod
    def mysql_mapping_data(source_table_id, sink_node_id, sink_table_name):
        return {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "schema": config.MYSQL_SINK['database'],
                        "status": "NOT_CREATED"
                    },
                    "sinkNodeId": sink_node_id,
                    "srcEntityId": source_table_id
                }
            ]
        }

    @staticmethod
    def oracle_mapping_data(source_table_id, sink_node_id, sink_table_name):
        return {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "database": config.ORACLE_SINK['database'],
                        "schema": config.ORACLE_SINK['schema'],
                        "status": "NOT_CREATED"
                    },
                    "sinkNodeId": sink_node_id,
                    "srcEntityId": source_table_id
                }
            ]
        }

    @staticmethod
    def sql_server_mapping_data(source_table_id, sink_node_id, sink_table_name):
        return {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "database": config.SQL_SERVER_SINK['database'],
                        "schema": config.SQL_SERVER_SINK['schema'],
                        "status": "NOT_CREATED"
                    },
                    "sinkNodeId": sink_node_id,
                    "srcEntityId": source_table_id
                }
            ]
        }

    @staticmethod
    def postgresql_mapping_data(source_table_id, sink_node_id, sink_table_name):
        return {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "database": config.POSTGRESQL_SINK['database'],
                        "schema": config.POSTGRESQL_SINK['schema'],
                        "status": "NOT_CREATED"
                    },
                    "sinkNodeId": sink_node_id,
                    "srcEntityId": source_table_id
                }
            ]
        }

    @staticmethod
    def db2_mapping_data(source_table_id, sink_node_id, sink_table_name):
        return {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "database": config.DB2_SINK['database'],
                        "schema": config.DB2_SINK['schema'],
                        "status": "NOT_CREATED"
                    },
                    "sinkNodeId": sink_node_id,
                    "srcEntityId": source_table_id
                }
            ]
        }

    @staticmethod
    def tidb_mapping_data(source_table_id, sink_node_id, sink_table_name):
        return {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "schema": config.TiDB_SINK['database'],
                        "status": "NOT_CREATED"
                    },
                    "sinkNodeId": sink_node_id,
                    "srcEntityId": source_table_id
                }
            ]
        }

    @staticmethod
    def opengauss_mapping_data(source_table_id, sink_node_id, sink_table_name):
        return {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "database": config.OPENGAUSS_SINK['database'],
                        "schema": config.OPENGAUSS_SINK['schema'],
                        "status": "NOT_CREATED"
                    },
                    "sinkNodeId": sink_node_id,
                    "srcEntityId": source_table_id
                }
            ]
        }

    @staticmethod
    def redshift_mapping_data(source_table_id, sink_node_id, sink_table_name):
        return {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "database": config.REDSHIFT_SINK['database'],
                        "schema": config.REDSHIFT_SINK['schema'],
                        "status": "NOT_CREATED"
                    },
                    "sinkNodeId": sink_node_id,
                    "srcEntityId": source_table_id
                }
            ]
        }

    @staticmethod
    def greenplum_mapping_data(source_table_id, sink_node_id, sink_table_name):
        return {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "database": config.GREENPLUM_SINK['database'],
                        "schema": config.GREENPLUM_SINK['schema'],
                        "status": "NOT_CREATED"
                    },
                    "sinkNodeId": sink_node_id,
                    "srcEntityId": source_table_id
                }
            ]
        }

    @staticmethod
    def ftp_csv_mapping_data(source_table_id, sink_node_id, sink_table_name, field_mapping):
        data = {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "status": "INVISIBILITY"
                    },
                    "fieldMappings": [],
                    "sinkNodeId": sink_node_id,
                    "sinkEntityConfig": {
                        "fileConfig": {
                            "fileType": "CSV",
                            "separator": config.DEFAULT_SEPARATOR,
                            "lineEnd": config.DEFAULT_LINEEND,
                            "headers": [
                                ""
                            ],
                            "escapeChar": "\\\\",
                            "quoteChar": "\\\"",
                            "skipBlankLine": False,
                            "charset": "utf8"
                        }
                    },
                    "srcEntityId": source_table_id
                }
            ]
        }
        for index, each_mapping in enumerate(field_mapping['mappings']):
            src_scale = each_mapping['srcFieldScale'] if 'srcFieldScale' in each_mapping.keys() else None
            src_precision = each_mapping['srcFieldPrecision'] if 'srcFieldPrecision' in each_mapping.keys() else None
            sink_scale = each_mapping['sinkFieldScale'] if 'sinkFieldScale' in each_mapping.keys() else None
            sink_precision = each_mapping['sinkFieldPrecision'] if 'sinkFieldPrecision' in each_mapping.keys() else None
            data['sinkInfos'][0]['fieldMappings'].append(
                {
                    "srcEntityField": {
                        "id": each_mapping['srcFieldId'],
                        "name": each_mapping['srcFieldName'],
                        "type": each_mapping['srcFieldType'],
                        "scale": src_scale,
                        "precision": src_precision,
                        "nullable": each_mapping['srcFieldNullable']
                    },
                    "sinkEntityField": {
                        "name": each_mapping['sinkFieldName'],
                        "type": each_mapping['sinkFieldType'],
                        "scale": sink_scale,
                        "precision": sink_precision,
                        "nullable": each_mapping['sinkFieldNullable']
                    }
                })
        return data

    @staticmethod
    def ftp_json_mapping_data(source_table_id, sink_node_id, sink_table_name, field_mapping):
        data = {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "status": "INVISIBILITY"
                    },
                    "fieldMappings": [],
                    "sinkNodeId": sink_node_id,
                    "sinkEntityConfig": {
                        "fileConfig": {
                            "fileType": "JSON",
                            "charset": "utf8"
                        }
                    },
                    "srcEntityId": source_table_id
                }
            ]
        }
        for index, each_mapping in enumerate(field_mapping['mappings']):
            src_scale = each_mapping['srcFieldScale'] if 'srcFieldScale' in each_mapping.keys() else None
            src_precision = each_mapping['srcFieldPrecision'] if 'srcFieldPrecision' in each_mapping.keys() else None
            sink_scale = each_mapping['sinkFieldScale'] if 'sinkFieldScale' in each_mapping.keys() else None
            sink_precision = each_mapping['sinkFieldPrecision'] if 'sinkFieldPrecision' in each_mapping.keys() else None
            data['sinkInfos'][0]['fieldMappings'].append(
                {
                    "srcEntityField": {
                        "id": each_mapping['srcFieldId'],
                        "name": each_mapping['srcFieldName'],
                        "type": each_mapping['srcFieldType'],
                        "scale": src_scale,
                        "precision": src_precision,
                        "nullable": each_mapping['srcFieldNullable']
                    },
                    "sinkEntityField": {
                        "name": each_mapping['sinkFieldName'],
                        "type": each_mapping['sinkFieldType'],
                        "scale": sink_scale,
                        "precision": sink_precision,
                        "nullable": each_mapping['sinkFieldNullable']
                    }
                })
        return data

    @staticmethod
    def hdfs_csv_mapping_data(source_table_id, sink_node_id, sink_table_name, field_mapping):
        data = {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "status": "INVISIBILITY"
                    },
                    "fieldMappings": [],
                    "sinkNodeId": sink_node_id,
                    "sinkEntityConfig": {
                        "fileConfig": {
                            "fileType": "CSV",
                            "separator": config.DEFAULT_SEPARATOR,
                            "lineEnd": config.DEFAULT_LINEEND,
                            "headers": [
                                ""
                            ],
                            "escapeChar": "\\\\",
                            "quoteChar": "\\\"",
                            "skipBlankLine": False,
                            "charset": "utf8"
                        }
                    },
                    "srcEntityId": source_table_id
                }
            ]
        }
        for index, each_mapping in enumerate(field_mapping['mappings']):
            src_scale = each_mapping['srcFieldScale'] if 'srcFieldScale' in each_mapping.keys() else None
            src_precision = each_mapping['srcFieldPrecision'] if 'srcFieldPrecision' in each_mapping.keys() else None
            sink_scale = each_mapping['sinkFieldScale'] if 'sinkFieldScale' in each_mapping.keys() else None
            sink_precision = each_mapping['sinkFieldPrecision'] if 'sinkFieldPrecision' in each_mapping.keys() else None
            data['sinkInfos'][0]['fieldMappings'].append(
                {
                    "srcEntityField": {
                        "id": each_mapping['srcFieldId'],
                        "name": each_mapping['srcFieldName'],
                        "type": each_mapping['srcFieldType'],
                        "scale": src_scale,
                        "precision": src_precision,
                        "nullable": each_mapping['srcFieldNullable']
                    },
                    "sinkEntityField": {
                        "name": each_mapping['sinkFieldName'],
                        "type": each_mapping['sinkFieldType'],
                        "scale": sink_scale,
                        "precision": sink_precision,
                        "nullable": each_mapping['sinkFieldNullable']
                    }
                })
        return data

    @staticmethod
    def hdfs_avro_mapping_data(source_table_id, sink_node_id, sink_table_name, field_mapping):
        data = {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "status": "INVISIBILITY"
                    },
                    "fieldMappings": [],
                    "sinkNodeId": sink_node_id,
                    "sinkEntityConfig": {
                        "fileConfig": {
                            "fileType": "AVRO",
                            "codecType": "UNCOMPRESSED"
                        }
                    },
                    "srcEntityId": source_table_id
                }
            ]
        }
        for index, each_mapping in enumerate(field_mapping['mappings']):
            src_scale = each_mapping['srcFieldScale'] if 'srcFieldScale' in each_mapping.keys() else None
            src_precision = each_mapping['srcFieldPrecision'] if 'srcFieldPrecision' in each_mapping.keys() else None
            sink_scale = each_mapping['sinkFieldScale'] if 'sinkFieldScale' in each_mapping.keys() else None
            sink_precision = each_mapping['sinkFieldPrecision'] if 'sinkFieldPrecision' in each_mapping.keys() else None
            data['sinkInfos'][0]['fieldMappings'].append(
                {
                    "srcEntityField": {
                        "id": each_mapping['srcFieldId'],
                        "name": each_mapping['srcFieldName'],
                        "type": each_mapping['srcFieldType'],
                        "scale": src_scale,
                        "precision": src_precision,
                        "nullable": each_mapping['srcFieldNullable']
                    },
                    "sinkEntityField": {
                        "name": each_mapping['sinkFieldName'],
                        "type": each_mapping['sinkFieldType'],
                        "scale": sink_scale,
                        "precision": sink_precision,
                        "nullable": each_mapping['sinkFieldNullable']
                    }
                })
        return data

    @staticmethod
    def sequoiadb_mapping_data(source_table_id, sink_node_id, sink_table_name):
        return {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "database": config.SEQUOIADB_SINK['database'],
                        "status": "NOT_CREATED"
                    },
                    "sinkNodeId": sink_node_id,
                    "srcEntityId": source_table_id
                }
            ]
        }

    @staticmethod
    def hive_csv_mapping_data(source_table_id, sink_node_id, sink_table_name):
        return {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "database": config.HIVE_SINK['database'],
                        "status": "NOT_CREATED"
                    },
                    "sinkNodeId": sink_node_id,
                    "sinkEntityConfig": {
                        "hive": {
                            "hiveTableType": "EXTERNAL_TABLE",
                            "location": sink_table_name,
                            "transaction": False,
                            "sinkPartitions": {
                                "partitionType": "NONE"
                            }
                        },
                        "fileConfig": {
                            "fileType": "CSV",
                            "charset": "utf8",
                            "escapeChar": "\\\\",
                            "quoteChar": "\\\"",
                            "lineEnd": "\\n",
                            "separator": ","
                        }
                    },
                    "srcEntityId": source_table_id
                }
            ]
        }

    @staticmethod
    def hive_avro_mapping_data(source_table_id, sink_node_id, sink_table_name):
        return {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "database": config.HIVE_SINK['database'],
                        "status": "NOT_CREATED"
                    },
                    "sinkNodeId": sink_node_id,
                    "sinkEntityConfig": {
                        "hive": {
                            "hiveTableType": "EXTERNAL_TABLE",
                            "location": sink_table_name,
                            "transaction": False,
                            "sinkPartitions": {
                                "partitionType": "NONE"
                            }
                        },
                        "fileConfig": {
                            "fileType": "AVRO",
                            "codecType": "UNCOMPRESSED"
                        }
                    },
                    "srcEntityId": source_table_id
                }
            ]
        }

    @staticmethod
    def hive_parquet_mapping_data(source_table_id, sink_node_id, sink_table_name):
        return {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "database": config.HIVE_SINK['database'],
                        "status": "NOT_CREATED"
                    },
                    "sinkNodeId": sink_node_id,
                    "sinkEntityConfig": {
                        "hive": {
                            "hiveTableType": "EXTERNAL_TABLE",
                            "location": sink_table_name,
                            "transaction": False,
                            "sinkPartitions": {
                                "partitionType": "NONE"
                            }
                        },
                        "fileConfig": {
                            "fileType": "PARQUET",
                            "codecType": "UNCOMPRESSED"
                        }
                    },
                    "srcEntityId": source_table_id
                }
            ]
        }

    @staticmethod
    def hive_orc_mapping_data(source_table_id, sink_node_id, sink_table_name):
        return {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "database": config.HIVE_SINK['database'],
                        "status": "NOT_CREATED"
                    },
                    "sinkNodeId": sink_node_id,
                    "sinkEntityConfig": {
                        "hive": {
                            "hiveTableType": "EXTERNAL_TABLE",
                            "location": sink_table_name,
                            "transaction": False,
                            "sinkPartitions": {
                                "partitionType": "NONE"
                            }
                        },
                        "fileConfig": {
                            "fileType": "ORC",
                            "codecType": "UNCOMPRESSED"
                        }
                    },
                    "srcEntityId": source_table_id
                }
            ]
        }

    @staticmethod
    def inceptor_csv_mapping_data(source_table_id, sink_node_id, sink_table_name):
        return {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "database": config.INCEPTOR_SINK['database'],
                        "status": "NOT_CREATED"
                    },
                    "sinkNodeId": sink_node_id,
                    "sinkEntityConfig": {
                        "hive": {
                            "hiveTableType": "EXTERNAL_TABLE",
                            "location": sink_table_name,
                            "transaction": False,
                            "sinkPartitions": {
                                "partitionType": "NONE"
                            }
                        },
                        "fileConfig": {
                            "fileType": "CSV",
                            "charset": "utf8",
                            "escapeChar": "\\\\",
                            "quoteChar": "\\\"",
                            "lineEnd": "\\n",
                            "separator": ","
                        }
                    },
                    "srcEntityId": source_table_id
                }
            ]
        }

    @staticmethod
    def inceptor_avro_mapping_data(source_table_id, sink_node_id, sink_table_name):
        return {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "database": config.INCEPTOR_SINK['database'],
                        "status": "NOT_CREATED"
                    },
                    "sinkNodeId": sink_node_id,
                    "sinkEntityConfig": {
                        "hive": {
                            "hiveTableType": "EXTERNAL_TABLE",
                            "location": sink_table_name,
                            "transaction": False,
                            "sinkPartitions": {
                                "partitionType": "NONE"
                            }
                        },
                        "fileConfig": {
                            "fileType": "AVRO",
                            "codecType": "UNCOMPRESSED"
                        }
                    },
                    "srcEntityId": source_table_id
                }
            ]
        }

    @staticmethod
    def inceptor_parquet_mapping_data(source_table_id, sink_node_id, sink_table_name):
        return {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "database": config.INCEPTOR_SINK['database'],
                        "status": "NOT_CREATED"
                    },
                    "sinkNodeId": sink_node_id,
                    "sinkEntityConfig": {
                        "hive": {
                            "hiveTableType": "EXTERNAL_TABLE",
                            "location": sink_table_name,
                            "transaction": False,
                            "sinkPartitions": {
                                "partitionType": "NONE"
                            }
                        },
                        "fileConfig": {
                            "fileType": "PARQUET",
                            "codecType": "UNCOMPRESSED"
                        }
                    },
                    "srcEntityId": source_table_id
                }
            ]
        }

    @staticmethod
    def inceptor_orc_mapping_data(source_table_id, sink_node_id, sink_table_name):
        return {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "sinkEntity": {
                        "name": sink_table_name,
                        "nodeId": sink_node_id,
                        "database": config.INCEPTOR_SINK['database'],
                        "status": "NOT_CREATED"
                    },
                    "sinkNodeId": sink_node_id,
                    "sinkEntityConfig": {
                        "hive": {
                            "hiveTableType": "EXTERNAL_TABLE",
                            "location": sink_table_name,
                            "transaction": False,
                            "sinkPartitions": {
                                "partitionType": "NONE"
                            }
                        },
                        "fileConfig": {
                            "fileType": "ORC",
                            "codecType": "UNCOMPRESSED"
                        }
                    },
                    "srcEntityId": source_table_id
                }
            ]
        }

    @staticmethod
    def kafka_mapping_data(source_table_id, sink_node_id, sink_table_id, field_mapping):
        data = {
                    "srcEntityId": source_table_id,
                    "sinkInfos": [
                        {
                            "fieldMappings": [],
                            "sinkEntity": {
                                "id": sink_table_id,
                            },
                            "sinkNodeId": sink_node_id,
                            "srcEntityId": source_table_id,
                            "sinkEntityConfig": {
                                "struct": {
                                    "valueTemplate": ""
                                }
                            }
                        }
                    ]
                }
        value_template = ''
        for index, each_mapping in enumerate(field_mapping['mappings']):
            if index == len(field_mapping['mappings'])-1:
                value_template += '  \"{0}\": \"[${1}]\"\n'.format(each_mapping['srcFieldName'],
                                                                   each_mapping['sinkFieldName'])
            else:
                value_template += '  \"{0}\": \"[${1}]\",\n'.format(each_mapping['srcFieldName'],
                                                                    each_mapping['sinkFieldName'])
            src_scale = each_mapping['srcFieldScale'] if 'srcFieldScale' in each_mapping.keys() else None
            src_precision = each_mapping['srcFieldPrecision'] if 'srcFieldPrecision' in each_mapping.keys() else None
            sink_scale = each_mapping['sinkFieldScale'] if 'sinkFieldScale' in each_mapping.keys() else None
            sink_precision = each_mapping['sinkFieldPrecision'] if 'sinkFieldPrecision' in each_mapping.keys() else None
            data['sinkInfos'][0]['fieldMappings'].append(
                {
                    "srcEntityField": {
                        "id": each_mapping['srcFieldId'],
                        "name": each_mapping['srcFieldName'],
                        "type": each_mapping['srcFieldType'],
                        "scale": src_scale,
                        "precision": src_precision,
                        "nullable": each_mapping['srcFieldNullable']
                    },
                    "sinkEntityField": {
                        "name": each_mapping['sinkFieldName'],
                        "type": each_mapping['sinkFieldType'],
                        "scale": sink_scale,
                        "precision": sink_precision,
                        "nullable": each_mapping['sinkFieldNullable']
                    }
                })
        data['sinkInfos'][0]['sinkEntityConfig']['struct']['valueTemplate'] = '{\n%s}' % value_template
        return data

    @staticmethod
    def redis_mapping_data(source_table_id, sink_node_id, sink_table_name, field_mapping):
        data = {
            "srcEntityId": source_table_id,
            "sinkInfos": [
                {
                    "fieldMappings": [],
                    "sinkEntity": {
                        "nodeId": sink_node_id,
                        "name": sink_table_name,
                        "status": "NOT_CREATED",
                    },
                    "sinkNodeId": sink_node_id,
                    "srcEntityId": source_table_id,
                    "sinkEntityConfig": {
                        "redis": {
                            "dbIndex": config.REDIS_SINK['database'],
                            "mode": "STRING"
                        },
                        "struct": {
                            "valueTemplate": "",
                            "keyTemplate": ""
                        }
                    }
                }
            ]
        }
        value_template = ''
        unique_key = ''
        # 如果上游表有主键，使用主键作为redis写入的key
        if 'srcIndices' in field_mapping.keys():
            unique_key = field_mapping['srcIndices'][0]['columnName']
        for index, each_mapping in enumerate(field_mapping['mappings']):
            # 如果上游表没有主键，使用mapping第一个字段作为写入的key
            if index == 0 and not unique_key:
                unique_key = each_mapping['sinkFieldName']

            if index == len(field_mapping['mappings'])-1:
                value_template += '  \"{0}\": \"[${1}]\"\n'.format(each_mapping['srcFieldName'],
                                                                   each_mapping['sinkFieldName'])
            else:
                value_template += '  \"{0}\": \"[${1}]\",\n'.format(each_mapping['srcFieldName'],
                                                                    each_mapping['sinkFieldName'])
            src_scale = each_mapping['srcFieldScale'] if 'srcFieldScale' in each_mapping.keys() else None
            src_precision = each_mapping['srcFieldPrecision'] if 'srcFieldPrecision' in each_mapping.keys() else None
            sink_scale = each_mapping['sinkFieldScale'] if 'sinkFieldScale' in each_mapping.keys() else None
            sink_precision = each_mapping['sinkFieldPrecision'] if 'sinkFieldPrecision' in each_mapping.keys() else None
            data['sinkInfos'][0]['fieldMappings'].append(
                {
                    "srcEntityField": {
                        "id": each_mapping['srcFieldId'],
                        "name": each_mapping['srcFieldName'],
                        "type": each_mapping['srcFieldType'],
                        "scale": src_scale,
                        "precision": src_precision,
                        "nullable": each_mapping['srcFieldNullable']
                    },
                    "sinkEntityField": {
                        "name": each_mapping['sinkFieldName'],
                        "type": each_mapping['sinkFieldType'],
                        "scale": sink_scale,
                        "precision": sink_precision,
                        "nullable": each_mapping['sinkFieldNullable']
                    }
                })
        data['sinkInfos'][0]['sinkEntityConfig']['struct']['valueTemplate'] = '{\n%s}' % value_template
        data['sinkInfos'][0]['sinkEntityConfig']['struct']['keyTemplate'] = sink_table_name + "_[${0}]".format(unique_key)
        return data

    @staticmethod
    def get_source_node_data(src_id, source_type: NodeType, is_real):
        if source_type == NodeType.kafka:
            node_data = LinkData.kafka_source_data(src_id)
        elif source_type == NodeType.ftp or \
                source_type == NodeType.hdfs or \
                source_type == NodeType.hive or \
                source_type == NodeType.inceptor:
            node_data = LinkData.scan_read_data(src_id)
        elif source_type == NodeType.mysql and is_real:
            node_data = LinkData.binlog_read_data(src_id)
        elif source_type == NodeType.oracle and is_real:
            node_data = LinkData.logminer_read_data(src_id)
        elif source_type == NodeType.postgresql and is_real:
            node_data = LinkData.wal2json_read_data(src_id)
        else:
            node_data = LinkData.jdbc_read_data(src_id)
        return node_data

    @staticmethod
    def get_sink_node_data(sink_id, sink_type: NodeType):
        if sink_type == NodeType.kafka:
            node_data = LinkData.kafka_sink_data(sink_id)
        elif sink_type == NodeType.ftp or \
                sink_type == NodeType.hdfs or \
                sink_type == NodeType.hive or \
                sink_type == NodeType.inceptor:
            node_data = LinkData.copy_write_data(sink_id)
        elif sink_type == NodeType.redis:
            node_data = LinkData.redis_sink_data(sink_id)
        elif sink_type == NodeType.sequoiadb:
            node_data = LinkData.sequoia_sink_data(sink_id)
        else:
            node_data = LinkData.jdbc_write_data(sink_id)
        return node_data

    @staticmethod
    def default_link_data(src_id, sink_id, source_type: NodeType, sink_type: NodeType,
                          is_real, source_tables, sink_tables, filed_mappings=None, detail_db_type=None):
        """
        创建链路基本信息数据payload
        :param src_id: 源端节点id
        :param sink_id: 目的端节点id
        :param source_type: 源端数据库类型
        :param sink_type: 目的端数据库类型
        :param is_real: 是否为实时任务
        :param source_tables: 源端数据库需要创建测试的表信息 - [(表名，表id)...]
        :param sink_tables: 目的端要创建的对应表信息 - [(表名，表id)...]
        :param filed_mappings: 表对应字段映射信息
        :param detail_db_type: 数据库详细信息
        :return: 接口调用的payload
        """
        link = {
            "hasEntityDiff": True,
            'srcNodes': [],
            'sinkNodes': [],
            'mappings': []
        }
        # 配置src, sink节点信息
        link['srcNodes'].append(LinkData.get_source_node_data(src_id, source_type, is_real))
        link['sinkNodes'].append(LinkData.get_sink_node_data(sink_id, sink_type))

        # 配置mapping信息
        for index, value in enumerate(source_tables):
            src_table_id = value[1]
            sink_table_name = sink_tables[index][0]
            sink_table_id = sink_tables[index][1]
            if sink_type == NodeType.mysql:
                each_mapping = LinkData.mysql_mapping_data(src_table_id, sink_id, sink_table_name)
            elif sink_type == NodeType.oracle:
                each_mapping = LinkData.oracle_mapping_data(src_table_id, sink_id, sink_table_name)
            elif sink_type == NodeType.sql_server:
                each_mapping = LinkData.sql_server_mapping_data(src_table_id, sink_id, sink_table_name)
            elif sink_type == NodeType.postgresql:
                each_mapping = LinkData.postgresql_mapping_data(src_table_id, sink_id, sink_table_name)
            elif sink_type == NodeType.db2:
                each_mapping = LinkData.db2_mapping_data(src_table_id, sink_id, sink_table_name)
            elif sink_type == NodeType.tidb:
                each_mapping = LinkData.tidb_mapping_data(src_table_id, sink_id, sink_table_name)
            elif sink_type == NodeType.opengauss:
                each_mapping = LinkData.opengauss_mapping_data(src_table_id, sink_id, sink_table_name)
            elif sink_type == NodeType.redshift:
                each_mapping = LinkData.redshift_mapping_data(src_table_id, sink_id, sink_table_name)
            elif sink_type == NodeType.greenplum:
                each_mapping = LinkData.greenplum_mapping_data(src_table_id, sink_id, sink_table_name)
            elif sink_type == NodeType.sequoiadb:
                each_mapping = LinkData.sequoiadb_mapping_data(src_table_id, sink_id, sink_table_name)
            elif sink_type == NodeType.hive:
                if detail_db_type.lower() == 'csv':
                    each_mapping = LinkData.hive_csv_mapping_data(src_table_id, sink_id, sink_table_name)
                elif detail_db_type.lower() == 'parquet':
                    each_mapping = LinkData.hive_parquet_mapping_data(src_table_id, sink_id, sink_table_name)
                elif detail_db_type.lower() == 'avro':
                    each_mapping = LinkData.hive_avro_mapping_data(src_table_id, sink_id, sink_table_name)
                else:
                    each_mapping = LinkData.hive_orc_mapping_data(src_table_id, sink_id, sink_table_name)
            elif sink_type == NodeType.inceptor:
                if detail_db_type.lower() == 'csv':
                    each_mapping = LinkData.inceptor_csv_mapping_data(src_table_id, sink_id, sink_table_name)
                elif detail_db_type.lower() == 'parquet':
                    each_mapping = LinkData.inceptor_parquet_mapping_data(src_table_id, sink_id, sink_table_name)
                elif detail_db_type.lower() == 'avro':
                    each_mapping = LinkData.inceptor_avro_mapping_data(src_table_id, sink_id, sink_table_name)
                else:
                    each_mapping = LinkData.inceptor_orc_mapping_data(src_table_id, sink_id, sink_table_name)
            elif sink_type == NodeType.ftp:
                if not filed_mappings:
                    raise FiledMappingNotFoundException('创建Sink为Ftp的任务必须提供字段映射关系')
                if detail_db_type.lower() == 'csv':
                    each_mapping = LinkData.ftp_csv_mapping_data(
                        src_table_id, sink_id, sink_table_name, filed_mappings[src_table_id])
                else:
                    each_mapping = LinkData.ftp_json_mapping_data(
                        src_table_id, sink_id, sink_table_name, filed_mappings[src_table_id])
            elif sink_type == NodeType.hdfs:
                if not filed_mappings:
                    raise FiledMappingNotFoundException('创建Sink为HDFS的任务必须提供字段映射关系')
                if detail_db_type.lower() == 'csv':
                    each_mapping = LinkData.hdfs_csv_mapping_data(
                        src_table_id, sink_id, sink_table_name, filed_mappings[src_table_id])
                else:
                    each_mapping = LinkData.hdfs_avro_mapping_data(
                        src_table_id, sink_id, sink_table_name, filed_mappings[src_table_id])
            elif sink_type == NodeType.kafka:
                if not filed_mappings:
                    raise FiledMappingNotFoundException('创建Sink为Kafka的任务必须提供字段映射关系')
                each_mapping = LinkData.kafka_mapping_data(
                    src_table_id, sink_id, sink_table_id, filed_mappings[src_table_id])
            elif sink_type == NodeType.redis:
                if not filed_mappings:
                    raise FiledMappingNotFoundException('创建Sink为Redis的任务必须提供字段映射关系')
                each_mapping = LinkData.redis_mapping_data(
                    src_table_id, sink_id, sink_table_name, filed_mappings[src_table_id])
            else:
                raise DBNotSupportedException('暂不支持该数据库操作，数据库：{0}-{1}'.
                                              format(source_type.name, sink_type.name))
            link['mappings'].append(each_mapping)
        return link

    @staticmethod
    def sink_node_policy_config(
            node_id, relation_id, entity_deleted="PAUSE", entity_field_added="PAUSE", entity_field_deleted="PAUSE",
            entity_field_modified="PAUSE",
            increment_data_mode="FULL", mapping_insert="MERGE", mapping_update="MERGE", mapping_delete="DELETE",
            primary_key_conflict="PAUSE"):
        return {
            "sinkNodes": [
                {
                    "node": int(node_id),
                    "relationId": int(relation_id),
                    "policyConfig": {
                        "@class": 'com.datapipeline.internal.bean.DataNodeRelationSinkPolicyConfig',
                        "entityDeleted": entity_deleted,
                        "entityFieldAdded": entity_field_added,
                        "entityFieldDeleted": entity_field_deleted,
                        "entityFieldModified": entity_field_modified,
                        "incrementData": {
                            "incrementDataMode": increment_data_mode,
                            "loadMethodMapping": {
                                "insert": mapping_insert,
                                "update": mapping_update,
                                "delete": mapping_delete
                            },
                            "primaryKeyConflict": primary_key_conflict
                        }
                    }
                }
            ]
        }
