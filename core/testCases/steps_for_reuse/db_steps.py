#!/usr/bin/env python
# encoding: utf-8
from core.testCases.testbase import *


def create_source_table(ddl_files: list, source_table_names: list, source_db_type: str, data_count=10):
    for index, table in enumerate(source_table_names):
        source = create_db_instance(source_db_type, table, True)
        source.download_path = TestBase.report_path
        if source_db_type == 'inceptor_orc' or source_db_type == 'hive_orc':
            source.buckets = 2
            source.clustered = 'dpid'
        source.create_table(read_yml_ddl_file(ddl_files[index]))
        source.insert_data(data_count)


def compare_db_data_and_schema(node_api: NodeApi, link_api: LinkApi, link_id: str, sink_node_id: str,
                               source_tables: list, source_db_type: str, source_type: NodeType,
                               sink_tables: list, sink_db_type: str, sink_type: NodeType,
                               order_by=None, unique_by=None):
    # 生成源端库、目的端库操作实例对象
    schema_fail_count = 0
    data_fail_count = 0
    for index, _ in enumerate(source_tables):
        source = create_db_instance(source_db_type, source_tables[index][0], True)
        sink = create_db_instance(sink_db_type, sink_tables[index][0], False)
        source.download_path = TestBase.report_path
        sink.download_path = TestBase.report_path

        if (sink_type == NodeType.kafka and sink_db_type.find('json') != -1) or \
                (sink_type == NodeType.hdfs and sink_db_type.find('csv') != -1) or \
                sink_type == NodeType.redis or sink_type == NodeType.sequoiadb or sink_type == NodeType.ftp:
            # 获取链路表映射字段信息，用于传递给下游数据库进行数据匹配、校验
            # node_api.refresh_node(sink_node_id)
            entities = node_api.get_node_existing_entity_tables(sink_node_id, sink_type, False,
                                                                supported_tables=[sink_tables[index][0]])
            sink.table_info = link_api.get_filed_mapping_column_info(
                link_id, source_tables[index][1], entities[0][1], sink_node_id, False)

        # 校验对比表结构以及所有数据字段
        db = DBOperator(source, sink, TestBase.data_path)
        if source_type == NodeType.kafka and source_db_type.find('json') != -1:
            source_db_data, sink_db_data = db.get_table_schema_and_data_for_kafka_source(order_by=order_by,
                                                                                         unique_by=unique_by)
        elif source_type == NodeType.kafka and source_db_type.find('avro') != -1:
            source_db_data, sink_db_data = db.get_table_schema_and_data_for_kafka_avro_source(order_by=order_by,
                                                                                              unique_by=unique_by)
        else:
            source_db_data, sink_db_data = db.get_table_schema_and_data(order_by=order_by, unique_by=unique_by)
        schema_fail_count += db.compare_schema(source_db_data, sink_db_data)
        data_fail_count += db.compare_data(source_db_data, sink_db_data)
    return schema_fail_count, data_fail_count
