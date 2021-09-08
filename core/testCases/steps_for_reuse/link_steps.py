#!/usr/bin/env python
# encoding: utf-8
from core.testCases.testbase import *


def create_single_basic_link(link_api: LinkApi, link_task_name: str,
                             source_type: NodeType, source_tables: list, source_node_id: str,
                             sink_type: NodeType, sink_tables: list, sink_node_id: str, is_real: bool, sink_db_type: str):
    """创建链路"""
    # 创建链路
    filed_mappings = {}
    detail_db_type = None
    link_id = link_api.create_link(link_task_name)
    # 拼接链路所需源端库、目的端库、表映射等信息，并返回接口payload
    for src_table in source_tables:
        if (sink_type == NodeType.ftp or sink_type == NodeType.hdfs) and sink_db_type.find('csv') != -1:
            filed_mappings[src_table[1]] = link_api.get_filed_mapping_info(
                link_id, src_table[1], sink_node_id, 'CSV')
            detail_db_type = 'csv'
        elif sink_type == NodeType.ftp and sink_db_type.find('json') != -1:
            filed_mappings[src_table[1]] = link_api.get_filed_mapping_info(
                link_id, src_table[1], sink_node_id, 'JSON')
            detail_db_type = 'json'
        elif sink_type == NodeType.hdfs and sink_db_type.find('avro') != -1:
            filed_mappings[src_table[1]] = link_api.get_filed_mapping_info(
                link_id, src_table[1], sink_node_id, 'AVRO')
            detail_db_type = 'avro'
        elif sink_type == NodeType.kafka or sink_type == NodeType.redis:
            filed_mappings[src_table[1]] = link_api.get_filed_mapping_info(
                link_id, src_table[1], sink_node_id)
        elif sink_type == NodeType.hive or sink_type == NodeType.inceptor:
            detail_db_type = sink_db_type.split('_')[1]
    payload = LinkData.default_link_data(src_id=source_node_id, sink_id=sink_node_id,
                                         source_type=source_type, sink_type=sink_type,
                                         is_real=is_real,
                                         source_tables=source_tables,
                                         sink_tables=sink_tables, filed_mappings=filed_mappings,
                                         detail_db_type=detail_db_type)

    # 更新链路
    link_api.update_link(link_id, payload)
    link_api.create_sink_tables(link_id)

    # 更新Kafka avro高级清洗策略
    if sink_type == NodeType.kafka and sink_db_type.find('avro') != -1:
        for index, _ in enumerate(source_tables):
            link_entity_mapping_id = link_api.get_filed_mapping_id(link_id, source_tables[index][1], sink_tables[index][1], sink_node_id)
            payload = LinkData.kafka_avro_code_engine(link_id, link_entity_mapping_id)
            link_api.add_code_engine(payload)

    # 更新链路加载策略
    if (sink_type != NodeType.sequoiadb and sink_type != NodeType.redis) and \
            (source_type == NodeType.ftp or source_type == NodeType.hdfs or
             source_type == NodeType.hive or source_type == NodeType.inceptor or
             source_type == NodeType.kafka):
        res = link_api.get_link_info_by_id(link_id)
        relation_id = res['sinkNodes'][0]['relationId']
        payload = LinkData.sink_node_policy_config(node_id=sink_node_id, relation_id=relation_id,
                                                   mapping_insert='INSERT', primary_key_conflict="PAUSE")
        link_api.update_link(link_id, payload)

    # 激活链路
    link_api.active_link(link_id)
    link_mappings = link_api.get_link_mapping_info(link_id)
    return link_id, link_mappings
