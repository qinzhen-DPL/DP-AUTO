#!/usr/bin/env python
# encoding: utf-8
from core.testCases.testbase import *


def init_single_source_sink_node_basic_info(node_api: NodeApi, source_table_names: list, ddl_files: list,
                                            source_db_type: str, source_name: str, source_type: NodeType,
                                            sink_db_type: str, sink_name: str, sink_type: NodeType,
                                            is_real: bool, now: str):
    """获取节点基本信息"""
    # 获取测试用源端与目的端节点，并校验该节点存在
    nodes = node_api.get_node_list()
    nodes_mapping = {node['name']: node['id'] for node in nodes}
    assert source_name in nodes_mapping.keys(), '源端数据库节点不存在，名称：{0}'.format(source_name)
    assert sink_name in nodes_mapping.keys(), '下游数据库节点不存在，名称：{0}'.format(sink_name)
    source_node_id = nodes_mapping[source_name]
    sink_node_id = nodes_mapping[sink_name]

    # FTP, HDFS创建源文件信息
    for index, table in enumerate(source_table_names):
        table_info = read_yml_ddl_file(ddl_files[index])
        if source_type == NodeType.ftp and source_db_type.find('csv') != -1:
            node_api.create_node_entity(source_node_id, source_type, 'csv', table,
                                        [x.name for x in table_info.columns])
        elif source_type == NodeType.ftp and source_db_type.find('json') != -1:
            from core.logics.db.dbs.ftp_db import FtpData
            example_file = os.path.join(TestBase.report_path, table + '.json')
            example_dict = {}
            for column in table_info.columns:
                random_value = getattr(FtpData, column.column_type + '_data')(
                    column.precision, column.scale, column.unsigned, column_type=column.column_type)
                example_dict[column.name] = random_value
            with open(example_file, 'w', encoding='utf8') as file:
                file.write(json.dumps(example_dict, ensure_ascii=False))
            file_id = node_api.update_node_file(example_file, 'SSO', source_node_id)
            os.remove(example_file)
            node_api.create_node_entity(source_node_id, source_type, 'json', table, file_id)
        elif source_type == NodeType.hdfs and source_db_type.find('csv') != -1:
            node_api.create_node_entity(source_node_id, source_type, 'csv', table,
                                        [x.name for x in table_info.columns])
        elif source_type == NodeType.hdfs and source_db_type.find('avro') != -1:
            from avro.datafile import DataFileWriter
            from avro.io import DatumWriter
            from core.logics.db.dbs.hdfs_db import HDFSData
            import collections
            db = create_db_instance('hdfs_avro', table, True)
            schema = db._get_avro_schema(db.get_table_info())
            example_file = os.path.join(TestBase.report_path, table + '.avsc')
            with DataFileWriter(open(example_file, "wb"), DatumWriter(), schema) as writer:
                data_dict = collections.OrderedDict()
                for column in table_info.columns:
                    random_value = getattr(HDFSData, column.column_type + '_data')(
                        column.precision, column.scale, column.unsigned, column_type=column.column_type)
                    data_dict[column.name] = random_value
                writer.append(data_dict)
            file_id = node_api.update_node_file(example_file, 'SSO', source_node_id)
            os.remove(example_file)
            node_api.create_node_entity(source_node_id, source_type, 'avro', table, file_id)

    # 获取源端数据库测试用表，如不存在则终止测试
    node_api.refresh_node(source_node_id)
    source_tables = node_api.get_node_existing_entity_tables(source_node_id, source_type, True, source_table_names)
    print('源端数据库测试表：{0}'.format(source_tables))
    assert len(source_tables) == len(source_table_names), '源端没有可测试表存在'

    # 根据源端测试表，为目的端写入表命名
    sink_tables = []
    for index, table in enumerate(source_tables):
        sink_table_name = naming_sink_table(table[0], source_db_type, sink_db_type, is_real=is_real, now=now)
        # 如果目的端为Kafka则创建该Topic，并获取entity_id
        if sink_type == NodeType.kafka:
            kafka = create_db_instance(sink_db_type, sink_table_name, False)
            kafka.create_table(None)
            node_api.refresh_node(sink_node_id)
            entities = node_api.get_node_existing_entity_tables(sink_node_id, sink_type, False,
                                                                supported_tables=[sink_table_name])
            print('Kafka数据库测试Topic：{0}'.format(entities))
            assert len(entities) == 1, 'Kafka没有可测试Topic存在'
            sink_tables.extend(entities)
            if sink_db_type.find('avro') != -1:
                node_api.update_kafka_topic_type(entities[0][1], sink_node_id)
        else:
            sink_tables.append([sink_table_name, None])

        if source_type == NodeType.kafka and source_db_type.find('avro') != -1:
            node_api.refresh_node(source_node_id)
            entities = node_api.get_node_existing_entity_tables(source_node_id, source_type, True,
                                                                supported_tables=[table[0]])
            print('Kafka数据库测试Topic：{0}'.format(entities))
            assert len(entities) == 1, 'Kafka没有可测试Topic存在'
            node_api.update_kafka_topic_type(entities[0][1], source_node_id)
    return source_node_id, sink_node_id, source_tables, sink_tables




