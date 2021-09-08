#!/usr/bin/env python
# encoding: utf-8
import collections
from decimal import *
from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic as AvroTopic
from confluent_kafka.avro import AvroProducer, AvroConsumer
from confluent_kafka import TopicPartition
from enum import Enum
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient, Schema
from kafka import KafkaConsumer, KafkaAdminClient, KafkaProducer
from kafka.admin.new_topic import NewTopic
from core.logics.db.db_driver import *


class KafkaData:
    """
    随机生成测试数据模块
    """

    @staticmethod
    def int_data(precision, scale, unsigned, **kwargs):
        if unsigned:
            return random_int(0, 4294967295)
        else:
            return random_int(-2147483648, 2147483647)

    @staticmethod
    def long_data(precision, scale, unsigned, **kwargs):
        if unsigned:
            return int(random_int(0, 18446744073709551615))
        else:
            return int(random_int(-9223372036854775808, 9223372036854775807))

    @staticmethod
    def string_data(precision, scale, unsigned, **kwargs):
        return random_str_without_punctuation(20)

    @staticmethod
    def boolean_data(precision, scale, unsigned, **kwargs):
        if random_int(0, 1) == 0:
            return False
        else:
            return True

    @staticmethod
    def decimal_data(precision, scale, unsigned, **kwargs):
        if precision is None:
            return float(random_decimal(5, 2, -1000, 1000, unsigned))
        else:
            return Decimal(random_decimal(precision, scale, -1000, 1000, unsigned))

    @staticmethod
    def float_data(precision, scale, unsigned, **kwargs):
        return float(random_decimal(8, 2, -1000000, 1000000, unsigned))

    @staticmethod
    def double_data(precision, scale, unsigned, **kwargs):
        return float(random_decimal(8, 4, -9999, 9999, unsigned))

    @staticmethod
    def bytes_data(precision, scale, unsigned, **kwargs):
        return random_bytes(20)


class KafkaType(Enum):
    json = 0
    avro = 1


class KafkaDB(DBDriver):
    ALIAS = 'KAFKA'
    NUM_PARTITIONS = 1
    REPLICATION_FACTOR = 1

    # Kafka-json所需的Schema关联topic后缀
    JSON_SCHEMA_TOPIC = '-schema'
    AVRO_SCHEMA_TOPIC = '-value'

    # DP sink schema特殊标识，当遇到时主动去除
    DP_SINK_SCHEMA_MARK = 'op_'

    # Avro schema字段信息
    AVRO_SCHEMA_NAME = 'value'
    AVRO_SCHEMA_TYPE = 'record'

    def __init__(self, db_config: dict, topic, kafka_type: KafkaType, auto_offset_reset='smallest', consumer_timeout_ms=2000, encoding='utf8'):
        self.host = db_config["host"]
        self.schema_registry_url = db_config["schema_registry"]
        self.table_name = topic
        self.auto_offset_reset = auto_offset_reset
        self.consumer_timeout_ms = consumer_timeout_ms
        self.encoding = encoding
        self.kafka_type = kafka_type

        # Kafka-json需要schema topic用于描述对应数据的结构、字段信息
        # 虽然json并没有强行数据要求限制，但为了拓展下游库数据转换、高级清洗等测试逻辑
        # 需要限制json中某一字段仅会生成属于一种类型的数据，所以需要额外存放、提供schema信息
        # 作为上游库时且通过框架创建表时将会自动创建该schema topic
        # 可通过该文件获取字段信息，用于生成随机数据
        self.table_schema_name = topic + self.JSON_SCHEMA_TOPIC

        # Kafka-avro存储schema的subject名字
        self.avro_schema_name = topic + self.AVRO_SCHEMA_TOPIC

        # Kafka-avro存储schema的默认兼容性限制, 有效数据：
        # None: 不做兼容性检查
        # All：BACKWARD + FORWARD
        # BACKWARD：向后兼容所有数据结构
        # FORWARD: 向前兼容所有数据结构
        self.compatibility = 'None'

        # 如作为下游库或没有该schema信息文件，需显示将self.table_info赋值为DBTable对象
        self.table_info = None

        self.producer = self.__connect_producer()
        self.admin = self.__connect_admin()

    def __del__(self):
        try:
            self.producer.close()
        except:
            pass

        try:
            self.admin.close()
        except:
            pass

    def __connect_consumer(self, topic):
        print('连接Kafka Consumer, bootstrap_servers: {0}, topic: {1}, auto_offset_reset: {2}, '
              'consumer_timeout_ms: {3}, schema registry:{4}'.format(
                self.host, topic, self.auto_offset_reset, self.consumer_timeout_ms, self.schema_registry_url))
        if self.kafka_type == KafkaType.json:
            db = KafkaConsumer(topic, bootstrap_servers=self.host, auto_offset_reset=self.auto_offset_reset,
                               consumer_timeout_ms=self.consumer_timeout_ms)
        else:
            db = AvroConsumer({'bootstrap.servers': self.host, 'group.id': 'automation',
                               'auto.offset.reset': self.auto_offset_reset,
                               'api.version.request': True, 'schema.registry.url': self.schema_registry_url})
            db.subscribe([topic])
            db.assign([TopicPartition(topic, 0, 0)])
        return db

    def __connect_producer(self):
        print('连接Kafka Producer, bootstrap_servers: {0}, schema registry: {1}'.format(
            self.host, self.schema_registry_url))
        if self.kafka_type == KafkaType.json:
            db = KafkaProducer(bootstrap_servers=self.host,
                               key_serializer=lambda k: json.dumps(k).encode(),
                               value_serializer=lambda v: json.dumps(v).encode())
        else:
            def delivery_report(err, msg):
                if err is not None:
                    raise ('Message delivery failed: {}'.format(err))
            db = AvroProducer({'bootstrap.servers': self.host, 'on_delivery': delivery_report,
                               # schema.registry.auto.register.schemas 修改为 False
                               # 代表每次提交kafka-avro数据时不自动注册新schema
                               # 既不允许因用户写入的数据不满足历史schema要求而重新注册
                               'schema.registry.auto.register.schemas': False,
                               'schema.registry.url': self.schema_registry_url})
        return db

    def __connect_admin(self):
        print('连接Kafka admin, bootstrap_servers: {0}'.format(self.host))
        if self.kafka_type == KafkaType.json:
            db = KafkaAdminClient(bootstrap_servers=self.host)
        else:
            db = AdminClient({'bootstrap.servers': self.host})
        return db

    def __connect_schema_registry(self):
        print('连接Kafka SchemaRegistry, url: {0}'.format(self.schema_registry_url))
        db = SchemaRegistryClient({'url': self.schema_registry_url})
        return db

    @db_call
    def __fetch_all_data(self):
        ret = []
        consumer = self.__connect_consumer(self.table_name)
        results = []
        # json则消费所有数据，转为有序字典
        if self.kafka_type == KafkaType.json:
            for msg in consumer:
                ret.append(msg.value)
            for msg in ret:
                result_dict = format_json(msg.decode(self.encoding), lower_key=False, encoding=self.encoding)
                results.append(list(result_dict.values()))
        # avro则消费所有数据，转为有序字典
        else:
            while True:
                msg = consumer.poll(int(self.consumer_timeout_ms/1000))
                if msg and not msg.error():
                    ret.append(msg.value())
                    consumer.commit(msg)
                elif msg:
                    raise msg.error()
                else:
                    break
            for msg in ret:
                results.append(list(msg.values()))
            consumer.commit()
        print('RET: {0}'.format(results))
        consumer.close()
        return results

    @db_call
    def __fetch_schema_data(self):
        # json则从额外的schema topic获取schema信息
        if self.kafka_type == KafkaType.json:
            results = []
            schema = self.__connect_consumer(self.table_schema_name)
            for msg in schema:
                results.append(msg.value)
            schema.close()
            assert len(results) > 0, '无法获取当前主题：{0}，有效的Schema结构，Schema主题：{1}'.format(
                self.table_name, self.table_schema_name)
            print('RET: {0}'.format(results))
            return results
        # avro则从schema_registry服务获取最新的schema信息
        # TODO: 考虑返回所有schema version
        else:
            schema = self.__connect_schema_registry()
            version = schema.get_latest_version(self.avro_schema_name)
            schema = version.schema.schema_str
            results = format_json(schema, lower_key=False, encoding=self.encoding)
            print('RET: {0}'.format(schema))
            return results

    @db_step('获取Kafka结构')
    def get_table_info(self):
        print('主题: {0}'.format(self.table_name))
        # 如果提供了self.table_info，则直接返回
        if self.table_info is not None:
            return self.table_info

        ret = self.__fetch_schema_data()
        # json则返回最新的schema信息
        if self.kafka_type == KafkaType.json:
            table_info = load_ddl_data(json.loads(ret[-1]))
            for column in table_info.columns:
                column.data_type = column.column_type
            return table_info
        # avro则返回最新的schema信息
        columns = []
        # 如果字包含DP_SINK_SCHEMA_MARK则去除
        if ret['fields'][0]['name'] == self.DP_SINK_SCHEMA_MARK:
            ret = ret['fields'][1:]
        else:
            ret = ret['fields'][0:]
        for each in ret:
            name = each['name'].lower()
            default = each['default'] if 'default' in each.keys() else None
            not_null = 'null' not in each['type']
            valid_type = list(filter(lambda x: x != 'null', each['type']))[0]
            # 基于数据类型，精度、标度获取位置不同
            if type(valid_type) == dict or type(valid_type) == collections.OrderedDict:
                data_type = valid_type['connect.type'] if 'connect.type' in valid_type.keys() else valid_type['type']
                if data_type == 'fixed':
                    data_type = valid_type
            else:
                data_type = valid_type
            auto_inc = False
            unsigned = None
            if type(data_type) == collections.OrderedDict:
                column_type = dict(data_type)
                pre = data_type.get('precision', None)
                sca = data_type.get('scale', None)
                data_type = data_type.get('logicalType', None)
            else:
                column_type = data_type
                pre = None
                sca = None
            column = DBColumn(name, data_type, column_type, default, not_null, auto_inc, pre, sca, unsigned)
            columns.append(column)
        table_info = DBTable(self.ALIAS, self.table_name, columns, [])
        return table_info

    def _is_sink_avro(self):
        ret = self.__fetch_schema_data()
        # 如果字包含DP_SINK_SCHEMA_MARK则去除
        if ret['fields'][0]['name'] == self.DP_SINK_SCHEMA_MARK:
            return True
        else:
            return False

    @db_step('获取Kafka所有数据')
    def get_table_data(self):
        print('主题: {0}'.format(self.table_name))
        results = self.__fetch_all_data()
        if self.kafka_type == KafkaType.avro and self._is_sink_avro():
            return [x[1:] for x in results]
        else:
            return results

    @db_step('删除Kafka schema registry subject')
    def delete_schema_registry_subject(self, raise_error=True):
        print('主题: {0}, 删除schema registry subject: {1}'.format(self.table_name, self.avro_schema_name))
        try:
            schema = self.__connect_schema_registry()
            schema.delete_subject(self.avro_schema_name)
        except Exception as e:
            print(e)
            if raise_error:
                raise e

    @db_step('删除Kafka Topic')
    def delete_table(self, raise_error=True):
        print('主题: {0}'.format(self.table_name))
        # 尝试重复删除topic直到其抛错，否则某些情况下topic无法成功删除
        if self.kafka_type == KafkaType.json:
            while True:
                try:
                    self.admin.delete_topics([self.table_name])
                except Exception as e:
                    print(e)
                    if raise_error:
                        raise e
                    break

            if self.table_info is None:
                while True:
                    try:
                        self.admin.delete_topics([self.table_schema_name])
                    except Exception as e:
                        print(e)
                        if raise_error:
                            raise e
                        break
        else:
            while True:
                try:
                    fa = self.admin.delete_topics([self.table_name])
                    for topic, f in fa.items():
                        f.result()
                except Exception as e:
                    print(e)
                    if raise_error:
                        raise e
                    break

    def _get_avro_schema(self, table_info):
        # 通过table_info转换为avro schema
        schema_json = collections.OrderedDict()
        schema_json['name'] = self.AVRO_SCHEMA_NAME
        schema_json['type'] = self.AVRO_SCHEMA_TYPE
        schema_json['fields'] = []
        for column in table_info.columns:
            field = {'name': column.name, 'type': []}
            field['type'].append(column.column_type)
            if not column.not_null:
                field['type'].append('null')
            if column.default is not None:
                field['default'] = column.default
            schema_json['fields'].append(field)
        return json.dumps(schema_json)

    def _commit_table_info(self, table_info):
        # json则将最新schema发送给schema topic
        if self.kafka_type == KafkaType.json:
            future = self.producer.send(self.table_schema_name, value=dump_ddl_data(table_info))
            future.get(timeout=2)
        # avro则将最新schema注册到schema_registry服务
        else:
            schema = self.__connect_schema_registry()
            schema.set_compatibility(self.avro_schema_name, self.compatibility)
            schema_str = self._get_avro_schema(table_info)
            schema_id = schema.register_schema(self.avro_schema_name,
                                               Schema(schema_str=schema_str, schema_type='AVRO'))
            print('Schema ID: {0}'.format(schema_id))

    @db_step('创建Kafka表')
    def create_table(self, table_info: DBTable):
        print('主题: {0}'.format(self.table_name))
        # json则创建topic以及schema topic
        if self.kafka_type == KafkaType.json:
            # table_info为空时只创建topic
            if table_info is None:
                self.admin.create_topics([NewTopic(self.table_name, self.NUM_PARTITIONS, self.REPLICATION_FACTOR)])
            else:
                if table_info.db.upper() != self.ALIAS:
                    raise DBNotMatchingException('当前DB实例为：{0}, 与所要操作的数据库：{1}, 不匹配'.format(
                        self.ALIAS, table_info.db))
                self.admin.create_topics([NewTopic(self.table_name, self.NUM_PARTITIONS, self.REPLICATION_FACTOR),
                                          NewTopic(self.table_schema_name, self.NUM_PARTITIONS, self.REPLICATION_FACTOR)])
                self._commit_table_info(table_info)
        # avro则只创建topic
        else:
            fs = self.admin.create_topics([AvroTopic(self.table_name, self.NUM_PARTITIONS, self.REPLICATION_FACTOR)])
            for topic, f in fs.items():
                f.result()
            if table_info is not None:
                self._commit_table_info(table_info)

    @db_step('重命名Kafka表')
    def rename_table(self, new_table_name: str):
        raise NotImplementedError

    @db_step('增加Kafka表字段')
    def add_column(self, column_info: DBColumn):
        print('主题: {0}, 新增字段：{1}'.format(self.table_name, column_info.name))
        table_info = self.get_table_info()
        table_info.columns.append(column_info)
        self._commit_table_info(table_info)

    @db_step('更新Kafka表字段')
    def update_column(self, column_info: DBColumn):
        print('主题: {0}, 修改字段：{1}'.format(self.table_name, column_info.name))
        table_info = self.get_table_info()
        for column in table_info.columns:
            if column.name == column_info.name:
                column.data_type = column_info.data_type
                column.column_type = column_info.column_type
                column.default = column_info.default
                column.not_null = column_info.not_null
                column.auto_inc = column_info.auto_inc
                column.precision = column_info.precision
                column.scale = column_info.scale
                column.unsigned = column_info.unsigned
        self._commit_table_info(table_info)

    @db_step('删除Kafka表字段')
    def delete_column(self, column_name: str):
        print('主题: {0}, 字段名：{1}'.format(self.table_name, column_name))
        table_info = self.get_table_info()
        columns = []
        for column in table_info.columns:
            if column.name == column_name:
                continue
            columns.append(column)
        table_info.columns = columns
        self._commit_table_info(table_info)

    @db_step('重命名Kafka表字段')
    def rename_column(self, old_column_name: str, new_column_name: str):
        print('主题: {0}, 字段名：{1}, 重命名为：{2}'.format(self.table_name, old_column_name, new_column_name))
        table_info = self.get_table_info()
        for column in table_info.columns:
            if column.name == old_column_name:
                column.name = new_column_name
        self._commit_table_info(table_info)

    @db_step('增加Kafka表主键')
    def add_primary_key(self, keys: list):
        raise NotImplementedError

    @db_step('删除Kafka表主键')
    def delete_primary_key(self):
        raise NotImplementedError

    def _upload_data_to_server(self, table_data, table_info):
        table_dict_data = []
        for data in table_data:
            data_dict = collections.OrderedDict()
            for index, column in enumerate(table_info.columns):
                data_dict[column.name] = data[index]
            table_dict_data.append(data_dict)

        if self.kafka_type == KafkaType.json:
            for data in table_dict_data:
                # TODO： 考虑增加Key作为版本控制, source为kafka json，下游对齐数据与schema时，需要考虑Schema版本（版本可通过produce设置key.value区分）
                future = self.producer.send(self.table_name, value=data)
                future.get(timeout=5)
        else:
            schema = self.__connect_schema_registry()
            version = schema.get_latest_version(self.avro_schema_name)
            value_schema = avro.loads(version.schema.schema_str)
            for data in table_dict_data:
                # TODO： 考虑增加Key作为版本控制, source为kafka json，下游对齐数据与schema时，需要考虑Schema版本（版本可通过produce设置key.value区分）
                self.producer.produce(topic=self.table_name, value=data, value_schema=value_schema)
                self.producer.flush(5)

    @db_step('插入Kafka表数据')
    def insert_data(self, count: int):
        print('主题: {0}, 插入数据'.format(self.table_name))
        # 获取DBTable对象
        table_info = self.get_table_info()

        # 获取当前所有数据
        table_data = self.get_table_data()

        # 插入多行数据
        new_table_data = []
        for _ in range(count):
            values = []
            for index, column in enumerate(table_info.columns):
                if self.kafka_type == KafkaType.json:
                    function_name = column.column_type + '_data'
                    # 自增字段将基于最大数据id
                    if column.auto_inc:
                        if len(table_data) > 0:
                            last_id = table_data[-1][index]
                        else:
                            last_id = -1
                        values.append(int(last_id) + 1)
                else:
                    function_name = column.data_type + '_data'
                # 如果字段名为SORTED_AND_UNIQUE_COLUMN_NAME，将会随机生成唯一值
                if column.name == SORTED_AND_UNIQUE_COLUMN_NAME:
                    values.append(random_sorted_and_unique_num())
                # 如果字段允许为空，则有概率插入None
                elif not column.not_null and random_int(0, 9) == 0:
                    values.append(None)
                # 基于数据类型随机生成符合类型、精度、标度的数据
                else:
                    random_value = getattr(KafkaData, function_name)(
                        column.precision, column.scale, column.unsigned, column_type=column.column_type)
                    values.append(random_value)
            new_table_data.append(values)
            table_data.append(values)
        self._upload_data_to_server(new_table_data, table_info)

    @db_step('手动插入Kafka表数据')
    def manual_insert_data(self, column_names: list, data_values: list):
        print('表名: {0}, 手动插入数据'.format(self.table_name))
        assert len(column_names) == len(data_values), '列数量与列名数量不匹配'
        # 获取DBTable对象
        table_info = self.get_table_info()

        # 获取当前所有数据
        table_data = self.get_table_data()

        # 插入多行数据
        values = []
        new_table_data = []
        for index, column in enumerate(table_info.columns):
            # 自增字段将基于最大数据id
            if column.auto_inc:
                if len(table_data) > 0:
                    last_id = table_data[-1][index]
                else:
                    last_id = -1
                values.append(int(last_id) + 1)
            # 如果字段名为SORTED_AND_UNIQUE_COLUMN_NAME，将会随机生成唯一值
            elif column.name == SORTED_AND_UNIQUE_COLUMN_NAME:
                values.append(random_sorted_and_unique_num())
            # 插入用户指定数据
            elif column.name in [x.lower() for x in column_names]:
                index = [x.lower() for x in column_names].index(column.name)
                values.append(data_values[index])
            # 未指定则插入None
            else:
                values.append(None)
        table_data.append(values)
        new_table_data.append(values)
        self._upload_data_to_server(new_table_data, table_info)

    @db_step('更新Kafka表数据')
    def update_data(self, count_or_condition):
        raise NotImplementedError

    @db_step('手动更新Kafka表数据')
    def manual_update_data(self, *args, **kwargs):
        raise NotImplementedError

    @db_step('删除Kafka表数据')
    def delete_data(self, count_or_condition):
        raise NotImplementedError

    @db_step('判断HDFS表是否存在')
    def is_table_exist(self):
        print('判断表名: {0}, 是否存在'.format(self.table_name))
        if self.kafka_type == KafkaType.json:
            topics = self.admin.list_topics()
            return self.table_name in topics
        else:
            topics = self.admin.list_topics()
            return self.table_name in topics.topics.keys()

    @db_step('判断HDFS列是否存在')
    def is_column_exist(self, column_info: DBColumn):
        print('判断表名: {0}, 列名：{1}，是否存在'.format(self.table_name, column_info.name))
        return self.get_table_info().is_column_exist(column_info)
