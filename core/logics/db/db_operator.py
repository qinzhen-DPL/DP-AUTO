#!/usr/bin/env python
# encoding: utf-8
import re
import json
import traceback
import cx_Oracle
import datetime
from core import settings
from core.utils.common import get_xls
from core.utils.wait import Wait
from core.exceptions.regression_related_exception import *
from core.logics.db.db_driver import DBDriver, DBTable, DBColumn
from core.logics.db.db_compare_rule import DBSchemaCompareRule, DBValueCompareRule
from core.logics.db.excepted_condition import *


COMPARE_LOGGING_START = '-' * 10 + 'DB-OPERATOR-START' + '-' * 10
COMPARE_LOGGING_END = '-' * 10 + 'DB-OPERATOR-END' + '-' * 10


def compare_call(func):
    """
    数据库调用装饰器，用于打印数据库行为
    """
    def inner_wrapper(*args, **kwargs):
        print(COMPARE_LOGGING_START)
        start_time = datetime.datetime.now()
        has_error = None
        ret = None
        try:
            ret = func(*args, **kwargs)
        except Exception as e:
            has_error = e
        stop_time = datetime.datetime.now()
        ms = (stop_time - start_time).microseconds / 1000
        print('Compare time: {0}ms'.format(str(ms)))
        print(COMPARE_LOGGING_END)
        if has_error:
            raise has_error
        return ret
    return inner_wrapper


class DBOperator:

    def __init__(self, source_db: DBDriver, sink_db: DBDriver,
                 data_path, excel_name='DefaultMapping.xlsx'):
        self.source_db = source_db
        self.sink_db = sink_db
        self.data_path = data_path
        self.excel_name = excel_name

        # 默认数据库字段映射关系从excel文件读取，可在对象创建后重新赋值
        self.mapping = self.__get_db_mapping()

        # 默认数据库字段校验规则，可在对象创建后重新赋值
        self.schema_compare_rule = DBSchemaCompareRule.default_compare

        # 默认数据库值校验规则，可在对象创建后重新赋值
        self.value_compare_rule = DBValueCompareRule.default_compare

    def __get_db_mapping(self):
        """
        读取并返回数据字段默认映射表
        :return:
        """
        print('读取DB字段默认映射表数据')
        source_db_name = self.source_db.ALIAS
        sink_db_name = self.sink_db.ALIAS
        ret = get_xls(self.data_path, self.excel_name, source_db_name)
        # 检查表结构
        if len(ret) < 2:
            raise DBDefaultMappingException('数据库默认映射关系表结构异常，应至少包含表头在内的两行，表：{0}'.
                                            format(source_db_name))
        header = ret[0]
        body = ret[1:]
        for index, cell in enumerate(header):
            if cell == sink_db_name:
                sink_mapping_column = index
                break
        else:
            raise DBDefaultMappingException('数据库默认映射关系表结构异常，不存在目标库的映射关系，表：{0}，目标表: {1}'.
                                            format(source_db_name, sink_db_name))
        for row in body:
            if len(row) != len(header):
                raise DBDefaultMappingException('数据库默认映射关系表结构异常，行与表头的数量不符，表：{0}'.
                                                format(source_db_name))
        # 拼接表转为字典
        ret_dict = {}
        for row in body:
            row_name = str(row[0]).lower()
            sink_mapping_value = str(row[sink_mapping_column]).lower()
            ret_dict[row_name] = sink_mapping_value
        return {'source': source_db_name, 'sink': sink_db_name, 'mapping': ret_dict}

    def get_column_mapping(self, source_column_info):
        """
        基于原表字段类型，获取目标表数据类型
        :param source_column_info: 原表特定列的定义
        :return:
        """
        source_type = source_column_info.data_type.lower()
        source_precision = source_column_info.precision
        source_scale = source_column_info.scale

        if source_type not in self.mapping['mapping'].keys():
            return None

        # 获取目标字段类型
        # 如目标字段类型基于精度有不同对应类型，则适配条件, 以获得对应的 类型+精度+标度
        ret = self.mapping['mapping'][source_type]
        ret_lines = ret.splitlines()
        ret_lines_len = len(ret_lines)
        sink_type, sink_pre, sink_sac = "default", "default", "default"

        if ret_lines_len == 1:
            if ";" not in ret_lines[0]:
                sink_type, sink_pre, sink_sac = ret_lines[0], None, None
            else:
                sink_type, sink_pre, sink_sac = ret_lines[0].split(";")
        else:
            for index, a_line in enumerate(ret_lines):
                search_result = re.search(r'([^:;]*?):(.*)', a_line, re.I)
                if not search_result and index < ret_lines_len - 1:
                    raise Exception("mapping excel 单元格的值不合法: ", a_line)
                else:
                    a_condition = search_result.group(1)
                    a_result = search_result.group(2)
                    a_condition = a_condition.replace("精度", "source_precision")
                    a_condition = a_condition.replace("标度", "source_scale")
                    if eval(a_condition):
                        if ";" not in ret_lines[0]:
                            sink_type, sink_pre, sink_sac = ret_lines[0], None, None
                        else:
                            sink_type, sink_pre, sink_sac = a_result.split(";")
                        break
                if index == ret_lines_len - 1:
                    if re.match(r'^[^:;]*?;[^:;]*?;[^:;]*?$', a_line, re.I):
                        sink_type, sink_pre, sink_sac = a_line.split(";")
                    else:
                        raise Exception("mapping excel 单元格的值不合法: ", ret_lines)

        if sink_pre in ['x', 'X']:
            sink_pre = source_precision
        if sink_sac in ['x', 'X']:
            sink_sac = source_scale
        if sink_pre == "":
            sink_pre = None
        if sink_sac == "":
            sink_sac = None
        if sink_type == "default":
            raise Exception("mapping 获取失败!", {'source_type': source_type, 'source_precision': source_precision,
                                              'source_scale': source_scale})

        return DBColumn(source_column_info.name, sink_type, None, None, None, None, sink_pre, sink_sac, None)

    def combine_schema_and_data(self, table_info, lines, order_by=None, unique_by=None):
        """
        将字段类型与所有数据进行融合
        :param table_info: 所有字段类型
        :param lines: 所有数据
        :param order_by: 所有数据基于某字段升序排列
        :param unique_by: 所有数据基于某字段进行去重
        :return: 融合后的数据 [{dict}...]
        """
        # 如设置order_by则检验该字段是否存在
        if order_by:
            print('基于{0}字段对表所有数据排序'.format(order_by))
            columns = [x.name for x in table_info.columns]
            order_by = order_by.lower()
            if order_by not in columns:
                raise OrderByNotInTableColumnException('表排序字段不存在，排序字段: {0}，表字段: {1}'.
                                                       format(order_by, columns))
        # 如设置unique_by则检验该字段是否存在
        if unique_by:
            print('基于{0}字段对表所有数据去重'.format(unique_by))
            columns = [x.name for x in table_info.columns]
            unique_by = unique_by.lower()
            if unique_by not in columns:
                raise UniqueByNotInTableColumnException('表去重字段不存在，去重字段: {0}，表字段: {1}'.
                                                        format(unique_by, columns))
        results = []
        for line in lines:
            line_dict = {}
            for index, db_column in enumerate(table_info.columns):
                column_value = line[index]
                # TODO: 可将以下类型转换代码放在每个DB类中处理，DB类返回的对象即为经过处理后的str，后续所有排序、去重、比较均不需类型处理
                if isinstance(column_value, cx_Oracle.LOB):
                    try:
                        column_value = column_value.read()
                    except:
                        column_value = None
                if isinstance(column_value, memoryview):
                    column_value = column_value.tobytes()
                column_name = db_column.name.lower()
                line_dict[column_name] = {'info': db_column, 'value': column_value}
            # 如需去重，则基于判断该字段是否已存在
            if unique_by and (line_dict[unique_by] in [x[unique_by] for x in results]):
                continue
            results.append(line_dict)
        # 如需排除，则基于字段升序排列
        if order_by:
            results.sort(key=lambda x: x[order_by]['value'])
        return results

    def combine_schema_and_data_for_kafka_source(self, source_table_info, lines, order_by=None, unique_by=None):
        """
        将字段类型与所有数据进行融合，只针对于kafka源的数据
        尝试将kafka写入的k:v进行分列处理，基于列明单独比较
        :param source_table_info: 上游表字段类型
        :param lines: 所有数据
        :param order_by: 所有数据基于某字段升序排列
        :param unique_by: 所有数据基于某字段进行去重
        :return: 融合后的数据 [{dict}...]
        """
        # 如设置order_by则检验该字段是否存在
        if order_by:
            print('基于{0}字段对表所有数据排序'.format(order_by))
            columns = [x.name for x in source_table_info.columns]
            order_by = order_by.lower()
            if order_by not in columns:
                raise OrderByNotInTableColumnException('表排序字段不存在，排序字段: {0}，表字段: {1}'.
                                                       format(order_by, columns))
        # 如设置unique_by则检验该字段是否存在
        if unique_by:
            print('基于{0}字段对表所有数据去重'.format(unique_by))
            columns = [x.name for x in source_table_info.columns]
            unique_by = unique_by.lower()
            if unique_by not in columns:
                raise UniqueByNotInTableColumnException('表去重字段不存在，去重字段: {0}，表字段: {1}'.
                                                        format(unique_by, columns))
        results = []
        for line in lines:
            line_dict = {}
            line_data = line[1]
            # TODO: 可将以下类型转换代码放在每个DB类中处理，DB类返回的对象即为经过处理后的str，后续所有排序、去重、比较均不需类型处理
            if isinstance(line_data, cx_Oracle.LOB):
                line_data = line_data.read()
            line_ori_dict = json.loads(line_data)
            for column in source_table_info.columns:
                column_name = column.name.lower()
                # TODO: source为kafka json，下游对齐数据与schema时，需要考虑Schema版本（版本可通过produce设置key.value区分）
                line_dict[column_name] = {'info': column, 'value': line_ori_dict[column_name]}
            # 如需去重，则基于判断该字段是否已存在
            if unique_by and (line_dict[unique_by] in [x[unique_by] for x in results]):
                continue
            results.append(line_dict)
        # 如需排除，则基于字段升序排列
        if order_by:
            results.sort(key=lambda x: x[order_by]['value'])
        return results

    def get_table_schema_and_data(self, order_by=None, unique_by=None, wait_timeout=None, wait_interval=10,
                                  wait_method=DBDataMatch, **wait_args):
        """
        获取source，sink表结构及数据的融合
        :param order_by: 所有数据基于某字段升序排列
        :param unique_by: 所有数据基于某字段进行去重
        :param wait_timeout: 等待下游表数据总量与上游表数据总量相同的超时时间
        :param wait_interval: 等待间隔时间
        :param wait_method: 等待条件
        :param wait_args: 等待参数
        :return: 上游融合数据, 下游融合数据
        """
        # 等待时间为空时使用默认值
        if wait_timeout is None:
            wait_timeout = settings.DB_SYNC_WAIT_TIMEOUT

        # 获取上游表数据及类型，并将两者融合
        source_types = self.source_db.get_table_info()
        source_lines = self.source_db.get_table_data()
        source_db_data = self.combine_schema_and_data(source_types, source_lines, order_by)

        # 等待并获取下游表数据及类型，并将两者融合
        if wait_timeout <= 0:
            db_types = self.sink_db.get_table_info()
            db_lines = self.sink_db.get_table_data()
            sink_db_data = self.combine_schema_and_data(db_types, db_lines, order_by, unique_by)
        else:
            sink_db_data = Wait(wait_timeout, wait_interval=wait_interval).until(
                wait_method(self, source_db_data, order_by, unique_by, **wait_args),
                "已等待{0}秒，目标数据库: {1}，表：{2}，{3}".format(wait_timeout,
                                                      self.sink_db.ALIAS, self.sink_db.table_name,
                                                      wait_method.ERROR_MSG))
        return source_db_data, sink_db_data

    def get_table_schema_and_data_for_kafka_source(self, order_by=None, unique_by=None, wait_timeout=None):
        """
        获取source，sink表结构及数据的融合，只针对于kafka源的数据
        尝试将kafka写入的k:v进行分列处理，基于列明单独比较
        :param order_by: 所有数据基于某字段升序排列
        :param unique_by: 所有数据基于某字段进行去重
        :param wait_timeout: 等待下游表数据总量与上游表数据总量相同的超时时间
        :return: 上游融合数据, 下游融合数据
        """
        # 等待时间为空时使用默认值
        if wait_timeout is None:
            wait_timeout = settings.DB_SYNC_WAIT_TIMEOUT
            wait_interval = 10
        # 等待时间小于等于0是不等待
        elif wait_timeout <= 0:
            wait_timeout = 0
            wait_interval = 0
        else:
            wait_interval = 10
        # 获取上游表数据及类型，并将两者融合
        source_types = self.source_db.get_table_info()
        source_lines = self.source_db.get_table_data()
        source_db_data = self.combine_schema_and_data(source_types, source_lines, order_by)
        source_data_count = len(source_db_data)

        class HasSinkData:
            def __init__(self, instance, total):
                self.instance = instance
                self.total = total

            def __call__(self):
                try:
                    db_lines = self.instance.sink_db.get_table_data()
                    db_data = self.instance.combine_schema_and_data_for_kafka_source(source_types, db_lines,
                                                                                     order_by, unique_by)
                    if len(db_data) != self.total:
                        print('数据量与源端数据库数量不符，原表：{0}，目标表：{1}'.format(self.total, len(db_data)))
                        return False
                    else:
                        print('数据量与源端数据库数量相同，原表：{0}，目标表：{1}'.format(self.total, len(db_data)))
                        return db_data
                except Exception as ex:
                    print(ex)
                    return False

        # 等待下游融合数据与上游数据总量相同
        sink_db_data = Wait(wait_timeout, wait_interval=wait_interval).until(
            HasSinkData(self, source_data_count),
            "已等待{0}秒，目标数据库: {1}，表：{2} 数据量与源端数据库数量不符".format(wait_timeout,
                                                            self.sink_db.ALIAS, self.sink_db.table_name))
        return source_db_data, sink_db_data

    def get_table_schema_and_data_for_kafka_avro_source(self, order_by=None, unique_by=None, wait_timeout=None):
        """
        获取source，sink表结构及数据的融合
        :param order_by: 所有数据基于某字段升序排列
        :param unique_by: 所有数据基于某字段进行去重
        :param wait_timeout: 等待下游表数据总量与上游表数据总量相同的超时时间
        :return: 上游融合数据, 下游融合数据
        """
        # 等待时间为空时使用默认值
        if wait_timeout is None:
            wait_timeout = settings.DB_SYNC_WAIT_TIMEOUT
            wait_interval = 10
        # 等待时间小于等于0是不等待
        elif wait_timeout <= 0:
            wait_timeout = 0
            wait_interval = 0
        else:
            wait_interval = 10
        # 获取上游表数据及类型，并将两者融合
        source_types = self.source_db.get_table_info()
        source_lines = self.source_db.get_table_data()
        source_db_data = self.combine_schema_and_data(source_types, source_lines, order_by)
        source_data_count = len(source_db_data)

        if order_by is not None:
            order_by = 'value_'+order_by
        if unique_by is not None:
            unique_by = 'value_'+unique_by

        class HasSinkData:
            def __init__(self, instance, total):
                self.instance = instance
                self.total = total

            def __call__(self):
                try:
                    db_types = self.instance.sink_db.get_table_info()
                    db_lines = self.instance.sink_db.get_table_data()
                    db_data = self.instance.combine_schema_and_data(db_types, db_lines, order_by, unique_by)
                    if len(db_data) != self.total:
                        print('数据量与源端数据库数量不符，原表：{0}，目标表：{1}'.format(self.total, len(db_data)))
                        return False
                    else:
                        print('数据量与源端数据库数量相同，原表：{0}，目标表：{1}'.format(self.total, len(db_data)))
                        return db_data
                except Exception as ex:
                    print(ex)
                    print(traceback.format_exc())
                    return False

        # 等待下游融合数据与上游数据总量相同
        sink_db_data = Wait(wait_timeout, wait_interval=wait_interval).until(
            HasSinkData(self, source_data_count),
            "已等待{0}秒，目标数据库: {1}，表：{2} 数据量与源端数据库数量不符".format(wait_timeout,
                                                            self.sink_db.ALIAS, self.sink_db.table_name))
        return source_db_data, sink_db_data

    @compare_call
    def compare_schema(self, source_db_data, sink_db_data):
        """
        基于原表数据以及目标表数据，进行数据字段结构比较
        :param source_db_data: 原表所有数据
        :param sink_db_data: 目标表所有数据
        :return: 返回不相同的数据数量
        """
        fail_count = 0
        for index, source_line in enumerate(source_db_data):
            sink_line = sink_db_data[index]
            for column_name, source_dict in source_line.items():
                source_column_info = source_dict['info']
                sink_column_info = sink_line[column_name]['info']
                expected_column_info = self.get_column_mapping(source_column_info)

                if expected_column_info is None:
                    print('{0} 没有找到对应的schema类型'.format(source_column_info))
                    fail_count += 1
                    continue
                # check schema
                fail_count += self.schema_compare_rule(column_name, source_column_info, sink_column_info,
                                                       expected_column_info, self.source_db.ALIAS, self.sink_db.ALIAS)
            break
        return fail_count

    @compare_call
    def compare_data(self, source_db_data, sink_db_data):
        """
        基于原表数据以及目标表数据，进行数据值比较
        :param source_db_data: 原表所有数据
        :param sink_db_data: 目标表所有数据
        :return: 返回不相同的数据数量
        """
        fail_count = 0
        for index, source_line in enumerate(source_db_data):
            sink_line = sink_db_data[index]
            for column, source_dict in source_line.items():
                source_column_info = source_dict['info']
                source_column_value = source_dict['value']
                sink_column_info = sink_line[column]['info']
                sink_column_value = sink_line[column]['value']

                # check value
                fail_count += self.value_compare_rule(index, column, source_column_value, sink_column_value,
                                                      source_column_info, sink_column_info,
                                                      self.source_db.ALIAS, self.sink_db.ALIAS)
        return fail_count

