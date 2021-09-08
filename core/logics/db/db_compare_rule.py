#!/usr/bin/env python
# encoding: utf-8
import cx_Oracle


class DBValueCompareRule:
    """
    数据库值比较规则
    """

    @staticmethod
    def default_compare(index, column_name, source_value, sink_value, source_info, sink_info, source_db, sink_db):
        """
        数据库值默认比较规则
        :param index: 行号
        :param column_name: 列名
        :param source_value: 原表值
        :param sink_value: 目标表值
        :param source_info: 原表该列的定义
        :param sink_info: 目标表对应列的定义
        :param source_db: 原DB类型-类别名
        :param sink_db: 目标DB类型-类别名
        :return: 如果相同返回0，不相同返回1
        """
        # TODO: 可将以下类型转换代码放在每个DB类中处理，DB类返回的对象即为经过处理后的str，后续所有排序、去重、比较均不需类型处理
        if isinstance(source_value, cx_Oracle.LOB):
            source_value = source_value.read()
        if isinstance(source_value, memoryview):
            source_value = source_value.tobytes()
        if isinstance(sink_value, cx_Oracle.LOB):
            sink_value = sink_value.read()
        if isinstance(sink_value, memoryview):
            sink_value = sink_value.tobytes()

        # 默认值比较规则：基于str的比较
        source_value = str(source_value)
        sink_value = str(sink_value)
        if source_value != sink_value:
            print('行数: {0}，字段名: {1}，原表数据：{2}，目标表数据：{3}'.
                  format(index, column_name, source_value, sink_value))
            return 1
        return 0


class DBSchemaCompareRule:
    """
    数据库表结构比较规则
    """

    @staticmethod
    def default_compare(column_name, source_column_info, sink_column_info, expected_column_info, source_db, sink_db):
        """
        数据库表结构默认比较规则
        :param column_name: 列名
        :param source_column_info: 原表该列的定义
        :param sink_column_info: 目标表对应列的定义
        :param expected_column_info, source_db: 期待表列的特征
        :param source_db: 原DB类型-类别名
        :param sink_db: 目标DB类型-类别名
        :return: 如果相同返回0，不相同返回1
        """

        source_column_3p = "{};{};{}".format(source_column_info.data_type.lower(),
                                             source_column_info.precision,
                                             source_column_info.scale
                                             )

        expected_column_3p = "{};{};{}".format(expected_column_info.data_type.lower(),
                                               expected_column_info.precision,
                                               expected_column_info.scale
                                               )
        sink_column_3p = "{};{};{}".format(sink_column_info.data_type.lower(),
                                           sink_column_info.precision,
                                           sink_column_info.scale
                                           )

        if expected_column_3p != sink_column_3p:
            print('字段名: {0}，原表结构: {1}，目标表结构：{2}，期待表结构：{3}'.
                  format(column_name, source_column_3p, sink_column_3p, expected_column_3p))
            return 1
        else:
            pass
        #             print('DEBUG---success---字段名: {0}，原表结构: {1}，目标表结构：{2}，期待表结构：{3}'.
        #                   format(column_name, source_column_3p, sink_column_3p, expected_column_3p))

        return 0
