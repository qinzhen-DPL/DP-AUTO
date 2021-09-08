#!/usr/bin/env python
# encoding: utf-8
import traceback
from core.exceptions.regression_related_exception import *

"""
该模块用于存储（可复用的）上下游数据比较的逻辑
在使用DBOperator.get_table_schema_and_data函数时，可指定等待下游表数据达成某些特定条件
将所需的类型传递给wait_method=DataCountMatch, 并可传递额外的参数**wait_args

框架在运行中会将，以下对象传递到等待条件实现类：
instance，代表当前DBOperator实例，可访问、使用其内部所有函数、属性
source_db_data = source_db_data，已经融合后的上游表中所有数据及schema
order_by，指定的排序列
unique_by，指定的去重列
**wait_args，其他额外信息通过字典变量传递
"""


class DBDataMatch:
    """
    等待下游表中的某些数据匹配

    使用时需提供condition参数
    例如：下游表中需要包含有id为1且name为demo的数据有2条
    db_operator.get_table_schema_and_data(wait_method=DBDataMatch,
                                          condition=lambda row: row['id']['value']=='1' and row['name']['value']=='demo',
                                          count=2)

    例如：下游表中需要包含有id数据类型为int的列，并且数据数量与上游相等
    db_operator.get_table_schema_and_data(wait_method=DBDataMatch,
                                          condition=lambda row: row['id']['info'].data_type == 'int')

    例如：下游表中数据数量为10
    db_operator.get_table_schema_and_data(wait_method=DBDataMatch, count=10)
    """

    ERROR_MSG = '下游数据与预期不符'

    def __init__(self, instance, source_db_data, order_by, unique_by, **wait_args):
        self.instance = instance
        self.source_db_data = source_db_data
        self.order_by = order_by
        self.unique_by = unique_by
        self.lambda_function = wait_args.get('condition', lambda row: row)
        self.match_count = wait_args.get('count', len(source_db_data))

    def __call__(self):
        try:
            db_types = self.instance.sink_db.get_table_info()
            db_lines = self.instance.sink_db.get_table_data()
            db_data = self.instance.combine_schema_and_data(db_types, db_lines, self.order_by, self.unique_by)
            if len(list(filter(self.lambda_function,  db_data))) == self.match_count:
                print('目标数据有满足条件的列')
                return db_data
            else:
                print('目标数据没有满足条件的列')
                return False
        except Exception as ex:
            print(ex)
            print(traceback.format_exc())
            return False
