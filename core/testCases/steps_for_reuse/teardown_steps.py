#!/usr/bin/env python
# encoding: utf-8
from core.testCases.testbase import *


def delete_table_link_task(task_api: TaskApi, link_api: LinkApi, task_id: str, link_id: str,
                           source_tables: list, sink_tables: list,
                           source_db_type: str, sink_db_type: str):
    """删除链路、任务"""
    # task_api.force_delete_task_by_id(task_id)
    # link_api.force_delete_link_by_id(link_id)

    # for table in source_tables:
    #     source = create_db_instance(source_db_type, table[0], True)
    #     source.delete_table(raise_error=False)
    #
    # for table in sink_tables:
    #     sink = create_db_instance(sink_db_type, table[0], False)
    #     sink.delete_table(raise_error=False)
