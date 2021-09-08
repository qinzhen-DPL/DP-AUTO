#!/usr/bin/env python
# encoding: utf-8
from core.testCases.testbase import *


def create_single_basic_task(task_api: TaskApi, link_task_name: str,
                             link_mappings, link_id: str,
                             source_node_id: str, source_type: NodeType,
                             sink_node_id: str, sink_type: NodeType,
                             is_real: bool):
    """创建任务"""
    # 创建任务
    task_id = task_api.create_task(link_task_name)

    # 拼接任务所需信息，并返回接口payload
    payload = TaskData.basic_increment_task(src_id=source_node_id,
                                            sink_id=sink_node_id,
                                            source_type=source_type,
                                            sink_type=sink_type,
                                            link_id=link_id,
                                            link_mappings=link_mappings, is_real=is_real)

    # 更新链路并激活
    task_api.update_task(task_id, payload)
    task_api.active_task(task_id)
    return task_id
