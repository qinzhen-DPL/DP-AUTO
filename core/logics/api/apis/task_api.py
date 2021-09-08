#!/usr/bin/env python
# encoding: utf-8
from core.logics.api.apis.node_api import NodeType
from core.logics.api.api_driver import *
from core.logics.api.apis.link_api import LinkData


class TaskApi(ApiDriver):

    path_dictionary = {
        'create_task': '/v3/data-tasks',
        'active_task': '/v3/data-tasks/{0}/active',
        'suspend_task': '/v3/data-tasks/{0}/suspend',
        'delete_task': '/v3/data-tasks/{0}',
        'update_task': '/v3/data-tasks/{0}',
        'list_task': '/v3/data-tasks/list?projectId={0}',
        'task_info': '/v3/data-tasks/{0}?includeNode=true',
    }

    data_dictionary = {
        'create_task': {"name": "%s", "description": ""},
    }

    def __init__(self, session):
        ApiDriver.__init__(self, session)

    @api_step(step_name='创建任务')
    def create_task(self, task_name):
        print('Task name: {0}'.format(task_name))
        url = self.combine_url(config.API_BASE_URL, self.url('create_task'))
        data = self.combine_data(self.data('create_task'), task_name)
        res = self.post(url, json=data, verify=True)
        self.raise_for_status(res)
        task_id = res.json()['data']['id']
        print('Task name: {0}, ID: {1}'.format(task_name, task_id))
        return task_id

    @api_step(step_name='更新任务')
    def update_task(self, link_id, data):
        url = self.combine_url(config.API_BASE_URL, self.url('update_task').format(str(link_id)))
        res = self.put(url, json=data, verify=True)
        self.raise_for_status(res)

    @api_step(step_name='激活任务')
    def active_task(self, task_id):
        url = self.combine_url(config.API_BASE_URL, self.url('active_task').format(str(task_id)))
        res = self.post(url, verify=True)
        self.raise_for_status(res)

    @api_step(step_name='暂停任务')
    def suspend_task(self, task_id):
        url = self.combine_url(config.API_BASE_URL, self.url('suspend_task').format(str(task_id)))
        res = self.post(url, verify=True)
        self.raise_for_status(res)

    @api_step(step_name='删除任务')
    def delete_task(self, task_id):
        url = self.combine_url(config.API_BASE_URL, self.url('delete_task').format(str(task_id)))
        res = self.delete(url, verify=True)
        self.raise_for_status(res)

    @api_step(step_name='获取所有任务列表')
    def get_task_list(self, project_id=1):
        url = self.combine_url(config.API_BASE_URL, self.url('list_task').format(str(project_id)))
        res = self.get(url, verify=True)
        self.raise_for_status(res)
        tasks = res.json()['data']['items']
        return tasks

    @api_step(step_name='通过任务名称获取任务信息')
    def get_task_info_by_name(self, task_name):
        tasks = self.get_task_list()
        filter_tasks = [task for task in tasks if task['name'] == task_name]
        if len(filter_tasks) == 1:
            return filter_tasks[0]
        return None

    @api_step(step_name='通过任务ID获取任务信息')
    def get_task_info_by_id(self, task_id, raise_exception=True):
        url = self.combine_url(config.API_BASE_URL, self.url('task_info').format(str(task_id)))
        res = self.get(url, verify=True)
        if raise_exception:
            self.raise_for_status(res)
            return res.json()['data']
        else:
            return res

    @api_step(step_name='通过任务ID，暂停并删除任务')
    def force_delete_task_by_id(self, task_id, wait_timeout=30):
        node_info = self.get_task_info_by_id(task_id)
        if node_info['state'] == 'ACTIVE' or \
           node_info['state'] == 'ACTIVATING' or \
           node_info['state'] == 'WAITING_AUTO_RESTART':
            self.suspend_task(task_id)

        class IsSuspend:
            def __init__(self, instance):
                self.instance = instance

            def __call__(self):
                status = self.instance.get_task_info_by_id(task_id)
                print('当前状态: {0}'.format(status['state']))
                if status['state'] == 'SUSPEND':
                    return True
                elif status['state'] == 'WAITING_AUTO_RESTART':
                    self.instance.suspend_task(task_id)
                return False
        Wait(wait_timeout, wait_interval=3).until(
            IsSuspend(self), "已等待{0}秒，任务仍不能暂停，任务ID: {1}".format(wait_timeout, task_id))
        self.delete_task(task_id)

        class IsDeleted:
            def __init__(self, instance):
                self.instance = instance

            def __call__(self):
                ret = self.instance.get_task_info_by_id(task_id, raise_exception=False)
                return ret.status_code == 404
        Wait(wait_timeout, wait_interval=3).until(
            IsDeleted(self), "已等待{0}秒，任务仍不能删除，任务ID: {1}".format(wait_timeout, task_id))

    @api_step(step_name='通过任务名称，暂停并删除任务')
    def force_delete_task_by_name(self, task_name, wait_timeout):
        task_info = self.get_task_info_by_name(task_name)
        assert task_info, '无法找到该任务，名称: {0}'.format(task_name)
        self.force_delete_task_by_id(task_info['id'], wait_timeout)

    @api_step(step_name='删除所有任务')
    def delete_all_tasks(self, wait_timeout):
        tasks = self.get_task_list()
        task_ids = [task['id'] for task in tasks]
        for task_id in task_ids:
            self.force_delete_task_by_id(task_id, wait_timeout)

    @api_step(step_name='等待任务当前状态满足条件')
    def wait_task_status_to_be(self, task_id, state, wait_timeout=90, wait_interval=3):
        class IsSuspend:
            def __init__(self, instance):
                self.instance = instance

            def __call__(self):
                status = self.instance.get_task_info_by_id(task_id)
                print('当前状态: {0}'.format(status['state']))
                if status['state'] == state.upper():
                    return True
                return False
        Wait(wait_timeout, wait_interval=wait_interval).until(
            IsSuspend(self), "已等待{0}秒，任务状态不匹配：{2}，任务ID: {1}".format(wait_timeout, task_id, state))


class TaskData:

    @staticmethod
    def basic_increment_task(src_id, sink_id, source_type: NodeType, sink_type: NodeType, link_id, link_mappings, is_real):
        task = {
            "linkId": int(link_id),
            'mappings': [{"mappingId": mapping['mappingId']} for mapping in link_mappings],
            "srcNodes": [],
            'sinkNodes': [],
            "basicConfig": {
                    "taskExecuteConfig": {
                        "syncMode": "INCREMENT",
                        "fullInitialization": True,
                    }
                },
        }
        if is_real:
            task['basicConfig']['taskExecuteConfig']['startingPointMode'] = 'FULL'
        else:
            task['basicConfig']['taskExecuteConfig']['startingPointMode'] = 'FULL'
            task['basicConfig']['taskExecuteConfig']['syncInterval'] = 1
            task['basicConfig']['taskExecuteConfig']['syncIntervalUnit'] = "MINUTE"

        # 配置src节点信息
        if source_type == NodeType.kafka:
            src_node = LinkData.kafka_source_data(src_id)
        elif source_type == NodeType.ftp or \
                source_type == NodeType.hdfs or \
                source_type == NodeType.hive or \
                source_type == NodeType.inceptor:
            src_node = LinkData.scan_read_data(src_id)
        else:
            src_node = LinkData.jdbc_read_data(src_id)
        src_node['basicConfig']['resourceGroup'] = 'group_connect_source_dp'
        task['srcNodes'].append(src_node)

        # 配置sink节点信息
        if sink_type == NodeType.kafka:
            sink_node = LinkData.kafka_sink_data(sink_id)
        elif sink_type == NodeType.ftp or \
                sink_type == NodeType.hdfs or \
                sink_type == NodeType.hive or \
                sink_type == NodeType.inceptor:
            sink_node = LinkData.copy_write_data(sink_id)
        elif sink_type == NodeType.redis:
            sink_node = LinkData.redis_sink_data(sink_id)
        elif sink_type == NodeType.sequoiadb:
            sink_node = LinkData.sequoia_sink_data(sink_id)
        else:
            sink_node = LinkData.jdbc_write_data(sink_id)
        sink_node['basicConfig']['resourceGroup'] = 'group_connect_sink_dp'
        task['sinkNodes'].append(sink_node)
        return task
