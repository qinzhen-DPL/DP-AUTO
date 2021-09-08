#!/usr/bin/env python
# encoding: utf-8
from abc import ABCMeta, abstractmethod
import json
import datetime
import ruamel.yaml
from core import config
from core.utils.common import *
from core.logics.other.random_data import *
from core.exceptions.regression_related_exception import *


DB_LOGGING_START = '-' * 10 + 'DB-START' + '-' * 10
DB_LOGGING_END = '-' * 10 + 'DB-END' + '-' * 10

# 创建数据库字段时，如使用该字段名，则动态输入的数据确保全局唯一
# 用于无主键表需要查找或其他操作的标识
SORTED_AND_UNIQUE_COLUMN_NAME = 'dpid'

# Oracle数据库返回为空时的特殊标识
EMPTY_MARK = 'EMPTY_VALUE_NO_NEED_BINDING_DATA'


def db_step(step_name):
    """
    数据库操作步骤装饰器，用于打印数据库行为
    """
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            print('[DB]: ' + step_name)
            return func(*args, **kwargs)
        return inner_wrapper
    return wrapper


def db_call(func):
    """
    数据库调用装饰器，用于打印数据库行为
    """
    def inner_wrapper(*args, **kwargs):
        print(DB_LOGGING_START)
        start_time = datetime.datetime.now()
        has_error = None
        ret = None
        try:
            ret = func(*args, **kwargs)
        except Exception as e:
            has_error = e
        stop_time = datetime.datetime.now()
        ms = (stop_time - start_time).microseconds / 1000
        print('Query time: {0}ms'.format(str(ms)))
        print(DB_LOGGING_END)
        if has_error:
            raise has_error
        return ret
    return inner_wrapper


class DBColumn:
    """
    所有数据库字段（关系型、非关系型）的实体对象，用于存储并表示该数据库字段的所有信息
    """

    def __init__(self, name=None, data_type=None, column_type=None, default=None, not_null=None, auto_inc=None,
                 precision=None, scale=None, unsigned=None, **kwargs):
        """
        :param name: 字段名称
        :param data_type: 数据类型
        :param column_type: 字段类型/建表类型
        :param default: 默认值
        :param not_null: 不为空
        :param auto_inc: 自增
        :param precision: 精度
        :param scale: 标度
        :param unsigned: 无符号
        :param kwargs: 其他信息
        """
        self.name = name

        # 注意该类型对于当前数据库为统一类型，不包含精度、标度等其他可变信息
        # 例如: decimal(10,2)为column_type，decimal为data_type
        # data_type用于随机生成有效数据，且用于schema比较
        self.data_type = data_type

        # 字段类型或理解为建表类型，对于关系型数据库可直接用过该信息生成DDL语句中的字段类型，包含精度、标度等其他可变信息
        # 例如: decimal(10,2)为column_type，decimal为data_type
        # column_type用于生成数据库表，部分库用于生成随机数据，不用于schema比较
        self.column_type = column_type

        # 精度、标度用于schema比较，以及随机数据生成
        self.precision = precision
        self.scale = scale

        self.default = default
        self.not_null = not_null
        self.auto_inc = auto_inc
        self.unsigned = unsigned

        # 用于拓展使用
        self.other_info = kwargs


class DBTable:
    """
    所有数据库表（关系型、非关系型）的实体对象，用于存储并表示该数据库表的所有信息
    """

    def __init__(self, db_name, description, all_columns: [DBColumn], primary_keys: [DBColumn]):
        """
        :param db_name: 数据库名称，可以使用DBDriver.ALIAS别名，用于标识数据库类型
        :param description: 该数据库表的作用描述
        :param all_columns: 该数据库表的所有字段信息，类型为[DBColumn]
        :param primary_keys: 该数据库表的所有主键字段信息，类型为[DBColumn]
        """
        self.db = db_name
        self.description = description
        self.columns = all_columns
        self.primary_keys = primary_keys

    def is_column_exist(self, column_info: DBColumn):
        assert column_info.name, '查找字段需提供字段名称'
        find_column_name = column_info.name.lower()

        # 查找匹配的列
        find_result = list(filter(lambda x: x.name == find_column_name, self.columns))
        if len(find_result) != 1:
            print('没有找到名称为：{0} 的字段'.format(find_column_name))
            return False
        find_result = find_result[0]

        # 依次对比找到列的属性
        if (column_info.data_type is not None) and find_result.data_type != column_info.data_type:
            print('提供的数据类型为：{0}，实际是：{1}'.format(column_info.data_type, find_result.data_type))
            return False
        if (column_info.column_type is not None) and find_result.column_type != column_info.column_type:
            print('提供的字段类型为：{0}，实际是：{1}'.format(column_info.column_type, find_result.column_type))
            return False
        if (column_info.precision is not None) and find_result.precision != column_info.precision:
            print('提供的精度为：{0}，实际是：{1}'.format(column_info.precision, find_result.precision))
            return False
        if (column_info.scale is not None) and find_result.scale != column_info.scale:
            print('提供的标度为：{0}，实际是：{1}'.format(column_info.scale, find_result.scale))
            return False
        if (column_info.default is not None) and find_result.default != column_info.default:
            print('提供的默认值为：{0}，实际是：{1}'.format(column_info.default, find_result.default))
            return False
        if (column_info.default is not None) and find_result.default != column_info.default:
            print('提供的非空属性为：{0}，实际是：{1}'.format(column_info.default, find_result.default))
            return False
        if (column_info.auto_inc is not None) and find_result.auto_inc != column_info.auto_inc:
            print('提供的自增为：{0}，实际是：{1}'.format(column_info.auto_inc, find_result.auto_inc))
            return False
        if (column_info.unsigned is not None) and find_result.unsigned != column_info.unsigned:
            print('提供的无符号属性为：{0}，实际是：{1}'.format(column_info.unsigned, find_result.unsigned))
            return False
        return True



def read_yml_ddl_file(ddl_file, fs=None):
    """
    读取Yaml文件，并转换为DBTable对象
    :param ddl_file: yaml文件路径
    :param fs: file stream
    :return: DBTable对象
    """
    # 文件流为空时，读取ddl文件
    if fs is None:
        if not ddl_file.endswith('.yml'):
            ddl_file = ddl_file + '.yml'
        with open(ddl_file, 'r', encoding='utf-8') as fs:
            data = dict(ruamel.yaml.round_trip_load(fs))
    # 文件流不为空时，读取文件流
    else:
        data = dict(ruamel.yaml.round_trip_load(fs))
    return load_ddl_data(data)


def write_yml_ddl_file(db_table: DBTable, ddl_file):
    """
    将DBTable对象，写入Yaml文件
    :param db_table: DBTable对象
    :param ddl_file: 写入yaml文件路径
    """
    db_info = dump_ddl_data(db_table)
    with open(ddl_file, "w", encoding="utf-8") as f:
        ruamel.yaml.dump(db_info, f, encoding='utf-8', allow_unicode=True, Dumper=ruamel.yaml.RoundTripDumper,
                         indent=4, block_seq_indent=4)


def load_ddl_data(data: dict):
    """
    将字典数据转换为DBTable对象
    :param data: 包含有效信息的字典
    :return: DBTable对象
    """
    # 检查字典中必须有DB, Columns, PK键
    assert 'DB' in data.keys()
    assert 'Columns' in data.keys()
    assert 'PK' in data.keys()

    # 获取字典中有效信息
    db = data.get('DB')
    description = data.get('Description', 'For automation test')
    pks = list(data.get('PK')) if data.get('PK') else []

    # 获取并添加所有字段信息
    columns = []
    for name, column in dict(data.get('Columns')).items():
        column_type = column.get('Type')
        default = column.get('Default', None)
        not_null = column.get('Not_Null', False)
        auto_inc = column.get('Auto_Inc', False)
        columns.append(DBColumn(name=name, data_type=None, column_type=column_type, default=default,
                                not_null=not_null, auto_inc=auto_inc, precision=None, scale=None, unsigned=None))

    # 检查并添加主键信息
    if len(tuple(pks)) != len(pks):
        raise PrimaryKeysSameException('主键包含相同的列')
    pk_columns = []
    for column_name in pks:
        for column in columns:
            if column_name == column.name:
                pk_columns.append(column)
                break
        else:
            raise PrimaryKeysNotExistException('主键：{0}不存在'.format(column_name))
    return DBTable(db, description, columns, pk_columns)


def dump_ddl_data(db_table: DBTable):
    """
    将DBTable对象转换为字典数据
    :param db_table: DBTable对象
    :return: dict
    """
    return {
        'DB': db_table.db,
        'Description': db_table.description,
        'Columns': {each.name: {
            'Type': each.column_type,
            'Default': each.default,
            'Not_Null': each.not_null,
            'Auto_Inc': each.auto_inc} for each in db_table.columns},
        'PK': [each.name for each in db_table.primary_keys]
    }


class DBDriver(metaclass=ABCMeta):

    # 实现类必须重写ALIAS，用于说明所实例的数据库对象的别名
    ALIAS = None

    def sql(self, what):
        if what not in self.query.keys():
            raise DBSQLNotFoundException('无法找到DBObject该实现类中的query. {0}'.format(what))
        return self.query[what]

    def _format_json(self, ori):
        """
        格式化json，用于打印json内容
        """
        return json.dumps(ori, ensure_ascii=False, sort_keys=True, indent=4)

    @abstractmethod
    def get_table_info(self) -> DBTable:
        # 子类必须实现获取表结构方法
        # 返回的表结构需基于表的字段创建顺序升序排列，返回类型-DBTable
        # 返回的column名字为全部小写
        pass

    @abstractmethod
    def get_table_data(self) -> [[]]:
        # 子类必须实现获取表数据方法
        # 返回的表数据需基于表结构字段顺序返回-[[cell...]]
        pass

    @abstractmethod
    def create_table(self, table_info: DBTable):
        # 子类必须实现建表方法
        # 基于DBTable对象信息，生成对应建表DDL
        pass

    @abstractmethod
    def rename_table(self, new_table_name: str):
        # 子类必须实现重命名表方法
        pass

    @abstractmethod
    def delete_table(self, raise_error=True):
        # 子类必须实现删除表方法
        # raise_error为True时，抛出删除失败的信息，否则失败不抛错
        pass

    @abstractmethod
    def add_column(self, column_info: DBColumn):
        # 子类必须实现增加字段方法
        # 基于DBColumn对象信息，生成对应字段的DDL
        pass

    @abstractmethod
    def update_column(self, column_info: DBColumn):
        # 子类必须实现更新字段方法
        # 基于DBColumn对象信息，生成对应字段的DDL
        pass

    @abstractmethod
    def delete_column(self, column_name: str):
        # 子类必须实现删除字段方法
        pass

    @abstractmethod
    def rename_column(self, old_column_name: str, new_column_name: str):
        # 子类必须实现重命名字段方法
        pass

    @abstractmethod
    def add_primary_key(self, keys: list):
        # 子类必须实现增加主键方法
        pass

    @abstractmethod
    def delete_primary_key(self):
        # 子类必须实现删除主键方法
        pass

    @abstractmethod
    def insert_data(self, count: int):
        # 子类必须实现自动插入数据方法
        # 可以指定插入的数量
        pass

    @abstractmethod
    def manual_insert_data(self, *args, **kwargs):
        # 子类必须实现手动插入数据方法
        # 参数实现类自定义
        pass

    @abstractmethod
    def update_data(self, count_or_condition):
        # 子类必须实现自动更新数据方法
        # 可以指定插入的数量，或根据指定的条件插入
        # 说明基于数量查找更新的逻辑:
        # 1. 有自增主键时，基于自增主键作为查找依据，更新最新count条数据
        # 2. 有主键，选择第一个主键作为查找依据，更新最新count条数据
        # 3. 无主键，选择第一个字段作为查找依据，更新最新count条数据
        pass

    @abstractmethod
    def manual_update_data(self, *args, **kwargs):
        # 子类必须实现手动更新数据方法
        # 参数实现类自定义
        pass

    @abstractmethod
    def delete_data(self, count_or_condition):
        # 子类必须实现删除数据方法
        # 可以指定删除的数量，或根据指定的条件删除
        # 说明基于数量查找删除的逻辑:
        # 1. 有自增主键时，基于自增主键作为查找依据，删除最新count条数据
        # 2. 有主键，选择第一个主键作为查找依据，删除最新count条数据
        # 3. 无主键，选择第一个字段作为查找依据，删除最新count条数据
        pass

    @abstractmethod
    def is_table_exist(self) -> bool:
        # 子类必须实现删除数据方法
        # 返回当前表是否存在
        pass

    @abstractmethod
    def is_column_exist(self, column_info: DBColumn) -> bool:
        # 子类必须实现删除数据方法
        # 返回输入的DBColumn是否存在，依次比较DBColumn提供的信息是否满足
        pass
