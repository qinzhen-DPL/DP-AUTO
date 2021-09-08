# 对象模块

框架为三层设计模式：
- 驱动层（底层）：框架封装了Requests接口测试库，以及所有DB操作库，便于使用
- 业务层（中间层）：创建ApiObject，DBObject以将业务逻辑封装到不同的类中，便于复用、维护
- 脚本层（上层）：通过调用业务抽象类，并传递不同参数，来组装测试脚本

## ApiObject

接口业务抽象类用于存放所有api路径、数据和操作。按照不同业务场景划分并存储于：core/logics/api/apis

所有子类需继承ApiDriver，该父类中存储了所有对于接口的调用、及报文拼接函数

	class LoginApi(ApiDriver):

将url及payload存储在类path_dictionary、data_dictionary变量中

	path_dictionary = {
        'login': '/v3/users/login',
    }

    data_dictionary = {
        'login': {"login": "%s", "password": "%s"},
    }

	# set data
	data = self.combine_data(self.data('login'), user_name, password)  
	data = self.data('token_grant')  
	
	# post  
	res = self.post(url, json=data)  
	  
	# check result  
	self.raise_for_status(res)
    token = res.json()['data']['token']


## DBObject

数据库抽象类用于统一化所有数据库操作的行为，按照数据库的类型划分并存储于：core/logics/db/dbs

所有子类需继承DBDriver，并实现父类的抽象函数，以达到用统一的API实现对不同关系型、非关系型数据库的操作

	class MySqlDB(DBDriver):

将sql或cmd存储在类query变量中

	query = {
        'get_table_schema':
            "select column_name, data_type, COLUMN_TYPE from information_schema.columns where TABLE_NAME = '{0}' order by ORDINAL_POSITION",
        'get_all_data':
            "select * from {0}",
        'delete_table':
            "drop table {0}",
    }

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
