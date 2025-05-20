import finhack.library.log as Log
from finhack.library.config import Config
from finhack.library.db_adpter.mysql_adapter import MySQLAdapter
from finhack.library.db_adpter.duckdb_adapter import DuckDBAdapter
from finhack.library.db_adpter.sqlite_adapter import SQLiteAdapter


class DbAdapterFactory:
    """数据库适配器工厂，负责创建适配器实例"""
    
    _adapters = {}  # 适配器缓存 {connection_name: adapter_instance}
    
    @staticmethod
    def get_adapter(connection='default'):
        """
        获取数据库适配器
        
        Args:
            connection: 数据库连接名称，对应配置文件中的节
            
        Returns:
            对应的数据库适配器实例
        """
        # 检查缓存
        if connection in DbAdapterFactory._adapters:
            return DbAdapterFactory._adapters[connection]
        
        # 读取数据库配置
        dbcfg = Config.get_config('db', connection)
        db_type = dbcfg.get('type', 'mysql').lower()
        
        # 根据数据库类型创建适配器
        adapter = None
        if db_type == 'duckdb':
            adapter = DuckDBAdapter(dbcfg)
        elif db_type == 'sqlite':
            adapter = SQLiteAdapter(dbcfg)
        else:
            # 默认使用MySQL适配器
            adapter = MySQLAdapter(dbcfg)
        
        # 缓存适配器实例
        DbAdapterFactory._adapters[connection] = adapter
        return adapter 