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
        # 检查是否传入了引擎对象而不是连接名
        connection_name = 'default'  # 默认使用default连接
        
        if isinstance(connection, str):
            connection_name = connection
        else:
            # 非字符串连接名处理
            Log.logger.error(f"get_adapter 接收到非字符串连接名: {connection}，将使用默认连接")
            
            # 尝试从引擎对象提取连接信息（主要用于调试）
            try:
                if hasattr(connection, 'url'):
                    url_str = str(connection.url)
                    Log.logger.debug(f"检测到可能的引擎对象，URL: {url_str}")
                else:
                    Log.logger.debug(f"无法识别的连接对象类型: {type(connection)}")
            except:
                pass
        
        # 检查缓存
        if connection_name in DbAdapterFactory._adapters:
            return DbAdapterFactory._adapters[connection_name]
        
        try:
            # 读取数据库配置
            dbcfg = Config.get_config('db', connection_name)
            if 'type' not in dbcfg:
                Log.logger.error(f"数据库配置 '{connection_name}' 中缺少 'type' 字段。")
                import sys
                sys.exit(1)
                raise ValueError(f"数据库配置 '{connection_name}' 中缺少 'type' 字段。")
            db_type = dbcfg['type'].lower()
            
            # 根据数据库类型创建适配器
            adapter = None
            if db_type == 'duckdb':
                adapter = DuckDBAdapter(dbcfg)
                Log.logger.debug(f"创建DuckDB适配器，连接：{connection_name}")
            elif db_type == 'sqlite':
                adapter = SQLiteAdapter(dbcfg)
                Log.logger.debug(f"创建SQLite适配器，连接：{connection_name}")
            elif db_type == 'mysql':
                # 确保MySQL所需配置都存在
                required_keys = ['host', 'port', 'user', 'password', 'db', 'charset']
                if all(key in dbcfg for key in required_keys):
                    adapter = MySQLAdapter(dbcfg)
                    Log.logger.debug(f"创建MySQL适配器，连接：{connection_name}")
                else:
                    missing_keys = [key for key in required_keys if key not in dbcfg]
                    Log.logger.error(f"MySQL适配器创建失败，缺少配置: {missing_keys}")
                    # 回退到SQLite以避免程序崩溃
                    adapter = SQLiteAdapter({'path': ':memory:'})
                    Log.logger.debug(f"回退到内存SQLite适配器")
            else:
                # 未知类型，默认使用SQLite
                Log.logger.warning(f"未知数据库类型: {db_type}，默认使用SQLite")
                adapter = SQLiteAdapter({'path': ':memory:'})
                
            # 缓存适配器实例
            DbAdapterFactory._adapters[connection_name] = adapter
            return adapter
        except Exception as e:
            Log.logger.error(f"创建数据库适配器异常: {str(e)}")
            # 返回内存SQLite适配器作为回退选项
            adapter = SQLiteAdapter({'path': ':memory:'})
            Log.logger.debug(f"异常回退到内存SQLite适配器")
            return adapter 