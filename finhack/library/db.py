import sys
import os
import re
import pandas as pd
from finhack.library.config import Config
import finhack.library.log as Log
from typing import List, Dict, Any, Optional, Union, Tuple
from finhack.library.db_adpter.adapter_factory import DbAdapterFactory

class DB:
    """
    数据库操作统一接口类
    提供统一的数据库访问方法，支持MySQL、DuckDB和SQLite
    隐藏底层适配器细节，让用户代码可以无缝切换数据库
    """
    
    # 用于缓存数据库适配器
    _adapters = {}
    
    @staticmethod
    def get_db_engine(connection='default'):
        """
        获取数据库引擎
        
        Args:
            connection: 数据库连接名
            
        Returns:
            数据库引擎对象
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        return adapter.get_engine()
    
    @staticmethod
    def get_adapter(db='default'):
        """
        获取指定的数据库适配器
        
        Args:
            db (str): 数据库配置名称，默认为'default'
            
        Returns:
            DbAdapter: 数据库适配器实例
        """
        try:
            # 检查是否已有缓存
            if db in DB._adapters:
                return DB._adapters[db]
            
            from finhack.library.db_adpter.adapter_factory import DbAdapterFactory
            adapter = DbAdapterFactory.get_adapter(db)
            
            if adapter:
                # 缓存适配器
                DB._adapters[db] = adapter
                return adapter
            else:
                Log.logger.error(f"无法创建数据库适配器: {db}")
                return None
        except Exception as e:
            # 处理可能的SQLite错误
            if "disk i/o error" in str(e).lower() or "database disk image is malformed" in str(e).lower():
                Log.logger.error(f"连接数据库时遇到磁盘I/O错误: {str(e)}")
                
                # 获取数据库文件路径
                try:
                    from finhack.library.config import Config
                    import os
                    
                    db_config = Config.get_config('db', db)
                    if db_config and 'type' in db_config and db_config['type'].lower() == 'sqlite' and 'path' in db_config:
                        db_path = db_config['path']
                        # 如果路径是相对路径，转换为绝对路径
                        if not os.path.isabs(db_path):
                            db_path = os.path.abspath(os.path.join(os.getcwd(), db_path))
                        
                        # 检查并删除可能的WAL文件
                        wal_path = db_path + "-wal"
                        shm_path = db_path + "-shm"
                        
                        if os.path.exists(wal_path):
                            try:
                                os.remove(wal_path)
                                Log.logger.warning(f"已删除可能损坏的WAL文件: {wal_path}")
                            except:
                                pass
                                
                        if os.path.exists(shm_path):
                            try:
                                os.remove(shm_path)
                                Log.logger.warning(f"已删除可能损坏的SHM文件: {shm_path}")
                            except:
                                pass
                
                        # 备份并尝试重新创建数据库
                        if os.path.exists(db_path):
                            import time
                            backup_path = f"{db_path}.bak.{int(time.time())}"
                            try:
                                import shutil
                                shutil.copy2(db_path, backup_path)
                                Log.logger.warning(f"已备份数据库文件: {db_path} -> {backup_path}")
                                
                                # 删除原数据库文件
                                os.remove(db_path)
                                Log.logger.warning(f"已删除损坏的数据库文件，将重新创建: {db_path}")
                                
                                # 重新获取适配器
                                from finhack.library.db_adpter.adapter_factory import AdapterFactory
                                adapter = AdapterFactory.get_adapter(db)
                                if adapter:
                                    # 缓存适配器
                                    DB._adapters[db] = adapter
                                    return adapter
                            except Exception as backup_error:
                                Log.logger.error(f"备份数据库文件失败: {str(backup_error)}")
                except Exception as config_error:
                    Log.logger.error(f"处理数据库文件时出错: {str(config_error)}")
            
            Log.logger.error(f"获取数据库适配器时出错: {str(e)}")
            return None
    
    @staticmethod
    def exec(sql: str, connection='default') -> None:
        """
        执行SQL语句
        
        Args:
            sql: SQL语句
            connection: 数据库连接名
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        try:
            adapter.exec_sql(sql)
        except Exception as e:
            Log.logger.error(f"执行SQL异常: {str(e)}")
            # 不抛出异常，避免中断程序流程
    
    @staticmethod
    def select_to_list(sql: str, connection='default') -> List[Dict[str, Any]]:
        """
        执行查询，返回字典列表
        
        Args:
            sql: SQL查询
            connection: 数据库连接名
            
        Returns:
            查询结果字典列表
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        try:
            return adapter.select_to_list(sql)
        except Exception as e:
            Log.logger.error(f"执行查询异常: {str(e)}")
            return []
    
    @staticmethod
    def select_to_df(sql: str, connection='default') -> pd.DataFrame:
        """
        执行查询，返回DataFrame
        
        Args:
            sql: SQL查询
            connection: 数据库连接名
            
        Returns:
            查询结果DataFrame
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        try:
            return adapter.select_to_df(sql)
        except Exception as e:
            Log.logger.error(f"执行查询异常: {str(e)}")
            return pd.DataFrame()
    
    @staticmethod
    def select(sql: str, connection='default') -> List[Dict[str, Any]]:
        """
        执行查询，返回结果集
        
        Args:
            sql: SQL查询
            connection: 数据库连接名
            
        Returns:
            查询结果集
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        try:
            return adapter.select(sql)
        except Exception as e:
            Log.logger.error(f"执行查询异常: {str(e)}")
            return []
    
    @staticmethod
    def select_one(sql: str, connection='default') -> Optional[Dict[str, Any]]:
        """
        执行查询，返回单条结果
        
        Args:
            sql: SQL查询
            connection: 数据库连接名
            
        Returns:
            单条查询结果
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        try:
            return adapter.select_one(sql)
        except Exception as e:
            Log.logger.error(f"执行查询异常: {str(e)}")
            return None
    
    @staticmethod
    def to_sql(df: pd.DataFrame, table_name: str, connection='default', if_exists='append', **kwargs) -> int:
        """
        将DataFrame写入数据库
        
        Args:
            df: 待写入的DataFrame
            table_name: 表名
            connection: 数据库连接名
            if_exists: 表已存在时的处理策略
            **kwargs: 其他参数
            
        Returns:
            写入的行数
        """
        # 确保connection是字符串
        if not isinstance(connection, str):
            # 尝试从引擎对象中提取连接名称
            if hasattr(connection, 'url') and 'sqlite' in str(connection.url):
                # 从URL中提取连接名: sqlite:///data/db/tushare.sqlite -> tushare
                url_str = str(connection.url)
                if 'tushare' in url_str:
                    Log.logger.warning(f"to_sql收到非字符串连接名, 提取连接为: tushare")
                    connection = 'tushare'
                else:
                    Log.logger.warning(f"to_sql收到非字符串连接名: {connection}，将使用'default'")
                    connection = 'default'
            else:
                Log.logger.warning(f"to_sql收到非字符串连接名: {connection}，将使用'default'")
                connection = 'default'
        
        # 空DataFrame处理
        if df.empty or len(df.columns) == 0:
            #Log.logger.warning(f"尝试写入空DataFrame到表 {table_name}, 操作已跳过")
            return 0
        
        adapter = DbAdapterFactory.get_adapter(connection)
        try:
            # 移除可能会导致问题的参数
            clean_kwargs = kwargs.copy()
            clean_kwargs.pop('con', None)
            clean_kwargs.pop('connection', None)
            
            return adapter.to_sql(df, table_name, if_exists=if_exists, **clean_kwargs)
        except Exception as e:
            Log.logger.error(f"写入DataFrame异常: {str(e)}")
            return 0
    
    @staticmethod
    def truncate_table(table: str, connection='default') -> bool:
        """
        截断表
        
        Args:
            table: 表名
            connection: 数据库连接名
            
        Returns:
            是否成功
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        try:
            return adapter.truncate_table(table)
        except Exception as e:
            Log.logger.error(f"截断表异常: {str(e)}")
            return False
    
    @staticmethod
    def delete(sql: str, connection='default') -> None:
        """
        执行删除操作
        
        Args:
            sql: SQL语句
            connection: 数据库连接名
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        try:
            adapter.delete(sql)
        except Exception as e:
            Log.logger.error(f"执行删除异常: {str(e)}")
    
    @staticmethod
    def safe_to_sql(df: pd.DataFrame, table_name: str, connection='default', **kwargs) -> int:
        """
        安全地将DataFrame写入数据库，处理可能的列缺失问题
        
        Args:
            df: 待写入的DataFrame
            table_name: 表名
            connection: 数据库连接名
            **kwargs: 其他参数
            
        Returns:
            写入的行数
        """
        # 确保connection是字符串
        if not isinstance(connection, str):
            # 尝试从引擎对象中提取连接名称
            if hasattr(connection, 'url') and 'sqlite' in str(connection.url):
                # 从URL中提取连接名: sqlite:///data/db/tushare.sqlite -> tushare
                url_str = str(connection.url)
                if 'tushare' in url_str:
                    Log.logger.warning(f"safe_to_sql收到非字符串连接名, 提取连接为: tushare")
                    connection = 'tushare'
                else:
                    Log.logger.warning(f"safe_to_sql收到非字符串连接名: {connection}，将使用'default'")
                    connection = 'default'
            else:
                Log.logger.warning(f"safe_to_sql收到非字符串连接名: {connection}，将使用'default'")
                connection = 'default'
        
        # 空DataFrame处理
        if df.empty or len(df.columns) == 0:
            #Log.logger.warning(f"尝试写入空DataFrame到表 {table_name}, 操作已跳过")
            return 0
        
        adapter = DbAdapterFactory.get_adapter(connection)
        try:
            # 移除可能会导致问题的参数
            clean_kwargs = kwargs.copy()
            clean_kwargs.pop('con', None)
            clean_kwargs.pop('connection', None)
            
            # 确保不重复传递if_exists参数
            if 'if_exists' not in clean_kwargs:
                clean_kwargs['if_exists'] = 'append'
            
            return adapter.safe_to_sql(df, table_name, **clean_kwargs)
        except Exception as e:
            Log.logger.error(f"安全写入DataFrame异常: {str(e)}")
            return 0
    
    @staticmethod
    def table_exists(table_name: str, connection='default') -> bool:
        """
        检查表是否存在
        
        Args:
            table_name: 表名
            connection: 数据库连接名
            
        Returns:
            表是否存在
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        try:
            return adapter.table_exists(table_name)
        except Exception as e:
            Log.logger.error(f"检查表是否存在异常: {str(e)}")
            return False
    
    @staticmethod
    def set_index(table: str, connection='default') -> None:
        """
        设置表索引
        
        Args:
            table: 表名
            connection: 数据库连接名
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        try:
            adapter.set_index(table)
        except Exception as e:
            Log.logger.error(f"设置表索引异常: {str(e)}")
    
    @staticmethod
    def replace_table(target_table: str, source_table: str, connection='default') -> str:
        """
        替换表（将source_table替换为target_table）
        处理不同数据库的表替换逻辑，返回最终使用的表名
        
        Args:
            target_table: 目标表名
            source_table: 源表名（通常是临时表）
            connection: 数据库连接名
            
        Returns:
            最终使用的表名
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        try:
            # replace_table方法应该返回(成功状态, 使用的表名)的元组
            result = adapter.replace_table(target_table, source_table)
            
            # 处理不同类型的返回值
            if isinstance(result, tuple) and len(result) == 2:
                success, table_to_use = result
                return table_to_use
            elif isinstance(result, bool):
                # 向后兼容：如果只返回成功状态，则根据成功状态返回表名
                return target_table if result else source_table
            elif isinstance(result, str):
                # 向后兼容：如果直接返回表名
                return result
            else:
                Log.logger.warning(f"replace_table返回值类型未知: {type(result)}")
                return source_table
        except Exception as e:
            Log.logger.error(f"替换表异常: {str(e)}")
            return source_table