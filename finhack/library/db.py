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
    
    @staticmethod
    def get_db_engine(connection='default', read_only=False):
        """
        获取数据库引擎
        
        Args:
            connection: 数据库连接名
            read_only: 是否为只读模式
            
        Returns:
            数据库引擎对象
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        return adapter.get_engine(read_only)
    
    @staticmethod
    def get_adapter(connection='default'):
        """
        获取数据库适配器
        
        Args:
            connection: 数据库连接名
            
        Returns:
            对应的数据库适配器实例
        """
        return DbAdapterFactory.get_adapter(connection)
    
    @staticmethod
    def exec(sql: str, connection='default') -> None:
        """
        执行SQL语句
        
        Args:
            sql: SQL语句
            connection: 数据库连接名
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        adapter.exec_sql(sql)
    
    @staticmethod
    def select_to_list(sql: str, connection='default') -> List[Dict[str, Any]]:
        """
        执行SQL查询，返回列表
        
        Args:
            sql: SQL查询语句
            connection: 数据库连接名
        
        Returns:
            查询结果列表
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        return adapter.select_to_list(sql)
    
    @staticmethod
    def select_to_df(sql: str, connection='default') -> pd.DataFrame:
        """
        执行SQL查询，返回DataFrame
        
        Args:
            sql: SQL查询语句
            connection: 数据库连接名
            
        Returns:
            查询结果DataFrame
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        return adapter.select_to_df(sql)
    
    @staticmethod
    def select(sql: str, connection='default') -> List[Dict[str, Any]]:
        """
        执行SQL查询，返回结果集
        
        Args:
            sql: SQL查询语句
            connection: 数据库连接名
            
        Returns:
            查询结果集
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        return adapter.select(sql)
    
    @staticmethod
    def select_one(sql: str, connection='default') -> Optional[Dict[str, Any]]:
        """
        执行SQL查询，返回单条结果
        
        Args:
            sql: SQL查询语句
            connection: 数据库连接名
            
        Returns:
            单条查询结果
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        return adapter.select_one(sql)
    
    @staticmethod
    def to_sql(df: pd.DataFrame, table_name: str, connection='default', if_exists='append', **kwargs) -> int:
        """
        将DataFrame写入数据库
        
        Args:
            df: 数据源DataFrame
            table_name: 表名
            connection: 数据库连接名
            if_exists: 如果表存在的处理方式 ('fail', 'replace', 'append')
            
        Returns:
            写入的行数
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        return adapter.to_sql(df, table_name, if_exists=if_exists, **kwargs)
    
    @staticmethod
    def truncate_table(table: str, connection='default') -> bool:
        """
        截断表
        
        Args:
            table: 表名
            connection: 数据库连接名
            
        Returns:
            操作是否成功
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        return adapter.truncate_table(table)
    
    @staticmethod
    def delete(sql: str, connection='default') -> None:
        """
        执行删除操作
        
        Args:
            sql: 删除SQL语句
            connection: 数据库连接名
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        adapter.delete(sql)
    
    @staticmethod
    def safe_to_sql(df: pd.DataFrame, table_name: str, connection='default', **kwargs) -> int:
        """
        安全地将DataFrame写入数据库，处理可能的列缺失问题
        
        Args:
            df: 数据源DataFrame
            table_name: 表名
            connection: 数据库连接名
            
        Returns:
            写入的行数
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        return adapter.safe_to_sql(df, table_name, **kwargs)
    
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
        return adapter.table_exists(table_name)
    
    @staticmethod
    def set_index(table: str, connection='default') -> None:
        """
        设置表索引
        
        Args:
            table: 表名
            connection: 数据库连接名
        """
        adapter = DbAdapterFactory.get_adapter(connection)
        adapter.set_index(table)
    
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
        success, table_to_use = adapter.replace_table(target_table, source_table)
        
        # 如果成功，返回目标表名；否则返回临时表名或其他可用表名
        return table_to_use