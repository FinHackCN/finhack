from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Dict, Any, Optional, Union, Tuple

class DbAdapter(ABC):
    """
    数据库适配器基类
    定义所有数据库适配器必须实现的方法
    """
    
    @abstractmethod
    def get_engine(self, read_only=False):
        """
        获取数据库引擎
        
        Args:
            read_only: 是否为只读模式
            
        Returns:
            数据库引擎对象
        """
        pass
    
    @abstractmethod
    def exec_sql(self, sql: str) -> None:
        """
        执行SQL语句
        
        Args:
            sql: SQL语句
        """
        pass
    
    @abstractmethod
    def select_to_list(self, sql: str) -> List[Dict[str, Any]]:
        """
        执行SQL查询，返回结果列表
        
        Args:
            sql: SQL查询语句
            
        Returns:
            字典列表，每个字典代表一行数据
        """
        pass
    
    @abstractmethod
    def select_to_df(self, sql: str) -> pd.DataFrame:
        """
        执行SQL查询，返回DataFrame
        
        Args:
            sql: SQL查询语句
            
        Returns:
            DataFrame对象
        """
        pass
    
    @abstractmethod
    def select(self, sql: str) -> List[Dict[str, Any]]:
        """
        执行SQL查询，返回结果集
        
        Args:
            sql: SQL查询语句
            
        Returns:
            查询结果集
        """
        pass
    
    @abstractmethod
    def select_one(self, sql: str) -> Optional[Dict[str, Any]]:
        """
        执行SQL查询，返回单条结果
        
        Args:
            sql: SQL查询语句
            
        Returns:
            单条查询结果，如果没有结果则返回None
        """
        pass
    
    @abstractmethod
    def to_sql(self, df: pd.DataFrame, table_name: str, if_exists='append', **kwargs) -> int:
        """
        将DataFrame写入数据库
        
        Args:
            df: 数据源DataFrame
            table_name: 表名
            if_exists: 如果表存在的处理方式 ('fail', 'replace', 'append')
            
        Returns:
            写入的行数
        """
        pass
    
    @abstractmethod
    def safe_to_sql(self, df: pd.DataFrame, table_name: str, **kwargs) -> int:
        """
        安全地将DataFrame写入数据库，处理可能的列缺失问题
        
        Args:
            df: 数据源DataFrame
            table_name: 表名
            
        Returns:
            写入的行数
        """
        pass
    
    @abstractmethod
    def truncate_table(self, table: str) -> bool:
        """
        截断表
        
        Args:
            table: 表名
            
        Returns:
            操作是否成功
        """
        pass
    
    @abstractmethod
    def delete(self, sql: str) -> None:
        """
        执行删除操作
        
        Args:
            sql: 删除SQL语句
        """
        pass
    
    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在
        
        Args:
            table_name: 表名
            
        Returns:
            表是否存在
        """
        pass
    
    @abstractmethod
    def set_index(self, table: str) -> None:
        """
        设置表索引
        
        Args:
            table: 表名
        """
        pass
    
    @abstractmethod
    def replace_table(self, target_table: str, source_table: str) -> Tuple[bool, str]:
        """
        替换表（将source_table替换为target_table）
        处理不同数据库的表替换逻辑
        
        Args:
            target_table: 目标表名
            source_table: 源表名（通常是临时表）
            
        Returns:
            一个元组 (成功状态, 最终使用的表名)
            如果替换成功，返回 (True, target_table)
            如果替换失败但可以使用源表，返回 (False, source_table)
        """
        pass 