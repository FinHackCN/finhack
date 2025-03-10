import sys
import os
import re
import pandas as pd
from finhack.library.config import Config
from finhack.library.mydb import mydb
import finhack.library.log as Log

class DB:
    """
    数据库操作类，作为mydb类的包装器
    提供统一的数据库接口，支持DuckDB和MySQL
    """
    
    @staticmethod
    def get_db_engine(connection='default'):
        """
        获取数据库引擎
        """
        return mydb.getDBEngine(connection)
    
    @staticmethod
    def get_adapter(connection='default'):
        """
        获取数据库适配器
        根据数据库类型返回适配器对象
        """
        dbcfg = Config.get_config('db', connection)
        db_type = dbcfg.get('type', 'mysql')
        
        # 创建一个简单的适配器类，只需要有__class__.__name__属性
        class DuckDBAdapter:
            pass
            
        class MySQLAdapter:
            pass
        
        if db_type.lower() == 'duckdb':
            return DuckDBAdapter()
        else:
            return MySQLAdapter()
    
    @staticmethod
    def exec(sql, connection='default'):
        """
        执行SQL语句
        """
        return mydb.exec(sql, connection)
    
    @staticmethod
    def select_to_list(sql, connection='default'):
        """
        执行SQL查询，返回列表
        """
        return mydb.selectToList(sql, connection)
    
    @staticmethod
    def select_to_df(sql, connection='default'):
        """
        执行SQL查询，返回DataFrame
        """
        return mydb.selectToDf(sql, connection)
    
    @staticmethod
    def select(sql, connection='default'):
        """
        执行SQL查询，返回结果集
        """
        return mydb.select(sql, connection)
    
    @staticmethod
    def select_one(sql, connection='default'):
        """
        执行SQL查询，返回单条结果
        """
        return mydb.selectOne(sql, connection)
    
    @staticmethod
    def to_sql(df, table_name, connection='default', if_exists='append', **kwargs):
        """
        将DataFrame写入数据库
        """
        return mydb.toSql(df, table_name, connection)
    
    @staticmethod
    def truncate_table(table, connection='default'):
        """
        截断表
        """
        return mydb.truncateTable(table, connection)
    
    @staticmethod
    def delete(sql, connection='default'):
        """
        执行删除操作
        """
        return mydb.delete(sql, connection)
    
    @staticmethod
    def safe_to_sql(df, table_name, connection='default', **kwargs):
        """
        安全地将DataFrame写入数据库表，处理可能的列缺失问题
        """
        engine = DB.get_db_engine(connection)
        return mydb.safe_to_sql(df, table_name, engine, **kwargs)