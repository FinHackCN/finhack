import os
import re
import pandas as pd
import sqlite3
from sqlalchemy import create_engine, text
from typing import List, Dict, Any, Optional, Tuple

import finhack.library.log as Log
from finhack.library.db_adpter.base import DbAdapter
from finhack.library.monitor import dbMonitor


class SQLiteAdapter(DbAdapter):
    """SQLite数据库适配器实现"""
    
    def __init__(self, config):
        """
        初始化SQLite适配器
        
        Args:
            config: 数据库配置
        """
        self.config = config
        self.db_path = config.get('path', ':memory:')
        
        # 非内存数据库需要检测路径
        if self.db_path not in (':memory:', ''):
            abs_path = os.path.abspath(self.db_path)
            parent_dir = os.path.dirname(abs_path)
            
            # 递归创建父目录（如果不存在）
            if parent_dir:  # 避免空路径（如当前目录）
                os.makedirs(parent_dir, exist_ok=True)
    
    @dbMonitor
    def get_engine(self, read_only=False):
        """获取SQLAlchemy引擎"""
        if read_only and self.db_path != ':memory:':
            uri = f"sqlite:///{self.db_path}?mode=ro"
        else:
            uri = f"sqlite:///{self.db_path}"
        return create_engine(uri)
    
    @dbMonitor
    def get_connection(self):
        """获取数据库连接"""
        conn = sqlite3.connect(self.db_path)
        # 启用外键支持
        conn.execute("PRAGMA foreign_keys = ON")
        # 设置返回结果为字典形式
        conn.row_factory = sqlite3.Row
        return conn, conn.cursor()
    
    @dbMonitor
    def exec_sql(self, sql: str) -> None:
        """执行SQL语句"""
        conn, cursor = self.get_connection()
        try:
            # 处理RENAME TABLE语法，转换为SQLite的ALTER TABLE语法
            if sql.lower().startswith('rename table'):
                parts = sql.split()
                if len(parts) >= 5 and parts[3].lower() == 'to':
                    old_table = parts[2]
                    new_table = parts[4].rstrip(';')
                    sql = f"ALTER TABLE {old_table} RENAME TO {new_table}"
            
            # 在SQLite中，TRUNCATE不是标准命令，需要转换为DELETE FROM
            if sql.lower().startswith('truncate table'):
                parts = sql.split()
                if len(parts) >= 3:
                    table = parts[2].rstrip(';')
                    sql = f"DELETE FROM {table}"
            
            cursor.execute(sql)
            conn.commit()
        except Exception as e:
            Log.logger.error(f"SQLite执行SQL错误: {str(e)}\nSQL: {sql}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    @dbMonitor
    def select_to_list(self, sql: str) -> List[Dict[str, Any]]:
        """执行SQL查询，返回结果列表"""
        result_list = []
        conn, cursor = self.get_connection()
        try:
            cursor.execute(sql)
            for row in cursor.fetchall():
                # 转换sqlite3.Row为字典
                result_dict = {key: row[key] for key in row.keys()}
                result_list.append(result_dict)
        except Exception as e:
            Log.logger.error(f"SQLite查询错误: {str(e)}\nSQL: {sql}")
        finally:
            conn.close()
        return result_list
    
    @dbMonitor
    def select_to_df(self, sql: str) -> pd.DataFrame:
        """执行SQL查询，返回DataFrame"""
        conn, cursor = self.get_connection()
        try:
            cursor.execute(sql)
            columns = [col[0] for col in cursor.description]
            results = cursor.fetchall()
            # 将结果转换为字典列表
            dict_results = []
            for row in results:
                dict_results.append({columns[i]: row[i] for i in range(len(columns))})
            # 创建DataFrame
            results_df = pd.DataFrame(dict_results)
        except Exception as e:
            Log.logger.error(f"SQLite查询错误: {str(e)}\nSQL: {sql}")
            results_df = pd.DataFrame()
        finally:
            conn.close()
        return results_df
    
    @dbMonitor
    def select(self, sql: str) -> List[Dict[str, Any]]:
        """执行SQL查询，返回结果集"""
        return self.select_to_list(sql)
    
    @dbMonitor
    def select_one(self, sql: str) -> Optional[Dict[str, Any]]:
        """执行SQL查询，返回单条结果"""
        results = self.select_to_df(sql)
        if len(results) > 0:
            return results.iloc[0:1]
        return pd.DataFrame()
    
    def to_sql(self, df: pd.DataFrame, table_name: str, if_exists='append', **kwargs) -> int:
        """将DataFrame写入数据库"""
        engine = self.get_engine()
        return df.to_sql(table_name, engine, index=False, if_exists=if_exists, chunksize=5000)
    
    def safe_to_sql(self, df: pd.DataFrame, table_name: str, **kwargs) -> int:
        """安全地将DataFrame写入数据库，处理可能的列缺失问题"""
        engine = self.get_engine()
        
        try:
            # 预处理数据，确保类型正确
            for col in df.columns:
                # 对于可能包含股票代码/日期的列，强制转为字符串类型
                if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                   'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                    df[col] = df[col].fillna('').astype(str)
                    # 将字符串'None'替换为空字符串
                    df[col] = df[col].replace('None', '')
                # 对于数值类型的列，将None值替换为0
                elif 'float' in str(df[col].dtype) or 'int' in str(df[col].dtype):
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                # 对于其他类型的列，确保None值替换为空字符串
                elif df[col].dtype == 'object':
                    df[col] = df[col].fillna('').astype(str)
            
            # 检查表是否存在，不存在则直接写入
            if not self.table_exists(table_name):
                return df.to_sql(table_name, engine, **kwargs)
                
            # 获取表结构
            conn, cursor = self.get_connection()
            try:
                cursor.execute(f"PRAGMA table_info({table_name})")
                table_info = cursor.fetchall()
                existing_columns = {col["name"] for col in table_info}
                
                # 检查是否有新列需要添加
                for col in df.columns:
                    if col not in existing_columns:
                        # 确定列类型
                        col_type = "TEXT"  # 默认为TEXT类型
                        if 'int' in str(df[col].dtype):
                            col_type = "INTEGER"
                        elif 'float' in str(df[col].dtype):
                            col_type = "REAL"
                        
                        try:
                            # 添加新列
                            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN \"{col}\" {col_type}")
                            conn.commit()
                            Log.logger.info(f"成功添加列 {col} 到表 {table_name}")
                        except Exception as add_col_error:
                            Log.logger.warning(f"添加列 {col} 失败: {str(add_col_error)}")
                            conn.rollback()
                            # 如果不是列已存在的错误，抛出异常
                            if "duplicate column name" not in str(add_col_error).lower():
                                raise
            finally:
                conn.close()
            
            # 写入数据
            return df.to_sql(table_name, engine, if_exists='append', **kwargs)
        except Exception as e:
            Log.logger.error(f"SQLite写入错误: {str(e)}")
            raise
    
    @dbMonitor
    def truncate_table(self, table: str) -> bool:
        """截断表"""
        try:
            self.exec_sql(f"DELETE FROM {table}")
            return True
        except Exception as e:
            Log.logger.error(f"截断表 {table} 失败: {str(e)}")
            return False
    
    @dbMonitor
    def delete(self, sql: str) -> None:
        """执行删除操作"""
        self.exec_sql(sql)
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            conn, cursor = self.get_connection()
            try:
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                result = cursor.fetchone()
                return result is not None
            finally:
                conn.close()
        except Exception as e:
            Log.logger.error(f"检查表 {table_name} 是否存在时发生错误: {str(e)}")
            return False
    
    def set_index(self, table: str) -> None:
        """设置表索引"""
        index_list=['ts_code','end_date','trade_date']
        for index in index_list:
            try:
                sql = f"CREATE INDEX IF NOT EXISTS idx_{table}_{index} ON {table}({index})"
                self.exec_sql(sql)
            except Exception as e:
                Log.logger.warning(f"为表 {table} 创建索引 {index} 失败: {str(e)}")
                
    @dbMonitor
    def replace_table(self, target_table: str, source_table: str) -> Tuple[bool, str]:
        """
        替换表（将source_table替换为target_table）
        专门处理SQLite的表替换逻辑
        
        Args:
            target_table: 目标表名
            source_table: 源表名（通常是临时表）
            
        Returns:
            一个元组 (成功状态, 最终使用的表名)
        """
        try:
            # 检查目标表是否存在
            if self.table_exists(target_table):
                # 表存在，先备份原表
                self.exec_sql(f"ALTER TABLE {target_table} RENAME TO {target_table}_old")
                Log.logger.info(f"成功将原表重命名为 {target_table}_old")
                
            # 重命名临时表为目标表
            self.exec_sql(f"ALTER TABLE {source_table} RENAME TO {target_table}")
            Log.logger.info(f"成功将临时表重命名为 {target_table}")
            
            # 如果有备份表，则删除它
            if self.table_exists(f"{target_table}_old"):
                self.exec_sql(f"DROP TABLE IF EXISTS {target_table}_old")
                Log.logger.info(f"成功删除旧表 {target_table}_old")
                
            return True, target_table
        except Exception as e:
            Log.logger.error(f"SQLite表替换失败: {str(e)}")
            # 如果重命名失败，尝试使用临时表
            if self.table_exists(source_table):
                Log.logger.warning(f"无法完成表替换，将使用临时表 {source_table} 作为最终表")
                return False, source_table
            # 如果临时表也不存在，尝试恢复原表
            elif self.table_exists(f"{target_table}_old"):
                try:
                    self.exec_sql(f"ALTER TABLE {target_table}_old RENAME TO {target_table}")
                    Log.logger.warning(f"恢复原表 {target_table}")
                    return False, target_table
                except Exception as restore_e:
                    Log.logger.error(f"恢复原表失败: {str(restore_e)}")
                    return False, f"{target_table}_old"
            else:
                return False, ""  # 无可用表 