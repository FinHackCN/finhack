import os
import re
import pandas as pd
import sqlite3
import time
from sqlalchemy import create_engine, text
from typing import List, Dict, Any, Optional, Tuple

import finhack.library.log as Log
from finhack.library.db_adpter.base import DbAdapter
from finhack.library.monitor import dbMonitor


class SQLiteAdapter(DbAdapter):
    """SQLite数据库适配器实现，增强并发支持"""
    
    def __init__(self, config):
        """
        初始化SQLite适配器
        
        Args:
            config: 数据库配置
        """
        self.config = config
        
        self.db_path = config.get('path', ':memory:')
        
        # 设置连接超时和重试参数
        self.timeout = config.get('timeout', 30.0)  # 连接超时时间(秒)
        self.max_retries = config.get('max_retries', 5)  # 最大重试次数
        self.retry_delay = config.get('retry_delay', 1.0)  # 重试间隔(秒)
        
        # 非内存数据库需要检测路径
        if self.db_path not in (':memory:', ''):
            abs_path = os.path.abspath(self.db_path)
            parent_dir = os.path.dirname(abs_path)
            
            # 递归创建父目录（如果不存在）
            if parent_dir:  # 避免空路径（如当前目录）
                os.makedirs(parent_dir, exist_ok=True)
    
    def get_engine(self):
        """获取SQLAlchemy引擎，增加连接超时和池大小配置"""
        try:
            uri = f"sqlite:///{self.db_path}"
            return create_engine(
                uri,
                connect_args={'timeout': self.timeout},
                # SQLite只支持简单的连接池配置，移除不支持的参数
                pool_pre_ping=True
            )
        except Exception as e:
            Log.logger.error(f"获取SQLAlchemy引擎异常: {str(e)}")
            raise
    
    @dbMonitor
    def get_connection(self):
        """获取数据库连接，添加重试机制"""
        retries = 0
        last_error = None
        
        while retries <= self.max_retries:
            try:
                conn = sqlite3.connect(self.db_path, timeout=self.timeout)
                
                # 尝试执行数据库优化设置，使用安全模式而非WAL模式
                try:
                    # 启用外键支持
                    conn.execute("PRAGMA foreign_keys = ON")
                    # 使用DELETE模式而非WAL模式，更安全但速度稍慢
                    conn.execute("PRAGMA journal_mode = DELETE")
                    # 降低同步级别以提高性能，但保持基本安全
                    conn.execute("PRAGMA synchronous = NORMAL")
                    # 增加缓存大小
                    conn.execute("PRAGMA cache_size = 10000")
                    # 临时文件存储在内存中
                    conn.execute("PRAGMA temp_store = MEMORY")
                except Exception as pragma_error:
                    # 如果设置PRAGMA失败，记录错误但不中断连接
                    Log.logger.warning(f"设置数据库PRAGMA参数失败: {str(pragma_error)}")
                
                # 设置返回结果为字典形式
                conn.row_factory = sqlite3.Row
                return conn, conn.cursor()
            except sqlite3.OperationalError as e:
                # 数据库锁定或繁忙，重试
                if "database is locked" in str(e) or "database is busy" in str(e):
                    retries += 1
                    last_error = e
                    if retries <= self.max_retries:
                        Log.logger.warning(f"数据库锁定，等待重试 ({retries}/{self.max_retries}): {str(e)}")
                        time.sleep(self.retry_delay)
                        continue
                    else:
                        Log.logger.error(f"数据库锁定，重试次数超限: {str(e)}")
                        raise
                elif "disk i/o error" in str(e).lower() or "database disk image is malformed" in str(e).lower():
                    Log.logger.error(f"数据库文件损坏或磁盘错误: {str(e)}")
                    # 尝试恢复数据库或创建新的数据库
                    try:
                        if os.path.exists(self.db_path):
                            backup_path = f"{self.db_path}.bak.{int(time.time())}"
                            Log.logger.warning(f"尝试创建数据库备份: {backup_path}")
                            # 尝试复制而不是移动文件
                            try:
                                import shutil
                                shutil.copy2(self.db_path, backup_path)
                            except:
                                # 如果复制失败，尝试移动
                                os.rename(self.db_path, backup_path)
                            # 删除可能存在的WAL文件
                            if os.path.exists(self.db_path + "-shm"):
                                os.remove(self.db_path + "-shm")
                            if os.path.exists(self.db_path + "-wal"):
                                os.remove(self.db_path + "-wal")
                            Log.logger.info(f"已备份损坏的数据库文件，将创建新文件")
                    except Exception as recovery_error:
                        Log.logger.error(f"尝试恢复数据库时出错: {str(recovery_error)}")
                    
                    # 返回None，让上层代码处理
                    raise
                else:
                    # 其他错误直接抛出
                    raise
            except Exception as e:
                # 其他错误直接抛出
                raise
        
        # 重试次数用尽，抛出最后一个错误
        if last_error:
            raise last_error
        else:
            raise sqlite3.OperationalError("获取数据库连接失败，原因未知")
    
    @dbMonitor
    def exec_sql(self, sql: str) -> None:
        """执行SQL语句，兼容MySQL和SQLite语法差异"""
        # 预处理SQL，确保兼容SQLite语法
        sql = self._adapt_sql_for_sqlite(sql)
        
        conn, cursor = self.get_connection()
        try:
            cursor.execute(sql)
            conn.commit()
        except Exception as e:
            Log.logger.error(f"SQLite执行SQL错误: {str(e)}\nSQL: {sql}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def _adapt_sql_for_sqlite(self, sql: str) -> str:
        """将MySQL语法的SQL转换为SQLite兼容的语法"""
        # 处理RENAME TABLE语法
        if sql.lower().startswith('rename table'):
            parts = sql.split()
            if len(parts) >= 5 and parts[3].lower() == 'to':
                old_table = parts[2]
                new_table = parts[4].rstrip(';')
                sql = f"ALTER TABLE {old_table} RENAME TO {new_table}"
        
        # 处理TRUNCATE语法
        if sql.lower().startswith('truncate table'):
            parts = sql.split()
            if len(parts) >= 3:
                table = parts[2].rstrip(';')
                sql = f"DELETE FROM {table}"
        
        # 处理CREATE INDEX语法，移除列名中的长度限制，例如ts_code(10) -> ts_code
        if sql.lower().startswith('create index'):
            # 使用正则表达式匹配类似 column_name(length) 的模式并替换为 column_name
            sql = re.sub(r'(\w+)\s*\(\d+\)', r'\1', sql)
        
        # 替换ISNULL函数为SQLite兼容的写法
        sql = re.sub(r'(?i)not\s+ISNULL\s*\(([^)]+)\)', r'\1 IS NOT NULL', sql)
        sql = re.sub(r'(?i)ISNULL\s*\(([^)]+)\)', r'\1 IS NULL', sql)
        
        # 替换其他MySQL特有函数
        # ...可以根据需要添加更多替换
        
        return sql
    
    @dbMonitor
    def select_to_list(self, sql: str) -> List[Dict[str, Any]]:
        """执行SQL查询，返回结果列表"""
        # 预处理SQL，确保兼容SQLite语法
        sql = self._adapt_sql_for_sqlite(sql)
        
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
        # 预处理SQL，确保兼容SQLite语法
        sql = self._adapt_sql_for_sqlite(sql)
        
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
        results = self.select_to_list(sql)
        if results and len(results) > 0:
            return results[0]
        return None
    
    def to_sql(self, df: pd.DataFrame, table_name: str, if_exists='append', **kwargs) -> int:
        """将DataFrame写入数据库"""
        # 空DataFrame检查
        if df.empty or len(df.columns) == 0:
            Log.logger.warning(f"尝试写入空DataFrame到表 {table_name}, 操作已跳过")
            return 0
        
        engine = self.get_engine()
        # 显式设置index=False，确保不传递conn参数给pandas
        kwargs.pop('connection', None)  # 移除可能存在的connection参数
        kwargs.pop('con', None)  # 移除可能存在的con参数
        
        # 确保正确设置参数
        kwargs.update({
            'index': False,
            'if_exists': if_exists,
            'chunksize': kwargs.get('chunksize', 5000)
        })
        
        return df.to_sql(table_name, engine, **kwargs)
    
    def safe_to_sql(self, df: pd.DataFrame, table_name: str, **kwargs) -> int:
        """安全地将DataFrame写入数据库，处理可能的列缺失问题。类型转换已移除。"""
        # 空DataFrame检查
        if df.empty or len(df.columns) == 0:
            Log.logger.warning(f"尝试写入空DataFrame到表 {table_name}, 操作已跳过")
            return 0
        
        engine = self.get_engine()
        
        final_to_sql_kwargs = kwargs.copy()
        final_to_sql_kwargs.setdefault('index', False)
        final_to_sql_kwargs.setdefault('if_exists', 'append')
        final_to_sql_kwargs.setdefault('chunksize', 5000) # Default for SQLite, can be overridden by kwargs

        try:
            # 检查表是否存在，不存在则直接写入
            if not self.table_exists(table_name):
                return df.to_sql(table_name, engine, **final_to_sql_kwargs)
                
            # 表存在，获取表的列信息
            conn_for_pragma, _ = self.get_connection() # Need a direct connection for PRAGMA
            try:
                cursor_for_pragma = conn_for_pragma.cursor()
                cursor_for_pragma.execute(f"PRAGMA table_info({table_name})")
                table_info = cursor_for_pragma.fetchall()
                existing_columns = {col["name"] for col in table_info}
                
                missing_columns = [col for col in df.columns if col not in existing_columns]
                
                if missing_columns:
                    Log.logger.info(f"表 {table_name} 缺少 {len(missing_columns)} 列：{ ', '.join(missing_columns)}")
                    for col in missing_columns:
                        try:
                            sql_type = "TEXT"  # 默认为TEXT类型
                            
                            alter_sql = f"ALTER TABLE {table_name} ADD COLUMN \"{col}\" {sql_type}"
                            cursor_for_pragma.execute(alter_sql)
                            conn_for_pragma.commit()
                            Log.logger.info(f"成功添加列 {col} 到表 {table_name}")
                        except Exception as add_col_error:
                            conn_for_pragma.rollback()
                            if "duplicate column name" not in str(add_col_error).lower():
                                Log.logger.error(f"添加列 {col} 到表 {table_name} 失败: {str(add_col_error)}")
                                raise # Re-raise if it's not a duplicate column error
                            else:
                                Log.logger.warning(f"尝试添加已存在的列 {col} 到表 {table_name}: {str(add_col_error)}")
            except Exception as e:
                Log.logger.error(f"检查表 {table_name} 结构或添加列时出错: {str(e)}")
                # 继续执行，尝试写入数据，让后续步骤处理错误
            finally:
                if conn_for_pragma:
                    conn_for_pragma.close()
            
            return df.to_sql(table_name, engine, **final_to_sql_kwargs)

        except Exception as e:
            error_str = str(e)
            # 检查是否是"no such column"错误 (SQLite specific)
            if "no such column" in error_str.lower():
                column_match = re.search(r"no such column:?\s*[\x27\x22]?([^\x27\x22 ]+)[\x27\x22]?", error_str.lower())
                if column_match:
                    missing_column = column_match.group(1).strip()
                    Log.logger.warning(f"写入时表 {table_name} 中缺少列 {missing_column}，尝试添加该列")
                    
                    if missing_column in df.columns:
                        conn_for_add, _ = self.get_connection()
                        try:
                            cursor_for_add = conn_for_add.cursor()
                            col_type_str = str(df[missing_column].dtype)
                            sql_type = "TEXT"
                            if 'int' in col_type_str:
                                sql_type = "INTEGER"
                            elif 'float' in col_type_str:
                                sql_type = "REAL"
                            elif 'bool' in col_type_str:
                                sql_type = "INTEGER"
                            elif 'datetime' in col_type_str:
                                sql_type = "TEXT"
                            
                            alter_sql = f"ALTER TABLE {table_name} ADD COLUMN \"{missing_column}\" {sql_type}"
                            cursor_for_add.execute(alter_sql)
                            conn_for_add.commit()
                            Log.logger.info(f"成功添加列 {missing_column} 到表 {table_name}")
                            
                            # 重试写入
                            return df.to_sql(table_name, engine, **final_to_sql_kwargs)
                        except Exception as add_error:
                            if conn_for_add: conn_for_add.rollback()
                            Log.logger.error(f"重试时添加列 {missing_column} 失败: {str(add_error)}")
                            raise
                        finally:
                            if conn_for_add:
                                conn_for_add.close()
                    else:
                        Log.logger.error(f"DataFrame 中不包含尝试添加的未知列 {missing_column}")
                        raise
                else:
                     Log.logger.error(f"SQLite写入错误 (无法从错误消息中提取列名): {error_str}")
                     raise # Could not parse column name, re-raise
            
            Log.logger.error(f"SQLite写入DataFrame异常: {error_str}")
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
        
        # 第一步：尝试识别表的主要类型和需要的唯一约束
        unique_constraint_cols = []
        
        # 价格相关表 - 使用(ts_code, trade_date)作为唯一约束
        if ('price_daily' in table or 'price_weekly' in table or 
            'price_monthly' in table or 'daily' in table or 
            'adj_factor' in table):
            if self.column_exists(table, 'ts_code') and self.column_exists(table, 'trade_date'):
                unique_constraint_cols = ['ts_code', 'trade_date']
        
        # 财务相关表 - 使用(ts_code, end_date)作为唯一约束
        elif ('income' in table or 'balancesheet' in table or 
              'cashflow' in table or 'fina_indicator' in table or
              'express' in table):
            if self.column_exists(table, 'ts_code') and self.column_exists(table, 'end_date'):
                unique_constraint_cols = ['ts_code', 'end_date']
        
        # 公告相关表 - 使用(ts_code, ann_date)作为唯一约束
        elif ('announcement' in table or 'forecast' in table or
              'pledge' in table or 'repurchase' in table or
              'dividend' in table):
            if self.column_exists(table, 'ts_code') and self.column_exists(table, 'ann_date'):
                unique_constraint_cols = ['ts_code', 'ann_date']
        
        # 如果没有识别到特定类型，但存在ts_code和trade_date，也使用它们
        elif self.column_exists(table, 'ts_code') and self.column_exists(table, 'trade_date'):
            unique_constraint_cols = ['ts_code', 'trade_date']
        
        # 如果没有识别到特定类型，但存在ts_code和end_date，使用它们
        elif self.column_exists(table, 'ts_code') and self.column_exists(table, 'end_date'):
            unique_constraint_cols = ['ts_code', 'end_date']
        
        # 第二步：为所有表创建索引
        for index in index_list:
            try:
                # 首先检查表中是否存在该列
                if self.column_exists(table, index):
                    # 对于识别出的需要唯一约束的表
                    if unique_constraint_cols and index == unique_constraint_cols[-1] and self.column_exists(table, unique_constraint_cols[0]):
                        try:
                            # 先尝试删除可能存在的重复数据
                            constraint_cols_str = " AND ".join([f"b.{col} = a.{col}" for col in unique_constraint_cols])
                            self.exec_sql(f"""
                                DELETE FROM {table} 
                                WHERE rowid NOT IN (
                                    SELECT MIN(rowid) 
                                    FROM {table} AS a
                                    GROUP BY {', '.join(unique_constraint_cols)}
                                )
                            """)
                            Log.logger.info(f"已删除表 {table} 中的重复数据")
                            
                            # 创建唯一索引
                            unique_index_name = f"idx_{table}_unique_{'_'.join(unique_constraint_cols)}"
                            self.exec_sql(f"CREATE UNIQUE INDEX IF NOT EXISTS {unique_index_name} ON {table}({', '.join(unique_constraint_cols)})")
                            Log.logger.info(f"为表 {table} 创建唯一索引 ({', '.join(unique_constraint_cols)})")
                        except Exception as unique_error:
                            Log.logger.error(f"为表 {table} 创建唯一索引失败: {str(unique_error)}")
                    
                    # 为所有索引列创建普通索引(如果不是唯一索引的一部分)
                    if not (unique_constraint_cols and index in unique_constraint_cols):
                        sql = f"CREATE INDEX IF NOT EXISTS idx_{table}_{index} ON {table}({index})"
                        self.exec_sql(sql)
                else:
                    Log.logger.warning(f"表 {table} 中不存在列 {index}，跳过创建索引")
            except Exception as e:
                Log.logger.warning(f"为表 {table} 创建索引 {index} 失败: {str(e)}")
                
    def column_exists(self, table: str, column: str) -> bool:
        """检查表中是否存在指定列"""
        try:
            conn, cursor = self.get_connection()
            try:
                cursor.execute(f"PRAGMA table_info({table})")
                columns = [col["name"] for col in cursor.fetchall()]
                return column in columns
            finally:
                conn.close()
        except Exception as e:
            Log.logger.error(f"检查列 {column} 是否存在时发生错误: {str(e)}")
            return False
    
    @dbMonitor
    def replace_table(self, target_table: str, source_table: str) -> Tuple[bool, str]:
        """
        替换表（将source_table替换为target_table）
        专门处理SQLite的表替换逻辑
        
        Args:
            target_table: 目标表名
            source_table: 源表名
            
        Returns:
            Tuple[bool, str]: (是否成功, 最终使用的表名)
        """
        try:
            # 检查源表是否存在
            if not self.table_exists(source_table):
                Log.logger.error(f"源表 {source_table} 不存在")
                return False, target_table
            
            # 检查目标表是否存在
            target_exists = self.table_exists(target_table)
            
            # 备份目标表（如果存在）
            if target_exists:
                backup_table = f"{target_table}_old"
                try:
                    # 先删除可能存在的旧备份表
                    if self.table_exists(backup_table):
                        self.exec_sql(f"DROP TABLE IF EXISTS {backup_table}")
                    
                    # 将目标表重命名为备份表
                    self.exec_sql(f"ALTER TABLE {target_table} RENAME TO {backup_table}")
                    Log.logger.info(f"成功将原表重命名为 {backup_table}")
                except Exception as e:
                    Log.logger.error(f"备份表失败: {str(e)}")
                    return False, target_table
            
            try:
                # 将源表重命名为目标表
                self.exec_sql(f"ALTER TABLE {source_table} RENAME TO {target_table}")
                Log.logger.info(f"成功将临时表重命名为 {target_table}")
            except Exception as e:
                # 如果重命名失败，尝试恢复原表（如果之前存在）
                if target_exists:
                    try:
                        self.exec_sql(f"ALTER TABLE {backup_table} RENAME TO {target_table}")
                        Log.logger.warning(f"源表重命名失败，已恢复原表: {str(e)}")
                    except:
                        Log.logger.error(f"源表重命名失败，且无法恢复原表: {str(e)}")
                return False, source_table
            
            # 删除备份表
            if target_exists:
                try:
                    self.exec_sql(f"DROP TABLE IF EXISTS {backup_table}")
                    Log.logger.info(f"成功删除旧表 {backup_table}")
                except Exception as e:
                    Log.logger.warning(f"删除备份表失败: {str(e)}")
                    # 不影响主流程，继续执行
            
            return True, target_table
        except Exception as e:
            Log.logger.error(f"替换表失败: {str(e)}")
            return False, source_table
    
    def get_table_columns(self, table_name: str) -> list:
        """
        获取表的列名列表
        
        Args:
            table_name: 表名
            
        Returns:
            列名列表
        """
        try:
            engine = self.get_engine()
            with engine.connect() as conn:
                query = f"PRAGMA table_info({table_name})"
                result = conn.execute(text(query)).fetchall()
                columns = [row[1] for row in result]  # 列名在索引1的位置
                return columns
        except Exception as e:
            Log.logger.error(f"获取表 {table_name} 的列失败: {str(e)}")
            return [] 