import os
import re
import pandas as pd
import duckdb
from typing import List, Dict, Any, Optional, Tuple

import finhack.library.log as Log
from finhack.library.db_adpter.base import DbAdapter
from finhack.library.monitor import dbMonitor


class DuckDBAdapter(DbAdapter):
    """DuckDB数据库适配器实现"""
    
    def __init__(self, config):
        """
        初始化DuckDB适配器
        
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
        """获取DuckDB连接"""
        return duckdb.connect(self.db_path, read_only=read_only)
    
    @dbMonitor
    def get_connection(self):
        """获取数据库连接"""
        db = duckdb.connect(self.db_path)
        return db, db
    
    @dbMonitor
    def exec_sql(self, sql: str) -> None:
        """执行SQL语句"""
        db = self.get_engine()
        try:
            # 处理表重命名操作，DuckDB使用ALTER TABLE而不是RENAME TABLE
            if sql.lower().startswith('rename table'):
                # 解析表名
                parts = sql.split()
                if len(parts) >= 5 and parts[3].lower() == 'to':
                    old_table = parts[2]
                    new_table = parts[4].rstrip(';')
                    # 使用DuckDB的ALTER TABLE语法
                    sql = f"ALTER TABLE {old_table} RENAME TO {new_table}"
            
            # 替换SQL中的ISNULL函数为DuckDB支持的IS NULL语法
            if 'ISNULL' in sql:
                # 替换 not ISNULL(column) 为 column IS NOT NULL
                sql = sql.replace('not ISNULL(', '').replace('NOT ISNULL(', '')
                sql = sql.replace(')', ' IS NOT NULL')
                # 替换 ISNULL(column) 为 column IS NULL
                sql = sql.replace('ISNULL(', '').replace('ISNULL(', '')
                sql = sql.replace(')', ' IS NULL')
                
            db.execute(sql)
        except Exception as e:
            Log.logger.error(f"DuckDB执行SQL错误: {str(e)}\nSQL: {sql}")
            raise
        finally:
            db.close()
    
    @dbMonitor
    def select_to_list(self, sql: str) -> List[Dict[str, Any]]:
        """执行SQL查询，返回结果列表"""
        result_list = []
        db = self.get_engine()
        try:
            # 替换SQL中的ISNULL函数为DuckDB支持的IS NULL语法
            if 'ISNULL' in sql:
                # 替换 not ISNULL(column) 为 column IS NOT NULL
                sql = sql.replace('not ISNULL(', '').replace('NOT ISNULL(', '')
                sql = sql.replace(')', ' IS NOT NULL')
                # 替换 ISNULL(column) 为 column IS NULL
                sql = sql.replace('ISNULL(', '').replace('ISNULL(', '')
                sql = sql.replace(')', ' IS NULL')
            
            results = db.execute(sql).fetchall()
            for row in results:
                # 转换为字典格式，与MySQL保持一致
                result_dict = {}
                for i, col_name in enumerate(db.execute(sql).description):
                    result_dict[col_name[0]] = row[i]
                result_list.append(result_dict)
        except Exception as e:
            Log.logger.error(f"DuckDB查询错误: {str(e)}\nSQL: {sql}")
        finally:
            db.close()
        return result_list
    
    @dbMonitor
    def select_to_df(self, sql: str) -> pd.DataFrame:
        """执行SQL查询，返回DataFrame"""
        db = self.get_engine()
        try:
            # 替换SQL中的ISNULL函数为DuckDB支持的IS NULL语法
            if 'ISNULL' in sql:
                # 替换 not ISNULL(column) 为 column IS NOT NULL
                sql = sql.replace('not ISNULL(', '').replace('NOT ISNULL(', '')
                sql = sql.replace(')', ' IS NOT NULL')
                # 替换 ISNULL(column) 为 column IS NULL
                sql = sql.replace('ISNULL(', '').replace('ISNULL(', '')
                sql = sql.replace(')', ' IS NULL')
                
            results = db.execute(sql).df()
        except Exception as e:
            Log.logger.error(f"DuckDB查询错误: {str(e)}\nSQL: {sql}")
            results = pd.DataFrame()
        finally:
            db.close()
        return results
    
    @dbMonitor
    def select(self, sql: str) -> List[Dict[str, Any]]:
        """执行SQL查询，返回结果集"""
        db = self.get_engine()
        try:
            results = db.execute(sql).fetchall()
            # 转换为与MySQL一致的格式
            dict_results = []
            for row in results:
                result_dict = {}
                for i, col_name in enumerate(db.execute(sql).description):
                    result_dict[col_name[0]] = row[i]
                dict_results.append(result_dict)
            results = dict_results
        except Exception as e:
            Log.logger.error(f"DuckDB查询错误: {str(e)}\nSQL: {sql}")
            results = []
        finally:
            db.close()
        return results
    
    @dbMonitor
    def select_one(self, sql: str) -> Optional[Dict[str, Any]]:
        """执行SQL查询，返回单条结果"""
        results = self.select_to_df(sql)
        if len(results) > 0:
            return results.iloc[0:1]
        return pd.DataFrame()
    
    def to_sql(self, df: pd.DataFrame, table_name: str, if_exists='append', **kwargs) -> int:
        """将DataFrame写入数据库"""
        conn = self.get_engine()
        
        try:
            # 预处理DataFrame，确保所有字段都转为字符串类型
            for col in df.columns:
                # 强制将所有列转为字符串，避免DuckDB尝试自动转换
                df[col] = df[col].fillna('').astype(str)
                # 将字符串'None'替换为空字符串
                df[col] = df[col].replace('None', '')
            
            # 创建表结构，所有字段都使用VARCHAR类型
            create_stmt = f"CREATE TABLE IF NOT EXISTS {table_name} ("
            columns = []
            for col in df.columns:
                # 所有字段都使用VARCHAR类型
                sql_type = "VARCHAR"
                columns.append(f"\"{col}\" {sql_type}")
            
            create_stmt += ", ".join(columns) + ")"
            conn.execute(create_stmt)
            
            # 将DataFrame注册为临时视图，然后插入数据
            conn.register("temp_df", df)
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
            return len(df)
        finally:
            conn.close()
    
    def safe_to_sql(self, df: pd.DataFrame, table_name: str, **kwargs) -> int:
        """安全地将DataFrame写入数据库，处理可能的列缺失问题"""
        # 空DataFrame检查
        if df.empty or len(df.columns) == 0:
            Log.logger.warning(f"尝试写入空DataFrame到表 {table_name}, 操作已跳过")
            return 0
            
        conn = self.get_engine()
        
        try:
            # 检查表是否存在
            table_exists = conn.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name='{table_name}'").fetchone() is not None
            
            # 预处理DataFrame，确保所有字段类型正确
            for col in df.columns:
                # 对于可能包含股票代码/日期的列，强制转为字符串类型
                if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                   'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                    # 将None值替换为空字符串，避免'None'字符串转换错误
                    df[col] = df[col].fillna('').astype(str)
                    # 将字符串'None'替换为空字符串
                    df[col] = df[col].replace('None', '')
                # 对于数值类型的列，将None、'None'和空字符串替换为0
                elif 'float' in str(df[col].dtype) or 'int' in str(df[col].dtype) or col in ['min_amount', 'exp_return']:
                    # 先将列转换为字符串类型，以便正确处理所有值
                    df[col] = df[col].astype(str)
                    # 将'None'、空字符串和各种NaN表示替换为None
                    df[col] = df[col].replace(['None', 'nan', 'NaN', 'NAN', '', 'null', 'NULL'], None)
                    # 将None替换为0，使用coerce模式处理无法转换的值
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                # 对于object类型的列，确保转为字符串类型
                elif df[col].dtype == 'object':
                    df[col] = df[col].fillna('').astype(str)
            
            # 如果表不存在，直接创建表
            if not table_exists:
                # 准备参数
                write_params = kwargs.copy()
                if 'if_exists' not in write_params:
                    write_params['if_exists'] = 'append'
                
                # 将DataFrame写入DuckDB
                df.to_sql(table_name, conn, **write_params)
                return len(df)
            
            # 表存在，检查列结构
            try:
                # 获取表中现有的列
                columns_info = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
                existing_columns = {row[1] for row in columns_info}  # 列名在索引1的位置
                
                # 检查是否需要添加列
                missing_columns = [col for col in df.columns if col not in existing_columns]
                if missing_columns:
                    Log.logger.info(f"表 {table_name} 缺少 {len(missing_columns)} 列：{', '.join(missing_columns)}")
                    
                    # 添加缺失的列
                    for col in missing_columns:
                        # 确定列类型
                        col_type = "VARCHAR"  # 默认类型
                        dtype = str(df[col].dtype)
                        
                        if "int" in dtype:
                            col_type = "BIGINT"
                        elif "float" in dtype:
                            col_type = "DOUBLE"
                        elif "bool" in dtype:
                            col_type = "BOOLEAN"
                        elif "datetime" in dtype:
                            col_type = "TIMESTAMP"
                        
                        try:
                            # 添加列
                            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN \"{col}\" {col_type}")
                            Log.logger.info(f"成功添加列 {col} 到表 {table_name}")
                        except Exception as add_col_error:
                            # 如果是列已存在错误，忽略它
                            if "Duplicate column name" not in str(add_col_error):
                                Log.logger.error(f"添加列 {col} 失败: {str(add_col_error)}")
                                # 继续处理其他列，不抛出异常
            
            except Exception as e:
                Log.logger.warning(f"检查表 {table_name} 结构时出错: {str(e)}")
                # 继续执行，尝试写入数据
            
            # 准备写入参数
            write_params = kwargs.copy()
            if 'if_exists' not in write_params:
                write_params['if_exists'] = 'append'
            
            # 尝试写入数据
            return df.to_sql(table_name, conn, **write_params)
            
        except Exception as e:
            error_str = str(e)
            
            # 检查是否是列不存在错误
            column_not_found = re.search(r"Column ([^'\"]+)['\"] .* does not exist", error_str) or \
                              re.search(r"column ['\"]?([^'\"]+)['\"]? does not exist", error_str)
            
            if column_not_found:
                missing_column = column_not_found.group(1)
                Log.logger.warning(f"写入时发现缺少列 {missing_column}，尝试添加并重试")
                
                try:
                    # 确定列类型
                    col_type = "VARCHAR"  # 默认类型
                    if missing_column in df.columns:
                        dtype = str(df[missing_column].dtype)
                        if "int" in dtype:
                            col_type = "BIGINT"
                        elif "float" in dtype:
                            col_type = "DOUBLE"
                        elif "bool" in dtype:
                            col_type = "BOOLEAN"
                        elif "datetime" in dtype:
                            col_type = "TIMESTAMP"
                    
                    # 添加列
                    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN \"{missing_column}\" {col_type}")
                    Log.logger.info(f"成功添加列 {missing_column} 到表 {table_name}")
                    
                    # 重试写入
                    return self.safe_to_sql(df, table_name, **kwargs)
                    
                except Exception as add_error:
                    Log.logger.error(f"添加列 {missing_column} 失败: {str(add_error)}")
                    raise
            
            Log.logger.error(f"DuckDB写入错误: {error_str}")
            raise
    
    @dbMonitor
    def truncate_table(self, table: str) -> bool:
        """截断表"""
        # DuckDB不支持TRUNCATE，使用DELETE代替
        self.exec_sql(f"DELETE FROM {table}")
        return True
    
    @dbMonitor
    def delete(self, sql: str) -> None:
        """执行删除操作"""
        self.exec_sql(sql)
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            db = self.get_engine()
            query = f"SELECT table_name FROM information_schema.tables WHERE table_name='{table_name}'"
            result = db.execute(query).fetchone()
            db.close()
            return result is not None
        except Exception as e:
            Log.logger.error(f"检查表 {table_name} 是否存在时发生错误: {str(e)}")
            return False
    
    def set_index(self, table: str) -> None:
        """设置表索引"""
        index_list=['ts_code','end_date','trade_date']
        for index in index_list:
            try:
                # DuckDB索引语法 - 不使用列长度限制
                sql = f"CREATE INDEX IF NOT EXISTS idx_{table}_{index} ON {table}({index})"
                self.exec_sql(sql)
            except Exception as e:
                Log.logger.warning(f"为表 {table} 创建索引 {index} 失败: {str(e)}")
        
    @dbMonitor
    def replace_table(self, target_table: str, source_table: str) -> Tuple[bool, str]:
        """
        替换表（将source_table替换为target_table）
        专门处理DuckDB的表替换逻辑
        
        Args:
            target_table: 目标表名
            source_table: 源表名（通常是临时表）
            
        Returns:
            一个元组 (成功状态, 最终使用的表名)
        """
        try:
            # 检查目标表是否存在
            result = self.select_to_list(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{target_table}'")
            if result:
                # 表存在，先尝试删除原表
                try:
                    # 使用 CASCADE 选项删除表及其依赖
                    self.exec_sql(f"DROP TABLE IF EXISTS {target_table} CASCADE")
                    Log.logger.info(f"成功删除原表 {target_table}")
                except Exception as e:
                    Log.logger.error(f"删除原表失败: {str(e)}")
                    # 尝试使用其他方式处理依赖关系
                    try:
                        # 查找依赖关系
                        deps_query = f"SELECT * FROM duckdb_dependencies() WHERE dependency_name = '{target_table}'"
                        deps = self.select_to_list(deps_query)
                        if deps:
                            Log.logger.info(f"表 {target_table} 存在依赖关系，尝试处理")
                        
                        # 再次尝试删除
                        self.exec_sql(f"DROP TABLE IF EXISTS {target_table}")
                    except Exception as inner_e:
                        Log.logger.error(f"处理依赖关系失败: {str(inner_e)}")
                        # 如果无法删除，备份原表
                        self.exec_sql(f"ALTER TABLE {target_table} RENAME TO {target_table}_backup")
                        Log.logger.info(f"已将原表重命名为 {target_table}_backup")
            
            # 重命名临时表
            self.exec_sql(f"ALTER TABLE {source_table} RENAME TO {target_table}")
            Log.logger.info(f"成功将临时表重命名为 {target_table}")
            return True, target_table
        except Exception as e:
            Log.logger.error(f"DuckDB表替换失败: {str(e)}")
            # 如果所有尝试都失败，使用临时表
            Log.logger.warning(f"无法完成表替换，将使用临时表 {source_table} 作为最终表")
            return False, source_table 