import os
import re
import pymysql
import pandas as pd
from sqlalchemy import create_engine, text
from typing import List, Dict, Any, Optional, Tuple

import finhack.library.log as Log
from finhack.library.db_adpter.base import DbAdapter
from finhack.library.monitor import dbMonitor


class MySQLAdapter(DbAdapter):
    """MySQL数据库适配器实现"""
    
    def __init__(self, config):
        """
        初始化MySQL适配器
        
        Args:
            config: 数据库配置
        """
        self.config = config
        
    @dbMonitor
    def get_engine(self, read_only=False):
        """获取SQLAlchemy引擎"""
        conn_str = f"mysql+pymysql://{self.config['user']}:{self.config['password']}@" \
                  f"{self.config['host']}:{self.config['port']}/{self.config['db']}?charset={self.config['charset']}"
        return create_engine(conn_str, echo=False)
    
    @dbMonitor
    def get_connection(self):
        """获取数据库连接"""
        db = pymysql.connect(
            host=self.config['host'],
            port=int(self.config['port']), 
            user=self.config['user'], 
            password=self.config['password'], 
            db=self.config['db'], 
            charset=self.config['charset'],
            cursorclass=pymysql.cursors.DictCursor)
        return db, db.cursor()
    
    @dbMonitor
    def exec_sql(self, sql: str) -> None:
        """执行SQL语句"""
        db, cursor = self.get_connection()
        try:
            cursor.execute(sql)
            db.commit()
        except Exception as e:
            Log.logger.error(f"MySQL执行SQL错误: {str(e)}\nSQL: {sql}")
            raise
        finally:
            db.close()
    
    @dbMonitor
    def select_to_list(self, sql: str) -> List[Dict[str, Any]]:
        """执行SQL查询，返回结果列表"""
        result_list = []
        db, cursor = self.get_connection()
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                result_list.append(row)
        except Exception as e:
            Log.logger.error(f"MySQL查询错误: {str(e)}\nSQL: {sql}")
            raise
        finally:
            db.close()
        return result_list
    
    @dbMonitor
    def select_to_df(self, sql: str) -> pd.DataFrame:
        """执行SQL查询，返回DataFrame"""
        db, cursor = self.get_connection()
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            results = pd.DataFrame(list(results))
        except Exception as e:
            Log.logger.error(f"MySQL查询错误: {str(e)}\nSQL: {sql}")
            results = pd.DataFrame()
        finally:
            db.close()
        return results
    
    @dbMonitor
    def select(self, sql: str) -> List[Dict[str, Any]]:
        """执行SQL查询，返回结果集"""
        db, cursor = self.get_connection()
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
        except Exception as e:
            Log.logger.error(f"MySQL查询错误: {str(e)}\nSQL: {sql}")
            results = []
        finally:
            cursor.close()
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
        engine = self.get_engine()
        return df.to_sql(table_name, engine, index=False, if_exists=if_exists, chunksize=5000)
    
    def safe_to_sql(self, df: pd.DataFrame, table_name: str, **kwargs) -> int:
        """安全地将DataFrame写入数据库，处理可能的列缺失问题"""
        engine = self.get_engine()
        try:
            return df.to_sql(table_name, engine, **kwargs)
        except Exception as e:
            error_str = str(e)
            # 检查是否是"Unknown column"错误
            unknown_col_match = re.search(r"Unknown column '([^']+)'", error_str)
            
            if unknown_col_match:
                missing_column = unknown_col_match.group(1)
                Log.logger.warning(f"表 {table_name} 中缺少列 {missing_column}，尝试添加该列")
                
                # 获取列的数据类型
                col_type = str(df[missing_column].dtype)
                sql_type = "VARCHAR(255)"  # 默认类型
                
                # 根据pandas数据类型映射到SQL类型
                if "int" in col_type:
                    sql_type = "BIGINT"
                elif "float" in col_type:
                    sql_type = "DOUBLE"
                elif "datetime" in col_type:
                    sql_type = "DATETIME"
                
                # 添加缺失的列
                try:
                    with engine.connect() as conn:
                        conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {missing_column} {sql_type}"))
                        conn.commit()
                    Log.logger.info(f"成功添加列 {missing_column} 到表 {table_name}")
                    
                    # 重试写入操作
                    return df.to_sql(table_name, engine, **kwargs)
                except Exception as add_col_error:
                    Log.logger.error(f"添加列失败: {add_col_error}")
                    raise
            else:
                # 如果不是列缺失错误，则抛出原始异常
                raise
    
    @dbMonitor
    def truncate_table(self, table: str) -> bool:
        """截断表"""
        self.exec_sql(f"TRUNCATE TABLE {table}")
        return True
    
    @dbMonitor
    def delete(self, sql: str) -> None:
        """执行删除操作"""
        self.exec_sql(sql)
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            engine = self.get_engine()
            with engine.connect() as connection:
                query = f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}'"
                result = connection.execute(text(query)).fetchone()
                return result is not None
        except Exception as e:
            Log.logger.error(f"检查表 {table_name} 是否存在时发生错误: {str(e)}")
            return False
    
    def set_index(self, table: str) -> None:
        """设置表索引"""
        index_list=['ts_code','end_date','trade_date']
        for index in index_list:
            try:
                # MySQL索引语法 - 使用列长度限制
                sql = f"CREATE INDEX {index} ON {table} ({index}(10))"
                self.exec_sql(sql)
            except Exception as e:
                Log.logger.warning(f"为表 {table} 创建索引 {index} 失败: {str(e)}")
                
    @dbMonitor
    def replace_table(self, target_table: str, source_table: str) -> Tuple[bool, str]:
        """
        替换表（将source_table替换为target_table）
        专门处理MySQL的表替换逻辑
        
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
                self.exec_sql(f"RENAME TABLE {target_table} TO {target_table}_old")
                Log.logger.info(f"成功将原表重命名为 {target_table}_old")
                
            # 重命名临时表为目标表
            self.exec_sql(f"RENAME TABLE {source_table} TO {target_table}")
            Log.logger.info(f"成功将临时表重命名为 {target_table}")
            
            # 如果有备份表，则删除它
            if self.table_exists(f"{target_table}_old"):
                self.exec_sql(f"DROP TABLE IF EXISTS {target_table}_old")
                Log.logger.info(f"成功删除旧表 {target_table}_old")
                
            return True, target_table
        except Exception as e:
            Log.logger.error(f"MySQL表替换失败: {str(e)}")
            # 如果重命名失败，尝试使用临时表
            if self.table_exists(source_table):
                Log.logger.warning(f"无法完成表替换，将使用临时表 {source_table} 作为最终表")
                return False, source_table
            # 如果临时表也不存在，尝试恢复原表
            elif self.table_exists(f"{target_table}_old"):
                try:
                    self.exec_sql(f"RENAME TABLE {target_table}_old TO {target_table}")
                    Log.logger.warning(f"恢复原表 {target_table}")
                    return False, target_table
                except Exception as restore_e:
                    Log.logger.error(f"恢复原表失败: {str(restore_e)}")
                    return False, f"{target_table}_old"
            else:
                return False, ""  # 无可用表 