import configparser
import os
import re
import pymysql
from sqlalchemy import create_engine
import pandas as pd
import mysql.connector.pooling
import duckdb
from functools import lru_cache
from finhack.library.config import Config
from finhack.library.monitor import dbMonitor
from sqlalchemy import text
import finhack.library.log as Log

class mydb:

    @dbMonitor
    def getDB(connection='default'):
        dbcfg=Config.get_config('db',connection)
        # 检查数据库类型
        db_type = dbcfg.get('type', 'mysql')

        if db_type.lower() == 'duckdb':
            # DuckDB连接
            db_path = dbcfg.get('path', ':memory:')
            db = duckdb.connect(db_path)
            return db, db
        else:
            # MySQL连接
            db = pymysql.connect(
                host=dbcfg['host'],
                port=int(dbcfg['port']), 
                user=dbcfg['user'], 
                password=dbcfg['password'], 
                db=dbcfg['db'], 
                charset=dbcfg['charset'],
                cursorclass=pymysql.cursors.DictCursor)
            return db,db.cursor() 
        
        
        
    def getDBEngine(connection='default', read_only=False):
        dbcfg = Config.get_config('db', connection)
        db_type = dbcfg.get('type', 'mysql').lower()
        if db_type == 'duckdb':
            db_path = dbcfg.get('path', ':memory:')
            
            # 非内存数据库需要检测路径
            if db_path not in (':memory:', ''):
                abs_path = os.path.abspath(db_path)
                parent_dir = os.path.dirname(abs_path)
                
                # 递归创建父目录（如果不存在）
                if parent_dir:  # 避免空路径（如当前目录）
                    os.makedirs(parent_dir, exist_ok=True)

            # 创建数据库连接
            return duckdb.connect(db_path, read_only=read_only)
        
        else:
            # 保持原有MySQL连接逻辑
            conn_str = f"mysql+pymysql://{dbcfg['user']}:{dbcfg['password']}@" \
                    f"{dbcfg['host']}:{dbcfg['port']}/{dbcfg['db']}?charset={dbcfg['charset']}"
            return create_engine(conn_str, echo=False)
    
    def toSql(df,table,connection='default'):
        dbcfg=Config.get_config('db',connection)
        db_type = dbcfg.get('type', 'mysql')
        
        if db_type.lower() == 'duckdb':
            # DuckDB写入
            conn = mydb.getDBEngine(connection)
            
            # 预处理DataFrame，确保所有字段都转为字符串类型
            for col in df.columns:
                # 强制将所有列转为字符串，避免DuckDB尝试自动转换
                df[col] = df[col].fillna('').astype(str)
                # 将字符串'None'替换为空字符串
                df[col] = df[col].replace('None', '')
            
            # 创建表结构，所有字段都使用VARCHAR类型
            create_stmt = f"CREATE TABLE IF NOT EXISTS {table} ("
            columns = []
            for col in df.columns:
                # 所有字段都使用VARCHAR类型
                sql_type = "VARCHAR"
                columns.append(f"\"{col}\" {sql_type}")
            
            create_stmt += ", ".join(columns) + ")"
            conn.execute(create_stmt)
            
            # 将DataFrame注册为临时视图，然后插入数据
            conn.register("temp_df", df)
            conn.execute(f"INSERT INTO {table} SELECT * FROM temp_df")
            return len(df)
        else:
            # MySQL写入
            engine=mydb.getDBEngine(connection)
            res = df.to_sql(table, engine, index=False, if_exists='append', chunksize=5000)
            return res
        
    @dbMonitor
    def selectToList(sql,connection='default'):
        result_list=[]
        dbcfg=Config.get_config('db',connection)
        db_type = dbcfg.get('type', 'mysql')
        
        if db_type.lower() == 'duckdb':
            # DuckDB查询
            db, _ = mydb.getDB(connection)
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
                print(sql+"DuckDB Error:%s" % str(e))
            db.close()
        else:
            # MySQL查询
            db,cursor = mydb.getDB(connection)
            try:
               cursor.execute(sql)
               results = cursor.fetchall()
               for row in results:
                    result_list.append(row)
            except Exception as e:
               print(sql+"MySQL Error:%s" % str(e))
            db.close()  
        return result_list
        
    @dbMonitor 
    def delete(sql,connection='default'):
        dbcfg=Config.get_config('db',connection)
        db_type = dbcfg.get('type', 'mysql')
        
        if db_type.lower() == 'duckdb':
            # DuckDB删除
            db, _ = mydb.getDB(connection)
            db.execute(sql)
            db.close()
        else:
            # MySQL删除
            db,cursor = mydb.getDB(connection)
            cursor.execute(sql)
            db.commit()
            db.close()  
        
    @dbMonitor  
    def exec(sql,connection='default'):
        dbcfg=Config.get_config('db',connection)
        db_type = dbcfg.get('type', 'mysql')
        
        if db_type.lower() == 'duckdb':
            # DuckDB执行
            db, _ = mydb.getDB(connection)
            # 处理表重命名操作，DuckDB使用ALTER TABLE而不是RENAME TABLE
            if sql.lower().startswith('rename table'):
                # 解析表名
                parts = sql.split()
                if len(parts) == 5 and parts[3].lower() == 'to':
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
            db.close()
        else:
            # MySQL执行
            db,cursor = mydb.getDB(connection)
            cursor.execute(sql)
            db.commit()
            db.close()          
        
        

    @dbMonitor
    def selectToDf(sql,connection='default'):
        dbcfg=Config.get_config('db',connection)
        db_type = dbcfg.get('type', 'mysql')
        
        if db_type.lower() == 'duckdb':
            # DuckDB查询到DataFrame
            db, _ = mydb.getDB(connection)
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
                print(sql+"DuckDB Error:%s" % str(e))
                results = pd.DataFrame()
            db.close()
        else:
            # MySQL查询到DataFrame
            db,cursor = mydb.getDB(connection)
            results=pd.DataFrame()
            cursor.execute(sql)
            results = cursor.fetchall()
            results= pd.DataFrame(list(results))
            db.close()  
        return results  
        


    @dbMonitor
    def select(sql,connection='default'):
        dbcfg=Config.get_config('db',connection)
        db_type = dbcfg.get('type', 'mysql')
        
        if db_type.lower() == 'duckdb':
            # DuckDB查询
            db, _ = mydb.getDB(connection)
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
                print(sql+"DuckDB Error:%s" % str(e))
                results = []
            db.close()
        else:
            # MySQL查询
            db,cursor = mydb.getDB(connection)
            results=pd.DataFrame()
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
            db.close()  
        return results     
        
    @dbMonitor
    def selectOne(sql,connection='default'):
        dbcfg=Config.get_config('db',connection)
        db_type = dbcfg.get('type', 'mysql')
        
        if db_type.lower() == 'duckdb':
            # DuckDB查询单条
            results = mydb.selectToDf(sql, connection)
            if len(results) > 0:
                return results.iloc[0:1]
            return pd.DataFrame()
        else:
            # MySQL查询单条
            db,cursor = mydb.getDB(connection)
            results=pd.DataFrame()
            cursor.execute(sql)
            results = cursor.fetchall()
            results= pd.DataFrame(list(results))
            db.close()  
        return results 
        
    @dbMonitor
    def truncateTable(table,connection='default'):
        dbcfg=Config.get_config('db',connection)
        db_type = dbcfg.get('type', 'mysql')
        
        if db_type.lower() == 'duckdb':
            # DuckDB截断表
            db, _ = mydb.getDB(connection)
            # DuckDB不支持TRUNCATE，使用DELETE代替
            db.execute(f"DELETE FROM {table}")
            db.close()
        else:
            # MySQL截断表
            db,cursor = mydb.getDB(connection)
            sql=" truncate table "+table
            cursor.execute(sql)
            db.close()  
        return True

    @dbMonitor
    def safe_to_sql(df, table_name, engine, **kwargs):
        """
        安全地将DataFrame写入数据库表，处理可能的列缺失问题
        """
        # 检查是否是DuckDB连接
        is_duckdb = isinstance(engine, duckdb.DuckDBPyConnection)
        
        if is_duckdb:
            try:
                # 检查表是否存在 - 使用正确的DuckDB语法
                table_exists = engine.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name='{table_name}'").fetchone() is not None
                
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
                
                if not table_exists:
                    # 创建表，所有字段都使用VARCHAR类型
                    create_stmt = "CREATE TABLE {}(".format(table_name)
                    columns = []
                    for col in df.columns:
                        # 所有字段都使用VARCHAR类型
                        sql_type = "VARCHAR"
                        columns.append(f"\"{col}\" {sql_type}")
                    
                    create_stmt += ", ".join(columns) + ")"
                    engine.execute(create_stmt)
                else:
                    # 检查是否需要添加列 - 使用DuckDB的information_schema
                    existing_columns = set()
                    for col in engine.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}'").fetchall():
                        existing_columns.add(col[0])  # 列名在第一个位置
                    
                    # 添加缺失的列
                    for col in df.columns:
                        if col not in existing_columns:
                            # 所有新增列都使用VARCHAR类型
                            sql_type = "VARCHAR"
                            try:
                                engine.execute(f"ALTER TABLE {table_name} ADD COLUMN \"{col}\" {sql_type}")
                                Log.logger.info(f"成功添加列 {col} 到表 {table_name}")
                            except Exception as add_col_error:
                                # 如果添加列失败，记录错误但继续处理其他列
                                Log.logger.warning(f"添加列 {col} 失败: {str(add_col_error)}")
                                # 如果错误是列已存在，可以忽略继续
                                if "already exists" not in str(add_col_error):
                                    raise
                
                # # 最后一次检查所有列，确保类型正确
                # for col in df.columns:
                #     # 对于股票代码和日期相关列，确保是字符串类型
                #     if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                #        'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                #         df[col] = df[col].fillna('').astype(str)
                #         df[col] = df[col].replace('None', '')
                #     # 对于特定的金融数值字段和数值类型列，确保正确处理
                #     elif col in ['min_amount', 'exp_return', 'fund_size', 'amount', 'value'] or \
                #          'float' in str(df[col].dtype) or 'int' in str(df[col].dtype):
                #         # 先将列转换为字符串类型，以便正确处理所有值
                #         df[col] = df[col].astype(str)
                #         # 将'None'、空字符串和各种NaN表示替换为None
                #         df[col] = df[col].replace(['None', 'nan', 'NaN', 'NAN', '', 'null', 'NULL'], None)
                #         # 使用coerce模式处理无法转换的值，将它们转为NaN
                #         df[col] = pd.to_numeric(df[col], errors='coerce')
                #         # 检查是否有NaN值，如果有则填充为0
                #         if df[col].isna().any():
                #             Log.logger.warning(f"列 {col} 中存在无法转换为数值的值，已替换为0")
                #             df[col] = df[col].fillna(0)
                #     # 对于其他object类型列，确保是字符串类型
                #     elif df[col].dtype == 'object':
                #         df[col] = df[col].fillna('').astype(str)
                
                # 将DataFrame注册为临时视图，然后插入数据
                if df.empty:
                    #Log.logger.warning(f"DataFrame {table_name} 为空，跳过写入")
                    return 0
                engine.register("temp_df", df)
                engine.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
                return len(df)
            except Exception as e:
                Log.logger.error(f"DuckDB写入错误: {str(e)}")
                Log.logger.error(f"错误数据: {df}, {table_name}, {engine}")
                raise
        else:
            # 原有的MySQL处理逻辑
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

    @staticmethod
    def tableExists(table_name, connection='default'):
        """
        检查数据表是否存在
        
        Args:
            table_name (str): 要检查的表名
            connection (str): 数据库连接配置名称
            
        Returns:
            bool: 表是否存在
        """
        try:
            dbcfg = Config.get_config('db', connection)
            db_type = dbcfg.get('type', 'mysql').lower()
            
            if db_type == 'duckdb':
                # DuckDB 的处理方式
                db = mydb.getDBEngine(connection)
                query = f"SELECT table_name FROM information_schema.tables WHERE table_name='{table_name}'"
                result = db.execute(query).fetchone()
                return result is not None
            else:
                # MySQL 的处理方式
                engine = mydb.getDBEngine(connection)
                with engine.connect() as connection:
                    query = f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}'"
                    result = connection.execute(text(query)).fetchone()
                    return result is not None
        except Exception as e:
            Log.logger.error(f"检查表 {table_name} 是否存在时发生错误: {str(e)}")
            return False

    def setIndex(table,db='default'):
        dbcfg=Config.get_config('db',db)
        db_type = dbcfg.get('type', 'mysql')
        
        index_list=['ts_code','end_date','trade_date']
        for index in index_list:
            try:
                if db_type.lower() == 'duckdb':
                    # DuckDB索引语法 - 不使用列长度限制
                    sql = f"CREATE INDEX IF NOT EXISTS idx_{table}_{index} ON {table}({index})"
                    # 执行创建索引 - DuckDB方式
                    db, _ = mydb.getDB(db)
                    db.execute(sql)
                    db.close()
                else:
                    # MySQL索引语法 - 使用列长度限制
                    sql = f"CREATE INDEX {index} ON {table} ({index}(10))"
                    # 执行创建索引 - MySQL方式
                    mydb.exec(sql, db)
            except Exception as e:
                Log.logger.warning(f"为表 {table} 创建索引 {index} 失败: {str(e)}")