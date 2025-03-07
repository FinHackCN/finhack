import configparser
import os
import re
import pymysql
from sqlalchemy import create_engine
import pandas as pd
import mysql.connector.pooling
from functools import lru_cache
from finhack.library.config import Config
from finhack.library.monitor import dbMonitor
from sqlalchemy import text
import finhack.library.log as Log

class mydb:

    @dbMonitor
    def getDB(connection='default'):
        dbcfg=Config.get_config('db',connection)
        db = pymysql.connect(
            host=dbcfg['host'],
            port=int(dbcfg['port']), 
            user=dbcfg['user'], 
            password=dbcfg['password'], 
            db=dbcfg['db'], 
            charset=dbcfg['charset'],
            cursorclass=pymysql.cursors.DictCursor)
        return db,db.cursor() 
        
        
        
    def getDBEngine(connection='default'):
        dbcfg=Config.get_config('db',connection)
        engine=create_engine('mysql+pymysql://'+dbcfg['user']+':'+dbcfg['password']+'@'+dbcfg['host']+':'+dbcfg['port']+'/'+dbcfg['db']+'?charset='+dbcfg['charset'],echo=False)  
        return engine
    
    def toSql(df,table,connection='default'):
        engine=Config.getDBEngine(connection)
        res = df.to_sql(table, engine, index=False, if_exists='append', chunksize=5000)
        
    @dbMonitor
    def selectToList(sql,connection='default'):
        result_list=[]
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
        db,cursor = mydb.getDB(connection)
        cursor.execute(sql)
        db.commit()
        db.close()  
        
    @dbMonitor  
    def exec(sql,connection='default'):
        db,cursor = mydb.getDB(connection)
        cursor.execute(sql)
        db.commit()
        db.close()          
        
        

    @dbMonitor
    def selectToDf(sql,connection='default'):
        db,cursor = mydb.getDB(connection)
        results=pd.DataFrame()
        cursor.execute(sql)
        results = cursor.fetchall()
        results= pd.DataFrame(list(results))
        db.close()  
        return results  
        


    @dbMonitor
    def select(sql,connection='default'):
        db,cursor = mydb.getDB(connection)
        results=pd.DataFrame()
        cursor.execute(sql)
        results = cursor.fetchall()
        cursor.close()
        db.close()  
        return results     
        
    @dbMonitor
    def selectOne(sql,connection='default'):
        db,cursor = mydb.getDB(connection)
        results=pd.DataFrame()
        cursor.execute(sql)
        results = cursor.fetchall()
        results= pd.DataFrame(list(results))
        db.close()  
        return results 
        
    @dbMonitor
    def truncateTable(table,connection='default'):
        db,cursor = mydb.getDB(connection)
        sql=" truncate table "+table
        cursor.execute(sql)
        db.close()  
        return True

    @dbMonitor
    def safe_to_sql(df, table_name, engine, **kwargs):
        """
        安全地将DataFrame写入SQL表，如果遇到列不存在的错误，会自动添加列并重试
        
        参数:
            df: 要写入的DataFrame
            table_name: 目标表名
            engine: SQLAlchemy引擎
            **kwargs: 传递给pandas.DataFrame.to_sql的其他参数
        
        返回:
            成功写入的行数
        """
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