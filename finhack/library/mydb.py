import configparser
import os
import pymysql
from sqlalchemy import create_engine
import pandas as pd
import mysql.connector.pooling
from functools import lru_cache
from finhack.library.config import Config
from finhack.library.monitor import dbMonitor

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