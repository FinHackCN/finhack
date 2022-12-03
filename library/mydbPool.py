import configparser
import os
import pymysql
from sqlalchemy import create_engine
import pandas as pd
from library.config import config
import mysql.connector.pooling
from functools import lru_cache
import traceback
import pymysql
from dbutils.pooled_db import PooledDB
from dbutils.persistent_db import PersistentDB
from config import *

class mydbClass(object):
    def __init__(self,connection='default'):
        dbcfg=config.getConfig('db',connection)
        self.pool=PersistentDB(
            creator=pymysql,# 使用链接数据库的模块
            maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
            setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
            ping=0,         # ping MySQL服务端，检查是否服务可用。# 如：0 = None = never, 1 = default = whenever it is requested, 2 = when a cursor is created, 4 = when a query is executed, 7 = always
            closeable=False,# 如果为False时， conn.close() 实际上被忽略，供下次使用，再线程关闭时，才会自动关闭链接。如果为True时， conn.close()则关闭链接，那么再次调用pool.connection时就会报错，因为已经真的关闭了连接（pool.steady_connection()可以获取一个新的链接）
            #threadlocal=local,  # 如果为none，用默认的threading.Loacl对象，否则可以自己封装一个local对象进行替换
            host=dbcfg['host'],
            port=int(dbcfg['port']), 
            user=dbcfg['user'], 
            password=dbcfg['password'], 
            database=dbcfg['db'], 
        )
        # self.pool=PooledDB(
        #     creator=pymysql, #使用的链接数据库模块
        #     maxconnections=100, #连接池允许的最大连接数
        #     mincached=1, #初始化时，连接池中至少创建的空闲链接，0表示不创建
        #     maxcached=50,#连接池中允许最多的空闲连接数
        #     maxshared=0,#连接池中最多允许共享的连接数
        #     blocking=True,#连接池中如果没有可用的连接，当前的连接请求是否阻塞
        #     maxusage=None,#一个连接最多被重复使用的次数，None表示不限制
        #     setsession=[],
        #     ping=0,
        #     host=dbcfg['host'],
        #     port=int(dbcfg['port']), 
        #     user=dbcfg['user'], 
        #     password=dbcfg['password'], 
        #     database=dbcfg['db'], 
        #     charset=dbcfg['charset']
        # )
        
    def open(self):
        conn=self.pool.connection(shareable=False)
        cursor=conn.cursor(cursor=pymysql.cursors.DictCursor)
        return conn,cursor

    def close(self,conn,cursor):
        cursor.close()
        conn.close()
 
    # @staticmethod
    # def getDB(connection='default'):
    #     dbcfg=config.getConfig('db',connection)
    #     db = pymysql.connect(
    #         host=dbcfg['host'],
    #         port=int(dbcfg['port']), 
    #         user=dbcfg['user'], 
    #         password=dbcfg['password'], 
    #         db=dbcfg['db'], 
    #         charset=dbcfg['charset'],
    #         cursorclass=pymysql.cursors.DictCursor)
    #     return db,db.cursor() 
        
        
        
    def getDBEngine(connection='default'):
        dbcfg=config.getConfig('db',connection)
        engine=create_engine('mysql+pymysql://'+dbcfg['user']+':'+dbcfg['password']+'@'+dbcfg['host']+':'+dbcfg['port']+'/'+dbcfg['db']+'?charset='+dbcfg['charset'],encoding='utf-8')  
        return engine
    
    def toSql(self,df,table,connection='default'):
        engine=config.getDBEngine(connection)
        res = df.to_sql(table, engine, index=False, if_exists='append', chunksize=5000)
        
    def selectToList(self,sql,connection='default'):
        result_list=[]
        conn,cursor=self.open()
        try:
           cursor.execute(sql)
           results = cursor.fetchall()
           for row in results:
                result_list.append(row)
        except Exception as e:
           print(sql+"MySQL Error:%s" % str(e))
        self.close(conn,cursor)
        return result_list
        
    def select(self,sql,connection='default'):
        results=None
        conn,cursor=self.open()
        
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
           
        except Exception as e:
            if "Result length not requested length" in str(e):
                print('xxxxxxxxxxxxxxxxxxx')
                self.close(conn,cursor)
                return self.select(sql)
            print(sql+"MySQL Error:%s" % str(e))
            traceback.print_exc()
            os.system("ps -ef | grep preload | awk {'print $2'} | xargs kill")
            exit()
        self.close(conn,cursor)
        return results   
        
        
    def delete(self,sql,connection='default'):
        conn,cursor=self.open()
        try:
            cursor.execute(sql)
            db.commit()
        except Exception as e:
           print(sql+"MySQL Error:%s" % str(e))
        self.close(conn,cursor)  
        
        
    def exec(self,sql,connection='default'):
        conn,cursor=self.open()
        #print("Run Sql:"+sql)
        try:
            cursor.execute(sql)
            db.commit()
        except Exception as e:
           print(sql+"MySQL Error:%s" % str(e))
        self.close(conn,cursor)          
        
        
 
    def selectToDf(self,sql,connection='default'):
        conn,cursor=self.open()
        results=pd.DataFrame()
        
        try:
           cursor.execute(sql)
           results = cursor.fetchall()
           results= pd.DataFrame(list(results))
        except Exception as e:
           print(sql+"MySQL Error:%s" % str(e))
        self.close(conn,cursor)
        return results  
        
        
        
    def selectOne(self,sql,connection='default'):
        conn,cursor=self.open()
        results=pd.DataFrame()
        
        try:
           cursor.execute(sql)
           results = cursor.fetchall()
           results= pd.DataFrame(list(results))
        except Exception as e:
           print(sql+"MySQL Error:%s" % str(e))
        self.close(conn,cursor) 
        return results      
        
    def truncateTable(self,table,connection='default'):
        conn,cursor=self.open()
        try:
            sql=" truncate table "+table
            cursor.execute(sql)
        except Exception as e:
            print(sql+"MySQL Error:%s" % str(e))
        self.close(conn,cursor) 
        return True

db_tushare=mydbClass(connection='tushare')