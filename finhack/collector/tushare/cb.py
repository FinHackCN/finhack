import sys
import time
import datetime
import traceback
import pandas as pd

from finhack.library.db import DB
from finhack.library.alert import alert
from finhack.library.monitor import tsMonitor
from finhack.collector.tushare.helper import tsSHelper
import finhack.library.log as Log

class tsCB:
    @tsMonitor
    def cb_basic(pro,db):
        tsSHelper.getDataAndReplace(pro,'cb_basic','cb_basic',db)
        
    
    @tsMonitor
    def cb_issue(pro,db):
        tsSHelper.getDataWithLastDate(pro,'cb_issue','cb_issue',db,'ann_date')
        
    @tsMonitor
    def cb_call(pro,db):
        tsSHelper.getDataAndReplace(pro,'cb_call','cb_call',db)
        
    @tsMonitor
    def cb_daily(pro,db):
        tsSHelper.getDataWithLastDate(pro,'cb_daily','cb_daily',db)
   
   
   
    @tsMonitor
    def get_cb_list(pro,db):
        sql='select * from cb_basic'
        data=DB.select_to_df(sql,db)
        return data
   
   
    @tsMonitor
    def cb_price_chg(pro,db):
        table='cb_price_chg'
        api='cb_price_chg'
        DB.exec("drop table if exists "+table+"_tmp",db)
        # 不需要获取engine对象，直接使用db连接名
        # engine = DB.get_db_engine(db)
        data=tsCB.get_cb_list(pro,db)
        cb_list=data['ts_code'].tolist()
        
        for ts_code in cb_list:
            try_times=0
            while True:
                try:
                    df = pro.cb_price_chg(ts_code=ts_code)
                    #df.to_sql(table+'_tmp', engine, index=False, if_exists='append', chunksize=5000)
                    DB.safe_to_sql(df, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
                    break
                except Exception as e:
                    if tsSHelper.is_permanent_error(e):
                        # 永久错误: token 无此接口权限, 重试再多次也没用 → 整表立即放弃(不再 10×重试 浪费时间)
                        Log.logger.error(api+": token 无此接口权限, 整表跳过: "+str(e).split('。')[0][:80])
                        return
                    if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning(api+":触发最多访问。\n"+str(e)) 
                        return
                    if "最多访问" in str(e):
                        Log.logger.warning(api+":触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    else:
                        if try_times<10:
                            try_times=try_times+1;
                            Log.logger.error(api+":函数异常，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:
                            info = traceback.format_exc()
                            alert.send(api,'函数异常',str(info))
                            Log.logger.error(info)
                            break
            
        # 【SQLite 兼容】原 "rename table X to Y" 是 MySQL 专有语法, SQLite 不认,
        # 导致 _tmp 堆了数据却换不过去、正式表永远空。改用 "ALTER TABLE X RENAME TO Y"(两者通用)。
        # 首次运行无正式表时第一条会失败, try 跳过即可。
        try:
            DB.exec(f"ALTER TABLE {table} RENAME TO {table}_old", db)
        except Exception:
            pass
        DB.exec(f"ALTER TABLE {table}_tmp RENAME TO {table}", db)
        DB.exec(f"DROP TABLE IF EXISTS {table}_old", db)
        tsSHelper.setIndex(table,db)  
        

    @tsMonitor
    def cb_share(pro,db):
        table='cb_share'
        api='cb_share'
        DB.exec("drop table if exists "+table+"_tmp",db)
        # 不需要获取engine对象，直接使用db连接名
        # engine = DB.get_db_engine(db)
        data=tsCB.get_cb_list(pro,db)
        cb_list=data['ts_code'].tolist()
        
        for ts_code in cb_list:
            try_times=0
            while True:
                try:
                    df = pro.cb_share(ts_code=ts_code)
                    #df.to_sql(table+'_tmp', engine, index=False, if_exists='append', chunksize=5000)
                    DB.safe_to_sql(df, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
                    break
                except Exception as e:
                    if tsSHelper.is_permanent_error(e):
                        # 永久错误: token 无此接口权限, 重试再多次也没用 → 整表立即放弃(不再 10×重试 浪费时间)
                        Log.logger.error(api+": token 无此接口权限, 整表跳过: "+str(e).split('。')[0][:80])
                        return
                    if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning(api+":触发最多访问。\n"+str(e)) 
                        return
                    if "最多访问" in str(e):
                        Log.logger.warning(api+":触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    else:
                        if try_times<10:
                            try_times=try_times+1;
                            Log.logger.error(api+":函数异常，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:
                            info = traceback.format_exc()
                            alert.send(api,'函数异常',str(info))
                            Log.logger.error(info)
                            break
            
        # 【SQLite 兼容】原 "rename table X to Y" 是 MySQL 专有语法, SQLite 不认,
        # 导致 _tmp 堆了数据却换不过去、正式表永远空。改用 "ALTER TABLE X RENAME TO Y"(两者通用)。
        # 首次运行无正式表时第一条会失败, try 跳过即可。
        try:
            DB.exec(f"ALTER TABLE {table} RENAME TO {table}_old", db)
        except Exception:
            pass
        DB.exec(f"ALTER TABLE {table}_tmp RENAME TO {table}", db)
        DB.exec(f"DROP TABLE IF EXISTS {table}_old", db)
        tsSHelper.setIndex(table,db)  