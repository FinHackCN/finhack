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
            
        DB.exec('rename table '+table+' to '+table+'_old;',db);
        DB.exec('rename table '+table+'_tmp to '+table+';',db);
        DB.exec("drop table if exists "+table+'_old',db)
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
            
        DB.exec('rename table '+table+' to '+table+'_old;',db);
        DB.exec('rename table '+table+'_tmp to '+table+';',db);
        DB.exec("drop table if exists "+table+'_old',db)
        tsSHelper.setIndex(table,db)  