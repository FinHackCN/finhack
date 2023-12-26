import sys
import time
import datetime
import traceback
import pandas as pd

from finhack.library.mydb import mydb
from finhack.library.alert import alert
from finhack.library.monitor import tsMonitor
from finhack.collector.tushare.helper import tsSHelper
import finhack.library.log as Log

class tsFund:
    @tsMonitor
    def fund_basic(pro,db):
        table='fund_basic'
        #mydb.truncateTable(table,db)
        mydb.exec("drop table if exists "+table+"_tmp",db)
        engine=mydb.getDBEngine(db)
        data=pro.fund_basic(market='E',status='D')
        data.to_sql(table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
        data=pro.fund_basic(market='E',status='I')
        data.to_sql(table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
        data=pro.fund_basic(market='E',status='L')
        data.to_sql(table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
        data=pro.fund_basic(market='O',status='D')
        data.to_sql(table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
        data=pro.fund_basic(market='O',status='I')
        data.to_sql(table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
        data=pro.fund_basic(market='O',status='L')
        data.to_sql(table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
        mydb.exec('rename table '+table+' to '+table+'_old;',db);
        mydb.exec('rename table '+table+'_tmp to '+table+';',db);
        mydb.exec("drop table if exists "+table+'_old',db)
        tsSHelper.setIndex(table,db)
    
    @tsMonitor
    def fund_company(pro,db):
        tsSHelper.getDataAndReplace(pro,'fund_company','fund_company',db)
    
    @tsMonitor
    def fund_manager(pro,db):
        table='fund_manager'
        mydb.exec("drop table if exists "+table+"_tmp",db)
        engine=mydb.getDBEngine(db)
        data=tsSHelper.getAllFund(db)
        fund_list=data['ts_code'].tolist()
        
        for i in range(0,len(fund_list),100):
            code_list=fund_list[i:i+100]
            try_times=0
            while True:
                try:
                    df = pro.fund_manager(ts_code=','.join(code_list))
                    df.to_sql('fund_manager_tmp', engine, index=False, if_exists='append', chunksize=5000)
                    break
                except Exception as e:
                    if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning("fund_manager:触发最多访问。\n"+str(e)) 
                        return
                    if "最多访问" in str(e):
                        Log.logger.warning("fund_manager:触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    else:
                        if try_times<10:
                            try_times=try_times+1;
                            Log.logger.error("fund_manager:函数异常，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:                        
                            info = traceback.format_exc()
                            alert.send('fund_manager','函数异常',str(info))
                            Log.logger.error(info)
                            break
        mydb.exec('rename table '+table+' to '+table+'_old;',db)
        mydb.exec('rename table '+table+'_tmp to '+table+';',db)
        mydb.exec("drop table if exists "+table+'_old',db)
        tsSHelper.setIndex(table,db)
    
    @tsMonitor
    def fund_share(pro,db):
        table='fund_share'
        mydb.exec("drop table if exists "+table+"_tmp",db)
        data=tsSHelper.getAllFund(db)
        fund_list=data['ts_code'].tolist()
        engine=mydb.getDBEngine(db)
        for i in range(0,len(fund_list),100):
            code_list=fund_list[i:i+100]
            for ts_code in code_list:
                try_times=0
                while True:
                    try:
                        df = pro.fund_share(ts_code=','.join(code_list))
                        df.to_sql('fund_share_tmp', engine, index=False, if_exists='append', chunksize=5000)
                        break
                    except Exception as e:
                        if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                            Log.logger.warning("fund_share:触发最多访问。\n"+str(e)) 
                            return
                        if "最多访问" in str(e):
                            Log.logger.warning("fund_share:触发限流，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:
                            if try_times<10:
                                try_times=try_times+1;
                                Log.logger.error("fund_share:函数异常，等待重试。\n"+str(e))
                                time.sleep(15)
                                continue
                            else:                            
                                info = traceback.format_exc()
                                alert.send('fund_share','函数异常',str(info))
                                Log.logger.error(info)
                                break


        #tsSHelper.getDataWithLastDate(pro,'fund_nav','fund_nav',db,'nav_date')
        mydb.exec('rename table '+table+' to '+table+'_old;',db)
        mydb.exec('rename table '+table+'_tmp to '+table+';',db)
        mydb.exec("drop table if exists "+table+'_old',db)
        tsSHelper.setIndex(table,db)
    
    @tsMonitor
    def fund_nav(pro,db):
        tsSHelper.getDataWithLastDate(pro,'fund_nav','fund_nav',db,'nav_date')
    
    @tsMonitor
    def fund_div(pro,db):
        tsSHelper.getDataWithLastDate(pro,'fund_div','fund_div',db,'ann_date')
    
    @tsMonitor
    def fund_portfolio(pro,db):
        tsSHelper.getDataWithLastDate(pro,'fund_portfolio','fund_portfolio',db,'ann_date')
    
    @tsMonitor
    def fund_daily(pro,db):
        tsSHelper.getDataWithLastDate(pro,'fund_daily','fund_daily',db)
    
    @tsMonitor
    def fund_adj(pro,db):
        tsSHelper.getDataWithLastDate(pro,'fund_adj','fund_adj',db)