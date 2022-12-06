import sys
import datetime
from library.config import config
from library.mydb import mydb
from collect.ts.helper import tsSHelper
from library.monitor import tsMonitor
import time
import pandas as pd
import traceback
from library.alert import alert

class tsFuntures:
    @tsMonitor
    def fut_basic(pro,db):
        table='futures_basic'
        #mydb.truncateTable(table,db)
        mydb.exec("drop table if exists "+table+"_tmp",db)
        engine=mydb.getDBEngine(db)
        exchange_list=['CFFEX','DCE','CZCE','SHFE','NE']
        for e in exchange_list:
            data=pro.fut_basic(exchange=e)
            data.to_sql(table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
        mydb.exec('rename table '+table+' to '+table+'_old;',db);
        mydb.exec('rename table '+table+'_tmp to '+table+';',db);
        mydb.exec("drop table if exists "+table+'_old',db)
        tsSHelper.setIndex(table,db)
            
    @tsMonitor    
    def trade_cal(pro,db):
        table='futures_trade_cal'
        #mydb.truncateTable(table,db)
        mydb.exec("drop table if exists "+table+"_tmp",db)
        engine=mydb.getDBEngine(db)
        exchange_list=['CFFEX','DCE','CZCE','SHFE','NE']
        for e in exchange_list:
            data=pro.trade_cal(exchange=e)
            data.to_sql(table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
        mydb.exec('rename table '+table+' to '+table+'_old;',db);
        mydb.exec('rename table '+table+'_tmp to '+table+';',db);
        mydb.exec("drop table if exists "+table+'_old',db)
        tsSHelper.setIndex(table,db)
            

    @tsMonitor    
    def fut_daily(pro,db):
        table='futures_daily'
        tsSHelper.getDataWithLastDate(pro,'fut_daily','futures_daily',db)
        
    @tsMonitor 
    def fut_holding(pro,db):
        table='futures_holding'
        tsSHelper.getDataWithLastDate(pro,'fut_holding','futures_holding',db)
        
