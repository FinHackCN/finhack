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
        
