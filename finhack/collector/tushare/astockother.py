import sys
import time
import datetime
import traceback
import pandas as pd

from finhack.library.mydb import mydb
from finhack.library.alert import alert
from finhack.library.monitor import tsMonitor
from finhack.collector.tushare.helper import tsSHelper
from finhack.collector.tushare.astockprice import tsAStockPrice
import finhack.library.log as Log

class tsAStockOther:
    @tsMonitor
    def margin(pro,db):
        tsSHelper.getDataWithLastDate(pro,'margin','astock_market_margin',db)
    
    @tsMonitor
    def margin_detail(pro,db):
        tsSHelper.getDataWithLastDate(pro,'margin_detail','astock_market_margin_detail',db)
    
    @tsMonitor
    def report_rc(pro,db):
        engine=mydb.getDBEngine(db)
        if True:
            try_times=0
            while True:
                try:
                    today = datetime.datetime.now()
                    today=today.strftime("%Y%m%d")
                    lastdate=tsSHelper.getLastDateAndDelete('astock_other_report_rc','report_date',ts_code='',db=db)
                    df =pro.report_rc(start_date=lastdate, end_date=today)
                    df.to_sql('astock_other_report_rc', engine, index=False, if_exists='append', chunksize=5000)
                    break
                except Exception as e:
                    if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning("report_rc:触发最多访问。\n"+str(e))
                        return
                    elif "每分钟最多访问" in str(e):
                        Log.logger.warning("report_rc:触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    else:
                        if try_times<10:
                            try_times=try_times+1;
                            Log.logger.error("report_rc:函数异常，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:
                            info = traceback.format_exc()
                            alert.send('report_rc','函数异常',str(info))
                            Log.logger.error(info)  
                            return
                        
 
    @tsMonitor
    def cyq_perf(pro,db):
        tsAStockPrice.getPrice(pro,'cyq_perf','astock_other_cyq_perf',db)
        # engine=mydb.getDBEngine(db)
        # if True:
        #     try_times=0
        #     while True:
        #         try:
        #             today = datetime.datetime.now()
        #             today=today.strftime("%Y%m%d")
        #             lastdate=tsSHelper.getLastDateAndDelete('astock_other_cyq_perf','trade_date',ts_code='',db=db)
        #             df =pro.cyq_perf(start_date=lastdate, end_date=today)
        #             df.to_sql('astock_other_cyq_perf', engine, index=False, if_exists='append', chunksize=5000)
        #             break
        #         except Exception as e:
        #             if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
        #                 print("cyq_perf:触发最多访问。\n"+str(e))
        #                 return
        #             elif "每分钟最多访问" in str(e):
        #                 print("cyq_perf:触发限流，等待重试。\n"+str(e))
        #                 time.sleep(15)
        #                 continue
        #             else:
        #                 if try_times<10:
        #                     try_times=try_times+1;
        #                     print("cyq_perf:函数异常，等待重试。\n"+str(e))
        #                     time.sleep(15)
        #                     continue
        #                 else:                    
        #                     info = traceback.format_exc()
        #                     alert.send('cyq_perf','函数异常',str(info))
        #                     print(info)   
        #                     return
 

    @tsMonitor
    def cyq_chips(pro,db):
        pass
    #感觉tushare的这个接口好像有问题
        table='astock_other_cyq_chips'
        mydb.exec("drop table if exists "+table+"_tmp",db)
        engine=mydb.getDBEngine(db)
        data=tsSHelper.getAllAStock(True,pro,db)
        stock_list=data['ts_code'].tolist()
        
        for ts_code in stock_list:
            try_times=0
            while True:
                try:
                    df = pro.cyq_chips(ts_code=ts_code)
                    df.to_sql('astock_other_cyq_chips_tmp', engine, index=False, if_exists='append', chunksize=5000)
                    break
                except Exception as e:
                    if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning("cyq_chips:触发最多访问。\n"+str(e)) 
                        return
                    if "最多访问" in str(e):
                        Log.logger.warning("cyq_chips:触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    else:
                        if try_times<10:
                            try_times=try_times+1;
                            Log.logger.error("cyq_chips:函数异常，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:
                            info = traceback.format_exc()
                            alert.send('cyq_chips','函数异常',str(info))
                            Log.logger.error(info)
                            break
            
        mydb.exec('rename table '+table+' to '+table+'_old;',db);
        mydb.exec('rename table '+table+'_tmp to '+table+';',db);
        mydb.exec("drop table if exists "+table+'_old',db)
        tsSHelper.setIndex(table,db)        
        
        # engine=mydb.getDBEngine(db)
        # if True:
        #     try_times=0
        #     while True:
        #         try:
        #             today = datetime.datetime.now()
        #             today=today.strftime("%Y%m%d")
        #             lastdate=tsSHelper.getLastDateAndDelete('astock_other_cyq_chips','trade_date',ts_code='',db=db)
        #             df =pro.cyq_chips(start_date=lastdate, end_date=today)
        #             df.to_sql('astock_other_cyq_chips', engine, index=False, if_exists='append', chunksize=5000)
        #             break
        #         except Exception as e:
        #             if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
        #                 print("cyq_chips:触发最多访问。\n"+str(e))
        #                 return
        #             elif "每分钟最多访问" in str(e):
        #                 print("cyq_chips:触发限流，等待重试。\n"+str(e))
        #                 time.sleep(15)
        #                 continue
        #             else:
        #                 if try_times<10:
        #                     try_times=try_times+1;
        #                     print("cyq_chips:函数异常，等待重试。\n"+str(e))
        #                     time.sleep(15)
        #                     continue
        #                 else:                    
        #                     info = traceback.format_exc()
        #                     alert.send('cyq_chips','函数异常',str(info))
        #                     print(info)     
        #                     return
    
    
    
    
    #broker_recommend