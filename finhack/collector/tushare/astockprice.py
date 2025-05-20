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

class tsAStockPrice:
    def getPrice(pro,api,table,db):
        engine=DB.get_db_engine(db)
        lastdate=tsSHelper.getLastDateAndDelete(table=table,filed='trade_date',ts_code="",db=db)
        begin = datetime.datetime.strptime(lastdate, "%Y%m%d")
        end = datetime.datetime.now()
        i=0
        while i<(end - begin).days+1:
            day = begin + datetime.timedelta(days=i)
            day=day.strftime("%Y%m%d")
            f = getattr(pro, api)
            try_times=0
            while True:
                try:
                    df=f(trade_date=day)
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
            #print(table+'-'+str(len(df))+'-'+day)
            #res = df.to_sql(table, engine, index=False, if_exists='append', chunksize=5000)
            DB.safe_to_sql(df, table, engine, index=False, if_exists='append', chunksize=5000)
            i=i+1
     
    
    @tsMonitor
    def daily(pro,db):
        table='astock_price_daily'
        engine=DB.get_db_engine(db)
        last_date = tsSHelper.getLastDateAndDelete(table=table, filed='trade_date', ts_code='000001.SZ', db=db)
        start_date = datetime.datetime.strptime(last_date, '%Y%m%d').date() - datetime.timedelta(days=1)
        end_date = datetime.datetime.now().date()
        start_date_str = start_date.strftime('%Y%m%d')
        end_date_str = end_date.strftime('%Y%m%d')
        df = pro.daily(start_date=start_date_str, end_date=end_date_str)
        # 预处理数据，确保股票代码等字段为字符串类型
        for col in df.columns:
            if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
               'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                df[col] = df[col].astype(str)
        DB.safe_to_sql(df, table, db, index=False, if_exists='append', chunksize=5000)

    @tsMonitor
    def weekly(pro,db):
        tsAStockPrice.getPrice(pro,'weekly','astock_price_weekly',db)

    
    @tsMonitor
    def monthly(pro,db):
        tsAStockPrice.getPrice(pro,'monthly','astock_price_monthly',db)
    
    # @tsMonitor
    # def pro_bar(pro,db):
    #     tsStockPrice.getPrice(pro,'daily','astock_price_daily',db)
    
    @tsMonitor
    def adj_factor(pro,db):
        tsAStockPrice.getPrice(pro,'adj_factor','astock_price_adj_factor',db)
    
    @tsMonitor
    def suspend_d(pro,db):
        tsAStockPrice.getPrice(pro,'suspend_d','astock_price_suspend_d',db)
    
    @tsMonitor
    def daily_basic(pro,db):
        tsAStockPrice.getPrice(pro,'daily_basic','astock_price_daily_basic',db)
    
    @tsMonitor
    def moneyflow(pro,db):
        tsAStockPrice.getPrice(pro,'moneyflow','astock_price_moneyflow',db)
    
    @tsMonitor
    def stk_limit(pro,db):
        tsAStockPrice.getPrice(pro,'stk_limit','astock_price_stk_limit',db)
    
    @tsMonitor
    def limit_list(pro,db):
        tsAStockPrice.getPrice(pro,'limit_list','astock_price_limit_list',db)
    
    @tsMonitor
    def moneyflow_hsgt(pro,db):
        tsAStockPrice.getPrice(pro,'moneyflow_hsgt','astock_price_moneyflow_hsgt',db)
    
    @tsMonitor
    def hsgt_top10(pro,db):
        tsAStockPrice.getPrice(pro,'hsgt_top10','astock_price_hsgt_top10',db)
    
    @tsMonitor
    def ggt_top10(pro,db):
        tsAStockPrice.getPrice(pro,'ggt_top10','astock_price_ggt_top10',db)
    
    @tsMonitor
    def hk_hold(pro,db):
        tsAStockPrice.getPrice(pro,'hk_hold','astock_price_hk_hold',db)
    
    @tsMonitor
    def ggt_daily(pro,db):
        tsSHelper.getDataAndReplace(pro,'ggt_daily','astock_price_ggt_daily',db)
    
    @tsMonitor
    def ggt_monthly(pro,db):
        tsSHelper.getDataAndReplace(pro,'ggt_monthly','astock_price_ggt_monthly',db)
    
    @tsMonitor
    def ccass_hold_detail(pro,db):
        pass #积分不够
        #tsStockPrice.getPrice(pro,'ccass_hold_detail','astock_price_ccass_hold_detail',db)
    
 