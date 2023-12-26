import sys
import time
import traceback

from finhack.collector.tushare.helper import tsSHelper
from finhack.library.mydb import mydb
from finhack.library.alert import alert
from finhack.library.monitor import tsMonitor
import finhack.library.log as Log

class tsAStockBasic:
    @tsMonitor
    def stock_basic(pro,db):
        table='astock_basic'
        mydb.exec("drop table if exists "+table+"_tmp",db)
        engine=mydb.getDBEngine(db)
        data=pro.stock_basic(list_status='L', fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
        # tsSHelper.getDataAndReplace(pro,'stock_basic','astock_basic',db)
        data.to_sql('astock_basic_tmp', engine, index=False, if_exists='append', chunksize=5000)
        data=pro.stock_basic(list_status='D', fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
        data.to_sql('astock_basic_tmp', engine, index=False, if_exists='append', chunksize=5000)
        mydb.exec('rename table '+table+' to '+table+'_old;',db);
        mydb.exec('rename table '+table+'_tmp to '+table+';',db);
        mydb.exec("drop table if exists "+table+'_old',db)
        tsSHelper.setIndex(table,db)
      
    @tsMonitor  
    def trade_cal(pro,db):
        tsSHelper.getDataAndReplace(pro,'trade_cal','astock_trade_cal',db)

        
    @tsMonitor
    def namechange(pro,db):
        tsSHelper.getDataAndReplace(pro,'namechange','astock_namechange',db)

    
    @tsMonitor   
    def hs_const(pro,db):
        table='astock_hs_const'
        mydb.exec("drop table if exists "+table+"_tmp",db)
        engine=mydb.getDBEngine(db)
        data = pro.hs_const(hs_type='SH')
        data.to_sql('astock_hs_const_tmp', engine, index=False, if_exists='append', chunksize=5000)
        data = pro.hs_const(hs_type='SZ')
        data.to_sql('astock_hs_const_tmp', engine, index=False, if_exists='append', chunksize=5000)
        mydb.exec('rename table '+table+' to '+table+'_old;',db);
        mydb.exec('rename table '+table+'_tmp to '+table+';',db);
        mydb.exec("drop table if exists "+table+'_old',db)
        tsSHelper.setIndex(table,db)
       
    @tsMonitor 
    def stock_company(pro,db):
        table='astock_stock_company'
        mydb.exec("drop table if exists "+table+"_tmp",db)
        engine=mydb.getDBEngine(db)
        data = pro.stock_company(exchange='SZSE', fields='ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,main_business,business_scope')
        data.to_sql('astock_stock_company_tmp', engine, index=False, if_exists='append', chunksize=5000)
        data = pro.stock_company(exchange='SSE', fields='ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,main_business,business_scope')
        data.to_sql('astock_stock_company_tmp', engine, index=False, if_exists='append', chunksize=5000)
        mydb.exec('rename table '+table+' to '+table+'_old;',db);
        mydb.exec('rename table '+table+'_tmp to '+table+';',db);
        mydb.exec("drop table if exists "+table+'_old',db)
        tsSHelper.setIndex(table,db)
    
    @tsMonitor
    def stk_managers(pro,db):
        tsSHelper.getDataAndReplace(pro,'stk_managers','astock_stk_managers',db)



    @tsMonitor
    def stk_rewards(pro,db):
        table='astock_stk_rewards'
        mydb.exec("drop table if exists "+table+"_tmp",db)
        engine=mydb.getDBEngine(db)
        data=tsSHelper.getAllAStock(True,pro,db)
        stock_list=data['ts_code'].tolist()
        
        for i in range(0,len(stock_list),100):
            code_list=stock_list[i:i+100]
            try_times=0
            while True:
                try:
                    df = pro.stk_rewards(ts_code=','.join(code_list))
                    df.to_sql('astock_stk_rewards_tmp', engine, index=False, if_exists='append', chunksize=5000)
                    break
                except Exception as e:
                    if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning("stk_rewards:触发最多访问。\n"+str(e)) 
                        return
                    if "最多访问" in str(e):
                        Log.logger.warning("stk_rewards:触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    else:
                        if try_times<10:
                            try_times=try_times+1;
                            Log.logger.error("stk_rewards:函数异常，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:
                            info = traceback.format_exc()
                            alert.send('stk_rewards','函数异常',str(info))
                            Log.logger.error(info)
                            break
            
        mydb.exec('rename table '+table+' to '+table+'_old;',db);
        mydb.exec('rename table '+table+'_tmp to '+table+';',db);
        mydb.exec("drop table if exists "+table+'_old',db)
        tsSHelper.setIndex(table,db)
        
    @tsMonitor       
    def new_share(pro,db):
        tsSHelper.getDataAndReplace(pro,'new_share','astock_new_share',db)

    