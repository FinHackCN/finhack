import sys
import time
import traceback

from finhack.collector.tushare.helper import tsSHelper
from finhack.library.db import DB
from finhack.library.alert import alert
from finhack.library.monitor import tsMonitor
import finhack.library.log as Log

class tsAStockBasic:
    @tsMonitor
    def stock_basic(pro,db):
        table='astock_basic'
        try:
            # 使用正确的参数顺序：SQL语句,连接名
            DB.exec("drop table if exists "+table+"_tmp", db)
            # 不需要获取engine对象，直接使用db连接名
            # engine = DB.get_db_engine(db)
            data=pro.stock_basic(list_status='L', fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
            # 预处理数据，确保股票代码等字段为字符串类型
            for col in data.columns:
                if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                    data[col] = data[col].astype(str)
            # 使用正确的参数顺序：DataFrame,表名,连接名,...
            DB.safe_to_sql(data, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
            data=pro.stock_basic(list_status='D', fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
            # 预处理数据，确保股票代码等字段为字符串类型
            for col in data.columns:
                if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                    data[col] = data[col].astype(str)
            DB.safe_to_sql(data, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
            
            # 使用统一的replace_table方法替换表
            table_to_use = DB.replace_table(table, table+"_tmp", db)
            
            tsSHelper.setIndex(table_to_use, db)
            return True
        except Exception as e:
            Log.logger.error(f"获取股票基本信息失败: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False
      
    @tsMonitor  
    def trade_cal(pro,db):
        try:
            return tsSHelper.getDataAndReplace(pro,'trade_cal','astock_trade_cal',db)
        except Exception as e:
            Log.logger.error(f"获取交易日历失败: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False

        
    @tsMonitor
    def namechange(pro,db):
        try:
            return tsSHelper.getDataAndReplace(pro,'namechange','astock_namechange',db)
        except Exception as e:
            Log.logger.error(f"获取股票名称变更失败: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False

    
    @tsMonitor   
    def hs_const(pro,db):
        table='astock_hs_const'
        try:
            DB.exec("drop table if exists "+table+"_tmp", db)
            # 不需要获取engine对象，直接使用db连接名
            # engine = DB.get_db_engine(db)
            data = pro.hs_const(hs_type='SH')
            # 预处理数据，确保股票代码等字段为字符串类型
            for col in data.columns:
                if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                    data[col] = data[col].astype(str)
            DB.safe_to_sql(data, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
            data = pro.hs_const(hs_type='SZ')
            # 预处理数据，确保股票代码等字段为字符串类型
            for col in data.columns:
                if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                    data[col] = data[col].astype(str)
            DB.safe_to_sql(data, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
            
            # 使用统一的replace_table方法替换表
            table_to_use = DB.replace_table(table, table+"_tmp", db)
            
            tsSHelper.setIndex(table_to_use, db)
            return True
        except Exception as e:
            Log.logger.error(f"获取沪深港通成分股失败: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False
       
    @tsMonitor 
    def stock_company(pro,db):
        table='astock_stock_company'
        DB.exec("drop table if exists "+table+"_tmp",db)
        # 不需要获取engine对象，直接使用db连接名
        # engine = DB.get_db_engine(db)
        data = pro.stock_company(exchange='SZSE', fields='ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,main_business,business_scope')
        # 预处理数据，确保股票代码等字段为字符串类型
        for col in data.columns:
            if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
               'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                data[col] = data[col].astype(str)
        DB.safe_to_sql(data, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
        data = pro.stock_company(exchange='SSE', fields='ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,main_business,business_scope')
        # 预处理数据，确保股票代码等字段为字符串类型
        for col in data.columns:
            if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
               'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                data[col] = data[col].astype(str)
        DB.safe_to_sql(data, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
        
        # 使用统一的replace_table方法替换表
        table_to_use = DB.replace_table(table, table+"_tmp", db)
        
        tsSHelper.setIndex(table_to_use,db)
    
    @tsMonitor
    def stk_managers(pro,db):
        tsSHelper.getDataAndReplace(pro,'stk_managers','astock_stk_managers',db)



    @tsMonitor
    def stk_rewards(pro,db):
        table='astock_stk_rewards'
        DB.exec("drop table if exists "+table+"_tmp",db)
        # 不需要获取engine对象，直接使用db连接名
        # engine = DB.get_db_engine(db)
        data=tsSHelper.getAllAStock(True,pro,db)
        stock_list=data['ts_code'].tolist()
        
        for i in range(0,len(stock_list),100):
            code_list=stock_list[i:i+100]
            try_times=0
            while True:
                try:
                    df = pro.stk_rewards(ts_code=','.join(code_list))
                    # 预处理数据，确保股票代码等字段为字符串类型
                    for col in df.columns:
                        if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                           'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                            df[col] = df[col].astype(str)
                    DB.safe_to_sql(df, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
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
                            try_times=try_times+1
                            Log.logger.warning("stk_rewards:函数异常，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:
                            info = traceback.format_exc()
                            alert.send("stk_rewards", '函数异常', str(info))
                            Log.logger.error(str(info))
                            return
        
        # 使用统一的replace_table方法替换表
        table_to_use = DB.replace_table(table, table+"_tmp", db)
        
        tsSHelper.setIndex(table_to_use,db)
        
    @tsMonitor       
    def new_share(pro,db):
        tsSHelper.getDataAndReplace(pro,'new_share','astock_new_share',db)

    