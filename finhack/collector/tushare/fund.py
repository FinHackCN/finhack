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

class tsFund:
    @tsMonitor
    def fund_basic(pro, db):
        try:
            table='fund_basic'
            DB.exec("drop table if exists "+table+"_tmp", db)
            
            # 交易所场内基金
            data=pro.fund_basic(market='E', status='D')
            # 预处理数据，确保字段为字符串类型
            for col in data.columns:
                if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                   'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                    data[col] = data[col].astype(str)
            DB.safe_to_sql(data, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
            
            # 获取其他类型的基金
            fund_markets = [('E', 'I'), ('E', 'L'), ('O', 'D'), ('O', 'I'), ('O', 'L')]
            for market, status in fund_markets:
                data = pro.fund_basic(market=market, status=status)
                # 预处理数据，确保字段为字符串类型
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
            Log.logger.error(f"获取基金基本信息失败: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False
    
    @tsMonitor
    def fund_company(pro,db):
        tsSHelper.getDataAndReplace(pro,'fund_company','fund_company',db)
    
    @tsMonitor
    def fund_manager(pro, db):
        try:
            table='fund_manager'
            DB.exec("drop table if exists "+table+"_tmp", db)
            data=tsSHelper.getAllFund(db)
            fund_list=data['ts_code'].tolist()
            
            for i in range(0, len(fund_list), 100):
                code_list=fund_list[i:i+100]
                try_times=0
                while True:
                    try:
                        df = pro.fund_manager(ts_code=','.join(code_list))
                        # 预处理数据，确保字段为字符串类型
                        for col in df.columns:
                            if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                               'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                                df[col] = df[col].astype(str)
                        DB.safe_to_sql(df, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
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
                                try_times=try_times+1
                                Log.logger.error("fund_manager:函数异常，等待重试。\n"+str(e))
                                time.sleep(15)
                                continue
                            else:                        
                                info = traceback.format_exc()
                                alert.send('fund_manager','函数异常',str(info))
                                Log.logger.error(info)
                                break
            
            # 使用统一的replace_table方法替换表
            table_to_use = DB.replace_table(table, table+"_tmp", db)
            
            tsSHelper.setIndex(table_to_use, db)
            return True
        except Exception as e:
            Log.logger.error(f"获取基金经理信息失败: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False
    
    @tsMonitor
    def fund_share(pro, db):
        try:
            table='fund_share'
            DB.exec("drop table if exists "+table+"_tmp", db)
            data=tsSHelper.getAllFund(db)
            fund_list=data['ts_code'].tolist()
            
            for i in range(0, len(fund_list), 100):
                code_list=fund_list[i:i+100]
                for ts_code in code_list:
                    try_times=0
                    while True:
                        try:
                            df = pro.fund_share(ts_code=','.join(code_list))
                            # 预处理数据，确保字段为字符串类型
                            for col in df.columns:
                                if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                                   'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                                    df[col] = df[col].astype(str)
                            DB.safe_to_sql(df, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
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
                                    try_times=try_times+1
                                    Log.logger.error("fund_share:函数异常，等待重试。\n"+str(e))
                                    time.sleep(15)
                                    continue
                                else:                            
                                    info = traceback.format_exc()
                                    alert.send('fund_share','函数异常',str(info))
                                    Log.logger.error(info)
                                    break
            
            # 使用统一的replace_table方法替换表
            table_to_use = DB.replace_table(table, table+"_tmp", db)
            
            tsSHelper.setIndex(table_to_use, db)
            return True
        except Exception as e:
            Log.logger.error(f"获取基金份额信息失败: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False
    
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