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

class tsFuntures:
    @tsMonitor
    def fut_basic(pro,db):
        table='futures_basic'
        #DB.truncate_table(table,db)
        DB.exec("drop table if exists "+table+"_tmp",db)
        # 不需要获取engine对象，直接使用db连接名
        # engine = DB.get_db_engine(db)
        exchange_list=['CFFEX','DCE','CZCE','SHFE','NE']
        for e in exchange_list:
            data=pro.fut_basic(exchange=e)
            # 预处理数据，确保股票代码等字段为字符串类型
            for col in data.columns:
                if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                   'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                    data[col] = data[col].astype(str)
            #data.to_sql(table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
            DB.safe_to_sql(data, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
        
        # 使用通用的DB.replace_table方法
        # DB.replace_table 会处理不同数据库的表替换逻辑，并返回最终使用的表名
        table_to_use = DB.replace_table(target_table=table, source_table=f"{table}_tmp", connection=db)

        if table_to_use == table:
            Log.logger.info(f"成功将临时表 {table}_tmp 替换为/更新到 {table}.")
        elif table_to_use == f"{table}_tmp":
            Log.logger.warning(f"表替换失败，将使用临时表 {table_to_use} 作为最终表.")
        elif not table_to_use: 
            Log.logger.error(f"表替换操作 {table} <= {table}_tmp 失败，未返回有效表名。")
            table_to_use = f"{table}_tmp" # Fallback to temporary table
            Log.logger.warning(f"回退使用临时表: {table_to_use}")
        else: 
            Log.logger.warning(f"表替换操作 {table} <= {table}_tmp 完成，最终表名为 {table_to_use} (可能为备份表或非预期状态).")
        
        tsSHelper.setIndex(table_to_use,db)
            
    @tsMonitor    
    def trade_cal(pro,db):
        table='futures_trade_cal'
        #DB.truncate_table(table,db)
        DB.exec("drop table if exists "+table+"_tmp",db)
        # 不需要获取engine对象，直接使用db连接名
        # engine = DB.get_db_engine(db)
        exchange_list=['CFFEX','DCE','CZCE','SHFE','NE']
        for e in exchange_list:
            data=pro.trade_cal(exchange=e)
            # 预处理数据，确保日期字段为字符串类型
            for col in data.columns:
                if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date', 'cal_date'] or \
                   'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                    data[col] = data[col].astype(str)
            #data.to_sql(table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
            DB.safe_to_sql(data, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
        
        # 使用通用的DB.replace_table方法
        # DB.replace_table 会处理不同数据库的表替换逻辑，并返回最终使用的表名
        table_to_use = DB.replace_table(target_table=table, source_table=f"{table}_tmp", connection=db)

        if table_to_use == table:
            Log.logger.info(f"成功将临时表 {table}_tmp 替换为/更新到 {table}.")
        elif table_to_use == f"{table}_tmp":
            Log.logger.warning(f"表替换失败，将使用临时表 {table_to_use} 作为最终表.")
        elif not table_to_use: 
            Log.logger.error(f"表替换操作 {table} <= {table}_tmp 失败，未返回有效表名。")
            table_to_use = f"{table}_tmp" # Fallback to temporary table
            Log.logger.warning(f"回退使用临时表: {table_to_use}")
        else: 
            Log.logger.warning(f"表替换操作 {table} <= {table}_tmp 完成，最终表名为 {table_to_use} (可能为备份表或非预期状态).")
        
        tsSHelper.setIndex(table_to_use,db)
            

    @tsMonitor    
    def fut_daily(pro,db):
        table='futures_daily'
        tsSHelper.getDataWithLastDate(pro,'fut_daily','futures_daily',db)
        
    @tsMonitor 
    def fut_holding(pro,db):
        table='futures_holding'
        tsSHelper.getDataWithLastDate(pro,'fut_holding','futures_holding',db)
        
