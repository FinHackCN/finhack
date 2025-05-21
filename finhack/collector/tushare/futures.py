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
        
        # 获取数据库适配器类型
        from finhack.library.db import DB
        adapter = DB.get_adapter(db)
        adapter_type = adapter.__class__.__name__
        
        if adapter_type == 'DuckDBAdapter':
            # DuckDB处理方式：尝试删除原表并重命名临时表
            try:
                # 检查表是否存在
                result = DB.select_to_list(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'", db)
                if result:
                    # 表存在，先尝试删除原表
                    try:
                        # 使用 CASCADE 选项删除表及其依赖
                        DB.exec(f"DROP TABLE IF EXISTS {table} CASCADE", db)
                        Log.logger.info(f"成功删除原表 {table}")
                    except Exception as e:
                        Log.logger.error(f"删除原表失败: {str(e)}")
                        # 尝试使用其他方式处理依赖关系
                        try:
                            # 查找依赖关系
                            deps_query = f"SELECT * FROM duckdb_dependencies() WHERE dependency_name = '{table}'"
                            deps = DB.select_to_list(deps_query, db)
                            if deps:
                                Log.logger.info(f"表 {table} 存在依赖关系，尝试处理")
                                # 可以在这里添加处理依赖的代码
                            
                            # 再次尝试删除
                            DB.exec(f"DROP TABLE IF EXISTS {table}", db)
                        except Exception as inner_e:
                            Log.logger.error(f"处理依赖关系失败: {str(inner_e)}")
                            # 如果无法删除，备份原表
                            DB.exec(f"ALTER TABLE {table} RENAME TO {table}_backup", db)
                            Log.logger.info(f"已将原表重命名为 {table}_backup")
                
                # 重命名临时表
                DB.exec(f"ALTER TABLE {table}_tmp RENAME TO {table}", db)
                table_to_use = table
                Log.logger.info(f"成功将临时表重命名为 {table}")
            except Exception as e:
                Log.logger.error(f"DuckDB表替换失败: {str(e)}")
                # 如果所有尝试都失败，使用临时表
                table_to_use = f"{table}_tmp"
                Log.logger.warning(f"无法完成表替换，将使用临时表 {table_to_use} 作为最终表")
        else:
            # MySQL重命名语法
            DB.exec('rename table '+table+' to '+table+'_old;',db);
            DB.exec('rename table '+table+'_tmp to '+table+';',db);
            DB.exec("drop table if exists "+table+'_old',db)
            table_to_use = table
        
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
        
        # 获取数据库适配器类型
        from finhack.library.db import DB
        adapter = DB.get_adapter(db)
        adapter_type = adapter.__class__.__name__
        
        if adapter_type == 'DuckDBAdapter':
            # DuckDB处理方式：尝试删除原表并重命名临时表
            try:
                # 检查表是否存在
                result = DB.select_to_list(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'", db)
                if result:
                    # 表存在，先尝试删除原表
                    try:
                        # 使用 CASCADE 选项删除表及其依赖
                        DB.exec(f"DROP TABLE IF EXISTS {table} CASCADE", db)
                        Log.logger.info(f"成功删除原表 {table}")
                    except Exception as e:
                        Log.logger.error(f"删除原表失败: {str(e)}")
                        # 尝试使用其他方式处理依赖关系
                        try:
                            # 查找依赖关系
                            deps_query = f"SELECT * FROM duckdb_dependencies() WHERE dependency_name = '{table}'"
                            deps = DB.select_to_list(deps_query, db)
                            if deps:
                                Log.logger.info(f"表 {table} 存在依赖关系，尝试处理")
                                # 可以在这里添加处理依赖的代码
                            
                            # 再次尝试删除
                            DB.exec(f"DROP TABLE IF EXISTS {table}", db)
                        except Exception as inner_e:
                            Log.logger.error(f"处理依赖关系失败: {str(inner_e)}")
                            # 如果无法删除，备份原表
                            DB.exec(f"ALTER TABLE {table} RENAME TO {table}_backup", db)
                            Log.logger.info(f"已将原表重命名为 {table}_backup")
                
                # 重命名临时表
                DB.exec(f"ALTER TABLE {table}_tmp RENAME TO {table}", db)
                table_to_use = table
                Log.logger.info(f"成功将临时表重命名为 {table}")
            except Exception as e:
                Log.logger.error(f"DuckDB表替换失败: {str(e)}")
                # 如果所有尝试都失败，使用临时表
                table_to_use = f"{table}_tmp"
                Log.logger.warning(f"无法完成表替换，将使用临时表 {table_to_use} 作为最终表")
        else:
            # MySQL重命名语法
            DB.exec('rename table '+table+' to '+table+'_old;',db);
            DB.exec('rename table '+table+'_tmp to '+table+';',db);
            DB.exec("drop table if exists "+table+'_old',db)
            table_to_use = table
        
        tsSHelper.setIndex(table_to_use,db)
            

    @tsMonitor    
    def fut_daily(pro,db):
        table='futures_daily'
        tsSHelper.getDataWithLastDate(pro,'fut_daily','futures_daily',db)
        
    @tsMonitor 
    def fut_holding(pro,db):
        table='futures_holding'
        tsSHelper.getDataWithLastDate(pro,'fut_holding','futures_holding',db)
        
