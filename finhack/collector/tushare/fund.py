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
        # 预处理数据，确保字段为字符串类型
        for col in data.columns:
            if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
               'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                data[col] = data[col].astype(str)
        mydb.safe_to_sql(data, table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
        data=pro.fund_basic(market='E',status='I')
        # 预处理数据，确保字段为字符串类型
        for col in data.columns:
            if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
               'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                data[col] = data[col].astype(str)
        mydb.safe_to_sql(data, table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
        data=pro.fund_basic(market='E',status='L')
        # 预处理数据，确保字段为字符串类型
        for col in data.columns:
            if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
               'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                data[col] = data[col].astype(str)
        mydb.safe_to_sql(data, table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
        data=pro.fund_basic(market='O',status='D')
        # 预处理数据，确保字段为字符串类型
        for col in data.columns:
            if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
               'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                data[col] = data[col].astype(str)
        mydb.safe_to_sql(data, table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
        data=pro.fund_basic(market='O',status='I')
        # 预处理数据，确保字段为字符串类型
        for col in data.columns:
            if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
               'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                data[col] = data[col].astype(str)
        mydb.safe_to_sql(data, table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
        data=pro.fund_basic(market='O',status='L')
        # 预处理数据，确保字段为字符串类型
        for col in data.columns:
            if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
               'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                data[col] = data[col].astype(str)
        mydb.safe_to_sql(data, table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
        
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
            mydb.exec('rename table '+table+' to '+table+'_old;',db);
            mydb.exec('rename table '+table+'_tmp to '+table+';',db);
            mydb.exec("drop table if exists "+table+'_old',db)
            table_to_use = table
        
        tsSHelper.setIndex(table_to_use,db)
    
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
                    # 预处理数据，确保字段为字符串类型
                    for col in df.columns:
                        if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                           'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                            df[col] = df[col].astype(str)
                    mydb.safe_to_sql(df, table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
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
            mydb.exec('rename table '+table+' to '+table+'_old;',db)
            mydb.exec('rename table '+table+'_tmp to '+table+';',db)
            mydb.exec("drop table if exists "+table+'_old',db)
            table_to_use = table
        
        tsSHelper.setIndex(table_to_use,db)
    
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
                        # 预处理数据，确保字段为字符串类型
                        for col in df.columns:
                            if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                               'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                                df[col] = df[col].astype(str)
                        mydb.safe_to_sql(df, table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
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
            mydb.exec('rename table '+table+' to '+table+'_old;',db)
            mydb.exec('rename table '+table+'_tmp to '+table+';',db)
            mydb.exec("drop table if exists "+table+'_old',db)
            table_to_use = table
        
        tsSHelper.setIndex(table_to_use,db)
    
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