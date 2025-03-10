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
        # 预处理数据，确保股票代码等字段为字符串类型
        for col in data.columns:
            if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
               'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                data[col] = data[col].astype(str)
        mydb.safe_to_sql(data, table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
        data=pro.stock_basic(list_status='D', fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
        # 预处理数据，确保股票代码等字段为字符串类型
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
        # 预处理数据，确保股票代码等字段为字符串类型
        for col in data.columns:
            if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
               'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                data[col] = data[col].astype(str)
        mydb.safe_to_sql(data, table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
        data = pro.hs_const(hs_type='SZ')
        # 预处理数据，确保股票代码等字段为字符串类型
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
    def stock_company(pro,db):
        table='astock_stock_company'
        mydb.exec("drop table if exists "+table+"_tmp",db)
        engine=mydb.getDBEngine(db)
        data = pro.stock_company(exchange='SZSE', fields='ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,main_business,business_scope')
        # 预处理数据，确保股票代码等字段为字符串类型
        for col in data.columns:
            if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
               'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                data[col] = data[col].astype(str)
        mydb.safe_to_sql(data, table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
        data = pro.stock_company(exchange='SSE', fields='ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,main_business,business_scope')
        # 预处理数据，确保股票代码等字段为字符串类型
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
            # DuckDB处理方式：对于已存在的表，始终使用临时表，不尝试重命名（避免依赖关系错误）
            # 检查表是否存在
            result = DB.select_to_list(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'", db)
            if result:
                # 如果表存在，直接使用临时表，不尝试重命名
                Log.logger.info(f"表 {table} 已存在，将使用临时表 {table}_tmp 作为最终表")
                table_to_use = f"{table}_tmp"
            else:
                # 如果表不存在，尝试重命名临时表
                try:
                    DB.exec(f"ALTER TABLE {table}_tmp RENAME TO {table}", db)
                    table_to_use = table
                    Log.logger.info(f"成功将临时表重命名为 {table}")
                except Exception as e:
                    Log.logger.error(f"DuckDB重命名表失败: {str(e)}")
                    # 如果重命名失败，使用临时表
                    table_to_use = f"{table}_tmp"
                    Log.logger.warning(f"将使用临时表 {table}_to_use 作为最终表")
        else:
            # MySQL重命名语法
            mydb.exec('rename table '+table+' to '+table+'_old;',db);
            mydb.exec('rename table '+table+'_tmp to '+table+';',db);
            mydb.exec("drop table if exists "+table+'_old',db)
            table_to_use = table
        
        tsSHelper.setIndex(table_to_use,db)
    
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
                    # 预处理数据，确保股票代码等字段为字符串类型
                    for col in df.columns:
                        if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                           'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                            df[col] = df[col].astype(str)
                    mydb.safe_to_sql(df, table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
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
    def new_share(pro,db):
        tsSHelper.getDataAndReplace(pro,'new_share','astock_new_share',db)

    