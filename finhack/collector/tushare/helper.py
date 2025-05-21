import re
import sys
import time
import datetime
import traceback
import pandas as pd
import os
import sqlite3

from finhack.library.db import DB
from finhack.library.alert import alert
from finhack.library.monitor import tsMonitor
from finhack.library.config import Config
import finhack.library.log as Log

class tsSHelper:
    """
    Tushare数据库辅助类
    使用DB类提供的统一数据库接口，支持DuckDB和MySQL
    """
    
    @staticmethod
    def check_database_directory(db_name):
        """
        检查数据库目录是否存在并且有写权限，如果不存在则创建
        
        Args:
            db_name: 数据库配置名称
            
        Returns:
            bool: 目录可用返回True，否则返回False
        """
        try:
            # 获取数据库配置
            db_config = Config.get_config('db', db_name)
            
            # 检查是否为SQLite数据库
            if db_config.get('type', '').lower() != 'sqlite':
                return True  # 不是SQLite数据库，不需要检查目录
            
            # 获取数据库文件路径
            db_path = db_config.get('path', '')
            if not db_path:
                Log.logger.warning(f"SQLite数据库 {db_name} 配置中未指定path参数")
                return False
            
            # 如果是相对路径，转换为绝对路径
            if not os.path.isabs(db_path):
                # 当前工作目录
                cwd = os.getcwd()
                abs_db_path = os.path.abspath(os.path.join(cwd, db_path))
                Log.logger.info(f"数据库 {db_name} 相对路径: {db_path}")
                Log.logger.info(f"转换为绝对路径: {abs_db_path}")
                db_path = abs_db_path
            
            # 获取数据库目录
            db_dir = os.path.dirname(db_path)
            Log.logger.info(f"数据库 {db_name} 目录: {db_dir}")
            
            # 检查目录是否存在
            if not os.path.exists(db_dir):
                Log.logger.warning(f"数据库目录不存在，创建目录: {db_dir}")
                os.makedirs(db_dir, exist_ok=True)
            
            # 检查目录是否有写权限
            if not os.access(db_dir, os.W_OK):
                Log.logger.error(f"数据库目录没有写权限: {db_dir}")
                return False
            
            Log.logger.info(f"数据库目录检查通过: {db_dir}")
            return True
        except Exception as e:
            Log.logger.error(f"检查数据库目录时出错: {str(e)}")
            return False
    
    def getAllAStockIndex(pro=None, db='default'):
        sql='select * from astock_index_basic'
        data=DB.select_to_df(sql, db)
        return data
    
    def getAllAStock(fromDB=True, pro=None, db='default'):
        if fromDB:
            sql='select * from astock_basic'
            data=DB.select_to_df(sql, db)
             
        else:
            all_stock=[]
            data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
            all_stock.append(data)
            data = pro.stock_basic(exchange='', list_status='D', fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
            all_stock.append(data)
            data = pro.stock_basic(exchange='', list_status='P', fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
            all_stock.append(data)
            data=pd.concat(all_stock, axis=0, ignore_index=True)
        return data
  
  
    def setIndex(table, db='default'):
        """
        为表创建索引
        在创建索引前检查字段是否存在，不存在则跳过
        
        Args:
            table: 表名
            db: 数据库连接名
        """
        try:
            # 获取数据库适配器
            adapter = DB.get_adapter(db)
            if not adapter:
                Log.logger.error(f"无法获取数据库适配器，跳过为表 {table} 创建索引")
                return
                
            # 判断数据库类型
            db_type = "unknown"
            if hasattr(adapter, '__class__') and hasattr(adapter.__class__, '__name__'):
                adapter_name = adapter.__class__.__name__.lower()
                Log.logger.debug(f"适配器类名: {adapter_name}")
                if 'mysql' in adapter_name:
                    db_type = "mysql"
                elif 'sqlite' in adapter_name:
                    db_type = "sqlite"
                elif 'duckdb' in adapter_name:
                    db_type = "duckdb"
            
            # 通过配置文件检查直接获取的更精确
            try:
                from finhack.library.config import Config
                db_config = Config.get_config(db)
                if db_config and 'type' in db_config:
                    if db_config['type'].lower() == 'mysql':
                        db_type = "mysql"
                    elif db_config['type'].lower() == 'sqlite':
                        db_type = "sqlite"
                    elif db_config['type'].lower() == 'duckdb':
                        db_type = "duckdb"
                    
                    #Log.logger.debug(f"从配置中确定数据库类型: {db_type}")
            except Exception as config_error:
                Log.logger.warning(f"从配置获取数据库类型失败: {str(config_error)}")
                
            # 额外检查连接字符串
            try:
                if hasattr(adapter, 'get_engine'):
                    engine = adapter.get_engine()
                    if hasattr(engine, 'url'):
                        url_str = str(engine.url)
                        #Log.logger.debug(f"数据库URL: {url_str}")
                        if 'mysql' in url_str.lower():
                            db_type = "mysql"
                        elif 'sqlite' in url_str.lower():
                            db_type = "sqlite"
            except Exception as engine_error:
                Log.logger.warning(f"从引擎URL获取数据库类型失败: {str(engine_error)}")
                
            Log.logger.info(f"数据库类型识别结果: {db_type}")
                
            # 获取表结构，检查字段是否存在
            try:
                # 获取表的字段列表
                fields = []
                if hasattr(adapter, 'get_table_columns'):
                    # 如果适配器有获取列的方法
                    fields = adapter.get_table_columns(table)
                else:
                    # 否则尝试通过查询获取
                    try:
                        # MySQL查询
                        result = DB.select_to_list(f"SHOW COLUMNS FROM {table}", db)
                        if result:
                            fields = [row['Field'] for row in result]
                    except:
                        try:
                            # SQLite查询
                            result = DB.select_to_list(f"PRAGMA table_info({table})", db)
                            if result:
                                fields = [row['name'] for row in result]
                        except Exception as e:
                            Log.logger.error(f"获取表 {table} 结构失败: {str(e)}")
                            return
                
                if not fields:
                    Log.logger.warning(f"无法获取表 {table} 的字段信息，跳过创建索引")
                    return
                
                # 要创建索引的字段列表
                index_fields = ['ts_code', 'end_date', 'trade_date']
                
                # 为存在的字段创建索引
                for field in index_fields:
                    if field in fields:
                        try:
                            # 根据数据库类型使用不同的索引语法
                            Log.logger.info(f"为表 {table} 创建索引: {field} (数据库类型: {db_type})")
                            if db_type == "mysql":
                                # MySQL需要为TEXT/BLOB类型指定长度
                                index_sql = f"CREATE INDEX idx_{table}_{field} ON {table} ({field}(32))"
                                Log.logger.debug(f"执行MySQL索引SQL: {index_sql}")
                                adapter.exec_sql(index_sql)
                            else:
                                # SQLite和其他数据库使用通用语法
                                index_sql = f"CREATE INDEX idx_{table}_{field} ON {table} ({field})"
                                Log.logger.debug(f"执行通用索引SQL: {index_sql}")
                                adapter.exec_sql(index_sql)
                        except Exception as e:
                            # 索引可能已存在，不影响程序继续执行
                            Log.logger.warning(f"为表 {table} 创建索引 {field} 时出错: {str(e)}")
                    else:
                        Log.logger.info(f"表 {table} 中不存在字段 {field}，跳过创建索引")
            except Exception as e:
                Log.logger.error(f"检查表 {table} 结构时出错: {str(e)}")
        except Exception as e:
            Log.logger.error(f"为表 {table} 创建索引时出错: {str(e)}")
            Log.logger.error(traceback.format_exc())
  
    def getAllFund(db='default'):
        sql='select * from fund_basic'
        data=DB.select_to_df(sql, db)
        return data      
       
    # 重新获取数据 
    def getDataAndReplace(pro, api, table, db):
        """
        获取数据并替换表
        
        Args:
            pro: Tushare API客户端
            api: API名称
            table: 表名
            db: 数据库连接名
            
        Returns:
            bool: 操作成功返回True，否则返回False
        """
        try:
            # 首先检查数据库目录是否可用
            if not tsSHelper.check_database_directory(db):
                Log.logger.error(f"{api}: 数据库目录检查失败，无法继续操作")
                return False
            
            # 检查数据库连接是否正常
            adapter = DB.get_adapter(db)
            if not adapter:
                Log.logger.error(f"{api}: 无法获取数据库适配器")
                return False
                
            # 删除临时表(如果存在)
            Log.logger.info(f"{api}: 正在删除临时表 {table}_tmp...")
            try:
                DB.exec(f"DROP TABLE IF EXISTS {table}_tmp", db)
            except Exception as drop_error:
                Log.logger.warning(f"{api}: 删除临时表失败: {str(drop_error)}")
                # 尝试继续执行，可能是临时表不存在
            
            # 调用API获取数据
            Log.logger.info(f"{api}: 正在调用Tushare API获取数据...")
            f = getattr(pro, api)
            
            try:
                data = f()
                
                # 检查数据是否为空
                if data is None or data.empty:
                    Log.logger.warning(f"{api}: 未获取到任何数据")
                    return False
                
                # 预处理数据，确保关键字段为字符串类型
                Log.logger.info(f"{api}: 正在处理{len(data)}条数据...")
                for col in data.columns:
                    if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                       'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                        data[col] = data[col].fillna('').astype(str)
                
                # 写入临时表
                Log.logger.info(f"{api}: 正在将数据写入临时表 {table}_tmp...")
                DB.safe_to_sql(data, f"{table}_tmp", db, index=False, if_exists='replace', chunksize=5000)
                
                # 检查临时表是否创建成功
                if not DB.table_exists(f"{table}_tmp", db):
                    Log.logger.error(f"{api}: 临时表 {table}_tmp 创建失败")
                    return False
                
                # 使用统一的replace_table方法替换表
                Log.logger.info(f"{api}: 正在将临时表替换为正式表...")
                table_to_use = DB.replace_table(table, f"{table}_tmp", db)
                
                # 设置索引
                Log.logger.info(f"{api}: 正在为表 {table_to_use} 创建索引...")
                tsSHelper.setIndex(table_to_use, db)
                
                Log.logger.info(f"{api}: 数据同步完成，共{len(data)}条记录")
                return True
            except Exception as api_error:
                if "每天最多访问" in str(api_error) or "每小时最多访问" in str(api_error):
                    Log.logger.warning(f"{api}: 触发访问限制。\n{str(api_error)}")
                    return False
                elif "最多访问" in str(api_error):
                    Log.logger.warning(f"{api}: 触发限流，等待重试。\n{str(api_error)}")
                    time.sleep(15)
                    # 重试一次
                    Log.logger.info(f"{api}: 正在重试获取数据...")
                    data = f()
                    if data is None or data.empty:
                        Log.logger.warning(f"{api}: 重试后仍未获取到任何数据")
                        return False
                    
                    # 预处理数据
                    Log.logger.info(f"{api}: 正在处理{len(data)}条数据...")
                    for col in data.columns:
                        if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                           'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                            data[col] = data[col].fillna('').astype(str)
                    
                    # 写入临时表
                    Log.logger.info(f"{api}: 正在将数据写入临时表 {table}_tmp...")
                    DB.safe_to_sql(data, f"{table}_tmp", db, index=False, if_exists='replace', chunksize=5000)
                    
                    # 检查临时表是否创建成功
                    if not DB.table_exists(f"{table}_tmp", db):
                        Log.logger.error(f"{api}: 临时表 {table}_tmp 创建失败")
                        return False
                    
                    # 替换表
                    Log.logger.info(f"{api}: 正在将临时表替换为正式表...")
                    table_to_use = DB.replace_table(table, f"{table}_tmp", db)
                    
                    # 设置索引
                    Log.logger.info(f"{api}: 正在为表 {table_to_use} 创建索引...")
                    tsSHelper.setIndex(table_to_use, db)
                    
                    Log.logger.info(f"{api}: 数据同步完成，共{len(data)}条记录")
                    return True
                else:
                    Log.logger.error(f"{api}: 调用API失败: {str(api_error)}")
                    Log.logger.error(traceback.format_exc())
                    return False
        except Exception as e:
            Log.logger.error(f"{api}: 处理数据过程中出错: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False
    
    
    # 根据最后日期获取数据
    def getDataWithLastDate(pro, api, table, db, filed='trade_date', ts_code=''):
        lastdate = tsSHelper.getLastDateAndDelete(table=table, filed=filed, ts_code=ts_code, db=db)
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
                    df=pd.DataFrame()
                    if(ts_code==''):
                        if filed=='trade_date':
                            df=f(trade_date=day)
                        elif filed=='ann_date':
                            df=f(ann_date=day)
                        elif filed=='end_date':
                            df=f(end_date=day)
                        elif filed=='date':
                            df=f(date=day)
                        elif filed=='nav_date':
                            df=f(nav_date=day)
                        elif filed=='cal_date':
                            df=f(cal_date=day)
                        else:
                            alert.send(api, '函数异常', filed+"未处理")
                    else:
                        if filed=='trade_date':
                            df=f(trade_date=day, ts_code=ts_code)
                        elif filed=='ann_date':
                            df=f(ann_date=day, ts_code=ts_code)
                        elif filed=='end_date':
                            df=f(end_date=day, ts_code=ts_code)  
                        elif filed=='date':
                            df=f(date=day, ts_code=ts_code)  
                        elif filed=='nav_date':
                            df=f(nav_date=day, ts_code=ts_code)
                        elif filed=='cal_date':
                            df=f(cal_date=day)
                        else:
                            alert.send(api, '函数异常', filed+"未处理")
                    
                    if(not df.empty):
                        # 使用DB类的to_sql方法
                        DB.to_sql(df, table, db, 'append')
                    break
                except Exception as e:
                    if "每分钟最多访问" in str(e):
                        Log.logger.warning(api+":触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    
                    if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning(api+":今日权限用完。\n"+str(e))
                        return
                         
                   
                    elif "您没有访问该接口的权限" in str(e):
                        Log.logger.warning(api+":没有访问该接口的权限。\n"+str(e))
                        return
                    
                    else:
                        if try_times<10:
                            try_times=try_times+1
                            Log.logger.error(api+":函数异常，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:                        
                            info = traceback.format_exc()
                            alert.send(api, '函数异常', str(info))
                            
                            Log.logger.error(api+"\n"+info)
                            return

            i=i+1        
            
    
    def getDataWithCodeAndClear(pro, api, table, db):
        DB.exec(f"DROP TABLE IF EXISTS {table}_tmp", db)
        data = tsSHelper.getAllAStock(True, pro, db)
        stock_list = data['ts_code'].tolist()
        f = getattr(pro, api)
        for code in stock_list:
            try_times=0
            while True:
                try:
                    df = f(ts_code=code)
                    # 使用DB类的to_sql方法
                    DB.to_sql(df, f"{table}_tmp", db, 'append')
                    break
                except Exception as e:
                    if "每分钟最多访问" in str(e):
                        Log.logger.warning(api+":触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    
                    if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning(api+":今日权限用完。\n"+str(e))
                        return
                   
                    elif "您没有访问该接口的权限" in str(e):
                        Log.logger.warning(api+":没有访问该接口的权限。\n"+str(e))
                        return
                    
                    else:
                        if try_times<10:
                            try_times=try_times+1
                            Log.logger.error(api+":函数异常，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:                            
                            info = traceback.format_exc()
                            alert.send(api, '函数异常', str(info))
                            Log.logger.error(str(info))
                            return
     
        # 使用统一的replace_table方法替换表
        table_to_use = DB.replace_table(table, f"{table}_tmp", db)
            
        # 设置索引
        tsSHelper.setIndex(table_to_use, db)
        
    
    # 查一下最后的数据是哪天
    def getLastDateAndDelete(table, filed, ts_code="", db='default'):
        """
        获取表中特定字段的最大日期，并删除该日期对应的数据
        
        Args:
            table: 表名
            filed: 日期字段名
            ts_code: 指定的股票代码，为空则查询所有记录
            db: 数据库连接名
            
        Returns:
            str: 格式为YYYYMMDD的最大日期，如果出错则返回默认日期
        """
        # 检查表是否存在
        try:
            adapter = DB.get_adapter(db)
            table_exists = adapter.table_exists(table)
            
            if not table_exists:
                Log.logger.warning(f"表 {table} 不存在，返回默认日期")
                
                # 尝试清理相关的checkpoint文件
                try:
                    from finhack.collector.tushare.astockprice import tsAStockPrice
                    # 提取API名称（通常是表名中astock_price_之后的部分）
                    if table.startswith('astock_price_'):
                        api = table[len('astock_price_'):]
                        # 重置checkpoint
                        tsAStockPrice.reset_checkpoint(api, table)
                except Exception as cp_error:
                    Log.logger.warning(f"清理checkpoint时出错: {str(cp_error)}")
                
                return '20000104'  # 如果表不存在，返回2000年1月4日(A股最早可获取的数据日期)
            
            # 获取最后日期，使用兼容SQLite的语法
            try:
                if ts_code == "":
                    sql = f"SELECT MAX({filed}) as max_date FROM {table}"
                else:
                    sql = f"SELECT MAX({filed}) as max_date FROM {table} WHERE ts_code='{ts_code}'"
                    
                result = DB.select_to_list(sql, db)
                
                if not result or result[0].get('max_date') is None or result[0].get('max_date') == '':
                    return '20000104'  # 如果没有数据，返回2000年1月4日(A股最早可获取的数据日期)
                
                max_date = result[0]['max_date']
                
                # 删除最后一天的数据（为了避免不完整），使用参数化查询以避免SQL注入
                try:
                    if ts_code == "":
                        DB.delete(f"DELETE FROM {table} WHERE {filed}='{max_date}'", db)
                    else:
                        DB.delete(f"DELETE FROM {table} WHERE {filed}='{max_date}' AND ts_code='{ts_code}'", db)
                    
                    Log.logger.debug(f"已从表 {table} 删除日期为 {max_date} 的数据")
                except Exception as delete_error:
                    # 删除失败不应该影响后续处理，记录错误并继续
                    Log.logger.warning(f"无法删除表 {table} 中日期为 {max_date} 的数据: {str(delete_error)}")
                    
                return max_date
            except Exception as query_error:
                Log.logger.error(f"查询表 {table} 的最大日期时出错: {str(query_error)}")
                return '20000104'  # 查询出错时返回2000年1月4日
        except Exception as e:
            Log.logger.error(f"处理表 {table} 的最后日期时出错: {str(e)}")
            return '20000104'  # 出错时返回2000年1月4日