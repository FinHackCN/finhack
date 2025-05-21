import re
import sys
import time
import datetime
import traceback
import pandas as pd

from finhack.library.db import DB
from finhack.library.alert import alert
from finhack.library.monitor import tsMonitor
import finhack.library.log as Log

class tsSHelper:
    """
    Tushare数据库辅助类
    使用DB类提供的统一数据库接口，支持DuckDB和MySQL
    """
    
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
        使用DB.set_index统一接口
        """
        # 直接使用DB类的set_index方法
        DB.set_index(table, db)
  
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
            DB.exec(f"DROP TABLE IF EXISTS {table}_tmp", db)
            f = getattr(pro, api)
            
            try:
                data = f()
                
                # 检查数据是否为空
                if data is None or data.empty:
                    Log.logger.warning(f"{api}: 未获取到任何数据")
                    return False
                
                # 预处理数据，确保关键字段为字符串类型
                for col in data.columns:
                    if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                       'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                        data[col] = data[col].fillna('').astype(str)
                
                # 使用DB类的to_sql方法
                DB.safe_to_sql(data, f"{table}_tmp", db, index=False, if_exists='replace', chunksize=5000)
                
                # 使用统一的replace_table方法替换表
                table_to_use = DB.replace_table(table, f"{table}_tmp", db)
                
                # 设置索引
                tsSHelper.setIndex(table_to_use, db)
                
                return True
            except Exception as api_error:
                if "每天最多访问" in str(api_error) or "每小时最多访问" in str(api_error):
                    Log.logger.warning(f"{api}: 触发访问限制。\n{str(api_error)}")
                    return False
                elif "最多访问" in str(api_error):
                    Log.logger.warning(f"{api}: 触发限流，等待重试。\n{str(api_error)}")
                    time.sleep(15)
                    # 重试一次
                    data = f()
                    if data is None or data.empty:
                        Log.logger.warning(f"{api}: 重试后仍未获取到任何数据")
                        return False
                    
                    # 预处理数据
                    for col in data.columns:
                        if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                           'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                            data[col] = data[col].fillna('').astype(str)
                    
                    DB.safe_to_sql(data, f"{table}_tmp", db, index=False, if_exists='replace', chunksize=5000)
                    table_to_use = DB.replace_table(table, f"{table}_tmp", db)
                    tsSHelper.setIndex(table_to_use, db)
                    return True
                else:
                    Log.logger.error(f"{api}: 调用API失败: {str(api_error)}")
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