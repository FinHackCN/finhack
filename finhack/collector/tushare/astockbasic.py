import sys
import time
import traceback
import pandas as pd
import os

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
            # 先检查数据库目录是否可用
            if not tsSHelper.check_database_directory(db):
                Log.logger.error("数据库目录检查失败，无法继续操作")
                return False
            
            # 检查数据库连接是否正常
            adapter = DB.get_adapter(db)
            if not adapter:
                Log.logger.error("无法获取数据库适配器")
                return False
                
            # 删除临时表(如果存在)
            try:
                DB.exec("drop table if exists "+table+"_tmp", db)
            except Exception as drop_error:
                Log.logger.warning(f"删除临时表失败: {str(drop_error)}")
                # 继续执行，临时表可能不存在
            
            # 获取上市股票数据
            Log.logger.info("正在获取上市股票数据...")
            data=pro.stock_basic(list_status='L', fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
            
            # 检查数据是否为空
            if data is None or data.empty:
                Log.logger.error("获取上市股票数据失败：返回数据为空")
                return False
                
            # 预处理数据，确保股票代码等字段为字符串类型
            for col in data.columns:
                if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                    data[col] = data[col].astype(str)
            
            # 创建临时表并写入数据
            Log.logger.info(f"正在将{len(data)}条上市股票数据写入临时表...")
            success = DB.safe_to_sql(data, table+"_tmp", db, index=False, if_exists='replace', chunksize=5000)
            if not success and success != 0:  # 0表示DataFrame为空，不视为错误
                Log.logger.error("写入上市股票数据到临时表失败")
                return False
                
            # 获取退市股票数据
            Log.logger.info("正在获取退市股票数据...")
            data=pro.stock_basic(list_status='D', fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
            
            # 检查数据是否为空
            if data is None or data.empty:
                Log.logger.warning("获取退市股票数据为空，仅使用上市股票数据")
            else:
                # 预处理数据，确保股票代码等字段为字符串类型
                for col in data.columns:
                    if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                    'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                        data[col] = data[col].astype(str)
                
                # 追加退市股票数据到临时表
                Log.logger.info(f"正在将{len(data)}条退市股票数据追加到临时表...")
                success = DB.safe_to_sql(data, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
                if not success and success != 0:
                    Log.logger.error("写入退市股票数据到临时表失败")
                    
            # 检查临时表是否存在
            if not DB.table_exists(table+"_tmp", db):
                Log.logger.error(f"临时表 {table}_tmp 不存在，无法替换目标表")
                return False
                
            # 使用统一的replace_table方法替换表
            Log.logger.info(f"正在将临时表替换为正式表...")
            table_to_use = DB.replace_table(table, table+"_tmp", db)
            
            # 创建索引
            Log.logger.info(f"正在为表 {table_to_use} 创建索引...")
            tsSHelper.setIndex(table_to_use, db)
            
            Log.logger.info(f"股票基本信息获取完成，成功保存到表 {table_to_use}")
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
            # 检查数据库连接是否正常
            adapter = DB.get_adapter(db)
            if not adapter:
                Log.logger.error("无法获取数据库适配器")
                return False
                
            # 删除临时表(如果存在)
            DB.exec("drop table if exists "+table+"_tmp", db)
            
            # 获取沪市沪深港通成分股
            Log.logger.info("正在获取沪市沪深港通成分股...")
            data = pro.hs_const(hs_type='SH')
            
            # 检查数据是否为空
            if data is None or data.empty:
                Log.logger.warning("获取沪市沪深港通成分股数据为空")
                data = pd.DataFrame()
            else:
                # 预处理数据，确保股票代码等字段为字符串类型
                for col in data.columns:
                    if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                    'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                        data[col] = data[col].astype(str)
                
                # 写入临时表
                Log.logger.info(f"正在将{len(data)}条沪市沪深港通成分股数据写入临时表...")
                DB.safe_to_sql(data, table+"_tmp", db, index=False, if_exists='replace', chunksize=5000)
            
            # 获取深市沪深港通成分股
            Log.logger.info("正在获取深市沪深港通成分股...")
            sz_data = pro.hs_const(hs_type='SZ')
            
            # 检查数据是否为空
            if sz_data is None or sz_data.empty:
                Log.logger.warning("获取深市沪深港通成分股数据为空")
            else:
                # 预处理数据，确保股票代码等字段为字符串类型
                for col in sz_data.columns:
                    if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                    'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                        sz_data[col] = sz_data[col].astype(str)
                
                # 如果临时表已有数据，使用append模式；否则使用replace模式
                if_exists = 'append' if not data.empty else 'replace'
                Log.logger.info(f"正在将{len(sz_data)}条深市沪深港通成分股数据写入临时表...")
                DB.safe_to_sql(sz_data, table+"_tmp", db, index=False, if_exists=if_exists, chunksize=5000)
            
            # 检查临时表是否存在
            if not DB.table_exists(table+"_tmp", db):
                Log.logger.error(f"临时表 {table}_tmp 不存在，无法替换目标表")
                return False
                
            # 使用统一的replace_table方法替换表
            Log.logger.info(f"正在将临时表替换为正式表...")
            table_to_use = DB.replace_table(table, table+"_tmp", db)
            
            # 创建索引
            Log.logger.info(f"正在为表 {table_to_use} 创建索引...")
            tsSHelper.setIndex(table_to_use, db)
            
            Log.logger.info(f"沪深港通成分股获取完成，成功保存到表 {table_to_use}")
            return True
        except Exception as e:
            Log.logger.error(f"获取沪深港通成分股失败: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False
       
    @tsMonitor 
    def stock_company(pro,db):
        table='astock_stock_company'
        try:
            # 检查数据库连接是否正常
            adapter = DB.get_adapter(db)
            if not adapter:
                Log.logger.error("无法获取数据库适配器")
                return False
                
            # 删除临时表(如果存在)
            DB.exec("drop table if exists "+table+"_tmp", db)
            
            # 获取深交所上市公司基本信息
            Log.logger.info("正在获取深交所上市公司基本信息...")
            data = pro.stock_company(exchange='SZSE', fields='ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,main_business,business_scope')
            
            # 检查数据是否为空
            if data is None or data.empty:
                Log.logger.warning("获取深交所上市公司基本信息为空")
                data = pd.DataFrame()
            else:
                # 预处理数据，确保股票代码等字段为字符串类型
                for col in data.columns:
                    if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                    'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                        data[col] = data[col].astype(str)
                
                # 写入临时表
                Log.logger.info(f"正在将{len(data)}条深交所上市公司数据写入临时表...")
                DB.safe_to_sql(data, table+"_tmp", db, index=False, if_exists='replace', chunksize=5000)
            
            # 获取上交所上市公司基本信息
            Log.logger.info("正在获取上交所上市公司基本信息...")
            sse_data = pro.stock_company(exchange='SSE', fields='ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,main_business,business_scope')
            
            # 检查数据是否为空
            if sse_data is None or sse_data.empty:
                Log.logger.warning("获取上交所上市公司基本信息为空")
            else:
                # 预处理数据，确保股票代码等字段为字符串类型
                for col in sse_data.columns:
                    if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                    'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                        sse_data[col] = sse_data[col].astype(str)
                
                # 如果临时表已有数据，使用append模式；否则使用replace模式
                if_exists = 'append' if not data.empty else 'replace'
                Log.logger.info(f"正在将{len(sse_data)}条上交所上市公司数据写入临时表...")
                DB.safe_to_sql(sse_data, table+"_tmp", db, index=False, if_exists=if_exists, chunksize=5000)
            
            # 检查临时表是否存在
            if not DB.table_exists(table+"_tmp", db):
                Log.logger.error(f"临时表 {table}_tmp 不存在，无法替换目标表")
                return False
                
            # 使用统一的replace_table方法替换表
            Log.logger.info(f"正在将临时表替换为正式表...")
            table_to_use = DB.replace_table(table, table+"_tmp", db)
            
            # 创建索引
            Log.logger.info(f"正在为表 {table_to_use} 创建索引...")
            tsSHelper.setIndex(table_to_use, db)
            
            Log.logger.info(f"上市公司基本信息获取完成，成功保存到表 {table_to_use}")
            return True
        except Exception as e:
            Log.logger.error(f"获取上市公司基本信息失败: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False
    
    @tsMonitor
    def stk_managers(pro,db):
        try:
            return tsSHelper.getDataAndReplace(pro,'stk_managers','astock_stk_managers',db)
        except Exception as e:
            Log.logger.error(f"获取公司管理层失败: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False

    @tsMonitor
    def stk_rewards(pro,db):
        table='astock_stk_rewards'
        try:
            # 检查数据库连接是否正常
            adapter = DB.get_adapter(db)
            if not adapter:
                Log.logger.error("无法获取数据库适配器")
                return False
                
            # 删除临时表(如果存在)
            DB.exec("drop table if exists "+table+"_tmp", db)
            
            # 获取股票列表
            Log.logger.info("正在获取股票列表...")
            data = tsSHelper.getAllAStock(True, pro, db)
            
            if data is None or data.empty:
                Log.logger.error("获取股票列表失败，无法继续获取管理层薪酬和持股")
                return False
                
            stock_list = data['ts_code'].tolist()
            Log.logger.info(f"共获取到{len(stock_list)}只股票，开始批量获取管理层薪酬和持股...")
            
            processed_count = 0
            for i in range(0, len(stock_list), 100):
                code_list = stock_list[i:i+100]
                processed_count += len(code_list)
                Log.logger.info(f"正在处理第{i+1}-{i+len(code_list)}只股票，共{len(stock_list)}只...")
                
                try_times = 0
                while True:
                    try:
                        df = pro.stk_rewards(ts_code=','.join(code_list))
                        
                        # 检查数据是否为空
                        if df is None or df.empty:
                            Log.logger.warning(f"获取股票{code_list[0]}等{len(code_list)}只股票的管理层薪酬和持股数据为空")
                            break
                            
                        # 预处理数据，确保股票代码等字段为字符串类型
                        for col in df.columns:
                            if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                               'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                                df[col] = df[col].astype(str)
                        
                        # 如果是第一批数据使用replace，否则使用append
                        if i == 0:
                            if_exists = 'replace'
                        else:
                            if_exists = 'append'
                            
                        Log.logger.info(f"正在将{len(df)}条管理层薪酬和持股数据写入临时表...")
                        DB.safe_to_sql(df, table+"_tmp", db, index=False, if_exists=if_exists, chunksize=5000)
                        break
                    except Exception as e:
                        if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                            Log.logger.warning(f"stk_rewards:触发最多访问限制: {str(e)}")
                            return False
                        if "最多访问" in str(e):
                            Log.logger.warning(f"stk_rewards:触发限流，等待重试: {str(e)}")
                            time.sleep(15)
                            continue
                        else:
                            if try_times < 10:
                                try_times = try_times + 1
                                Log.logger.warning(f"stk_rewards:函数异常，等待重试({try_times}/10): {str(e)}")
                                time.sleep(15)
                                continue
                            else:
                                Log.logger.error(f"stk_rewards:函数异常，重试10次失败: {str(e)}")
                                Log.logger.error(traceback.format_exc())
                                return False
            
            # 检查临时表是否存在并有数据
            if not DB.table_exists(table+"_tmp", db):
                Log.logger.error(f"临时表 {table}_tmp 不存在，无法替换目标表")
                return False
                
            # 使用统一的replace_table方法替换表
            Log.logger.info(f"正在将临时表替换为正式表...")
            table_to_use = DB.replace_table(table, table+"_tmp", db)
            
            # 创建索引
            Log.logger.info(f"正在为表 {table_to_use} 创建索引...")
            tsSHelper.setIndex(table_to_use, db)
            
            Log.logger.info(f"管理层薪酬和持股数据获取完成，成功保存到表 {table_to_use}")
            return True
        except Exception as e:
            Log.logger.error(f"获取管理层薪酬和持股失败: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False
        
    @tsMonitor       
    def new_share(pro,db):
        try:
            return tsSHelper.getDataAndReplace(pro,'new_share','astock_new_share',db)
        except Exception as e:
            Log.logger.error(f"获取新股上市信息失败: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False

    