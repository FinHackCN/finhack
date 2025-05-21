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

class tsAStockPrice:
    def getPrice(pro,api,table,db):
        # 不需要获取engine对象，直接使用db连接名
        # engine = DB.get_db_engine(db)
        lastdate=tsSHelper.getLastDateAndDelete(table=table,filed='trade_date',ts_code="",db=db)
        start_date = datetime.datetime.strptime(lastdate, "%Y%m%d").date()
        end_date = datetime.datetime.now().date()
        
        # 按批次获取数据
        batch_days = 180  # 每批次获取180天的数据
        current_start_date = start_date
        
        while current_start_date <= end_date:
            # 计算当前批次的结束日期
            current_end_date = min(current_start_date + datetime.timedelta(days=batch_days), end_date)
            current_start_str = current_start_date.strftime('%Y%m%d')
            current_end_str = current_end_date.strftime('%Y%m%d')
            
            Log.logger.info(f"{api}: 获取数据, 日期范围: {current_start_str} 至 {current_end_str}")
            
            try:
                # 尝试使用起止日期范围获取数据
                f = getattr(pro, api)
                df = f(start_date=current_start_str, end_date=current_end_str)
                
                # 检查和去除重复记录
                if df is not None and not df.empty:
                    # 计算数据大小
                    original_count = len(df)
                    
                    # 按股票代码和交易日期去重（如果存在这两个字段）
                    if 'ts_code' in df.columns and 'trade_date' in df.columns:
                        df = df.drop_duplicates(subset=['ts_code', 'trade_date'], keep='first')
                        
                        # 输出去重结果
                        removed_count = original_count - len(df)
                        if removed_count > 0:
                            Log.logger.warning(f"{api}: 在日期范围 {current_start_str} 至 {current_end_str} 的源数据中发现 {removed_count} 条重复记录已被去除")
                    
                    # 预处理数据，确保关键字段为字符串类型
                    for col in df.columns:
                        if col in ['ts_code', 'symbol', 'code', 'trade_date'] or \
                           'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                            df[col] = df[col].fillna('').astype(str)
                    
                    # 检查未来日期数据
                    today_str = datetime.datetime.now().strftime('%Y%m%d')
                    if 'trade_date' in df.columns:
                        future_data = df[df['trade_date'] > today_str]
                        if not future_data.empty:
                            Log.logger.warning(f"{api}: 发现 {len(future_data)} 条未来日期数据, 最大日期: {future_data['trade_date'].max()}")
                            
                            # 移除未来日期数据
                            df = df[df['trade_date'] <= today_str]
                            Log.logger.info(f"{api}: 已移除未来日期数据，剩余 {len(df)} 条记录")
                    
                    Log.logger.info(f"{api}: 写入 {len(df)} 条数据(日期范围: {current_start_str} 至 {current_end_str})到 {table} 表")
                    DB.safe_to_sql(df, table, db, index=False, if_exists='append', chunksize=5000)
                else:
                    Log.logger.warning(f"{api}: 未获取到任何数据，日期范围: {current_start_str} 至 {current_end_str}")
                
            except Exception as e:
                if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                    Log.logger.warning(f"{api}: 触发最多访问。\n{str(e)}")
                    return
                elif "最多访问" in str(e):
                    Log.logger.warning(f"{api}: 触发限流，等待重试。\n{str(e)}")
                    time.sleep(30)
                    continue
                else:
                    Log.logger.error(f"{api}: 获取数据失败: {str(e)}")
                    # 继续处理下一批次，不中断整个流程
            
            # 移动到下一个批次
            current_start_date = current_end_date + datetime.timedelta(days=1)
            
            # 防止过快请求导致API限制，每批次间隔一些时间
            time.sleep(3)

    @tsMonitor
    def daily(pro,db):
        table='astock_price_daily'
        # 不需要获取engine对象，直接使用db连接名
        # engine = DB.get_db_engine(db)
        last_date = tsSHelper.getLastDateAndDelete(table=table, filed='trade_date', ts_code='000001.SZ', db=db)
        start_date = datetime.datetime.strptime(last_date, '%Y%m%d').date()
        end_date = datetime.datetime.now().date()
        
        # 如果开始日期和结束日期之间的间隔太大，按批次获取数据
        batch_days = 180  # 每批次获取180天的数据
        current_start_date = start_date
        
        while current_start_date <= end_date:
            # 计算当前批次的结束日期
            current_end_date = min(current_start_date + datetime.timedelta(days=batch_days), end_date)
            current_start_str = current_start_date.strftime('%Y%m%d')
            current_end_str = current_end_date.strftime('%Y%m%d')
            
            Log.logger.info(f"获取日线数据, 日期范围: {current_start_str} 至 {current_end_str}")
            
            try:
                # 使用日期范围获取数据
                df = pro.daily(start_date=current_start_str, end_date=current_end_str)
                
                # 检查和去除重复记录
                if not df.empty:
                    # 计算数据大小
                    original_count = len(df)
                    
                    # 按股票代码和交易日期去重
                    df = df.drop_duplicates(subset=['ts_code', 'trade_date'], keep='first')
                    
                    # 输出去重结果
                    removed_count = original_count - len(df)
                    if removed_count > 0:
                        Log.logger.warning(f"在源数据中发现 {removed_count} 条重复记录已被去除")
                    
                    # 预处理数据，确保股票代码等字段为字符串类型
                    for col in df.columns:
                        if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                           'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                            df[col] = df[col].fillna('').astype(str)
                    
                    # 检查未来日期数据
                    today_str = datetime.datetime.now().strftime('%Y%m%d')
                    future_data = df[df['trade_date'] > today_str]
                    if not future_data.empty:
                        Log.logger.warning(f"发现 {len(future_data)} 条未来日期数据, 最大日期: {future_data['trade_date'].max()}")
                        
                        # 移除未来日期数据
                        df = df[df['trade_date'] <= today_str]
                        Log.logger.info(f"已移除未来日期数据，剩余 {len(df)} 条记录")
                    
                    # 使用safe_to_sql写入数据
                    Log.logger.info(f"写入 {len(df)} 条日线数据到 {table} 表")
                    DB.safe_to_sql(df, table, db, index=False, if_exists='append', chunksize=5000)
                else:
                    Log.logger.warning(f"未获取到任何日线数据，日期范围: {current_start_str} 至 {current_end_str}")
            
            except Exception as e:
                if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                    Log.logger.warning(f"触发Tushare访问限制，今日额度已用完: {str(e)}")
                    break
                elif "最多访问" in str(e):
                    Log.logger.warning(f"触发Tushare访问限制，等待重试: {str(e)}")
                    time.sleep(60)  # 等待60秒后继续当前批次
                    continue
                else:
                    Log.logger.error(f"获取日线数据失败: {str(e)}")
                    # 继续处理下一批次，不中断整个流程
            
            # 移动到下一个批次
            current_start_date = current_end_date + datetime.timedelta(days=1)
            
            # 防止过快请求导致API限制，每批次间隔一些时间
            time.sleep(5)

    @tsMonitor
    def weekly(pro,db):
        """获取周线行情数据"""
        table = 'astock_price_weekly'
        # 不需要获取engine对象，直接使用db连接名
        last_date = tsSHelper.getLastDateAndDelete(table=table, filed='trade_date', ts_code='000001.SZ', db=db)
        start_date = datetime.datetime.strptime(last_date, '%Y%m%d').date()
        end_date = datetime.datetime.now().date()
        
        # 如果开始日期和结束日期之间的间隔太大，按批次获取数据
        batch_days = 360  # 周线数据可以获取更长时间范围
        current_start_date = start_date
        
        while current_start_date <= end_date:
            # 计算当前批次的结束日期
            current_end_date = min(current_start_date + datetime.timedelta(days=batch_days), end_date)
            current_start_str = current_start_date.strftime('%Y%m%d')
            current_end_str = current_end_date.strftime('%Y%m%d')
            
            Log.logger.info(f"获取周线数据, 日期范围: {current_start_str} 至 {current_end_str}")
            
            try:
                # 查询股票列表进行分批处理
                stock_list = DB.select_to_df("SELECT ts_code FROM astock_basic LIMIT 100", db)
                if stock_list.empty:
                    Log.logger.error("获取股票列表失败，请确保astock_basic表已正确创建并包含数据")
                    return
                
                # 对每个股票获取周线数据
                for index, row in stock_list.iterrows():
                    ts_code = row['ts_code']
                    try:
                        # 使用ts_code和日期范围获取数据
                        df = pro.weekly(ts_code=ts_code, start_date=current_start_str, end_date=current_end_str)
                        
                        if not df.empty:
                            # 预处理数据，确保股票代码等字段为字符串类型
                            for col in df.columns:
                                if col in ['ts_code', 'symbol', 'code', 'trade_date'] or \
                                   'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                                    df[col] = df[col].fillna('').astype(str)
                            
                            # 写入数据
                            DB.safe_to_sql(df, table, db, index=False, if_exists='append', chunksize=5000)
                            Log.logger.info(f"为股票 {ts_code} 写入 {len(df)} 条周线数据")
                        
                        # 避免频繁调用API
                        time.sleep(0.3)
                    except Exception as e:
                        Log.logger.error(f"获取股票 {ts_code} 的周线数据失败: {str(e)}")
                        # 继续处理下一个股票，不中断整个流程
                        time.sleep(1)
                        continue
            
            except Exception as e:
                Log.logger.error(f"处理周线数据时发生错误: {str(e)}")
            
            # 移动到下一个批次
            current_start_date = current_end_date + datetime.timedelta(days=1)
            
            # 防止过快请求导致API限制，每批次间隔一些时间
            time.sleep(5)

    @tsMonitor
    def monthly(pro,db):
        """获取月线行情数据"""
        table = 'astock_price_monthly'
        # 不需要获取engine对象，直接使用db连接名
        last_date = tsSHelper.getLastDateAndDelete(table=table, filed='trade_date', ts_code='000001.SZ', db=db)
        start_date = datetime.datetime.strptime(last_date, '%Y%m%d').date()
        end_date = datetime.datetime.now().date()
        
        # 如果开始日期和结束日期之间的间隔太大，按批次获取数据
        batch_days = 720  # 月线数据可以获取更长时间范围
        current_start_date = start_date
        
        while current_start_date <= end_date:
            # 计算当前批次的结束日期
            current_end_date = min(current_start_date + datetime.timedelta(days=batch_days), end_date)
            current_start_str = current_start_date.strftime('%Y%m%d')
            current_end_str = current_end_date.strftime('%Y%m%d')
            
            Log.logger.info(f"获取月线数据, 日期范围: {current_start_str} 至 {current_end_str}")
            
            try:
                # 查询股票列表进行分批处理
                stock_list = DB.select_to_df("SELECT ts_code FROM astock_basic LIMIT 100", db)
                if stock_list.empty:
                    Log.logger.error("获取股票列表失败，请确保astock_basic表已正确创建并包含数据")
                    return
                
                # 对每个股票获取月线数据
                for index, row in stock_list.iterrows():
                    ts_code = row['ts_code']
                    try:
                        # 使用ts_code和日期范围获取数据
                        df = pro.monthly(ts_code=ts_code, start_date=current_start_str, end_date=current_end_str)
                        
                        if not df.empty:
                            # 预处理数据，确保股票代码等字段为字符串类型
                            for col in df.columns:
                                if col in ['ts_code', 'symbol', 'code', 'trade_date'] or \
                                   'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                                    df[col] = df[col].fillna('').astype(str)
                            
                            # 写入数据
                            DB.safe_to_sql(df, table, db, index=False, if_exists='append', chunksize=5000)
                            Log.logger.info(f"为股票 {ts_code} 写入 {len(df)} 条月线数据")
                        
                        # 避免频繁调用API
                        time.sleep(0.3)
                    except Exception as e:
                        Log.logger.error(f"获取股票 {ts_code} 的月线数据失败: {str(e)}")
                        # 继续处理下一个股票，不中断整个流程
                        time.sleep(1)
                        continue
            
            except Exception as e:
                Log.logger.error(f"处理月线数据时发生错误: {str(e)}")
            
            # 移动到下一个批次
            current_start_date = current_end_date + datetime.timedelta(days=1)
            
            # 防止过快请求导致API限制，每批次间隔一些时间
            time.sleep(5)

    @tsMonitor
    def adj_factor(pro,db):
        tsAStockPrice.getPrice(pro,'adj_factor','astock_price_adj_factor',db)
    
    @tsMonitor
    def suspend_d(pro,db):
        tsAStockPrice.getPrice(pro,'suspend_d','astock_price_suspend_d',db)
    
    @tsMonitor
    def daily_basic(pro,db):
        tsAStockPrice.getPrice(pro,'daily_basic','astock_price_daily_basic',db)
    
    @tsMonitor
    def moneyflow(pro,db):
        tsAStockPrice.getPrice(pro,'moneyflow','astock_price_moneyflow',db)
    
    @tsMonitor
    def stk_limit(pro,db):
        tsAStockPrice.getPrice(pro,'stk_limit','astock_price_stk_limit',db)
    
    @tsMonitor
    def limit_list(pro,db):
        tsAStockPrice.getPrice(pro,'limit_list','astock_price_limit_list',db)
    
    @tsMonitor
    def moneyflow_hsgt(pro,db):
        tsAStockPrice.getPrice(pro,'moneyflow_hsgt','astock_price_moneyflow_hsgt',db)
    
    @tsMonitor
    def hsgt_top10(pro,db):
        tsAStockPrice.getPrice(pro,'hsgt_top10','astock_price_hsgt_top10',db)
    
    @tsMonitor
    def ggt_top10(pro,db):
        """获取港股通十大成交股"""
        table = 'astock_price_ggt_top10'
        last_date = tsSHelper.getLastDateAndDelete(table=table, filed='trade_date', db=db)
        start_date = datetime.datetime.strptime(last_date, '%Y%m%d').date()
        end_date = datetime.datetime.now().date()
        
        # 如果开始日期和结束日期之间的间隔太大，按批次获取数据
        batch_days = 180  # 每批次获取180天的数据
        current_start_date = start_date
        
        while current_start_date <= end_date:
            # 计算当前批次的结束日期
            current_end_date = min(current_start_date + datetime.timedelta(days=batch_days), end_date)
            current_start_str = current_start_date.strftime('%Y%m%d')
            current_end_str = current_end_date.strftime('%Y%m%d')
            
            Log.logger.info(f"获取港股通十大成交股数据, 日期范围: {current_start_str} 至 {current_end_str}")
            
            try:
                # 分别获取沪市和深市的港股通十大成交股
                for market_type in ['1', '3']:  # 1:沪市 3:深市
                    try:
                        df = pro.ggt_top10(market_type=market_type, start_date=current_start_str, end_date=current_end_str)
                        
                        if not df.empty:
                            # 预处理数据，确保股票代码等字段为字符串类型
                            for col in df.columns:
                                if col in ['ts_code', 'symbol', 'code', 'trade_date'] or \
                                   'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                                    df[col] = df[col].fillna('').astype(str)
                            
                            # 写入数据
                            DB.safe_to_sql(df, table, db, index=False, if_exists='append', chunksize=5000)
                            Log.logger.info(f"为市场类型 {market_type} 写入 {len(df)} 条港股通十大成交股数据")
                        else:
                            Log.logger.warning(f"未获取到市场类型 {market_type} 在日期范围 {current_start_str} 至 {current_end_str} 的港股通十大成交股数据")
                        
                        # 避免频繁调用API
                        time.sleep(1)
                    except Exception as e:
                        Log.logger.error(f"获取市场类型 {market_type} 的港股通十大成交股数据失败: {str(e)}")
                        time.sleep(3)
                        continue
            
            except Exception as e:
                Log.logger.error(f"处理港股通十大成交股数据时发生错误: {str(e)}")
            
            # 移动到下一个批次
            current_start_date = current_end_date + datetime.timedelta(days=1)
            
            # 防止过快请求导致API限制，每批次间隔一些时间
            time.sleep(5)
    
    @tsMonitor
    def hk_hold(pro,db):
        tsAStockPrice.getPrice(pro,'hk_hold','astock_price_hk_hold',db)
    
    @tsMonitor
    def ggt_daily(pro,db):
        tsSHelper.getDataAndReplace(pro,'ggt_daily','astock_price_ggt_daily',db)
    
    @tsMonitor
    def ggt_monthly(pro,db):
        tsSHelper.getDataAndReplace(pro,'ggt_monthly','astock_price_ggt_monthly',db)
    
    @tsMonitor
    def ccass_hold_detail(pro,db):
        pass #积分不够
        #tsStockPrice.getPrice(pro,'ccass_hold_detail','astock_price_ccass_hold_detail',db)
    
    # @tsMonitor
    # def pro_bar(pro,db):
    #     tsStockPrice.getPrice(pro,'daily','astock_price_daily',db)

 