import sys
import time
import datetime
import traceback
import pandas as pd

from finhack.library.db import DB
from finhack.library.alert import alert
from finhack.library.monitor import tsMonitor
from finhack.collector.tushare.helper import tsSHelper
from finhack.collector.tushare.astockprice import tsAStockPrice
import finhack.library.log as Log

class tsAStockOther:
    @tsMonitor
    def margin(pro,db):
        tsSHelper.getDataWithLastDate(pro,'margin','astock_market_margin',db)
    
    @tsMonitor
    def margin_detail(pro,db):
        tsSHelper.getDataWithLastDate(pro,'margin_detail','astock_market_margin_detail',db)
    
    @tsMonitor
    def report_rc(pro,db):
        table="astock_other_report_rc"
        
        today = datetime.datetime.now().strftime("%Y%m%d")
        lastdate = tsSHelper.getLastDateAndDelete('astock_other_report_rc','report_date',ts_code='',db=db)
        
        # 按天循环获取数据，避免日期范围导致的重复数据问题
        begin = datetime.datetime.strptime(lastdate, "%Y%m%d")
        end = datetime.datetime.strptime(today, "%Y%m%d")
        current_date = begin
        
        while current_date <= end:
            day = current_date.strftime("%Y%m%d")
            try_times = 0
            
            while True:
                try:
                    # 统一使用trade_date参数获取单日数据
                    df = pro.report_rc(trade_date=day)
                    if not df.empty:
                        DB.safe_to_sql(df, table, db, index=False, if_exists='append', chunksize=5000)
                    break
                except Exception as e:
                    if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning(f"report_rc:日期{day}触发最多访问。\n{str(e)}")
                        return
                    elif "每分钟最多访问" in str(e):
                        Log.logger.warning(f"report_rc:日期{day}触发限流，等待重试。\n{str(e)}")
                        time.sleep(15)
                        continue
                    else:
                        if try_times < 10:
                            try_times += 1
                            Log.logger.error(f"report_rc:日期{day}函数异常，等待重试。\n{str(e)}")
                            time.sleep(15)
                            continue
                        else:
                            info = traceback.format_exc()
                            alert.send('report_rc', f'日期{day}函数异常', str(info))
                            Log.logger.error(info)  
                            break  # 跳过这一天，继续下一天
            
            # 移动到下一天
            current_date += datetime.timedelta(days=1)
            time.sleep(0.1)  # 避免请求过快
                        
 
    @tsMonitor
    def cyq_perf(pro,db):
        """
        获取A股每日筹码平均成本和胜率情况
        接口要求提供ts_code或trade_date至少一个参数
        """
        table = 'astock_other_cyq_perf'
        tsSHelper.getDataWithLastDate(pro,'cyq_perf','astock_other_cyq_perf',db,'trade_date')
        
        # # 获取最近交易日期，用于增量更新
        # lastdate = tsSHelper.getLastDateAndDelete(table=table, filed='trade_date', db=db)
        # start_date = datetime.datetime.strptime(lastdate, '%Y%m%d').date()
        # end_date = datetime.datetime.now().date()
        
        # # 获取股票列表
        # try:
        #     stock_list = DB.select_to_df("SELECT ts_code FROM astock_basic", db)
        #     if stock_list.empty:
        #         Log.logger.error("获取股票列表失败，请确保astock_basic表已正确创建并包含数据")
        #         return
            
        #     # 按批次处理日期
        #     batch_days = 60  # 每批次处理60天，避免数据过多
        #     current_start_date = start_date
            
        #     while current_start_date <= end_date:
        #         # 计算当前批次的结束日期
        #         current_end_date = min(current_start_date + datetime.timedelta(days=batch_days), end_date)
        #         current_start_str = current_start_date.strftime('%Y%m%d')
        #         current_end_str = current_end_date.strftime('%Y%m%d')
                
        #         Log.logger.info(f"获取筹码及胜率数据, 日期范围: {current_start_str} 至 {current_end_str}")
                
        #         # 对每只股票获取数据
        #         for idx, row in stock_list.iterrows():
        #             ts_code = row['ts_code']
        #             try_times = 0
                    
        #             while try_times < 10:
        #                 try:
        #                     # 使用ts_code和日期范围获取数据
        #                     df = pro.cyq_perf(ts_code=ts_code, start_date=current_start_str, end_date=current_end_str)
                            
        #                     if df is not None and not df.empty:
        #                         # 预处理数据，确保关键字段为字符串类型
        #                         for col in df.columns:
        #                             if col in ['ts_code', 'trade_date'] or \
        #                                'code' in col.lower() or 'date' in col.lower():
        #                                 df[col] = df[col].fillna('').astype(str)
                                
        #                         # 写入数据库
        #                         Log.logger.info(f"为股票 {ts_code} 写入 {len(df)} 条筹码及胜率数据")
        #                         DB.safe_to_sql(df, table, db, index=False, if_exists='append', chunksize=5000)
                            
        #                     # 成功获取数据，跳出循环
        #                     break
                            
        #                 except Exception as e:
        #                     if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
        #                         Log.logger.warning(f"cyq_perf: 触发最多访问。\n{str(e)}")
        #                         return
        #                     elif "每分钟最多访问" in str(e) or "最多访问" in str(e):
        #                         Log.logger.warning(f"cyq_perf: 触发限流，等待重试。\n{str(e)}")
        #                         time.sleep(30)  # 限流时等待更长时间
        #                         try_times += 1
        #                         continue
        #                     else:
        #                         try_times += 1
        #                         Log.logger.error(f"cyq_perf: 获取股票 {ts_code} 数据失败: {str(e)}")
        #                         time.sleep(5)
        #                         continue
                    
        #             # 避免频繁调用API导致限流
        #             time.sleep(0.5)
                
        #         # 移动到下一个批次
        #         current_start_date = current_end_date + datetime.timedelta(days=1)
                
        #         # 批次之间的间隔
        #         time.sleep(5)
                
        # except Exception as e:
        #     info = traceback.format_exc()
        #     alert.send('cyq_perf', '函数异常', str(info))
        #     Log.logger.error(f"cyq_perf: 处理数据时发生错误: {str(e)}\n{info}")
        #     return False
            
        # return True

    @tsMonitor
    def cyq_chips(pro,db):
        """筹码分布 cyq_chips —— 增量模式。每只股票从其最大交易日续抓, 只写新增, 不再每次全量重建。
        tushare cyq_chips 必传 ts_code 且单次最多 ~6000 行(近60天), 故无法按 trade_date 批量、也拿不到完整历史;
        调用次数固定 5000/次(~10分钟, endpoint 限制), 但写入从全量30M降到每日增量 ~50万行。
        首次运行每股票抓近60天(一次性~30M), 之后每日每股票只补新增几天。自然可续(每股票独立 lastdate)。"""
        table='astock_other_cyq_chips'
        today=datetime.datetime.now().strftime("%Y%m%d")
        data=tsSHelper.getAllAStock(True,pro,db)
        stock_list=data['ts_code'].tolist()
        done=ok=0
        for ts_code in stock_list:
            try:
                # 该股票在表里的最大交易日(表空返回默认早日期); 同时删掉当天以便完整重抓(幂等)
                lastdate=tsSHelper.getLastDateAndDelete(table,'trade_date',ts_code=ts_code,db=db)
                try_times=0; df=None
                while True:
                    try:
                        df=pro.cyq_chips(ts_code=ts_code, start_date=lastdate, end_date=today)
                        break
                    except Exception as e:
                        if "没有接口" in str(e) or "访问权限" in str(e) or "没有权限" in str(e):
                            # 永久错误: token 无此接口权限, 整表立即放弃(否则 5000股×10×15s 重试 = 几十小时)
                            Log.logger.error(f"cyq_chips: token 无此接口权限, 整表跳过(不重试): {str(e).split('。')[0][:80]}"); return
                        if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                            Log.logger.warning(f"cyq_chips 触发时段访问上限, 终止: {e}"); return
                        if "最多访问" in str(e) or "频率超限" in str(e):
                            time.sleep(15); continue
                        if try_times<10:
                            try_times+=1
                            Log.logger.error(f"cyq_chips {ts_code} 异常, 重试#{try_times}: {e}")
                            time.sleep(15); continue
                        Log.logger.error(f"cyq_chips {ts_code} 重试耗尽, 跳过: {e}"); df=None; break
                if df is not None and not df.empty:
                    df=df.drop_duplicates(subset=['ts_code','trade_date','price'],keep='first')
                    DB.safe_to_sql(df, table, db, index=False, if_exists='append', chunksize=5000)
                    ok+=1
                done+=1
                if done%500==0:
                    Log.logger.info(f"cyq_chips 进度: {done}/{len(stock_list)}, 有新增 {ok} 只")
            except Exception as e:
                Log.logger.error(f"cyq_chips 处理 {ts_code} 未预期错误: {e}")
            time.sleep(0.12)  # 贴 500/分钟天花板
        Log.logger.info(f"cyq_chips 完成: {done}/{len(stock_list)} 只, {ok} 只有新增写入")
        tsSHelper.setIndex(table,db)
        
        # engine=DB.get_db_engine(db)
        # if True:
        #     try_times=0
        #     while True:
        #         try:
        #             today = datetime.datetime.now()
        #             today=today.strftime("%Y%m%d")
        #             lastdate=tsSHelper.getLastDateAndDelete('astock_other_cyq_chips','trade_date',ts_code='',db=db)
        #             df =pro.cyq_chips(start_date=lastdate, end_date=today)
        #             df.to_sql('astock_other_cyq_chips', engine, index=False, if_exists='append', chunksize=5000)
        #             break
        #         except Exception as e:
        #             if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
        #                 print("cyq_chips:触发最多访问。\n"+str(e))
        #                 return
        #             elif "每分钟最多访问" in str(e):
        #                 print("cyq_chips:触发限流，等待重试。\n"+str(e))
        #                 time.sleep(15)
        #                 continue
        #             else:
        #                 if try_times<10:
        #                     try_times=try_times+1;
        #                     print("cyq_chips:函数异常，等待重试。\n"+str(e))
        #                     time.sleep(15)
        #                     continue
        #                 else:                    
        #                     info = traceback.format_exc()
        #                     alert.send('cyq_chips','函数异常',str(info))
        #                     print(info)     
        #                     return
    
    
    
    
    #broker_recommend