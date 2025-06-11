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
        # 不需要获取engine对象，直接使用db连接名
        # engine = DB.get_db_engine(db)
        if True:
            try_times=0
            while True:
                try:
                    today = datetime.datetime.now()
                    today=today.strftime("%Y%m%d")
                    lastdate=tsSHelper.getLastDateAndDelete('astock_other_report_rc','report_date',ts_code='',db=db)
                    df =pro.report_rc(start_date=lastdate, end_date=today)
                    #df.to_sql('astock_other_report_rc', engine, index=False, if_exists='append', chunksize=5000)
                    DB.safe_to_sql(df, table, db, index=False, if_exists='append', chunksize=5000)
                    break
                except Exception as e:
                    if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning("report_rc:触发最多访问。\n"+str(e))
                        return
                    elif "每分钟最多访问" in str(e):
                        Log.logger.warning("report_rc:触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    else:
                        if try_times<10:
                            try_times=try_times+1;
                            Log.logger.error("report_rc:函数异常，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:
                            info = traceback.format_exc()
                            alert.send('report_rc','函数异常',str(info))
                            Log.logger.error(info)  
                            return
                        
 
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
        pass
    #感觉tushare的这个接口好像有问题
        table='astock_other_cyq_chips'
        DB.exec("drop table if exists "+table+"_tmp",db)
        # 不需要获取engine对象，直接使用db连接名
        # engine = DB.get_db_engine(db)
        data=tsSHelper.getAllAStock(True,pro,db)
        stock_list=data['ts_code'].tolist()
        
        for ts_code in stock_list:
            try_times=0
            while True:
                try:
                    df = pro.cyq_chips(ts_code=ts_code)
                    #df.to_sql('astock_other_cyq_chips_tmp', engine, index=False, if_exists='append', chunksize=5000)
                    DB.safe_to_sql(df, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
                    break
                except Exception as e:
                    if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning("cyq_chips:触发最多访问。\n"+str(e)) 
                        return
                    if "最多访问" in str(e):
                        Log.logger.warning("cyq_chips:触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    else:
                        if try_times<10:
                            try_times=try_times+1;
                            Log.logger.error("cyq_chips:函数异常，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:
                            info = traceback.format_exc()
                            alert.send('cyq_chips','函数异常',str(info))
                            Log.logger.error(info)
                            break
            
        DB.exec('rename table '+table+' to '+table+'_old;',db);
        DB.exec('rename table '+table+'_tmp to '+table+';',db);
        DB.exec("drop table if exists "+table+'_old',db)
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