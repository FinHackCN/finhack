import sys
import time
import datetime
import traceback
import pandas as pd
import threading
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from finhack.library.db import DB
from finhack.library.alert import alert
from finhack.library.monitor import tsMonitor
from finhack.collector.tushare.helper import tsSHelper
import finhack.library.log as Log

class tsAStockIndex:
    def get_date_range(start, end, periods=2, freq='1D', format='%Y%m%d'):
        """
        使用pandas库的date_range方法生成日期间隔里的所有日期, start, end, periods和freq必须指定三个
        :param start: 起始日期
        :param end: 结束日期
        :param periods: 周期
        :param freq: 时间间隔
        :param format: 格式化输出
        :return: 日期list
        """
        periods = None if start and end else periods
        date_list = pd.date_range(start=start, end=end, periods=periods, freq=freq)
        if len(date_list) < 2:
            date_list = date_list.union(date_list.shift(1)[-1:])
    
        return [item.strftime(format) for item in date_list]    

    # 添加线程锁用于保护共享资源
    _lock = Lock()

    @staticmethod
    def _process_index_daily_worker(pro, db, index_list, today):
        """
        [已废弃/不再调用] 旧的逐代码逐日抓取worker, 每行一次API调用, 极慢。
        index_daily() 已改为按 trade_date 逐日抓(1次拿全天全部指数)。保留此函数仅作存档, 勿再调用。
        线程工作函数，处理指数日线数据
        """
        table = "astock_index_daily"
        
        for ts_code in index_list:
            try:
                lastdate = tsSHelper.getLastDateAndDelete('astock_index_daily', 'trade_date', ts_code=ts_code, db=db)
                
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
                            df = pro.index_daily(ts_code=ts_code, trade_date=day)
                            if not df.empty:
                                # 使用线程锁保护数据库写入操作
                                with tsAStockIndex._lock:
                                    DB.safe_to_sql(df, table, db, index=False, if_exists='append', chunksize=5000)
                            break
                        except Exception as e:
                            if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                                Log.logger.warning(f"线程处理{ts_code}日期{day}时触发最多访问限制: {str(e)}")
                                return
                            if "最多访问" in str(e):
                                Log.logger.warning(f'线程处理{ts_code}日期{day}时触发限流，等待重试: {str(e)}')
                                time.sleep(15)
                                continue
                            else:
                                if try_times < 10:
                                    try_times += 1
                                    Log.logger.error(f"线程处理{ts_code}日期{day}时函数异常，等待重试: {str(e)}")
                                    time.sleep(15)
                                    continue
                                else:
                                    info = traceback.format_exc()
                                    alert.send('index_daily', f'线程处理{ts_code}日期{day}异常', str(info))
                                    Log.logger.error(f'线程处理{ts_code}日期{day}异常: {info}')
                                    break
                    
                    # 移动到下一天
                    current_date += datetime.timedelta(days=1)
                    time.sleep(0.1)  # 避免请求过快
            except Exception as e:
                Log.logger.error(f"处理指数{ts_code}时发生未预期的错误: {str(e)}")
                continue

    @tsMonitor
    def index_daily(pro, db):
        """指数日线 —— 按 trade_date 逐日抓(1次调用返回当天全部~8000个指数)。

        【效率优化】原实现按 ts_code × 日期 双重循环, 每行一次 API 调用, 8000指数×数千日=百万级
        调用, 全卡在 tushare 500次/分钟限流上, 跑几天才几百个代码。改为按交易日抓:
        调用量从 (代码×日) 降到 (日), 全量回填 ~6300 个交易日按限速约十几分钟, 之后每天增量 1 次。
        500/min 是硬天花板, 并发无益, 故顺序抓贴着限速即可。
        注: 起点取全表最大交易日(增量)。若要给空代码补全历史, 把下方 start 改回 '20000104' 跑一次。
        """
        table = "astock_index_daily"
        # 起点 = 全表最大交易日(增量续传); getLastDateAndDelete 会删掉当天以便完整重抓
        lastdate = tsSHelper.getLastDateAndDelete(table, 'trade_date', ts_code='', db=db)
        start = datetime.datetime.strptime(lastdate, "%Y%m%d")
        end = datetime.datetime.now()
        cur = start
        ok_days = empty_days = 0
        while cur <= end:
            day = cur.strftime("%Y%m%d")
            df = None
            try_times = 0
            while True:  # 单日抓取 + 限流重试
                try:
                    df = pro.index_daily(trade_date=day)  # 1 次拿当天全部指数
                    break
                except Exception as e:
                    msg = str(e)
                    if "每天最多访问" in msg or "每小时最多访问" in msg:
                        Log.logger.warning(f"index_daily 触发时段访问上限, 终止: {msg}")
                        return
                    if "最多访问" in msg or "频率超限" in msg:
                        time.sleep(15); continue
                    if try_times < 10:
                        try_times += 1
                        Log.logger.error(f"index_daily {day} 异常, 重试#{try_times}: {msg}")
                        time.sleep(15); continue
                    Log.logger.error(f"index_daily {day} 重试耗尽, 跳过: {msg}")
                    df = None; break
            if df is not None and not df.empty:
                df = df.drop_duplicates(subset=['ts_code', 'trade_date'], keep='first')
                with tsAStockIndex._lock:
                    DB.delete(f"DELETE FROM {table} WHERE trade_date='{day}'", db)  # 删当天再写, 幂等
                    DB.safe_to_sql(df, table, db, index=False, if_exists='append', chunksize=5000)
                ok_days += 1
                if ok_days % 50 == 0:
                    Log.logger.info(f"index_daily 进度: 已写 {ok_days} 个交易日, 当前 {day}, 本日 {len(df)} 条")
            else:
                empty_days += 1
            cur += datetime.timedelta(days=1)
            time.sleep(0.12)  # ≈8次/秒, 贴着 500/分钟天花板, 不触发限流
        Log.logger.info(f"index_daily 完成: {start.date()}~{end.date()}, 有数据{ok_days}天, 空{empty_days}天")

    @tsMonitor
    def index_basic(pro,db):
        tsSHelper.getDataAndReplace(pro,'index_basic','astock_index_basic',db)

    @tsMonitor
    def index_weekly(pro,db):
        tsSHelper.getDataWithLastDate(pro,'index_weekly','astock_index_weekly',db)

    @tsMonitor
    def index_monthly(pro,db):
        """
        获取指数月线数据（按指数代码循环增量更新）
        修复：原实现不传ts_code，导致API返回空数据
        """
        table = 'astock_index_monthly'
        try:
            # 获取所有指数代码
            data = tsSHelper.getAllAStockIndex(pro, db)
            if data is None or data.empty:
                Log.logger.error("获取指数列表失败")
                return False

            index_list = data['ts_code'].tolist()
            Log.logger.info(f"共获取到{len(index_list)}个指数，开始获取月线数据...")

            today = datetime.datetime.now().strftime("%Y%m%d")
            success_count = 0
            error_count = 0

            for ts_code in index_list:
                try_times = 0
                while True:
                    try:
                        # 获取该指数的最后日期
                        lastdate = tsSHelper.getLastDateAndDelete(table, 'trade_date', ts_code=ts_code, db=db)

                        # 调用API获取增量数据
                        df = pro.index_monthly(ts_code=ts_code, start_date=lastdate, end_date=today)

                        if df is not None and not df.empty:
                            # 预处理数据
                            for col in df.columns:
                                if 'code' in col.lower() or 'date' in col.lower():
                                    df[col] = df[col].astype(str)
                            DB.to_sql(df, table, db, 'append')
                            Log.logger.debug(f"{ts_code}: 获取到{len(df)}条月线数据")

                        success_count += 1
                        break
                    except Exception as e:
                        if "每分钟最多访问" in str(e) or "最多访问" in str(e):
                            Log.logger.warning(f"index_monthly: 触发限流，等待重试")
                            time.sleep(15)
                            continue
                        elif "您没有访问该接口的权限" in str(e):
                            Log.logger.warning(f"index_monthly: 没有访问权限")
                            break
                        else:
                            if try_times < 3:
                                try_times += 1
                                Log.logger.error(f"index_monthly {ts_code}: 获取失败，重试 {try_times}")
                                time.sleep(5)
                                continue
                            else:
                                Log.logger.error(f"index_monthly {ts_code}: 获取失败 - {str(e)}")
                                error_count += 1
                                break

                # 避免请求过快
                time.sleep(0.3)

            Log.logger.info(f"指数月线数据获取完成: 成功{success_count}, 失败{error_count}")
            return True

        except Exception as e:
            Log.logger.error(f"获取指数月线数据失败: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False

    # @tsMonitor
    # def index_weight(pro,db):
    #     tsSHelper.getDataWithLastDate(pro,'index_weight','astock_index_weight',db)
    
    @tsMonitor
    def index_dailybasic(pro,db):
        tsSHelper.getDataWithLastDate(pro,'index_dailybasic','astock_index_dailybasic',db)
    
    @tsMonitor
    def index_classify(pro,db):
        tsSHelper.getDataAndReplace(pro,'index_classify','astock_index_classify',db)
    
    # @tsMonitor
    # def index_member(pro,db):
    #     tsSHelper.getDataWithCodeAndClear(pro,'index_member','astock_index_member',db)
    #     pass
    
    @tsMonitor
    def daily_info(pro,db):
        tsSHelper.getDataWithLastDate(pro,'daily_info','astock_index_daily_info',db)
    
    @tsMonitor
    def sz_daily_info(pro,db):
        tsSHelper.getDataWithLastDate(pro,'sz_daily_info','astock_index_sz_daily_info',db)
    
    # @tsMonitor
    # def index_weekly(pro,db):
    #     data=tsSHelper.getAllAStockIndex(pro,db)
    #     index_list=data['ts_code'].tolist()
    #     for ts_code in index_list:
    #         lastdate=tsSHelper.getLastDateAndDelete('astock_index_weekly','trade_date',ts_code=ts_code,db=db)
    #         engine=DB.get_db_engine(db)   
    #         today = datetime.datetime.now()
    #         today=today.strftime("%Y%m%d")
    #         #print(ts_code)
    #         while True:
    #             try:
    #                 df=pro.index_weekly(ts_code=ts_code, start_date=lastdate, end_date=today)
    #                 if(not df.empty):
    #                     res = df.to_sql('astock_index_weekly', engine, index=False, if_exists='append', chunksize=5000)
    #                     #print(df)
    #                 break
    #             except Exception as e:
    #                 if "最多访问" in str(e):
    #                     print('index_daily'+":触发限流，等待重试。\n"+str(e))
    #                     time.sleep(15)
    #                     continue
    #                 else:
    #                     info = traceback.format_exc()
    #                     alert.send('index_weekly','函数异常',str(info))
                        
    #                     print('index_weekly'+"\n"+info)
    #                     break      
                    
    
    # @tsMonitor
    # def index_monthly(pro,db):
    #     data=tsSHelper.getAllAStockIndex(pro,db)
    #     index_list=data['ts_code'].tolist()
    #     for ts_code in index_list:
    #         lastdate=tsSHelper.getLastDateAndDelete('astock_index_monthly','trade_date',ts_code=ts_code,db=db)
    #         engine=DB.get_db_engine(db)   
    #         today = datetime.datetime.now()
    #         today=today.strftime("%Y%m%d")
    #         #print(ts_code)
    #         while True:
    #             try:
    #                 df=pro.index_monthly(ts_code=ts_code, start_date=lastdate, end_date=today)
    #                 if(not df.empty):
    #                     res = df.to_sql('astock_index_monthly', engine, index=False, if_exists='append', chunksize=5000)
    #                     #print(df)
    #                 break
    #             except Exception as e:
    #                 if "最多访问" in str(e):
    #                     print('index_daily'+":触发限流，等待重试。\n"+str(e))
    #                     time.sleep(15)
    #                     continue
    #                 else:
    #                     info = traceback.format_exc()
    #                     alert.send('index_monthly','函数异常',str(info))
                        
    #                     print('index_monthly'+"\n"+info)
    #                     break      
    
    @tsMonitor
    def index_weight(pro,db):
        table="astock_index_weight"
        # 不需要获取engine对象，直接使用db连接名
        # engine = DB.get_db_engine(db)
        #DB.truncate_table('astock_index_weight',db)
        data=tsSHelper.getAllAStockIndex(pro,db)
        #index_list=data['ts_code'].tolist()
        index_list=['000001.SH','000300.SH','000852.SH','000905.SH']
        for ts_code in index_list:
            try_times=0
            
            today = datetime.datetime.now()
            today=today.strftime("%Y%m%d")
            lastdate=tsSHelper.getLastDateAndDelete('astock_index_weight','trade_date',ts_code=ts_code,db=db)   
            if lastdate<"20120101":
                lastdate="20120101"
            date_range=tsAStockIndex.get_date_range(lastdate,today)
            
            for dt in date_range:
                while True:
                    try:
                        #Log.logger.debug(dt)
                        df = pro.index_weight(index_code=ts_code,start_date=dt, end_date=dt)
                        df = df.rename({'index_code':'ts_code'}, axis='columns')
                        #df.to_sql('astock_index_weight', engine, index=False, if_exists='append', chunksize=5000)
                        DB.safe_to_sql(df, table, db, index=False, if_exists='append', chunksize=5000)
                        break
                    except Exception as e:
                        if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                            Log.logger.warning("index_weight:触发最多访问。\n"+str(e)) 
                            return
                        if "最多访问" in str(e):
                            Log.logger.warning("index_weight:触发限流，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:
                            if try_times<10:
                                try_times=try_times+1;
                                Log.logger.error("index_weight:函数异常，等待重试。\n"+str(e))
                                time.sleep(15)
                                continue
                            else:                    
                                info = traceback.format_exc()
                                alert.send('index_weight','函数异常',str(info))
                                Log.logger.error(info)    
        
 
    
    # @tsMonitor
    # def index_dailybasic(pro,db):
    #     engine=DB.get_db_engine(db)
    #     #DB.truncate_table('astock_index_weight',db)
    #     data=tsSHelper.getAllAStockIndex(pro,db)
    #     index_list=['000001.SH','000300.SH ','000905.SH','399001.SZ','399005.SZ','399006.SZ','399016.SZ ','399300.SZ']
    #     for ts_code in index_list:
    #         while True:
    #             try:
    #                 today = datetime.datetime.now()
    #                 today=today.strftime("%Y%m%d")
    #                 lastdate=tsSHelper.getLastDateAndDelete('astock_index_dailybasic','trade_date',ts_code=ts_code,db=db)
    #                 df = pro.index_dailybasic(ts_code=ts_code,start_date=lastdate, end_date=today)
    #                 df.to_sql('astock_index_dailybasic', engine, index=False, if_exists='append', chunksize=5000)
    #                 break
    #             except Exception as e:
    #                 if "最多访问" in str(e):
    #                     print("index_dailybasic:触发限流，等待重试。\n"+str(e))
    #                     time.sleep(15)
    #                     continue
    #                 else:
    #                     info = traceback.format_exc()
    #                     alert.send('index_dailybasic','函数异常',str(info))
    #                     print(info)  
    
    # @tsMonitor
    # def index_classify(pro,db):
    #     DB.truncate_table('astock_index_classify',db)
    #     engine=DB.get_db_engine(db)
    #     #获取申万一级行业列表
    #     df = pro.index_classify(level='L1', src='SW2021')
    #     df.to_sql('astock_index_classify', engine, index=False, if_exists='append', chunksize=5000)
    #     #获取申万二级行业列表
    #     df = pro.index_classify(level='L2', src='SW2021')
    #     df.to_sql('astock_index_classify', engine, index=False, if_exists='append', chunksize=5000)
    #     #获取申万三级级行业列表
    #     df = pro.index_classify(level='L3', src='SW2021')
    #     df.to_sql('astock_index_classify', engine, index=False, if_exists='append', chunksize=5000)


    
    @tsMonitor
    def index_member(pro,db):
        table='astock_index_member'
        # 安全地删除临时表（如果存在）
        try:
            adapter = DB.get_adapter(db)
            if adapter.table_exists(f"{table}_tmp"):
                DB.exec("drop table if exists "+table+"_tmp",db)
                Log.logger.debug(f"已删除临时表 {table}_tmp")
        except Exception as e:
            Log.logger.warning(f"删除临时表 {table}_tmp 时出错: {str(e)}")
        # 不需要获取engine对象，直接使用db连接名
        # engine = DB.get_db_engine(db)
        sql='select * from astock_index_classify'
        data=DB.select_to_df(sql,db)

        index_code_list=data['index_code'].to_list()
        for index_code in index_code_list:
            try_times=0
            while True:
                try:
                    df = pro.index_member(index_code=index_code,fileds="index_code,index_name,con_code,con_name,in_date,out_date,is_new")
                    df = df.rename({'is_new':'isnew'}, axis='columns')
                    if(not df.empty):
                        #df.to_sql('astock_index_member_tmp', engine, index=False, if_exists='append', chunksize=5000)
                        DB.safe_to_sql(df, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
                    break
                except Exception as e:
                    if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning(self.func.__name__+":触发最多访问。\n"+str(e)) 
                        return
                    if "最多访问" in str(e):
                        Log.logger.warning("astock_index_member:触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    else:
                        if try_times<10:
                            try_times=try_times+1;
                            Log.logger.error("astock_index_member:函数异常，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:                        
                            info = traceback.format_exc()
                            alert.send('astock_index_member','函数异常',str(info))
                            Log.logger.error(info)  

        # 使用统一的replace_table方法替换表，该方法会检查表是否存在
        table_to_use = DB.replace_table(table, table+"_tmp", db)
        if table_to_use != table:
            Log.logger.warning(f"表替换可能未完全成功，当前使用表: {table_to_use}")
        tsSHelper.setIndex(table,db)
    
    # @tsMonitor
    # def daily_info(pro,db):
    #     engine=DB.get_db_engine(db)
    #     if True:
    #         while True:
    #             try:
    #                 today = datetime.datetime.now()
    #                 today=today.strftime("%Y%m%d")
    #                 lastdate=tsSHelper.getLastDateAndDelete('astock_index_daily_info','trade_date',ts_code='',db=db)
    #                 df =pro.daily_info(start_date=lastdate, end_date=today)
    #                 df.to_sql('astock_index_daily_info', engine, index=False, if_exists='append', chunksize=5000)
    #                 break
    #             except Exception as e:
    #                 if "最多访问" in str(e):
    #                     print("index_daily_info:触发限流，等待重试。\n"+str(e))
    #                     time.sleep(15)
    #                     continue
    #                 else:
    #                     info = traceback.format_exc()
    #                     alert.send('index_daily_info','函数异常',str(info))
    #                     print(info)  
    
    # @tsMonitor
    # def sz_daily_info(pro,db):
    #     engine=DB.get_db_engine(db)
    #     if True:
    #         while True:
    #             try:
    #                 today = datetime.datetime.now()
    #                 today=today.strftime("%Y%m%d")
    #                 lastdate=tsSHelper.getLastDateAndDelete('astock_index_sz_daily_info','trade_date',ts_code='',db=db)
    #                 df =pro.sz_daily_info(start_date=lastdate, end_date=today)
    #                 df.to_sql('astock_index_sz_daily_info', engine, index=False, if_exists='append', chunksize=5000)
    #                 break
    #             except Exception as e:
    #                 if "最多访问" in str(e):
    #                     print("index_sz_daily_info:触发限流，等待重试。\n"+str(e))
    #                     time.sleep(15)
    #                     continue
    #                 else:
    #                     info = traceback.format_exc()
    #                     alert.send('index_sz_daily_info','函数异常',str(info))
    #                     print(info)  
    
    # @tsMonitor
    # def ths_daily(pro,db):
    #     pass
    #     #tsSHelper.getDataWithLastDate(pro,'ths_daily','astock_index_ths_daily',db)
    