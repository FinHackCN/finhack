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

class tsFund:
    @tsMonitor
    def fund_basic(pro, db):
        try:
            table='fund_basic'
            # 安全地删除临时表（如果存在）
            try:
                adapter = DB.get_adapter(db)
                if adapter.table_exists(f"{table}_tmp"):
                    DB.exec("drop table if exists "+table+"_tmp", db)
                    Log.logger.debug(f"已删除临时表 {table}_tmp")
            except Exception as e:
                Log.logger.warning(f"删除临时表 {table}_tmp 时出错: {str(e)}")
            
            # 交易所场内基金
            data=pro.fund_basic(market='E', status='D')
            # 预处理数据，确保字段为字符串类型
            for col in data.columns:
                if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                   'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                    data[col] = data[col].astype(str)
            DB.safe_to_sql(data, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
            
            # 获取其他类型的基金
            fund_markets = [('E', 'I'), ('E', 'L'), ('O', 'D'), ('O', 'I'), ('O', 'L')]
            for market, status in fund_markets:
                data = pro.fund_basic(market=market, status=status)
                # 预处理数据，确保字段为字符串类型
                for col in data.columns:
                    if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                       'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                        data[col] = data[col].astype(str)
                DB.safe_to_sql(data, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
            
            # 使用统一的replace_table方法替换表
            table_to_use = DB.replace_table(table, table+"_tmp", db)
            
            tsSHelper.setIndex(table_to_use, db)
            return True
        except Exception as e:
            Log.logger.error(f"获取基金基本信息失败: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False
    
    @tsMonitor
    def fund_company(pro,db):
        tsSHelper.getDataAndReplace(pro,'fund_company','fund_company',db)

    @tsMonitor
    def fund_manager(pro, db):
        """
        获取基金经理信息（带数据校验保护）

        修复：添加数据量校验，防止空数据或不完整数据覆盖原表
        """
        try:
            table='fund_manager'

            # 获取原表记录数（用于数据校验）
            adapter = DB.get_adapter(db)
            old_count = 0
            try:
                if adapter.table_exists(table):
                    result = DB.select_to_list(f"SELECT COUNT(*) as cnt FROM {table}", db)
                    old_count = result[0]['cnt'] if result else 0
                    Log.logger.info(f"fund_manager: 原表有 {old_count} 条记录")
            except Exception as count_error:
                Log.logger.warning(f"fund_manager: 无法获取原表记录数: {str(count_error)}")

            DB.exec("drop table if exists "+table+"_tmp", db)
            data=tsSHelper.getAllFund(db)

            if data is None or data.empty:
                Log.logger.error("fund_manager: 获取基金列表失败")
                return False

            fund_list=data['ts_code'].tolist()
            Log.logger.info(f"fund_manager: 共获取到{len(fund_list)}只基金")

            total_records = 0
            processed = 0

            for i in range(0, len(fund_list), 100):
                code_list=fund_list[i:i+100]
                try_times=0
                while True:
                    try:
                        df = pro.fund_manager(ts_code=','.join(code_list))

                        if df is not None and not df.empty:
                            # 预处理数据，确保字段为字符串类型
                            for col in df.columns:
                                if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
                                   'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
                                    df[col] = df[col].astype(str)
                            DB.safe_to_sql(df, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
                            total_records += len(df)

                        processed += len(code_list)
                        if processed % 1000 == 0:
                            Log.logger.info(f"fund_manager: 已处理 {processed}/{len(fund_list)} 只基金，累计 {total_records} 条记录")

                        break
                    except Exception as e:
                        if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                            Log.logger.warning("fund_manager:触发最多访问。\n"+str(e))
                            # 检查临时表数据量，如果足够则保留
                            if total_records > 0 and (old_count == 0 or total_records >= old_count * 0.5):
                                Log.logger.warning(f"fund_manager: 虽然触发限流，但已获取{total_records}条记录，尝试保留")
                                break
                            return
                        if "最多访问" in str(e):
                            Log.logger.warning("fund_manager:触发限流，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:
                            if try_times<10:
                                try_times=try_times+1
                                Log.logger.error("fund_manager:函数异常，等待重试。\n"+str(e))
                                time.sleep(15)
                                continue
                            else:
                                info = traceback.format_exc()
                                alert.send('fund_manager','函数异常',str(info))
                                Log.logger.error(info)
                                break

            # 数据校验：检查临时表数据量是否合理
            Log.logger.info(f"fund_manager: 共获取 {total_records} 条记录，原表有 {old_count} 条记录")

            if total_records == 0:
                Log.logger.error("fund_manager: 未获取到任何数据，保留原表不变")
                return False

            # 如果原表有数据，且新数据量远少于原数据（少于50%），拒绝替换
            if old_count > 0 and total_records < old_count * 0.5:
                Log.logger.error(f"fund_manager: 新数据量({total_records})远少于原数据量({old_count})，保留原表不变")
                return False

            # 使用统一的replace_table方法替换表
            table_to_use = DB.replace_table(table, table+"_tmp", db)

            tsSHelper.setIndex(table_to_use, db)
            Log.logger.info(f"fund_manager: 数据同步完成，共{total_records}条记录")
            return True
        except Exception as e:
            Log.logger.error(f"获取基金经理信息失败: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False
    
    @tsMonitor
    def fund_share(pro, db):
        #try:
        table='fund_share'
        tsSHelper.getDataWithLastDate(pro,'fund_share','fund_share',db,'trade_date')
        #     DB.exec("drop table if exists "+table+"_tmp", db)
        #     data=tsSHelper.getAllFund(db)
        #     fund_list=data['ts_code'].tolist()
            
        #     for i in range(0, len(fund_list), 100):
        #         code_list=fund_list[i:i+100]
        #         for ts_code in code_list:
        #             try_times=0
        #             while True:
        #                 try:
        #                     df = pro.fund_share(ts_code=','.join(code_list))
        #                     # 预处理数据，确保字段为字符串类型
        #                     for col in df.columns:
        #                         if col in ['ts_code', 'symbol', 'code', 'ann_date', 'end_date', 'trade_date', 'pre_date', 'actual_date'] or \
        #                            'code' in col.lower() or 'symbol' in col.lower() or 'date' in col.lower():
        #                             df[col] = df[col].astype(str)
        #                     DB.safe_to_sql(df, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
        #                     break
        #                 except Exception as e:
        #                     if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
        #                         Log.logger.warning("fund_share:触发最多访问。\n"+str(e)) 
        #                         return
        #                     if "最多访问" in str(e):
        #                         Log.logger.warning("fund_share:触发限流，等待重试。\n"+str(e))
        #                         time.sleep(15)
        #                         continue
        #                     else:
        #                         if try_times<10:
        #                             try_times=try_times+1
        #                             Log.logger.error("fund_share:函数异常，等待重试。\n"+str(e))
        #                             time.sleep(15)
        #                             continue
        #                         else:                            
        #                             info = traceback.format_exc()
        #                             alert.send('fund_share','函数异常',str(info))
        #                             Log.logger.error(info)
        #                             break
            
        #     # 使用统一的replace_table方法替换表
        #     table_to_use = DB.replace_table(table, table+"_tmp", db)
            
        #     tsSHelper.setIndex(table_to_use, db)
        #     return True
        # except Exception as e:
        #     Log.logger.error(f"获取基金份额信息失败: {str(e)}")
        #     Log.logger.error(traceback.format_exc())
        #     return False
    
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