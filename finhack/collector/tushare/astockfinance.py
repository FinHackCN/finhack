import sys
import time
import datetime
import traceback
import pandas as pd
import threading
import pickle
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import functools
from finhack.library.db import DB
from finhack.library.alert import alert
from finhack.library.monitor import tsMonitor
from finhack.collector.tushare.helper import tsSHelper
import finhack.library.log as Log

# 导入常量定义
try:
    from runtime.constant import CACHE_DIR
except ImportError:
    # 如果导入失败，使用相对路径
    CACHE_DIR = "data/cache/"

# 全局表状态管理器
class TableStateManager:
    """线程安全的表状态管理器"""
    
    def __init__(self):
        self._table_status = {}  # {db_name: {table_name: {'exists': bool, 'creating': bool}}}
        self._lock = threading.RLock()  # 使用可重入锁
        
    def is_table_exists(self, db, table_name):
        """检查表是否存在"""
        with self._lock:
            if db not in self._table_status:
                self._table_status[db] = {}
            
            table_key = table_name
            if table_key not in self._table_status[db]:
                # 首次检查，查询数据库
                adapter = DB.get_adapter(db)
                exists = adapter.table_exists(table_name)
                self._table_status[db][table_key] = {
                    'exists': exists,
                    'creating': False
                }
                Log.logger.debug(f"首次检查表 {table_name}，存在状态: {exists}")
                return exists
            else:
                # 已有缓存状态
                status = self._table_status[db][table_key]
                return status['exists']
    
    def mark_table_creating(self, db, table_name):
        """标记表正在创建中，返回是否成功获取创建权限"""
        with self._lock:
            if db not in self._table_status:
                self._table_status[db] = {}
            
            table_key = table_name
            
            # 检查是否已经在创建中
            if table_key in self._table_status[db]:
                current_status = self._table_status[db][table_key]
                if current_status.get('creating', False):
                    Log.logger.debug(f"表 {table_name} 已由其他线程标记为创建中，当前线程等待")
                    return False  # 其他线程已经在创建
                elif current_status.get('exists', False):
                    Log.logger.debug(f"表 {table_name} 已存在，无需创建")
                    return False  # 表已存在
            
            # 设置创建中状态
            self._table_status[db][table_key] = {'exists': False, 'creating': True}
            Log.logger.debug(f"成功标记表 {table_name} 为创建中状态")
            return True  # 成功获取创建权限
    
    def mark_table_created(self, db, table_name):
        """标记表已创建完成"""
        with self._lock:
            if db not in self._table_status:
                self._table_status[db] = {}
            
            table_key = table_name
            self._table_status[db][table_key] = {'exists': True, 'creating': False}
            Log.logger.info(f"表 {table_name} 已创建完成")
    
    def is_table_creating(self, db, table_name):
        """检查表是否正在创建中"""
        with self._lock:
            if db not in self._table_status:
                return False
            
            table_key = table_name
            if table_key not in self._table_status[db]:
                return False
            
            return self._table_status[db][table_key].get('creating', False)
    
    def wait_for_table_creation(self, db, table_name, timeout=300):
        """等待表创建完成"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if not self.is_table_creating(db, table_name):
                return self.is_table_exists(db, table_name)
            time.sleep(1)  # 等待1秒后重试
        
        Log.logger.warning(f"等待表 {table_name} 创建超时")
        return False
    
    def clear_cache(self, db=None, table_name=None):
        """清除缓存"""
        with self._lock:
            if db is None:
                self._table_status.clear()
                Log.logger.debug("清除所有表状态缓存")
            elif table_name is None:
                if db in self._table_status:
                    self._table_status[db].clear()
                    Log.logger.debug(f"清除数据库 {db} 的所有表状态缓存")
            else:
                if db in self._table_status and table_name in self._table_status[db]:
                    del self._table_status[db][table_name]
                    Log.logger.debug(f"清除表 {table_name} 的状态缓存")

# 全局表状态管理器实例
table_state_manager = TableStateManager()

class tsAStockFinance:
    
    @staticmethod
    def _ensure_disclosure_table(pro, db):
        """
        确保公告表存在并有数据，这是所有财务数据的基础
        """
        adapter = DB.get_adapter(db)
        disclosure_table_exists = adapter.table_exists('astock_finance_disclosure_date')
        
        if not disclosure_table_exists:
            Log.logger.info("公告表不存在，自动创建公告表并获取数据...")
            try:
                # 调用 disclosure_date 方法创建和填充公告表
                tsAStockFinance.disclosure_date(pro, db)
                Log.logger.info("公告表创建完成")
            except Exception as e:
                Log.logger.error(f"创建公告表失败: {str(e)}")
                raise
        else:
            Log.logger.debug("公告表已存在")
    
    def getPeriodList(db):
        lastdate_sql="select max(end_date) as max from astock_finance_disclosure_date"
        lastdate=DB.select_to_df(lastdate_sql,db)
        if(type(lastdate) == bool or lastdate.empty):
            lastdate='19980321'            
        else:
            lastdate=lastdate['max'].tolist()[0]
        
        
        plist=[]
        end_date_list=['0331','0630','0930','1231']
        end_year=time.strftime("%Y", time.localtime())
        for i in range(1999,int(end_year)+1):
            for d in end_date_list:
                #p=end_year+str(d)
                p=str(i)+d  #这里是被Claude修改的
                if p<lastdate:
                    plist.append(p)
        return plist

    def getEndDateListDiff(table,ts_code,db,report_type=0):
        """
        获取需要更新的end_date列表，支持线程安全的表状态管理
        确保公告表存在，如果目标表不存在则获取全量数据
        """
        # 使用全局表状态管理器检查表状态
        if table_state_manager.is_table_creating(db, table):
            Log.logger.info(f"表 {table} 正在创建中，等待创建完成...")
            table_exists = table_state_manager.wait_for_table_creation(db, table)
        else:
            table_exists = table_state_manager.is_table_exists(db, table)
        
        table_list = []
        disclosure_list = []
        
        # 首先检查公告表是否存在
        adapter = DB.get_adapter(db)
        disclosure_table_exists = adapter.table_exists('astock_finance_disclosure_date')
        
        # 如果目标表存在，查询已有数据
        if table_exists:
            try:
                table_sql="select end_date from "+table+" where ts_code='"+ts_code+"'"
                if(report_type>0):
                    table_sql=table_sql+" and report_type="+str(report_type)
                table_df=DB.select_to_df(table_sql,db)
                if(type(table_df) != bool and not table_df.empty):
                    table_list=table_df['end_date'].unique().tolist()
            except Exception as e:
                Log.logger.warning(f"查询目标表 {table} 数据时出错: {str(e)}")
        else:
            Log.logger.info(f"表 {table} 不存在，将获取全量数据")
        
        # 查询披露日期数据（如果公告表存在）
        if disclosure_table_exists:
            try:
                disclosure_sql="select end_date from astock_finance_disclosure_date where ts_code='"+ts_code+"'  and not ISNULL(actual_date)"
                disclosure_df=DB.select_to_df(disclosure_sql,db)
                if(type(disclosure_df) != bool and not disclosure_df.empty):
                    disclosure_list=disclosure_df['end_date'].unique().tolist()
            except Exception as e:
                Log.logger.warning(f"查询公告表数据时出错: {str(e)}")
        else:
            Log.logger.warning(f"公告表 astock_finance_disclosure_date 不存在，将使用全量期间列表")
        
        # 决定使用全量数据的条件：
        # 1. 目标表不存在
        # 2. 目标表存在但没有数据
        # 3. 公告表不存在或没有披露数据
        if not table_exists or len(table_list) == 0 or not disclosure_table_exists or len(disclosure_list) == 0:
            Log.logger.info(f"触发全量数据获取条件 - 目标表存在:{table_exists}, 目标表数据:{len(table_list)}, 公告表存在:{disclosure_table_exists}, 公告数据:{len(disclosure_list)}")
            disclosure_list=tsAStockFinance.getPeriodList(db)
            
        diff_list = set(disclosure_list)-set(table_list)
        diff_list=list(diff_list)
        diff_list.sort()
        
        Log.logger.debug(f"股票 {ts_code} 在表 {table} 需要更新的期间: {len(diff_list)} 个")
        return diff_list
        
        
    #比较数据完整性: 这个方法比较两个数据源中记录的数量：
    #table_count: 目标财务数据表中已存在的记录数量
    #disclosure_count: 财务披露日期表中应该有的记录数量
    #如果table_count小于disclosure_count，则返回True，表示需要获取更多数据
    def getLastDateCountDiff(table,end_date,ts_code,db,report_type=0):
        """
        比较表中数据数量与披露数据数量，支持线程安全的表状态管理
        """
        # 使用全局表状态管理器检查表状态
        if table_state_manager.is_table_creating(db, table):
            Log.logger.debug(f"表 {table} 正在创建中，等待创建完成...")
            table_exists = table_state_manager.wait_for_table_creation(db, table)
        else:
            table_exists = table_state_manager.is_table_exists(db, table)
        
        table_count = 0
        if table_exists:
            table_sql="select * from "+table+" where ts_code='"+ts_code+"' and end_date='"+end_date+"'"
            if(report_type>0):
                table_sql=table_sql+" and report_type="+str(report_type)
            table_res=DB.select_to_df(table_sql,db)
            if type(table_res) == bool:
                table_count=0
            else:
                table_count=len(table_res)
        else:
            # 表不存在，计数为0
            table_count = 0
            
        disclosure_sql="select * from astock_finance_disclosure_date where ts_code='"+ts_code+"' and end_date='"+end_date+"' and not ISNULL(actual_date)"
        disclosure_res=DB.select_to_df(disclosure_sql,db)
        disclosure_count=len(disclosure_res)
        #print(str(table_count)+","+str(disclosure_count)+","+ts_code+","+str(report_type))
        return table_count<disclosure_count

   
    def getFinanceStockList(pro, db, table, report_type=0):
        def check_stock(ts_code):
            #Log.logger.info(f"检查股票{ts_code}在{table}-{report_type}是否需要更新")
            """检查单个股票是否需要更新"""
            try:
                diff_list = tsAStockFinance.getEndDateListDiff(table, ts_code, db, report_type)
                if diff_list and all(date < '20010101' for date in diff_list):
                    #Log.logger.debug(f"跳过{ts_code}在{api}的更新，{diff_list}")
                    return None  # 返回None表示跳过

                # 使用全局表状态管理器检查表状态
                if table_state_manager.is_table_creating(db, table):
                    Log.logger.debug(f"表 {table} 正在创建中，等待创建完成...")
                    table_exists = table_state_manager.wait_for_table_creation(db, table)
                else:
                    table_exists = table_state_manager.is_table_exists(db, table)
                
                lastdate = '20000321'  # 默认值
                if table_exists:
                    lastdate_sql="select max(end_date) as max from "+table+" where ts_code='"+ts_code+"'"
                    if(report_type>0):
                        lastdate_sql=lastdate_sql+" and report_type="+str(report_type)
                    lastdate_df=DB.select_to_df(lastdate_sql,db)
                    if(type(lastdate_df) != bool and not lastdate_df.empty):
                        lastdate_value = lastdate_df['max'].tolist()[0]
                        if lastdate_value is not None:
                            lastdate = lastdate_value
                
                diff_count=tsAStockFinance.getLastDateCountDiff(table,lastdate,ts_code,db,report_type)
                end_list=[]
                for end_date in diff_list:
                    if(lastdate>end_date):
                        continue
                    end_list.append(end_date)
                if len(end_list)>0:
                    return ts_code  # 返回股票代码表示需要处理
                else:
                    return None
            except Exception as e:
                Log.logger.error(f"检查股票{ts_code}时出错: {str(e)}")
                return None
        
        stock_list_data = tsSHelper.getAllAStock(True, pro, db)
        all_stock_list = stock_list_data['ts_code'].tolist()
        thread_count=3
        Log.logger.info(f"开始使用{thread_count}个线程筛选{table}-{report_type}需要更新的股票，总数: {len(all_stock_list)}")
        
        return_list = []
        
        # 使用10个线程并行处理股票筛选
        with ThreadPoolExecutor(max_workers=thread_count, thread_name_prefix="StockFilter") as executor:
            # 提交所有股票检查任务
            futures = {executor.submit(check_stock, ts_code): ts_code for ts_code in all_stock_list}
            
            # 收集结果
            for future in as_completed(futures):
                ts_code = futures[future]
                try:
                    result = future.result()
                    if result is not None:  # 如果返回的不是None，说明需要处理
                        return_list.append(result)
                except Exception as e:
                    Log.logger.error(f"处理股票{ts_code}时出现异常: {str(e)}")
        
        Log.logger.info(f"{table}-{report_type}筛选完成，需要更新的股票数量: {len(return_list)}/{len(all_stock_list)}")
        print(return_list)

        
        return return_list



    def getFinance(pro,api,table,fileds,db,report_type=0):
        """
        获取财务数据，支持线程安全的表创建管理
        确保公告表优先存在，支持自动创建所有必要的表
        """
        # 首先确保公告表存在（这是所有财务数据的基础）
        adapter = DB.get_adapter(db)
        disclosure_table_exists = adapter.table_exists('astock_finance_disclosure_date')
        
        if not disclosure_table_exists:
            Log.logger.warning(f"{api} - 公告表不存在，需要先创建公告表")
            Log.logger.info(f"{api} - 建议先运行 disclosure_date 获取公告数据")
        
        # 检查表状态，决定是否需要等待或创建
        if table_state_manager.is_table_creating(db, table):
            Log.logger.info(f"{api} - 表 {table} 正在创建中，等待创建完成...")
            table_exists = table_state_manager.wait_for_table_creation(db, table)
            if not table_exists:
                Log.logger.error(f"{api} - 表 {table} 创建失败，跳过处理")
                return
            is_table_creator = False
        elif not table_state_manager.is_table_exists(db, table):
            # 表不存在，尝试获取创建权限
            got_create_permission = table_state_manager.mark_table_creating(db, table)
            if got_create_permission:
                Log.logger.info(f"{api} - 表 {table} 不存在，当前线程获得创建权限")
                is_table_creator = True
            else:
                # 其他线程已在创建，等待完成
                Log.logger.info(f"{api} - 表 {table} 正在被其他线程创建，等待完成...")
                table_exists = table_state_manager.wait_for_table_creation(db, table)
                if not table_exists:
                    Log.logger.error(f"{api} - 表 {table} 创建失败，跳过处理")
                    return
                is_table_creator = False
        else:
            # 表已存在，进行增量更新
            Log.logger.debug(f"{api} - 表 {table} 已存在，进行增量更新")
            is_table_creator = False
        
        # 使用多线程筛选需要处理的股票列表
        stock_list = tsAStockFinance.getFinanceStockList(pro, db, table, report_type)
        
        # 如果没有需要处理的股票，可能是因为缺少公告数据
        if len(stock_list) == 0:
            if not disclosure_table_exists:
                Log.logger.warning(f"{api} - 没有找到需要处理的股票，可能是因为缺少公告表数据")
                Log.logger.info(f"{api} - 请先运行: tsAStockFinance.disclosure_date(pro, '{db}')")
            else:
                Log.logger.info(f"{api} - 所有数据都是最新的，无需更新")
            
            # 如果是表创建者但没有数据要处理，清除创建中状态
            if 'is_table_creator' in locals() and is_table_creator:
                with table_state_manager._lock:
                    if db in table_state_manager._table_status and table in table_state_manager._table_status[db]:
                        table_state_manager._table_status[db][table] = {'exists': False, 'creating': False}
                Log.logger.debug(f"{api} - 清除表 {table} 的创建中状态")
            return
        
        Log.logger.info(f"{api} - 开始处理 {len(stock_list)} 只股票，report_type={report_type}")
        
        # 用于跟踪是否已成功写入第一批数据（仅当是表创建者时使用）
        first_write_success = False
        
        for ts_code in stock_list:
            if api in ['disclosure_date','fina_indicator'] and report_type!=0:
                continue
            if report_type>0:
                Log.logger.info(api+","+ts_code+",report_type="+str(report_type))
            diff_list=tsAStockFinance.getEndDateListDiff(table,ts_code,db,report_type)

            # 如果diff_list中的所有元素都小于'20010101'，则跳过当前股票
            if diff_list and all(date < '20010101' for date in diff_list):
                #Log.logger.info(f"跳过{ts_code}在{api}的更新，{diff_list}")
                continue
            
            # 使用全局表状态管理器检查表状态，避免查询不存在的表
            if table_state_manager.is_table_creating(db, table):
                Log.logger.debug(f"表 {table} 正在创建中，等待创建完成...")
                table_exists = table_state_manager.wait_for_table_creation(db, table)
            else:
                table_exists = table_state_manager.is_table_exists(db, table)
            
            lastdate = '20000321'  # 默认值
            if table_exists:
                lastdate_sql="select max(end_date) as max from "+table+" where ts_code='"+ts_code+"'"
                if(report_type>0):
                    lastdate_sql=lastdate_sql+" and report_type="+str(report_type)
                lastdate_df=DB.select_to_df(lastdate_sql,db)
                if(type(lastdate_df) != bool and not lastdate_df.empty):
                    lastdate_value = lastdate_df['max'].tolist()[0]
                    if lastdate_value is not None:
                        lastdate = lastdate_value
            diff_count=tsAStockFinance.getLastDateCountDiff(table,lastdate,ts_code,db,report_type)




            if(diff_count):
                # 检查表是否存在再执行删除操作
                if table_state_manager.is_table_exists(db, table):
                    sql="delete from "+table+" where ts_code='"+ts_code+"' and end_date='"+lastdate+"'"
                    if(report_type>0):
                        sql=sql+" and report_type="+str(report_type)
                    try:
                        DB.delete(sql,db)
                        Log.logger.debug(f"已删除表 {table} 中 {ts_code} 在 {lastdate} 的数据")
                    except Exception as delete_error:
                        Log.logger.warning(f"删除表 {table} 中 {ts_code} 在 {lastdate} 的数据时出错: {str(delete_error)}")
                else:
                    Log.logger.debug(f"表 {table} 不存在，跳过删除操作")
                diff_list.insert(0,lastdate)
            
            #print(diff_count)
            # print(lastdate)
            #exit()
            
            df=pd.DataFrame()
            # 不需要获取engine对象，直接使用db连接名
            # engine=DB.get_db_engine(db)
            


            
            end_list=[]
            for end_date in diff_list:
                if(lastdate>end_date):
                    continue
                end_list.append(end_date)
                
            if end_list==[]:
                continue
            f = getattr(pro, api)
            try_times=0

            # print(ts_code)
            # print("diff_list:",diff_list)
            # print("lastdate:",lastdate)
            # print("diff_count:",diff_count)
            # print("end_list:",end_list)
            # exit()
            while True:
                try:
                    # print(lastdate)
                    # print(end_date)
                    # print(end_list)
                    # exit()
                    if report_type>0:
                        if len(end_list)>1:
                            df=f(ts_code=ts_code,start_date=end_list[0],end_date=datetime.datetime.now().strftime('%Y%m%d'),fileds=fileds,report_type=report_type)
                        else:
                            df=f(ts_code=ts_code,period=end_list[-1],fileds=fileds,report_type=report_type)
                    #有些api没有report type字段
                    else:
                        if len(end_list)>1:
                            df=f(ts_code=ts_code,start_date=end_list[0],end_date=datetime.datetime.now().strftime('%Y%m%d'),fileds=fileds)
                        else:
                            df=f(ts_code=ts_code,period=end_list[-1],fileds=fileds)
                    # 使用db连接名代替engine对象
                    # 使用动态chunksize，避免SQLite "too many SQL variables"错误
                    DB.safe_to_sql(df, table, db, index=False, if_exists='append')
                    
                    # 如果是表创建者且首次写入成功，标记表已创建完成
                    if 'is_table_creator' in locals() and is_table_creator and not first_write_success:
                        table_state_manager.mark_table_created(db, table)
                        first_write_success = True
                        Log.logger.info(f"{api} - 表 {table} 首次写入成功，标记为已创建")
                    
                    break
                except Exception as e:
                    if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning("api:触发最多访问。\n"+str(e)) 
                        return
                    if "最多访问" in str(e):
                        Log.logger.warning(api+":触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    elif "未知错误" in str(e):
                        info = traceback.format_exc()
                        Log.logger.error(ts_code)
                        #Log.logger.error(period)
                        Log.logger.error(fileds)
                        Log.logger.error(report_type)
                        alert.send(api,'未知错误',str(info))
                        Log.logger.error(info)
                        break
                    else:
                        if try_times<10:
                            try_times=try_times+1;
                            Log.logger.error(api+":函数异常，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:
                            info = traceback.format_exc()
                            alert.send(api,'函数异常',str(info))
                            Log.logger.error(info)
                            
                            # 如果是表创建者且出现严重错误，清除创建中状态
                            if 'is_table_creator' in locals() and is_table_creator and not first_write_success:
                                with table_state_manager._lock:
                                    if db in table_state_manager._table_status and table in table_state_manager._table_status[db]:
                                        table_state_manager._table_status[db][table] = {'exists': False, 'creating': False}
                                Log.logger.warning(f"{api} - 表 {table} 创建失败，清除创建中状态")
                            
                            break
     
    
    @tsMonitor
    def disclosure_date(pro,db):
        try:
            table = 'astock_finance_disclosure_date'
            
            # 使用SQLite兼容的方法检查表是否存在
            adapter = DB.get_adapter(db)
            table_exists = adapter.table_exists(table)
            if not table_exists:
                Log.logger.info(f"创建表 {table}")
            
            # 从此处开始获取数据
            tsSHelper.getDataAndReplace(pro, 'disclosure_date', table, db)
            return True
        except Exception as e:
            Log.logger.error(f"获取财务披露日期失败: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False
    
    @tsMonitor
    def income(pro,db):
        # 首先确保公告表存在
        tsAStockFinance._ensure_disclosure_table(pro, db)
        
        fileds="ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit,rd_exp,fin_exp_int_exp,fin_exp_int_inc,transfer_surplus_rese,transfer_housing_imprest,transfer_oth,adj_lossgain,withdra_legal_surplus,withdra_legal_pubfund,withdra_biz_devfund,withdra_rese_fund,withdra_oth_ersu,workers_welfare,distr_profit_shrhder,prfshare_payable_dvd,comshare_payable_dvd,capit_comstock_div,net_after_nr_lp_correct,credit_impa_loss,net_expo_hedging_benefits,oth_impair_loss_assets,total_opcost,amodcost_fin_assets,oth_income,asset_disp_income,continued_net_profit,end_net_profit,update_flag"
        
        # 避免多线程竞争条件：序列化处理同一个表的不同report_type
        # 因为两个report_type都写入同一个表 astock_finance_income
        Log.logger.info("开始获取利润表数据（序列化处理避免表创建冲突）")
        
        for report_type in [1, 6]:
            try:
                Log.logger.info(f"开始获取利润表数据，report_type={report_type}")
                tsAStockFinance.getFinance(pro,'income','astock_finance_income',fileds,db,report_type)
                Log.logger.info(f"完成获取利润表数据，report_type={report_type}")
            except Exception as e:
                Log.logger.error(f"获取利润表数据失败，report_type={report_type}: {str(e)}")
                # 继续处理下一个report_type，不中断整个流程
                continue
        
        Log.logger.info("利润表数据获取完成")
    
    @tsMonitor
    def balancesheet(pro,db):
        # 首先确保公告表存在
        tsAStockFinance._ensure_disclosure_table(pro, db)
        
        fileds="ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,total_share,cap_rese,undistr_porfit,surplus_rese,special_rese,money_cap,trad_asset,notes_receiv,accounts_receiv,oth_receiv,prepayment,div_receiv,int_receiv,inventories,amor_exp,nca_within_1y,sett_rsrv,loanto_oth_bank_fi,premium_receiv,reinsur_receiv,reinsur_res_receiv,pur_resale_fa,oth_cur_assets,total_cur_assets,fa_avail_for_sale,htm_invest,lt_eqt_invest,invest_real_estate,time_deposits,oth_assets,lt_rec,fix_assets,cip,const_materials,fixed_assets_disp,produc_bio_assets,oil_and_gas_assets,intan_assets,r_and_d,goodwill,lt_amor_exp,defer_tax_assets,decr_in_disbur,oth_nca,total_nca,cash_reser_cb,depos_in_oth_bfi,prec_metals,deriv_assets,rr_reins_une_prem,rr_reins_outstd_cla,rr_reins_lins_liab,rr_reins_lthins_liab,refund_depos,ph_pledge_loans,refund_cap_depos,indep_acct_assets,client_depos,client_prov,transac_seat_fee,invest_as_receiv,total_assets,lt_borr,st_borr,cb_borr,depos_ib_deposits,loan_oth_bank,trading_fl,notes_payable,acct_payable,adv_receipts,sold_for_repur_fa,comm_payable,payroll_payable,taxes_payable,int_payable,div_payable,oth_payable,acc_exp,deferred_inc,st_bonds_payable,payable_to_reinsurer,rsrv_insur_cont,acting_trading_sec,acting_uw_sec,non_cur_liab_due_1y,oth_cur_liab,total_cur_liab,bond_payable,lt_payable,specific_payables,estimated_liab,defer_tax_liab,defer_inc_non_cur_liab,oth_ncl,total_ncl,depos_oth_bfi,deriv_liab,depos,agency_bus_liab,oth_liab,prem_receiv_adva,depos_received,ph_invest,reser_une_prem,reser_outstd_claims,reser_lins_liab,reser_lthins_liab,indept_acc_liab,pledge_borr,indem_payable,policy_div_payable,total_liab,treasury_share,ordin_risk_reser,forex_differ,invest_loss_unconf,minority_int,total_hldr_eqy_exc_min_int,total_hldr_eqy_inc_min_int,total_liab_hldr_eqy,lt_payroll_payable,oth_comp_income,oth_eqt_tools,oth_eqt_tools_p_shr,lending_funds,acc_receivable,st_fin_payable,payables,hfs_assets,hfs_sales,cost_fin_assets,fair_value_fin_assets,cip_total,oth_pay_total,long_pay_total,debt_invest,oth_debt_invest,oth_eq_invest,oth_illiq_fin_assets,oth_eq_ppbond,receiv_financing,use_right_assets,lease_liab,contract_assets,contract_liab,accounts_receiv_bill,accounts_pay,oth_rcv_total,fix_assets_total,update_flag"
        
        # 避免多线程竞争条件：序列化处理同一个表的不同report_type
        # 因为两个report_type都写入同一个表 astock_finance_balancesheet
        Log.logger.info("开始获取资产负债表数据（序列化处理避免表创建冲突）")
        
        for report_type in [1, 6]:
            try:
                Log.logger.info(f"开始获取资产负债表数据，report_type={report_type}")
                tsAStockFinance.getFinance(pro,'balancesheet','astock_finance_balancesheet',fileds,db,report_type)
                Log.logger.info(f"完成获取资产负债表数据，report_type={report_type}")
            except Exception as e:
                Log.logger.error(f"获取资产负债表数据失败，report_type={report_type}: {str(e)}")
                # 继续处理下一个report_type，不中断整个流程
                continue
        
        Log.logger.info("资产负债表数据获取完成")
    
    @tsMonitor
    def cashflow(pro,db):
        # 首先确保公告表存在
        tsAStockFinance._ensure_disclosure_table(pro, db)
        
        fileds=""
        
        # 避免多线程竞争条件：序列化处理同一个表的不同report_type
        # 因为两个report_type都写入同一个表 astock_finance_cashflow
        Log.logger.info("开始获取现金流量表数据（序列化处理避免表创建冲突）")
        
        for report_type in [1, 6]:
            try:
                Log.logger.info(f"开始获取现金流量表数据，report_type={report_type}")
                tsAStockFinance.getFinance(pro,'cashflow','astock_finance_cashflow',fileds,db,report_type)
                Log.logger.info(f"完成获取现金流量表数据，report_type={report_type}")
            except Exception as e:
                Log.logger.error(f"获取现金流量表数据失败，report_type={report_type}: {str(e)}")
                # 继续处理下一个report_type，不中断整个流程
                continue
        
        Log.logger.info("现金流量表数据获取完成")
    
    @tsMonitor
    def forecast(pro,db):
        # 首先确保公告表存在
        tsAStockFinance._ensure_disclosure_table(pro, db)
        
        fileds=""
        tsAStockFinance.getFinance(pro,'forecast','astock_finance_forecast',fileds,db)
    
    @tsMonitor
    def express(pro,db):
        # 首先确保公告表存在
        tsAStockFinance._ensure_disclosure_table(pro, db)
        
        fileds=""
        tsAStockFinance.getFinance(pro,'express','astock_finance_express',fileds,db)
    
    @tsMonitor
    def dividend(pro,db):
        # 移除引擎对象，使用连接名
        tsSHelper.getDataWithLastDate(pro,'dividend','astock_finance_dividend',db,'ann_date')
        # table='astock_finance_dividend'
        # stock_list_data=tsSHelper.getAllAStock(True,pro,db)
        # stock_list=stock_list_data['ts_code'].tolist()
        # for ts_code in stock_list:
        #     try_times=0
        #     while True:
        #         try:
        #             df = pro.dividend(ts_code=ts_code)
        #             # 使用db连接名代替engine对象
        #             # DB.safe_to_sql(df, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
        #             DB.safe_to_sql(df, table+"_tmp", db, index=False, if_exists='append', chunksize=5000)
        #             break
        #         except Exception as e:
        #             if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
        #                 Log.logger.warning("dividend:触发最多访问。\n"+str(e)) 
        #                 return
        #             if "最多访问" in str(e):
        #                 Log.logger.warning("dividend:触发限流，等待重试。\n"+str(e))
        #                 time.sleep(15)
        #                 continue
        #             else:
        #                 if try_times<10:
        #                     try_times=try_times+1;
        #                     Log.logger.warning("dividend:函数异常，等待重试。\n"+str(e))
        #                     time.sleep(15)
        #                     continue
        #                 else:
        #                     info = traceback.format_exc()
        #                     alert.send('dividend','函数异常',str(info))
        #                     Log.logger.error(info)
        # DB.exec('rename table '+table+' to '+table+'_old;',db);
        # DB.exec('rename table '+table+'_tmp to '+table+';',db);
        # DB.exec("drop table if exists "+table+'_old',db)
        # tsSHelper.setIndex(table,db)
            
    @tsMonitor
    def fina_indicator(pro,db):
        # 首先确保公告表存在
        tsAStockFinance._ensure_disclosure_table(pro, db)
        
        fileds="ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,quick_ratio,cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,op_income,valuechange_income,interst_income,daa,ebit,ebitda,fcff,fcfe,current_exint,noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,networking_capital,invest_capital,retained_earnings,diluted2_eps,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,grossprofit_margin,cogs_of_sales,expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,finaexp_of_gr,impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,roe_yearly,roa2_yearly,roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,dtprofit_to_profit,salescash_to_or,ocf_to_or,ocf_to_opincome,capitalized_to_da,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,currentdebt_to_debt,longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt,tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,ebit_to_interest,longdebt_to_workingcapital,ebitda_to_debt,turn_days,roa_yearly,roa_dp,fixed_assets,profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,ocf_to_profit,cash_to_liqdebt,cash_to_liqdebt_withinterest,op_to_liqdebt,op_to_debt,roic_yearly,total_fa_trun,profit_to_op,q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_exp_to_sales,q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,q_impair_to_gr_ttm,q_gc_to_gr,q_op_to_gr,q_roe,q_dt_roe,q_npta,q_opincome_to_ebt,q_investincome_to_ebt,q_dtprofit_to_profit,q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or,basic_eps_yoy,dt_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_gr_qoq,q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,q_netprofit_qoq,equity_yoy,rd_exp,update_flag"
        tsAStockFinance.getFinance(pro,'fina_indicator','astock_finance_indicator',fileds,db)
    
    @tsMonitor
    def fina_audit(pro,db):
        # 首先确保公告表存在
        tsAStockFinance._ensure_disclosure_table(pro, db)
        
        fileds=""
        tsAStockFinance.getFinance(pro,'fina_audit','astock_finance_audit',fileds,db)
    
    @tsMonitor
    def fina_mainbz(pro,db):
        # 首先确保公告表存在
        tsAStockFinance._ensure_disclosure_table(pro, db)
        
        fileds=""
        tsAStockFinance.getFinance(pro,'fina_mainbz','astock_finance_mainbz',fileds,db)

    @tsMonitor
    def top10_holders(pro,db):
        # 首先确保公告表存在
        tsAStockFinance._ensure_disclosure_table(pro, db)
        
        fileds=""
        tsAStockFinance.getFinance(pro,'top10_holders','astock_market_top10_holders',fileds,db)
        

    @tsMonitor
    def top10_floatholders(pro,db):
        # 首先确保公告表存在
        tsAStockFinance._ensure_disclosure_table(pro, db)
        
        fileds=""
        tsAStockFinance.getFinance(pro,'top10_floatholders','astock_market_top10_floatholders',fileds,db)

    # 新增VIP版本的方法
    @staticmethod
    def get_quarter_end_dates():
        """获取季度结束日期列表"""
        return ['0331', '0630', '0930', '1231']
    
    @staticmethod
    def get_next_quarter_periods(current_year=None):
        """
        获取需要更新的季度期间列表
        格式：YYYYMMDD (如20231231)
        """
        if current_year is None:
            current_year = datetime.datetime.now().year
        
        periods = []
        quarter_ends = tsAStockFinance.get_quarter_end_dates()
        
        # 从2000年开始到当前年份
        for year in range(2000, current_year + 1):
            for quarter_end in quarter_ends:
                period = f"{year}{quarter_end}"
                periods.append(period)
        
        return periods
    
    @staticmethod
    def get_max_end_date_from_table(table_name, db='default'):
        """
        从指定表获取最大的end_date
        """
        try:
            adapter = DB.get_adapter(db)
            if not adapter.table_exists(table_name):
                return None
                
            sql = f"select max(end_date) as max_end_date from {table_name}"
            result = DB.select_to_df(sql, db)
            
            if result is not None and not result.empty and result['max_end_date'].iloc[0] is not None:
                return result['max_end_date'].iloc[0]
            else:
                return None
        except Exception as e:
            Log.logger.warning(f"获取表{table_name}最大end_date失败: {str(e)}")
            return None
    
    @staticmethod
    def delete_max_end_date_records(table_name, max_end_date, db='default'):
        """
        删除最大end_date的记录
        """
        try:
            sql = f"delete from {table_name} where end_date = '{max_end_date}'"
            DB.delete(sql, db)
            Log.logger.info(f"已删除表{table_name}中end_date为{max_end_date}的记录")
            return True
        except Exception as e:
            Log.logger.error(f"删除表{table_name}中end_date为{max_end_date}的记录失败: {str(e)}")
            return False
    
    @staticmethod
    def get_periods_to_update(table_name, db='default'):
        """
        获取需要更新的期间列表
        逻辑：获取所有季度期间，排除已存在的期间，按时间顺序返回下一个需要更新的期间
        """
        try:
            # 获取所有可能的期间
            all_periods = tsAStockFinance.get_next_quarter_periods()
            
            adapter = DB.get_adapter(db)
            if not adapter.table_exists(table_name):
                # 表不存在，返回最早的期间
                return [all_periods[0]] if all_periods else []
            
            # 获取表中已存在的期间
            sql = f"select distinct end_date from {table_name} order by end_date"
            existing_periods_df = DB.select_to_df(sql, db)
            
            existing_periods = []
            if existing_periods_df is not None and not existing_periods_df.empty:
                existing_periods = existing_periods_df['end_date'].tolist()
            
            # 找出需要更新的期间（不在现有期间中的）
            periods_to_update = [p for p in all_periods if p not in existing_periods]
            
            # 只返回下一个需要更新的期间
            if periods_to_update:
                return periods_to_update[:1]  # 只返回第一个
            else:
                return []
                
        except Exception as e:
            Log.logger.error(f"获取需要更新的期间列表失败: {str(e)}")
            # 如果查询失败，返回最早的期间
            all_periods = tsAStockFinance.get_next_quarter_periods()
            return [all_periods[0]] if all_periods else []
    
    @staticmethod
    def collect_vip_data(pro, api_name, table_name, fields, db='default'):
        """
        使用VIP接口收集财务数据的通用方法
        """
        try:
            Log.logger.info(f"{api_name}_vip - 开始收集数据到表 {table_name}")
            
            # 获取最大end_date
            max_end_date = tsAStockFinance.get_max_end_date_from_table(table_name, db)
            
            # 如果存在最大end_date，先删除该期间的数据
            if max_end_date:
                Log.logger.info(f"{api_name}_vip - 发现最大end_date: {max_end_date}")
                tsAStockFinance.delete_max_end_date_records(table_name, max_end_date, db)
            
            # 获取需要更新的期间
            periods_to_update = tsAStockFinance.get_periods_to_update(table_name, db)
            
            if not periods_to_update:
                Log.logger.info(f"{api_name}_vip - 没有需要更新的期间")
                return
            
            Log.logger.info(f"{api_name}_vip - 开始获取期间: {periods_to_update}")
            
            # 获取API函数
            api_func = getattr(pro, f"{api_name}_vip")
            
            for period in periods_to_update:
                try:
                    Log.logger.info(f"{api_name}_vip - 开始获取期间 {period} 的数据")
                    
                    # 调用VIP接口
                    if fields:
                        df = api_func(period=period, fields=fields)
                    else:
                        df = api_func(period=period)
                    
                    if df is not None and not df.empty:
                        # 保存数据，使用动态chunksize避免SQLite参数限制
                        DB.safe_to_sql(df, table_name, db, index=False, if_exists='append')
                        Log.logger.info(f"{api_name}_vip - 成功获取并保存期间 {period} 的数据，共 {len(df)} 条记录")
                        
                        # 创建索引
                        try:
                            tsSHelper.setIndex(table_name, db)
                        except Exception as index_error:
                            Log.logger.warning(f"为表 {table_name} 创建索引失败: {str(index_error)}")
                    else:
                        Log.logger.warning(f"{api_name}_vip - 期间 {period} 没有返回数据")
                    
                    # 避免请求过于频繁
                    time.sleep(0.5)
                    
                except Exception as e:
                    if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning(f"{api_name}_vip - 触发每日/每小时访问限制: {str(e)}")
                        break
                    elif "最多访问" in str(e):
                        Log.logger.warning(f"{api_name}_vip - 触发访问限制，等待重试: {str(e)}")
                        time.sleep(15)
                        continue
                    else:
                        Log.logger.error(f"{api_name}_vip - 获取期间 {period} 数据失败: {str(e)}")
                        continue
            
        except Exception as e:
            Log.logger.error(f"{api_name}_vip - 收集数据失败: {str(e)}")
            traceback.print_exc()

    # VIP版本的具体方法
    
    @tsMonitor
    def income_vip(pro, db):
        """利润表VIP接口"""
        fields = "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit,rd_exp,fin_exp_int_exp,fin_exp_int_inc,transfer_surplus_rese,transfer_housing_imprest,transfer_oth,adj_lossgain,withdra_legal_surplus,withdra_legal_pubfund,withdra_biz_devfund,withdra_rese_fund,withdra_oth_ersu,workers_welfare,distr_profit_shrhder,prfshare_payable_dvd,comshare_payable_dvd,capit_comstock_div,net_after_nr_lp_correct,credit_impa_loss,net_expo_hedging_benefits,oth_impair_loss_assets,total_opcost,amodcost_fin_assets,oth_income,asset_disp_income,continued_net_profit,end_net_profit,update_flag"
        table_name = "astock_finance_income"
        
        Log.logger.info("开始使用VIP接口获取利润表数据")
        tsAStockFinance.collect_vip_data(pro, "income", table_name, fields, db)
    
    @tsMonitor
    def balancesheet_vip(pro, db):
        """资产负债表VIP接口"""
        fields = "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,total_share,cap_rese,undistr_porfit,surplus_rese,special_rese,money_cap,trad_asset,notes_receiv,accounts_receiv,oth_receiv,prepayment,div_receiv,int_receiv,inventories,amor_exp,nca_within_1y,sett_rsrv,loanto_oth_bank_fi,premium_receiv,reinsur_receiv,reinsur_res_receiv,pur_resale_fa,oth_cur_assets,total_cur_assets,fa_avail_for_sale,htm_invest,lt_eqt_invest,invest_real_estate,time_deposits,oth_assets,lt_rec,fix_assets,cip,const_materials,fixed_assets_disp,produc_bio_assets,oil_and_gas_assets,intan_assets,r_and_d,goodwill,lt_amor_exp,defer_tax_assets,decr_in_disbur,oth_nca,total_nca,cash_reser_cb,depos_in_oth_bfi,prec_metals,deriv_assets,rr_reins_une_prem,rr_reins_outstd_cla,rr_reins_lins_liab,rr_reins_lthins_liab,refund_depos,ph_pledge_loans,refund_cap_depos,indep_acct_assets,client_depos,client_prov,transac_seat_fee,invest_as_receiv,total_assets,st_loan,st_borr,cb_borr,depos_ib_deposits,loan_oth_bank,trading_fl,notes_payable,acct_payable,adv_receipts,sold_for_repur_fa,comm_payable,payroll_payable,taxes_payable,int_payable,div_payable,oth_payable,acc_exp,deferred_inc,st_bonds_payable,payable_to_reinsurer,rsrv_insur_cont,acting_trading_sec,acting_uw_sec,non_cur_liab_due_1y,oth_cur_liab,total_cur_liab,bond_payable,lt_payable,specific_payables,estimated_liab,defer_tax_liab,defer_inc_non_cur_liab,oth_ncl,total_ncl,depos_oth_bfi,deriv_liab,depos,agency_bus_liab,oth_liab,prem_receiv_adva,depos_received,ph_invest,reser_une_prem,reser_outstd_claims,reser_lins_liab,reser_lthins_liab,indept_acc_liab,pledge_borr,indem_payable,policy_div_payable,total_liab,treasury_share,ordin_risk_reser,forex_differ,invest_loss_unconf,minority_int,total_hldr_eqy_exc_min_int,total_hldr_eqy_inc_min_int,total_liab_hldr_eqy,lt_payroll_payable,oth_comp_income,oth_eqt_tools,oth_eqt_tools_p_shr,lending_funds,acc_receivable,st_fin_payable,payables,hfs_assets,hfs_sales,cost_fin_assets,fair_value_fin_assets,cip_total,oth_pay_total,long_pay_total,debt_invest,oth_debt_invest,oth_eq_invest,oth_illiq_fin_assets,oth_eq_ppbond,receiv_financing,use_right_assets,lease_liab,contract_assets,contract_liab,accounts_receiv_bill,accounts_pay,oth_rcv_total,fix_assets_total,update_flag"
        table_name = "astock_finance_balancesheet"
        
        Log.logger.info("开始使用VIP接口获取资产负债表数据")
        tsAStockFinance.collect_vip_data(pro, "balancesheet", table_name, fields, db)
    
    @tsMonitor
    def cashflow_vip(pro, db):
        """现金流量表VIP接口"""
        fields = "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,net_profit,finan_exp,c_fr_sale_sg,recp_tax_rends,n_depos_incr_fi,n_incr_loans_cb,n_inc_borr_oth_fi,prem_fr_orig_contr,n_incr_insured_dep,n_reinsur_prem,n_incr_disp_tfa,ifc_cash_incr,n_incr_disp_faas,n_incr_loans_oth_bank,n_cap_incr_repur,c_fr_oth_operate_a,c_inf_fr_operate_a,c_paid_goods_s,c_paid_to_for_empl,c_paid_for_taxes,n_incr_clt_loan_adv,n_incr_dep_cbob,c_pay_claims_orig_inco,pay_handling_chrg,pay_comm_insur_plcy,oth_cash_pay_oper_act,st_cash_out_act,n_cashflow_act,oth_recp_ral_inv_act,c_disp_withdrwl_invest,c_recp_return_invest,n_recp_disp_fiolta,n_recp_disp_sobu,stot_inflows_inv_act,c_pay_acq_const_fiolta,c_paid_invest,n_disp_subs_oth_biz,oth_pay_ral_inv_act,n_incr_pledge_loan,stot_out_inv_act,n_cashflow_inv_act,c_recp_borrow,proc_issue_bonds,oth_cash_recp_ral_fnc_act,stot_cash_in_fnc_act,free_cashflow,c_prepay_amt_borr,c_pay_dist_dpcp_int_exp,incl_dvd_profit_paid_sc_ms,oth_cashpay_ral_fnc_act,stot_cashout_fnc_act,n_cash_flows_fnc_act,eff_fx_flu_cash,n_incr_cash_cash_equ,c_cash_equ_beg_period,c_cash_equ_end_period,c_recp_cap_contrib,incl_cash_rec_saims,uncon_invest_loss,prov_depr_assets,depr_fa_coga_dpba,amort_intang_assets,lt_amort_deferred_exp,decr_deferred_exp,incr_acc_exp,loss_disp_fiolta,loss_scr_fa,loss_fv_chg,invest_loss,decr_def_inc_tax_assets,incr_def_inc_tax_liab,decr_inventories,decr_oper_payable,incr_oper_payable,others,im_net_cashflow_oper_act,conv_debt_into_cap,conv_copbonds_due_within_1y,fa_fnc_leases,end_bal_cash,beg_bal_cash,end_bal_cash_equ,beg_bal_cash_equ,im_n_incr_cash_equ,update_flag"
        table_name = "astock_finance_cashflow"
        
        Log.logger.info("开始使用VIP接口获取现金流量表数据")
        tsAStockFinance.collect_vip_data(pro, "cashflow", table_name, fields, db)
    
    @tsMonitor
    def forecast_vip(pro, db):
        """业绩预告VIP接口"""
        fields = "ts_code,ann_date,end_date,type,p_change_min,p_change_max,net_profit_min,net_profit_max,last_parent_net,first_ann_date,summary,change_reason"
        table_name = "astock_finance_forecast"
        
        Log.logger.info("开始使用VIP接口获取业绩预告数据")
        tsAStockFinance.collect_vip_data(pro, "forecast", table_name, fields, db)
    
    @tsMonitor
    def express_vip(pro, db):
        """业绩快报VIP接口"""
        fields = "ts_code,ann_date,end_date,revenue,operate_profit,total_profit,n_income,total_assets,total_hldr_eqy_exc_min_int,diluted_eps,diluted_roe,yoy_net_profit,bps,yoy_sales,yoy_op,yoy_tp,yoy_dedt_np,yoy_eps,yoy_roe,growth_assets,yoy_equity,growth_bps,or_last_year,op_last_year,tp_last_year,np_last_year,eps_last_year,open_net_assets,open_bps,perf_summary,is_audit,remark"
        table_name = "astock_finance_express"
        
        Log.logger.info("开始使用VIP接口获取业绩快报数据")
        tsAStockFinance.collect_vip_data(pro, "express", table_name, fields, db)
    
    @tsMonitor
    def fina_indicator_vip(pro, db):
        """财务指标VIP接口"""
        fields = "ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,quick_ratio,cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,op_income,valuechange_income,interst_income,daa,ebit,ebitda,fcff,fcfe,current_exint,noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,networking_capital,invest_capital,retained_earnings,diluted2_eps,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,grossprofit_margin,cogs_of_sales,expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,finaexp_of_gr,impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,roe_yearly,roa2_yearly,roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,dtprofit_to_profit,salescash_to_or,ocf_to_or,ocf_to_opincome,capitalized_to_da,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,currentdebt_to_debt,longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt,tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,ebit_to_interest,longdebt_to_workingcapital,ebitda_to_debt,turn_days,roa_yearly,roa_dp,fixed_assets,profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,ocf_to_profit,cash_to_liqdebt,cash_to_liqdebt_withinterest,op_to_liqdebt,op_to_debt,roic_yearly,total_fa_trun,profit_to_op,q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_exp_to_sales,q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,q_impair_to_gr_ttm,q_gc_to_gr,q_op_to_gr,q_roe,q_dt_roe,q_npta,q_opincome_to_ebt,q_investincome_to_ebt,q_dtprofit_to_profit,q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or,basic_eps_yoy,dt_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_gr_qoq,q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,q_netprofit_qoq,equity_yoy,rd_exp,update_flag"
        table_name = "astock_finance_indicator"
        
        Log.logger.info("开始使用VIP接口获取财务指标数据")
        tsAStockFinance.collect_vip_data(pro, "fina_indicator", table_name, fields, db)
    
    @tsMonitor
    def fina_audit_vip(pro, db):
        """财务审计意见VIP接口"""
        fields = "ts_code,ann_date,end_date,audit_result,audit_fees,audit_agency,audit_sign"
        table_name = "astock_finance_audit"
        
        Log.logger.info("开始使用VIP接口获取财务审计意见数据")
        tsAStockFinance.collect_vip_data(pro, "fina_audit", table_name, fields, db)