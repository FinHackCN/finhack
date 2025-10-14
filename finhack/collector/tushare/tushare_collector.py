import sys
import time
from datetime import datetime
from finhack.library.db import DB
from finhack.library.config import Config
from finhack.library.thread import collectThread
from finhack.collector.tushare.astockbasic import tsAStockBasic
from finhack.collector.tushare.astockprice import tsAStockPrice
from finhack.collector.tushare.astockfinance import tsAStockFinance
from finhack.collector.tushare.astockfinance_vip import tsAStockFinanceVIP
from finhack.collector.tushare.astockindex import tsAStockIndex
from finhack.collector.tushare.astockother import tsAStockOther
from finhack.collector.tushare.astockmarket import tsAStockMarket
from finhack.collector.tushare.futures import tsFuntures
from finhack.collector.tushare.fund import tsFund
from finhack.collector.tushare.other import tsOther
from finhack.collector.tushare.econo import tsEcono
from finhack.collector.tushare.ustock import tsUStock
from finhack.collector.tushare.hstock import tsHStock
from finhack.collector.tushare.cb import tsCB
from finhack.collector.tushare.fx import tsFX
from finhack.collector.tushare.helper import tsSHelper
import finhack.library.log as Log
from datetime import datetime, timedelta
import tushare as ts
import traceback
import pandas as pd
import os
import sqlite3
from runtime.constant import *

class TushareCollector:
    def __init__(self,args):
        self.thread_list = []
        self.max_thread_runtime = 12 * 60 * 60  # 12小时，单位为秒
        # 用于跟踪各个数据表的获取状态
        self.dependency_status = {
            'astock_basic': False,           # 股票基本信息
            'astock_trade_cal': False,      # 交易日历
            'astock_index_basic': False,    # 指数基本信息
            'astock_finance_disclosure_date': False,  # 财务披露日期
            'fund_basic': False,            # 基金基本信息
            'cb_basic': False,              # 可转债基本信息
            'hk_basic': True,              # 港股基本信息
            'fx_basic': True               # 外汇基本信息
        }


    def check_dependency(self, table_name):
        """检查依赖表是否存在，如果不存在但是有创建该表的功能则尝试创建"""
        if table_name in self.dependency_status:
            return self.dependency_status[table_name]
        else:
            # 检查表是否存在
            try:
                # 获取数据库适配器
                adapter = DB.get_adapter(self.db)
                
                # 检查表是否存在
                if adapter.table_exists(table_name):
                    # 表存在，尝试查询是否有数据
                    result = DB.selectToList(f"SELECT 1 FROM {table_name} LIMIT 1", self.db)
                    return len(result) > 0
                else:
                    # 表不存在，尝试创建基本表
                    if table_name == 'astock_basic':
                        Log.logger.warning(f"表 {table_name} 不存在，尝试创建...")
                        # 创建股票基本信息表
                        success = tsAStockBasic.stock_basic(self.pro, self.db)
                        if success:
                            self.dependency_status['astock_basic'] = True
                            return True
                        else:
                            Log.logger.error(f"创建表 {table_name} 失败")
                            return False
                    elif table_name == 'astock_trade_cal':
                        Log.logger.warning(f"表 {table_name} 不存在，尝试创建...")
                        # 创建交易日历表
                        success = tsAStockBasic.trade_cal(self.pro, self.db)
                        if success:
                            self.dependency_status['astock_trade_cal'] = True
                            return True
                        else:
                            Log.logger.error(f"创建表 {table_name} 失败")
                            return False
                    elif table_name == 'astock_finance_disclosure_date':
                        Log.logger.warning(f"表 {table_name} 不存在，尝试创建...")
                        from finhack.collector.tushare.astockfinance import tsAStockFinance
                        # 创建财务披露日期表
                        success = tsAStockFinance.disclosure_date(self.pro, self.db)
                        if success:
                            self.dependency_status['astock_finance_disclosure_date'] = True
                            return True
                        else:
                            Log.logger.error(f"创建表 {table_name} 失败")
                            return False
                    else:
                        Log.logger.warning(f"表 {table_name} 不存在，无法自动创建")
                        return False
            except Exception as e:
                Log.logger.error(f"检查表 {table_name} 失败: {str(e)}")
                return False
        
    def ensure_db_directory(self, db_name):
        """确保数据库目录存在"""
        try:
            # 获取数据库配置
            db_config = Config.get_config('db', db_name)
            
            # 检查是否为SQLite数据库
            if db_config.get('type', '') != 'sqlite':
                return True
            
            # 获取数据库文件路径
            db_path = db_config.get('path', '')
            if not db_path:
                Log.logger.warning(f"SQLite数据库配置中未指定path参数")
                return False
            
            # 如果是相对路径，使用项目数据目录作为基础路径
            if not os.path.isabs(db_path):
                from runtime.constant import BASE_DIR
                abs_db_path = os.path.abspath(os.path.join(BASE_DIR, db_path))
                Log.logger.info(f"数据库相对路径: {db_path}")
                Log.logger.info(f"转换为绝对路径: {abs_db_path}")
                db_path = abs_db_path
            
            # 获取数据库目录
            db_dir = os.path.dirname(db_path)
            Log.logger.info(f"数据库目录: {db_dir}")
            
            # 检查目录是否存在
            if not os.path.exists(db_dir):
                Log.logger.warning(f"数据库目录不存在，创建目录: {db_dir}")
                os.makedirs(db_dir, exist_ok=True)
                return True
            
            # 检查目录是否有写权限
            if not os.access(db_dir, os.W_OK):
                Log.logger.error(f"数据库目录没有写权限: {db_dir}")
                return False
                
            # 如果数据库文件存在，尝试检查是否可读写
            if os.path.exists(db_path):
                if not os.access(db_path, os.R_OK | os.W_OK):
                    Log.logger.error(f"数据库文件没有读写权限: {db_path}")
                    return False
                
                # 尝试连接数据库进行验证
                try:
                    conn = sqlite3.connect(db_path, timeout=10)
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    conn.close()
                    Log.logger.info(f"数据库文件可正常访问: {db_path}")
                except Exception as e:
                    Log.logger.error(f"数据库文件可能已损坏: {str(e)}")
                    # 重命名损坏的数据库文件
                    backup_path = f"{db_path}.bak.{int(time.time())}"
                    try:
                        os.rename(db_path, backup_path)
                        Log.logger.warning(f"已将可能损坏的数据库文件重命名为: {backup_path}")
                    except Exception as rename_error:
                        Log.logger.error(f"无法重命名损坏的数据库文件: {str(rename_error)}")
                        return False
            
            return True
        except Exception as e:
            Log.logger.error(f"检查数据库目录时出错: {str(e)}")
            return False

    def run(self):
        """按照依赖关系顺序执行数据采集"""
        Log.logger.info("开始执行Tushare数据采集...")
        
        # 初始化Tushare API
        cfgTS=Config.get_config('ts')
        ts.set_token(cfgTS['token'])
        self.pro = ts.pro_api()
        self.db=cfgTS['db']
        
        # 确保数据库目录存在
        if not self.ensure_db_directory(self.db):
            Log.logger.error("数据库目录检查失败，终止数据采集")
            return False
        
        # 第一步：获取基础数据（股票、交易日历等）
        Log.logger.info("第一步：获取基础数据...")
        success = self.getAStockBasic()
        if not success:
            Log.logger.error("获取股票基本信息失败，终止数据采集")
            return False

        # 第二步：获取行情数据
        # 依赖于股票基本信息
        Log.logger.info("第二步：获取行情数据...")
        if True or self.check_dependency('astock_basic'):
            self.getAStockPrice()
        else:
            Log.logger.error("获取行情数据失败：股票基本信息数据不存在")
            
        # 第三步：获取财务数据
        # 依赖于股票基本信息和财务披露日期
        Log.logger.info("第三步：获取财务数据...")
        if self.check_dependency('astock_basic'):
            self.getAStockFinance()
        else:
            Log.logger.error("获取财务数据失败：股票基本信息数据不存在")
            
        # 第四步：获取市场数据
        # 依赖于股票基本信息
        Log.logger.info("第四步：获取市场数据...")
        if self.check_dependency('astock_basic'):
            self.getAStockMarket()
        else:
            Log.logger.error("获取市场数据失败：股票基本信息数据不存在")
            
        # 第五步：获取指数数据
        # 部分依赖于股票基本信息
        Log.logger.info("第五步：获取指数数据...")
        self.getAStockIndex()
        
        # 第六步：获取其他A股数据
        Log.logger.info("第六步：获取其他A股数据...")
        self.getAStockOther()
        
        # 第七步：获取基金数据
        Log.logger.info("第七步：获取基金数据...")
        self.getFund()
        
        # 第八步：获取宏观经济数据
        Log.logger.info("第八步：获取宏观经济数据...")
        self.getEcono()
        
        # 第九步：获取其他数据
        Log.logger.info("第九步：获取其他数据...")
        self.getOther()
        
        # 第十步：获取期货数据
        Log.logger.info("第十步：获取期货数据...")
        self.getFutures()
        
        # 第十一步：获取美股数据
        Log.logger.info("第十一步：获取美股数据...")
        self.getUStock()
        
        # 第十二步：获取港股数据
        Log.logger.info("第十二步：获取港股数据...")
        self.getHStock()
        
        # 第十三步：获取可转债数据
        Log.logger.info("第十三步：获取可转债数据...")
        self.getCB()
        
        # 第十四步：获取外汇数据
        Log.logger.info("第十四步：获取外汇数据...")
        self.getFX()
        
        # 启动所有线程
        Log.logger.info(f"启动 {len(self.thread_list)} 个数据采集线程...")
        for t in self.thread_list:
            t.setDaemon(True)
            t.start()

        # 等待所有线程完成
        for t in self.thread_list:
            t.join()
            
        Log.logger.info("所有数据采集线程已完成")
        
        return True
    
        # cfgTS=Config.get_config('ts')
        # db=cfgTS['db']
        
        # tables_list=DB.selectToList('show tables',db)
        # for v in tables_list:
        #     table=list(v.values())[0]
        #     tsSHelper.setIndex(table,db)    
    
    def save(self):
        """将数据库中的数据导出到CSV文件，使用多线程提高效率"""
        try:
            from finhack.collector.tushare.save import TushareSaver
            import threading
            import concurrent.futures
            
            saver = TushareSaver()
            results = {}
            
            # 创建线程池
            max_workers = min(5, os.cpu_count() or 4)  # 限制最大线程数
            Log.logger.info(f"启动数据保存任务，使用{max_workers}线程...")
            
            # 使用线程池执行各个保存任务
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交各个保存任务
                futures = {
                    'kline': executor.submit(saver.save_kline_to_csv),
                    'lists': executor.submit(saver.save_lists_to_csv),
                    'adj': executor.submit(saver.save_adj_factors_to_csv),
                    'calendar': executor.submit(saver.save_calendars_to_csv),
                    'finance': executor.submit(saver.save_table_data_to_csv),
                }
                
                # 获取各个任务的结果
                for name, future in futures.items():
                    try:
                        results[name] = future.result()
                        status = "成功" if results[name] else "失败"
                        Log.logger.info(f"{name}数据保存{status}")
                    except Exception as e:
                        results[name] = False
                        Log.logger.error(f"{name}数据保存出错: {str(e)}")
                        Log.logger.error(traceback.format_exc())
            
            # 所有步骤都成功才返回True
            all_success = all(results.values())
            Log.logger.info(f"所有数据保存任务已完成，状态: {'成功' if all_success else '部分失败'}")
            return all_success
            
        except Exception as e:
            Log.logger.error(f"导出数据时发生错误: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False
    
    def getAStockBasic(self):
        """获取A股基本信息，这是最基础的数据，其他大多数数据都依赖于此"""
        try:
            # 获取股票基本信息
            Log.logger.info("获取股票基本信息...")
            tsAStockBasic.stock_basic(self.pro, self.db)
            self.dependency_status['astock_basic'] = True
            
            # 获取交易日历
            Log.logger.info("获取交易日历...")
            tsAStockBasic.trade_cal(self.pro, self.db)
            self.dependency_status['astock_trade_cal'] = True
            
            # 检查基础数据是否获取成功
            if not self.check_dependency('astock_basic'):
                Log.logger.error("获取股票基本信息失败")
                return False
                
            # 获取股票名称变更记录
            Log.logger.info("获取股票名称变更记录...")
            self.mTread(tsAStockBasic, 'namechange', 'astock_basic')
            
            # 获取沪深港通成分股
            Log.logger.info("获取沪深港通成分股...")
            self.mTread(tsAStockBasic, 'hs_const', 'astock_basic')
            
            # 获取上市公司基本信息
            Log.logger.info("获取上市公司基本信息...")
            self.mTread(tsAStockBasic, 'stock_company', 'astock_basic')
            
            # 获取公司管理层
            Log.logger.info("获取公司管理层...")
            self.mTread(tsAStockBasic, 'stk_managers', 'astock_basic')
            
            # 获取管理层薪酬和持股
            Log.logger.info("获取管理层薪酬和持股...")
            self.mTread(tsAStockBasic, 'stk_rewards', 'astock_basic')
            
            # 获取新股上市信息
            Log.logger.info("获取新股上市信息...")
            self.mTread(tsAStockBasic, 'new_share', 'astock_basic')
            
            return True
        except Exception as e:
            Log.logger.error(f"获取A股基本信息时发生错误: {str(e)}")
            return False


    def getAStockPrice(self):
        """获取A股价格数据，依赖于股票基本信息"""
        try:
            # 检查股票基本信息是否存在
            if not self.check_dependency('astock_basic'):
                Log.logger.error("获取价格数据失败：股票基本信息数据不存在")
                return False
                
            # 获取日线行情
            Log.logger.info("获取日线行情...")
            self.mTread(tsAStockPrice, 'daily', 'astock_basic')
            
            # 获取周线行情
            Log.logger.info("获取周线行情...")
            self.mTread(tsAStockPrice, 'weekly', 'astock_basic')
            
            # 获取月线行情
            Log.logger.info("获取月线行情...")
            self.mTread(tsAStockPrice, 'monthly', 'astock_basic')
            
            # 获取复权因子
            Log.logger.info("获取复权因子...")
            self.mTread(tsAStockPrice, 'adj_factor', 'astock_basic')
            
            # 获取停复牌信息
            Log.logger.info("获取停复牌信息...")
            self.mTread(tsAStockPrice, 'suspend_d', 'astock_basic')
            
            # 获取每日指标
            Log.logger.info("获取每日指标...")
            self.mTread(tsAStockPrice, 'daily_basic', 'astock_basic')
            
            # 获取个股资金流向
            Log.logger.info("获取个股资金流向...")
            self.mTread(tsAStockPrice, 'moneyflow', 'astock_basic')
            
            # 获取每日涨跌停价格
            Log.logger.info("获取每日涨跌停价格...")
            self.mTread(tsAStockPrice, 'stk_limit', 'astock_basic')
            
            # 获取涨跌停股票
            Log.logger.info("获取涨跌停股票...")
            self.mTread(tsAStockPrice, 'limit_list', 'astock_basic')
            
            # # 获取沪深港通资金流向
            # Log.logger.info("获取沪深港通资金流向...")
            # self.mTread(tsAStockPrice, 'moneyflow_hsgt', 'astock_basic')
            
            # # 获取沪深股通十大成交股
            # Log.logger.info("获取沪深股通十大成交股...")
            # self.mTread(tsAStockPrice, 'hsgt_top10', 'astock_basic')
            
            # # 获取港股通十大成交股
            # Log.logger.info("获取港股通十大成交股...")
            # self.mTread(tsAStockPrice, 'ggt_top10', 'astock_basic')
            
            # # 获取沪深港股通持股明细
            # Log.logger.info("获取沪深港股通持股明细...")
            # self.mTread(tsAStockPrice, 'hk_hold', 'astock_basic')
            
            # # 获取港股通每日成交统计
            # Log.logger.info("获取港股通每日成交统计...")
            # self.mTread(tsAStockPrice, 'ggt_daily', 'astock_basic')
            
            # 暂时不获取的数据
            # self.mTread(tsAStockPrice,'ggt_monthly')
            # self.mTread(tsAStockPrice,'ccass_hold_detail')
            
            return True
        except Exception as e:
            Log.logger.error(f"获取A股价格数据时发生错误: {str(e)}")
            return False


    def getAStockFinance(self):
        """获取A股财务数据，结合VIP接口和传统方法"""
        try:
            # 首先获取财务披露日期（基础依赖数据）
            Log.logger.info("获取财务披露日期...")
            tsAStockFinance.disclosure_date(self.pro, self.db)
            self.dependency_status['astock_finance_disclosure_date'] = True
            
            # 检查财务披露日期是否获取成功
            if not self.check_dependency('astock_finance_disclosure_date'):
                Log.logger.warning("财务披露日期数据不存在，VIP接口可能仍能正常工作")
            
            Log.logger.info("开始使用VIP接口获取主要财务数据")
            
            # 获取利润表（VIP接口）
            Log.logger.info("获取利润表（VIP接口）...")
            tsAStockFinanceVIP.income_vip(self.pro, self.db)
            
            # 获取资产负债表（VIP接口）
            Log.logger.info("获取资产负债表（VIP接口）...")
            tsAStockFinanceVIP.balancesheet_vip(self.pro, self.db)
            
            # 获取现金流量表（VIP接口）
            Log.logger.info("获取现金流量表（VIP接口）...")
            tsAStockFinanceVIP.cashflow_vip(self.pro, self.db)
            
            # 获取业绩预告（VIP接口）
            Log.logger.info("获取业绩预告（VIP接口）...")
            tsAStockFinanceVIP.forecast_vip(self.pro, self.db)
            
            # 获取业绩快报（VIP接口）
            Log.logger.info("获取业绩快报（VIP接口）...")
            tsAStockFinanceVIP.express_vip(self.pro, self.db)
            
            # 获取财务指标数据（VIP接口）
            Log.logger.info("获取财务指标数据（VIP接口）...")
            tsAStockFinanceVIP.fina_indicator_vip(self.pro, self.db)
            
            # 获取财务审计意见（VIP接口）
            Log.logger.info("获取财务审计意见（VIP接口）...")
            tsAStockFinanceVIP.fina_audit_vip(self.pro, self.db)
            
            # 保留原有的其他方法（传统方法）
            # 获取主营业务构成
            Log.logger.info("获取主营业务构成...")
            self.mTread(tsAStockFinance, 'fina_mainbz', 'astock_finance_disclosure_date')
            
            # 获取分红送股数据
            Log.logger.info("获取分红送股数据...")
            self.mTread(tsAStockFinance, 'dividend', 'astock_finance_disclosure_date')
            
            # 获取前十大股东
            Log.logger.info("获取前十大股东...")
            self.mTread(tsAStockFinance, 'top10_holders', 'astock_finance_disclosure_date')
            
            # 获取前十大流通股东
            Log.logger.info("获取前十大流通股东...")
            self.mTread(tsAStockFinance, 'top10_floatholders', 'astock_finance_disclosure_date')
            
            return True
        except Exception as e:
            Log.logger.error(f"获取A股财务数据时发生错误: {str(e)}")
            return False

       
    def getAStockMarket(self):
        """获取A股市场数据，依赖于股票基本信息"""
        try:
            # 检查股票基本信息是否存在
            if not self.check_dependency('astock_basic'):
                Log.logger.error("获取市场数据失败：股票基本信息数据不存在")
                return False
                
            # 获取融资融券交易汇总
            Log.logger.info("获取融资融券交易汇总...")
            self.mTread(tsAStockMarket, 'margin', 'astock_basic')
            
            # 获取融资融券交易明细
            Log.logger.info("获取融资融券交易明细...")
            self.mTread(tsAStockMarket, 'margin_detail', 'astock_basic')
            
            # 获取龙虎榜每日明细
            Log.logger.info("获取龙虎榜每日明细...")
            self.mTread(tsAStockMarket, 'top_list', 'astock_basic')
            
            # 获取龙虎榜机构明细
            Log.logger.info("获取龙虎榜机构明细...")
            self.mTread(tsAStockMarket, 'top_inst', 'astock_basic')
            
            # 获取股权质押统计数据
            Log.logger.info("获取股权质押统计数据...")
            self.mTread(tsAStockMarket, 'pledge_stat', 'astock_basic')
            
            # 获取股权质押明细
            Log.logger.info("获取股权质押明细...")
            self.mTread(tsAStockMarket, 'pledge_detail', 'astock_basic')
            
            # 获取股票回购
            Log.logger.info("获取股票回购...")
            self.mTread(tsAStockMarket, 'repurchase', 'astock_basic')
            
            # 获取概念股分类
            Log.logger.info("获取概念股分类...")
            self.mTread(tsAStockMarket, 'concept', 'astock_basic')
            
            # # 获取概念股列表
            # Log.logger.info("获取概念股列表...")
            # self.mTread(tsAStockMarket, 'concept_detail', 'astock_basic')
            
            # 获取限售股解禁
            Log.logger.info("获取限售股解禁...")
            self.mTread(tsAStockMarket, 'share_float', 'astock_basic')
            
            # 获取大宗交易
            Log.logger.info("获取大宗交易...")
            self.mTread(tsAStockMarket, 'block_trade', 'astock_basic')
            
            # 获取股东人数
            Log.logger.info("获取股东人数...")
            self.mTread(tsAStockMarket, 'stk_holdernumber', 'astock_basic')
            
            # 获取股东增减持
            Log.logger.info("获取股东增减持...")
            self.mTread(tsAStockMarket, 'stk_holdertrade', 'astock_basic')
            
            return True
        except Exception as e:
            Log.logger.error(f"获取A股市场数据时发生错误: {str(e)}")
            return False
     
    def getAStockIndex(self):
        """获取A股指数数据，部分依赖于股票基本信息"""
        try:
            # 获取指数基本信息
            Log.logger.info("获取指数基本信息...")
            tsAStockIndex.index_basic(self.pro, self.db)
            self.dependency_status['astock_index_basic'] = True
            
            # 获取申万行业分类
            Log.logger.info("获取申万行业分类...")
            tsAStockIndex.index_classify(self.pro, self.db)
            
            # 检查指数基本信息是否获取成功
            if not self.check_dependency('astock_index_basic'):
                Log.logger.error("获取指数数据失败：指数基本信息数据不存在")
                return False
                
            # 获取指数日线数据
            Log.logger.info("获取指数日线数据...")
            self.mTread(tsAStockIndex, 'index_daily', 'astock_index_basic')
            
            # 获取指数周线数据
            Log.logger.info("获取指数周线数据...")
            self.mTread(tsAStockIndex, 'index_weekly', 'astock_index_basic')
            
            # 获取指数月线数据
            Log.logger.info("获取指数月线数据...")
            self.mTread(tsAStockIndex, 'index_monthly', 'astock_index_basic')
            
            # 获取指数成分权重
            Log.logger.info("获取指数成分权重...")
            self.mTread(tsAStockIndex, 'index_weight', 'astock_index_basic')
            
            # 获取大盘指数每日指标
            Log.logger.info("获取大盘指数每日指标...")
            self.mTread(tsAStockIndex, 'index_dailybasic', 'astock_index_basic')
            
            # 获取申万行业成分股
            Log.logger.info("获取申万行业成分股...")
            self.mTread(tsAStockIndex, 'index_member', 'astock_index_basic')
            
            return True
        except Exception as e:
            Log.logger.error(f"获取A股指数数据时发生错误: {str(e)}")
            return False

    def getAStockOther(self):
        """获取A股其他数据"""
        try:
            # 获取券商研报
            Log.logger.info("获取券商研报...")
            self.mTread(tsAStockOther, 'report_rc')
            
            # 获取每日筹码及胜率
            Log.logger.info("获取每日筹码及胜率...")
            self.mTread(tsAStockOther, 'cyq_perf')
            
            # 暂时不获取的数据
            # self.mTread(tsAStockOther, 'cyq_chips')
            # broker_recommend
            
            return True
        except Exception as e:
            Log.logger.error(f"获取A股其他数据时发生错误: {str(e)}")
            return False
    
    def getFund(self):
        """获取基金数据，依赖于基金基本信息"""
        try:
            # 获取基金基本信息
            Log.logger.info("获取基金基本信息...")
            tsFund.fund_basic(self.pro, self.db)
            self.dependency_status['fund_basic'] = True
            
            # 检查基金基本信息是否获取成功
            if not self.check_dependency('fund_basic'):
                Log.logger.error("获取基金数据失败：基金基本信息数据不存在")
                return False
                
            # 获取基金公司
            Log.logger.info("获取基金公司...")
            self.mTread(tsFund, 'fund_company', 'fund_basic')
            
            # 获取基金经理
            Log.logger.info("获取基金经理...")
            self.mTread(tsFund, 'fund_manager', 'fund_basic')
            
            # 获取基金份额
            Log.logger.info("获取基金份额...")
            self.mTread(tsFund, 'fund_share', 'fund_basic')
            
            # 获取基金净值
            Log.logger.info("获取基金净值...")
            self.mTread(tsFund, 'fund_nav', 'fund_basic')
            
            # 获取基金分红
            Log.logger.info("获取基金分红...")
            self.mTread(tsFund, 'fund_div', 'fund_basic')
            
            # 获取基金日线数据
            Log.logger.info("获取基金日线数据...")
            self.mTread(tsFund, 'fund_daily', 'fund_basic')
            
            # 获取基金持仓数据
            Log.logger.info("获取基金持仓数据...")
            self.mTread(tsFund, 'fund_portfolio', 'fund_basic')
            
            # 获取基金复权数据
            Log.logger.info("获取基金复权数据...")
            self.mTread(tsFund, 'fund_adj', 'fund_basic')
            
            return True
        except Exception as e:
            Log.logger.error(f"获取基金数据时发生错误: {str(e)}")
            return False

    def getEcono(self):
        """获取宏观经济数据"""
        try:
            # 获取Shibor利率数据
            Log.logger.info("获取Shibor利率数据...")
            self.mTread(tsEcono, 'shibor')
            
            # 获取Shibor报价数据
            Log.logger.info("获取Shibor报价数据...")
            self.mTread(tsEcono, 'shibor_quote')
            
            # 获取LPR数据
            Log.logger.info("获取LPR数据...")
            self.mTread(tsEcono, 'shibor_lpr')
            
            # 暂时不获取的数据
            # self.mTread(tsEcono, 'libor')
            # self.mTread(tsEcono, 'hibor')
            
            # 获取温州民间借贷利率
            Log.logger.info("获取温州民间借贷利率...")
            self.mTread(tsEcono, 'wz_index')
            
            # 获取广州民间借贷利率
            Log.logger.info("获取广州民间借贷利率...")
            self.mTread(tsEcono, 'gz_index')
            
            # 获取国内生产总值数据
            Log.logger.info("获取国内生产总值数据...")
            self.mTread(tsEcono, 'cn_gdp')
            
            # 获取居民消费价格指数
            Log.logger.info("获取居民消费价格指数...")
            self.mTread(tsEcono, 'cn_cpi')
            
            # 获取工业品出厂价格指数
            Log.logger.info("获取工业品出厂价格指数...")
            self.mTread(tsEcono, 'cn_ppi')
            
            # 获取货币供应量
            Log.logger.info("获取货币供应量...")
            self.mTread(tsEcono, 'cn_m')
            
            # 获取美国国债收益率数据
            Log.logger.info("获取美国国债收益率数据...")
            self.mTread(tsEcono, 'us_tycr')
            self.mTread(tsEcono, 'us_trycr')
            self.mTread(tsEcono, 'us_tbr')
            self.mTread(tsEcono, 'us_tltr')
            self.mTread(tsEcono, 'us_trltr')
            
            # 获取经济日历数据
            Log.logger.info("获取经济日历数据...")
            self.mTread(tsEcono, 'eco_cal')
            
            return True
        except Exception as e:
            Log.logger.error(f"获取宏观经济数据时发生错误: {str(e)}")
            return False

    def getFutures(self):
        """获取期货数据"""
        try:
            # 获取期货基本信息
            Log.logger.info("获取期货基本信息...")
            self.mTread(tsFuntures, 'fut_basic')
            
            # 获取期货交易日历
            Log.logger.info("获取期货交易日历...")
            self.mTread(tsFuntures, 'trade_cal')
            
            # 获取期货日线行情
            Log.logger.info("获取期货日线行情...")
            self.mTread(tsFuntures, 'fut_daily')
            
            # 获取期货持仓量
            Log.logger.info("获取期货持仓量...")
            self.mTread(tsFuntures, 'fut_holding')
            
            return True
        except Exception as e:
            Log.logger.error(f"获取期货数据时发生错误: {str(e)}")
            return False

    def getUStock(self):
        """获取美股数据"""
        try:
            # 美股数据暂时被注释，保留接口
            Log.logger.info("美股数据采集已被禁用...")
            # tsUStock.us_basic(self.pro, self.db)
            # self.mTread(tsUStock, 'us_tradecal')
            # self.mTread(tsUStock, 'us_daily')
            return True
        except Exception as e:
            Log.logger.error(f"获取美股数据时发生错误: {str(e)}")
            return False

    def getHStock(self):
        """获取港股数据，依赖于港股基本信息"""
        try:
            # 获取港股基本信息
            Log.logger.info("获取港股基本信息...")
            tsHStock.hk_basic(self.pro, self.db)
            self.dependency_status['hk_basic'] = True
            
            # 检查港股基本信息是否获取成功
            if not self.check_dependency('hk_basic'):
                Log.logger.error("获取港股数据失败：港股基本信息数据不存在")
                return False
                
            # 获取港股交易日历
            Log.logger.info("获取港股交易日历...")
            self.mTread(tsHStock, 'hk_tradecal', 'hk_basic')
            
            # 获取港股行情
            Log.logger.info("获取港股行情...")
            self.mTread(tsHStock, 'hk_daily', 'hk_basic')
            
            return True
        except Exception as e:
            Log.logger.error(f"获取港股数据时发生错误: {str(e)}")
            return False

    def getCB(self):
        """获取可转债数据，依赖于可转债基本信息"""
        try:
            # 获取可转债基本信息
            Log.logger.info("获取可转债基本信息...")
            tsCB.cb_basic(self.pro, self.db)
            self.dependency_status['cb_basic'] = True
            
            # 检查可转债基本信息是否获取成功
            if not self.check_dependency('cb_basic'):
                Log.logger.error("获取可转债数据失败：可转债基本信息数据不存在")
                return False
                
            # 获取可转债发行
            Log.logger.info("获取可转债发行...")
            self.mTread(tsCB, 'cb_issue', 'cb_basic')
            
            # 获取可转债赎回
            Log.logger.info("获取可转债赎回...")
            self.mTread(tsCB, 'cb_call', 'cb_basic')
            
            # 获取可转债行情
            Log.logger.info("获取可转债行情...")
            self.mTread(tsCB, 'cb_daily', 'cb_basic')
            
            # 暂时不获取的数据
            # self.mTread(tsCB, 'cb_price_chg')
            
            return True
        except Exception as e:
            Log.logger.error(f"获取可转债数据时发生错误: {str(e)}")
            return False    
    
    def getFX(self):
        """获取外汇数据，依赖于外汇基本信息"""
        try:
            # 获取外汇基本信息
            Log.logger.info("获取外汇基本信息...")
            tsFX.fx_basic(self.pro, self.db)
            self.dependency_status['fx_basic'] = True
            
            # 检查外汇基本信息是否获取成功
            if not self.check_dependency('fx_basic'):
                Log.logger.error("获取外汇数据失败：外汇基本信息数据不存在")
                return False
                
            # 获取外汇行情
            Log.logger.info("获取外汇行情...")
            self.mTread(tsFX, 'fx_daily', 'fx_basic')
            
            return True
        except Exception as e:
            Log.logger.error(f"获取外汇数据时发生错误: {str(e)}")
            return False   

    def getOther(self):
        """获取其他数据"""
        try:
            # 暂时不获取的数据
            Log.logger.info("其他数据采集已被禁用...")
            # self.mTread(tsOther, 'cctv_news')
            return True
        except Exception as e:
            Log.logger.error(f"获取其他数据时发生错误: {str(e)}")
            return False
    
    def mTread(self, className, functionName, dependency_check=None):
        """
        创建并启动数据采集线程
        
        Args:
            className: 数据采集类
            functionName: 要调用的函数名
            dependency_check: 依赖检查的表名，如果为None则不检查
        """
        # 检查依赖是否满足
        if dependency_check and not self.check_dependency(dependency_check):
            Log.logger.warning(f"跳过线程 {functionName}，因为依赖 {dependency_check} 未满足")
            return
        
        # 控制并发线程数量 - 检查当前活动线程数
        max_concurrent_threads = 20  # 最大并发线程数
        active_threads = sum(1 for t in self.thread_list if t.is_alive())
        
        if active_threads >= max_concurrent_threads:
            # 超过最大并发线程数，等待部分线程完成
            Log.logger.info(f"已达到最大并发线程数({max_concurrent_threads})，等待部分线程完成后再启动 {functionName}")
            
            # 等待任意线程完成
            while active_threads >= max_concurrent_threads:
                # 清理已完成的线程
                completed_threads = [t for t in self.thread_list if not t.is_alive()]
                for t in completed_threads:
                    if t in self.thread_list:
                        self.thread_list.remove(t)
                
                # 重新计算活动线程数
                active_threads = sum(1 for t in self.thread_list if t.is_alive())
                
                # 如果仍然超过限制，等待一段时间
                if active_threads >= max_concurrent_threads:
                    time.sleep(1)
        
        # 创建新线程
        thread = collectThread(className, functionName, self.pro, self.db)
        thread.set_max_runtime(self.max_thread_runtime)  # 设置最大运行时间
        Log.logger.info(f"创建线程: {functionName}，最大运行时间: {self.max_thread_runtime/3600}小时")
        
        # 添加到线程列表，但不立即启动
        self.thread_list.append(thread)
        
        # 短暂等待，避免同时启动过多线程导致数据库锁定
        time.sleep(0.1)
    
    def _get_date_range(self):
        """获取数据修复的日期范围，支持从参数中读取或使用默认值"""
        try:
            # 尝试从args中获取日期参数
            start_date_param = getattr(self.args, 'start_date', '') if hasattr(self, 'args') else ''
            end_date_param = getattr(self.args, 'end_date', '') if hasattr(self, 'args') else ''
            
            # 处理start_date
            if start_date_param and start_date_param.strip():
                start_date = self._parse_date(start_date_param.strip())
            else:
                # 默认使用10年前
                end_date = datetime.now().date()
                start_date = end_date - timedelta(days=365 * 10)
            
            # 处理end_date  
            if end_date_param and end_date_param.strip():
                end_date = self._parse_date(end_date_param.strip())
            else:
                # 默认使用今天
                end_date = datetime.now().date()
            
            # 如果只指定了start_date但没有指定end_date，确保end_date是今天
            if start_date_param and start_date_param.strip() and (not end_date_param or not end_date_param.strip()):
                end_date = datetime.now().date()
                
            return start_date, end_date
            
        except Exception as e:
            Log.logger.warning(f"解析日期参数时出错: {str(e)}，使用默认的10年范围")
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=365 * 10)
            return start_date, end_date
    
    def _parse_date(self, date_str):
        """解析日期字符串，支持YYYY-MM-DD或YYYYMMDD格式"""
        try:
            # 去除空格
            date_str = date_str.strip()
            
            # 尝试解析YYYY-MM-DD格式
            if '-' in date_str:
                return datetime.strptime(date_str, '%Y-%m-%d').date()
            
            # 尝试解析YYYYMMDD格式
            elif len(date_str) == 8 and date_str.isdigit():
                return datetime.strptime(date_str, '%Y%m%d').date()
            
            else:
                raise ValueError(f"不支持的日期格式: {date_str}")
                
        except Exception as e:
            raise ValueError(f"解析日期'{date_str}'失败: {str(e)}")
    
    def fix(self):
        """检测并修复丢失的数据，限于近10年的数据"""
        try:
            Log.logger.info("开始数据修复检测...")
            
            # 初始化Tushare API
            cfgTS = Config.get_config('ts')
            ts.set_token(cfgTS['token'])
            self.pro = ts.pro_api()
            self.db = cfgTS['db']
            
            # 确保数据库目录存在
            if not self.ensure_db_directory(self.db):
                Log.logger.error("数据库目录检查失败，终止数据修复")
                return False
            
            # 定义数据映射关系
            data_mappings = {
                'cn_stock': {
                    'calendar': 'astock_trade_cal',
                    'collector_class': tsAStockPrice,
                    'table_method_mappings': {
                        'astock_price_daily': 'daily',
                        'astock_price_daily_basic': 'daily_basic'
                    }
                },
                'cn_fund': {
                    'calendar': 'astock_trade_cal',
                    'collector_class': tsFund,
                    'table_method_mappings': {
                        'fund_daily': 'fund_daily'
                    }
                },
                'cn_index': {
                    'calendar': 'astock_trade_cal',
                    'collector_class': tsAStockIndex,
                    'table_method_mappings': {
                        'astock_index_daily': 'index_daily'
                    }
                },
                'cn_cb': {
                    'calendar': 'astock_trade_cal',
                    'collector_class': tsCB,
                    'table_method_mappings': {
                        'cb_daily': 'cb_daily'
                    }
                },
                'cn_future': {
                    'calendar': 'futures_trade_cal',
                    'collector_class': tsFuntures,
                    'table_method_mappings': {
                        'futures_daily': 'fut_daily'
                    }
                },
                'fx_daily': {
                    'calendar': None,  # 外汇没有日历，每天都有数据
                    'collector_class': tsFX,
                    'table_method_mappings': {
                        'fx_daily': 'fx_daily'
                    }
                }
            }
            
            # 获取日期范围 - 支持从参数中读取或使用默认的10年范围
            start_date, end_date = self._get_date_range()
            
            Log.logger.info(f"检测时间范围: {start_date} 至 {end_date}")
            
            # 统计修复结果
            fix_results = {}
            
            # 遍历每种数据类型进行检测和修复
            total_markets = len(data_mappings)
            for i, (market_type, config) in enumerate(data_mappings.items(), 1):
                Log.logger.info(f"开始检测 {market_type} 数据... ({i}/{total_markets})")
                print(f"\n{'='*60}")
                print(f"正在处理: {market_type.upper()} ({i}/{total_markets})")
                print('='*60)
                
                fix_results[market_type] = self._fix_market_data(
                    market_type, config, start_date, end_date
                )
                
                # 显示当前市场的简要结果
                result = fix_results[market_type]
                print(f"✓ {market_type} 处理完成:")
                print(f"  - 总交易日: {result['total_trading_days']}")
                print(f"  - 缺失日期: {len(result['missing_days'])}")
                print(f"  - 修复成功: {len(result['fixed_days'])}")
                print(f"  - 修复失败: {len(result['failed_days'])}")
                print(f"  - 质量警告: {len(result['quality_warnings'])}")
                
                if i < total_markets:
                    Log.logger.info(f"即将开始下一个市场数据检测...")
            
            # 输出修复结果
            self._print_fix_summary(fix_results)
            
            Log.logger.info("数据修复检测完成")
            return True
            
        except Exception as e:
            Log.logger.error(f"数据修复检测失败: {str(e)}")
            Log.logger.error(traceback.format_exc())
            return False

    def _get_trading_dates(self, calendar_table, start_date, end_date):
        """获取指定时间范围内的交易日日期集合"""
        if calendar_table is None:
            # 外汇数据每天都有，生成所有日期
            dates = []
            current_date = start_date
            while current_date <= end_date:
                dates.append(current_date.strftime('%Y%m%d'))
                current_date += timedelta(days=1)
            return dates
        
        try:
            # 查询交易日历表
            sql = f"""
            SELECT DISTINCT cal_date 
            FROM {calendar_table} 
            WHERE cal_date >= '{start_date.strftime('%Y%m%d')}' 
            AND cal_date <= '{end_date.strftime('%Y%m%d')}' 
            AND is_open = '1'
            ORDER BY cal_date
            """
            results = DB.select_to_list(sql, self.db)
            return [row['cal_date'] for row in results]
        except Exception as e:
            Log.logger.error(f"获取交易日期失败: {str(e)}")
            return []

    def _fix_market_data(self, market_type, config, start_date, end_date):
        """修复特定市场的数据"""
        fix_result = {
            'total_trading_days': 0,
            'missing_days': [],
            'fixed_days': [],
            'failed_days': [],
            'quality_warnings': []
        }
        
        try:
            # 获取交易日日期集合
            trading_dates = self._get_trading_dates(config['calendar'], start_date, end_date)
            fix_result['total_trading_days'] = len(trading_dates)
            
            if not trading_dates:
                Log.logger.warning(f"{market_type}: 未找到交易日日期")
                return fix_result
            
            Log.logger.info(f"{market_type}: 共找到 {len(trading_dates)} 个交易日")
            
            # 检查每个表的数据完整性，使用正确的表和方法映射
            for table_name, method_name in config['table_method_mappings'].items():
                Log.logger.info(f"{market_type}: 检查表 {table_name}（对应方法：{method_name}）")
                
                missing_dates = self._check_missing_data(table_name, trading_dates)
                fix_result['missing_days'].extend(missing_dates)
                
                # 检查数据质量（ts_code数量变化）
                quality_warnings = self._check_data_quality(table_name, trading_dates)
                fix_result['quality_warnings'].extend(quality_warnings)
                
                # 修复缺失的数据
                if missing_dates:
                    Log.logger.info(f"{market_type}: 表 {table_name} 发现 {len(missing_dates)} 个缺失交易日，使用方法 {method_name} 开始修复...")
                    
                    if hasattr(config['collector_class'], method_name):
                        fixed_dates, failed_dates = self._fix_missing_dates(
                            config['collector_class'], method_name, missing_dates, table_name
                        )
                        fix_result['fixed_days'].extend(fixed_dates)
                        fix_result['failed_days'].extend(failed_dates)
                    else:
                        Log.logger.warning(f"{market_type}: 方法 {method_name} 不存在")
                
                # 处理质量异常数据
                if quality_warnings:
                    should_fix_quality = self._handle_quality_warnings(market_type, table_name, quality_warnings)
                    
                    if should_fix_quality:
                        if hasattr(config['collector_class'], method_name):
                            fixed_quality_dates, failed_quality_dates = self._fix_quality_warnings(
                                config['collector_class'], method_name, quality_warnings, table_name
                            )
                            fix_result['fixed_days'].extend(fixed_quality_dates)
                            fix_result['failed_days'].extend(failed_quality_dates)
                            
                            # 统计质量修复结果
                            if 'quality_fixed' not in fix_result:
                                fix_result['quality_fixed'] = []
                            if 'quality_failed' not in fix_result:
                                fix_result['quality_failed'] = []
                            fix_result['quality_fixed'].extend(fixed_quality_dates)
                            fix_result['quality_failed'].extend(failed_quality_dates)
                        else:
                            Log.logger.warning(f"{market_type}: 方法 {method_name} 不存在，无法修复质量异常")
                    else:
                        Log.logger.info(f"{market_type}: 跳过表 {table_name} 的质量异常修复")
                else:
                    Log.logger.info(f"{market_type}: 表 {table_name} 无质量异常")
        
        except Exception as e:
            Log.logger.error(f"{market_type}: 数据修复过程中发生错误: {str(e)}")
        
        return fix_result

    def _check_missing_data(self, table_name, trading_dates):
        """检查缺失的数据"""
        missing_dates = []
        
        try:
            # 检查表是否存在
            adapter = DB.get_adapter(self.db)
            if not adapter.table_exists(table_name):
                Log.logger.warning(f"表 {table_name} 不存在，所有交易日都缺失")
                return trading_dates
            
            # 检查每个交易日是否有数据 - 批量处理提高效率
            Log.logger.info(f"开始检查 {len(trading_dates)} 个交易日的数据...")
            
            # 批量查询缺失日期，更高效
            if trading_dates:
                dates_str = "','".join(trading_dates)
                batch_sql = f"""
                SELECT trade_date, COUNT(*) as count 
                FROM {table_name} 
                WHERE trade_date IN ('{dates_str}')
                GROUP BY trade_date
                """
                
                try:
                    existing_dates = {}
                    result = DB.select_to_list(batch_sql, self.db)
                    for row in result:
                        if row['count'] > 0:
                            existing_dates[row['trade_date']] = row['count']
                    
                    # 找出缺失的日期
                    for i, trade_date in enumerate(trading_dates):
                        if trade_date not in existing_dates:
                            missing_dates.append(trade_date)
                        
                        # 每检查100个日期显示一次进度
                        if (i + 1) % 100 == 0 or (i + 1) == len(trading_dates):
                            progress = (i + 1) / len(trading_dates) * 100
                            Log.logger.info(f"数据检查进度: {i + 1}/{len(trading_dates)} ({progress:.1f}%)")
                            print(f"  数据完整性检查: {progress:.1f}% ({i + 1}/{len(trading_dates)})")
                            
                except Exception as e:
                    Log.logger.error(f"批量检查数据时出错: {str(e)}，回退到逐个检查")
                    # 如果批量查询失败，回退到逐个检查
                    missing_dates = []
                    for i, trade_date in enumerate(trading_dates):
                        sql = f"SELECT COUNT(*) as count FROM {table_name} WHERE trade_date = '{trade_date}'"
                        try:
                            result = DB.select_to_list(sql, self.db)
                            if result and result[0]['count'] == 0:
                                missing_dates.append(trade_date)
                        except Exception as e:
                            Log.logger.error(f"检查日期 {trade_date} 数据时出错: {str(e)}")
                            missing_dates.append(trade_date)
                        
                        # 每检查100个日期显示一次进度
                        if (i + 1) % 100 == 0 or (i + 1) == len(trading_dates):
                            progress = (i + 1) / len(trading_dates) * 100
                            Log.logger.info(f"数据检查进度: {i + 1}/{len(trading_dates)} ({progress:.1f}%)")
                            print(f"  数据完整性检查(逐个): {progress:.1f}% ({i + 1}/{len(trading_dates)})")
            
            Log.logger.info(f"数据检查完成，发现 {len(missing_dates)} 个缺失日期")
        
        except Exception as e:
            Log.logger.error(f"检查表 {table_name} 缺失数据时出错: {str(e)}")
            
        return missing_dates

    def _check_data_quality(self, table_name, trading_dates):
        """检查数据质量，识别ts_code数量异常变化"""
        warnings = []
        
        try:
            # 检查表是否存在
            adapter = DB.get_adapter(self.db)
            if not adapter.table_exists(table_name):
                return warnings
            
            prev_count = None
            prev_date = None
            
            Log.logger.info(f"开始数据质量检查...")
            
            for i, trade_date in enumerate(trading_dates):
                sql = f"SELECT COUNT(DISTINCT ts_code) as count FROM {table_name} WHERE trade_date = '{trade_date}'"
                try:
                    result = DB.select_to_list(sql, self.db)
                    if result:
                        current_count = result[0]['count']
                        
                        if prev_count is not None and current_count > 0 and prev_count > 0:
                            # 计算变化百分比
                            change_percent = abs(current_count - prev_count) / prev_count
                            
                            # 如果变化超过10%，且当天以及上个交易日均非周末
                            if change_percent > 0.1:
                                # 检查当天是否为周末
                                current_date_obj = datetime.strptime(trade_date, '%Y%m%d').date()
                                prev_date_obj = datetime.strptime(prev_date, '%Y%m%d').date()
                                
                                # 只有当前日期和上个交易日都不是周末时才提示
                                if current_date_obj.weekday() < 5 and prev_date_obj.weekday() < 5:  
                                    warning = {
                                        'table': table_name,
                                        'date': trade_date,
                                        'prev_date': prev_date,
                                        'current_count': current_count,
                                        'prev_count': prev_count,
                                        'change_percent': change_percent * 100
                                    }
                                    warnings.append(warning)
                        
                        prev_count = current_count
                        prev_date = trade_date
                
                except Exception as e:
                    Log.logger.error(f"检查日期 {trade_date} 数据质量时出错: {str(e)}")
                
                # 每检查50个日期显示一次进度
                if (i + 1) % 50 == 0 or (i + 1) == len(trading_dates):
                    progress = (i + 1) / len(trading_dates) * 100
                    Log.logger.info(f"质量检查进度: {i + 1}/{len(trading_dates)} ({progress:.1f}%) [当前: {trade_date}]")
                    print(f"  质量检查进度: {progress:.1f}% ({i + 1}/{len(trading_dates)})")
            
            Log.logger.info(f"数据质量检查完成，发现 {len(warnings)} 个质量警告")
            
            # 输出详细的警告信息
            if warnings:
                Log.logger.warning(f"数据质量警告详情:")
                for i, warning in enumerate(warnings, 1):
                    Log.logger.warning(f"  警告 {i}: 表 {warning['table']} 在 {warning['date']} 的记录数从 {warning['prev_date']} 的 {warning['prev_count']} 条变化到 {warning['current_count']} 条，变化幅度 {warning['change_percent']:.1f}%")
        
        except Exception as e:
            Log.logger.error(f"检查表 {table_name} 数据质量时出错: {str(e)}")
            
        return warnings

    def _handle_quality_warnings(self, market_type, table_name, quality_warnings):
        """处理数据质量警告，根据auto参数决定是否需要确认"""
        if not quality_warnings:
            return False
        
        # 检查是否开启自动修复模式
        auto_fix = getattr(self.args, 'auto', 'false').lower() == 'true'
        
        print(f"\n{'='*60}")
        print(f"发现 {market_type} 市场表 {table_name} 的质量异常数据")
        print(f"{'='*60}")
        
        # 显示质量警告详情
        print(f"质量异常详情（共{len(quality_warnings)}项）:")
        for i, warning in enumerate(quality_warnings[:5], 1):  # 最多显示前5个
            print(f"  {i}. 日期 {warning['date']}: 记录数从 {warning['prev_count']} 条变为 {warning['current_count']} 条")
            print(f"     变化幅度: {warning['change_percent']:.1f}% (相比 {warning['prev_date']})")
        
        if len(quality_warnings) > 5:
            print(f"  ... 还有 {len(quality_warnings) - 5} 项异常未显示")
        
        # 自动模式直接修复
        if auto_fix:
            print(f"\n✅ 自动修复模式已开启，将自动修复这些质量异常数据")
            Log.logger.info(f"{market_type}: 自动修复模式，开始修复表 {table_name} 的 {len(quality_warnings)} 个质量异常")
            return True
        
        # 交互模式需要用户确认
        print(f"\n❓ 是否修复这些质量异常数据？")
        print("   y/Y/yes - 修复这些异常数据")
        print("   n/N/no  - 跳过质量异常修复")
        print("   提示: 可使用 --auto=true 参数启用自动修复模式")
        
        while True:
            try:
                choice = input(f"\n请选择 (y/n): ").strip().lower()
                if choice in ['y', 'yes']:
                    print(f"✅ 确认修复表 {table_name} 的质量异常数据")
                    Log.logger.info(f"{market_type}: 用户确认修复表 {table_name} 的质量异常数据")
                    return True
                elif choice in ['n', 'no']:
                    print(f"⏭️ 跳过表 {table_name} 的质量异常修复")
                    Log.logger.info(f"{market_type}: 用户选择跳过表 {table_name} 的质量异常修复")
                    return False
                else:
                    print("❌ 无效选择，请输入 y 或 n")
            except KeyboardInterrupt:
                print(f"\n\n⏹️ 用户中断操作，跳过质量异常修复")
                Log.logger.info(f"{market_type}: 用户中断操作，跳过质量异常修复")
                return False
            except Exception as e:
                print(f"❌ 输入处理错误: {str(e)}，跳过质量异常修复")
                Log.logger.warning(f"{market_type}: 输入处理错误 {str(e)}，跳过质量异常修复")
                return False

    def _fix_quality_warnings(self, collector_class, method_name, quality_warnings, table_name):
        """修复质量异常数据"""
        if not quality_warnings:
            return [], []
        
        fixed_dates = []
        failed_dates = []
        
        # 提取需要修复的日期
        warning_dates = [warning['date'] for warning in quality_warnings]
        
        Log.logger.info(f"开始修复 {len(warning_dates)} 个质量异常日期的数据")
        
        for warning in quality_warnings:
            trade_date = warning['date']
            retry_count = 0
            max_retries = 2  # 质量异常修复减少重试次数
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    if retry_count > 0:
                        Log.logger.info(f"修复质量异常日期 {trade_date} 的数据... (重试 {retry_count}/{max_retries})")
                    else:
                        Log.logger.info(f"修复质量异常日期 {trade_date} 的数据...")
                        print(f"  修复日期: {trade_date} (记录数异常: {warning['prev_count']} -> {warning['current_count']})")
                    
                    # 删除该日期的现有数据
                    try:
                        DB.exec(f"DELETE FROM {table_name} WHERE trade_date = '{trade_date}'", self.db)
                    except Exception as e:
                        Log.logger.debug(f"删除质量异常日期 {trade_date} 数据时出错: {str(e)}")
                    
                    # 使用对应方法重新获取数据
                    success = self._fix_single_date_data(collector_class, method_name, trade_date, table_name)
                    
                    if success:
                        # 验证修复结果
                        sql = f"SELECT COUNT(*) as count FROM {table_name} WHERE trade_date = '{trade_date}'"
                        result = DB.select_to_list(sql, self.db)
                        if result and result[0]['count'] > 0:
                            fixed_dates.append(trade_date)
                            Log.logger.info(f"成功修复质量异常日期 {trade_date} 的数据")
                            break
                        else:
                            Log.logger.warning(f"修复质量异常日期 {trade_date} 后未发现数据")
                            success = False
                    
                    retry_count += 1
                    if not success and retry_count < max_retries:
                        time.sleep(1)
                        
                except Exception as e:
                    Log.logger.error(f"修复质量异常日期 {trade_date} 时出错: {str(e)}")
                    retry_count += 1
                    if retry_count < max_retries:
                        time.sleep(1)
            
            if not success:
                failed_dates.append(trade_date)
                Log.logger.error(f"修复质量异常日期 {trade_date} 失败，已重试 {max_retries} 次")
        
        return fixed_dates, failed_dates

    def _fix_missing_dates(self, collector_class, method_name, missing_dates, table_name):
        """修复缺失日期的数据"""
        fixed_dates = []
        failed_dates = []
        
        # 过滤出需要修复的日期（排除今天及未来日期）
        today = datetime.now().date()
        valid_missing_dates = []
        
        for trade_date in missing_dates:
            date_obj = datetime.strptime(trade_date, '%Y%m%d').date()
            if date_obj < today:  # 只修复过去的日期
                valid_missing_dates.append(trade_date)
            else:
                Log.logger.info(f"跳过未来日期 {trade_date}（大于等于今天 {today}）")
        
        if not valid_missing_dates:
            Log.logger.info("没有需要修复的历史日期")
            return fixed_dates, failed_dates
        
        Log.logger.info(f"开始修复 {len(valid_missing_dates)} 个历史缺失日期")
        
        for trade_date in valid_missing_dates:
            retry_count = 0
            max_retries = 3
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    if retry_count > 0:
                        Log.logger.info(f"修复日期 {trade_date} 的数据... (重试 {retry_count}/{max_retries})")
                    else:
                        Log.logger.info(f"修复日期 {trade_date} 的数据...")
                    
                    # 删除该日期的现有数据（如果有）
                    try:
                        DB.exec(f"DELETE FROM {table_name} WHERE trade_date = '{trade_date}'", self.db)
                    except Exception as e:
                        Log.logger.debug(f"删除日期 {trade_date} 数据时出错: {str(e)}")
                    
                    # 使用特殊的按日期修复方法
                    success = self._fix_single_date_data(collector_class, method_name, trade_date, table_name)
                    
                    if success:
                        # 验证数据是否已成功写入
                        sql = f"SELECT COUNT(*) as count FROM {table_name} WHERE trade_date = '{trade_date}'"
                        result = DB.select_to_list(sql, self.db)
                        if result and result[0]['count'] > 0:
                            fixed_dates.append(trade_date)
                            Log.logger.info(f"成功修复日期 {trade_date} 的数据")
                            break
                        else:
                            Log.logger.warning(f"修复日期 {trade_date} 后未发现数据")
                            success = False
                    
                    if not success:
                        retry_count += 1
                        if retry_count < max_retries:
                            Log.logger.warning(f"修复日期 {trade_date} 失败，将重试...")
                            time.sleep(1)  # 重试前等待更长时间
                    
                except Exception as e:
                    retry_count += 1
                    Log.logger.error(f"修复日期 {trade_date} 时发生错误 (尝试 {retry_count}/{max_retries}): {str(e)}")
                    if retry_count < max_retries:
                        time.sleep(1)
            
            if not success:
                failed_dates.append(trade_date)
                Log.logger.error(f"修复日期 {trade_date} 失败，已重试 {max_retries} 次，跳过此日期")
            
            # 短暂等待，避免API限制
            time.sleep(0.5)
        
        return fixed_dates, failed_dates

    def _fix_single_date_data(self, collector_class, method_name, trade_date, table_name):
        """修复单个日期的数据，使用特殊的历史数据获取方法"""
        try:
            # 对于大多数Tushare API方法，需要使用特殊的按日期获取方式
            # 这里需要根据不同的方法使用不同的策略
            
            if hasattr(collector_class, method_name):
                method = getattr(collector_class, method_name)
                
                # 检查是否是外汇数据 - 外汇数据需要特殊处理
                if 'fx_daily' in method_name.lower():
                    # 对于外汇数据，使用helper的方法按日期获取
                    from finhack.collector.tushare.helper import tsSHelper
                    # 外汇数据使用特殊的按日期获取方式
                    try:
                        # 调用tsSHelper的按日期获取方法
                        df = self.pro.fx_daily(trade_date=trade_date)
                        if df is not None and not df.empty:
                            # 直接写入数据库
                            adapter = DB.get_adapter(self.db)
                            adapter.to_sql(df, table_name, if_exists='append')
                            Log.logger.debug(f"成功获取并写入 {trade_date} 的外汇数据，共 {len(df)} 条记录")
                            return True
                        else:
                            Log.logger.warning(f"日期 {trade_date} 的外汇数据为空")
                            return False
                    except Exception as e:
                        Log.logger.error(f"获取日期 {trade_date} 的外汇数据时出错: {str(e)}")
                        return False
                else:
                    # 对于支持单日期修复的方法，使用新的target_date参数进行单日期修复
                    if method_name in ['daily', 'daily_basic']:
                        Log.logger.info(f"使用增强的{method_name}方法修复日期 {trade_date} 的数据")
                        return method(self.pro, self.db, target_date=trade_date)
                    else:
                        # 对于其他类型的数据，尝试调用原方法
                        # 注意：大多数Tushare API无法获取历史特定日期的数据
                        # 这里仅作为fallback，实际效果可能有限
                        Log.logger.warning(f"方法 {method_name} 可能无法获取历史日期 {trade_date} 的数据，尝试调用...")
                        return method(self.pro, self.db)
            else:
                Log.logger.error(f"方法 {method_name} 不存在于类 {collector_class}")
                return False
                
        except Exception as e:
            Log.logger.error(f"修复单日期数据时出错: {str(e)}")
            return False

    def _print_fix_summary(self, fix_results):
        """打印修复结果摘要"""
        print("\n" + "="*80)
        print("数据修复检测结果摘要")
        print("="*80)
        
        total_missing = 0
        total_fixed = 0
        total_failed = 0
        total_warnings = 0
        total_quality_fixed = 0
        total_quality_failed = 0
        
        for market_type, result in fix_results.items():
            print(f"\n【{market_type.upper()}】")
            print(f"  总交易日数: {result['total_trading_days']}")
            print(f"  缺失日期数: {len(result['missing_days'])}")
            print(f"  修复成功数: {len(result['fixed_days'])}")
            print(f"  修复失败数: {len(result['failed_days'])}")
            print(f"  质量警告数: {len(result['quality_warnings'])}")
            
            # 显示质量修复统计
            if 'quality_fixed' in result and result['quality_fixed']:
                print(f"  质量修复成功数: {len(result['quality_fixed'])}")
            if 'quality_failed' in result and result['quality_failed']:
                print(f"  质量修复失败数: {len(result['quality_failed'])}")
            
            # 显示质量警告详情（限制显示数量）
            if result['quality_warnings']:
                print("  质量警告详情:")
                display_count = min(3, len(result['quality_warnings']))
                for i, warning in enumerate(result['quality_warnings'][:display_count]):
                    print(f"    - {warning['date']}: ts_code数量从{warning['prev_count']}变化到{warning['current_count']} "
                          f"(变化{warning['change_percent']:.1f}%)")
                if len(result['quality_warnings']) > display_count:
                    print(f"    - ... 还有 {len(result['quality_warnings']) - display_count} 个质量警告")
            
            total_missing += len(result['missing_days'])
            total_fixed += len(result['fixed_days'])
            total_failed += len(result['failed_days'])
            total_warnings += len(result['quality_warnings'])
            
            # 累加质量修复统计
            if 'quality_fixed' in result:
                total_quality_fixed += len(result['quality_fixed'])
            if 'quality_failed' in result:
                total_quality_failed += len(result['quality_failed'])
        
        print(f"\n【总计】")
        print(f"  总缺失日期数: {total_missing}")
        print(f"  总修复成功数: {total_fixed}")
        print(f"  总修复失败数: {total_failed}")
        print(f"  总质量警告数: {total_warnings}")
        
        if total_quality_fixed > 0:
            print(f"  质量修复成功数: {total_quality_fixed}")
        if total_quality_failed > 0:
            print(f"  质量修复失败数: {total_quality_failed}")
        
        if total_failed > 0:
            print(f"\n⚠️  有 {total_failed} 个日期修复失败，建议检查网络连接和API限制")
        
        if total_warnings > 0:
            print(f"\n⚠️  发现 {total_warnings} 个数据质量异常，建议进一步检查")
        
        if total_missing == 0:
            print(f"\n✅ 所有数据完整，无需修复")
        elif total_fixed == total_missing:
            print(f"\n✅ 所有缺失数据已成功修复")
        
        print("="*80)

    def count(self):
        """统计分析Tushare数据库"""
        try:
            # 导入数据库分析器
            from finhack.library.db_analyzer import DatabaseAnalyzer
            
            Log.logger.info("开始Tushare数据库统计分析...")
            
            # 获取数据库配置
            cfgTS = Config.get_config('ts')
            db_name = cfgTS['db']
            
            print(f"开始分析数据库: {db_name}")
            print("-" * 80)
            
            # 创建数据库分析器
            analyzer = DatabaseAnalyzer(db_name)
            
            # 显示数据库基本信息
            file_size = analyzer.get_database_file_size()
            if file_size:
                print(f"数据库文件大小: {file_size:.2f} GB")
                print("-" * 80)
            
            # 分析所有表
            start_time = time.time()
            results = analyzer.analyze_all_tables()
            total_time = time.time() - start_time
            
            print(f"\n分析完成! 总耗时: {total_time:.2f} 秒")
            print("-" * 80)
            
            # 生成并显示报告
            report = analyzer.generate_report(results)
            print(report)
            
            # 保存报告和数据
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # 保存到项目的test目录（如果存在）或数据目录
            if os.path.exists(BASE_DIR + "/test"):
                report_dir = BASE_DIR + "/test"
            else:
                report_dir = BASE_DIR + "/data/reports"
                os.makedirs(report_dir, exist_ok=True)
            
            report_file = f"{report_dir}/tushare_db_analysis_report_{timestamp}.txt"
            csv_file = f"{report_dir}/tushare_db_analysis_data_{timestamp}.csv"
            
            # 保存文本报告
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            print(f"\n报告已保存到: {report_file}")
            
            # 保存CSV数据
            analyzer.save_to_csv(results, csv_file)
            
            Log.logger.info("Tushare数据库分析完成")
            
        except Exception as e:
            Log.logger.error(f"Tushare数据库分析失败: {e}")
            import traceback
            traceback.print_exc()
            print(f"分析过程中发生错误: {e}")
            return False
        
        return True








