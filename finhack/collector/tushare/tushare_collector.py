import sys
import time
from finhack.library.db import DB
from finhack.library.config import Config
from finhack.library.thread import collectThread
from finhack.collector.tushare.astockbasic import tsAStockBasic
from finhack.collector.tushare.astockprice import tsAStockPrice
from finhack.collector.tushare.astockfinance import tsAStockFinance
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

class TushareCollector:
    def __init__(self):
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
            
            # 如果是相对路径，尝试转换为绝对路径
            if not os.path.isabs(db_path):
                # 当前工作目录
                cwd = os.getcwd()
                abs_db_path = os.path.abspath(os.path.join(cwd, db_path))
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
        """获取A股财务数据，依赖于股票基本信息和财务披露日期"""
        try:
            # 获取财务披露日期
            Log.logger.info("获取财务披露日期...")
            tsAStockFinance.disclosure_date(self.pro, self.db)
            self.dependency_status['astock_finance_disclosure_date'] = True
            
            # 检查财务披露日期是否获取成功
            if not self.check_dependency('astock_finance_disclosure_date'):
                Log.logger.error("获取财务数据失败：财务披露日期数据不存在")
                return False
                
            # 获取利润表
            Log.logger.info("获取利润表...")
            self.mTread(tsAStockFinance, 'income', 'astock_finance_disclosure_date')
            
            # 获取资产负债表
            Log.logger.info("获取资产负债表...")
            self.mTread(tsAStockFinance, 'balancesheet', 'astock_finance_disclosure_date')
            
            # 获取现金流量表
            Log.logger.info("获取现金流量表...")
            self.mTread(tsAStockFinance, 'cashflow', 'astock_finance_disclosure_date')
            
            # 获取业绩预告
            Log.logger.info("获取业绩预告...")
            self.mTread(tsAStockFinance, 'forecast', 'astock_finance_disclosure_date')
            
            # 获取业绩快报
            Log.logger.info("获取业绩快报...")
            self.mTread(tsAStockFinance, 'express', 'astock_finance_disclosure_date')
            
            # 获取财务指标数据
            Log.logger.info("获取财务指标数据...")
            self.mTread(tsAStockFinance, 'fina_indicator', 'astock_finance_disclosure_date')
            
            # 获取财务审计意见
            Log.logger.info("获取财务审计意见...")
            self.mTread(tsAStockFinance, 'fina_audit', 'astock_finance_disclosure_date')
            
            # 获取主营业务构成
            Log.logger.info("获取主营业务构成...")
            self.mTread(tsAStockFinance, 'fina_mainbz', 'astock_finance_disclosure_date')
            
            # 获取分红送股数据
            Log.logger.info("获取分红送股数据...")
            self.mTread(tsAStockFinance, 'dividend', 'astock_finance_disclosure_date')
            
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
            
            # 获取概念股列表
            Log.logger.info("获取概念股列表...")
            self.mTread(tsAStockMarket, 'concept_detail', 'astock_basic')
            
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
            
            # 获取前十大股东
            Log.logger.info("获取前十大股东...")
            self.mTread(tsAStockFinance, 'top10_holders', 'astock_basic')
            
            # 获取前十大流通股东
            Log.logger.info("获取前十大流通股东...")
            self.mTread(tsAStockFinance, 'top10_floatholders', 'astock_basic')
            
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








