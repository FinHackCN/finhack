import sys
import time
from finhack.library.mydb import mydb
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
            'hk_basic': False,              # 港股基本信息
            'fx_basic': False               # 外汇基本信息
        }
        
    def check_dependency(self, table_name):
        """检查依赖表是否存在"""
        if table_name in self.dependency_status:
            return self.dependency_status[table_name]
        else:
            # 检查表是否存在
            try:
                result = mydb.selectToList(f"SELECT 1 FROM {table_name} LIMIT 1", self.db)
                return len(result) > 0
            except Exception as e:
                Log.logger.warning(f"检查表 {table_name} 失败: {str(e)}")
                return False
        
    def run(self):
        """按照依赖关系顺序执行数据采集"""
        Log.logger.info("开始执行Tushare数据采集...")
        
        # 初始化Tushare API
        cfgTS=Config.get_config('ts')
        ts.set_token(cfgTS['token'])
        self.pro = ts.pro_api()
        self.db=cfgTS['db']
        self.engine=mydb.getDBEngine(cfgTS['db'])
        
        # 第一步：获取基础数据（股票、交易日历等）
        # 这些数据是其他数据的基础依赖
        Log.logger.info("第一步：获取基础数据...")
        success = self.getAStockBasic()
        if not success:
            Log.logger.error("获取股票基本信息失败，终止数据采集")
            return False
            
        # 第二步：获取行情数据
        # 依赖于股票基本信息
        Log.logger.info("第二步：获取行情数据...")
        if self.check_dependency('astock_basic'):
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
    
        cfgTS=Config.get_config('ts')
        db=cfgTS['db']
        
        tables_list=mydb.selectToList('show tables',db)
        for v in tables_list:
            table=list(v.values())[0]
            tsSHelper.setIndex(table,db)    
    
    
    def save(self):
        """将数据库中的数据导出到CSV文件"""
        import os
        import pandas as pd
        from datetime import datetime
        
        Log.logger.info("开始导出数据到CSV文件...")
        
        # 初始化配置
        cfgTS = Config.get_config('ts')
        self.db = cfgTS['db']
        # self.engine = mydb.getDBEngine(cfgTS['db'])
        
        # 定义数据表与dataname的映射关系
        table_dataname_map = {
            'astock_price_daily': 'cn_stock',
            'astock_index_daily': 'cn_index',
            'cb_daily': 'cn_cb',
            'fund_daily': 'cn_fund',
            'fx_daily': 'global_fx',
            'hk_daily': 'hk_stock'
        }
        
        # 基础目录
        base_dir = "/Users/woldy/Code/demo_project/data"
        
        # 遍历所有需要导出的表
        for table_name, dataname in table_dataname_map.items():
            try:
                Log.logger.info(f"开始处理表 {table_name}...")
                
                # 检查表是否存在
                check_sql = f"SELECT 1 FROM {table_name} LIMIT 1"
                try:
                    mydb.selectToList(check_sql, self.db)
                except Exception as e:
                    Log.logger.warning(f"表 {table_name} 不存在或无法访问: {str(e)}")
                    continue
                
                # 获取最近30天的数据进行增量更新
                try:
                    # 获取表中最新的交易日期
                    latest_date_sql = f"SELECT MAX(trade_date) as max_date FROM {table_name}"
                    latest_date_result = mydb.selectToList(latest_date_sql, self.db)
                    
                    if not latest_date_result or 'max_date' not in latest_date_result[0] or not latest_date_result[0]['max_date']:
                        Log.logger.warning(f"表 {table_name} 中没有交易日期数据")
                        continue
                        
                    max_date_str = latest_date_result[0]['max_date']
                    max_date = datetime.strptime(max_date_str, '%Y%m%d')
                    start_date = max_date - timedelta(days=30)
                    start_date_str = start_date.strftime('%Y%m%d')
                    
                    data_sql = f"SELECT * FROM {table_name} WHERE trade_date >= '{start_date_str}'"
                except Exception as e:
                    Log.logger.error(f"计算日期范围失败: {str(e)}")
                    continue
                # 这里假设trade_date格式为YYYYMMDD
                data_sql = f"SELECT * FROM {table_name} WHERE trade_date >= '{start_date_str}'"
                df = mydb.selectToDf(data_sql, self.db)
                
                if df.empty:
                    Log.logger.warning(f"表 {table_name} 没有最近数据")
                    continue
                
                # 处理数据格式
                # 将trade_date转换为datetime格式
                df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
                
                # 添加time列，设置为当天的收盘时间（假设为15:00:00）
                df['time'] = df['trade_date'].dt.strftime('%Y-%m-%d') + ' 15:00:00+08:00'
                
                # 重命名列以匹配目标格式
                column_mapping = {
                    'ts_code': 'code',
                    'open': 'open',
                    'high': 'high',
                    'low': 'low',
                    'close': 'close',
                    'vol': 'volume',
                    'amount': 'amount'
                }
                
                # 创建新的DataFrame，只包含需要的列
                new_df = pd.DataFrame()
                new_df['time'] = df['time']
                new_df['code'] = df['ts_code']
                
                # 处理不同表的列名差异
                for target_col, source_col in column_mapping.items():
                    if target_col in df.columns:
                        if target_col == 'open' or target_col == 'high' or target_col == 'low' or target_col == 'close':
                            new_df[source_col] = pd.to_numeric(df[target_col], errors='coerce')
                        elif target_col == 'vol':
                            new_df['volume'] = pd.to_numeric(df[target_col], errors='coerce')
                        elif target_col == 'amount':
                            new_df['amount'] = pd.to_numeric(df[target_col], errors='coerce')
                
                # 按日期分组处理数据
                grouped = df.groupby(df['trade_date'].dt.date)
                
                # 处理timebased数据
                freq = '1d'  # 日频数据
                
                for date, group in grouped:
                    # 提取年、月、日
                    year = date.year
                    month = f"{date.month:02d}"
                    day = f"{date.day:02d}"
                    
                    # 构建目录路径
                    timebased_dir = os.path.join(base_dir, 'market', 'kline', 'timebased', dataname, freq, str(year), month, day)
                    os.makedirs(timebased_dir, exist_ok=True)
                    
                    # 构建文件路径
                    timebased_file = os.path.join(timebased_dir, f"{dataname}_kline_tushare.csv")
                    merged_file = os.path.join(timebased_dir, f"{dataname}_kline_merged.csv")
                    
                    # 获取当天的数据
                    day_df = new_df[new_df['time'].str.startswith(f"{year}-{month}-{day}")]
                    
                    if not day_df.empty:
                        # 检查文件是否已存在
                        if os.path.exists(timebased_file):
                            # 读取现有文件
                            existing_df = pd.read_csv(timebased_file, header=None, names=['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                            
                            # 合并数据，去除重复项
                            combined_df = pd.concat([existing_df, day_df])
                            combined_df = combined_df.drop_duplicates(subset=['time', 'code'])
                            
                            # 保存合并后的数据
                            combined_df.to_csv(timebased_file, index=False, header=False)
                            Log.logger.info(f"更新文件: {timebased_file}")
                        else:
                            # 创建新文件
                            day_df.to_csv(timebased_file, index=False, header=False)
                            Log.logger.info(f"创建文件: {timebased_file}")
                        
                        # 处理merged文件
                        if os.path.exists(merged_file):
                            # 读取现有merged文件
                            merged_df = pd.read_csv(merged_file, header=None, names=['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                            
                            # 合并数据，去除重复项
                            combined_merged = pd.concat([merged_df, day_df])
                            combined_merged = combined_merged.drop_duplicates(subset=['time', 'code'])
                            
                            # 保存合并后的数据
                            combined_merged.to_csv(merged_file, index=False, header=False)
                            Log.logger.info(f"更新merged文件: {merged_file}")
                        else:
                            # 创建新merged文件
                            day_df.to_csv(merged_file, index=False, header=False)
                            Log.logger.info(f"创建merged文件: {merged_file}")
                
                # 处理codebased数据
                codebased_dir = os.path.join(base_dir, 'market', 'kline', 'codebased', dataname, freq, str(year))
                os.makedirs(codebased_dir, exist_ok=True)
                
                # 按代码分组
                code_grouped = new_df.groupby('code')
                
                for code, code_group in code_grouped:
                    # 构建文件路径
                    code_file = os.path.join(codebased_dir, f"{code}.csv")
                    
                    # 排序数据
                    code_group = code_group.sort_values('time')
                    
                    # 检查文件是否已存在
                    if os.path.exists(code_file):
                        # 读取现有文件
                        existing_code_df = pd.read_csv(code_file, header=None, names=['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                        
                        # 合并数据，去除重复项
                        combined_code_df = pd.concat([existing_code_df, code_group])
                        combined_code_df = combined_code_df.drop_duplicates(subset=['time'])
                        combined_code_df = combined_code_df.sort_values('time')
                        
                        # 保存合并后的数据
                        combined_code_df.to_csv(code_file, index=False, header=False)
                        Log.logger.info(f"更新代码文件: {code_file}")
                    else:
                        # 创建新文件
                        code_group.to_csv(code_file, index=False, header=False)
                        Log.logger.info(f"创建代码文件: {code_file}")
                
                Log.logger.info(f"表 {table_name} 处理完成")
                
            except Exception as e:
                Log.logger.error(f"处理表 {table_name} 时发生错误: {str(e)}")
                Log.logger.error(f"错误详情: {traceback.format_exc()}")
        
        Log.logger.info("数据导出完成")
        return True
        

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
            
            # 获取沪深港通资金流向
            Log.logger.info("获取沪深港通资金流向...")
            self.mTread(tsAStockPrice, 'moneyflow_hsgt', 'astock_basic')
            
            # 获取沪深股通十大成交股
            Log.logger.info("获取沪深股通十大成交股...")
            self.mTread(tsAStockPrice, 'hsgt_top10', 'astock_basic')
            
            # 获取港股通十大成交股
            Log.logger.info("获取港股通十大成交股...")
            self.mTread(tsAStockPrice, 'ggt_top10', 'astock_basic')
            
            # 获取沪深港股通持股明细
            Log.logger.info("获取沪深港股通持股明细...")
            self.mTread(tsAStockPrice, 'hk_hold', 'astock_basic')
            
            # 获取港股通每日成交统计
            Log.logger.info("获取港股通每日成交统计...")
            self.mTread(tsAStockPrice, 'ggt_daily', 'astock_basic')
            
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
        # 检查依赖是否满足
        if dependency_check and not self.check_dependency(dependency_check):
            Log.logger.warning(f"跳过线程 {functionName}，因为依赖 {dependency_check} 未满足")
            return
            
        thread = collectThread(className, functionName, self.pro, self.db)
        thread.set_max_runtime(self.max_thread_runtime)  # 设置最大运行时间
        Log.logger.info(f"启动线程: {functionName}，最大运行时间: {self.max_thread_runtime/3600}小时")
        self.thread_list.append(thread)


    

       



