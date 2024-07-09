import finhack.library.log as Log
from runtime.constant import *
import runtime.global_var as global_var
from finhack.market.astock.astock import AStock
import time
from finhack.factor.default.alphaEngine import alphaEngine
from finhack.factor.default.factorManager import factorManager
from finhack.factor.default.factorAnalyzer import factorAnalyzer
from finhack.factor.default.factorPkl import factorPkl
from finhack.library.mydb import mydb
import pandas as pd
import numpy as np
from finhack.library.ai import AI
from finhack.factor.default.preCheck import preCheck
from finhack.trader.default.performance import Performance 
from finhack.trader.default.context import context,g
from finhack.trader.default.calendar import Calendar
class DefaultTestmodule():
    #finhack testmodule run
    def __init__(self):
        pass
    def run(self):
        Log.logger.debug("----%s----" % (__file__))
        Log.logger.debug("this is Testmodule")
        Log.logger.debug("vendor is default")
        print(self.args)
        while True:
            time.sleep(1)
            Log.logger.debug(".")
        
        
    def run2(self):
        print(self.args)
        print('run2')
        stock_list=AStock.getStockCodeList(strict=False, db='tushare')
        print(stock_list)
        
        
    def run3(self):
        factorAnalyzer.alphalens("pe_0")
        
    def run4(self):
        #factorPkl.save()
        factors=factorManager.getFactors(['open','close','ADOSC_0','AD_0','APO_0','AROONDOWN_0','ARRONUP_0'],start_date='20150101',end_date="20230101",stock_list=['000001.SZ','000002.SZ','002624.SZ'])
        print(factors)
        
        
    def run5(self):
        # import dask.dataframe as dd
        # path = '/data/code/finhack/examples/demo-project/data/factors/single_factors_parquet/*.parquet'
        # ddf = dd.read_parquet(path)
        # print(ddf)

        # ddf_single = dd.read_parquet('/data/code/finhack/examples/demo-project/data/factors/single_factors_parquet/turnoverRatef_0.parquet')
        # print(ddf_single)

        df_price=AStock.getStockDailyPriceByCode("000023.SZ",fq='hfq',db="tushare")
        print(df_price[df_price['trade_date']=='20100104'])
        exit()

        df=factorManager.getFactors(factor_list=['open','close'])
        # 首先定义你想要选取的索引列表
        df=df.reset_index()
        print(df)
        # 使用 .loc 方法选取这些索引对应的行
        df = df[df.trade_date=='20240118']
        df = df[df.trade_date=='20240118']


        print(df)
        
    def run6(self):
        c_list=preCheck.checkAllFactors() #chenged factor，代码发生变化
        pass
    
    

    def run7(self):
        bt_list=mydb.selectToDf("SELECT instance_id FROM `finhack`.`backtest`  order by sharpe desc",'finhack')   
        for btid in bt_list['instance_id'].tolist():
            try:
                # 查询数据库中存储的数据
                sql = 'SELECT * FROM backtest WHERE instance_id="%s"' % (btid)
                result = mydb.selectToDf(sql, 'finhack')

                if not result.empty:
                    # 从查询结果中提取数据并还原到 context 变量中
                    row = result.iloc[0]

                    # 还原 trade 相关的字段
                    context.id=btid
                    context.trade.model_id = row['model']
                    context.trade.strategy = row['strategy']
                    context.trade.start_time = row['start_date']
                    context.trade.end_time = row['end_date']
                    context.trade.benchmark = row['benchmark']
                    context.trade.strategy_code = row['strategy_code']

                    # 还原 portfolio 相关的字段
                    context.portfolio.starting_cash = row['init_cash']
                    context.portfolio.total_value = row['total_value']

                    # 还原 performance 相关的字段
                    context.performance.returns = list(map(float, row['returns'].split(',')))
                    context.performance.bench_returns = list(map(float, row['benchReturns'].split(',')))
                    context.performance.indicators.alpha = float(row['alpha'])
                    context.performance.indicators.beta = float(row['beta'])
                    context.performance.indicators.annual_return = float(row['annual_return'])
                    context.performance.indicators.cagr = float(row['cagr'])
                    context.performance.indicators.annual_volatility = float(row['annual_volatility'])
                    context.performance.indicators.info_ratio = float(row['info_ratio'])
                    context.performance.indicators.downside_risk = float(row['downside_risk'])
                    context.performance.indicators.R2 = float(row['R2'])
                    context.performance.indicators.sharpe = float(row['sharpe'])
                    context.performance.indicators.sortino = float(row['sortino'])
                    context.performance.indicators.calmar = float(row['calmar'])
                    context.performance.indicators.omega = float(row['omega'])
                    context.performance.indicators.max_down = float(row['max_down'])
                    context.performance.indicators.sqn = float(row['SQN'])
                    context.performance.win = float(row['win'])
                    context.performance.trade_num = int(row['trade_num'])
                    


                    calendar=Calendar.get_calendar(context.trade.start_time,context.trade.end_time,market='astock')
                    calendar=pd.to_datetime(calendar)


                    calendar=calendar[0:len(context.performance.returns)]
                    

                    context.performance.returns = pd.DataFrame(context.performance.returns, index=calendar)
                    context.performance.bench_returns = pd.DataFrame(context.performance.bench_returns, index=calendar)
                    Performance.save_chart(context)
                    print("+")
                else:
                    print("-")
            except Exception as e:
                print("/")

    def run8():
        from finhack.trainer.trainer import Trainer
        from finhack.trainer.lightgbm.lightgbm_trainer import LightgbmTrainer
        preds_df=Trainer.getPredData(model_id="04e084fda50a9c3813ee02fa245dc280",start_date="20200101",end_date="20240101")
        clsLgbTrainer=LightgbmTrainer()
        preds=clsLgbTrainer.pred(preds_df,md5=model_id,save=False)    
        print(preds)

    def run9(self):
        import pickle

        # 假设你有一个.pkl文件的路径
        pkl_file_path = '/data/code/demo_project/data/preds/model_15bd82b2d1f19aa38d00c0f15404bf51_pred.pkl'

        # 打开文件以进行读取（注意：以二进制模式读取）
        with open(pkl_file_path, 'rb') as file:
            # 使用pickle的load函数加载文件内容
            data = pickle.load(file)

        # 现在data包含了.pkl文件中的Python对象
        print(data)


    def limit12(self):
        calendar=Calendar.get_calendar("2019-03-20 00:00:00","2024-03-20 00:00:00",market='astock')
        #print(calendar)
        calendar = [date.replace('-', '') for date in calendar]
        df_limit=mydb.selectToDf("SELECT * FROM astock_price_limit_list where `limit`='U' and trade_date>'2019-03-20'",'tushare')
        #print(ulist)

        # 假设 df_limit 是您的涨停记录DataFrame，calendar 是交易日列表
        # 首先，将 trade_date 转换为日期格式
        #df_limit['trade_date'] = pd.to_datetime(df_limit['trade_date'], format='%Y%m%d')

        # 将 calendar 也转换为日期格式
        #calendar = pd.to_datetime(calendar)

        # 设置连续涨停的天数
        n = 12  # 举例，我们查找连续3个交易日都涨停的股票

        # 初始化一个字典来记录每个股票的连续涨停天数
        consecutive_limit_dict = {code: {'count':0,'max':0,'first':''} for code in df_limit['ts_code'].unique()}

        # 初始化一个集合，用于保存满足条件的股票代码
        consecutive_limit_stocks = set()

        # 遍历交易日历
        for date in calendar:
            # 找出当天涨停的股票
            limit_stocks_today = df_limit[df_limit['trade_date'] == date]['ts_code']
            limit_stocks_today=limit_stocks_today.tolist()
            
            # 对于当天涨停的股票，更新其连续涨停天数
            for stock in limit_stocks_today:
                consecutive_limit_dict[stock]['count'] += 1
                if consecutive_limit_dict[stock]['count'] == 1:
                    if consecutive_limit_dict[stock]['max']<12:
                        consecutive_limit_dict[stock]['first']=date
                # 如果达到连续n天涨停，则将其添加到结果集合中
                if consecutive_limit_dict[stock]['count'] == n:
                    consecutive_limit_stocks.add(stock)

            
            # 对于当天未涨停的股票，重置其连续涨停天数
            for stock in consecutive_limit_dict.keys():
                if stock not in limit_stocks_today:
                    if consecutive_limit_dict[stock]['count']>consecutive_limit_dict[stock]['max']:
                        consecutive_limit_dict[stock]['max']=consecutive_limit_dict[stock]['count']
                    consecutive_limit_dict[stock]['count'] = 0
                    #consecutive_limit_dict[stock]['first'] = ""


        # 转换字典为DataFrame
        consecutive_limit_df = pd.DataFrame.from_dict(consecutive_limit_dict, orient='index')

        # 重置索引，如果您想要将股票代码作为一个普通的列
        consecutive_limit_df.reset_index(inplace=True)
        consecutive_limit_df.rename(columns={'index': 'ts_code'}, inplace=True)
        consecutive_limit_df=consecutive_limit_df[consecutive_limit_df['max']>=12]
        # 现在 consecutive_limit_df 是一个DataFrame，包含股票代码、计数和第一次涨停日期

        df_basic=mydb.selectToDf("SELECT * FROM `tushare`.`astock_basic`",'tushare')
        # 进行合并操作，假设 df_basic 是包含完整信息的DataFrame
        merged_df = pd.merge(consecutive_limit_df, df_basic[['ts_code', 'name',  'industry', 'list_date']], on='ts_code', how='left')
        merged_df.reset_index(drop=True)
        del merged_df['count']
        # 显示合并后的DataFrame
        # 设置显示配置，以便能够打印所有的列和行（如果行数很多的话，这可能会导致输出非常长）
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)

        # 打印完整的 DataFrame
        print(merged_df.to_string())
