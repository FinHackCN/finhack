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
        print('test')
        pass
    
    

    def run7(self):
        
 
        
        stock_list=['300787.SZ', '002136.SZ', '688310.SH', '003030.SZ', '688168.SH', '002539.SZ', '002399.SZ', '002686.SZ', '600611.SH', '688056.SH', '300370.SZ', '300258.SZ', '839680.BJ', '688299.SH', '002037.SZ', '688555.SH', '300389.SZ', '833346.BJ', '836270.BJ', '832023.BJ', '600695.SH', '836149.BJ', '300508.SZ', '600691.SH', '836717.BJ', '603569.SH', '300138.SZ', '688309.SH', '601233.SH', '000739.SZ', '300108.SZ', '600743.SH', '300935.SZ', '835185.BJ', '830839.BJ', '831768.BJ', '430510.BJ', '300971.SZ', '300038.SZ', '871396.BJ', '688105.SH', '000023.SZ', '002006.SZ', '873223.BJ', '300441.SZ', '600847.SH', '002462.SZ', '688588.SH', '002115.SZ', '300242.SZ', '300384.SZ', '002342.SZ', '688300.SH', '002274.SZ', '300226.SZ', '835508.BJ', '688511.SH', '836942.BJ', '688385.SH', '688199.SH', '873833.BJ', '831087.BJ', '600205.SH', '002452.SZ', '603096.SH', '688287.SH', '600501.SH', '430017.BJ', '831445.BJ', '000034.SZ', '300023.SZ', '300290.SZ', '688380.SH', '000975.SZ', '300599.SZ', '688728.SH', '603229.SH', '301073.SZ', '688298.SH', '300347.SZ', '300360.SZ', '835640.BJ', '002388.SZ', '300809.SZ', '688596.SH', '300059.SZ', '688100.SH', '833819.BJ', '002903.SZ', '300587.SZ', '300340.SZ', '688112.SH', '837046.BJ', '000971.SZ', '832145.BJ', '000906.SZ', '832089.BJ', '601838.SH', '430300.BJ', '688597.SH', '000701.SZ', '300554.SZ', '836260.BJ', '000301.SZ', '688225.SH', '831627.BJ', '603603.SH', '300177.SZ', '300372.SZ', '300884.SZ', '300950.SZ', '300016.SZ', '600851.SH', '688051.SH', '002210.SZ', '688010.SH', '603626.SH', '300871.SZ', '300270.SZ', '688700.SH', '301055.SZ', '300639.SZ', '600830.SH', '300217.SZ', '002249.SZ', '688018.SH', '838701.BJ', '603025.SH', '300099.SZ', '830799.BJ', '688179.SH', '300889.SZ', '688308.SH', '688669.SH', '838030.BJ', '834058.BJ', '300621.SZ', '603319.SH', '688282.SH', '300936.SZ', '688079.SH', '688317.SH', '870866.BJ', '430476.BJ', '600962.SH', '002365.SZ', '836422.BJ', '000860.SZ', '688001.SH', '831834.BJ', '603195.SH', '835179.BJ', '688459.SH', '000403.SZ', '688103.SH', '836957.BJ', '600331.SH', '002680.SZ', '835368.BJ', '300021.SZ', '600613.SH', '832978.BJ', '300518.SZ', '688137.SH', '688305.SH', '688096.SH', '835892.BJ', '000606.SZ', '300236.SZ', '688665.SH', '688093.SH', '300157.SZ', '872190.BJ', '835985.BJ', '836871.BJ', '833454.BJ', '001872.SZ', '600208.SH', '688337.SH', '688312.SH', '600075.SH', '830832.BJ', '300390.SZ', '688301.SH', '002567.SZ', '600237.SH', '002613.SZ', '600388.SH', '688505.SH', '688381.SH', '600560.SH', '300165.SZ', '000672.SZ', '831039.BJ', '688129.SH', '600751.SH', '601299.SH', '838670.BJ', '300494.SZ', '688655.SH', '872953.BJ', '601339.SH', '837663.BJ', '873703.BJ', '300240.SZ', '688399.SH', '688178.SH', '000778.SZ', '833914.BJ', '688215.SH', '301252.SZ', '002066.SZ', '300635.SZ', '300677.SZ', '688314.SH', '300329.SZ', '688479.SH', '300139.SZ', '002406.SZ', '688660.SH', '301439.SZ', '834261.BJ', '688033.SH', '831370.BJ', '688020.SH', '870726.BJ', '688075.SH', '833781.BJ', '688233.SH', '000736.SZ', '301042.SZ', '600361.SH', '688533.SH', '688067.SH', '430198.BJ', '601882.SH', '300513.SZ', '600750.SH', '835305.BJ', '688367.SH', '830809.BJ', '000541.SZ', '300421.SZ', '688126.SH', '873167.BJ', '300426.SZ', '300552.SZ', '688778.SH', '300293.SZ', '300235.SZ', '300202.SZ', '832735.BJ', '300984.SZ', '688696.SH', '300848.SZ', '600439.SH', '836414.BJ', '832110.BJ', '000782.SZ', '300036.SZ', '003013.SZ', '838171.BJ', '688143.SH', '603698.SH', '300158.SZ', '300003.SZ', '688138.SH', '000501.SZ', '300758.SZ', '833171.BJ', '600817.SH', '300145.SZ', '688522.SH', '834599.BJ', '870204.BJ', '688039.SH', '688005.SH', '688551.SH', '601558.SH', '300420.SZ', '300245.SZ', '688139.SH', '600387.SH', '838262.BJ', '300106.SZ', '833284.BJ', '300573.SZ', '872351.BJ', '600309.SH', '300423.SZ', '300753.SZ', '688078.SH', '300625.SZ', '831726.BJ', '832471.BJ', '832885.BJ', '430418.BJ', '300375.SZ', '002473.SZ', '002464.SZ']
        #stock_list=[]
        alpha="rank(ts_min(delta($close_0, 1), 5)) * ts_rank(covariance($high, $low, 10), 5) - max(correlation($turnoverRate_0, $volume, 5), 15)"
        
        
        df_check=alphaEngine.get_df(formula=alpha,df=pd.DataFrame(),name='alpha',ignore_notice=True,stock_list=stock_list,diff=False)
        df_alpha=alphaEngine.calc(formula=alpha,df=df_check,name='alpha',check=False,save=False,ignore_notice=True)
        
        

        df_analys=df_check.copy()
        df_analys['alpha']=df_alpha
        df_analys=df_analys[['alpha']]
        df_base=factorManager.getFactors(factor_list=['open','close'],cache=True)

        merged_df = df_analys.merge(df_base, left_index=True, right_index=True, how='left')
        
        print(merged_df)

        factorAnalyzer.analys('alpha',df=merged_df,start_date='',end_date='',formula=alpha,source='chatgpt',table='factors_mining',replace=True,ignore_error=False)     
        exit()
        
        
        df_check=alphaEngine.get_df(formula=alpha,df=pd.DataFrame(),name='alpha',ignore_notice=True,stock_list=stock_list,diff=False)
        df_alpha=alphaEngine.calc(formula=alpha,df=df_check,name='alpha',check=False,save=False,ignore_notice=True)

        df_analys=df_check.copy()
        df_analys['alpha']=df_alpha
        df_analys=df_analys[['alpha']]
        df_base=factorManager.getFactors(factor_list=['open','close'],cache=True)
        merged_df = df_analys.merge(df_base, left_index=True, right_index=True, how='left')
        
        
        n = 10  # 例如，将alpha_std分为10等分
        day=10
        df=merged_df
        df['return']=df.groupby('ts_code',group_keys=False).apply(lambda x: x['close'].shift(-1*day)/x['open'].shift(-1))
        
        def standardize_alpha(group):
            alpha_mean = group['alpha'].mean()
            alpha_std = group['alpha'].std()
            # 防止除以零
            if alpha_std != 0:
                group['alpha_std'] = (group['alpha'] - alpha_mean) / alpha_std
            else:
                group['alpha_std'] = 0
            return group
        
        # 定义一个函数来对alpha_std进行分桶
        def quantile_cut(group, n):
            group['alpha_quantile'] = pd.qcut(group['alpha_std'], q=n, labels=range(1, n+1))
            return group
            
        # 按照trade_date分组并应用标准化函数
        df = df.groupby('trade_date').apply(standardize_alpha)
        # 按照trade_date分组并应用分桶函数
        df = df.groupby('trade_date').apply(quantile_cut, n=n)        
        df=df.dropna()
        
        
        quantile_return_lists = {}
        quantile_performance = {}
        
        # 按照trade_date和alpha_quantile分组，计算平均return
        grouped = df.groupby(['trade_date', 'alpha_quantile'])
        
        
        
        
        # 遍历每个alpha_quantile分组
        for quantile in df['alpha_quantile'].unique():
            mean_df=grouped['return'].mean().fillna(1)
            quantile_return_lists[quantile] = mean_df.loc[pd.IndexSlice[:, quantile]].tolist()       
            
            excess_returns = [x - 1 for x in quantile_return_lists[quantile]]
            # 计算平均超额收益率
            average_excess_return = np.mean(excess_returns)
            # 计算超额收益率的标准差
            standard_deviation = np.std(excess_returns)
            # 计算夏普比率
            sharpe_ratio = average_excess_return / standard_deviation if standard_deviation != 0 else 0
            
            quantile_performance[quantile]={
                'net_value':np.cumprod(quantile_return_lists[quantile])[-1],
                'sharpe_ratio':sharpe_ratio 
            }

        print(quantile_performance)
        
        # 计算总净值
        total_net_value = sum(item['net_value'] for item in quantile_performance.values())
        
        # 计算加权平均夏普比率
        weighted_sharpe_ratios = sum((item['net_value'] / total_net_value) * item['sharpe_ratio'] for item in quantile_performance.values())
        
        print(weighted_sharpe_ratios)
        
        #factorAnalyzer.analys('alpha',df=merged_df,start_date='',end_date='',formula=alpha,source='chatgpt',table='factors_mining')
        pass