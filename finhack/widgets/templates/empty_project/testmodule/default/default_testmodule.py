import finhack.library.log as Log
from runtime.constant import *
import runtime.global_var as global_var
from finhack.market.astock.astock import AStock
import time
from finhack.factor.default.factorManager import factorManager
from finhack.factor.default.factorAnalyzer import factorAnalyzer
from finhack.factor.default.factorPkl import factorPkl
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
        factors=factorManager.getFactors(['ADOSC_0','AD_0','APO_0','AROONDOWN_0','ARRONUP_0','pe_0','alpha101_012','alpha101_013'],start_date='20150101',end_date="20230101")
        print(factors)
        
        
    def run5(self):
        # import dask.dataframe as dd
        # path = '/data/code/finhack/examples/demo-project/data/factors/single_factors_parquet/*.parquet'
        # ddf = dd.read_parquet(path)
        # print(ddf)

        # ddf_single = dd.read_parquet('/data/code/finhack/examples/demo-project/data/factors/single_factors_parquet/turnoverRatef_0.parquet')
        # print(ddf_single)

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