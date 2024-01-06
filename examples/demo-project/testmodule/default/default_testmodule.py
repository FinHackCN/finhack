import finhack.library.log as Log
from runtime.constant import *
import runtime.global_var as global_var
from finhack.market.astock.astock import AStock
import time
from finhack.factor.default.factorManager import factorManager
from finhack.factor.default.factorAnalyzer import factorAnalyzer
class DefaultTestmodule():
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