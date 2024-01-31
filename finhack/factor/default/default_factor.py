from runtime.constant import *
from finhack.library.config import Config
from finhack.factor.default.preCheck import preCheck
from finhack.factor.default.indicatorCompute import indicatorCompute
from finhack.factor.default.alphaEngine import alphaEngine
from finhack.market.astock.astock import AStock
from finhack.factor.default.taskRunner import taskRunner
from finhack.factor.default.factorManager import factorManager
from finhack.factor.default.factorAnalyzer import factorAnalyzer 

class DefaultFactor:
    def __init__(self):
        pass

    def run(self):
        pass
        taskRunner.runTask()
        
    def test(self):
        print(self.args)
        
        
    def list(self):
        factor_list=factorManager.getFactorsList()
        print(factor_list)
        
    def show(self):
        factor_name=self.args.factor
        factor=factorManager.getFactors([factor_name])
        print(factor)
        print(factor.describe())
        
    def analys(self):
        factor_name=self.args.factor
        factorAnalyzer.alphalens(factor_name)
        
    def alpha(self):
        formula=self.args.formula
        print(formula)
        df_alpha=alphaEngine.calc(formula=formula,name="alpha",check=True,replace=False)
        print(df_alpha)
        
        factorAnalyzer.alphalens(factor_name='alpha',df=df_alpha)