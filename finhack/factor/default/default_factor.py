import random
from runtime.constant import *
from finhack.library.config import Config
from finhack.factor.default.preCheck import preCheck
from finhack.factor.default.indicatorCompute import indicatorCompute
from finhack.factor.default.alphaEngine import alphaEngine
from finhack.market.astock.astock import AStock
from finhack.factor.default.taskRunner import taskRunner
from finhack.factor.default.factorManager import factorManager
from finhack.factor.default.factorAnalyzer import factorAnalyzer 
from finhack.factor.default.factorMining import factorMining
from finhack.library.mycache import mycache

class DefaultFactor:
    def __init__(self):
        pass

    def run(self):
        taskRunner.runTask(self.args.task_list)
        
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
        
        
    def mining(self):
        method=self.args.method
        stock25=self.args.stock25.split(',')
        stock300=self.args.stock300.split(',')
        
        if method=="gplearn":
            while True:
                min_n=int(self.args.min_n)
                max_n=int(self.args.max_n)
                flist=factorManager.getAnalysedIndicatorsList()
                random.shuffle(flist)
                n=random.randint(min_n,max_n)
                factor_list=[]
                

            
                for i in range(0,n):
                    factor_list.append(flist.pop())
                #factor_list=factor_list+['open','high','low','close','pre_close','change','returns','volume','amount','vwap'] 
                factor_list=factor_list+['open','close']
            
        
                df_all_25=factorManager.getFactors(factor_list=factor_list,stock_list=stock25,start_date='20130823',end_date='20230823')
                df_all_25=df_all_25.reset_index() 
                df_all_25['Y']=df_all_25.groupby('ts_code',group_keys=False).apply(lambda x: x['close'].shift(-10)/x['open'].shift(-1))
                df_all_25=df_all_25.dropna()
        
                df_all_300=factorManager.getFactors(factor_list=factor_list,stock_list=stock300,start_date='20130823',end_date='20230823')
                df_all_300['Y']=df_all_300.groupby('ts_code',group_keys=False).apply(lambda x: x['close'].shift(-10)/x['open'].shift(-1))
                df_all_300=df_all_300.dropna() 
        
                df_tmp=df_all_25[['ts_code','trade_date']]
                df_tmp=df_tmp.reset_index(drop=True)
                
                label = df_all_25['Y']
                train = df_all_25.drop(columns=['ts_code','trade_date','Y'])   
                factorMining.gplearn(train,label,df_all_25,df_all_300)
                
        elif method.lower()=="gpt" or method.lower()=="chatgpt":
            factorMining.gpt(self.args.prompt,self.args.model,stock300)
        
    def alpha(self):
        formula=self.args.formula
        print(formula)
        df_alpha=alphaEngine.calc(formula=formula,name="alpha",check=True,replace=False)
        print(df_alpha)
        
        factorAnalyzer.alphalens(factor_name='alpha',df=df_alpha)