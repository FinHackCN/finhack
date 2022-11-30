import sys
import os
import time
import random
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
import pandas as pd
from library.config import config
from library.mydb import mydb
from library.astock import AStock
from factors.factorManager import factorManager


from factors.factorManager import factorManager


#思路
#1、遍历所有因子，取出lastdate的所有因子
#2、把这些因子丢到

class runing():
    
    
    def getLastFactorData(factor,lastdate):
        df_factor=factorManager.getFactors([factor])
        df_factor=df_factor.reset_index()
        df_factor=df_factor[df_factor.trade_date==lastdate] 
        df_factor=df_factor.set_index('ts_code',drop=True)
        del df_factor['trade_date']
        return df_factor
    
    def prepare():
        t1=time.time()
        factor_list=factorManager.getFactorsList(valid=False,ignore=False)
        factor_list.remove('close')
 
        df_close=factorManager.getFactors(['close'])
        df_close=df_close.reset_index()
        lastdate=df_close['trade_date'].max()
        df_close=df_close[df_close.trade_date==lastdate] 
        df_close=df_close.set_index('ts_code',drop=True)
        del df_close['trade_date']
        df_all=df_close
        
        
 
        
        
        
        with ProcessPoolExecutor(max_workers=24) as pool:
            tasklist=[]
            for factor in factor_list:
                mytask=pool.submit(runing.getLastFactorData,factor,lastdate)
                tasklist.append(mytask)
            wait(tasklist, return_when=ALL_COMPLETED)
     
     
        for task in tasklist:
            factor_name=task.result().columns.tolist()[0]
            df_all[factor_name]=task.result()
            
        print(df_all)
     
        print(time.time()-t1)