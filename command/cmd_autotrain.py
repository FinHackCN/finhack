import sys
import os
import time
import shutil
import random
import traceback
import pandas as pd
import datetime
import importlib
import hashlib
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from train.lgbtrain import lgbtrain
from factors.factorManager import factorManager
from library.backtest import bt
from library.mydb import mydb
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED

def auto_lgbtrain(factor_list,init_cash=1000,loss='ds',backtest=True):
    pass


 

while True:
        try:
            flist=factorManager.getTopAnalysedFactorsList(top=300)
            random.shuffle(flist)
            n=random.randint(10,40)
            factor_list=[]
                
            for i in range(0,n):
                factor_list.append(flist.pop())
            factor_list.sort()
            
            print(factor_list)        
            for loss in ['ds','mse']:
                lgbtrain.run('20000101','20080101','20100101',factor_list,'abs',10,{},loss)

        except Exception as e:
            print("error:"+str(e))
            print("err exception is %s" % traceback.format_exc())