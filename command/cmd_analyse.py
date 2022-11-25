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

from factors.factorAnalyzer import factorAnalyzer
from factors.factorManager import factorManager
 
t1=time.time()


factors=factorManager.getFactorsList()
# factors=['alpha101_001']


# for factor in factors:
#     factorAnalyzer.analys(factor)
   


with ProcessPoolExecutor(max_workers=10) as pool:
    tasklist=[]
    for factor in factors:
        mytask=pool.submit(factorAnalyzer.analys,factor)
        tasklist.append(mytask)
    wait(tasklist, return_when=ALL_COMPLETED)
 
 
    print(time.time()-t1)


