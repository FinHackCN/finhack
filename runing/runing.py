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


#思路
#1、遍历所有因子，取出lastdate的所有因子
#2、把这些因子丢到

class runing():
    def prepare():
        factor_list=factorManager.getFactorsList(valid=False,ignore=False)
        print(factors)
        