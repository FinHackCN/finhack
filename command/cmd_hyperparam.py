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
from itertools import product
import math 

 




def search(param,feature_list):
    param['num_leaves']=int(math.pow(2,param['max_depth'])-1)
    lgbtrain.run('20000101','20080101','20100101',features=feature_list.split(","),label='abs',shift=10,param=param,loss='ds')
    lgbtrain.run('20000101','20080101','20100101',features=feature_list.split(","),label='abs',shift=10,param=param,loss='mse')

gird={
    "max_depth":[5,7,9],
    #"subsample":[0.8,0.9,1],
    #"colsample_bytree":[0.8,0.9,1],
    # "min_child_sample":[64,128,256,512,1024],
    "learning_rate":[0.05,0.1,0.15]
}


value_list=[]

feature_list=mydb.selectToDf("SELECT * FROM `backtest` WHERE `annual_return` > '0.05' ORDER BY annual_return desc",'finhack')
feature_list=feature_list['features_list'].to_list()

feature_list=list(set(feature_list))


for key in gird.keys():
    value_list.append(gird[key])

 

p_list = list(product(*value_list))
k_list=list(gird.keys())

for features in feature_list:
    factorManager.getFactors(factor_list=features.split(',')+['open','close'],cache=True)
    for p in p_list:
        i=0
        param={}
        for v in p:
            param[k_list[i]]=v
            i=i+1
        search(param=param,feature_list=features)