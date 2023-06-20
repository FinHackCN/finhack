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
from backtest.backtest import bt
from library.mydb import mydb
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
from library.globalvar import *

def auto_lgbtrain(factor_list,init_cash=1000,loss='ds',backtest=True):
    pass



min_f=150
max_f=200
if len(sys.argv)>=3:
    min_f=int(sys.argv[1])
    max_f=int(sys.argv[2])

print(sys.argv)

train_name='lgbtrain'

while True:
        try:
            flist=factorManager.getTopAnalysedFactorsList(top=200)
            
            # with open(CONFIG_DIR+"/factorlist/trainlist/autotrain", 'r', encoding='utf-8') as f:
            #     flist=[_.rstrip('\n') for _ in f.readlines()]


            #print(flist)


            
            random.shuffle(flist)
            n=random.randint(min_f,max_f)
            factor_list=[]
                
            for i in range(0,n):
                factor_list.append(flist.pop())
            factor_list.sort()
            
            
            
            df=factorManager.getFactors(factor_list=factor_list+['open','close'])
            # 计算相关性矩阵
            correlation_matrix = df.corr()
            

            
            # print(df)
            
            
            
            # # 打印相关性矩阵
            # print(correlation_matrix)
            
            
            new_factor_list=[]
            
            
            # 遍历每一对名称和相关系数
            
            
            for factor in factor_list:
                if factor in  ['open','close'] :
                    continue
                append=True
                for factor2 in new_factor_list:

                    for column1, series in correlation_matrix.iteritems():
                        if column1!=factor:
                            continue
                        for column2, correlation in series.iteritems():
                            if column1==column2:
                                continue
                            if column2!=factor2:
                                continue
                            if abs(correlation)>0.7:
                                append=False
                if append:
                    new_factor_list.append(factor)
                        
            
            
            factor_list=new_factor_list
               
            
            if os.path.exists(USER_DIR+'train/'+train_name+'.py'):
                train_module = importlib.import_module('.'+train_name,package='user.train')
            else:
                train_module = importlib.import_module('.'+train_name,package='train')
            train_instance = getattr(train_module, train_name)
            train_instance.run('20070101','20150101','20170101',factor_list,'abs',10,{},'ds')    
            train_instance.run('20070101','20150101','20170101',factor_list,'abs',10,{},'mse') 
            
            
#             for loss in ['ds']:
#                 lgbtrain.run('20070101','20150101','20170101',factor_list,'abs',10,{},loss)

        except Exception as e:
            print("error:"+str(e))
            print("err exception is %s" % traceback.format_exc())
            
            
            

