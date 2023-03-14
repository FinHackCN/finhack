import pandas as pd
import importlib
import os
import sys
import datetime
import time
sys.path.append('/data/code/finhack')
from library.backtest import bt
import traceback
from library.mydb import mydb
import hashlib
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
from library.astock import AStock
import json
import math
from train.lgbtrain import lgbtrain
from factors.factorManager import factorManager
from library.backtest import bt
from library.mydb import mydb
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
from itertools import product
import math 
from train.trainhelper import trainhelper
from library.globalvar import *


df_all=AStock.getStockDailyPrice(fq='qfq')
df_all=AStock.getStockDailyPrice(fq='hfq')


def start_bt(init_cash,start_date,strategy_name,model_hash,args):
                try:
                        bt_instance=bt.run(
                            cash=init_cash,
                            start_date='20100101',
                            strategy_name=strategy_name,
                            data_path="lgb_model_"+model_hash+"_pred.pkl",
                            args=args,
                            replace=True
                        )
                        return True
                except Exception as e:
                        print(str(e))
                        print("err exception is %s" % traceback.format_exc())   


bt_list=mydb.selectToDf("SELECT * FROM backtest where max_down>-0.5 ORDER BY annual_return desc limit 2000",'finhack')


model_list=[]
for row in bt_list.itertuples():
    model=getattr(row,'model')
    model_list.append(model)
    
model_list=list(set(model_list))



with ProcessPoolExecutor(max_workers=32) as pool:
    tasklist=[]
    for model in model_list:
        model_info=mydb.selectToDf("SELECT * FROM auto_train where hash='%s'" % model,'finhack')
        features=model_info['features'].values.tolist()[0]
        
        
        
        param=model_info['param'].values.tolist()[0]
        loss=model_info['loss'].values.tolist()[0]
        param=json.loads(param)
        if 'max_depth' in param.keys():
            param['num_leaves']=int(math.pow(2,param['max_depth'])-1)
        
        if not PREDS_DIR+"lgb_model_"+model+"_pred.pkl"):
            if not MODELS_DIR+"lgb_model_"+model+".txt"):
                lgbtrain.run('20000101','20080101','20100101',features=features.split(","),label='abs',shift=10,param=param,loss=loss,replace=True)
            else:
                print('nopred')
                data_train,data_valid,df_pred,data_path=trainhelper.getTrainData('20000101','20080101','20100101',features=features.split(","),label='abs',shift=10)
                lgbtrain.pred(df_pred,data_path,model)
                print('---')
            
        
        bt_list=mydb.selectToDf("SELECT * FROM backtest  where model='%s' and annual_return >0.1 ORDER BY annual_return desc limit 10" % model,'finhack')
    
        for row in bt_list.itertuples():
            created_at=getattr(row,'created_at')
            # if '2022-12-15 15:' in str(created_at):
            #     continue
            args=getattr(row,'args')
            args=json.loads(args)
            model_hash=getattr(row,'model')
            init_cash=getattr(row,'init_cash')
            
            start_bt(int(init_cash),'20100101','aiTopN',model_hash,args)
            exit()
            
            mytask=pool.submit(start_bt,int(init_cash),'20100101','aiTopN',model_hash,args)
            tasklist.append(mytask)
    wait(tasklist, return_when=ALL_COMPLETED)            
            
 
    
    #features_list,model_hash,loss,algorithm,init_cash,hold_day,hold_n
    