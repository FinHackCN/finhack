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




def start_bt(features_list,model_hash,loss,algorithm,init_cash,hold_day,hold_n):
                try:
                        train=algorithm+'_'+loss
                        strategy='aiTopN'
                        
                        args={
                                "features_list":features_list,
                                "train":train,
                                "model":model_hash,
                                "loss":loss,
                                "strategy_args":{
                                        "hold_day":hold_day,
                                        "hold_n":hold_n,
                                }
                        }
                
                        bt_instance=bt.run(
                            cash=init_cash,
                            start_date='20100101',
                            strategy_name=strategy,
                            data_path="lgb_model_"+model_hash+"_pred.pkl",
                            args=args
                        )
                        return True
                except Exception as e:
                        print(str(e))
                        print("err exception is %s" % traceback.format_exc())   


bt_list=mydb.selectToDf("SELECT * FROM backtest  ORDER BY annual_return desc limit 1000",'finhack')


model_list=[]
for row in bt_list.itertuples():
    model=getattr(row,'model')
    model_list.append(model)
    
model_list=list(set(model_list))


for model in model_list:
    model_info=mydb.selectToDf("SELECT * FROM auto_train where hash='%s'" % model,'finhack')
    features=model_info['features'].values.tolist()[0]
    param=model_info['param'].values.tolist()[0]
    loss=model_info['loss'].values.tolist()[0]
    param=json.loads(param)
    if 'max_depth' in param.keys():
        param['num_leaves']=int(math.pow(2,param['max_depth'])-1)
    
    if not os.path.isfile(os.path.dirname(__file__)+"/../data/preds/"+"lgb_model_"+model+"_pred.pkl"):
        lgbtrain.run('20000101','20080101','20100101',features=features.split(","),label='abs',shift=10,param=param,loss=loss,replace=True)
    
    bt_list=mydb.selectToDf("SELECT * FROM backtest  where model='%s' ORDER BY annual_return desc limit 10" % model,'finhack')

    for row in bt_list.itertuples():
        created_at=getattr(row,'created_at')
        if '2022-12-15 15:' in str(created_at):
            continue
        args=getattr(row,'args')
        args=json.loads(args)
        model_hash=getattr(row,'model')
        init_cash=getattr(row,'init_cash')
        bt_instance=bt.run(
                cash=int(init_cash),
                start_date='20100101',
                strategy_name='aiTopN',
                data_path="lgb_model_"+model_hash+"_pred.pkl",
                args=args,
                replace=True
        )
    
    #features_list,model_hash,loss,algorithm,init_cash,hold_day,hold_n
    