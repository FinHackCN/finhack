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
from train.trainhelper import trainhelper
import lightgbm as lgb
from library.globalvar import *

def start_bt(features_list,model_hash,loss,algorithm,init_cash,hold_day,hold_n,filters_name,strategy):
                try:
                        train=algorithm+'_'+loss
                        
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
                        
                        if filters_name!='':
                                args['filter']=filters_name
                                
                                
                                
                        if not PREDS_DIR+"lgb_model_"+model+"_pred.pkl"):
                                data_train,data_valid,df_pred,data_path=trainhelper.getTrainData('20000101','20080101','20100101',features=features.split(","),label='abs',shift=10)
                                lgbtrain.pred(df_pred,data_path,model_hash)
                                
                                
                
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
                
                

df_all=AStock.getStockDailyPrice(fq='qfq')
df_all=AStock.getStockDailyPrice(fq='hfq')




tested_list=mydb.selectToDf('select model from  (select model,COUNT(model) as c from backtest GROUP BY (model) ) as x where c>=50','finhack')
if not tested_list.empty:
        tested_list=tested_list['model'].to_list()
else:
        tested_list=[]

#print(tested_list)
# for tested_model in tested_list:
#         if os.path.exists('/home/woldy/finhack/data/preds/lgb_model_'+tested_model+'_pred.pkl'):
#                 os.remove('/home/woldy/finhack/data/preds/lgb_model_'+tested_model+'_pred.pkl')



with ProcessPoolExecutor(max_workers=16) as pool:
        model_list=mydb.selectToDf('select * from auto_train','finhack')
        for row in model_list.itertuples():
                features_list=getattr(row,'features')
                model_hash=getattr(row,'hash')
                filters_name=getattr(row,'filter')
                if model_hash in tested_list:
                        continue
                
                
                if not MODELS_DIR+"lgb_model_"+model_hash+"_pred.pkl"):
                    continue              
                
                loss=getattr(row,'loss')
                algorithm=getattr(row,'algorithm')
                
                if True:
                        tasklist=[]
                        #print(model_hash)
                        for init_cash in [10000,100000]:
                                for hold_day in  [3,5,7,9,11]:
                                        for hold_n in  [2,4,6,8,10]:
                                                for strategy in ['aiTopN']:
                                                        time.sleep(1)
                                                        mytask=pool.submit(start_bt,features_list,model_hash,loss,algorithm,init_cash,hold_day,hold_n,filters_name,strategy)
                                                        #tasklist.append(mytask)
                        #wait(tasklist, return_when=ALL_COMPLETED)
        #time.sleep(60)
print('all done')

 