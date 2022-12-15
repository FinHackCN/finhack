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

def start_bt(features_list,model_hash,loss,algorithm,init_cash,hold_day,hold_n,filters_name):
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
                        
                        if filters_name!='':
                                args['filter']=filters_name
                
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




tested_list=mydb.selectToDf('select model from  (select model,COUNT(model) as c from backtest GROUP BY (model) ) as x where c>=100','finhack')
tested_list=tested_list['model'].to_list()

#print(tested_list)
for tested_model in tested_list:
        if os.path.exists('/home/woldy/finhack/data/preds/lgb_model_'+tested_model+'_pred.pkl'):
                os.remove('/home/woldy/finhack/data/preds/lgb_model_'+tested_model+'_pred.pkl')




        

 

while True:
        model_list=mydb.selectToDf('select * from auto_train','finhack')
        for row in model_list.itertuples():
                features_list=getattr(row,'features')
                model_hash=getattr(row,'hash')
                filters_name=getattr(row,'filter')
                if model_hash in tested_list:
                        continue
                
                loss=getattr(row,'loss')
                algorithm=getattr(row,'algorithm')
                                
                with ProcessPoolExecutor(max_workers=32) as pool:
                        tasklist=[]
                        for init_cash in [10000,20000,30000,50000]:
                                for hold_day in  [3,5,7,9,11]:
                                        for hold_n in  [2,4,6,8,10]:
                                                #time.sleep(1)
                                                mytask=pool.submit(start_bt,features_list,model_hash,loss,algorithm,init_cash,hold_day,hold_n,filters_name)
                                                tasklist.append(mytask)
                        wait(tasklist, return_when=ALL_COMPLETED)
        time.sleep(60)

# for row in strategy_list.itertuples():
#         for init_cash in [10000,30000,50000,100000]:
#                 for hold_day in [10]:
#                         for hold_n in [10]:
#                                 start_bt(row,init_cash,hold_day,hold_n)
