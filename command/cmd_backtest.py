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
import redis

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
                                
                                
                                
                        if not os.path.exists(PREDS_DIR+"lgb_model_"+model_hash+"_pred.pkl"):
                                data_train,data_valid,df_pred,data_path=trainhelper.getTrainData('20000101','20100101','20130101',features=features.split(","),label='abs',shift=10)
                                lgbtrain.pred(df_pred,data_path,model_hash)
                                
                                
                
                        bt_instance=bt.run(
                            cash=init_cash,
                            start_date='20180101',
                            strategy_name=strategy,
                            data_path="lgb_model_"+model_hash+"_pred.pkl",
                            args=args,
                            benchmark='000852.SH',
                            slip=0, #无滑点
                        )

                        return True
                except Exception as e:
                        print(str(e))
                        print("err exception is %s" % traceback.format_exc())        
                
                

# df_all=AStock.getStockDailyPrice(fq='qfq')
# df_all=AStock.getStockDailyPrice(fq='hfq')
# bt.load_price()



# tested_list=mydb.selectToDf('select model from  (select model,COUNT(model) as c from backtest GROUP BY (model) ) as x where c>=50','finhack')
# if not tested_list.empty:
#         tested_list=tested_list['model'].to_list()
# else:
#         tested_list=[]

#print(tested_list)
# for tested_model in tested_list:
#         if os.path.exists('/home/woldy/finhack/data/preds/lgb_model_'+tested_model+'_pred.pkl'):
#                 os.remove('/home/woldy/finhack/data/preds/lgb_model_'+tested_model+'_pred.pkl')








while True:
        with ProcessPoolExecutor(max_workers=28) as pool:
                model_list=mydb.selectToDf('select * from auto_train','finhack')
                #print(model_list)

                tasklist=[]
                #print(model_hash)
                for init_cash in [10000000]:
                        for hold_day in  [10]:
                                for hold_n in  [12,2,4,6,8,10]:
                                        for strategy in ['IndexPlus3']:
                                                for row in model_list.itertuples():
                                                        features_list=getattr(row,'features')
                                                        model_hash=getattr(row,'hash')
                                                        filters_name=getattr(row,'filter')
                                                        # if model_hash in tested_list:
                                                        #         print('model_hash in tested_list')
                                                        #         continue
                                                        if model_hash !="4cc29a3522864b947bf5d31f8c44f84d":
                                                                continue
                                                        
                                                        
                                                        if not os.path.exists(PREDS_DIR+"lgb_model_"+model_hash+"_pred.pkl"):
                                                                print('preds deleted')
                                                                continue          
                                                        
                                                        #print("backtesting "+model_hash)
                                                        
                                                        loss=getattr(row,'loss')
                                                        algorithm=getattr(row,'algorithm')
                        

                                                        time.sleep(1)
                                                        #mytask=pool.submit(start_bt,features_list,model_hash,loss,algorithm,init_cash,hold_day,hold_n,filters_name,strategy)
                                                                
                                                        start_bt(features_list,model_hash,loss,algorithm,init_cash,hold_day,hold_n,filters_name,strategy)
                                                        exit()
                                                                #tasklist.append(mytask)
                                #wait(tasklist, return_when=ALL_COMPLETED)
        time.sleep(60)
print('all done')






# while True:
#         with ProcessPoolExecutor(max_workers=15) as pool:
#                 model_list=mydb.selectToDf('select * from auto_train','finhack')
#                 #print(model_list)


#                 tasklist=[]
#                                 #print(model_hash)
#                 for init_cash in [1000000,2500000,5000000]:
#                         for hold_day in  [5,10]:
#                                 for hold_n in  [int(init_cash/10000),int(init_cash/20000),int(init_cash/50000)]:
#                                         for strategy in ['IndexPlusTR']:#,'aiTopN','IndexPlus','aiTopNTR',]:
#                                                 for row in model_list.itertuples():
#                                                         features_list=getattr(row,'features')
#                                                         model_hash=getattr(row,'hash')
#                                                         filters_name=getattr(row,'filter')
#                                                         # if model_hash in tested_list:
#                                                         #         print('model_hash in tested_list')
#                                                         #         continue
                                                        
                                                        
#                                                         if not os.path.exists(PREDS_DIR+"lgb_model_"+model_hash+"_pred.pkl"):
#                                                                 print('preds deleted')
#                                                                 continue          
                                                        
#                                                         #print("backtesting "+model_hash)
                                                        
#                                                         loss=getattr(row,'loss')
#                                                         algorithm=getattr(row,'algorithm')
                        

#                                                         time.sleep(1)
#                                                         #mytask=pool.submit(start_bt,features_list,model_hash,loss,algorithm,init_cash,hold_day,hold_n,filters_name,strategy)
                                                                
#                                                         start_bt(features_list,model_hash,loss,algorithm,init_cash,hold_day,hold_n,filters_name,strategy)
#                                                                 #tasklist.append(mytask)
#                                 #wait(tasklist, return_when=ALL_COMPLETED)
#         time.sleep(60)
# print('all done')

 