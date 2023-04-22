import pandas as pd
import importlib
import os
import sys
import datetime
import time
sys.path.append('/data/code/finhack')
from backtest.backtest import bt
import traceback
from library.mydb import mydb
import hashlib
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
from astock.astock import AStock
import json
from train.trainhelper import trainhelper
import lightgbm as lgb
from library.globalvar import *
import redis
from astock.market import market

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
                            end_date='20230401',
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






market.load_dividend()
market.load_price()

while True:
        with ProcessPoolExecutor(max_workers=27) as pool:
                model_list=mydb.selectToDf('select * from auto_train','finhack')
                tasklist=[]
                for init_cash in [20000]:
                        for hold_day in  [3,5,7]:
                                for hold_n in  [3,5,7]:
                                        for strategy in ['aiTopN','aiTopNTR']:
                                                for row in model_list.itertuples():
                                                        features_list=getattr(row,'features')
                                                        model_hash=getattr(row,'hash')
                                                        filters_name='MainBoardNoST' #只交易主板
                                                        # if model_hash in tested_list:
                                                        #         print('model_hash in tested_list')
                                                        #         continue
                                                        # if model_hash !="4cc29a3522864b947bf5d31f8c44f84d":
                                                        #         continue
                                                        
                                                        
                                                        if not os.path.exists(PREDS_DIR+"lgb_model_"+model_hash+"_pred.pkl"):
                                                                print('preds deleted')
                                                                continue          
                                                        
                                                        #print("backtesting "+model_hash)
                                                        
                                                        loss=getattr(row,'loss')
                                                        algorithm=getattr(row,'algorithm')
                        

                                                        time.sleep(1)
                                                        mytask=pool.submit(start_bt,features_list,model_hash,loss,algorithm,init_cash,hold_day,hold_n,filters_name,strategy)
                                                        #start_bt(features_list,model_hash,loss,algorithm,init_cash,hold_day,hold_n,filters_name,strategy)
                                                        # exit()
                                                        tasklist.append(mytask)
                                wait(tasklist, return_when=ALL_COMPLETED)
        time.sleep(60)
