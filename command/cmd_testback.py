# import pandas as pd
# import importlib
# import os
# import sys
# import datetime
# import time
# sys.path.append('/data/code/finhack')
# from library.backtest import bt
# import traceback
# from library.mydb import mydb
# import hashlib
# from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
# from library.astock import AStock
# import json

# def start_bt(features_list,model_hash,loss,algorithm,init_cash,hold_day,hold_n):
#                 try:
#                         train=algorithm+'_'+loss
#                         strategy='aiTopN2'
                        
#                         args={
#                                 "features_list":features_list,
#                                 "train":train,
#                                 "model":model_hash,
#                                 "loss":loss,
#                                 "strategy_args":{
#                                         "hold_day":hold_day,
#                                         "hold_n":hold_n,
#                                 }
#                         }
                
#                         bt_instance=bt.run(
#                             cash=init_cash,
#                             start_date='20100101',
#                             strategy_name=strategy,
#                             data_path="lgb_model_"+model_hash+"_pred.pkl",
#                             args=args
#                         )
#                         return True
#                 except Exception as e:
#                         print(str(e))
#                         print("err exception is %s" % traceback.format_exc())        
                
                

# df_all=AStock.getStockDailyPrice(fq='qfq')
# df_all=AStock.getStockDailyPrice(fq='hfq')



# tested_list=mydb.selectToDf('select model from  (select model,COUNT(model) as c from backtest GROUP BY (model) ) as x where c>=100','finhack')
# tested_list=tested_list['model'].to_list()

# while True:
#         model_list=mydb.selectToDf('select * from auto_train where hash in (select model from backtest where sharpe>0.5)','finhack')
#         for row in model_list.itertuples():
#                 features_list=getattr(row,'features')
#                 model_hash=getattr(row,'hash')
#                 loss=getattr(row,'loss')
#                 algorithm=getattr(row,'algorithm')
      
#                 if model_hash in tested_list:
#                         continue
                
#                 with ProcessPoolExecutor(max_workers=1) as pool:
#                         tasklist=[]
#                         for init_cash in [10000,30000,50000,100000]:
#                                 for hold_day in [3,5,7,9,11]:
#                                         for hold_n in [2,4,6,8,10]:
#                                                 #time.sleep(1)
#                                                 mytask=pool.submit(start_bt,features_list,model_hash,loss,algorithm,init_cash,hold_day,hold_n)
#                                                 tasklist.append(mytask)
#                         wait(tasklist, return_when=ALL_COMPLETED)
#         time.sleep(60)

# for row in strategy_list.itertuples():
#         for init_cash in [10000,30000,50000,100000]:
#                 for hold_day in [10]:
#                         for hold_n in [10]:
#                                 start_bt(row,init_cash,hold_day,hold_n)



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
                            benchmark='000852.SH'
                        )

                        return True
                except Exception as e:
                        print(str(e))
                        print("err exception is %s" % traceback.format_exc())        
                
                

df_all=AStock.getStockDailyPrice(fq='qfq')
df_all=AStock.getStockDailyPrice(fq='hfq')

features_list="ADX_0,AROONDOWN_0,AROONDOWN_14_0,ARRONUP_0,DX_0,FASTDRSI_0,HTDCPHASE_0,LEADSINE_0,MACDFIX_0,MACDHISTFIX_0,MACDSIGNALX_0,MACDSIGNAL_0,MFI_14_0,MININDEX_120_0,PLUSDI_0,QUADRATURE_0,ROCR_0,ROC_14_0,RSI_0,SLOWK_0,ULTOSC_0,WILLR_0,WILLR_14_0,alphaA_010,alphaA_012,alphaA_015,alphaA_018,alphaA_019,alphaA_022,alphaA_024,circMv_0,dvRatio_0,dvTtm_0,totalMv_0,turnoverRate_0"
model_hash="7a0b00aed4ef45c49fed7cbc456b56df"
loss="ds"
algorithm="lgb"
init_cash=1000000
hold_day=10
hold_n=20
filters_name=""
strategy="aiTopN"


start_bt(features_list,model_hash,loss,algorithm,init_cash,hold_day,hold_n,filters_name,strategy)
 