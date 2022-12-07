import sys
import os
import time
import random
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
import pandas as pd
from library.config import config
from library.mydb import mydb
import lightgbm as lgb
from library.astock import AStock
from factors.factorManager import factorManager


from factors.factorManager import factorManager
import warnings
warnings.filterwarnings('ignore')

#思路
#1、遍历所有因子，取出lastdate的所有因子
#2、把这些因子丢到

class runing():
    

    def prepare(lastdate=''):
        path = os.path.dirname(__file__)+"/../data/date_factors/"+lastdate
        df=pd.DataFrame()
        df_list=[]
        for subfile in os.listdir(path):
            if not '__' in subfile:
                alpha_name=subfile.split('.')[0]
                df_tmp=pd.read_csv(path+'/'+subfile,encoding="utf-8-sig", names=["ts_code",'trade_date',alpha_name])
                df_tmp=df_tmp.set_index('ts_code')
                #df_tmp=df_tmp[[alpha_name]]
                #df_list.append(df_tmp)

                if df.empty:
                    df=df_tmp
                else:
                    df[alpha_name]=df_tmp[alpha_name]
            #df=pd.concat(df_list,axis=1)
        return df
        
        
    def pred_bt(instance_id='',trade_date='',df=pd.DataFrame()):
        bt_sql="select features_list,model from backtest where instance_id='%s'" % (instance_id)
        bt=mydb.selectToDf(bt_sql,'finhack')
        features=bt['features_list'].values.tolist()[0]
        model=bt['model'].values.tolist()[0]
        if df.empty:
            df=runing.prepare(trade_date)
        return runing.pred(df,model,features)
    
    
    def pred(df,model='c0c4544c03c2f63336abb675dd41d6bd',features=''):
        features=features.split(',')
        df=df.reset_index(drop=False)
        df_pred=df[['ts_code']+features]
        gbm = lgb.Booster(model_file='/home/woldy/finhack/data/models/lgb_model_'+model+'.txt')
        pred=df_pred[['ts_code']]

        #print(df_pred.columns.tolist())

        x_pred= df_pred.drop('ts_code', axis=1)  
        # 模型预测
        y_pred = gbm.predict(x_pred, num_iteration=gbm.best_iteration)
        pred['pred']=y_pred
        pred=pred.dropna()
        pred=pred.sort_values(by='pred',ascending=False)
        pred=pred.reset_index()
        return pred
        
        
        
 
        