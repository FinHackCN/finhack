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
    
    def pred(df,model='c9909808629985b8934106fda842899b'):
        features='AVGPRICE_0,FLOOR_0,MACDX_0,MINIDX_0,SMA_0,SUB_0,TYPPRICE_0,alpha101_083,alpha191_004,alpha191_005,alpha191_027,alpha191_057,alpha191_060,alpha191_100,alpha191_126,alpha191_153,alpha191_187,totalRevenuePs_0,open'
        features=features.split(',')
        df_pred=df[['ts_code']+features]
        gbm = lgb.Booster(model_file='/home/woldy/finhack/data/models/lgb_model_'+model+'.txt')
        pred=df_pred[['ts_code']]

        print(df_pred.columns.tolist())

        x_pred= df_pred.drop('ts_code', axis=1)  

        # 模型预测
        y_pred = gbm.predict(x_pred, num_iteration=gbm.best_iteration)
        
        print(y_pred)
        
        pred['pred']=y_pred

        
        pred=pred.dropna()
        #pred=pred.sort_values(by='pred',ascending=False)[0:10]
        
        
        return pred
        
        
        
 
        