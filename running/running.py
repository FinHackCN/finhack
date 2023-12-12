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
from astock.astock import AStock
from factors.factorManager import factorManager
import warnings
warnings.filterwarnings('ignore')
from library.globalvar import *
#思路
#1、遍历所有因子，取出lastdate的所有因子
#2、把这些因子丢到

class running():
    
    
    def readFactor(factor_name,trade_date,df,n=0):
        #n代表重试次数
        if n>3:
            print("read factor fail:"+factor_name)
            exit()
        path = DATE_FACTORS_DIR+trade_date
        df_tmp=pd.read_csv(path+'/'+factor_name+'.csv',encoding="utf-8-sig", names=["ts_code",'trade_date',factor_name])
        df_tmp.drop_duplicates(subset=['ts_code'],keep='first',inplace=True)
        df_tmp=df_tmp.set_index('ts_code')
                

        if df.empty:
            df=df_tmp
                #数不对，删掉这个文件
        elif len(df_tmp)<4000:
            os.remove(path+'/'+factor_name+'.csv')
            print("数据不对，删除"+path+'/'+factor_name+'.csv')
            running.repair(factor_name,trade_date)
            df=running.readFactor(factor_name,trade_date,df,n+1)
        else:
            df[factor_name]=df_tmp[factor_name]
            if factor_name not in ['name','lLastTime_0','lFirstTime_0','lLimit_0']:
                try:
                    df[factor_name]=df[factor_name].astype('float')
                except Exception as e:
                    print(factor_name)
                    print(str(e))     
        return df
        
    
    def prepare(lastdate=''):
        path = DATE_FACTORS_DIR+lastdate
        df=pd.DataFrame()
        df_list=[]
        #将当日所有因子拼成很长的一行
        for subfile in os.listdir(path):
            if not '__' in subfile:
                factor_name=subfile.split('.')[0]
                df=running.readFactor(factor_name,lastdate,df)
        return df
        
        
    def pred_bt(instance_id='',trade_date='',df=pd.DataFrame()):
        bt_sql="select features_list,model from backtest where instance_id='%s'" % (instance_id)
        bt=mydb.selectToDf(bt_sql,'finhack')
        features=bt['features_list'].values.tolist()[0]
        model=bt['model'].values.tolist()[0]
        if df.empty:
            df=running.prepare(trade_date)
        return running.pred(df,model,features,trade_date)
    
    
    #若date_factors里没有，则使用single_factors中的数据进行修复
    def repair(factor_name,trade_date):
        date_factors_path=DATE_FACTORS_DIR+trade_date+"/"
        if not os.path.exists(date_factors_path): 
            try:
                os.mkdir(date_factors_path)
            except Exception as e:
                print(str(e))  
        
        df=pd.read_csv(SINGLE_FACTORS_DIR+'/'+factor_name+'.csv',encoding="utf-8-sig", names=["ts_code",'trade_date',factor_name])
        df=df[df['trade_date']==int(trade_date)]
        if df.empty or len(df)<1000:
            print(df)
            print(factor_name+"因子修复失败，请先尝试重新计算")
        else:
            print("当日因子行数"+str(len(df)))
            print("正在修复%s日的%s因子" % (trade_date,factor_name))
            print(date_factors_path+factor_name+'.csv')
            df.to_csv(date_factors_path+factor_name+'.csv',mode='w',encoding='utf-8',header=False,index=False)
            
        
    
    def pred(df,model='c0c4544c03c2f63336abb675dd41d6bd',features='',trade_date=""):
        features=features.split(',')
        diff_list=list(set(features) - set(df.columns.tolist()))
        
        #有不存在的因子
        trytimes=0
        while trytimes<10 and diff_list!=[]:
        #if diff_list!=[]:
            for factor in diff_list:
                print('repairing '+factor)
                running.repair(factor,trade_date)
            df=running.prepare(lastdate=trade_date)
            trytimes=trytimes+1

        df=df.reset_index(drop=False)
        
        diff_list=list(set(features) - set(df.columns.tolist()))
        if diff_list!=[]:
            print(trade_date)
            print("修复失败！")
            print(diff_list)
            print(model)
            return
            exit()
        
        
        
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
        pred=pred.reset_index(drop=True)
        return pred
        
        
        
 
        