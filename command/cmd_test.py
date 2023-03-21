import sys
import os
import time
import random
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from library.astock import AStock
from factors.indicatorCompute import indicatorCompute
from library.mydb import mydb
from library.globalvar import *
import pandas as pd
import pickle



model_hash="7c39c4ad28b4e9d4ffca3e5c917c526f"

pkl_file="/data/code/finhack/data/preds/lgb_model_"+model_hash+"_pred.pkl"
with open(pkl_file,'rb') as f:
    df=pickle.load(f)
    df=df[df.pred>1]
    df=df.reset_index(drop=True)
    df.to_csv(pkl_file+".csv")
print(df)




exit()
from astock.indexHelper import indexHelper

index_list=indexHelper.get_index_weights()
print(index_list)

exit()

def in_index(idx_code="000852.SH"):

    
    df_index=mydb.selectToDf("select  * from astock_index_weight where ts_code='%s' " % idx_code,'tushare')
    df_pred=pd.read_pickle(PREDS_DIR+'lgb_model_c004ee451144084e85b2d3f2a577dd57_pred.pkl')
    
    del df_index['ts_code']
    
    df_index['ts_code']=df_index['con_code']
    
    
    df_pred['weight']=0
    
    print(df_pred)
    
    
    
    
    
    for trade_date,df_idx in df_index.groupby('trade_date'):  # 遍历.DataFrameGroupBy对象
        print(trade_date)
        #print(df_idx)
        df_pred.loc[(df_pred.trade_date>trade_date) ,'weight']=0
        for c,w in zip(df_idx.ts_code,df_idx.weight):
            df_pred.loc[(df_pred.trade_date>trade_date) & (df_pred.ts_code==c) ,'weight']=w
            
    
    #df_res=pd.merge(df_pred, df_index, how='left', on='trade_date',validate="one_to_many", copy=True, indicator=False)
    
    print(df_pred)

    pass


in_index()