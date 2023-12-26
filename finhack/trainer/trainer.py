import pandas as pd
from finhack.factor.default.factorManager import factorManager
from math import e
import traceback
import os
import importlib
import lightgbm as lgb
from runtime.constant import *


class Trainer:
    def getTrainData(self,start_date='20000101',valid_date="20080101",end_date='20100101',features=[],label='abs',shift=10,filter_name='',dropna=False,norm=False):
            data_path=DATA_DIR
            df=factorManager.getFactors(factor_list=features+['open','close','high','low'])
            df.reset_index(inplace=True)
            df['trade_date']= df['trade_date'].astype('string')
            df=df.sort_values('trade_date')
            if filter_name!='':
                #filters_module = importlib.import_module('.filters.filters',package='strategies')
                func_filter=getattr(filters,filter_name)
                df=func_filter(df)
    
    
            #绝对涨跌幅
            if label=='abs':
                df['label']=df.groupby('ts_code',group_keys=False).apply(lambda x: x['close'].shift(-1*shift)/x['open'].shift(-1))
            elif label=='mv':
                formula="mean(shift($close,%s),%s)/mean(shift($close,%s),%s)/shift($open,1)+1" % (str(-1*shift),str(shift-1),str(-1*shift),str(shift-1))
                df=df.set_index(['ts_code','trade_date'])
                df_tmp=df[['open','close']]
                df['label']=alphaEngine.calc(formula=formula,df=df_tmp,name="label_mv",check=True)
                
                
            df=df[df.high!=df.low]    

            df_train=df[df.trade_date>=start_date]
            df_train=df[df.trade_date<valid_date]        
            
            df_valid=df[df.trade_date>=valid_date]
            df_valid=df[df.trade_date<end_date]  
    
            df_pred=df[df.trade_date>=end_date]
            
            if dropna:
                df_train=df_train.replace([np.inf, -np.inf], np.nan).dropna()
                df_valid=df_train.replace([np.inf, -np.inf], np.nan).dropna()
            #归一化
            # print(df_valid.columns)
            if norm:
                columns = features
                g = df_valid.groupby('trade_date')[columns]
                df_valid[columns] = (df_valid[columns] - g.transform('min')) / (g.transform('max') - g.transform('min'))
            y_train=df_train['label']
            x_train=df_train.drop('label', axis=1)
            x_train=x_train.drop('close', axis=1)
            x_train=x_train.drop('open', axis=1)
            y_valid=df_valid['label']
            x_valid=df_valid.drop('label', axis=1)  
            x_valid=x_valid.drop('close', axis=1)  
            x_valid=x_valid.drop('open', axis=1) 

            return x_train,y_train,x_valid,y_valid,df_pred,data_path  