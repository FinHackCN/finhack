from factors.factorManager import factorManager
import pandas as pd
import numpy as np
import math
from library.mydb import mydb
import hashlib
import lightgbm as lgb
from math import e
class lgbtrain:
    def run(start_date='20000101',valid_date="20080101",end_date='20100101',features=[],label='close',shift=10):
        features=[
            'TANH_0','CCI_0','buyLgAmount_0','NATR_0','WILLR_0','MACDHISTFIX_0','buyMdVol_0','CMO_0','RSI_0','SQRT_0','LN_0'
            ]
        df=factorManager.getFactors(factor_list=features+[label])
        df.reset_index(inplace=True)
        df['trade_date']= df['trade_date'].astype('string')

        #df['label']=df[label]/df.groupby('ts_code')[label].shift(1*shift)
        
        df['label']=df.groupby('ts_code')[label].apply(lambda x: x.shift(-1*shift)/x)
        
        print(df)
        
        df_train=df[df.trade_date>=start_date]
        df_train=df[df.trade_date<valid_date]        
        
        df_valid=df[df.trade_date>=valid_date]
        df_valid=df[df.trade_date<end_date]  

        df_pred=df[df.trade_date>=end_date]
        
        df_train=df_train.drop('trade_date', axis=1)   
        df_valid=df_valid.drop('trade_date', axis=1)  
        
        df_train=df_train.drop('ts_code', axis=1)   
        df_valid=df_valid.drop('ts_code', axis=1)  
        
        
        y_train=df_train['label']
        x_train=df_train.drop('label', axis=1)
        x_train=x_train.drop('close', axis=1)
        y_valid=df_valid['label']
        x_valid=df_valid.drop('label', axis=1)  
        x_valid=x_valid.drop('close', axis=1)  
        data_train = lgb.Dataset(x_train, y_train)
        data_valid = lgb.Dataset(x_valid, y_valid)        
        
        lgbtrain.train(data_train,data_valid)
        lgbtrain.pred(df_pred)



    def custom_obj(y_pred,y_true): #损失函数
        y_true=y_true.get_label()
        residual = (y_true - y_pred).astype("float")#真实数据与预测数据的差距
        grad = 100*e**(y_pred-y_true)-100
        hess =100*e**(y_pred-y_true)
        return grad, hess

    def custom_eval(y_pred,y_true): #评估函数
        y_true=y_true.get_label()
        residual = (y_true - y_pred).astype("float")
        loss = 100*e**(-residual)+100*residual-100
        return "ds", np.mean(loss), False


    def train(data_train,data_valid):
 
        
        # 参数设置
        params = {
                'boosting_type': 'gbdt',
                'max_depth': 7,
                'num_leaves': 64,  # 叶子节点数
                #'n_estimators':1000,
                'learning_rate': 0.1,  # 学习速率
                'feature_fraction': 0.9,  # 建树的特征选择比例colsample_bytree
                'bagging_fraction': 0.9,  # 建树的样本采样比例subsample
                'bagging_freq': 5,  # k 意味着每 k 次迭代执行bagging
                'verbose': -1,  # <0 显示致命的, =0 显示错误 (警告), >0 显示信息
                'lambda_l1':0,
                'lambda_l2':0, 
  
        }   
        
        print('Starting training...')
        # 模型训练
        gbm = lgb.train(params,
                        data_train,
                        num_boost_round=1000,
                        valid_sets=data_valid,
                        early_stopping_rounds=5,
                        fobj=lgbtrain.custom_obj,
                        feval=lgbtrain.custom_eval
                        
                        )
        
        print('Saving model...')
        # 模型保存
        gbm.save_model('/tmp/lgb_model.txt')
        # 模型加载
        
    def pred(df_pred):
        # df_pred=df_pred.drop('symbol', axis=1)   
        gbm = lgb.Booster(model_file='/tmp/lgb_model.txt')
        pred=df_pred[['ts_code','trade_date']]
        x_pred=df_pred.drop('label', axis=1)  
        x_pred= x_pred.drop('ts_code', axis=1)  
        x_pred= x_pred.drop('trade_date', axis=1)  
        x_pred= x_pred.drop('close', axis=1) 
        # 模型预测
        y_pred = gbm.predict(x_pred, num_iteration=gbm.best_iteration)
        pred['pred']=y_pred
        
        
        
        print(pred)
        
        pred.to_pickle("/tmp/lgb_model.pkl")
        
        return 