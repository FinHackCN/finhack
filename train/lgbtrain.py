from factors.factorManager import factorManager
import pandas as pd
import numpy as np
import math
from library.mydb import mydb
import hashlib
import lightgbm as lgb
from math import e
import traceback
import os
from factors.alphaEngine import alphaEngine
class lgbtrain:
    def score_mv(x,shift,md5):
        pass
        # mypath=os.path.dirname(os.path.dirname(__file__))
        # cache_path=mypath+'/cache/factors/'
        # cache_file=cache_path+'label_'+hashlib.md5(md5.encode(encoding='utf-8')).hexdigest()+'.pkl'
        # if os.path.isfile(cache_file):
        #         return pd.read_pickle(cache_file)  
                
        # tmp=x.copy()
        # open=tmp['open'].shift(-1)
        # for i in range(1,shift):
        #     tmp['x_'+str(i)].append(tmp['close'].shift(-1*i)/open)
        
        # print(tmp)
        
        



        
        
    def run(start_date='20000101',valid_date="20080101",end_date='20100101',features=[],label='abs',shift=10,param={},loss='ds'):
        print("start log_train:loss=%s" % (loss))
        try:
            hashstr=start_date+"-"+valid_date+"-"+end_date+"-"+",".join(features)+","+label+","+str(shift)+","+str(param)+","+str(loss)
            md5=hashlib.md5(hashstr.encode(encoding='utf-8')).hexdigest()


            hassql='select * from auto_train where hash="%s"' % (md5)
            has=mydb.selectToDf(hassql,'finhack')

            #有值且不替换
            if(not has.empty):  
                return md5         

            data_path=os.path.dirname(os.path.dirname(__file__))+'/data'
            df=factorManager.getFactors(factor_list=features+['open','close'])
            df.reset_index(inplace=True)
            df['trade_date']= df['trade_date'].astype('string')
    
            #绝对涨跌幅
            if label=='abs':
                df['label']=df.groupby('ts_code',group_keys=False).apply(lambda x: x['close'].shift(-1*shift)/x['open'].shift(-1))
            elif label=='mv':
                formula="mean(shift($close,%s),%s)/mean(shift($close,%s),%s)/shift($open,1)+1" % (str(-1*shift),str(shift-1),str(-1*shift),str(shift-1))
                df=df.set_index(['ts_code','trade_date'])
                df_tmp=df[['open','close']]
                df['label']=alphaEngine.calc(formula=formula,df=df_tmp,name="label_mv",check=True)
                
            
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
            x_train=x_train.drop('open', axis=1)
            y_valid=df_valid['label']
            x_valid=df_valid.drop('label', axis=1)  
            x_valid=x_valid.drop('close', axis=1)  
            x_valid=x_valid.drop('open', axis=1) 
            data_train = lgb.Dataset(x_train, y_train)
            data_valid = lgb.Dataset(x_valid, y_valid)        
            
            lgbtrain.train(data_train,data_valid,data_path,md5,loss,param)
            lgbtrain.pred(df_pred,data_path,md5)
            
            insert_sql="INSERT INTO auto_train (start_date, valid_date, end_date, features, label, shift, param, hash,loss,algorithm) VALUES ('%s', '%s', '%s', '%s', '%s', %s, '%s', '%s','%s','%s')" % (start_date,valid_date,end_date,','.join(features),label,str(shift),str(param).replace("'",'"'),md5,loss,'lgb')
            
            mydb.exec(insert_sql,'finhack')            
            
            return md5
        except Exception as e:
            print("error:"+str(e))
            print("err exception is %s" % traceback.format_exc())


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


    def train(data_train,data_valid,data_path='/tmp',md5='test',loss="ds",param={}):
 
        
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
                # 'lambda_l1':0,
                # 'lambda_l2':0, 
        }   
        
        
        for key in param.keys():
            params[key]=param[key]
        
        print('Starting training...')
        # 模型训练
        
        if loss=="ds":
            gbm = lgb.train(params,
                            data_train,
                            num_boost_round=1000,
                            valid_sets=data_valid,
                            early_stopping_rounds=5,
                            fobj=lgbtrain.custom_obj,
                            feval=lgbtrain.custom_eval
                            )
        else:
             gbm = lgb.train(params,
                            data_train,
                            num_boost_round=1000,
                            valid_sets=data_valid,
                            early_stopping_rounds=5
                            )           
        
        print('Saving model...')
        # 模型保存
        gbm.save_model(data_path+'/models/lgb_model_'+md5+'.txt')
        # 模型加载
        
    def pred(df_pred,data_path='/tmp',md5='test'):
        # df_pred=df_pred.drop('symbol', axis=1)   
        
        gbm = lgb.Booster(model_file=data_path+'/models/lgb_model_'+md5+'.txt')
        pred=df_pred[['ts_code','trade_date']]
        x_pred=df_pred.drop('label', axis=1)  
        x_pred= x_pred.drop('ts_code', axis=1)  
        x_pred= x_pred.drop('trade_date', axis=1)  
        x_pred= x_pred.drop('close', axis=1) 
        x_pred= x_pred.drop('open', axis=1) 
        # 模型预测
        y_pred = gbm.predict(x_pred, num_iteration=gbm.best_iteration)
        pred['pred']=y_pred
        #今天预测的，其实是明天的
        pred['pred']=pred.groupby('ts_code',group_keys=False).apply(lambda x: x['pred'].shift(1))
        
        pred=pred.dropna()
        
        pred.to_pickle(data_path+'/preds/lgb_model_'+md5+'_pred.pkl')
        
        return 