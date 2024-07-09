
import pandas as pd
import traceback
import hashlib
import lightgbm as lgb
import json
from math import e
import numpy as np
from runtime.constant import *
from finhack.library.mydb import mydb
import os
import runtime.global_var as global_var
from finhack.library.config import Config
from finhack.factor.default.preCheck import preCheck
from finhack.factor.default.indicatorCompute import indicatorCompute
from finhack.factor.default.alphaEngine import alphaEngine
from finhack.market.astock.astock import AStock
from finhack.factor.default.taskRunner import taskRunner
from finhack.factor.default.factorManager import factorManager
from lightgbm import log_evaluation, early_stopping
from finhack.trainer.trainer import Trainer
import shutil
import random
import tracemalloc
import multiprocessing
from multiprocessing import Process, current_process

def start_train_wrapper(start_date, valid_date, end_date, features, label, shift, param, loss, filter_name, replace):
    try:
        trainer = LightgbmTrainer()  # 假设LightgbmTrainer可以无参数初始化
        trainer.start_train(
            start_date=start_date,
            valid_date=valid_date,
            end_date=end_date,
            features=features,
            label=label,
            shift=shift,
            param=param,
            loss=loss,
            filter_name=filter_name,
            replace=replace
        )
    except Exception as e:
        print(f"Error in process {current_process().name}: {e}")
        traceback.print_exc()


class LightgbmTrainer(Trainer):
    # ... 保持您原有的方法不变 ...

    def auto(self):
        args = global_var.args
        max_processes=int(args.process)
        
        processes = []

        while True:
            # 清理已经完成的进程
            processes = [p for p in processes if p.is_alive()]

            # 如果当前进程数量小于最大进程数，则创建新进程
            if len(processes) < max_processes:
                try:
                    flist = factorManager.getFactorsList()
                    random.shuffle(flist)
                    n = random.randint(int(args.min_f), int(args.max_f))
                    factor_list = [flist.pop() for _ in range(n)]
                    factor_list.sort()
                    df = factorManager.getFactors(factor_list=factor_list + ['open', 'close'])
                    correlation_matrix = df.corr(numeric_only=True)
                    new_factor_list = []

                    for factor in factor_list:
                        if factor in ['open', 'close']:
                            continue
                        append = True
                        for factor2 in new_factor_list:
                            if abs(correlation_matrix[factor][factor2]) > 0.7:
                                append = False
                                break
                        if append:
                            new_factor_list.append(factor)

                    p = Process(target=start_train_wrapper, args=(
                        args.start_date,
                        args.valid_date,
                        args.end_date,
                        new_factor_list,
                        args.label,
                        int(args.shift),
                        json.loads(args.param) if args.param != '' else {},
                        args.loss,
                        args.filter_name,
                        args.replace,
                    ))
                    p.start()
                    processes.append(p)

                except Exception as e:
                    print("error:"+str(e))
                    print("err exception is %s" % traceback.format_exc())
            else:
                # 如果达到了最大进程数，则等待任一进程完成
                for p in processes:
                    p.join(timeout=0.1)
                    if not p.is_alive():
                        break

        
    def run(self):
        args=global_var.args
        return self.start_train(
            start_date=args.start_date,
            valid_date=args.valid_date,
            end_date=args.end_date,
            features=args.features.split(',') if args.features!='' else [],
            label=args.label,
            shift=int(args.shift),
            param=json.loads(args.param) if args.param!='' else {},
            loss=args.loss,
            filter_name=args.filter_name,
            replace=args.replace
        )
        #finhack trainer run --vendor=lightgbm --features="alpha101_001,alpha191_001,pe_0,WILLR_0,MORNINGDOJISTAR_0" --start_date=20200101 --valid_date=20210101 --end_date=20220101
        
    
    def start_train(self,start_date='20000101',valid_date="20080101",end_date='20100101',features=[],label='abs',shift=10,param={},loss='ds',filter_name='',replace=False):
        print("start log_train:loss=%s" % (loss))
        try:
            hashstr=start_date+"-"+valid_date+"-"+end_date+"-"+",".join(features)+","+label+","+str(shift)+","+str(param)+","+str(loss)+filter_name
            md5=hashlib.md5(hashstr.encode(encoding='utf-8')).hexdigest()


            hassql='select * from auto_train where hash="%s"' % (md5)
            has=mydb.selectToDf(hassql,'finhack')

            #有值且不替换
            if(not has.empty):  
                if replace==False:
                    return md5
 
            data_train,data_valid,df_pred,data_path=self.getLGBTrainData(start_date=start_date,valid_date=valid_date,end_date=end_date,features=features,label=label,shift=shift,filter_name='')


            
            self.train(data_train,data_valid,data_path,md5,loss,param)
            self.pred(df_pred,data_path,md5,True)
            insert_sql="INSERT INTO auto_train (start_date, valid_date, end_date, features, label, shift, param, hash,loss,algorithm,filter) VALUES ('%s', '%s', '%s', '%s', '%s', %s, '%s', '%s','%s','%s','%s')" % (start_date,valid_date,end_date,','.join(features),label,str(shift),str(param).replace("'",'"'),md5,loss,'lgb',filter_name)
            if(has.empty): 
                mydb.exec(insert_sql,'finhack')            
            self.score(md5)
            return md5
        except Exception as e:
            print("error:"+str(e))
            print("err exception is %s" % traceback.format_exc())



    def getLGBTrainData(self,start_date='20000101',valid_date="20080101",end_date='20100101',features=[],label='abs',shift=10,filter_name=''):
        x_train,y_train,x_valid,y_valid,df_pred,data_path=self.getTrainData(start_date,valid_date,end_date,features,label,shift,filter_name)
        
        x_train=x_train.drop('ts_code', axis=1)   
        x_valid=x_valid.drop('ts_code', axis=1)  
        x_train=x_train.drop('trade_date', axis=1)   
        x_valid=x_valid.drop('trade_date', axis=1)          
        
        data_train = lgb.Dataset(x_train, y_train)
        data_valid = lgb.Dataset(x_valid, y_valid)  

        return data_train,data_valid,df_pred,data_path    

    def custom_obj(self,y_pred,y_true): #损失函数
        y_true=y_true.get_label()
        residual = (y_true - y_pred).astype("float")#真实数据与预测数据的差距
        grad = 100*e**(y_pred-y_true)-100
        hess =100*e**(y_pred-y_true)
        return grad, hess

    def custom_eval(self,y_pred,y_true): #评估函数
        y_true=y_true.get_label()
        residual = (y_true - y_pred).astype("float")
        loss = 100*e**(-residual)+100*residual-100
        #print(np.mean(loss))
        return "ds", np.mean(loss), False


    def train(self,data_train,data_valid,data_path=DATA_DIR,md5='test',loss="ds",param={}):
        args=global_var.args
        #cfg=Config.get_config('train','lightgbm')
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
                'verbosity': -1,
                "device" : args.device
        }   
        
        
        for key in param.keys():
            params[key]=param[key]
        
        print('Starting training...')
        # 模型训练
        #callbacks = [log_evaluation(period=100), early_stopping(stopping_rounds=30)]
        callbacks = [lgb.early_stopping(30, verbose=0), lgb.log_evaluation(period=0)]
        if loss=="ds":
            params['objective']=self.custom_obj
            gbm = lgb.train(params,
                            data_train,
                            num_boost_round=100,
                            valid_sets=data_valid,
                            callbacks=callbacks,

                            feval=self.custom_eval
                            )
        else:
             gbm = lgb.train(params,
                            data_train,
                            num_boost_round=100,
                            valid_sets=data_valid,
                            callbacks=callbacks
                            )           
        model_file=data_path+'/models/lgb_model_'+md5+'.txt'
        print('Saving model to '+model_file+'...')
        # 模型保存
        gbm.save_model(model_file)
        # 模型加载
        
    def pred(self,df_pred,data_path=DATA_DIR,md5='test',save=False):
        if 'symbol' in df_pred:
            df_pred=df_pred.drop('symbol', axis=1)   

        gbm = lgb.Booster(model_file=data_path+'/models/lgb_model_'+md5+'.txt')
        pred=df_pred[['ts_code','trade_date']]
        if 'label' in df_pred:
            x_pred=df_pred.drop('label', axis=1) 
        else:
            x_pred=df_pred
        x_pred= x_pred.drop('ts_code', axis=1)  
        x_pred= x_pred.drop('trade_date', axis=1)  
        
        if 'close' in x_pred:
            x_pred= x_pred.drop('close', axis=1) 
        if 'open' in x_pred:
            x_pred= x_pred.drop('open', axis=1) 
        # 模型预测
        y_pred = gbm.predict(x_pred, num_iteration=gbm.best_iteration)
        pred['pred']=y_pred
        #今天预测的，其实是明天要操作的;所以要把今天的数据写成昨天的值


        # 确定数据集中的最后一个交易日
        last_trade_date = pd.to_datetime(pred['trade_date'].max(), format='%Y%m%d')
        # 计算下一个交易日（注意这里没有考虑周末或假日）
        next_trade_date = (last_trade_date + pd.Timedelta(days=1)).strftime('%Y%m%d')

        # 获取最后一个交易日的预测值
        last_trade_date_pred = pred[pred['trade_date'] == last_trade_date.strftime('%Y%m%d')]

        # 创建新的DataFrame，包含每个ts_code的下一个交易日
        next_day_pred = last_trade_date_pred.copy()
        next_day_pred['trade_date'] = next_trade_date  # 设置新的交易日为下一个交易日

        # 将新的交易日DataFrame添加到原始预测DataFrame中
        pred = pd.concat([pred, next_day_pred], ignore_index=True)


        pred=pred.sort_values('trade_date')
        pred['pred'] = pred.groupby('ts_code')['pred'].transform(lambda x: x.shift(1))

        
        pred=pred.dropna()
        
        if save:
            pred.to_pickle(data_path+'/preds/lgb_model_'+md5+'_pred.pkl')
        else:
            return pred
            
        
        
        return 
    
    
    
    def score(self,md5='test',df_pred=pd.DataFrame()):
        data_path=DATA_DIR
        model=mydb.selectToDf('select * from auto_train where hash="'+md5+'"','finhack')
        model=model.iloc[0]
        start_date=model['start_date']
        valid_date=model['valid_date']
        end_date=model['end_date']
        features=model['features'].split(',')
        label=model['label']
        shift=model['shift']
        filter_name=model['filter']        
        
        
        
        pred_file=data_path+'/preds/lgb_model_'+md5+'_pred.pkl'
        
        if os.path.exists(pred_file):
            df_preded=pd.read_pickle(pred_file)
            df_preded=df_preded.set_index(['ts_code','trade_date']) 
            
            df=factorManager.getFactors(factor_list=['open','close'])

        else:
            print(pred_file+" not found!")
            return False
        
        if model.empty:
            print("model "+md5+" is empty!")
            return False

 

        df = df_preded.join(df, how='left')

        #df['pred']=df_preded['pred']
        df['label']=df.groupby('ts_code',group_keys=False).apply(lambda x: x['close'].shift(-1*shift)/x['open'].shift(-1))
        df=df.dropna()
        
        
        count = len(df[df['pred'] > df['label']])
        mean_diff = (df['label'] - df['pred']).mean()
        
        std = (df['label'] - df['pred']).std()
        score=mean_diff/std
        
        sql="UPDATE auto_train SET score = %s WHERE hash='%s'" %(score,md5)
        
        mydb.exec(sql,'finhack')
        # if score<0.03:
        os.remove(pred_file)
        #print(score)
        pass