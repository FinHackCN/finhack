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
from running.running import running
from library.globalvar import *
class simulate:
    def updateTopBt(n=10):
        # top_features_sql="SELECT avg(annual_return) as mean,count(features_list) as c,features_list FROM `finhack`.`backtest`  where sortino>2 and max_down>-0.5  GROUP BY features_list order  by mean desc";
        # top_features=mydb.selectToDf(top_features_sql,'finhack')
        # for row in top_features.itertuples():
        #     features=getattr(row,'features_list')
        #     mydb.exec("update backtest set simulate=1 where max_down>-0.3 and annual_return>0 and instance_id in (select t2.instance_id from (SELECT instance_id FROM backtest where features_list='%s' ORDER BY sortino desc limit 10) as t2)" % features,'finhack')
        
        #mydb.exec("update backtest set simulate=0" ,'finhack')
        #mydb.exec("update backtest set simulate=1 where max_down>-0.5 and annual_return>0.15 and sharpe>0.8 ORDER BY sortino desc limit %s" % str(n),'finhack')
        mydb.exec("update backtest set simulate=1 where instance_id in (select t2.instance_id from (SELECT instance_id FROM backtest  ORDER BY sharpe desc limit %s) as t2)" % str(n),'finhack')

    def getSimulateList():
        mydb.exec("truncate table  simulate_record",'woldycvm')
        mydb.exec("update backtest set simulate=0",'finhack')
        simulate.updateTopBt()
        bt_list=mydb.selectToDf("SELECT * FROM backtest where simulate=1 order by sortino desc",'finhack')
        bt_list['rank']=bt_list['sortino'].rank(ascending=False)
        bt_list['rank']=bt_list['rank'].astype(int)
        return bt_list
        
    def loadData(bt_list):
        date_list=[]
        for subfile in os.listdir(DATE_FACTORS_DIR):
            if not '__' in subfile:
                date_list.append(subfile)
                
        date_list.sort()
        date_list = [d for d in date_list if d>'20221208']
        for row in bt_list.itertuples():
            model_hash=getattr(row,'model')
            instance_id=getattr(row,'instance_id')
            features_list=getattr(row,'features_list')      
        
            running_pred_path=RUNNING_DIR+'running_pred_'+model_hash+'.csv'
            
            old_date=[]
            if os.path.isfile(running_pred_path):
                pred_df=pd.read_csv(running_pred_path,names=['ts_code','pred','trade_date'])
                pred_df['trade_date']=pred_df['trade_date'].astype('string')
                old_date=list(set(pred_df['trade_date'].to_list()))
                
            diff_date=list(set(date_list) - set(old_date))  
            if 'nan' in diff_date:
                diff_date.remove('nan')

            for date in diff_date:
                pred=running.pred_bt(instance_id=instance_id,trade_date=date)
                #print(pred)
                pred['trade_date']=date
                pred.to_csv(running_pred_path,mode='a',encoding='utf-8',header=False,index=False)
                pass
            
            
            
            
            pred_df=pd.read_csv(running_pred_path,names=['ts_code','pred','trade_date'])
            pred_df=pred_df.sort_values('trade_date')
            pred_df['pred']=pred_df.groupby('ts_code',group_keys=False).apply(lambda x: x['pred'].shift(1))
            pred_df=pred_df.dropna()
            pred_df['pred']=pred_df['pred'].astype('float')
            pred_df['trade_date']=pred_df['trade_date'].astype('string')
            pkl_path=PREDS_DIR+"lgb_model_simulate_"+model_hash+"_pred.pkl"
            pred_df.to_pickle(pkl_path)
        
    def testAll():
        bt_list=simulate.getSimulateList()
        for row in bt_list.itertuples():
            args=getattr(row,'args')
            args=json.loads(args)
            model_hash=getattr(row,'model')
            init_cash=getattr(row,'init_cash')
            strategy=getattr(row,'strategy')
            created_at=getattr(row,'created_at')
            instance_id=getattr(row,'instance_id')
            rank=getattr(row,'rank')
            strategy_name=strategy.split('_')[0]
  
            bt_instance=bt.run(
                    instance_id=instance_id,
                    cash=int(init_cash),
                    start_date='20221209',
                    end_date='20990101',
                    strategy_name=strategy_name,
                    data_path="lgb_model_simulate_"+model_hash+"_pred.pkl",
                    args=args,
                    g={'rank':rank},
                    replace=True,
                    type='simulate',
                    slip=0.0015
            )
            

    
    