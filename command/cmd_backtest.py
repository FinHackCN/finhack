import pandas as pd
import importlib
import os
import sys
import datetime
import time
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
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
import redis
from astock.market import market
import argparse
from library.config import config
import itertools

def start_bt(features_list,model_hash,loss,algorithm,init_cash,strategy,strategy_args,params):
                try:
                        train=algorithm+'_'+loss
 
                        args={
                                "features_list":features_list,
                                "train":train,
                                "model":model_hash,
                                "loss":loss,
                                "strategy_args":strategy_args
                        }
              
                        
                        if params['filter_name']!='':
                                args['filter']=params['filter_name']
                                
                                
                                
                        if not os.path.exists(PREDS_DIR+"lgb_model_"+model_hash+"_pred.pkl"):
                                data_train,data_valid,df_pred,data_path=trainhelper.getTrainData('20000101','20100101','20130101',features=features.split(","),label='abs',shift=10)
                                lgbtrain.pred(df_pred,data_path,model_hash)
                                
                
                        bt_instance=bt.run(
                                cash=init_cash,
                                strategy_name=strategy,
                                data_path="lgb_model_"+model_hash+"_pred.pkl",
                                args=args,
                                start_date=params['start_date'],
                                end_date=params['end_date'],
                                benchmark=params['benchmark'],
                                fees=params['fees'],
                                min_fees=params['min_fees'],
                                tax=params['tax'],
                                slip=params['slip'],
                                replace=params['replace'],
                                log_type=params['log_type'],
                                record_type=params['record_type']
                        )
                        return True
                except Exception as e:
                        print(str(e))
                        print("err exception is %s" % traceback.format_exc())        
                



def grid_search(params):
    param_list = list(params.values())
    for values in itertools.product(*param_list):
        yield {list(params.keys())[i]: value for i, value in enumerate(values)}

cfg=config.getConfig('backtest','backtest')

parser = argparse.ArgumentParser(description='backtest args parser')
parser.add_argument('--model', type=str,help='model hash')
parser.add_argument('--thread', type=int,help='thread num')
parser.add_argument('--cash', type=int,help='init cash')
parser.add_argument('--strategy', type=str,help='strategy list')
parser.add_argument('--args', type=str,help='strategy args')
parser.add_argument('--start', type=str,help='start date')
parser.add_argument('--end', type=str,help='end date')
parser.add_argument('--filter', type=str,help='filter name')
parser.add_argument('--benchmark', type=str,help='benchmark')
parser.add_argument('--fees', type=float,help='fees')
parser.add_argument('--min_fees', type=float,help='min_fees')
parser.add_argument('--tax', type=float,help='tax')
parser.add_argument('--slip', type=float,help='slip')
parser.add_argument('--replace', type=int,help='if have,then replace old record')
parser.add_argument('--log', type=int,help='log all transaction')
parser.add_argument('--record', type=int,help='record to database')


#python command/cmd_backtest.py --model=fe13e7d8b3874dda83663d7571aedf85 --thread=1 --cash=100000 --strategy=aiTopN --args="{'hold_n':10,'hold_day':10}" --start=20200101 --end=20230101 --filter=MainBoardNoST --benchmark="000001.SH" --fees=0.0003 --min_fees=5 --tax=0.001 --slip=0.005 --replace=1 --log=1 --record=1

args = parser.parse_args()

model=args.model if args.model!=None else 'all'
thread=args.thread if args.thread!=None else int(cfg['thread'])
cash_list=[args.cash] if args.cash!=None else list(map(int, cfg['cash'].split(',')))
strategy_list=[args.strategy] if args.strategy!=None else cfg['strategy'].split(',')

        
params={
        'start_date':args.start if args.start!=None else cfg['start'],
        'end_date':args.end if args.end!=None else cfg['end'],
        'filter_name':args.filter if args.filter!=None else cfg['filter'] ,
        'benchmark':args.benchmark if args.benchmark!=None else cfg['benchmark'],
        'fees':args.fees if args.fees!=None else float(cfg['fees']),
        'min_fees':args.min_fees if args.min_fees!=None else float(cfg['min_fees']),
        'tax':args.tax if args.tax!=None else float(cfg['tax']),
        'replace':args.replace if args.replace!=None else int(cfg['replace']),
        'slip':args.slip if args.slip!=None else float(cfg['slip']),
        'log_type':args.log if args.log!=None else False,#默认不记日志
        'record_type':args.record if args.record!=None else 9,#根据配置文件判断
}
        

args_list=args.args
if args_list==None:
        cfg_arg_list=config.getConfig('backtest','strategy_args')
        for k in cfg_arg_list:
                cfg_arg_list[k]=list(map(float, cfg_arg_list[k].split(',')))
        
        args_list=grid_search(cfg_arg_list)
else:
        args_list=[json.loads(args_list.replace("'", "\""))]
args_list=list(args_list)

price_state,dividend_state=market.get_state()

if price_state==None or time.time()-price_state>60*60*24:
        print("正在加载行情数据……")
        market.load_price() 
        print("加载行情数据完毕！")
if dividend_state==None or time.time()-dividend_state>60*60*24:
        print("正在加载分红送股数据……")
        market.load_dividend()
        print("加载分红送股数据完毕！")
        
print("开始执行策略！")




while True:
        with ProcessPoolExecutor(max_workers=thread) as pool:
                if model=='all':
                        model_list=mydb.selectToDf('select * from auto_train','finhack')
                else:
                        model_list=mydb.selectToDf('select * from auto_train where hash="'+model+'"','finhack')
                tasklist=[]
                for row in model_list.itertuples():
                        for init_cash in cash_list:
                                for strategy_args in args_list:
                                        for strategy in strategy_list:
                                                features_list=getattr(row,'features')
                                                model_hash=getattr(row,'hash')
                                                filters_name='MainBoardNoST' #只交易主板
                                                if not os.path.exists(PREDS_DIR+"lgb_model_"+model_hash+"_pred.pkl"):
                                                        print('preds deleted')
                                                        continue          
                                                loss=getattr(row,'loss')
                                                algorithm=getattr(row,'algorithm')
                                                time.sleep(1)
                                                if thread==1:
                                                        start_bt(features_list,model_hash,loss,algorithm,init_cash,strategy,strategy_args,params)
                                                else:
                                                        #print('submit')
                                                        mytask=pool.submit(start_bt,features_list,model_hash,loss,algorithm,init_cash,strategy,strategy_args,params)
                                                        tasklist.append(mytask)
                
                if model!='all':
                        break
                wait(tasklist, return_when=ALL_COMPLETED)
        
        time.sleep(60)
