# -*- coding: utf-8 -*-
import sys
import os
import time
import random
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)
import pickle
from rqalpha.apis import *
from rqalpha import run_func
import time
import math


model_hash="b00a2f5b2d3b55d2b2ccd3ec0fc14003"
hold_day=3
hold_n=3
benchmark="000001.XSHG"

if len(sys.argv)>=2:
    model_hash=sys.argv[1]
if len(sys.argv)>=3:
    hold_n=int(sys.argv[2])
if len(sys.argv)>=4:
    hold_day=int(sys.argv[3])

hook=False


def diff_bt(context,buy_str):

    instance_id="now"
    file="/data/code/finhack/data/logs/backtest/bt_%s.log" % instance_id
    if context.diff['content']=="":
      f=open(file, 'r') 
      context.diff['content'] = f.readlines()
    start=1
    total_value=context.stock_account.total_value
    for i in range(start,len(context.diff['content'])):
      context.diff['n']=context.diff['n']+1
      if "无法" not in context.diff['content'][context.diff['n']+start] and "warn" not in context.diff['content'][context.diff['n']+start]:
        finhack_log=context.diff['content'][context.diff['n']+start].strip().split('[trade')[0]
        rqalpha_log=buy_str.split('[trade')[0]+""+str(round(total_value/10000,0))+" "
      else:
        continue
      
      
      print(finhack_log+" finhack")
      print(rqalpha_log+" rqalpha")
      print("\n")
      if  finhack_log==rqalpha_log:
        return True
      #print("\n")
      
      
      
      
      input('按任意键继续') 
      return True



def on_settlement_handler(context,event):
  return
  now_date=context.now.strftime("%Y%m%d")
  if now_date<"20191213":
    return

  print(event)
  n=0
  for pos in get_positions():
    code=pos.order_book_id  
    value=pos.market_value
    amount=pos.quantity
    n=n+value
    print("%s,%s,%s" % (code,amount,value))
  print("n=%s" % n)
  print('-------------------')
  pass

def on_trade_handler(context,event):
    trade = event.trade
    order = event.order
    account = event.account
    
    code=order.order_book_id
    # print("after trade")
    # print(get_position(code, POSITION_DIRECTION.LONG).quantity)
    # print(get_position(code, POSITION_DIRECTION.LONG).market_value)   
    
    code=code.replace('XSHG','SH') 
    code=code.replace('XSHE','SZ')
    now_date=context.now.strftime("%Y%m%d")
    vol=str(order.filled_quantity)
    price=str(order.avg_price)
    side=""
    if order.side==SIDE.BUY:
      side="买入"
    else:
      side="卖出"
    buy_str="%s %s %s%s股，当前价格%s [trade-rqalpha]" %(now_date,code,side,vol,price)
    
    # print(trade)
    # print(order)
    # print(account.cash)
    # print(account.management_fees)
    # print(account.position_equity)
    # print(account.market_value)
 
    # info={
    #       "total_value":context.stock_account.total_value,
    #       "position_equity":context.stock_account.position_equity,
    #       "cash":context.stock_account.cash
          
    # }
    # print(info)  
    
    # if '000971' in code and now_date=="20180426":
    #   exit()
    #   print(get_position(code, POSITION_DIRECTION.LONG).quantity)
    #   print(get_position(code, POSITION_DIRECTION.LONG).market_value)     
    diff_bt(context,buy_str)
      
    
 
    
    #logger.info(buy_str)


def init(context):
    
    pred_path="/data/code/finhack/data/preds/lgb_model_%s_pred.pkl" % model_hash
    pred=pickle.load(open(pred_path, "rb"))  
    pred=pred.set_index(['trade_date','ts_code'])
    logger.info("init")
    context.pred = pred
    
    context.hold_day=3
    context.hold_n=3
    context.n=0
    context.benchmark=benchmark
    if hook:
      subscribe_event(EVENT.TRADE,on_trade_handler)
      subscribe_event(EVENT.SETTLEMENT,on_settlement_handler)
 
    context.diff={
      "n":0,
      "finhack":"",
      "rqalpha":"",
      "content":""
    }


def before_trading(context):
    pass




def handle_bar(context, bar_dict):
    #print(context.now)
    pass   # order_percent并且传入1代表买入该股票并且使其占有投资组合的100%
    #order_percent(context.s1, 1)
 
 
def open_auction(context, bar_dict):
    #now=time.strftime("%Y%m%d", time.localtime(context.now))
    now_date=context.now.strftime("%Y%m%d")
    
    n=0
    # if now_date=="20180316":
    #     for pos in get_positions():
    #       code=pos.order_book_id  
    #       value=pos.market_value
    #       amount=pos.quantity
    #       n=n+value
    #       print("%s,%s,%s" % (code,amount,value))
    #     print("n=%s" % n)
    #     print('-------------------')
    
    
 
        

        #第9日尾盘清仓
    if context.n % context.hold_day==context.hold_day-1:
        for pos in get_positions():
            code=pos.order_book_id
            order_target_percent(code, 0)
     
        
        #第10日开盘买入
    elif context.n % context.hold_day==0:
        pred=context.pred.loc[now_date]
        pred=pred.sort_values(by='pred',ascending=False, inplace=False) 
        pred=pred.dropna()
        pred=pred[pred.pred>1]
        pred=pred[~pred.index.duplicated()]

        i=1
        for ts_code, row in pred.iterrows():
            if i==context.hold_n:
                break        
               
            ts_code=ts_code.replace('SH','XSHG') 
            ts_code=ts_code.replace('SZ','XSHE')      
            if 'BJ' in ts_code:
                continue
                
            N=context.hold_n
            a1=N
            ai=N-(i-1)*(N/i)
            Sn=N*N/2
            wi=ai/Sn
            
            order_value(ts_code,context.stock_account.cash*wi)
            i=i+1       
    
    context.n=context.n+1
    if context.n>=context.hold_day:
        context.n=0
        
        

        

 




config = {
  "base": {
    "start_date": "2018-01-01",
    "end_date": "2023-04-01",
    "accounts": {
      "stock": 20000
    }
  },
  "extra": {
   "log_level": "error",#verbose | code:info | warning | error
  },
  "mod": {
    "sys_progress": {
            "enabled": True,
            "show": True
    },
    "sys_analyser": {
      "enabled": True,
      "plot": True,
      "output_file": "data/logs/backtest/"+model_hash+".pkl",
      "plot_save_file":"data/logs/backtest/"+model_hash+".png",
      "benchmark":benchmark
    },
    "sys_transaction_cost":{
        "enabled": True,
        "cn_stock_min_commission": 5,
        "commission_multiplier": 0.375,
        "tax_multiplier": 1
      }
  }
}

# 您可以指定您要传递的参数
run_func(init=init, before_trading=before_trading,open_auction=open_auction, handle_bar=handle_bar, config=config)
# 如果你的函数命名是按照 API 规范来，则可以直接按照以下方式来运行
# run_func(**globals())



result_dict = pickle.load(open("data/logs/backtest/"+model_hash+".pkl", "rb"))   # 从输出pickle中读取数据

print(result_dict["summary"])

print("data/logs/backtest/"+model_hash+".pkl")
print("data/logs/backtest/"+model_hash+".png")




 
