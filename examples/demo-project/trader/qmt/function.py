from trader.qmt.context import context
from trader.qmt.data import Data
from trader.qmt.object import Order,Position
from trader.qmt.rules import Rules
from trader.qmt.qmtClient import qclient
import re
from finhack.library.config import Config
import bisect
import redis
import finhack.library.log as Log
import hashlib
import json
from runtime.constant import *
import os
import pandas as pd
from finhack.trainer.trainer import Trainer
import shutil
from datetime import datetime, timedelta
from finhack.trainer.lightgbm.lightgbm_trainer import LightgbmTrainer

def init_context(args):
    args=args.__dict__
    
    context['args']=args['args']
    context['trade']['market']=args['market']
    context['trade']['start_time']=args['start_time']
    context['trade']['end_time']=args['end_time']
    context['trade']['benchmark']=args['benchmark']
    context['trade']['strategy']=args['strategy']
    strategy_path=f"{BASE_DIR}/strategy/{args['strategy']}.py"
    with open(strategy_path, 'r', encoding='utf-8') as file:
        context['trade']['strategy_code'] = file.read()

    if args['args']!=None and args['args']!='':
        aargs=json.loads(args['args'])
        context['args']=aargs
        if 'model_id' in aargs:
            context['trade']['model_id']=aargs['model_id']
    else:
        context['args']={}
    if args['model_id']!='':
        context['trade']['model_id']=args['model_id']

    context['trade']['slip']=float(args['slip'])
    context['trade']['sliptype']=args['sliptype']
    context['trade']['rule_list']=args['rule_list']
    
    
    # context['account']['username']=args['username']
    # context['account']['password']=args['password']
    # context['account']['account_id']=''


    # context['account']['open_tax']=args['open_tax']
    # context['account']['close_tax']=args['close_tax']
    # context['account']['open_commission']=args['open_commission']
    # context['account']['close_commission']=args['close_commission']
    # context['account']['close_today_commission']=args['close_today_commission']
    # context['account']['min_commission']=args['min_commission']
        
    context['portfolio']['previous_value']=float(args['cash'])
    context['portfolio']['total_value']=float(args['cash'])
    context['portfolio']['cash']=float(args['cash'])
    context['portfolio']['starting_cash']=float(args['cash'])
    context['data']['data_source']=args['data_source']
    
    if args['data_source']=='redis':
        cfg=Config.get_config('db','redis')
        redisPool = redis.ConnectionPool(host=cfg['host'],port=int(cfg['port']),password=cfg['password'],db=int(cfg['db']))
        client = redis.Redis(connection_pool=redisPool) 
        context.data.client=client
    
    context_json = str(args)+str(context['trade'])+str(context['account'])+str(context['portfolio']['cash'])
    hash_value = hashlib.md5(context_json.encode()).hexdigest()
    context.id=hash_value
    # qclient.assetSync(context)
    # qclient.positionSync(context)


def set_benchmark(code):
    context['trade']['benchmark']=code
    
def set_option(key,value):
    if key in context['trade']:
        context['trade'][key]=value
    elif key in context['account']:
        context['account'][key]=value
    elif key in context['portfolio']:
        context['portfolio'][key]=value
    pass

def set_order_cost(cost,type=None):
    pass

def set_slippage(obj,type=None):
    pass
    
def insert_sorted_list(sorted_list, new_element):
    bisect.insort(sorted_list, new_element, key=lambda x: x['event_time'])
    
def run_interval(func,time,interval='daily',date_list=[]):
    if time=='every_bar':
        time='00:00:01'
    pattern = r"\d{1,2}:\d{2}"
    match = re.match(pattern, time)
    if match and time.count(':')==1:
        hours, minutes = time.split(":")
        time = f"{hours.zfill(2)}:{minutes.zfill(2)}:00"
     
    for date in date_list:
        new_event={
            'event_name':func.__name__,
            'event_func':func,
            'event_time':date+' '+time,
            'event_type':'user_event'
        }
        context['data']['event_list'].append(new_event)
        context['data']['event_list'].sort(key=lambda x: x['event_time'])
    
def run_daily(func,time,reference_security=None):
    return run_interval(func,time,'daily',date_list=context['data']['calendar'])


# #这个函数是按交易日，预留，懒得写逻辑
# def run_tmonthly(func, monthday, time='9:30', reference_security=None, force=False):
#     return run_interval(func,time,interval='monthly',days=monthday)

# #此处和聚宽有差异，这里是自然日
# def run_monthly(func, monthday, time='9:30', reference_security=None, force=False):
#     return run_interval(func,time,interval='monthly',days=monthday)

# # 按周运行
# def run_weekly(func, weekday, time='9:30', reference_security=None, force=False):
#     return run_interval(func,time,interval='daily',days=weekday)


def inout_cash(cash,pindex=None):
    context['portfolio']['cash']=context['portfolio']['cash']+cash
    

    
#目标股数下单
def order_target(security, amount, style=None, side='long', pindex=0, close_today=False):
    pass
    
    cash=context['portfolio']['available_cash']
    context['portfolio']['cash']=context['portfolio']['cash']+cash
    context['portfolio']['available_cash']=context['portfolio']['available_cash']+cash
    

#按股数下单
def order(security, amount, style=None, side='long', pindex=0, close_today=False):
    if amount>0:
        return order_buy(security,amount)
    else:
        return order_sell(security,-amount)



    
#按价值下单
def order_value(security, value, style=None, side='long', pindex=0, close_today=False):
    price=Data.get_price(code=security,context=context)
    if price==0:
        return False
    #print(price)
    if price==None:
        #print(f"can not get price of {security}")
        return  False
    if value>0:
        amount=int(value/price)
        return order_buy(security,amount)
    elif value<0:
        amount=-int(value/price)
        return order_sell(security,amount)
        

# #目标股数下单
# def order_target(security, amount, style=None, side='long', pindex=0, close_today=False):
#     if amount>0:
#         order_buy(security,amount)
#     else:
#         order_sell(security,amount)
        

#目标价值下单
def order_target_value(security, value, style=None, side='long', pindex=0, close_today=False):
    price=Data.get_price(code=security,context=context)
    if price==None:
        #print(f"can not get price of {security}")
        return  False
    target_amount=int(value/price)
    now_amount=context.portfolio.positions[security].amount
    change_amount=target_amount-now_amount
    return order(security,change_amount)
    
    # now_value=0
    # price=Data.get_price(code=security,context=context)
    # if price==None:
    #     #print(f"can not get price of {security}")
    #     return  
    # if security in context.portfolio.positions:
    #     pos=context.portfolio.positions[security]
    #     now_value=pos.amount*price
    # print("order_target_value",value,now_value)
    # print("last_price:"+str(pos.last_sale_price))
    # print("now_price:"+str(price))
    # print("diff:"+str(price/pos.last_sale_price))
    # order_value(security,value-now_value)



#cancel_order 撤单
def cancel_order(order):
    pass

#获取未完成订单
def get_open_orders():
    pass

#获取订单信息
def get_orders(order_id=None, security=None, status=None):
    pass

#获取成交信息
def get_trades():
    pass




# total_value: 总的权益, 包括现金, 保证金(期货)或者仓位(股票)的总价值, 可用来计算收益
# returns: 总权益的累计收益；（当前总资产 + 今日出入金 - 昨日总资产） / 昨日总资产；
# starting_cash: 初始资金, 现在等于 inout_cash
# positions_value: 持仓价值
#买入


# class Position():
#     def __init__(self,code,amount,enable_amount,last_sale_price):
#         self.code=code
#         self.amount=amount
#         self.enable_amount=enable_amount
#         self.last_sale_price=last_sale_price
#         self.cost_basis=last_sale_price
#         self.total_value=amount*last_sale_price

def order_buy(security,amount,price=0):  
    o=Order(code=security,amount=amount,is_buy=True,context=context)
    rules=Rules(order=o,context=context,log=log)
    
    o=rules.apply()
    print(o)

    if o.status!=1:
        return False
    if o.amount==0:
        #log(f"{o.code}--{o.amount}，买单数为0，自动取消订单")  
        return False

    qclient.OrderBuy(o.code,o.amount,o.price)
    log(f"下单买入{o.code}共计{o.amount}股，单价{o.price}")   
    return True
      

def order_sell(security,amount,price=0):
    o=Order(code=security,amount=amount,is_buy=False,context=context)
    rules=Rules(order=o,context=context,log=log)
    o=rules.apply()
    if o.status!=1:
        return False
    if o.amount==0:
        log(f"{o.code}--{o.amount}，卖单数为0，自动取消订单")  
        return False
    qclient.OrderSell(o.code,o.amount,o.price)
    log(f"下单卖出{o.code}共计{o.amount}股，单价{o.price}")   
    return True
    

    
    
    

def log(message,level='info'):
    if context.current_dt==None:
        msg='0000-00-00 00:00:00'+" - "+message
    else:
        msg=str(context.current_dt)+" - "+message
    if level == 'info':
        Log.tlogger.info(msg)
    elif level == 'error':
        Log.tlogger.error(msg)
    elif level in ['trace', 'debug', 'success', 'warning', 'critical']:
        getattr(Log.tlogger, level)(msg)
    else:
        Log.tlogger.info(msg)    
    


def load_preds_data(model_id,cache=False,trainer='lightgbm',start_time="",end_time=""):
    pred_data_path=PREDS_DIR+f"model_{model_id}_pred.pkl"
    if cache==True:
        try:
            if os.path.isfile(pred_data_path):
                pred_data=pd.read_pickle(pred_data_path)
                return pred_data
        except Exception as e:
            pass

    start_time=context.trade.start_time if start_time=="" else start_time
    end_time=context.trade.end_time if end_time=="" else end_time
    start_date=start_time.replace("-",'')[0:8]
    end_date=end_time.replace("-",'')[0:8]
    preds_df=Trainer.getPredData(model_id,start_date,end_date)
    clsLgbTrainer=LightgbmTrainer()
    preds=clsLgbTrainer.pred(preds_df,md5=model_id,save=False)

    if cache==True:
        preds.to_pickle(pred_data_path)
    return preds
    
def delete_preds_data(model_id):
    pred_data_path=PREDS_DIR+f"model_{model_id}_pred.pkl"
    if os.path.exists(pred_data_path):
        os.remove(pred_data_path)
    
 
def bind_action(strategy):
    strategy.set_benchmark=set_benchmark
    strategy.set_option=set_option
    strategy.set_order_cost=set_order_cost
    strategy.set_slippage=set_slippage
    strategy.run_daily=run_daily
    #懒得写逻辑，后面补吧
    # strategy.run_weekly=run_weekly
    # strategy.run_monthly=run_monthly
    strategy.inout_cash=inout_cash
    strategy.order_value=order_value
    strategy.order_target_value=order_target_value
    strategy.order_buy=order_buy
    strategy.order_sell=order_sell
    strategy.log=log
    strategy.load_preds_data=load_preds_data
    strategy.get_price=Data.get_price
    strategy.sync=qclient.sync



