from .context import context
from .data import Data
from .object import Order,Position
from .rules import Rules
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

def init_context(args):
    args=args.__dict__
    
    context['args']=args['args']
    context['trade']['market']=args['market']
    context['trade']['start_time']=args['start_time']
    context['trade']['end_time']=args['end_time']
    context['trade']['benchmark']=args['benchmark']
    context['trade']['strategy']=args['strategy']
    context['trade']['slip']=float(args['slip'])
    context['trade']['sliptype']=args['sliptype']
    context['trade']['rule_list']=args['rule_list']
    
    
    context['account']['username']=args['username']
    context['account']['password']=args['password']
    context['account']['account_id']=''


    context['account']['open_tax']=args['open_tax']
    context['account']['close_tax']=args['close_tax']
    context['account']['open_commission']=args['open_commission']
    context['account']['close_commission']=args['close_commission']
    context['account']['close_today_commission']=args['close_today_commission']
    context['account']['min_commission']=args['min_commission']
        
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
    
    context_json = str(context['trade'])+str(context['account'])+str(context['portfolio']['cash'])
    hash_value = hashlib.sha256(context_json.encode()).hexdigest()
    context.id=hash_value



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
    context['account']['open_tax']=cost.open_tax
    context['account']['close_tax']=cost.close_tax
    context['account']['open_commission']=cost.open_commission
    context['account']['close_commission']=cost.close_commission
    context['account']['close_today_commission']=cost.close_today_commission
    context['account']['min_commission']=cost.min_commission

def set_slippage(obj,type=None):
    context['trade']['slip']=obj.value
    context['trade']['sliptype']=obj.type
    
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
    
    

#这里粗算一下，不算那么细了，没必要
def compute_cost(value,action='open'):
    #绝对值
    if value<0:
        value=-value
    account=context.account
    tax=value*account[action+"_tax"]
    commission=value*account[action+"_commission"]
    if commission<account['min_commission']:
        commission=account['min_commission']
    cost=tax+commission
    return tax,commission,cost
    

#按股数下单
def order(security, amount, style=None, side='long', pindex=0, close_today=False):
    if amount>0:
        order_buy(security,amount)
    else:
        order_sell(security,-amount)



    
#按价值下单
def order_value(security, value, style=None, side='long', pindex=0, close_today=False):
    price=Data.get_price(code=security,context=context)
    #print(price)
    if price==None:
        #print(f"can not get price of {security}")
        return
    if value>0:
        amount=int(value/price)
        order_buy(security,amount)
    elif value<0:
        amount=-int(value/price)
        order_sell(security,amount)
        

#目标股数下单
def order_target(security, amount, style=None, side='long', pindex=0, close_today=False):
    if amount>0:
        order_buy(security,amount)
    else:
        order_sell(security,amount)
        

#目标价值下单
def order_target_value(security, value, style=None, side='long', pindex=0, close_today=False):
    price=Data.get_price(code=security,context=context)
    if price==None:
        #print(f"can not get price of {security}")
        return  
    target_amount=int(value/price)
    now_amount=context.portfolio.positions[security].amount
    change_amount=target_amount-now_amount
    order(security,change_amount)
    
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

def order_buy(security,amount):
    o=Order(code=security,amount=amount,is_buy=True,context=context)
    rules=Rules(order=o,context=context,log=log)
    
    o=rules.apply()
    #print(o)
    
    
    if o.status!=1:
        return
    if o.amount==0:
        #log(f"{o.code}--{o.amount}，买单数为0，自动取消订单")  
        return
    is_new=False
    #没有持仓
    if security not in context.portfolio.positions:
        is_new=True
        pos=Position(code=security,amount=o.amount,enable_amount=o.enable_amount,last_sale_price=o.last_sale_price)
        pos.total_cost=pos.total_cost+o.cost
        context.portfolio.positions[security]=pos
        context.portfolio.cash=context.portfolio.cash-o.value-o.cost
        context.portfolio.positions_value=context.portfolio.positions_value+o.value
        context.portfolio.total_value=context.portfolio.cash+context.portfolio.positions_value
        
    else:
        pos=context.portfolio.positions[security]
        #last_value=pos.total_value
        
        pos.amount=pos.amount+o.amount
        pos.enable_amount=pos.enable_amount
        pos.last_sale_price=o.last_sale_price
        pos.total_cost=pos.total_cost+o.cost+o.amount*o.price
        pos.cost_basis=pos.total_cost/pos.amount
        pos.total_value=pos.amount*pos.last_sale_price
        
        context.portfolio.cash=context.portfolio.cash-o.value-o.cost
        context.portfolio.positions_value=context.portfolio.positions_value+o.value
        context.portfolio.total_value=context.portfolio.cash+context.portfolio.positions_value
        
    now_date=context.current_dt.strftime('%Y%m%d')
    buy_value = context.logs.history.get(now_date, {}).get('buy_value', 0)
    if buy_value != 0:
        # 如果存在 buy_value 键，则执行对应的操作
        context.logs.history[now_date]['buy_value']=buy_value+o.value
    else:
        # 如果不存在 buy_value 键，则执行其他操作
        context.logs.history[now_date]={}
        context.logs.history[now_date]['buy_value']=o.value 
        
    log(f"买入{o.code}共计{o.amount}股，单价为{round(pos.last_sale_price,2)}，价值为{round(o.value,2)}")   
    log(f"-------------------{is_new}-------------",'trace')
    log(f"当前现金："+str(context.portfolio.cash),'trace')
    log(f"当前持仓："+str(context.portfolio.positions_value),'trace')
    log(f"当前市值："+str(context.portfolio.total_value),'trace')        

def order_sell(security,amount):
    o=Order(code=security,amount=amount,is_buy=False,context=context)
    rules=Rules(order=o,context=context,log=log)
    o=rules.apply()
    if o.status!=1:
        return
    if o.amount==0:
        log(f"{o.code}--{o.amount}，卖单数为0，自动取消订单")  
        return
    
    if security in context.portfolio.positions:
        context.portfolio.cash=context.portfolio.cash+o.value-o.cost
        pos=context.portfolio.positions[security]
        if pos.amount-o.amount==0:
            del context.portfolio.positions[security]
        else:
            pos=context.portfolio.positions[security]
            pos.amount=pos.amount-o.amount
            pos.enable_amount=pos.enable_amount-o.amount
            pos.last_sale_price=o.last_sale_price
            pos.total_cost=pos.total_cost+o.cost-o.value
            pos.total_value=pos.amount*pos.last_sale_price
            pos.cost_basis=pos.total_cost/pos.amount

        context.portfolio.positions_value=context.portfolio.positions_value-o.value
        context.portfolio.total_value=context.portfolio.cash+context.portfolio.positions_value
    
    
            #卖价>均价
    if (o.value-o.cost)/o.amount>pos.cost_basis:
        context.performance.win=context.performance.win+1
    context.performance.trade_num=context.performance.trade_num+1
    
    now_date=context.current_dt.strftime('%Y%m%d')
    sell_value = context.logs.history.get(now_date, {}).get('sell_value', 0)
    if sell_value !=0:
        # 如果存在 buy_value 键，则执行对应的操作
        context.logs.history[now_date]['sell_value']=sell_value+o.value
    else:
        # 如果不存在 buy_value 键，则执行其他操作
        context.logs.history[now_date]={}
        context.logs.history[now_date]['sell_value']=o.value 
    
    trade_return=(o.value-o.cost)/o.amount/pos.cost_basis-1
    context.logs.trade_returns.append(trade_return)
    
    log(f"卖出{o.code}共计{o.amount}股，单价为{round(pos.last_sale_price,2)}，价值为{round(o.value,2)}")   
    log('---------------------------------','trace')
    log(f"当前现金："+str(context.portfolio.cash),'trace')
    log(f"当前持仓："+str(context.portfolio.positions_value),'trace')
    log(f"当前市值："+str(context.portfolio.total_value),'trace')        
    
    # print(security)
    # print(value)
    # tax,commission,cost=compute_cost(value,'open')
    
    # info=Data.get_daily_info(code=security,context=context)
    # price=Data.get_price(code=security,context=context)
    
    # order=preOrder(code,amount)
    # order=Rules.apply()
    
    
    # print(info)
    # print(price)
    
    # exit()    
    
    
    
def test():
    if 1:
        
        #按该票上次收盘价估算市值，兼容rqalpha
        if ts_code in instance['positions']:
            try:
                last_value=instance['positions'][ts_code]['last_close']*amount
            except Exception as e:
                print(ts_code)
                print(now_date)
                print(instance['positions'][ts_code])
        else:
            last_value=price*amount
        
        #再检测一下，排除买了1手手续费不够的情况
        if amount>0:
            value=amount*price
            fees=5
            if value*instance['setting']['fees']>5:
                fees=value*instance['setting']['fees']
                
            instance['cash']=instance['cash']-value-fees
            
            #理论上total_value是不应该变化的，但是rqalpha似乎是这样做的
            instance['total_value']=instance['total_value']-fees-value+last_value
            
            if ts_code not in instance['positions']:
                instance['positions'][ts_code]={
                    "amount":amount,
                    "avg_price":(value+fees)/amount,
                    "total_value":value+fees,
                    "last_close":price
                }
            else:
                instance['positions'][ts_code]['total_value']=instance['positions'][ts_code]['total_value']
                instance['positions'][ts_code]['amount']=instance['positions'][ts_code]['amount']+amount
                instance['positions'][ts_code]['avg_price']=(instance['positions'][ts_code]['total_value']+price+fees)/instance['positions'][ts_code]['amount']
                instance['positions'][ts_code]['last_close']=price
            instance['position_value']=instance['position_value']+value
        else:
            #bt.log(instance=instance,ts_code=ts_code,msg="钱不够，无法买入！",type='warn')
            return False     

        bt.log(instance=instance,ts_code=ts_code,msg="买入"+str(amount)+"股，当前价格"+str(round(price,2)),type='trade')
        buy_value = instance['history'].get(now_date, {}).get('buy_value', 0)
        if buy_value != 0:
            # 如果存在 buy_value 键，则执行对应的操作
            instance['history'][now_date]['buy_value']=buy_value+amount*price
        else:
            # 如果不存在 buy_value 键，则执行其他操作
            instance['history'][now_date]={}
            instance['history'][now_date]['buy_value']=amount*price

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
    


def load_preds(model_id,type='lgb'):
    pred_data_path=f"{type}_model_{model_id}_pred.pkl"
    if os.path.isfile(PREDS_DIR+pred_data_path):
        pred_data=pd.read_pickle(PREDS_DIR+pred_data_path)
    else:
        print(PREDS_DIR+data_path+' not found!')
        return False
    return pred_data
 
 
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
    strategy.log=log
    strategy.load_preds=load_preds



