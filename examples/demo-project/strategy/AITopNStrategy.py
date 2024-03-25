'''
finhack trader run --strategy=AITopNStrategy --args='{"model_id":"45813be38c1e215dbed056ccc32e38da"}'
'''
import datetime
import os
import random
import json
from finhack.factor.default.factorManager import factorManager
from finhack.market.astock.astock import AStock
from finhack.trainer.trainer import Trainer
from finhack.trainer.lightgbm.lightgbm_trainer import LightgbmTrainer

## 初始化函数，设定要操作的股票、基准等等
def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('000001.SH')
    # True为开启动态复权模式，使用真实价格交易
    set_option('use_real_price', True) 
    # 设定成交量比例
    set_option('order_volume_ratio', 1)
    # # 股票类交易手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, \
                             open_commission=0.0003, close_commission=0.0003,\
                             close_today_commission=0, min_commission=5), type='stock')
    
    # 为股票设定滑点为百分比滑点                       
    set_slippage(PriceRelatedSlippage(0.00246),type='stock')
    # 持仓数量
    g.stocknum = int(context.get('args', {}).get('stocknum', 10))
    # 交易日计时器
    g.days = 0 
    # 调仓频率
    g.refresh_rate = int(context.get('args', {}).get('refresh_rate', 10))

    model_id=context.trade.model_id
    preds=load_preds_data(model_id)
    g.preds=preds

    run_daily(trade, time="19:30")
    




## 交易函数
def trade(context):
    sync(context)
    if g.days%g.refresh_rate == 0:
        #print(context.portfolio.cash)
        sell_list = list(context.portfolio.positions.keys()) 
        if len(sell_list) > 0 :
            for stock in sell_list:
                order_target_value(stock, 0)  
    
        if len(context.portfolio.positions) < g.stocknum :
            Num = g.stocknum - len(context.portfolio.positions)
            Cash = context.portfolio.cash/Num
        else: 
            Cash = 0
    
        ## 选股
        now_date=context.current_dt.strftime('%Y%m%d')
        pred=g.preds[g.preds['trade_date']==now_date]
        pred=pred.sort_values(by='pred',ascending=False, inplace=False) 
        stock_list = pred.head(g.stocknum)['ts_code'].tolist()
        #print(stock_list)
        ## 买入股票
        for stock in stock_list:
            n=0
            # if len(context.portfolio.positions.keys()) < g.stocknum:
            if True:
                i=len(context.portfolio.positions.keys())+1
                N=g.stocknum
                a1=N
                ai=N-(i-1)*(N/i)
                Sn=N*N/2
                wi=ai/Sn
                o=order_value(stock, Cash*wi*10)
                #order_value(stock, Cash/(N-i+1))
                if o==True:
                    n=n+1
                if n==g.stocknum:
                    break 

        # 天计数加一
        g.days = 1
    else:
        g.days += 1
