import datetime
import os
import random
import math
from finhack.market.astock.astock import AStock
from finhack.market.astock.tushare.indexHelper import indexHelper
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

    g.power = float(context.get('params', {}).get('stocknum', 10))  # 
    g.change_threshold = float(context.get('params', {}).get('refresh_rate', 0.3))  # 

    # 运行函数
    inout_cash(100000)

    model_id = context.trade.model_id
    preds_data = load_preds_data(model_id)
    g.preds=preds_data

    run_daily(trade, time="09:30")


## 交易函数
def trade(context):
    now_date = context.current_dt.strftime('%Y%m%d')
    idx_weight=indexHelper.get_index_weights('000852.SH',now_date)
    idx_weight=idx_weight.set_index('ts_code',drop=True)
    print(idx_weight)

    total_value=context.portfolio.total_value

    print(g.preds)
    exit()

    pred=g.preds[g.preds['trade_date']==now_date]
    pred=pred.sort_values(by='pred',ascending=False, inplace=False) 
    pred=pred[~pred.index.duplicated()] 
        
    sr=pred
    #低于0.95的全部
    sr.loc[sr.index[200:], 'pred'] = 0
        
    for index,row in sr.iterrows():
        symbol=index
        if symbol in idx_weight.index.values:
            idx_weight.loc[symbol,'weight']=float(idx_weight.loc[symbol]['weight'])*math.pow(row['pred'],g.power)
            
            

                        
    for idx,row in idx_weight.iterrows():
        code=idx
        weight=float(row['weight'])
        target_pos_cash=total_value*weight*0.01
            
        if target_pos_cash<100:
            continue
        now_pos_cash=0
        
        if code in context.portfolio.positions.keys():
            now_pos_cash=context.portfolio.positions[code].total_value
        x_cash=target_pos_cash-now_pos_cash
     
        if x_cash<0 and (-x_cash)/(now_pos_cash+1)>g.change_threshold:
            order_value(code,x_cash)
 
        
                
                
                
    for idx,row in idx_weight.iterrows():
        code=idx
        weight=float(row['weight'])
        target_pos_cash=total_value*weight*0.01
                
        if target_pos_cash<100:
            continue
        now_pos_cash=0
        
        if code in context.portfolio.positions.keys():
            now_pos_cash=context.portfolio.positions[code].total_value
        x_cash=target_pos_cash-now_pos_cash

            
        if x_cash>0 and  (x_cash)/(now_pos_cash+1)>g.change_threshold:
            order_value(code,x_cash)

