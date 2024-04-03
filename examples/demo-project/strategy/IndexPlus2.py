import datetime
import os
import random
import math
from finhack.market.astock.astock import AStock
from finhack.market.astock.tushare.indexHelper import indexHelper
# 初始化函数，设定要操作的股票、基准等等
def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('000852.SH')
    # 开启动态复权模式，使用真实价格交易
    set_option('use_real_price', True) 
    # 设定成交量比例
    set_option('order_volume_ratio', 1)
    # 设定交易成本
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, close_today_commission=0, min_commission=5), type='stock')
    # 设定滑点
    set_slippage(PriceRelatedSlippage(0.00246), type='stock')
    
    # 自定义全局变量
    g.power = float(context.get('params', {}).get('power', 10))
    g.change_threshold = float(context.get('params', {}).get('change_threshold', 0.1))
    g.top = float(context.get('params', {}).get('top', 200))
    
    # 初始化资金
    inout_cash(100000)
    
    # 加载预测数据
    model_id = context.trade.model_id
    g.preds = load_preds_data(model_id)
    
    # 每日运行交易函数
    run_daily(trade, time="09:30")

# 交易函数
def trade(context):
    now_date = context.current_dt.strftime('%Y%m%d')
    # 获取指数权重数据
    idx_weight = indexHelper.get_index_weights('000852.SH', now_date)
    idx_weight = idx_weight.set_index('ts_code', drop=True)

    total_value = context.portfolio.total_value
    pred = g.preds[g.preds['trade_date'] == now_date]
    pred = pred.sort_values(by='pred', ascending=False, inplace=False) 
    pred = pred[~pred.index.duplicated()]
    
    # 选取预测涨幅前200的股票
    sr = pred.head(int(g.top))
    
    # 根据预测调整权重
    for index, row in sr.iterrows():
        symbol = index
        if symbol in idx_weight.index.values:
            idx_weight.loc[symbol, 'weight'] *= row['pred'] ** g.power

    # 执行交易
    for idx, row in idx_weight.iterrows():
        code = idx
        weight = float(row['weight'])
        target_pos_value = total_value * weight
        
        # 如果目标持仓金额小于100元，则忽略不交易
        if target_pos_value < 100:
            continue
        now_pos_value = 0
        if code in context.portfolio.positions.keys():
            now_pos_value = context.portfolio.positions[code].total_value
        
        # 计算需要调整的现金量
        delta_cash = target_pos_value - now_pos_value
        
        # 如果需要调整的现金量大于变动阈值，则执行交易
        if delta_cash<0 and abs(delta_cash) / (now_pos_value + 1) > g.change_threshold:
            order_value(code, delta_cash)


    # 执行交易，买
    for idx, row in idx_weight.iterrows():
        if context.portfolio.cash<1000:
            break
        code = idx
        weight = float(row['weight'])
        target_pos_value = total_value * weight
        
        # 如果目标持仓金额小于100元，则忽略不交易
        if target_pos_value < 100:
            continue
        now_pos_value = 0
        if code in context.portfolio.positions.keys():
            now_pos_value = context.portfolio.positions[code].total_value
        
        # 计算需要调整的现金量
        delta_cash = target_pos_value - now_pos_value
        
        # 如果需要调整的现金量大于变动阈值，则执行交易
        if delta_cash>0 and abs(delta_cash) / (now_pos_value + 1) > g.change_threshold:
            order_value(code, delta_cash)