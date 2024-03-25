import numpy as np
import datetime
import os
import random
import json
from finhack.factor.default.factorManager import factorManager
from finhack.market.astock.astock import AStock
from finhack.trainer.trainer import Trainer
from finhack.trainer.lightgbm.lightgbm_trainer import LightgbmTrainer

# 初始化函数
def initialize(context):
    # 设定基准
    set_benchmark('000001.SH')
    # 开启动态复权模式
    set_option('use_real_price', True)
    # 设定成交量比例
    set_option('order_volume_ratio', 1)
    # 设定手续费和税
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
    # 设定滑点
    set_slippage(PriceRelatedSlippage(0.00246), type='stock')
    
    model_id = context.trade.model_id
    preds_data = load_preds_data(model_id)
    g.preds=preds_data

    # 全局变量初始化
    g.stock_num = int(context.get('args', {}).get('stocknum', 10))  # 持仓股票数量
    g.refresh_rate = int(context.get('args', {}).get('refresh_rate', 10))  # 调仓频率，动态调整
    g.max_drawdown_limit = 0.2  # 最大回撤限制
    g.max_portfolio_exposure = 0.95  # 最大投资组合暴露度
    g.stop_loss_threshold = 0.95  # 止损阈值
    g.stop_gain_threshold = 1.2  # 止盈阈值
    g.days = 0  # 交易日计时器
    
    # 每日运行
    run_daily(trade, time="09:30")

# 动态调整策略参数
def adjust_dynamic_parameters(context):
    # 示例：根据市场状况调整持仓数量，此处留空，由用户根据实际情况填写
    pass

# 选股策略
def select_stocks(context):
    # 加载AI模型预测数据

    now_date = context.current_dt.strftime('%Y%m%d')
    preds_data=g.preds
    # 筛选今日预测数据，并排序
    pred_today = preds_data[preds_data['trade_date'] == now_date]
    pred_today_sorted = pred_today.sort_values(by='pred', ascending=False)
    
    # 返回股票列表
    return pred_today_sorted['ts_code'].tolist()

# 是否卖出
def should_sell(stock, context):
    # 止损和止盈逻辑
    current_price = get_price(stock, context)
    if current_price==None:
        return False
    cost_price = context.portfolio.positions[stock].cost_basis
    if current_price <= cost_price * g.stop_loss_threshold or current_price >= cost_price * g.stop_gain_threshold:
        return True
    return False

# 是否买入
def should_buy(stock, context):
    # 此处可加入财务指标、技术指标等筛选条件，示例留空
    return True

# 交易逻辑
def trade(context):
    adjust_dynamic_parameters(context)
    
    # 卖出逻辑
    for stock in list(context.portfolio.positions.keys()):
        if should_sell(stock, context):
            order_target_value(stock, 0)
    
    # 买入逻辑
    if g.days % g.refresh_rate == 0:
        stock_list = select_stocks(context)
        num_stocks_to_buy = min(len(stock_list), g.stock_num - len(context.portfolio.positions))
        if num_stocks_to_buy==0:
            g.days += 1
            return 
        cash_per_stock = context.portfolio.cash / num_stocks_to_buy
        for stock in stock_list[:num_stocks_to_buy]:
            if should_buy(stock, context):
                order_value(stock, cash_per_stock)
                
    g.days += 1
