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
    
    run_daily(trade, time="09:30")
    model_id=context.trade.model_id

    preds=load_preds_data(model_id)
    g.preds=preds


def trade(context):
    if g.days % g.refresh_rate == 0:
        # 获取当前持有的股票列表
        current_holdings = list(context.portfolio.positions.keys())
        # 预测数据中的今日日期
        now_date = context.current_dt.strftime('%Y%m%d')
        # 获取今日的预测数据
        today_preds = g.preds[g.preds['trade_date'] == now_date]

        # 卖出策略：卖出预测净值下降的股票
        for stock in current_holdings:
            filtered_preds = today_preds[today_preds['ts_code'] == stock]['pred']
            if not filtered_preds.empty and filtered_preds.iloc[0] < 1:
            #if today_preds[today_preds['ts_code'] == stock]['pred'].iloc[0] < 1:
                order_sell(stock, context.portfolio.positions[stock].amount)

        # 买入策略：选择预测增长最高的股票
        # 首先，我们过滤出预测增长的股票
        potential_buys = today_preds[today_preds['pred'] > 1]
        # 按预测值排序，选择增长预测最高的股票
        potential_buys = potential_buys.sort_values(by='pred', ascending=False)
        # 确定买入的股票数量
        num_stocks_to_buy = min(g.stocknum - len(current_holdings), len(potential_buys))
        
        # 如果有股票需要买入
        n=0
        if num_stocks_to_buy > 0:
            # 计算每只股票的买入资金
            sync(context)
            cash_per_stock = context.portfolio.cash / num_stocks_to_buy
            # 买入股票
            for i, row in potential_buys.iterrows():
                stock_to_buy = row['ts_code']
                # 如果股票不在当前持仓中，则买入
                if stock_to_buy not in current_holdings:
                    o=order_value(stock_to_buy, cash_per_stock)
                    if o==True:
                        n=n+1
                    if n==num_stocks_to_buy:
                        break

        # 更新交易日计数器
        g.days = 1
    else:
        # 如果不是调仓日，交易日计数器累加
        g.days += 1
 
