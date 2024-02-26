'''
理解您的要求，我将为您提供一个更复杂和专业的量化交易策略，这将基于您提供的股票因子，并适应您的量化回测框架。这个策略将侧重于多因子选择股票，并结合动量和价值因子，同时考虑股票的市值大小和技术指标，以期实现更好的交易表现。

这个策略的基本思路是：
使用多个因子（如市盈率、市净率、市值、MACD等）综合评估股票。
选择表现最佳的股票进行买入，并定期调整持仓。
利用技术指标作为辅助决策工具，如使用均线判断市场趋势。
以下是完整的示例策略代码：

'''
import datetime
import os
import random
import json
from finhack.factor.default.factorManager import factorManager
from finhack.market.astock.astock import AStock
from finhack.trainer.trainer import Trainer
from finhack.trainer.lightgbm.lightgbm_trainer import LightgbmTrainer

# 初始化函数，设定股票、基准等
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
    g.stocknum = 20
    # 交易日计时器
    g.days = 0 
    # 调仓频率
    g.refresh_rate = 10
    
    run_daily(trade, time="09:30")
    model_id=context.trade.model_id

    preds_data=load_preds_data(model_id)
    clsLgbTrainer=LightgbmTrainer()
    preds=clsLgbTrainer.pred(preds_data,md5=model_id,save=False)
    g.preds=preds

    g.stock_num = 20
    g.max_holding_days = 10
    g.pred_threshold = 1
    g.stop_loss_threshold = 0.95
    g.technical_indicators = ['MACD_0', 'RSI_14_0']
    g.financial_indicators = ['pe_0', 'pb_0']
    g.holding_days = {}
    g.holding_info = {}    
    
    g.factors = factorManager.getFactors(g.technical_indicators+g.financial_indicators)
    g.factors=g.factors.reset_index()    
    
def trade(context):
    # 更新持仓天数
    for stock in list(g.holding_days.keys()):
        g.holding_days[stock] += 1

    # 获取今日日期
    now_date = context.current_dt.strftime('%Y%m%d')

    # 获取今日的预测数据
    today_preds = g.preds[g.preds['trade_date'] == now_date]

    # 获取今日的技术指标和财务指标
    today_factors = g.factors[g.factors['trade_date'] == now_date]

    # 卖出逻辑
    for stock in list(context.portfolio.positions.keys()):
        # 止损逻辑
        price=get_price(code=stock,context=context)
        if price==None:
            print(stock,now_date)
            continue
        if price / g.holding_info[stock]['buy_price'] < g.stop_loss_threshold:
            order_sell(stock, context.portfolio.positions[stock].amount)
            if not stock in list(context.portfolio.positions.keys()):
                g.holding_days.pop(stock, None)
                g.holding_info.pop(stock, None)
            continue

        # 持仓时间过长或预测结果不好的股票卖出
        if g.holding_days.get(stock, 0) > g.max_holding_days or today_preds[today_preds['ts_code'] == stock]['pred'].iloc[0] < g.pred_threshold:
            order_sell(stock, context.portfolio.positions[stock].amount)
            if not stock in list(context.portfolio.positions.keys()):
                g.holding_days.pop(stock, None)
                g.holding_info.pop(stock, None)

    # 买入逻辑
    # 过滤出符合条件的股票
    candidates = today_preds[today_preds['pred'] > g.pred_threshold]
    candidates = candidates.merge(today_factors, on='ts_code')

    # 结合技术指标和财务指标进行筛选
    candidates = candidates[(candidates['MACD_0'] > 0) & (candidates['RSI_14_0'] < 70) & (candidates['pe_0'] > 0) & (candidates['pb_0'] < 2)]

    # 根据预测值排序
    candidates = candidates.sort_values(by='pred', ascending=False)

    # 计算每只股票的买入资金
    cash_per_stock = context.portfolio.cash / (min(g.stock_num, len(candidates))+0.1)

    # 买入股票
    for _, row in candidates.iterrows():
        stock_to_buy = row['ts_code']
        # 如果股票不在当前持仓中，则买入
        if stock_to_buy not in context.portfolio.positions:
            order_value(stock_to_buy, cash_per_stock)
            if stock_to_buy in context.portfolio.positions:#买入成功
                g.holding_days[stock_to_buy] = 0
                g.holding_info[stock_to_buy] = {'buy_price': context.portfolio.positions[stock_to_buy].last_sale_price}
            if len(context.portfolio.positions) >= g.stock_num:
                break  # 达到持仓上限，停止买入
