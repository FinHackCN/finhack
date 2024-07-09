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
from finhack.factor.default.factorManager import factorManager
from finhack.market.astock.astock import AStock

# 初始化函数，设定股票、基准等
def initialize(context):
    set_benchmark('000001.SH')
    set_option('use_real_price', True)
    set_option('order_volume_ratio', 1)
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, close_today_commission=0, min_commission=5), type='stock')
    set_slippage(PriceRelatedSlippage(0.00246), type='stock')

    g.stocknum = 10
    g.days = 0
    g.refresh_rate = 10
    run_daily(trade, time="09:30")
    log.info('Initial setup completed')

    g.factors = factorManager.getFactors(['pe_0', 'MACD_0', 'peTtm_0', 'pb_0', 'totalMv_0', 'MA_90_0','close_0'])
    g.factors=g.factors.reset_index()
    
def trade(context):
    if g.days % g.refresh_rate == 0:
        sell_all_stocks(context)
        cash_per_stock = context.portfolio.cash / g.stocknum

        now_date = context.current_dt.strftime('%Y%m%d')
        df = g.factors.query("trade_date == '{}'".format(now_date))

        # 多因子筛选逻辑
        df = filter_stocks(df)
        stock_list = df.head(g.stocknum)['ts_code'].tolist()

        # 买入股票
        buy_stocks(context, stock_list, cash_per_stock)

        g.days = 1
    else:
        g.days += 1

def sell_all_stocks(context):
    sell_list = list(context.portfolio.positions.keys()) 
    if len(sell_list) > 0 :
        for stock in sell_list:
            order_target_value(stock, 0)  

def buy_stocks(context, stock_list, cash_per_stock):
    for stock in stock_list:
        if len(context.portfolio.positions.keys()) < g.stocknum:
            order_value(stock, cash_per_stock)

def filter_stocks(df):
    # 基于市盈率、市净率、MACD、市值和90日均线的简单多因子模型
    df_filtered = df[df['pe_0'] > 0]
    df_filtered = df_filtered[df_filtered['pb_0'] < 2]
    df_filtered = df_filtered[df_filtered['MACD_0'] < 0]
    df_filtered = df_filtered[df_filtered['peTtm_0'] < df_filtered['pe_0']]
    df_filtered = df_filtered[df_filtered['totalMv_0'] > 1e5]  # 筛选市值较大的股票
    df_filtered = df_filtered[df_filtered['close_0'] > df_filtered['MA_90_0']]  # 当前价格高于90日均线
    df_filtered = df_filtered.sort_values(by='totalMv_0', ascending=True)
    return df_filtered