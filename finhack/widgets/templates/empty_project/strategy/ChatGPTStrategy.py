'''
我理解您对先前策略的担忧。让我们尝试一个不同的策略，这次我们将使用动量和反转因子结合市值和流动性指标，来寻找可能的投资机会。该策略将试图捕捉那些近期表现良好且具备合理市值和流动性的股票，同时也考虑到那些可能即将反转的股票。

以下是新策略的大致框架：

动量和反转因子：使用例如12个月的价格变动率作为动量因子，以及近期的价格下跌作为可能的反转信号。
市值和流动性：选取市值较大且交易量健康的股票，以确保流动性和稳定性。
技术指标：使用如MACD等技术指标来辅助判断市场趋势和交易时机。
以下是根据这些原则修改的策略代码：

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

    g.factors = factorManager.getFactors(['MACD_0', 'RSI_14_0', 'ATR_14_0', 'totalMv_0', 'volumeRatio_0', 'pe_0', 'pb_0'])
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
    # 应用筛选条件
    filtered_df = df.copy()
    filtered_df = filtered_df[filtered_df['MACD_0'] > 0]  # 正MACD值，表明上升趋势
    filtered_df = filtered_df[filtered_df['RSI_14_0'] < 70]  # RSI值低于70，避免超买状态
    filtered_df = filtered_df[filtered_df['totalMv_0'] > 1e5]  # 选择市值较大的股票
    filtered_df = filtered_df[filtered_df['volumeRatio_0'] > 1]  # 成交量比率大于1
    filtered_df = filtered_df[(filtered_df['pe_0'] > 0) & (filtered_df['pe_0'] < 50)]  # 合理的市盈率范围
    filtered_df = filtered_df[(filtered_df['pb_0'] > 0) & (filtered_df['pb_0'] < 2)]  # 合理的市净率范围

    # 根据MACD值排序
    filtered_df = filtered_df.sort_values(by='MACD_0', ascending=False)
    return filtered_df
