'''
筛选出市值介于20-30亿的股票，选取其中市值最小的三只股票，
每天开盘买入，持有五个交易日，然后调仓。
'''
import datetime
import os
import random

from finhack.market.astock.astock import AStock
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
    g.stocknum = 10
    # 交易日计时器
    g.days = 0 
    # 调仓频率
    g.refresh_rate = 3
    # 运行函数
    inout_cash(100000)

    run_daily(trade, time="09:30")
    # run_daily(trade, time="8:05")
    log.info('get code list')
    g.stock_list=AStock.getStockCodeList(strict=False)

## 交易函数
def trade(context):
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
        stock_list = random.sample(g.stock_list['ts_code'].tolist(), 10)
        ## 买入股票
        for stock in stock_list:
            if len(context.portfolio.positions.keys()) < g.stocknum:
                order_value(stock, Cash)

        # 天计数加一
        g.days = 1
    else:
        g.days += 1
