import pandas as pd
import plotext as plt
from finhack.market.astock.astock import AStock
import importlib
import os
import quantstats as qs
import time
import math
import datetime
import empyrical as ey
import hashlib
import numpy as np
from tabulate import tabulate
class Performance:
    def analyse(context):
        context.performance.returns=pd.DataFrame(context.performance.returns,columns=['trade_date', 'returns'])
        context.performance.returns['values']=context.performance.returns['returns'].cumprod()
        context.performance.returns["trade_date"] = pd.to_datetime(context.performance.returns["trade_date"], format='%Y%m%d')
        context.performance.returns=context.performance.returns.set_index('trade_date')
        
        benchmark=context.trade.benchmark
        start_date=context.trade.start_time[0:10].replace('-','')
        end_date=context.trade.end_time[0:10].replace('-','')
        
        index=AStock.getIndexPrice(ts_code=benchmark,start_date=start_date,end_date=end_date)
        index['returns']=index['close']/index['close'].shift(1)
        index['values']=index['returns'].cumprod()
        index["trade_date"] = pd.to_datetime(index["trade_date"], format='%Y%m%d')
        index=index.set_index('trade_date')         
        

        returns=context.performance.returns['returns']-1
        benchReturns=index['returns']-1
        
        try:
            alpha, beta = ey.alpha_beta(returns = returns, factor_returns = benchReturns, annualization=252) 
        except Exception as e:
            print(str(e))
        indicators = {}
        indicators["alpha"] = alpha
        indicators["beta"] = beta
        indicators['aggregate_returns']=ey.aggregate_returns(returns,convert_to='yearly')
        indicators['annual_return']=ey.annual_return(returns=returns, period='daily', annualization=252)
        indicators['cagr']=ey.cagr(returns)
        indicators['annual_volatility']=ey.annual_volatility(returns)
        indicators['info_ratio'] = ey.excess_sharpe(returns =  returns, factor_returns = benchReturns)
        #indicators['cum_returns(累积收益)']=ey.cum_returns(returns)
        indicators['downside_risk']=ey.downside_risk(returns)
        indicators['R2'] =ey.stability_of_timeseries(returns)
        indicators['sharpe'] = ey.sharpe_ratio(returns = returns, risk_free=0, period='daily', annualization=None)
        indicators['sortino'] = ey.sortino_ratio(returns = returns,required_return=0, period='daily', annualization=None, _downside_risk=None)
    
        
        indicators['calmar'] = ey.calmar_ratio(returns = returns,period='daily', annualization=None)
        indicators['omega'] = ey.omega_ratio(returns = returns, risk_free=0.0, required_return=0.0, annualization=252)
        indicators['max_down']=ey.max_drawdown(returns)
        
        #可能有问题，老版本移植过来的逻辑，忘了咋回事了
        indicators['sqn']=math.sqrt(context.performance.trade_num) * np.mean(context.logs.trade_returns) / np.std(context.logs.trade_returns)
        #indicators['vola']=indicators['annual_volatility']
        indicators['rnorm']=indicators['annual_return']
        indicators['trade_num']=context.performance.trade_num
        indicators['roto']=context.portfolio.total_value/context.portfolio.starting_cash-1
        if(context.performance.trade_num>0):
            indicators['win_ratio']=context.performance.win/context.performance.trade_num
        else:
            indicators['win_ratio']=0
        context.performance['values']=returns
        context.performance['bench_values']=benchReturns
        for key in indicators.keys():
            if key not in ['returns','bench_returns']:
                try:
                    if math.isnan(indicators[key]) or math.isinf(indicators[key]) :
                        indicators[key]=0
                except Exception as e:
                    indicators[key]=0
        
        context.performance.indicators=indicators        
        
        
        Performance.show_console(context,index)
        #Performance.notebook(context,index)
        print('')
        performance_data=context.performance.indicators
        # 如果某些值是单元素 Series，使用 .iloc[0] 转换它们
        for key in performance_data:
            if isinstance(performance_data[key], pd.Series) and len(performance_data[key]) == 1:
                performance_data[key] = float(performance_data[key].iloc[0])
        
        # 构造表格数据
        table_data = [(key, value) for key, value in performance_data.items()]
        
        # 输出表格
        print(tabulate(table_data, headers=["Metric", "Value"], tablefmt="grid"))
        
    def show_console(context,index):
        p_df=context.performance.returns
        i_df=index
        
        p_dates = p_df.index.strftime('%d/%m/%Y').tolist()
        p_values = p_df['values'].tolist()
        
        
        i_dates = i_df.index.strftime('%d/%m/%Y').tolist()
        i_values = i_df['values'].tolist()
        
        # 绘图
        # 设置图表样式
        plt.plotsize(100, 30)
        plt.canvas_color("default")  # 设置透明背景
        plt.ticks_color("default")  # 设置刻度颜色以匹配终端默认颜色
        plt.axes_color("default") 

        plt.plot(p_dates, p_values, marker='dot',label = context['trade']['strategy'])
        plt.plot(i_dates, i_values, marker='dot',label = context.trade.benchmark)
        plt.title("Daily Returns")
        plt.xlabel("Date")
        plt.ylabel("Return")
        plt.grid(True)
        plt.show()       
        
    def show_notebook(p_values,i_values):
        qs.extend_pandas()
        qs.reports.full(p_values,i_values)
