import tushare as ts
import numpy as np
import pandas as pd
import riskfolio as rp
import matplotlib.pyplot as plt

 

# 获取股票数据
stock_codes =['002624.SZ','000001.SZ','002555.SZ', '600028.SH', '601866.SH']  # 示例股票代码，您可以根据自己的需求修改
start_date = '2020-01-01'  # 示例开始日期，您可以根据自己的需求修改
end_date = '2020-12-31'  # 示例结束日期，您可以根据自己的需求修改

# 获取股票历史收益率数据
returns = []
for code in stock_codes:
    pro = ts.pro_api()
    df = ts.pro_bar(ts_code=code, start_date=start_date, end_date=end_date)
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.set_index('trade_date')
    df = df.sort_index()
    returns.append(df['close'].pct_change().dropna())

# 将收益率数据转换为DataFrame对象
returns_df = pd.concat(returns, axis=1)
returns_df.columns = stock_codes

# 创建Portfolio对象
port = rp.Portfolio(returns=returns_df)

# To display dataframes values in percentage format
pd.options.display.float_format = '{:.4%}'.format

# Choose the risk measure
rm = 'MSV'  # Semi Standard Deviation

# Estimate inputs of the model (historical estimates)
method_mu='hist' # Method to estimate expected returns based on historical data.
method_cov='hist' # Method to estimate covariance matrix based on historical data.

port.assets_stats(method_mu=method_mu, method_cov=method_cov, d=0.94)

# Estimate the portfolio that maximizes the risk adjusted return ratio
w1 = port.optimization(model='Classic', rm=rm, obj='Sharpe', rf=0.0, l=0, hist=True)

# Estimate points in the efficient frontier mean - semi standard deviation
ws = port.efficient_frontier(model='Classic', rm=rm, points=5, rf=0, hist=True)

# Estimate the risk parity portfolio for semi standard deviation
w2 = port.rp_optimization(model='Classic', rm=rm, rf=0, b=None, hist=True)


print(w1)
print(ws)
print(w2)