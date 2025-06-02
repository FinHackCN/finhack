# 回测框架使用说明

## 概述

这是一个基于事件驱动的量化交易回测框架，包含四大核心组件：

1. **EventCenter（事件中心）**: 管理所有事件的生成、调度和分发
2. **TradeCenter（交易中心）**: 管理账户、持仓、订单和成交等交易相关对象
3. **DataCenter（数据中心）**: 负责数据的加载、缓存和计算
4. **Strategy（策略基类）**: 用户策略的基类，提供便捷的接口

## 框架特点

- **事件驱动架构**: 支持各种市场事件（开盘、收盘、Bar事件、分红送股等）
- **灵活的规则引擎**: 内置多种交易规则，支持自定义规则
- **完整的交易模拟**: 支持市价单、限价单、止损单等多种订单类型
- **真实的成本计算**: 考虑手续费、印花税、滑点等交易成本
- **丰富的统计指标**: 计算收益率、夏普率、最大回撤、胜率等指标
- **支持多品种交易**: 预留了期货、期权等品种的支持接口

## 快速开始

### 1. 编写策略

继承 `Strategy` 基类并实现 `on_init` 方法：

```python
from finhack.trader.backtest import Strategy

class MyStrategy(Strategy):
    def on_init(self):
        """策略初始化"""
        self.symbols = ['000001.SZ', '000002.SZ']
        self.ma_short = 5
        self.ma_long = 20
        
        # 注册定时器
        self.register_timer("09:35")
        
    def on_bar(self, time, data):
        """处理K线事件"""
        for symbol in self.symbols:
            # 获取K线数据
            kline = self.get_kline(symbol, frequency="1d", count=30)
            
            # 计算指标
            ma_short = kline['close'].tail(self.ma_short).mean()
            ma_long = kline['close'].tail(self.ma_long).mean()
            
            # 生成交易信号
            if ma_short > ma_long:
                self.place_order(
                    code=symbol,
                    quantity=100,
                    side="buy",
                    order_type="market"
                )
```

### 2. 配置回测参数

创建配置文件 `config.json`：

```json
{
    "start_date": "20240101",
    "end_date": "20240331", 
    "frequency": "1d",
    "cache_enabled": true,
    "log_level": "INFO",
    "report_path": "./backtest_report.json",
    "strategies": [
        {
            "adapter_id": "strategy_01",
            "strategy_path": "./my_strategy.py",
            "strategy_class": "MyStrategy",
            "initial_cash": 1000000,
            "params": {
                "ma_short": 5,
                "ma_long": 20
            }
        }
    ]
}
```

### 3. 运行回测

```bash
python -m finhack.trader.backtest.backtest_trader --config config.json
```

或者在代码中：

```python
from finhack.trader.backtest import BacktestTrader

# 创建回测器
backtester = BacktestTrader(config)

# 添加策略
for strategy_config in config['strategies']:
    backtester.add_strategy(strategy_config)
    
# 运行回测
backtester.run()
```

## 策略接口

### 事件处理方法

- `on_init()`: 策略初始化
- `on_bar(time, data)`: K线事件
- `on_timer(time)`: 定时器事件
- `on_order(order)`: 订单更新事件
- `on_trade(trade)`: 成交事件
- `on_market_open(time, data)`: 开盘事件
- `on_market_close(time, data)`: 收盘事件

### 交易接口

- `place_order(code, quantity, side, order_type, price, stop_price)`: 下单
- `cancel_order(order_id)`: 撤单
- `get_account()`: 获取账户信息
- `get_positions()`: 获取所有持仓
- `get_position(code)`: 获取单个持仓
- `get_orders(active_only)`: 获取订单列表
- `get_trades()`: 获取成交记录
- `get_price(code, price_type)`: 获取价格

### 数据接口

- `get_kline(codes, frequency, end_time, count)`: 获取K线数据
- `get_factors(codes, factor_names, end_time, count)`: 获取因子数据
- `compute_factors(factor_list, data)`: 计算因子
- `ml_predict(model_id, features)`: 机器学习预测

## 规则引擎

框架内置了以下交易规则：

- **停牌检查**: 停牌股票不能交易
- **涨跌停限制**: 涨停不能买入，跌停不能卖出
- **最小下单量**: A股买入必须是100的整数倍
- **成交量限制**: 单笔订单不能超过平均成交量的25%
- **资金检查**: 买入时检查可用资金是否充足
- **持仓检查**: 卖出时检查可用持仓是否充足

### 自定义规则

```python
from finhack.trader.backtest import Rule, RuleEngine

class MyRule(Rule):
    def check(self, order, trade_center):
        # 实现规则检查逻辑
        return True
        
    def get_name(self):
        return "my_rule"

# 添加自定义规则
trade_center.rule_engine.add_rule(MyRule())
```

## 回测报告

回测完成后会生成详细的统计报告，包括：

- 总收益率、年化收益率
- 夏普率、最大回撤
- 胜率、盈亏比
- 总手续费、印花税、滑点
- 最终持仓明细
- 所有交易记录

## 注意事项

1. **T+1限制**: 当日买入的股票不能当日卖出
2. **手续费设置**: 默认买卖手续费率0.03%，最低5元，印花税0.1%（仅卖出）
3. **滑点设置**: 默认0.1%的价格滑点
4. **数据缓存**: 建议开启缓存以提高回测速度
5. **内存管理**: 对于长时间回测，注意数据窗口大小设置

## 扩展功能

框架预留了以下扩展接口：

- 支持期货、期权等品种（保证金、做空等）
- 支持更多订单类型（止损限价单等）
- 支持更多市场（美股、港股等）
- 支持实盘交易接口对接 