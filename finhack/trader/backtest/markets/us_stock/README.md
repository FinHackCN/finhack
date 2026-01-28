# 美股市场适配器 (US Stock Market Adapter)

## 概述

美股市场适配器实现了美国股票市场的完整交易规则，支持盘前盘后交易、熔断机制、PDT规则、零碎股等特性。

## 目录结构

```
us_stock/
├── __init__.py                      # 模块导出
├── us_stock_adapter.py              # 主适配器类
├── us_trading_rules_versions.py     # 规则版本定义
├── us_calculator.py                 # 计算器模块
├── test_us_stock_adapter.py         # 完整测试
└── test_simple.py                   # 简单测试
```

## 主要特性

### 1. 交易时段

- **盘前交易**: 04:00 - 09:30 (美东时间)
- **正常交易**: 09:30 - 16:00 (美东时间)
- **盘后交易**: 16:00 - 20:00 (美东时间)

### 2. 熔断机制

#### 市场级熔断
- **Level 1**: S&P 500 下跌 7%，暂停 15 分钟
- **Level 2**: S&P 500 下跌 13%，暂停 15 分钟
- **Level 3**: S&P 500 下跌 20%，当日剩余时间暂停
- **特殊规则**: 下午 3:25 后触发 Level 1 不暂停

#### 个股级熔断
- **2020年前**: 涨跌 10% 暂停 5 分钟
- **2020年4月起**: 涨跌 5% 暂停 5 分钟

### 3. 结算周期

- **2024年5月前**: T+2 结算
- **2024年5月28日起**: T+1 结算

### 4. PDT 规则（Pattern Day Trader）

- **适用条件**: 账户净值低于 $25,000
- **限制**: 5 个交易日内最多 3 次日内回转交易
- **现金账户**: 不受 PDT 规则限制

### 5. 零碎股交易

- **最小买入**: 1 股
- **支持零碎股**: 可交易小数股（如 0.5 股）
- **零碎股精度**: 0.0001 股

### 6. 手续费结构

- **佣金**: 0（零佣金时代）
- **SEC 费用**: 约 $0.0131/股（仅卖出）
- **交易活动费**: 约 $0.119/股（仅卖出）
- **印花税**: 无

### 7. 卖空规则（Rule 201）

- **提价规则**: 当股票日跌幅超过 10% 时触发
- **限制期**: 触发后 5 个交易日
- **限制内容**: 卖空价格需高于当前最优买价

## 使用示例

### 基础使用

```python
from datetime import date
from finhack.trader.backtest.markets.us_stock import USStockMarketAdapter

# 创建适配器
adapter = USStockMarketAdapter()

# 获取市场信息
market_info = adapter.get_market_info('AAPL')
print(market_info)

# 生成交易日事件
trade_date = date(2024, 6, 1)
events = adapter.generate_daily_events(trade_date, '1d', 'AAPL')

# 判断交易时间
from datetime import datetime
dt = datetime(2024, 6, 1, 10, 0)
is_trading = adapter.is_trading_time(dt, '1d', 'AAPL')
```

### 熔断检查

```python
from finhack.trader.backtest.markets.us_stock import USCircuitBreakerCalculator
from datetime import time

# 市场级熔断检查
result = USCircuitBreakerCalculator.check_circuit_breaker(
    index_level=4500,          # S&P 500 当前点位
    previous_close=5000,       # 前收盘价
    trigger_time=time(10, 0),  # 触发时间
    query_date=date(2024, 6, 1)
)
print(result)  # {'triggered': True, 'level': 1, ...}

# 个股熔断检查
result = USCircuitBreakerCalculator.check_individual_pause(
    symbol='AAPL',
    current_price=190.0,
    reference_price=175.0,
    query_date=date(2024, 6, 1)
)
print(result)  # {'triggered': True, 'move_pct': 0.0857, ...}
```

### 零碎股交易

```python
from finhack.trader.backtest.markets.us_stock import USLotSizeCalculator

# 获取零碎股信息
lot_info = USLotSizeCalculator.get_lot_size_info('AAPL', date.today())
print(lot_info)  # {'min_buy': 1, 'fractional_shares': True, ...}

# 标准化订单数量
normalized, msg = USLotSizeCalculator.normalize_order_volume(
    'AAPL', 0.5, 'buy', date.today()
)
print(normalized)  # 0.5
```

### 手续费计算

```python
from finhack.trader.backtest.markets.us_stock import USCommissionCalculator

# 计算手续费
fee = USCommissionCalculator.calculate_commission(
    symbol='AAPL',
    volume=100,
    price=175.0,
    side='sell',  # 买入无额外费用，卖出有SEC和交易活动费
    query_date=date.today()
)
print(fee)
# {'commission': 0.0, 'sec_fee': 0.2293, 'trading_activity_fee': 2.0825, ...}
```

### PDT 规则检查

```python
# 检查PDT限制
result = adapter.check_pdt_restriction(
    account_equity=24000,    # 账户净值
    day_trades_count=2       # 已日内交易次数
)
print(result)
# {'restricted': True, 'remaining_trades': 1, ...}
```

### 卖空规则检查

```python
# 检查卖空限制
result = adapter.check_short_sale_restriction(
    symbol='AAPL',
    daily_change_pct=-0.12,  # 当日下跌12%
    bid_price=165.0
)
print(result)
# {'restricted': True, 'rule': 'rule_201', 'min_price': 165.0}
```

## 规则版本管理

所有规则都支持基于时间的版本控制，可以查询历史规则：

```python
from finhack.trader.backtest.markets.us_stock import RuleVersion, SETTLEMENT_VERSIONS

# 查询特定日期的结算规则
rule_2023 = RuleVersion.get_applicable_rule(
    SETTLEMENT_VERSIONS,
    date(2023, 6, 1)
)
print(rule_2023)  # {'settlement_cycle': 'T+2'}

rule_2024 = RuleVersion.get_applicable_rule(
    SETTLEMENT_VERSIONS,
    date(2024, 6, 1)
)
print(rule_2024)  # {'settlement_cycle': 'T+1'}
```

## 支持的数据频率

- `1d`: 日频
- `1m`: 1分钟
- `5m`: 5分钟
- `15m`: 15分钟
- `30m`: 30分钟
- `60m`: 60分钟

## 与 cn_fund 的对比

| 特性 | cn_fund (中国ETF) | us_stock (美股) |
|------|------------------|----------------|
| 交易时段 | 9:30-11:30, 13:00-15:00 | 4:00-20:00 (盘前+正常+盘后) |
| 涨跌停 | 有 (10%/20%/30%) | 无 (但有熔断机制) |
| 最小单位 | 100股起 (主板) | 1股 (支持零碎股) |
| 结算周期 | T+1 | T+1/T+2 |
| 手续费 | 有佣金 | 零佣金 (有SEC费用) |
| 印花税 | 有 (卖出) | 无 |
| 卖空 | 限制较多 | Rule 201限制 |
| 日内交易 | T+1限制 | PDT规则限制 |

## 测试

运行测试验证功能：

```bash
cd /mnt/ssd2/finhack-dev/finhack/finhack/trader/backtest/markets/us_stock
python test_simple.py
```

## 注意事项

1. **时区**: 所有时间均为美东时间 (America/New_York)
2. **交易日历**: 需要实现 USStockCalendar 类来处理美国节假日
3. **股票分类**: 目前交易所判断为简化版本，实际应用中需要维护完整代码列表
4. **熔断机制**: 目前基于 S&P 500 指数，个股熔断基于前收盘价

## 未来扩展

- [ ] 实现完整的美股交易日历 (USStockCalendar)
- [ ] 支持期权交易规则
- [ ] 支持更多股票类型 (ADR, REIT 等)
- [ ] 实现更精确的交易所代码映射
- [ ] 支持美股指数期货和ETF
- [ ] 实现股息除权处理
