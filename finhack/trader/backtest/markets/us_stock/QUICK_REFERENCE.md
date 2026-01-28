# 美股市场适配器快速参考

## 文件对照表

| cn_fund (中国ETF) | us_stock (美股) | 说明 |
|------------------|----------------|------|
| `cn_fund_adapter.py` | `us_stock_adapter.py` | 主适配器类 |
| `etf_trading_rules_versions.py` | `us_trading_rules_versions.py` | 规则版本定义 |
| `etf_calculator.py` | `us_calculator.py` | 计算器模块 |
| `get_etf_board_type()` | `get_exchange_from_symbol()` | 获取交易所/板块 |
| `get_etf_type()` | `get_stock_type()` | 获取类型 |
| `ETFPriceCalculator` | `USCircuitBreakerCalculator` | 价格/熔断计算 |
| `ETFLotSizeCalculator` | `USLotSizeCalculator` | 交易单位计算 |
| `ETFCommissionCalculator` | `USCommissionCalculator` | 手续费计算 |

## 主要区别

### 1. 交易时段

**中国ETF (cn_fund)**:
- 集合竞价: 09:15-09:25
- 上午: 09:30-11:30
- 下午: 13:00-15:00
- 盘后: 15:05-15:30 (科创板/创业板)

**美股 (us_stock)**:
- 盘前: 04:00-09:30
- 正常: 09:30-16:00
- 盘后: 16:00-20:00

### 2. 价格限制

**中国ETF (cn_fund)**:
- 主板: ±10%
- 科创板/创业板: ±20%
- 北交所: ±30%

**美股 (us_stock)**:
- 无涨跌停限制
- 市场级熔断: S&P 500下跌7%/13%/20%
- 个股级熔断: 涨跌5%暂停5分钟

### 3. 交易单位

**中国ETF (cn_fund)**:
```python
# 主板: 100股起，100股整数倍
# 科创板: 200股起，1股递增
# 创业板/北交所: 100股起，1股递增
```

**美股 (us_stock)**:
```python
# 1股起买，支持零碎股
# 最小单位: 0.0001股
```

### 4. 结算周期

**中国ETF (cn_fund)**: T+1

**美股 (us_stock)**:
- 2024年5月前: T+2
- 2024年5月28日起: T+1

### 5. 手续费

**中国ETF (cn_fund)**:
- 佣金: 万三 (0.03%)
- 印花税: 卖出0.1%
- 最低佣金: 5元

**美股 (us_stock)**:
- 佣金: 0 (零佣金)
- SEC费用: 卖出约$0.0131/股
- 交易活动费: 卖出约$0.119/股
- 印花税: 无

### 6. 特殊规则

**中国ETF (cn_fund)**:
- T+1制度（当日买入次日才能卖出）
- 价格笼子机制
- 涨跌停限制

**美股 (us_stock)**:
- PDT规则（小额账户限制日内交易）
- 卖空提价规则（Rule 201）
- 熔断机制
- 支持日内交易（T+0）

## 代码示例对比

### 创建适配器

**中国ETF**:
```python
from finhack.trader.backtest.markets.cn_fund import CnFundMarketAdapter

adapter = CnFundMarketAdapter()
```

**美股**:
```python
from finhack.trader.backtest.markets.us_stock import USStockMarketAdapter

adapter = USStockMarketAdapter()
```

### 获取价格限制

**中国ETF**:
```python
limits = adapter.get_price_limits('510300.SH', prev_close=4.5)
# 返回: {'upper_limit': 4.95, 'lower_limit': 4.05, ...}
```

**美股**:
```python
limits = adapter.get_price_limits('AAPL', prev_close=175.0)
# 返回: {'upper_limit': inf, 'lower_limit': 0.0, 'circuit_breaker_enabled': True}
```

### 检查熔断

**中国ETF**:
```python
# 中国ETF无市场级熔断，只有涨跌停
```

**美股**:
```python
result = adapter.check_circuit_breaker(
    index_level=4500,
    previous_close=5000,
    trigger_time=time(10, 0)
)
# 返回: {'triggered': True, 'level': 1, 'pause_duration': 15}
```

### 手续费计算

**中国ETF**:
```python
from finhack.trader.backtest.markets.cn_fund import ETFCommissionCalculator

fee = ETFCommissionCalculator.calculate_commission(
    '510300.SH', 1000, 4.5, 'buy'
)
# 返回: {'commission': 5.0, 'stamp_tax': 0.0, 'total_fee': 5.0}
# 佣金最低5元
```

**美股**:
```python
from finhack.trader.backtest.markets.us_stock import USCommissionCalculator

fee = USCommissionCalculator.calculate_commission(
    'AAPL', 100, 175.0, 'sell'
)
# 返回: {'commission': 0.0, 'sec_fee': 0.2293, 'trading_activity_fee': 2.0825, ...}
# 零佣金，但卖出有SEC费用
```

### PDT规则检查（美股独有）

**美股**:
```python
result = adapter.check_pdt_restriction(
    account_equity=24000,  # 账户净值低于$25,000
    day_trades_count=2
)
# 返回: {'restricted': True, 'remaining_trades': 1}
```

## 迁移指南

如果你已经熟悉 `cn_fund` 适配器，迁移到 `us_stock` 很简单：

1. **导入替换**:
   ```python
   # 从
   from finhack.trader.backtest.markets.cn_fund import CnFundMarketAdapter
   # 改为
   from finhack.trader.backtest.markets.us_stock import USStockMarketAdapter
   ```

2. **时区注意**:
   - 中国使用 `Asia/Shanghai`
   - 美股使用 `America/New_York`

3. **货币注意**:
   - 中国使用 `CNY`
   - 美股使用 `USD`

4. **交易时段**:
   - 美股有盘前盘后，事件生成会更复杂

5. **特殊规则**:
   - 美股需要考虑PDT规则和熔断机制
   - 中国ETF需要考虑涨跌停和价格笼子

## 常见问题

**Q: 美股没有涨跌停吗？**
A: 是的，美股没有涨跌停限制，但有熔断机制（市场级和个股级）。

**Q: 美股可以T+0交易吗？**
A: 是的，美股支持T+0（日内交易），但小额账户受PDT规则限制。

**Q: 美股支持零碎股吗？**
A: 是的，美股支持零碎股交易，最小精度可达0.0001股。

**Q: 美股手续费是多少？**
A: 现在美股主要券商都是零佣金，但卖出时需要支付SEC费用和交易活动费。

**Q: 美股的结算周期是多久？**
A: 2024年5月28日起从T+2改为T+1，与中国相同。

## 测试

运行完整测试：
```bash
cd /mnt/ssd2/finhack-dev/finhack/finhack/trader/backtest/markets/us_stock
python test_simple.py
```

查看详细文档：
```bash
cat README.md
```
