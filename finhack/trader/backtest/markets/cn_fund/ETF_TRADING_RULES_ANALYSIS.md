# A股ETF交易规则分析报告

## 执行摘要

已为cn_fund(ETF)适配器实现完整的A股ETF交易规则，包括基于时间的规则版本控制机制。所有核心功能已实现并通过测试验证。

---

## 一、已实现的功能

### 1.1 新增文件

| 文件 | 功能描述 |
|-----|---------|
| `etf_trading_rules_versions.py` | ETF交易规则版本定义 |
| `etf_calculator.py` | ETF交易规则计算器 |
| `cn_fund_adapter.py` (重构) | 更新后的ETF适配器 |
| `test_etf_adapter.py` | 单元测试脚本 |
| `__init__.py` | 模块导出 |

### 1.2 规则版本控制机制

实现了基于时间的规则版本查询系统：
- 所有规则按生效日期定义
- 支持根据查询日期自动获取适用规则
- 支持板块、交易所等维度的规则差异化

### 1.3 已实现的交易规则

| 规则类别 | 实现状态 | 说明 |
|---------|---------|------|
| 交易时间(连续竞价) | ✓ | 09:30-11:30, 13:00-15:00 |
| 集合竞价时段 | ✓ | 9:15-9:25 (1991年7月3日起) |
| 收盘集合竞价 | ✓ | 14:57-15:00 (深市2006/7/1, 沪市2018/8/20) |
| 盘后定价交易 | ✓ | 15:05-15:30 (科创板/创业板) |
| T+1制度 | ✓ | |
| 涨跌幅限制 | ✓ | 主板10%, 科创板/创业板20%, 北交所30% |
| 涨跌停价计算 | ✓ | 包含不同板块的精度规则 |
| 价格笼子机制 | ✓ | 主板2023/2/20, 科创板/创业板2020/8/24 |
| 最小交易单位 | ✓ | 主板100股, 科创板200股, 创业板100股(1股递增) |
| 手续费规则 | ✓ | 股票ETF万三+印花税, 债券/跨境ETF免印花税, 货币ETF免费 |

---

## 二、ETF板块识别

### 2.1 代码前缀与板块映射

| 代码前缀 | 板块 | 示例 |
|---------|------|------|
| 510-513, 515-518, 560 | 主板 | 510300.SH(沪深300ETF) |
| 588 | 科创板 | 588000.SH(科创50ETF) |
| 1590-1591 | 主板 | 159001.SZ |
| 1599 | 创业板 | 159949.SZ(创业板50ETF) |
| 150, 300, 360 | 创业板 | 1599xx.SZ |

### 2.2 ETF类型识别

| 类型 | 识别规则 |
|-----|---------|
| 股票ETF | 默认类型 |
| 债券ETF | 5110-5115开头 |
| 跨境ETF | 5130-5131开头 |
| 货币ETF | 5118-5119开头, 159001 |

---

## 三、规则变更时间表

### 3.1 交易时段变更

| 生效日期 | 变更内容 | 适用范围 |
|---------|---------|---------|
| 1991-07-03 | 引入集合竞价 | 全市场 |
| 1993-11-01 | 上午交易延长至11:30 | 全市场 |
| 2006-07-01 | 新增收盘集合竞价 | 深市 |
| 2018-08-20 | 新增收盘集合竞价 | 沪市 |
| 2019-07-22 | 盘后定价交易 | 科创板 |
| 2020-08-24 | 盘后定价交易 | 创业板 |

### 3.2 涨跌幅限制变更

| 生效日期 | 变更内容 | 适用范围 |
|---------|---------|---------|
| 1996-12-16 | 10%涨跌停 | 主板 |
| 2009-10-30 | 10%涨跌停 | 创业板(开板) |
| 2019-07-22 | 20%涨跌停, 前5日无限制 | 科创板 |
| 2020-08-24 | 20%涨跌停, 前5日无限制 | 创业板(注册制) |
| 2021-11-15 | 30%涨跌停 | 北交所 |
| 2023-02-20 | 前5日无限制 | 主板(注册制) |

### 3.3 最小交易单位变更

| 生效日期 | 变更内容 | 适用范围 |
|---------|---------|---------|
| 1990-12-19 | 100股整数倍 | 主板 |
| 2003-01-01 | 允许零股卖出 | 主板 |
| 2019-07-22 | 200股起买, 1股递增 | 科创板 |
| 2020-08-24 | 100股起买, 1股递增 | 创业板 |

### 3.4 价格笼子机制变更

| 生效日期 | 变更内容 | 适用范围 |
|---------|---------|---------|
| 2020-08-24 | ±2%或±10单位 | 科创板/创业板 |
| 2021-11-15 | ±5%或±50单位 | 北交所 |
| 2023-02-20 | ±2%或±0.1元 | 主板 |

---

## 四、使用示例

### 4.1 基本使用

```python
from datetime import date
from finhack.trader.backtest.markets.cn_fund import CnFundMarketAdapter

# 创建适配器（支持设置当前日期）
adapter = CnFundMarketAdapter(current_date=date(2024, 1, 15))

# 获取涨跌幅限制
limits = adapter.get_price_limits('510300.SH', prev_close=4.5)
# {'upper_limit': 4.95, 'lower_limit': 4.05, 'limit_ratio': 0.10, ...}

# 获取交易时段
sessions = adapter.get_trading_sessions_for_date(date(2024, 1, 15), '588000.SH')
# [{'type': 'pre_opening', 'start': 09:15, 'end': 09:25, ...}, ...]

# 验证价格笼子
result = adapter.validate_price_cage('510300.SH', 4.95, 'buy', 4.80, ask_price=4.85)
# {'valid': True, ...}
```

### 4.2 历史规则查询

```python
# 查询不同时期的规则
from finhack.trader.backtest.markets.cn_fund.etf_calculator import ETFPriceCalculator

# 创业板注册制前
limits_2020 = ETFPriceCalculator.calculate_limit_prices(
    '159949.SZ', 2.0, date(2020, 8, 20)
)
# limit_ratio = 0.10 (10%)

# 创业板注册制后
limits_2021 = ETFPriceCalculator.calculate_limit_prices(
    '159949.SZ', 2.0, date(2020, 8, 25)
)
# limit_ratio = 0.20 (20%)
```

---

## 五、测试验证

所有功能已通过单元测试验证，测试覆盖：

1. ✓ ETF板块识别
2. ✓ 涨跌停价计算
3. ✓ 不同时期规则切换
4. ✓ 最小交易单位
5. ✓ 规则变更日期查询
6. ✓ 涨跌停价计算精度
7. ✓ 手续费计算

测试输出：
```
【测试1】ETF板块识别
✓ 510300.SH (沪深300ETF): board=main_board, type=stock, exchange=sse
✓ 588000.SH (科创50ETF): board=star_market, type=stock, exchange=sse
✓ 159949.SZ (创业板50ETF): board=gem, type=stock, exchange=szse

【测试2】涨跌停价计算
✓ 510300.SH (前收盘: 4.5): 涨停: 4.950, 跌停: 4.050, 涨跌幅: 10%
✓ 588000.SH (前收盘: 1.2): 涨停: 1.440, 跌停: 0.960, 涨跌幅: 20%
✓ 159949.SZ (前收盘: 2.3): 涨停: 2.760, 跌停: 1.840, 涨跌幅: 20%

【测试3】不同时期的涨跌幅规则（创业板）
2020-08-20 (注册制前): 涨跌幅 = 10%
2020-08-25 (注册制后): 涨跌幅 = 20%
```

---

## 六、事件类型扩展

新增了盘后定价交易相关的事件类型：

```python
# 在 EventTypeEnum 中新增
POST_TRADING_START = "POST_TRADING_START"  # 盘后定价交易开始
POST_TRADING_END = "POST_TRADING_END"      # 盘后定价交易结束
```

---

## 七、API接口

### 7.1 CnFundMarketAdapter

| 方法 | 说明 |
|-----|------|
| `set_current_date(date)` | 设置当前日期 |
| `get_price_limits(symbol, prev_close, listing_date)` | 获取涨跌幅限制 |
| `validate_price_cage(symbol, order_price, side, ...)` | 验证价格笼子 |
| `get_lot_size(symbol)` | 获取最小交易单位 |
| `get_trading_sessions_for_date(date, symbol)` | 获取交易时段 |
| `get_commission_rate(symbol, direction)` | 获取手续费率 |

### 7.2 ETFPriceCalculator

| 方法 | 说明 |
|-----|------|
| `calculate_limit_prices(symbol, prev_close, date, listing_date)` | 计算涨跌停价 |

### 7.3 ETFLotSizeCalculator

| 方法 | 说明 |
|-----|------|
| `get_lot_size_info(symbol, date)` | 获取最小单位信息 |
| `normalize_order_volume(symbol, volume, side, date)` | 标准化订单数量 |

### 7.4 ETFCommissionCalculator

| 方法 | 说明 |
|-----|------|
| `get_commission_info(symbol, date)` | 获取手续费信息 |
| `calculate_commission(symbol, volume, price, side, date)` | 计算手续费 |

### 7.5 ETFPriceCageValidator

| 方法 | 说明 |
|-----|------|
| `validate_order_price(symbol, order_price, side, ...)` | 验证价格笼子 |

### 7.6 ETFDividendAdjuster

| 方法 | 说明 |
|-----|------|
| `calculate_ex_dividend_price(close, dividend)` | 计算除息价 |
| `calculate_ex_split_price(close, ratio)` | 计算除权价 |
| `calculate_ex_combined_price(close, dividend, ratio)` | 计算复合除权除息价 |

---

## 八、待扩展功能

以下功能已预留接口，可根据需要扩展：

1. **集合竞价撮合逻辑** - 当前仅生成事件，未实现撮合算法
2. **盘后定价交易撮合** - 当前仅生成事件
3. **新股判断** - 需要提供listing_date参数
4. **ST股票识别** - 需要股票名称或代码判断
5. **停牌处理** - 需要停牌信息数据源

---

## 九、参考数据源

- 上交所：http://www.sse.com.cn/
- 深交所：http://www.szse.cn/
- 北交所：http://www.bse.cn/
