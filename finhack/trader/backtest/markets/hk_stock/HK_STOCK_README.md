# 港股市场适配器

## 概述

港股市场适配器实现了香港股票市场的交易规则和事件生成功能，支持回测系统中的港股交易模拟。

## 目录结构

```
hk_stock/
├── __init__.py                      # 模块导出
├── hk_stock_adapter.py              # 主适配器类
├── hk_trading_rules_versions.py     # 规则版本定义
├── hk_calculator.py                 # 计算器模块
├── test_hk_adapter.py               # 适配器测试
└── test_hk_standalone.py            # 独立测试脚本
```

## 主要功能

### 1. 交易时间

- **盘前交易**: 09:00-09:30
- **上午交易**: 09:30-12:00
- **午休**: 12:00-13:00
- **下午交易**: 13:00-16:00
- **收市竞价**: 16:00-16:10（随机收市机制）

### 2. 交易规则

#### T+0交易，T+2结算
- 当天买入的股票可以当天卖出
- 交易日后2个交易日进行资金和股票结算

#### 无涨跌停限制
- 港股没有涨跌停限制
- 但有VCM（波动调节机制）对部分大盘股进行价格波动控制

#### VCM波动调节机制
- 适用对象：81只恒指及H股指数成分股（如腾讯00700.HK、汇丰00005.HK等）
- 触发条件：5分钟内波动10%
- 冷静期：5分钟
- 价格限制：冷静期内价格限制在触发价的±10%

### 3. 最小交易单位

港股不同股票的"一手"股数不同，根据股价范围确定：

| 价格区间（港元） | 最小交易单位（股） |
|----------------|------------------|
| 0.01 - 0.25    | 1000             |
| 0.25 - 0.50    | 500              |
| 0.50 - 10.00   | 100              |
| 10.00 - 20.00  | 50               |
| 20.00 - 100.00 | 20               |
| 100.00 - 200.00| 10               |
| 200.00 - 500.00| 5                |
| 500.00 - 1000.00| 2              |
| 1000.00以上    | 1                |

### 4. 价格精度（Tick Size）

港股最小价格变动单位根据股价范围不同：

| 价格区间（港元） | 最小价格变动单位 |
|----------------|----------------|
| 0.01 - 0.25    | 0.001           |
| 0.25 - 0.50    | 0.005           |
| 0.50 - 10.00   | 0.010           |
| 10.00 - 20.00  | 0.020           |
| 20.00 - 100.00 | 0.050           |
| 100.00 - 200.00| 0.100           |
| 200.00 - 500.00| 0.200           |
| 500.00 - 1000.00| 0.500          |
| 1000.00以上    | 1.000           |

### 5. 手续费结构

#### 买入费用
- **佣金**: 0.1%（可协商，典型0.01%-0.25%）
- **交易费**: 0.005%（十万分之五）
- **交易征费**: 0.0027%（十万分之2.7）
- **交收费**: 0.002%（十万分之二，最高200港元）

#### 卖出费用
- **佣金**: 0.1%
- **印花税**: 0.1%（仅卖出）
- **交易费**: 0.005%
- **交易征费**: 0.0027%
- **交收费**: 0.002%（最高200港元）

### 6. 支持的频率

- 1d（日频）
- 1m（分钟频）
- 30m（30分钟频）
- 120m（2小时频）

## 使用示例

### 创建适配器

```python
from finhack.trader.backtest.markets.hk_stock import HKStockMarketAdapter
from datetime import date

# 创建适配器
adapter = HKStockMarketAdapter(current_date=date(2024, 1, 15))
```

### 生成每日事件

```python
# 生成每日事件
events = adapter.generate_daily_events(date(2024, 1, 15), '1d', '00700.HK')

# 查看事件
for event in events:
    print(f"{event.event_time} {event.event_type.value}: {event.event_description}")
```

### 获取交易时段

```python
# 获取交易时段
sessions = adapter.get_trading_sessions_for_date(date(2024, 1, 15), '00700.HK', '1d')

for session in sessions:
    print(f"{session['type']}: {session['start']}-{session['end']}")
```

### 计算手续费

```python
from finhack.trader.backtest.markets.hk_stock import HKStockCommissionCalculator
from datetime import date

# 计算买入费用
fee = HKStockCommissionCalculator.calculate_commission(
    '00700.HK', 1000, 380.0, 'buy', date(2024, 1, 15)
)

print(f"总费用: {fee['total_fee']} 港元")
```

### 检查VCM触发

```python
from finhack.trader.backtest.markets.hk_stock import HKVCMValidator

# 检查VCM是否触发
vcm_result = HKVCMValidator.check_vcm_trigger(
    '00700.HK', current_price=400.0, reference_price=360.0,
    query_date=date(2024, 1, 15)
)

if vcm_result['triggered']:
    print(f"VCM触发！冷静期{vcm_result['cooling_period_minutes']}分钟")
    print(f"价格限制: {vcm_result['price_limit']}")
```

### 获取最小交易单位

```python
# 获取lot size
lot_size = adapter.get_lot_size('00700.HK', price=380.0)
print(f"最小交易单位: {lot_size} 股")
```

## 板块分类

### 主板（Main Board）
- 代码范围：00001-79999
- 大部分港股都在主板

### 创业板（GEM）
- 代码范围：80000-89999
- 代码以8开头的股票

## 测试

运行独立测试：

```bash
cd /mnt/ssd2/finhack-dev/finhack/finhack/trader/backtest/markets/hk_stock
python test_hk_standalone.py
```

测试结果示例：
- 板块识别测试
- 最小交易单位测试
- 手续费计算测试
- VCM机制测试
- Lot Size详细规则

## 注意事项

1. **交易日历**: 当前版本使用简单的周一至周五判断，实际港股节假日需要专门的交易日历数据

2. **VCM股票列表**: 当前只包含了主要的VCM股票，实际使用时需要完整的VCM股票列表

3. **Lot Size**: 实际港股的lot size是每只股票单独设定的，这里使用基于价格的简化规则

4. **随机收市**: 收市竞价时段（16:00-16:10）会在16:08-16:10之间随机收市，测试时使用日期哈希值作为种子确保可重现

## 未来改进

- [ ] 添加完整的港股交易日历
- [ ] 支持窝轮、牛熊证等衍生品
- [ ] 支持港股通交易规则
- [ ] 添加更精确的lot size数据
- [ ] 完善VCM股票列表
