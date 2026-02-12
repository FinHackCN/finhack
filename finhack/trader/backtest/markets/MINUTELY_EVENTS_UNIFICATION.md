# 分钟线回测逻辑统一方案

## 概述

确保所有市场的1分钟回测逻辑一致，只是具体规则不同。

## 统一的事件生成模板

### 标准分钟线事件序列

所有市场都应该遵循以下标准事件序列：

```
1. DAY_START      - 日开始，标记新交易日的开始
2. BEFORE_MARKET  - 交易前，用于策略初始化和调仓
3. MARKET_START   - 开盘
4. TRY_MATCH      - 每分钟撮合事件（贯穿所有交易时段）
5. MARKET_END     - 收盘
6. AFTER_MARKET   - 交易后处理
7. DAY_END        - 日终，关键！用于记录每日净值和计算绩效
```

### 关键设计决策

| 决策 | 说明 | 原因 |
|------|------|------|
| **不生成 DAILY_BAR_CLOSED** | 统一框架只生成 DAY_END，不生成 DAILY_BAR_CLOSED | 避免重复记录每日净值 |
| **交易时段由 get_trading_sessions() 定义** | 各市场自行实现交易时段逻辑 | 保持灵活性，支持不同市场的特殊规则 |
| **使用 BaseMinutelyEventGenerator** | 统一的事件生成框架 | 减少代码重复，确保一致性 |

## 各市场的具体规则差异

### 交易时段

| 市场 | 交易时段 | 特殊处理 |
|------|---------|---------|
| cn_stock | 09:30-11:30, 13:00-15:00 | 午休分割 |
| cn_future | 09:00-15:00, 21:00-02:30+1 | 日盘+夜盘，跨日 |
| global_cryptospot | 00:00-23:59 | 24/7交易 |
| us_stock | 盘前+正常+盘后 | 多时段 |
| hk_stock | 09:30-12:00, 13:00-16:00+随机 | 午休+随机收市 |
| cn_fund | 09:30-11:30, 13:00-15:00 | 同A股 |

### 额外事件（市场特定）

各市场可以在统一框架基础上添加额外事件：

- **cn_future**: 结算价确定、保证金检查、夜盘事件
- **cn_stock/hk_stock**: 集合竞价事件、收盘阶段事件
- **us_stock**: 盘前盘后事件
- **cn_fund**: 科创板/创业板盘后定价交易

## 修复内容

### 1. 创建统一框架

**文件**: `base_minutely_events.py`

提供 `BaseMinutelyEventGenerator.generate_minutely_events()` 方法，所有市场都可以使用。

### 2. 修复 cn_future_adapter.py

**问题**: 缺少 DAY_END 事件，导致每日净值无法记录

**修复**:
- 导入 `BaseMinutelyEventGenerator`
- 使用统一框架生成基础事件
- 保留期货特有的额外事件（结算价、保证金等）

### 3. 修复 global_forex/global_cryptospot_adapter.py

**问题**: 代码重复（上午和下午循环相同），包含 DAILY_BAR_CLOSED

**修复**: 使用统一框架，移除重复代码

### 4. 修复 global_cryptospot/global_cryptospot_adapter.py

**问题**: 代码重复（上午和下午循环相同），包含 DAILY_BAR_CLOSED

**修复**: 使用统一框架，移除重复代码

### 5. 修复 backtest_engine.py

**问题**: 交易日历硬编码跳过周末

**修复**:
```python
# 检查市场类型，判断是否跳过周末
market_name = self.context.get('settings', {}).get('market', '')
skip_weekend = not market_name.startswith('global_crypto')
```

## 使用指南

### 为新市场实现适配器

1. 继承 `BaseMarket`
2. 实现 `get_trading_sessions()` 方法
3. 在 `generate_daily_events()` 中：
   - 对于1m频率，调用 `BaseMinutelyEventGenerator.generate_minutely_events()`
   - 添加市场特定的额外事件

### 示例代码

```python
from ..base_minutely_events import BaseMinutelyEventGenerator

class MyMarketAdapter(BaseMarket):
    def generate_daily_events(self, trade_date: date, frequency: str = '1d'):
        events = []

        if frequency == '1m':
            # 使用统一框架
            events = BaseMinutelyEventGenerator.generate_minutely_events(
                adapter=self,
                trade_date=trade_date,
                frequency='1m'
            )

            # 添加市场特定的额外事件
            events.extend(self._get_market_specific_events(trade_date))

        return events
```

## 验证清单

- [x] 创建统一的分钟线事件生成框架
- [x] cn_future 使用统一框架
- [x] global_cryptospot 使用统一框架
- [x] global_forex 使用统一框架
- [x] 交易日历支持加密货币周末交易
- [x] 移除 DAILY_BAR_CLOSED 避免重复记录
- [x] 所有市场都生成 DAY_END 事件

## 兼容性

这些修复保持向后兼容：
- 现有策略无需修改
- 事件处理逻辑保持不变
- 只是统一了事件生成的方式

## 文件清单

修改的文件：
1. `backtest_engine.py` - 交易日历支持周末交易
2. `base_minutely_events.py` - 新建统一框架
3. `cn_future/cn_future_adapter.py` - 使用统一框架
4. `global_forex/global_cryptospot_adapter.py` - 使用统一框架
5. `global_cryptospot/global_cryptospot_adapter.py` - 使用统一框架
