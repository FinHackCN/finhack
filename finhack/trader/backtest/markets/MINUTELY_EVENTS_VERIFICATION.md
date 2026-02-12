# 分钟线回测逻辑统一 - 验证报告

## 验证结果

### 1. 期货市场 (cn_future) - 1m回测

**状态**: ✅ 通过

```
回测区间: 2024-01-02 ~ 2024-01-03
每日历史记录数量: 2
回测绩效 - 总收益: 0.00%, 年化收益: 0.00%, 夏普比率: 0.00, 最大回撤: 0.00%, 胜率: 0.00%, 交易次数: 0
```

- [x] DAY_END 事件正常生成
- [x] 每日净值正常记录
- [x] 绩效指标正常计算
- [x] 使用统一框架 (BaseMinutelyEventGenerator)

### 2. 加密货币市场 (global_cryptospot) - 1m回测

**状态**: ✅ 通过

```
回测区间: 2025-02-06 ~ 2025-02-10
每日历史记录数量: 5
回测绩效 - 总收益: -0.03%, 年化收益: -2.05%, 夏普比率: -157.88, 最大回撤: -0.03%, 胜率: 0.00%, 交易次数: 9
```

- [x] DAY_END 事件正常生成
- [x] 每日净值正常记录
- [x] 绩效指标正常计算
- [x] 周末交易支持（2025-02-08, 2025-02-09）
- [x] 使用统一框架 (BaseMinutelyEventGenerator)

## 修复内容总结

### 文件修改

| 文件 | 修改内容 | 状态 |
|------|----------|------|
| `base_minutely_events.py` | 新建统一框架 | ✅ |
| `cn_future/cn_future_adapter.py` | 使用统一框架，添加DAY_END事件 | ✅ |
| `global_forex/global_cryptospot_adapter.py` | 使用统一框架，移除重复代码 | ✅ |
| `global_cryptospot/global_cryptospot_adapter.py` | 使用统一框架，移除重复代码 | ✅ |
| `backtest_engine.py` | 交易日历支持周末交易 | ✅ |

### 统一事件序列

所有市场现在都遵循相同的分钟线事件序列：

```
1. DAY_START      → 日开始
2. BEFORE_MARKET  → 交易前
3. MARKET_START   → 开盘
4. TRY_MATCH      → 每分钟撮合（贯穿交易时段）
5. MARKET_END     → 收盘
6. AFTER_MARKET   → 交易后
7. DAY_END        → 日终（关键！）
```

### 关键设计决策

1. **不生成 DAILY_BAR_CLOSED**: 统一框架只生成 DAY_END，避免重复记录
2. **兼容2元组和3元组**: `trading_sessions` 可以是 `(start, end)` 或 `(start, end, session_type)`
3. **市场特定额外事件**: 各市场可以在统一框架基础上添加额外事件

## 代码示例

### 为新市场实现适配器

```python
from ..base_minutely_events import BaseMinutelyEventGenerator

class NewMarketAdapter(BaseMarket):
    def generate_daily_events(self, trade_date: date, frequency: str = '1d'):
        events = []

        if frequency == '1m':
            # 使用统一框架生成标准事件
            events = BaseMinutelyEventGenerator.generate_minutely_events(
                adapter=self,
                trade_date=trade_date,
                frequency='1m'
            )

            # 添加市场特定的额外事件
            additional_events = []
            if 'SPECIAL_EVENT' in self.trading_schedule.get('1m', {}):
                additional_events.append(MarketEvent(
                    event_type=EventTypeEnum.SPECIAL_EVENT,
                    event_time=datetime.combine(trade_date, time(...)),
                    market=self.market_name,
                    frequency='1m',
                    event_description="特殊事件"
                ))

            events.extend(additional_events)
            events.sort(key=lambda x: x.event_time)

        return events

    def get_trading_sessions(self, trade_date: date, frequency: str = '1d',
                            symbol: str = None) -> List[tuple]:
        """返回交易时段列表，每个元素可以是：
        - 2元组: (start_time, end_time)
        - 3元组: (start_time, end_time, session_type)
        """
        if frequency == '1m':
            return [
                (time(9, 30), time(11, 30), 'morning'),
                (time(13, 0), time(15, 0), 'afternoon')
            ]
        # ...
```

## 兼容性

- [x] 向后兼容 - 现有策略无需修改
- [x] 事件处理逻辑保持不变
- [x] 只是统一了事件生成方式

## 下一步

如果需要为其他市场（如 us_stock, hk_stock, cn_fund）也使用统一框架，可以按照相同的模式进行修改。
