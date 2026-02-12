# 事件统一框架 - 快速参考指南

## 快速开始

### 为新市场实现适配器

```python
from typing import List, Dict, Any
from datetime import datetime, date, time
from ..base_market import BaseMarket
from ..base_minutely_events import BaseMinutelyEventGenerator
from ..base_daily_events import BaseDailyEventGenerator
from ...events.event_types import BaseEvent, MarketEvent, EventTypeEnum

class MyMarketAdapter(BaseMarket):
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__('my_market', config or self._get_default_config())

    def generate_daily_events(self, trade_date: date, frequency: str = '1d') -> List[BaseEvent]:
        """生成指定日期和频率的事件列表"""
        events = []

        if frequency == '1d':
            # 使用日线统一框架
            events = BaseDailyEventGenerator.generate_daily_events(
                adapter=self,
                trade_date=trade_date
            )
        elif frequency in ['1m', '30m', '120m']:
            # 使用分钟线统一框架
            events = BaseMinutelyEventGenerator.generate_minutely_events(
                adapter=self,
                trade_date=trade_date,
                frequency=frequency
            )

        # 添加市场特有的额外事件（可选）
        additional_events = self._get_market_specific_events(trade_date, frequency)
        events.extend(additional_events)

        # 按时间排序
        events.sort(key=lambda x: x.event_time)
        return events

    def _get_market_specific_events(self, trade_date: date, frequency: str) -> List[BaseEvent]:
        """生成市场特有的额外事件（可选）"""
        events = []

        # 示例：添加集合竞价事件
        if frequency == '1d':
            events.append(MarketEvent(
                event_type=EventTypeEnum.PRE_OPENING_START,
                event_time=datetime.combine(trade_date, time(9, 15)),
                market=self.market_name,
                frequency=frequency,
                event_description="集合竞价开始"
            ))

        return events

    def get_trading_sessions(self, trade_date: date, frequency: str = '1d'):
        """返回交易时段列表

        返回格式：List[Tuple]
        - 2元组: (start_time, end_time)
        - 3元组: (start_time, end_time, session_type)
        """
        if frequency == '1d':
            return [(time(9, 30), time(15, 0))]
        elif frequency == '1m':
            return [
                (time(9, 30), time(11, 30), 'morning'),
                (time(13, 0), time(15, 0), 'afternoon')
            ]

    def _get_default_config(self) -> Dict[str, Any]:
        """返回默认配置"""
        return {
            'market_name': 'my_market',
            'supported_frequencies': ['1d', '1m', '30m', '120m'],
            'timezone': 'Asia/Shanghai',
            'currency': 'CNY',
            'trading_schedule': {
                '1d': {
                    'morning_start': '09:30',
                    'morning_end': '11:30',
                    'afternoon_start': '13:00',
                    'afternoon_end': '15:00'
                },
                '1m': {
                    'morning_start': '09:30',
                    'morning_end': '11:30',
                    'afternoon_start': '13:00',
                    'afternoon_end': '15:00'
                }
            }
        }
```

## 统一框架 API

### BaseMinutelyEventGenerator.generate_minutely_events()

```python
events = BaseMinutelyEventGenerator.generate_minutely_events(
    adapter=self,           # 市场适配器实例
    trade_date=trade_date,  # 交易日期
    frequency='1m'          # 数据频率：1m, 30m, 120m
)
```

**生成的事件**:
1. DAY_START - 日开始
2. BEFORE_MARKET - 交易前准备
3. MARKET_START - 开盘
4. TRY_MATCH - 每分钟撮合（贯穿交易时段）
5. MARKET_END - 收盘
6. AFTER_MARKET - 交易后处理
7. DAY_END - 日终（记录净值）

### BaseDailyEventGenerator.generate_daily_events()

```python
events = BaseDailyEventGenerator.generate_daily_events(
    adapter=self,           # 市场适配器实例
    trade_date=trade_date   # 交易日期
)
```

**生成的事件**:
1. DAY_START - 日开始
2. BEFORE_MARKET - 交易前准备
3. MARKET_START - 开盘
4. TRY_MATCH - 日级撮合
5. MARKET_END - 收盘
6. AFTER_MARKET - 交易后处理（根据市场类型选择时间）
7. DAY_END - 日终（记录净值）

**AFTER_MARKET 时间规则**:
- 加密货币: 23:59
- A股: 18:00
- 其他市场: 收盘时间

## 市场特定事件类型

### 常用事件类型

| 事件类型 | 说明 | 适用市场 |
|---------|------|---------|
| PRE_OPENING_START | 集合竞价开始 | 股票、基金 |
| PRE_OPENING_END | 集合竞价结束 | 股票、基金 |
| MATCHING_START | 集合竞价撮合 | 股票、基金 |
| OPENING_PRICE_DETERMINED | 开盘价确定 | 股票、基金 |
| MORNING_END | 上午收盘 | 午休市场 |
| AFTERNOON_START | 下午开盘 | 午休市场 |
| CLOSING_START | 收盘集合竞价开始 | 股票、港股 |
| CLOSING_END | 收盘集合竞价结束 | 股票、港股 |
| CLOSING_PRICE_DETERMINED | 收盘价确定 | 股票、港股 |
| SETTLEMENT_PRICE_DETERMINED | 结算价确定 | 期货 |
| MARGIN_CALL_CHECK | 保证金检查 | 期货 |

### K线事件类型

| 事件类型 | 说明 |
|---------|------|
| MARKET_BAR_1M | 1分钟K线 |
| MARKET_BAR_30M | 30分钟K线 |
| MARKET_BAR_120M | 120分钟K线 |
| MARKET_BAR_1D | 日K线 |
| DAILY_BAR_CLOSED | 日K线生成（与DAY_END重复，不推荐使用） |

## 检查清单

### 实现新市场适配器时

- [ ] 导入统一框架模块
- [ ] 实现 generate_daily_events 方法
- [ ] 实现 get_trading_sessions 方法
- [ ] 实现 _get_default_config 方法
- [ ] 添加市场特有的额外事件（可选）
- [ ] 运行回测验证 DAY_END 事件触发
- [ ] 验证每日历史记录正确
- [ ] 验证绩效指标正常计算

### 迁移现有市场时

- [ ] 备份原始文件
- [ ] 添加统一框架导入
- [ ] 重构 generate_daily_events 方法
- [ ] 提取额外事件为独立方法
- [ ] 移除重复代码
- [ ] 运行回测对比结果
- [ ] 验证事件序列一致
- [ ] 提交更改

## 常见问题

### Q: 为什么不生成 DAILY_BAR_CLOSED？

A: 因为 DAY_END 和 DAILY_BAR_CLOSED 都触发 `_handle_day_end_sync`，会导致每日净值重复记录。统一框架只生成 DAY_END。

### Q: 如何添加市场特定的额外事件？

A: 在调用统一框架后，添加额外事件到 events 列表，然后排序：

```python
events = BaseMinutelyEventGenerator.generate_minutely_events(...)

# 添加额外事件
events.extend(self._get_market_specific_events(...))

# 排序
events.sort(key=lambda x: x.event_time)
```

### Q: 如何处理跨日交易时段（如期货夜盘）？

A: 使用3元组格式，第一个元素可以是 datetime 对象：

```python
return [
    (time(21, 0), time(23, 59), 'night'),     # 当日
    (datetime.combine(trade_date + timedelta(days=1), time(0, 0)),
     datetime.combine(trade_date + timedelta(days=1), time(2, 30)),
     'night_next_day')  # 次日
]
```

### Q: DAY_END 时间应该在什么时候？

A: 统一框架使用收盘时间作为 DAY_END 时间。但A股使用18:00（收盘后3小时），这可以在 BaseDailyEventGenerator 中特殊处理。

## 相关文档

- `UNIFIED_EVENTS_FRAMEWORK.md` - 完整框架说明
- `FINAL_VERIFICATION_REPORT.md` - 验证报告
- `MINUTELY_EVENTS_UNIFICATION.md` - 分钟线统一方案
- `MINUTELY_EVENTS_VERIFICATION.md` - 分钟线验证报告
