"""
统一的日线事件生成框架

确保所有市场的日线回测逻辑一致，只是具体规则不同。
"""

from typing import List, Dict, Any, Tuple
from datetime import datetime, date, time, timedelta
from .base_market import BaseMarket
from ..events.event_types import BaseEvent, MarketEvent, EventTypeEnum


class BaseDailyEventGenerator:
    """统一的日线事件生成器基类

    提供标准的日线事件生成模板，各市场只需实现具体规则：
    - get_trading_sessions(): 返回交易时段
    - has_auction(): 是否有集合竞价
    - has_closing_auction(): 是否有收盘集合竞价
    """

    @staticmethod
    def generate_daily_events(
        adapter: BaseMarket,
        trade_date: date,
        additional_events: List[BaseEvent] = None
    ) -> List[BaseEvent]:
        """生成日线频率的标准事件列表

        这是所有市场都应该遵循的统一模板：
        1. 日开始事件 (DAY_START)
        2. 交易前事件 (BEFORE_MARKET)
        3. 开盘事件 (MARKET_START)
        4. 撮合事件 (TRY_MATCH) - 日线通常只有一个
        5. 收盘事件 (MARKET_END)
        6. 交易后事件 (AFTER_MARKET)
        7. 日终事件 (DAY_END) - 关键！用于记录每日净值

        Args:
            adapter: 市场适配器实例
            trade_date: 交易日期
            additional_events: 额外的市场特定事件

        Returns:
            List[BaseEvent]: 事件列表
        """
        events = []

        # 获取交易时段
        trading_sessions = adapter.get_trading_sessions(trade_date, '1d')
        if not trading_sessions:
            return events

        # 获取第一个交易时段的开始和最后一个交易时段的结束
        first_session_start = trading_sessions[0][0]
        last_session_end = trading_sessions[-1][1]

        # 转换为datetime对象
        if isinstance(first_session_start, datetime):
            start_time = first_session_start
        else:
            start_time = datetime.combine(trade_date, first_session_start)

        if isinstance(last_session_end, datetime):
            end_time = last_session_end
        else:
            end_time = datetime.combine(trade_date, last_session_end)

        # 1. 日开始事件 - 在交易开始前触发
        events.append(MarketEvent(
            event_type=EventTypeEnum.DAY_START,
            event_time=start_time,
            market=adapter.market_name,
            frequency='1d',
            event_description="日开始"
        ))

        # 2. 交易前事件 - 用于策略初始化和调仓
        events.append(MarketEvent(
            event_type=EventTypeEnum.BEFORE_MARKET,
            event_time=start_time,
            market=adapter.market_name,
            frequency='1d',
            event_description="交易前准备"
        ))

        # 3. 开盘事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.MARKET_START,
            event_time=start_time,
            market=adapter.market_name,
            frequency='1d',
            event_description="开盘"
        ))

        # 4. 撮合事件 - 日线通常在开盘时撮合一次
        events.append(MarketEvent(
            event_type=EventTypeEnum.TRY_MATCH,
            event_time=start_time,
            market=adapter.market_name,
            frequency='1d',
            event_description="日级撮合"
        ))

        # 5. 收盘事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.MARKET_END,
            event_time=end_time,
            market=adapter.market_name,
            frequency='1d',
            event_description="收盘"
        ))

        # 6. 交易后事件 - 在收盘后一段时间触发
        # 根据市场类型选择合适的时间
        if adapter.market_name.startswith('global_crypto'):
            # 加密货币使用23:59
            after_market_time = datetime.combine(trade_date, time(23, 59))
        elif adapter.market_name == 'cn_stock':
            # A股使用18:00
            after_market_time = datetime.combine(trade_date, time(18, 0))
        else:
            # 其他市场使用收盘时间
            after_market_time = end_time

        events.append(MarketEvent(
            event_type=EventTypeEnum.AFTER_MARKET,
            event_time=after_market_time,
            market=adapter.market_name,
            frequency='1d',
            event_description="交易后处理"
        ))

        # 7. 日终事件 - 关键！用于记录每日净值和计算绩效
        # 注意：不生成 DAILY_BAR_CLOSED，避免重复记录
        events.append(MarketEvent(
            event_type=EventTypeEnum.DAY_END,
            event_time=after_market_time,
            market=adapter.market_name,
            frequency='1d',
            event_description="日终处理"
        ))

        # 8. 添加市场特定的额外事件
        if additional_events:
            events.extend(additional_events)

        # 按时间排序
        events.sort(key=lambda x: x.event_time)

        return events


def create_daily_event_template(market_name: str) -> str:
    """为特定市场生成日线事件生成模板代码

    Args:
        market_name: 市场名称

    Returns:
        生成的模板代码字符串
    """
    return f'''
def _generate_daily_events_1d(self, trade_date: date) -> List[BaseEvent]:
    """生成日线频率的事件列表"""
    from .base_daily_events import BaseDailyEventGenerator

    return BaseDailyEventGenerator.generate_daily_events(
        adapter=self,
        trade_date=trade_date
    )
'''
