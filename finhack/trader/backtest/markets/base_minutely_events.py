"""
统一的分钟线事件生成框架

确保所有市场的1分钟回测逻辑一致，只是具体规则不同。
"""

from typing import List, Dict, Any, Tuple
from datetime import datetime, date, time, timedelta
from .base_market import BaseMarket
from ..events.event_types import BaseEvent, MarketEvent, EventTypeEnum


class BaseMinutelyEventGenerator:
    """统一的分钟线事件生成器基类

    提供标准的分钟线事件生成模板，各市场只需实现具体规则：
    - get_trading_sessions(): 返回交易时段
    - should_skip_weekends(): 是否跳过周末（通过交易日历控制）
    - get_end_time(): 返回日终时间
    """

    @staticmethod
    def generate_minutely_events(
        adapter: BaseMarket,
        trade_date: date,
        frequency: str = '1m',
        additional_events: List[BaseEvent] = None
    ) -> List[BaseEvent]:
        """生成分钟线频率的标准事件列表

        这是所有市场都应该遵循的统一模板：
        1. 日开始事件 (DAY_START)
        2. 交易前事件 (BEFORE_MARKET)
        3. 开盘事件 (MARKET_START)
        4. 交易时段的撮合事件 (TRY_MATCH)
        5. 收盘事件 (MARKET_END)
        6. 交易后事件 (AFTER_MARKET)
        7. 日终事件 (DAY_END) - 关键！用于记录每日净值

        Args:
            adapter: 市场适配器实例
            trade_date: 交易日期
            frequency: 数据频率
            additional_events: 额外的市场特定事件

        Returns:
            List[BaseEvent]: 事件列表
        """
        events = []

        # 获取交易时段
        trading_sessions = adapter.get_trading_sessions(trade_date, frequency)
        if not trading_sessions:
            return events

        # 获取第一个交易时段的开始和最后一个交易时段的结束
        # 兼容2元组 (start, end) 和3元组 (start, end, session_type) 格式
        first_session = trading_sessions[0]
        last_session = trading_sessions[-1]

        if len(first_session) >= 2:
            first_session_start = first_session[0]
        else:
            first_session_start = first_session

        if len(last_session) >= 2:
            last_session_end = last_session[1]
        else:
            last_session_end = last_session

        # 如果是跨日（如期货夜盘），last_session_end可能是第二天的时间
        if isinstance(last_session_end, datetime):
            end_time = last_session_end
        else:
            end_time = datetime.combine(trade_date, last_session_end)

        # 如果第一个时段是跨日的（如期货夜盘从上一日开始）
        if isinstance(first_session_start, datetime):
            start_time = first_session_start
        else:
            start_time = datetime.combine(trade_date, first_session_start)

        # 1. 日开始事件 - 标记新交易日的开始
        events.append(MarketEvent(
            event_type=EventTypeEnum.DAY_START,
            event_time=start_time,
            market=adapter.market_name,
            frequency=frequency,
            event_description="日开始"
        ))

        # 2. 交易前事件 - 用于策略初始化和调仓
        events.append(MarketEvent(
            event_type=EventTypeEnum.BEFORE_MARKET,
            event_time=start_time,
            market=adapter.market_name,
            frequency=frequency,
            event_description="交易前准备"
        ))

        # 3. 开盘事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.MARKET_START,
            event_time=start_time,
            market=adapter.market_name,
            frequency=frequency,
            event_description="开盘"
        ))

        # 4. 生成每个交易时段的撮合事件
        interval_minutes = 1  # 1分钟频率
        for session in trading_sessions:
            # 兼容2元组和3元组格式
            if len(session) == 2:
                session_start, session_end = session
            elif len(session) >= 3:
                session_start, session_end, _ = session[:3]

            # 转换为datetime对象
            if isinstance(session_start, time):
                session_start_dt = datetime.combine(trade_date, session_start)
            else:
                session_start_dt = session_start

            if isinstance(session_end, time):
                session_end_dt = datetime.combine(trade_date, session_end)
            else:
                session_end_dt = session_end

            # 生成该时段的撮合事件
            current_time = session_start_dt
            while current_time < session_end_dt:
                match_event = MarketEvent(
                    event_type=EventTypeEnum.TRY_MATCH,
                    event_time=current_time,
                    market=adapter.market_name,
                    frequency=frequency,
                    event_description=f"{frequency}级撮合"
                )
                events.append(match_event)
                current_time += timedelta(minutes=interval_minutes)

        # 5. 收盘事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.MARKET_END,
            event_time=end_time,
            market=adapter.market_name,
            frequency=frequency,
            event_description="收盘"
        ))

        # 6. 交易后事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.AFTER_MARKET,
            event_time=end_time,
            market=adapter.market_name,
            frequency=frequency,
            event_description="交易后处理"
        ))

        # 7. 日终事件 - 关键！用于记录每日净值和计算绩效
        # 注意：不生成 DAILY_BAR_CLOSED，避免重复记录
        events.append(MarketEvent(
            event_type=EventTypeEnum.DAY_END,
            event_time=end_time,
            market=adapter.market_name,
            frequency=frequency,
            event_description="日终处理"
        ))

        # 8. 添加市场特定的额外事件
        if additional_events:
            events.extend(additional_events)

        # 按时间排序
        events.sort(key=lambda x: x.event_time)

        return events


def create_minutely_event_template(adapted_market: str) -> str:
    """为特定市场生成分钟线事件生成模板代码

    Args:
        adapted_market: 市场名称

    Returns:
        生成的模板代码字符串
    """
    return f'''
def _generate_daily_events_1m(self, trade_date: date, schedule: Dict[str, time]) -> List[BaseEvent]:
    """生成分钟线频率的事件列表"""
    from .base_minutely_events import BaseMinutelyEventGenerator

    return BaseMinutelyEventGenerator.generate_minutely_events(
        adapter=self,
        trade_date=trade_date,
        frequency='1m'
    )
'''
