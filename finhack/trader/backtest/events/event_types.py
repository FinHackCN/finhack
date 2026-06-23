"""
事件类型定义

定义回测系统中的所有事件类型，支持多市场多频次
"""

from enum import Enum
from dataclasses import dataclass
from datetime import datetime, time
from typing import Dict, Any, Optional


class EventTypeEnum(Enum):
    """事件类型枚举"""
    # 时间驱动事件
    DAY_START = "DAY_START"
    DAY_END = "DAY_END"
    
    # 市场事件
    BEFORE_MARKET = "BEFORE_MARKET"
    PRE_OPENING_START = "PRE_OPENING_START"
    PRE_OPENING_END = "PRE_OPENING_END"
    MATCHING_START = "MATCHING_START"
    OPENING_PRICE_DETERMINED = "OPENING_PRICE_DETERMINED"
    MARKET_START = "MARKET_START"
    MORNING_END = "MORNING_END"
    AFTERNOON_START = "AFTERNOON_START"
    CLOSING_START = "CLOSING_START"
    CLOSING_END = "CLOSING_END"
    CLOSING_PRICE_DETERMINED = "CLOSING_PRICE_DETERMINED"
    MARKET_END = "MARKET_END"
    DAILY_BAR_CLOSED = "DAILY_BAR_CLOSED"
    AFTER_MARKET = "AFTER_MARKET"

    # 盘后定价交易（科创板/创业板）
    POST_TRADING_START = "POST_TRADING_START"
    POST_TRADING_END = "POST_TRADING_END"

    # K线事件
    MARKET_BAR_1D = "MARKET_BAR_1D"
    MARKET_BAR_1M = "MARKET_BAR_1M"
    MARKET_BAR_30M = "MARKET_BAR_30M"
    MARKET_BAR_120M = "MARKET_BAR_120M"
    
    # 期货特有事件
    DAY_SESSION_START = "DAY_SESSION_START"
    DAY_SESSION_END = "DAY_SESSION_END"
    SETTLEMENT_PRICE_DETERMINED = "SETTLEMENT_PRICE_DETERMINED"
    MARGIN_CALL_CHECK = "MARGIN_CALL_CHECK"
    FUNDING_RATE_SETTLE = "FUNDING_RATE_SETTLE"
    BEFORE_NIGHT_SESSION = "BEFORE_NIGHT_SESSION"
    NIGHT_AUCTION_START = "NIGHT_AUCTION_START"
    NIGHT_SESSION_START = "NIGHT_SESSION_START"
    NIGHT_SESSION_END = "NIGHT_SESSION_END"
    
    # 动态事件
    ON_TIME = "ON_TIME"
    ORDER_SUBMISSION = "ORDER_SUBMISSION"
    ORDER_CANCELLATION = "ORDER_CANCELLATION"
    ORDER_REJECT = "ORDER_REJECT"
    ORDER_FILL = "ORDER_FILL"
    TRY_MATCH = "TRY_MATCH"
    
    # 公司行为事件
    CORPORATE_ACTION = "CORPORATE_ACTION"


class EventPriorityEnum(Enum):
    """事件优先级枚举"""
    HIGHEST = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4
    LOWEST = 5


@dataclass
class BaseEvent:
    """基础事件类"""
    event_type: EventTypeEnum
    event_time: datetime
    market: str
    frequency: str = "1d"
    priority: EventPriorityEnum = EventPriorityEnum.NORMAL
    data: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.data is None:
            self.data = {}


@dataclass
class MarketEvent(BaseEvent):
    """市场事件"""
    event_description: str = ""


@dataclass
class TimeEvent(BaseEvent):
    """时间事件（用户定义的定时任务）"""
    function_name: str = ""
    task_id: str = ""


@dataclass
class TradeEvent(BaseEvent):
    """交易事件"""
    order_id: Optional[str] = None
    symbol: Optional[str] = None


# 导出所有事件类型
__all__ = [
    "EventTypeEnum",
    "EventPriorityEnum", 
    "BaseEvent",
    "MarketEvent",
    "TimeEvent",
    "TradeEvent"
] 