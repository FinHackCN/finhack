"""
事件基类和事件类型定义
"""

from enum import Enum
from datetime import datetime
from typing import Any, Dict, Optional
from abc import ABC, abstractmethod


class EventType(Enum):
    """事件类型枚举"""
    
    # 时间驱动事件
    START_INTERVAL = "START_INTERVAL"
    START_MARKET = "START_MARKET"
    BEFORE_MARKET = "BEFORE_MARKET"
    PRE_OPENING_START = "PRE_OPENING_START"
    PRE_OPENING_END = "PRE_OPENING_END"
    MATCHING_START = "MATCHING_START"
    MORNING_START = "MORNING_START"
    MORNING_END = "MORNING_END"
    AFTERNOON_START = "AFTERNOON_START"
    CLOSING_START = "CLOSING_START"
    CLOSING_END = "CLOSING_END"
    AFTERNOON_END = "AFTERNOON_END"
    AFTER_MARKET = "AFTER_MARKET"
    END_MARKET = "END_MARKET"
    END_INTERVAL = "END_INTERVAL"
    
    # 市场数据事件
    OPENING_PRICE_DETERMINED = "OPENING_PRICE_DETERMINED"
    MINUTE_BAR_MORNING = "MINUTE_BAR_MORNING"
    MINUTE_BAR_AFTERNOON = "MINUTE_BAR_AFTERNOON"
    QUOTE_UPDATE_MORNING = "QUOTE_UPDATE_MORNING"
    QUOTE_UPDATE_AFTERNOON = "QUOTE_UPDATE_AFTERNOON"
    TRADE_TICK_MORNING = "TRADE_TICK_MORNING"
    TRADE_TICK_AFTERNOON = "TRADE_TICK_AFTERNOON"
    CLOSING_PRICE_DETERMINED = "CLOSING_PRICE_DETERMINED"
    DAILY_BAR_CLOSED = "DAILY_BAR_CLOSED"
    FINANCIAL_DATA_UPDATE = "FINANCIAL_DATA_UPDATE"
    NEWS_ANNOUNCEMENT = "NEWS_ANNOUNCEMENT"
    
    # 公司行为事件
    DIVIDEND_STOCK_SPLIT = "DIVIDEND_STOCK_SPLIT"
    RIGHTS_ISSUE_ADDITIONAL_ISSUANCE = "RIGHTS_ISSUE_ADDITIONAL_ISSUANCE"
    TRADING_SUSPENSION_RESUMPTION = "TRADING_SUSPENSION_RESUMPTION"
    DELISTING = "DELISTING"
    
    # 交易相关事件
    ORDER_SUBMISSION = "ORDER_SUBMISSION"
    ORDER_FILL = "ORDER_FILL"
    ORDER_CANCELLATION = "ORDER_CANCELLATION"
    POSITION_UPDATE = "POSITION_UPDATE"
    
    # 系统内部事件
    BACKTEST_START = "BACKTEST_START"
    BACKTEST_END = "BACKTEST_END"
    ERROR_EXCEPTION = "ERROR_EXCEPTION"
    
    # 用户自定义事件
    USER_DAILY = "USER_DAILY"
    USER_HOURLY = "USER_HOURLY"
    USER_MINUTELY = "USER_MINUTELY"
    USER_WEEKLY = "USER_WEEKLY"
    USER_MONTHLY = "USER_MONTHLY"
    USER_INTERVAL = "USER_INTERVAL"


class BaseEvent(ABC):
    """事件基类"""
    
    def __init__(self, event_type: EventType, event_time: datetime, 
                 market: str = "cn_stock", adapter_id: str = "default",
                 data: Optional[Dict[str, Any]] = None):
        """
        初始化事件
        
        Args:
            event_type: 事件类型
            event_time: 事件发生时间
            market: 市场标识 (cn_stock, hk_stock, us_stock等)
            adapter_id: 适配器ID
            data: 事件附加数据
        """
        self.event_type = event_type
        self.event_time = event_time
        self.market = market
        self.adapter_id = adapter_id
        self.data = data or {}
        self.processed = False
    
    @property
    def event_name(self) -> str:
        """事件名称"""
        return self.event_type.value
    
    @property
    def timestamp(self) -> float:
        """事件时间戳"""
        return self.event_time.timestamp()
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'event_type': self.event_type.value,
            'event_time': self.event_time.isoformat(),
            'market': self.market,
            'adapter_id': self.adapter_id,
            'data': self.data,
            'processed': self.processed
        }
    
    @abstractmethod
    def process(self, context) -> bool:
        """
        处理事件
        
        Args:
            context: 回测上下文
            
        Returns:
            bool: 是否处理成功
        """
        pass
    
    def __str__(self) -> str:
        return f"Event({self.event_type.value}, {self.event_time}, {self.market})"
    
    def __repr__(self) -> str:
        return self.__str__()
    
    def __lt__(self, other):
        """支持事件排序（按时间排序）"""
        if isinstance(other, BaseEvent):
            return self.event_time < other.event_time
        return NotImplemented
    
    def __eq__(self, other):
        """事件相等性比较"""
        if isinstance(other, BaseEvent):
            return (self.event_type == other.event_type and 
                    self.event_time == other.event_time and
                    self.market == other.market and
                    self.adapter_id == other.adapter_id)
        return NotImplemented 