"""
市场事件实现
"""

from datetime import datetime
from typing import Any, Dict, Optional

from .base_event import BaseEvent, EventType


class MarketEvent(BaseEvent):
    """市场事件基类"""
    
    def __init__(self, event_type: EventType, event_time: datetime,
                 market: str = "cn_stock", adapter_id: str = "default",
                 data: Optional[Dict[str, Any]] = None):
        super().__init__(event_type, event_time, market, adapter_id, data)
    
    def process(self, context) -> bool:
        """
        处理市场事件
        
        Args:
            context: 回测上下文
            
        Returns:
            bool: 是否处理成功
        """
        try:
            # 更新当前时间
            context.current_dt = self.event_time
            
            # 根据事件类型调用相应的处理方法
            handler_name = f"handle_{self.event_type.value.lower()}"
            
            # 检查事件中心是否有对应的处理方法
            if hasattr(context.event_center, handler_name):
                handler = getattr(context.event_center, handler_name)
                handler(context, self)
            
            # 检查交易中心是否有对应的处理方法
            if hasattr(context.trade_center, handler_name):
                handler = getattr(context.trade_center, handler_name)
                handler(context, self)
            
            # 检查数据中心是否有对应的处理方法
            if hasattr(context.data_center, handler_name):
                handler = getattr(context.data_center, handler_name)
                handler(context, self)
            
            # 如果策略有对应的处理方法，也调用
            if context.strategy and hasattr(context.strategy, handler_name):
                handler = getattr(context.strategy, handler_name)
                handler(context, self)
            
            self.processed = True
            return True
            
        except Exception as e:
            context.logger.error(f"处理市场事件失败: {self.event_type.value}, 错误: {str(e)}")
            return False


class StartIntervalEvent(MarketEvent):
    """开始区间事件"""
    
    def __init__(self, event_time: datetime, market: str = "cn_stock", 
                 adapter_id: str = "default"):
        super().__init__(EventType.START_INTERVAL, event_time, market, adapter_id)


class BeforeMarketEvent(MarketEvent):
    """盘前事件"""
    
    def __init__(self, event_time: datetime, market: str = "cn_stock", 
                 adapter_id: str = "default"):
        super().__init__(EventType.BEFORE_MARKET, event_time, market, adapter_id)


class MorningStartEvent(MarketEvent):
    """上午开盘事件"""
    
    def __init__(self, event_time: datetime, market: str = "cn_stock", 
                 adapter_id: str = "default"):
        super().__init__(EventType.MORNING_START, event_time, market, adapter_id)


class MorningEndEvent(MarketEvent):
    """上午收盘事件"""
    
    def __init__(self, event_time: datetime, market: str = "cn_stock", 
                 adapter_id: str = "default"):
        super().__init__(EventType.MORNING_END, event_time, market, adapter_id)


class AfternoonStartEvent(MarketEvent):
    """下午开盘事件"""
    
    def __init__(self, event_time: datetime, market: str = "cn_stock", 
                 adapter_id: str = "default"):
        super().__init__(EventType.AFTERNOON_START, event_time, market, adapter_id)


class AfternoonEndEvent(MarketEvent):
    """下午收盘事件"""
    
    def __init__(self, event_time: datetime, market: str = "cn_stock", 
                 adapter_id: str = "default"):
        super().__init__(EventType.AFTERNOON_END, event_time, market, adapter_id)


class AfterMarketEvent(MarketEvent):
    """盘后事件"""
    
    def __init__(self, event_time: datetime, market: str = "cn_stock", 
                 adapter_id: str = "default"):
        super().__init__(EventType.AFTER_MARKET, event_time, market, adapter_id)


class DailyBarClosedEvent(MarketEvent):
    """日线数据完成事件"""
    
    def __init__(self, event_time: datetime, market: str = "cn_stock", 
                 adapter_id: str = "default", bar_data: Optional[Dict] = None):
        data = {"bar_data": bar_data} if bar_data else {}
        super().__init__(EventType.DAILY_BAR_CLOSED, event_time, market, adapter_id, data)


class MinuteBarEvent(MarketEvent):
    """分钟Bar事件"""
    
    def __init__(self, event_time: datetime, market: str = "cn_stock", 
                 adapter_id: str = "default", bar_data: Optional[Dict] = None,
                 session: str = "morning"):
        """
        Args:
            session: 'morning' 或 'afternoon'
        """
        event_type = EventType.MINUTE_BAR_MORNING if session == "morning" else EventType.MINUTE_BAR_AFTERNOON
        data = {"bar_data": bar_data, "session": session} if bar_data else {"session": session}
        super().__init__(event_type, event_time, market, adapter_id, data) 