"""
用户事件实现
"""

from datetime import datetime
from typing import Any, Dict, Optional, Callable

from .base_event import BaseEvent, EventType


class UserEvent(BaseEvent):
    """用户事件基类"""
    
    def __init__(self, event_type: EventType, event_time: datetime,
                 callback: Callable, market: str = "cn_stock", 
                 adapter_id: str = "default", data: Optional[Dict[str, Any]] = None):
        super().__init__(event_type, event_time, market, adapter_id, data)
        self.callback = callback
    
    def process(self, context) -> bool:
        """
        处理用户事件
        
        Args:
            context: 回测上下文
            
        Returns:
            bool: 是否处理成功
        """
        try:
            # 更新当前时间
            context.current_dt = self.event_time
            
            # 调用用户回调函数
            if self.callback:
                self.callback(context)
            
            self.processed = True
            return True
            
        except Exception as e:
            context.logger.error(f"处理用户事件失败: {self.event_type.value}, 错误: {str(e)}")
            return False


class UserDailyEvent(UserEvent):
    """用户每日事件"""
    
    def __init__(self, event_time: datetime, callback: Callable,
                 market: str = "cn_stock", adapter_id: str = "default"):
        super().__init__(EventType.USER_DAILY, event_time, callback, market, adapter_id)


class UserHourlyEvent(UserEvent):
    """用户每小时事件"""
    
    def __init__(self, event_time: datetime, callback: Callable,
                 market: str = "cn_stock", adapter_id: str = "default"):
        super().__init__(EventType.USER_HOURLY, event_time, callback, market, adapter_id)


class UserMinutelyEvent(UserEvent):
    """用户每分钟事件"""
    
    def __init__(self, event_time: datetime, callback: Callable,
                 market: str = "cn_stock", adapter_id: str = "default"):
        super().__init__(EventType.USER_MINUTELY, event_time, callback, market, adapter_id)


class UserWeeklyEvent(UserEvent):
    """用户每周事件"""
    
    def __init__(self, event_time: datetime, callback: Callable,
                 market: str = "cn_stock", adapter_id: str = "default"):
        super().__init__(EventType.USER_WEEKLY, event_time, callback, market, adapter_id)


class UserMonthlyEvent(UserEvent):
    """用户每月事件"""
    
    def __init__(self, event_time: datetime, callback: Callable,
                 market: str = "cn_stock", adapter_id: str = "default"):
        super().__init__(EventType.USER_MONTHLY, event_time, callback, market, adapter_id)


class UserIntervalEvent(UserEvent):
    """用户自定义间隔事件"""
    
    def __init__(self, event_time: datetime, callback: Callable,
                 interval: int, market: str = "cn_stock", adapter_id: str = "default"):
        """
        Args:
            interval: 间隔时间（秒）
        """
        data = {"interval": interval}
        super().__init__(EventType.USER_INTERVAL, event_time, callback, market, adapter_id, data)
    
    @property
    def interval(self) -> int:
        """获取间隔时间"""
        return self.data.get("interval", 0) 