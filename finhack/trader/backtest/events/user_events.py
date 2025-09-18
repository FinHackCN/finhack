"""
用户事件实现
"""

from datetime import datetime
from typing import Any, Dict, Optional, Callable

from .base_event import BaseEvent, EventType


class UserEvent(BaseEvent):
    """用户事件基类"""
    
    def __init__(self, event_type: EventType, event_time: datetime,
                 callback: Callable = None, market: str = "cn_stock", 
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
            
            # 优先使用指定的回调函数
            if self.callback:
                self.callback(context)
            else:
                # 如果没有指定回调函数，尝试从策略中查找对应的方法
                if context.strategy:
                    method_name = self._get_strategy_method_name()
                    if hasattr(context.strategy, method_name):
                        method = getattr(context.strategy, method_name)
                        if callable(method):
                            if context.logger:
                                context.logger.debug(f"调用策略方法: {method_name}")
                            method(context)
                        else:
                            if context.logger:
                                context.logger.warning(f"策略方法不可调用: {method_name}")
                    else:
                        if context.logger:
                            context.logger.debug(f"策略中未找到方法: {method_name}")
            
            self.processed = True
            return True
            
        except Exception as e:
            if context.logger:
                context.logger.error(f"处理用户事件失败: {self.event_type.value}, 错误: {str(e)}")
            return False
    
    def _get_strategy_method_name(self) -> str:
        """获取策略方法名称"""
        # 将事件类型转换为策略方法名称
        # 例如：USER_DAILY -> run_daily
        event_name = self.event_type.value.lower()
        if event_name.startswith('user_'):
            event_name = event_name[5:]  # 去掉'user_'前缀
        return f"run_{event_name}"


class UserDailyEvent(UserEvent):
    """用户每日事件"""
    
    def __init__(self, event_time: datetime, callback: Callable = None,
                 market: str = "cn_stock", adapter_id: str = "default"):
        super().__init__(EventType.USER_DAILY, event_time, callback, market, adapter_id)


class UserHourlyEvent(UserEvent):
    """用户每小时事件"""
    
    def __init__(self, event_time: datetime, callback: Callable = None,
                 market: str = "cn_stock", adapter_id: str = "default"):
        super().__init__(EventType.USER_HOURLY, event_time, callback, market, adapter_id)


class UserMinutelyEvent(UserEvent):
    """用户每分钟事件"""
    
    def __init__(self, event_time: datetime, callback: Callable = None,
                 market: str = "cn_stock", adapter_id: str = "default"):
        super().__init__(EventType.USER_MINUTELY, event_time, callback, market, adapter_id)


class UserWeeklyEvent(UserEvent):
    """用户每周事件"""
    
    def __init__(self, event_time: datetime, callback: Callable = None,
                 market: str = "cn_stock", adapter_id: str = "default"):
        super().__init__(EventType.USER_WEEKLY, event_time, callback, market, adapter_id)


class UserMonthlyEvent(UserEvent):
    """用户每月事件"""
    
    def __init__(self, event_time: datetime, callback: Callable = None,
                 market: str = "cn_stock", adapter_id: str = "default"):
        super().__init__(EventType.USER_MONTHLY, event_time, callback, market, adapter_id)


class UserIntervalEvent(UserEvent):
    """用户自定义间隔事件"""
    
    def __init__(self, event_time: datetime, callback: Callable = None,
                 interval: int = 60, market: str = "cn_stock", adapter_id: str = "default"):
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