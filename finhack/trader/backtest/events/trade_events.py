"""
交易事件实现
"""

from datetime import datetime
from typing import Any, Dict, Optional

from .base_event import BaseEvent, EventType


class TradeEvent(BaseEvent):
    """交易事件基类"""
    
    def __init__(self, event_type: EventType, event_time: datetime,
                 market: str = "cn_stock", adapter_id: str = "default",
                 data: Optional[Dict[str, Any]] = None):
        super().__init__(event_type, event_time, market, adapter_id, data)
    
    def process(self, context) -> bool:
        """
        处理交易事件
        
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
            
            # 优先让交易中心处理交易事件
            if hasattr(context.trade_center, handler_name):
                handler = getattr(context.trade_center, handler_name)
                result = handler(context, self)
                if not result:
                    return False
            
            # 如果策略有对应的处理方法，也调用
            if context.strategy and hasattr(context.strategy, handler_name):
                handler = getattr(context.strategy, handler_name)
                handler(context, self)
            
            self.processed = True
            return True
            
        except Exception as e:
            context.logger.error(f"处理交易事件失败: {self.event_type.value}, 错误: {str(e)}")
            return False


class OrderSubmissionEvent(TradeEvent):
    """订单提交事件"""
    
    def __init__(self, event_time: datetime, order_data: Dict[str, Any],
                 market: str = "cn_stock", adapter_id: str = "default"):
        data = {"order": order_data}
        super().__init__(EventType.ORDER_SUBMISSION, event_time, market, adapter_id, data)
    
    @property
    def order_data(self) -> Dict[str, Any]:
        """获取订单数据"""
        return self.data.get("order", {})


class OrderFillEvent(TradeEvent):
    """订单成交事件"""
    
    def __init__(self, event_time: datetime, trade_data: Dict[str, Any],
                 market: str = "cn_stock", adapter_id: str = "default"):
        data = {"trade": trade_data}
        super().__init__(EventType.ORDER_FILL, event_time, market, adapter_id, data)
    
    @property
    def trade_data(self) -> Dict[str, Any]:
        """获取成交数据"""
        return self.data.get("trade", {})


class OrderCancellationEvent(TradeEvent):
    """订单取消事件"""
    
    def __init__(self, event_time: datetime, order_id: str,
                 market: str = "cn_stock", adapter_id: str = "default"):
        data = {"order_id": order_id}
        super().__init__(EventType.ORDER_CANCELLATION, event_time, market, adapter_id, data)
    
    @property
    def order_id(self) -> str:
        """获取订单ID"""
        return self.data.get("order_id", "")


class PositionUpdateEvent(TradeEvent):
    """持仓更新事件"""
    
    def __init__(self, event_time: datetime, position_data: Dict[str, Any],
                 market: str = "cn_stock", adapter_id: str = "default"):
        data = {"position": position_data}
        super().__init__(EventType.POSITION_UPDATE, event_time, market, adapter_id, data)
    
    @property
    def position_data(self) -> Dict[str, Any]:
        """获取持仓数据"""
        return self.data.get("position", {}) 