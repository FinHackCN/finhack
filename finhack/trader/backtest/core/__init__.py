"""
回测核心组件模块

包含四大核心组件：
- EventCenter: 事件中心
- TradeCenter: 交易中心  
- DataCenter: 数据中心
- StrategyExecutor: 策略执行器
"""

from .event_center import EventCenter
from .trade_center import TradeCenter
from .data_center import DataCenter
from .strategy_executor import StrategyExecutor

__all__ = [
    "EventCenter",
    "TradeCenter", 
    "DataCenter",
    "StrategyExecutor"
] 