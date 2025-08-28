"""
FinHack回测模块

基于事件驱动的量化交易回测框架，包含四大核心组件：
- EventCenter: 事件中心
- TradeCenter: 交易中心  
- DataCenter: 数据中心
- Strategy: 策略执行器
"""

from .backtest_trader import BacktestTrader
from .engine.backtest_engine import BacktestEngine
from .engine.context_manager import ContextManager
from .core.event_center import EventCenter
from .core.trade_center import TradeCenter
from .core.data_center import DataCenter
from .core.strategy_executor import StrategyExecutor

__version__ = "1.0.0"
__author__ = "FinHack Team"

__all__ = [
    "BacktestTrader",
    "BacktestEngine",
    "ContextManager", 
    "EventCenter",
    "TradeCenter",
    "DataCenter",
    "StrategyExecutor"
] 