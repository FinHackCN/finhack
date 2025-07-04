"""
新一代回测框架模块

基于事件驱动的回测系统，包含四大核心组件：
- EventCenter: 事件中心，负责事件的生成、调度和分发
- TradeCenter: 交易中心，负责账户、持仓、订单、成交管理
- DataCenter: 数据中心，负责行情数据、因子数据的获取和缓存
- Strategy: 策略接口，提供策略与回测引擎的交互接口

作者: Assistant
创建日期: 2024
"""

from .backtest_trader import BacktestTrader
from .core.event_center import EventCenter
from .core.trade_center import TradeCenter
from .core.data_center import DataCenter
from .core.context import Context
from .strategy.base_strategy import BaseStrategy
from .strategy.strategy_manager import StrategyManager
from .performance.performance_analyzer import PerformanceAnalyzer
from .performance.report_generator import ReportGenerator

__all__ = [
    'BacktestTrader',
    'EventCenter', 
    'TradeCenter',
    'DataCenter',
    'Context',
    'BaseStrategy',
    'StrategyManager',
    'PerformanceAnalyzer',
    'ReportGenerator'
] 