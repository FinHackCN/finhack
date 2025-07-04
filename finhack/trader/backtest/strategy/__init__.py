"""
策略接口模块

提供策略基类和策略管理器，支持事件驱动的策略回调机制
"""

from .base_strategy import BaseStrategy
from .strategy_manager import StrategyManager

__all__ = [
    'BaseStrategy',
    'StrategyManager'
] 