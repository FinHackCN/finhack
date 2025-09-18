"""
撮合引擎模块

包含：
- BaseMatcher: 基础撮合器
- LimitMatcher: 限价单撮合器
- MarketMatcher: 市价单撮合器
- Slippage: 滑点计算
"""

from .base_matcher import BaseMatcher
from .limit_matcher import LimitMatcher
from .market_matcher import MarketMatcher
from .slippage import Slippage

__all__ = [
    "BaseMatcher",
    "LimitMatcher",
    "MarketMatcher", 
    "Slippage"
] 