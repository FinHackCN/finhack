"""
全球外汇市场适配器

包含：
- ForexAdapter: 外汇市场适配器
- events_config: 外汇市场事件配置
- trading_rules: 外汇交易规则
"""

from .forex_adapter import ForexAdapter
from .events_config import EVENTS_CONFIG
from .trading_rules import TRADING_RULES

__all__ = [
    "ForexAdapter",
    "EVENTS_CONFIG",
    "TRADING_RULES"
] 