"""
中国期货市场适配器

包含：
- CnFutureAdapter: 期货市场适配器
- events_config: 期货市场事件配置
- trading_rules: 期货交易规则
"""

from .cn_future_adapter import CnFutureAdapter
from .events_config import EVENTS_CONFIG
from .trading_rules import TRADING_RULES

__all__ = [
    "CnFutureAdapter",
    "EVENTS_CONFIG",
    "TRADING_RULES"
] 