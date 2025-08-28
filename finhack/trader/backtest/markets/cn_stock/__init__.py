"""
中国股票市场适配器

包含：
- CnStockAdapter: 股票市场适配器
- events_config: 股票市场事件配置
- trading_rules: 股票交易规则
"""

from .cn_stock_adapter import CnStockAdapter
from .events_config import EVENTS_CONFIG
from .trading_rules import TRADING_RULES

__all__ = [
    "CnStockAdapter",
    "EVENTS_CONFIG", 
    "TRADING_RULES"
] 