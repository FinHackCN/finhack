"""
回测规则引擎模块
支持通用规则、市场特定规则和用户自定义规则
"""

from .base_rule import BaseRule, RuleType, RuleResult
from .rule_engine import RuleEngine
from .rule_factory import RuleFactory
from .rule_manager import RuleManager

# 通用规则
from .common.trading_rules import *
from .common.market_rules import *
from .common.risk_rules import *

# 市场特定规则
from .markets.cn_stock_rules import *
from .markets.hk_stock_rules import *
from .markets.us_stock_rules import *

__all__ = [
    'BaseRule', 'RuleType', 'RuleResult',
    'RuleEngine', 'RuleFactory', 'RuleManager'
] 