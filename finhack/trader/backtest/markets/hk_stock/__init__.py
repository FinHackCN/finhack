"""
港股市场适配器模块

提供香港股票市场的交易规则、事件生成和计算功能
"""

from .hk_stock_adapter import HKStockMarketAdapter
from .hk_trading_rules_versions import (
    get_hk_stock_board_type,
    get_exchange_from_symbol,
    RuleVersion,
)
from .hk_calculator import (
    HKStockPriceCalculator,
    HKStockCommissionCalculator,
    HKStockLotSizeCalculator,
    HKVCMValidator,
)

__all__ = [
    'HKStockMarketAdapter',
    'get_hk_stock_board_type',
    'get_exchange_from_symbol',
    'RuleVersion',
    'HKStockPriceCalculator',
    'HKStockCommissionCalculator',
    'HKStockLotSizeCalculator',
    'HKVCMValidator',
]
