"""
全球加密货币现货市场模块

提供加密货币市场的适配器、规则版本和计算器
"""

from .global_cryptospot_adapter import GlobalCryptoSpotAdapter
from .crypto_trading_rules_versions import (
    RuleVersion,
    get_crypto_exchange,
    get_crypto_pair_type,
    get_base_currency,
    get_quote_currency,
)
from .crypto_calculator import (
    CryptoPriceCalculator,
    CryptoLotSizeCalculator,
    CryptoCommissionCalculator,
    CryptoFundingRateCalculator,
    CryptoLeverageCalculator,
    CryptoLiquidationCalculator,
)

__all__ = [
    # 适配器
    'GlobalCryptoSpotAdapter',

    # 规则版本
    'RuleVersion',
    'get_crypto_exchange',
    'get_crypto_pair_type',
    'get_base_currency',
    'get_quote_currency',

    # 计算器
    'CryptoPriceCalculator',
    'CryptoLotSizeCalculator',
    'CryptoCommissionCalculator',
    'CryptoFundingRateCalculator',
    'CryptoLeverageCalculator',
    'CryptoLiquidationCalculator',
]
