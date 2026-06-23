"""
全球加密货币现货市场模块

提供加密货币市场的适配器、规则版本和计算器
"""

from .global_cryptospot_adapter import GlobalCryptoSpotMarketAdapter

try:
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
except ImportError:
    pass

# 向后兼容别名
GlobalCryptoSpotAdapter = GlobalCryptoSpotMarketAdapter

__all__ = [
    'GlobalCryptoSpotMarketAdapter',
    'GlobalCryptoSpotAdapter',
]
