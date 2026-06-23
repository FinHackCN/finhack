"""
市场适配器模块

支持不同市场的事件生成和交易规则
"""

from .cn_stock.cn_stock_adapter import CnStockMarketAdapter
from .cn_fund.cn_fund_adapter import CnFundMarketAdapter
from .cn_future.cn_future_adapter import CnFutureMarketAdapter
from .global_cryptospot.global_cryptospot_adapter import GlobalCryptoSpotMarketAdapter
from .global_cryptoswap.global_cryptoswap_adapter import GlobalCryptoSwapMarketAdapter
from .hk_stock.hk_stock_adapter import HKStockMarketAdapter
from .us_stock.us_stock_adapter import USStockMarketAdapter

# 市场适配器映射
MARKET_ADAPTERS = {
    'cn_stock': CnStockMarketAdapter,
    'cn_fund': CnFundMarketAdapter,
    'cn_index': CnStockMarketAdapter,  # 指数使用相同的市场规则
    'cn_cb': CnStockMarketAdapter,     # 可转债使用相同的市场规则
    'cn_future': CnFutureMarketAdapter,
    'global_cryptospot': GlobalCryptoSpotMarketAdapter,
    'global_cryptoswap': GlobalCryptoSwapMarketAdapter,
    'hk_stock': HKStockMarketAdapter,
    'us_stock': USStockMarketAdapter,
}

# 为了向后兼容，保留旧的类名
CnStockAdapter = CnStockMarketAdapter
CnFutureAdapter = CnFutureMarketAdapter

__all__ = [
    'CnStockMarketAdapter',
    'CnFundMarketAdapter',
    'CnFutureMarketAdapter',
    'GlobalCryptoSpotMarketAdapter',
    'GlobalCryptoSwapMarketAdapter',
    'HKStockMarketAdapter',
    'USStockMarketAdapter',
    'CnStockAdapter',  # 向后兼容
    'CnFutureAdapter',  # 向后兼容
    'MARKET_ADAPTERS'
]
