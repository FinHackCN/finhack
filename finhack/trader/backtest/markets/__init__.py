"""
市场适配器模块

支持不同市场的事件生成和交易规则
"""

from .cn_stock_adapter import CnStockMarketAdapter

# 市场适配器映射
MARKET_ADAPTERS = {
    'cn_stock': CnStockMarketAdapter,
    'cn_fund': CnStockMarketAdapter,  # 基金使用相同的市场规则
    'cn_index': CnStockMarketAdapter,  # 指数使用相同的市场规则
    'cn_cb': CnStockMarketAdapter,     # 可转债使用相同的市场规则
}

__all__ = [
    'CnStockMarketAdapter',
    'MARKET_ADAPTERS'
] 