#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
finhack 市场配置单一来源（唯一 source of truth）。

吸收并替代了原本散落三处的市场配置：
  - library/data.py:MarketConfig（freq_support / adj / continuous）
  - trader/backtest/backtest_trader.py:MARKET_DEFAULTS（currency / account_type / fees）
  - 原 market_context（calendar / benchmark / industry / tz）
+ 新增衍生品/交易规则字段（is_derivatives / margin_ratio / t_plus / funding / ...）+ adapter 类名。

**目标**：新加市场 = 在 MARKET_CONFIG 加一行 + 写一个 adapter 类，零改引擎/数据/分析代码。
所有市场属性只读这里；freq 感知只经 factorManager.loadFactorsAuto。

account_type 用字符串（'CASH'/'FUTURES'/'CRYPTO'），backtest 层用 AccountTypeEnum(account_type) 转换。
adapter 字段是 trader/backtest/markets/ 下 adapter 类名（由 MARKET_ADAPTERS 反查）。
"""
from typing import Dict, List, Optional


def _fees(open_tax=0.0, close_tax=0.0, open_commission=0.0003, close_commission=0.0003,
          min_commission=5.0, slip_value=0.001):
    return {'open_tax': open_tax, 'close_tax': close_tax,
            'open_commission': open_commission, 'close_commission': close_commission,
            'min_commission': min_commission, 'slip_value': slip_value}


MARKET_CONFIG: Dict[str, dict] = {
    # —— 中国 A 股 ——
    'cn_stock': {
        'tz': 'Asia/Shanghai', 'currency': 'CNY', 'account_type': 'CASH',
        'calendar': 'csv', 'continuous': False, 'adj': True, 'freq_support': ['1d', '1m'],
        'fees': _fees(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5.0, slip_value=0.001),
        't_plus': 1, 'lot_size': 100,
        'is_derivatives': False, 'margin_ratio': 1.0, 'has_funding_rate': False, 'funding': None,
        'has_delivery': False, 'tick_size_source': 'none',
        'benchmark': '000300.SH', 'industry': 'astock', 'adapter': 'CnStockMarketAdapter',
    },
    # —— 中国基金/ETF ——
    'cn_fund': {
        'tz': 'Asia/Shanghai', 'currency': 'CNY', 'account_type': 'CASH',
        'calendar': 'csv', 'continuous': False, 'adj': True, 'freq_support': ['1d', '1m'],
        'fees': _fees(close_tax=0.0, min_commission=5.0, slip_value=0.001),
        't_plus': 1, 'lot_size': 100,
        'is_derivatives': False, 'margin_ratio': 1.0, 'has_funding_rate': False, 'funding': None,
        'has_delivery': False, 'tick_size_source': 'none',
        'benchmark': 'skip', 'industry': 'skip', 'adapter': 'CnFundMarketAdapter',
    },
    # —— 中国指数（沿用 cn_stock 规则）——
    'cn_index': {
        'tz': 'Asia/Shanghai', 'currency': 'CNY', 'account_type': 'CASH',
        'calendar': 'csv', 'continuous': False, 'adj': False, 'freq_support': ['1d', '1m'],
        'fees': _fees(),
        't_plus': 1, 'lot_size': 1,
        'is_derivatives': False, 'margin_ratio': 1.0, 'has_funding_rate': False, 'funding': None,
        'has_delivery': False, 'tick_size_source': 'none',
        'benchmark': 'skip', 'industry': 'skip', 'adapter': 'CnStockMarketAdapter',
    },
    # —— 中国可转债（沿用 cn_stock 规则）——
    'cn_cb': {
        'tz': 'Asia/Shanghai', 'currency': 'CNY', 'account_type': 'CASH',
        'calendar': 'csv', 'continuous': False, 'adj': False, 'freq_support': ['1d', '1m'],
        'fees': _fees(close_tax=0.0, min_commission=5.0, slip_value=0.001),
        't_plus': 0, 'lot_size': 10,
        'is_derivatives': False, 'margin_ratio': 1.0, 'has_funding_rate': False, 'funding': None,
        'has_delivery': False, 'tick_size_source': 'none',
        'benchmark': 'skip', 'industry': 'skip', 'adapter': 'CnStockMarketAdapter',
    },
    # —— 中国期货 ——
    'cn_future': {
        'tz': 'Asia/Shanghai', 'currency': 'CNY', 'account_type': 'FUTURES',
        'calendar': 'csv', 'continuous': False, 'adj': False, 'freq_support': ['1d', '1m'],
        'fees': _fees(open_commission=0.0001, close_commission=0.0001, min_commission=5.0, slip_value=0.0005),
        't_plus': 0, 'lot_size': 1,
        'is_derivatives': True, 'margin_ratio': 0.12, 'has_funding_rate': False, 'funding': None,
        'has_delivery': True, 'tick_size_source': 'cn_future',
        'benchmark': 'skip', 'industry': 'skip', 'adapter': 'CnFutureMarketAdapter',
    },
    # —— 港股 ——
    'hk_stock': {
        'tz': 'Asia/Hong_Kong', 'currency': 'HKD', 'account_type': 'CASH',
        'calendar': 'csv', 'continuous': False, 'adj': False, 'freq_support': ['1d'],
        'fees': _fees(close_tax=0.0013, open_commission=0.0003, close_commission=0.0003,
                      min_commission=5.0, slip_value=0.001),
        't_plus': 0, 'lot_size': 100,
        'is_derivatives': False, 'margin_ratio': 1.0, 'has_funding_rate': False, 'funding': None,
        'has_delivery': False, 'tick_size_source': 'none',
        'benchmark': 'HSI', 'industry': 'skip', 'adapter': 'HKStockMarketAdapter',
    },
    # —— 美股 ——
    'us_stock': {
        'tz': 'America/New_York', 'currency': 'USD', 'account_type': 'CASH',
        'calendar': 'csv', 'continuous': False, 'adj': False, 'freq_support': ['1d'],
        'fees': _fees(close_tax=0.0, open_commission=0.0003, close_commission=0.0003,
                      min_commission=5.0, slip_value=0.001),
        't_plus': 0, 'lot_size': 1,
        'is_derivatives': False, 'margin_ratio': 1.0, 'has_funding_rate': False, 'funding': None,
        'has_delivery': False, 'tick_size_source': 'none',
        'benchmark': 'SPX', 'industry': 'skip', 'adapter': 'USStockMarketAdapter',
    },
    # —— 加密现货 ——
    'global_cryptospot': {
        'tz': 'UTC', 'currency': 'USD', 'account_type': 'CRYPTO',
        'calendar': '7x24', 'continuous': True, 'adj': False, 'freq_support': ['1d', '1m'],
        'fees': _fees(open_commission=0.001, close_commission=0.001, min_commission=0.0, slip_value=0.0005),
        't_plus': 0, 'lot_size': 1,
        'is_derivatives': False, 'margin_ratio': 1.0, 'has_funding_rate': False, 'funding': None,
        'has_delivery': False, 'tick_size_source': 'none',
        'benchmark': 'BTC', 'industry': 'skip', 'adapter': 'GlobalCryptoSpotMarketAdapter',
    },
    # —— 加密永续合约 ——
    'global_cryptoswap': {
        'tz': 'UTC', 'currency': 'USD', 'account_type': 'CRYPTO',
        'calendar': '7x24', 'continuous': True, 'adj': False, 'freq_support': ['1d', '1m'],
        'fees': _fees(open_commission=0.0004, close_commission=0.0004, min_commission=0.0, slip_value=0.0005),
        't_plus': 0, 'lot_size': 1,
        'is_derivatives': True, 'margin_ratio': 0.10, 'has_funding_rate': True,
        'funding': {'mode': 'fixed', 'rate': 0.0001, 'interval_hours': 8},
        'has_delivery': False, 'tick_size_source': 'none',
        'benchmark': 'BTC', 'industry': 'skip', 'adapter': 'GlobalCryptoSwapMarketAdapter',
    },
}

# 安全默认（未知市场）
_DEFAULT = {
    'tz': 'UTC', 'currency': 'USD', 'account_type': 'CASH',
    'calendar': 'csv', 'continuous': False, 'adj': False, 'freq_support': ['1d'],
    'fees': _fees(), 't_plus': 0, 'lot_size': 1,
    'is_derivatives': False, 'margin_ratio': 1.0, 'has_funding_rate': False, 'funding': None,
    'has_delivery': False, 'tick_size_source': 'none',
    'benchmark': 'skip', 'industry': 'skip', 'adapter': None,
}


def get_market_config(market: str) -> dict:
    """返回指定市场的完整配置；未知市场返回安全默认。"""
    return dict(MARKET_CONFIG.get(market, _DEFAULT))


def list_markets() -> List[str]:
    """枚举所有已配置市场。"""
    return list(MARKET_CONFIG.keys())


def is_freq_supported(market: str, freq: str) -> bool:
    return freq in get_market_config(market).get('freq_support', [])


def is_continuous(market: str) -> bool:
    return bool(get_market_config(market).get('continuous', False))


def supports_adj(market: str) -> bool:
    return bool(get_market_config(market).get('adj', False))


def get_fees(market: str) -> dict:
    """返回费率字典（open_tax/close_tax/open_commission/close_commission/min_commission/slip_value）。"""
    return dict(get_market_config(market).get('fees', _fees()))


def get_currency(market: str) -> str:
    return get_market_config(market).get('currency', 'USD')


def get_account_type(market: str) -> str:
    """返回账户类型字符串（'CASH'/'FUTURES'/'CRYPTO'）；backtest 层用 AccountTypeEnum() 转。"""
    return get_market_config(market).get('account_type', 'CASH')


def get_funding_config(market: str) -> Optional[dict]:
    return get_market_config(market).get('funding', None)


def benchmark_code(market: str):
    """基准指数代码；'skip' 表示该市场不适用基准。"""
    return get_market_config(market).get('benchmark', 'skip')


def industry_mode(market: str) -> str:
    """行业中性化模式：'astock'=用 AStock 行业数据，'skip'=跳过。"""
    return get_market_config(market).get('industry', 'skip')


def get_adapter_name(market: str) -> Optional[str]:
    """返回该市场 adapter 类名（供 trader/backtest/markets 反查 MARKET_ADAPTERS）。"""
    return get_market_config(market).get('adapter')
