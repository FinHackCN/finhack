#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
市场级配置单点表。

把"每个市场的日历来源/是否复权/基准指数/行业/tz"集中声明，供
factorAnalyzer（行业分发）、trader adapter（日历）、basics 指标层
（基准指数注入）按市场分发，消除散落的 cn_stock 硬编码。

benchmark 取值：
  - 具体指数/标的代码 → basics 指标从 factors/matrix 取该 code 的 close/open 作为 banchmarkindex*
  - 'skip' → 该市场禁用 alpha191 里依赖 benchmark 的公式（#75/#149/#181/#182）
"""
MARKET_CONFIG = {
    'cn_stock':           {'calendar': 'csv',   'adj': True,  'benchmark': '000300.SH', 'industry': 'astock', 'tz': 'Asia/Shanghai'},
    'hk_stock':           {'calendar': 'csv',   'adj': False, 'benchmark': 'HSI',       'industry': 'skip',   'tz': 'Asia/Hong_Kong'},
    'us_stock':           {'calendar': 'csv',   'adj': False, 'benchmark': 'SPX',       'industry': 'skip',   'tz': 'America/New_York'},
    'global_cryptospot':  {'calendar': '7x24',  'adj': False, 'benchmark': 'BTC',       'industry': 'skip',   'tz': 'UTC'},
    'global_cryptoswap':  {'calendar': '7x24',  'adj': False, 'benchmark': 'BTC',       'industry': 'skip',   'tz': 'UTC'},
    'cn_future':          {'calendar': 'csv',   'adj': False, 'benchmark': 'skip',      'industry': 'skip',   'tz': 'Asia/Shanghai'},
}

_DEFAULT = {'calendar': 'csv', 'adj': False, 'benchmark': 'skip', 'industry': 'skip', 'tz': 'UTC'}


def get_market_config(market: str) -> dict:
    """返回指定市场的配置，未知市场返回安全默认（不复权/无基准/跳过行业）。"""
    return dict(MARKET_CONFIG.get(market, _DEFAULT))


def supports_adj(market: str) -> bool:
    return bool(get_market_config(market).get('adj'))


def benchmark_code(market: str):
    """返回基准指数代码；'skip' 表示该市场不适用基准（相关 alpha 公式应禁用）。"""
    return get_market_config(market).get('benchmark')


def industry_mode(market: str) -> str:
    """行业中性化模式：'astock'=用 AStock 行业数据，'skip'=跳过。"""
    return get_market_config(market).get('industry', 'skip')
