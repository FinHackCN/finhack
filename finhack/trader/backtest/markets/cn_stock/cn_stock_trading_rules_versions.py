# -*- coding: utf-8 -*-
"""
中国A股交易规则版本定义（涨跌停）

板块识别 + 涨跌停版本规则。A股票市场的板块涨跌停演变历史与 ETF 完全一致
（主板10%、科创板20%[2019-07-22]、创业板10%→20%[2020-08-24]、北交所30%[2021-11-15]、
ST5%、新股N日无限制），因此**直接复用** `cn_fund.etf_trading_rules_versions` 中的
`PRICE_LIMIT_VERSIONS` / `LIMIT_PRICE_CALCULATION_VERSIONS` / `RuleVersion` /
`get_exchange_from_symbol`，避免重复维护两份相同的A股规则。

本模块只新增 A股**个股**的板块识别（`get_stock_board_type`）——ETF 的代码前缀与个股不同，
因此无法直接复用 `get_etf_board_type`。
"""

from typing import Optional

# 复用 cn_fund 的A股版本规则（单一真相源）
try:
    from ..cn_fund.etf_trading_rules_versions import (
        PRICE_LIMIT_VERSIONS,
        LIMIT_PRICE_CALCULATION_VERSIONS,
        RuleVersion,
        get_exchange_from_symbol,
    )
except ImportError:  # 单元测试 / 直接运行（cn_fund 在 sys.path，按裸模块导入以绕开包 __init__）
    from etf_trading_rules_versions import (  # type: ignore
        PRICE_LIMIT_VERSIONS,
        LIMIT_PRICE_CALCULATION_VERSIONS,
        RuleVersion,
        get_exchange_from_symbol,
    )


# A股个股代码前缀 → 板块（key 与 PRICE_LIMIT_VERSIONS 的板块 key 完全一致）
# 注意：匹配时按前缀长度从长到短，避免 '8' 抢先于 '83'。
STOCK_CODE_PREFIX_MAP = {
    # 科创板（上交所）
    '688': 'star_market',
    # 创业板（深交所）
    '300': 'gem',
    '301': 'gem',
    # 沪市主板 600/601/603/605
    '60': 'main_board',
    # 深市主板 000/001/002/003
    '00': 'main_board',
    '001': 'main_board',
    '002': 'main_board',
    '003': 'main_board',
    # 北交所（原新三板精选层 + 北交所）430/830/870/920 等
    '43': 'bse',
    '83': 'bse',
    '87': 'bse',
    '92': 'bse',
    '8': 'bse',
    '4': 'bse',
}


def get_stock_board_type(symbol: str, category: Optional[str] = None) -> str:
    """根据股票代码/分类识别板块

    Args:
        symbol: 股票代码，如 '600519.SH' / '300750.SZ' / '688981.SH' / '830799.BJ'
        category: 标的列表里的分类列（如 '主板'/'创业板'），权威优先

    Returns:
        板块 key: 'main_board' | 'star_market' | 'gem' | 'bse'
        （与 PRICE_LIMIT_VERSIONS 的 key 一致）
    """
    # 1) 优先用权威的分类列
    if category:
        cat = str(category)
        if '创业' in cat:
            return 'gem'
        if '科创' in cat:
            return 'star_market'
        if '北交' in cat:
            return 'bse'
        if '主板' in cat:
            return 'main_board'

    # 2) .BJ 后缀直接判北交所
    if isinstance(symbol, str) and symbol.upper().endswith('.BJ'):
        return 'bse'

    # 3) 代码前缀匹配（最长优先）
    code = (symbol or '').split('.')[0]
    for prefix in sorted(STOCK_CODE_PREFIX_MAP, key=len, reverse=True):
        if code.startswith(prefix):
            return STOCK_CODE_PREFIX_MAP[prefix]

    # 默认主板
    return 'main_board'


def is_st_stock(name: Optional[str]) -> bool:
    """根据股票名称判断是否为 ST/*ST 股

    ST 标识在名称里（不在代码里），形如 'ST平安'、'*ST海马'、'S*STxxx'。
    """
    if not name:
        return False
    n = str(name).upper().strip()
    # 覆盖 ST / *ST / SST / S*ST 等前缀形式
    return n.startswith(('*ST', 'ST', 'SST', 'S*ST'))


def _norm_date8(s) -> str:
    """把各种日期形态归一成 YYYYMMDD 字符串（取前8位数字）"""
    if s is None:
        return ''
    import re as _re
    return _re.sub(r'\D', '', str(s))[:8]


def resolve_name_as_of(code: str, query_date, namechange_df) -> Optional[str]:
    """从名称变更历史取 (code, query_date) 当日生效的名字

    Args:
        code: 股票代码（不含后缀，如 '000001'）
        query_date: 查询日期
        namechange_df: 名称变更 DataFrame，需含 code/name/start_date 列

    Returns:
        当日生效的名字；无记录返回 None（调用方应回退到静态快照）
    """
    if namechange_df is None or len(namechange_df) == 0:
        return None
    if 'code' not in namechange_df.columns or 'start_date' not in namechange_df.columns:
        return None
    sub = namechange_df[namechange_df['code'].astype(str).str.startswith(code)]
    if sub.empty:
        return None
    q = query_date.strftime('%Y%m%d') if hasattr(query_date, 'strftime') else _norm_date8(query_date)
    starts = sub['start_date'].map(_norm_date8)
    mask = (starts != '') & (starts <= q)
    sub = sub.assign(_s=starts)[mask]
    if sub.empty:
        return None
    return str(sub.sort_values('_s').iloc[-1].get('name', ''))


def st_status_from_namechange(code: str, query_date, namechange_df) -> Optional[bool]:
    """按日精确判定 ST：从 namechange 取当日名字再判 ST

    Returns:
        True/False（有 namechange 记录）；None（无记录，调用方回退静态快照）
    """
    name = resolve_name_as_of(code, query_date, namechange_df)
    if not name:
        return None
    return is_st_stock(name)
