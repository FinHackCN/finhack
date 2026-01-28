"""
中国ETF市场交易规则版本配置

定义了ETF交易规则的历史版本，支持按时间查询规则变更
"""

from datetime import date
from typing import Dict, List, Any, Optional

# ETF板块分类
ETF_BOARD_TYPES = {
    'MAIN_BOARD': 'main_board',      # 主板
    'STAR_MARKET': 'star_market',    # 科创板
    'GEM': 'gem',                    # 创业板
    'BSE': 'bse',                    # 北交所
}

# ETF代码前缀与板块映射
ETF_CODE_PREFIX_MAP = {
    # 沪市主板 (5xx000, 5xx100, 56xxxx)
    '510': 'main_board',
    '511': 'main_board',
    '512': 'main_board',
    '515': 'main_board',
    '516': 'main_board',
    '518': 'main_board',
    '560': 'main_board',

    # 科创板 (588xxx)
    '588': 'star_market',

    # 深市主板 (159xxx) - 159000-159199为主板ETF
    '1590': 'main_board',
    '1591': 'main_board',

    # 创业板ETF (159900-159999)
    '1599': 'gem',

    # 创业板 (15xxxx, 30xxxx, 36xxxx)
    '150': 'gem',
    '300': 'gem',
    '360': 'gem',

    # 跨境ETF (5130-5131开头的通常是跨境ETF)
    '5130': 'cross_border',
    '5131': 'cross_border',

    # 货币ETF
    '5118': 'money',
    '5119': 'money',
    '159001': 'money',
}

# ============================================================================
# 交易时段规则版本
# ============================================================================
TRADING_SCHEDULE_VERSIONS = [
    {
        'version': 'v19910703',
        'effective_date': date(1991, 7, 3),
        'description': '深交所引入集合竞价',
        'rules': {
            'pre_opening_start': '09:15',
            'pre_opening_end': '09:25',
            'pre_opening_cancel_end': '09:20',  # 9:20后不可撤单
            'morning_start': '09:30',
            'morning_end': '11:30',
            'afternoon_start': '13:00',
            'afternoon_end': '15:00',
        }
    },
    {
        'version': 'v19931101',
        'effective_date': date(1993, 11, 1),
        'description': '沪深交易所延长早盘至11:30',
        'rules': {
            'morning_end': '11:30',  # 从11:00延长至11:30
        }
    },
    {
        'version': 'v20060701_szse',
        'effective_date': date(2006, 7, 1),
        'description': '深交所新增收盘集合竞价',
        'rules': {
            'closing_auction_start': '14:57',
            'closing_auction_end': '15:00',
        },
        'scope': ['szse']  # 仅深市
    },
    {
        'version': 'v20180820_sse',
        'effective_date': date(2018, 8, 20),
        'description': '上交所全面实施收盘集合竞价',
        'rules': {
            'closing_auction_start': '14:57',
            'closing_auction_end': '15:00',
        },
        'scope': ['sse']  # 仅沪市
    },
    {
        'version': 'v20190722_star',
        'effective_date': date(2019, 7, 22),
        'description': '科创板推出盘后定价交易',
        'rules': {
            'post_trading_start': '15:05',
            'post_trading_end': '15:30',
        },
        'scope': ['star_market']  # 仅科创板
    },
    {
        'version': 'v20200824_gem',
        'effective_date': date(2020, 8, 24),
        'description': '创业板实施盘后定价交易',
        'rules': {
            'post_trading_start': '15:05',
            'post_trading_end': '15:30',
        },
        'scope': ['gem']  # 仅创业板
    },
]

# ============================================================================
# 涨跌幅限制规则版本
# ============================================================================
PRICE_LIMIT_VERSIONS = [
    {
        'version': 'v19961216',
        'effective_date': date(1996, 12, 16),
        'description': '实施涨跌幅限制',
        'rules': {
            'main_board': {
                'daily_limit': 0.10,  # 10%
                'new_stock_first_day_limit': 0.44,  # 44%
                'st_limit': 0.05,  # 5%
            }
        }
    },
    {
        'version': 'v20091030_gem',
        'effective_date': date(2009, 10, 30),
        'description': '创业板开板',
        'rules': {
            'gem': {
                'daily_limit': 0.10,  # 注册制前10%
                'new_stock_first_day_limit': 0.44,
                'st_limit': 0.05,
            }
        }
    },
    {
        'version': 'v20190722_star',
        'effective_date': date(2019, 7, 22),
        'description': '科创板开市，20%涨跌停',
        'rules': {
            'star_market': {
                'daily_limit': 0.20,  # 20%
                'new_stock_no_limit_days': 5,  # 前5日无限制
                'st_limit': 0.20,  # ST股也20%
            }
        }
    },
    {
        'version': 'v20200824_gem',
        'effective_date': date(2020, 8, 24),
        'description': '创业板注册制改革，20%涨跌停',
        'rules': {
            'gem': {
                'daily_limit': 0.20,  # 改为20%
                'new_stock_no_limit_days': 5,  # 前5日无限制
                'st_limit': 0.20,  # ST股也20%
            }
        }
    },
    {
        'version': 'v20211115_bse',
        'effective_date': date(2021, 11, 15),
        'description': '北交所开市，30%涨跌停',
        'rules': {
            'bse': {
                'daily_limit': 0.30,  # 30%
                'new_stock_no_limit_days': 0,  # 首日无限制
                'st_limit': 0.30,  # 北交所无ST标识
            }
        }
    },
    {
        'version': 'v20230220_main',
        'effective_date': date(2023, 2, 20),
        'description': '主板全面注册制改革',
        'rules': {
            'main_board': {
                'daily_limit': 0.10,  # 维持10%
                'new_stock_no_limit_days': 5,  # 前5日无限制（之前是首日44%）
                'st_limit': 0.05,  # ST股5%
            }
        }
    },
]

# ============================================================================
# 最小交易单位规则版本
# ============================================================================
LOT_SIZE_VERSIONS = [
    {
        'version': 'v19901219',
        'effective_date': date(1990, 12, 19),
        'description': '沪深主板100股整数倍',
        'rules': {
            'main_board': {
                'min_buy': 100,
                'buy_increment': 100,  # 买入需100股整数倍
                'sell_allow_fractional': True,  # 卖出可零股
                'sell_min': 1,
            }
        }
    },
    {
        'version': 'v2003_zero_sell',
        'effective_date': date(2003, 1, 1),
        'description': '主板允许零股卖出',
        'rules': {
            'main_board': {
                'sell_allow_fractional': True,
            }
        }
    },
    {
        'version': 'v20190722_star',
        'effective_date': date(2019, 7, 22),
        'description': '科创板200股起买，1股递增',
        'rules': {
            'star_market': {
                'min_buy': 200,
                'buy_increment': 1,  # 1股递增
                'sell_allow_fractional': True,
                'sell_min': 1,
                'sell_below_min_buy': True,  # 卖出可低于200股
            }
        }
    },
    {
        'version': 'v20200824_gem',
        'effective_date': date(2020, 8, 24),
        'description': '创业板注册制后，100股起买，1股递增',
        'rules': {
            'gem': {
                'min_buy': 100,
                'buy_increment': 1,  # 1股递增
                'sell_allow_fractional': True,
                'sell_min': 1,
                'sell_below_min_buy': True,  # 卖出可低于100股
            }
        }
    },
    {
        'version': 'v20211115_bse',
        'effective_date': date(2021, 11, 15),
        'description': '北交所100股起买，1股递增',
        'rules': {
            'bse': {
                'min_buy': 100,
                'buy_increment': 1,
                'sell_allow_fractional': True,
                'sell_min': 1,
            }
        }
    },
]

# ============================================================================
# 价格笼子机制规则版本
# ============================================================================
PRICE_CAGE_VERSIONS = [
    {
        'version': 'v20200824_star_gem',
        'effective_date': date(2020, 8, 24),
        'description': '科创板/创业板引入价格笼子',
        'rules': {
            'star_market': {
                'enabled': True,
                'buy_type': 'percentage_or_units',  # 取两者较大值
                'buy_limit_pct': 1.02,  # 卖一价×102%
                'buy_limit_units': 10,  # 或卖一价+10个最小单位
                'sell_type': 'percentage_or_units',  # 取两者较小值
                'sell_limit_pct': 0.98,  # 买一价×98%
                'sell_limit_units': 10,  # 或买一价-10个最小单位
            },
            'gem': {
                'enabled': True,
                'buy_type': 'percentage_or_units',
                'buy_limit_pct': 1.02,
                'buy_limit_units': 10,
                'sell_type': 'percentage_or_units',
                'sell_limit_pct': 0.98,
                'sell_limit_units': 10,
            }
        }
    },
    {
        'version': 'v20211115_bse',
        'effective_date': date(2021, 11, 15),
        'description': '北交所价格笼子',
        'rules': {
            'bse': {
                'enabled': True,
                'buy_type': 'percentage_or_units',
                'buy_limit_pct': 1.05,  # 105%
                'buy_limit_units': 50,  # 或+50个最小单位
                'sell_type': 'percentage_or_units',
                'sell_limit_pct': 0.95,  # 95%
                'sell_limit_units': 50,  # 或-50个最小单位
            }
        }
    },
    {
        'version': 'v20230220_main',
        'effective_date': date(2023, 2, 20),
        'description': '主板注册制引入价格笼子',
        'rules': {
            'main_board': {
                'enabled': True,
                'buy_type': 'min',  # 取两者较小值
                'buy_limit_pct': 1.02,
                'buy_limit_abs': 0.1,  # 或+0.1元
                'sell_type': 'max',  # 取两者较大值
                'sell_limit_pct': 0.98,
                'sell_limit_abs': 0.1,  # 或-0.1元
            }
        }
    },
]

# ============================================================================
# 涨跌停价计算规则版本
# ============================================================================
LIMIT_PRICE_CALCULATION_VERSIONS = [
    {
        'version': 'v_default',
        'effective_date': date(1990, 1, 1),
        'description': '默认规则',
        'rules': {
            'main_board': {
                'tick_size': 0.01,
                'tick_size_threshold': None,  # 无阈值
                'rounding_method': 'round',  # 四舍五入
                'rounding_decimals': 2,
            }
        }
    },
    {
        'version': 'v20190722_star',
        'effective_date': date(2019, 7, 22),
        'description': '科创板最小价格单位分段',
        'rules': {
            'star_market': {
                'tick_size': 0.01,
                'tick_size_threshold': 200.0,  # 股价≥200元时tick_size为0.10
                'tick_size_high': 0.10,
                'rounding_method': 'ceil',  # 向上进位
            }
        }
    },
    {
        'version': 'v20200824_gem',
        'effective_date': date(2020, 8, 24),
        'description': '创业板最小价格单位分段（同科创板）',
        'rules': {
            'gem': {
                'tick_size': 0.01,
                'tick_size_threshold': 200.0,
                'tick_size_high': 0.10,
                'rounding_method': 'ceil',
            }
        }
    },
    {
        'version': 'v20211115_bse',
        'effective_date': date(2021, 11, 15),
        'description': '北交所直接截断',
        'rules': {
            'bse': {
                'tick_size': 0.01,
                'rounding_method': 'truncate',  # 直接截断
                'rounding_decimals': 2,
            }
        }
    },
]

# ============================================================================
# 手续费规则版本
# ============================================================================
COMMISSION_VERSIONS = [
    {
        'version': 'v2003',
        'effective_date': date(2003, 1, 1),
        'description': '佣金浮动制，最低5元',
        'rules': {
            'stock_etf': {
                'commission_rate': 0.0003,  # 万三
                'min_commission': 5.0,
                'stamp_tax_buy': 0.0,
                'stamp_tax_sell': 0.001,  # 千分之一
            },
            'bond_etf': {
                'commission_rate': 0.0003,
                'min_commission': 5.0,
                'stamp_tax_buy': 0.0,
                'stamp_tax_sell': 0.0,  # 债券ETF免印花税
            },
            'cross_border_etf': {
                'commission_rate': 0.0003,
                'min_commission': 5.0,
                'stamp_tax_buy': 0.0,
                'stamp_tax_sell': 0.0,  # 跨境ETF免印花税
            },
            'money_etf': {
                'commission_rate': 0.0,
                'min_commission': 0.0,
                'stamp_tax_buy': 0.0,
                'stamp_tax_sell': 0.0,
            }
        }
    },
]

# ============================================================================
# ETF类型分类辅助函数
# ============================================================================
def get_etf_board_type(symbol: str) -> str:
    """根据ETF代码获取板块类型

    Args:
        symbol: ETF代码，如 '510300.SH' 或 '159915.SZ'

    Returns:
        板块类型: 'main_board', 'star_market', 'gem', 'bse', 'cross_border', 'money'
    """
    # 提取代码
    code = symbol.split('.')[0]

    # 特殊代码处理（完整代码匹配）
    if code in ETF_CODE_PREFIX_MAP:
        return ETF_CODE_PREFIX_MAP[code]

    # 按前缀长度从长到短匹配，确保更精确的前缀优先匹配
    sorted_prefixes = sorted(ETF_CODE_PREFIX_MAP.items(), key=lambda x: -len(x[0]))

    for prefix, board in sorted_prefixes:
        if code.startswith(prefix):
            return board

    # 默认返回主板
    return 'main_board'


def get_etf_type(symbol: str) -> str:
    """根据ETF代码获取ETF类型

    Args:
        symbol: ETF代码

    Returns:
        ETF类型: 'stock', 'bond', 'cross_border', 'money'
    """
    code = symbol.split('.')[0]

    # 货币ETF - 常见货币ETF代码
    money_etf_codes = [
        '511880', '511880', '511870', '511860', '511850',  # 银华日利等
        '159001',  # 易方达货币
    ]
    money_etf_prefixes = ['5118', '5119']

    # 检查是否为货币ETF
    if code in money_etf_codes:
        return 'money'
    if any(code.startswith(p) for p in money_etf_prefixes):
        return 'money'

    # 债券ETF - 常见债券ETF代码
    bond_etf_prefixes = ['5110', '5111', '5112', '5113', '5115']
    if any(code.startswith(p) for p in bond_etf_prefixes):
        return 'bond'

    # 跨境ETF（513开头，且不是国内指数）
    # 这里简化处理：5130-5131通常是跨境ETF
    if code.startswith('5130') or code.startswith('5131'):
        return 'cross_border'

    # 默认为股票ETF
    return 'stock'


def get_exchange_from_symbol(symbol: str) -> str:
    """从代码中提取交易所

    Args:
        symbol: ETF代码，如 '510300.SH'

    Returns:
        交易所: 'sse'(上交所), 'szse'(深交所), 'bse'(北交所)
    """
    if '.' in symbol:
        suffix = symbol.split('.')[1].upper()
        exchange_map = {
            'SH': 'sse',
            'SZ': 'szse',
            'BJ': 'bse',
        }
        return exchange_map.get(suffix, 'sse')

    # 根据代码前缀判断
    code = symbol.split('.')[0]
    if code.startswith('5') or code.startswith('6'):
        return 'sse'
    elif code.startswith('0') or code.startswith('1') or code.startswith('3'):
        return 'szse'
    elif code.startswith('8') or code.startswith('4'):
        return 'bse'

    return 'sse'


# ============================================================================
# 规则查询辅助类
# ============================================================================
class RuleVersion:
    """规则版本查询类"""

    @staticmethod
    def get_applicable_rule(versions: List[Dict], board: str,
                           query_date: date, exchange: str = None) -> Optional[Dict]:
        """获取指定日期适用的规则版本

        Args:
            versions: 规则版本列表
            board: 板块类型
            query_date: 查询日期
            exchange: 交易所（用于scope过滤）

        Returns:
            适用的规则配置，如果未找到则返回None
        """
        applicable_versions = []

        for version in versions:
            # 检查生效日期
            if version['effective_date'] > query_date:
                continue

            # 检查板块是否存在规则
            if 'rules' in version:
                # 全局规则或特定板块规则
                if board in version['rules'] or 'all' in version['rules']:
                    # 检查scope（交易所限制）
                    if 'scope' in version:
                        if exchange is None or exchange not in version['scope']:
                            continue

                    applicable_versions.append(version)

        if not applicable_versions:
            return None

        # 返回最新生效的版本（按effective_date降序排序）
        applicable_versions.sort(key=lambda x: x['effective_date'], reverse=True)

        # 合并所有适用的规则（从最新到最旧）
        # 只保留最新版本的每个键的值
        merged_rules = {}
        for v in applicable_versions:
            if board in v['rules']:
                # 只添加尚未添加的键（保持最新值）
                for key, value in v['rules'][board].items():
                    if key not in merged_rules:
                        merged_rules[key] = value
            elif 'all' in v['rules']:
                for key, value in v['rules']['all'].items():
                    if key not in merged_rules:
                        merged_rules[key] = value

        return merged_rules

    @staticmethod
    def get_rule_change_dates(versions: List[Dict], board: str = None) -> List[date]:
        """获取规则变更日期列表

        Args:
            versions: 规则版本列表
            board: 板块类型（可选）

        Returns:
            规则变更日期列表
        """
        dates = set()
        for version in versions:
            if 'scope' not in version:  # 不包含scope限制的
                dates.add(version['effective_date'])
            elif board is None:  # 或者没有指定板块
                dates.add(version['effective_date'])

        return sorted(dates)


if __name__ == '__main__':
    # 测试代码
    test_symbols = [
        '510300.SH',  # 沪深300ETF - 主板
        '588000.SH',  # 科创50ETF - 科创板
        '159915.SZ',  # 创业板ETF - 创业板
    ]

    for symbol in test_symbols:
        print(f"{symbol}: board={get_etf_board_type(symbol)}, "
              f"type={get_etf_type(symbol)}, "
              f"exchange={get_exchange_from_symbol(symbol)}")
