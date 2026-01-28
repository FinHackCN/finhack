"""
美股市场交易规则版本配置

定义了美股市场交易规则的历史版本，支持按时间查询规则变更
"""

from datetime import date
from typing import Dict, List, Any, Optional

# 美股交易所分类
US_EXCHANGES = {
    'NYSE': 'nyse',      # 纽约证券交易所
    'NASDAQ': 'nasdaq',  # 纳斯达克
    'AMEX': 'amex',      # 美国证券交易所
}

# 美股代码前缀与交易所映射
US_CODE_PREFIX_MAP = {
    # 无统一前缀规则，需根据代码判断
    # 通常NYSE代码多为1-3个字母
    # NASDAQ代码多为4-5个字母
}

# ============================================================================
# 交易时段规则版本
# ============================================================================
TRADING_SCHEDULE_VERSIONS = [
    {
        'version': 'v1952',
        'effective_date': date(1952, 1, 1),
        'description': '现代美股交易时间确立',
        'rules': {
            'pre_market_start': '04:00',   # 盘前交易开始
            'pre_market_end': '09:30',     # 盘前交易结束
            'regular_start': '09:30',      # 正常交易开始
            'regular_end': '16:00',        # 正常交易结束
            'post_market_start': '16:00',  # 盘后交易开始
            'post_market_end': '20:00',    # 盘后交易结束
        }
    },
]

# ============================================================================
# 涨跌幅限制规则版本（熔断机制）
# ============================================================================
PRICE_LIMIT_VERSIONS = [
    {
        'version': 'v19871019',
        'effective_date': date(1987, 10, 19),
        'description': '黑色星期一后引入熔断机制',
        'rules': {
            'circuit_breaker_level1': 0.07,   # S&P 500下跌7%
            'circuit_breaker_level2': 0.13,   # S&P 500下跌13%
            'circuit_breaker_level3': 0.20,   # S&P 500下跌20%
            'level1_pause': 15,               # 7%触发暂停15分钟
            'level2_pause': 15,               # 13%触发暂停15分钟
            'level3_pause': 'rest_of_day',    # 20%触发当日剩余时间暂停
        }
    },
    {
        'version': 'v20130204',
        'effective_date': date(2013, 2, 4),
        'description': '调整熔断阈值',
        'rules': {
            'circuit_breaker_level1': 0.07,
            'circuit_breaker_level2': 0.13,
            'circuit_breaker_level3': 0.20,
            'level1_pause': 15,
            'level2_pause': 15,
            'level3_pause': 'rest_of_day',
            'individual_security_pause': 0.10,  # 个股涨跌10%暂停5分钟
            'individual_pause_duration': 5,
        }
    },
    {
        'version': 'v20200421',
        'effective_date': date(2020, 4, 21),
        'description': '调整个股熔断规则至涨跌5%',
        'rules': {
            'individual_security_pause': 0.05,  # 降为5%
            'individual_pause_duration': 5,
        }
    },
]

# ============================================================================
# 最小交易单位规则版本（支持零碎股）
# ============================================================================
LOT_SIZE_VERSIONS = [
    {
        'version': 'v_default',
        'effective_date': date(1900, 1, 1),
        'description': '美股支持1股和零碎股交易',
        'rules': {
            'min_buy': 1,                     # 最小买入1股
            'buy_increment': 1,               # 买入增量1股
            'sell_allow_fractional': True,    # 允许零碎股卖出
            'sell_min': 0.0001,               # 支持极小零碎股
            'fractional_shares': True,        # 支持零碎股
            'fractional_increment': 0.0001,   # 零碎股最小增量
        }
    },
]

# ============================================================================
# 卖空规则版本（提价规则 Rule 201）
# ============================================================================
SHORT_SALE_RULES_VERSIONS = [
    {
        'version': 'v20100222',
        'effective_date': date(2010, 2, 22),
        'description': '引入提价规则（Rule 201）',
        'rules': {
            'short_sale_rule': 'rule_201',           # 提价规则
            'circuit_breaker_threshold': -0.10,     # 日跌幅超过10%触发限制
            'restriction_duration': 5,               # 限制持续天数
            'require_higher_price': True,            # 卖空价需高于最优买价
        }
    },
]

# ============================================================================
# PDT规则（日内回转交易限制）
# ============================================================================
PDT_RULE_VERSIONS = [
    {
        'version': 'v200109',
        'effective_date': date(2001, 9, 1),
        'description': 'PDT规则实施',
        'rules': {
            'account_threshold': 25000,              # 账户净值低于$25,000
            'max_day_trades': 3,                     # 5个交易日内最多3次日内回转
            'rolling_window': 5,                     # 5个交易日滚动窗口
            'cash_account_unlimited': False,         # 现金账户不受限制
        }
    },
]

# ============================================================================
# 结算周期规则版本
# ============================================================================
SETTLEMENT_VERSIONS = [
    {
        'version': 'v_default_t2',
        'effective_date': date(1900, 1, 1),
        'description': 'T+2结算周期',
        'rules': {
            'settlement_cycle': 'T+2',
        }
    },
    {
        'version': 'v20240528',
        'effective_date': date(2024, 5, 28),
        'description': '转为T+1结算周期',
        'rules': {
            'settlement_cycle': 'T+1',
        }
    },
]

# ============================================================================
# 价格单位和精度规则
# ============================================================================
PRICE_PRECISION_VERSIONS = [
    {
        'version': 'v2001',
        'effective_date': date(2001, 1, 29),
        'description': '全面采用十进制定价',
        'rules': {
            'tick_size': 0.01,                    # 最小价格单位1美分
            'fractional_pricing': False,          # 不再支持分数定价
            'rounding_method': 'round',           # 四舍五入
            'rounding_decimals': 2,               # 保留2位小数
        }
    },
    {
        'version': 'v2022_fractional',
        'effective_date': date(2022, 1, 1),
        'description': '部分券商支持零碎股定价',
        'rules': {
            'tick_size': 0.01,
            'fractional_pricing': True,           # 支持零碎股定价
            'fractional_decimals': 4,             # 零碎股4位小数
            'rounding_method': 'round',
            'rounding_decimals': 2,
        }
    },
]

# ============================================================================
# 手续费规则版本
# ============================================================================
COMMISSION_VERSIONS = [
    {
        'version': 'v2018',
        'effective_date': date(2018, 1, 1),
        'description': '零佣金时代',
        'rules': {
            'stock': {
                'commission_rate': 0.0,           # 零佣金
                'min_commission': 0.0,
                'per_share_fee': 0.0,             # 无每股费用
                'sec_fee': 0.0000131,             # SEC费用（卖出）约$0.0131/股
                'trading_activity_fee': 0.000119, # 交易活动费（卖出）
                'stamp_tax_buy': 0.0,
                'stamp_tax_sell': 0.0,            # 美股无印花税
            },
            'etf': {
                'commission_rate': 0.0,
                'min_commission': 0.0,
                'per_share_fee': 0.0,
                'sec_fee': 0.0000131,
                'trading_activity_fee': 0.000119,
                'stamp_tax_buy': 0.0,
                'stamp_tax_sell': 0.0,
            },
            'option': {
                'commission_rate': 0.0,
                'per_contract_fee': 0.65,         # 每张合约费用
                'min_commission': 0.0,
            }
        }
    },
]

# ============================================================================
# 辅助函数
# ============================================================================

def get_exchange_from_symbol(symbol: str) -> str:
    """从代码中提取交易所

    Args:
        symbol: 美股代码，如 'AAPL', 'TSLA', 'BRK.A'

    Returns:
        交易所: 'nyse', 'nasdaq', 'amex'
    """
    # 简化处理：根据代码特征判断
    # 实际应用中需要维护交易所代码列表
    code = symbol.split('.')[0].upper()

    # NYSE常见股票（示例）
    nyse_stocks = {
        'BRK.A', 'BRK.B', 'WMT', 'JPM', 'V', 'PG', 'JNJ', 'XOM',
        'UNH', 'HD', 'MA', 'BAC', 'PFE', 'KO', 'PEP', 'CVX',
        # 需要维护完整列表
    }

    # AMEX常见股票
    amex_stocks = {
        # 需要维护完整列表
    }

    if code in nyse_stocks:
        return 'nyse'
    elif code in amex_stocks:
        return 'amex'
    else:
        # 默认为NASDAQ（科技股多在NASDAQ）
        return 'nasdaq'


def get_stock_type(symbol: str) -> str:
    """根据代码获取股票类型

    Args:
        symbol: 股票代码

    Returns:
        股票类型: 'common', 'etf', 'adr', 'reit'
    """
    code = symbol.split('.')[0].upper()

    # ETF代码判断（通常包含特定标识）
    etf_patterns = ['SPY', 'QQQ', 'IWM', 'VTI', 'VOO', 'GLD', 'SLV', 'TLT']
    if any(pattern in code for pattern in etf_patterns):
        return 'etf'

    # ADR判断（通常代码后带Y或其他标识）
    if len(code) > 4 or '.' in code:
        return 'adr'

    # REIT判断（需要维护列表）
    reit_list = {'O', 'CCI', 'AMT', 'PLD', 'EQIX'}
    if code in reit_list:
        return 'reit'

    # 默认为普通股
    return 'common'


def is_market_hours(dt, session: str = 'regular') -> bool:
    """判断是否在指定交易时段

    Args:
        dt: 日期时间
        session: 时段类型 ('pre_market', 'regular', 'post_market')

    Returns:
        是否在交易时段
    """
    hour = dt.hour
    minute = dt.minute
    time_val = hour + minute / 60

    if session == 'pre_market':
        return 4.0 <= time_val < 9.5
    elif session == 'regular':
        return 9.5 <= time_val < 16.0
    elif session == 'post_market':
        return 16.0 <= time_val < 20.0
    else:
        return False


# ============================================================================
# 规则查询辅助类
# ============================================================================
class RuleVersion:
    """规则版本查询类"""

    @staticmethod
    def get_applicable_rule(versions: List[Dict], query_date: date,
                           exchange: str = None) -> Optional[Dict]:
        """获取指定日期适用的规则版本

        Args:
            versions: 规则版本列表
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
        merged_rules = {}
        for v in applicable_versions:
            if 'rules' in v:
                # 只添加尚未添加的键（保持最新值）
                rules = v['rules']
                if isinstance(rules, dict):
                    for key, value in rules.items():
                        if key not in merged_rules:
                            merged_rules[key] = value

        return merged_rules if merged_rules else None

    @staticmethod
    def get_rule_change_dates(versions: List[Dict]) -> List[date]:
        """获取规则变更日期列表

        Args:
            versions: 规则版本列表

        Returns:
            规则变更日期列表
        """
        dates = set()
        for version in versions:
            dates.add(version['effective_date'])

        return sorted(dates)


if __name__ == '__main__':
    # 测试代码
    test_symbols = [
        'AAPL',   # NASDAQ
        'TSLA',   # NASDAQ
        'WMT',    # NYSE
    ]

    for symbol in test_symbols:
        print(f"{symbol}: exchange={get_exchange_from_symbol(symbol)}, "
              f"type={get_stock_type(symbol)}")
