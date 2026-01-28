"""
加密货币市场交易规则版本配置

定义加密货币市场的交易规则历史版本，支持不同交易所的规则查询
"""

from datetime import date
from typing import Dict, List, Any, Optional

# 加密货币交易所类型
CRYPTO_EXCHANGES = {
    'BINANCE': 'binance',
    'OKX': 'okx',
    'BYBIT': 'bybit',
    'BITGET': 'bitget',
    'HUOBI': 'huobi',
    'COINBASE': 'coinbase',
    'KRAKEN': 'kraken',
}

# 加密货币交易对类型
CRYPTO_PAIR_TYPES = {
    'SPOT': 'spot',           # 现货
    'FUTURES': 'futures',     # 永续合约
    'MARGIN': 'margin',       # 杠杆
}

# 加密货币代码前缀与交易所映射
CRYPTO_EXCHANGE_PREFIX_MAP = {
    # Binance
    'BTCUSDT': 'binance',
    'ETHUSDT': 'binance',
    'BNBUSDT': 'binance',

    # OKX
    'BTC-USDT': 'okx',
    'ETH-USDT': 'okx',

    # Bybit
    'BTCPERP': 'bybit',
    'ETHPERP': 'bybit',
}

# ============================================================================
# 交易时段规则版本
# ============================================================================
TRADING_SCHEDULE_VERSIONS = [
    {
        'version': 'v2009_genesis',
        'effective_date': date(2009, 1, 3),
        'description': '比特币创世，加密货币24/7交易开始',
        'rules': {
            'all': {
                'trading_hours': '24/7',
                'trading_days': 'all_days',  # 包括周末和节假日
                'maintenance_windows': [],   # 无维护窗口
            }
        }
    },
]

# ============================================================================
# 涨跌幅限制规则版本
# ============================================================================
PRICE_LIMIT_VERSIONS = [
    {
        'version': 'v_default',
        'effective_date': date(2009, 1, 3),
        'description': '加密货币无涨跌幅限制',
        'rules': {
            'all': {
                'daily_limit': None,  # 无涨跌幅限制
                'has_circuit_breaker': False,  # 无熔断机制
            }
        }
    },
    {
        'version': 'v2021_binance_limits',
        'effective_date': date(2021, 5, 1),
        'description': '部分交易所引入临时限制（极端行情保护）',
        'rules': {
            'binance': {
                'daily_limit': None,
                'has_circuit_breaker': True,
                'circuit_breaker_threshold': {
                    'single_minute': 0.05,  # 单分钟涨跌5%触发
                    'five_minute': 0.10,    # 5分钟涨跌10%触发
                },
            },
            'okx': {
                'daily_limit': None,
                'has_circuit_breaker': True,
                'circuit_breaker_threshold': {
                    'single_minute': 0.05,
                },
            }
        }
    },
]

# ============================================================================
# 最小交易单位规则版本
# ============================================================================
LOT_SIZE_VERSIONS = [
    {
        'version': 'v_default',
        'effective_date': date(2009, 1, 3),
        'description': '默认最小交易单位',
        'rules': {
            'all': {
                'min_notional': 10.0,  # 最小名义价值（USDT）
                'min_quantity': 0.00001,  # 最小数量（因币种而异）
                'quantity_increment': 0.00001,  # 数量增量
                'price_increment': 0.01,  # 价格增量（USDT）
                'allow_fractional': True,  # 支持小数交易
            }
        }
    },
    {
        'version': 'v2023_binance_precision',
        'effective_date': date(2023, 1, 1),
        'description': 'Binance提高精度要求',
        'rules': {
            'binance': {
                'min_notional': 5.0,  # 降低至5 USDT
                'min_quantity': 0.00001,
                'quantity_increment': 0.00001,
                'price_increment': 0.01,
                'allow_fractional': True,
            },
            'okx': {
                'min_notional': 1.0,  # 更低门槛
                'min_quantity': 0.00001,
                'quantity_increment': 0.00001,
                'price_increment': 0.01,
                'allow_fractional': True,
            }
        }
    },
]

# ============================================================================
# 手续费规则版本
# ============================================================================
COMMISSION_VERSIONS = [
    {
        'version': 'v_default',
        'effective_date': date(2009, 1, 3),
        'description': '默认手续费率',
        'rules': {
            'all': {
                'maker_fee': 0.001,   # 0.1% 挂单手续费
                'taker_fee': 0.001,   # 0.1% 吃单手续费
                'min_commission': 0.0,  # 无最低手续费
            }
        }
    },
    {
        'version': 'v2021_binance_vip',
        'effective_date': date(2021, 1, 1),
        'description': 'VIP等级手续费（简化版）',
        'rules': {
            'binance': {
                'maker_fee': 0.001,
                'taker_fee': 0.001,
                'min_commission': 0.0,
                'vip_discounts': {
                    'vip0': {'maker': 0.001, 'taker': 0.001},
                    'vip1': {'maker': 0.0009, 'taker': 0.001},
                    'vip2': {'maker': 0.0008, 'taker': 0.001},
                    'vip3': {'maker': 0.0007, 'taker': 0.0009},
                }
            },
            'okx': {
                'maker_fee': 0.0008,  # OKX默认更低
                'taker_fee': 0.001,
                'min_commission': 0.0,
            },
            'bybit': {
                'maker_fee': -0.00025,  # 挂单返佣
                'taker_fee': 0.00075,
                'min_commission': 0.0,
            }
        }
    },
]

# ============================================================================
# 杠杆规则版本
# ============================================================================
LEVERAGE_VERSIONS = [
    {
        'version': 'v_default',
        'effective_date': date(2009, 1, 3),
        'description': '默认杠杆设置',
        'rules': {
            'all': {
                'max_leverage': 125,  # 最高125倍（部分交易所）
                'default_leverage': 10,
                'leverage_increment': 1,  # 杠杆增量
            }
        }
    },
    {
        'version': 'v2021_risk_control',
        'effective_date': date(2021, 7, 1),
        'description': '加强风险控制，限制高杠杆',
        'rules': {
            'binance': {
                'max_leverage': 125,
                'default_leverage': 20,
                'leverage_increment': 1,
                'position_limit_tiers': {
                    '1-20x': 1000000,  # USDT
                    '21-50x': 500000,
                    '51-75x': 200000,
                    '76-125x': 50000,
                }
            },
            'okx': {
                'max_leverage': 125,
                'default_leverage': 10,
                'leverage_increment': 1,
            },
            'bybit': {
                'max_leverage': 100,
                'default_leverage': 10,
                'leverage_increment': 1,
            }
        }
    },
]

# ============================================================================
# 资金费率规则版本
# ============================================================================
FUNDING_RATE_VERSIONS = [
    {
        'version': 'v2016_bitmex',
        'effective_date': date(2016, 1, 1),
        'description': 'BitMEX引入资金费率机制',
        'rules': {
            'all': {
                'funding_interval_hours': 8,  # 每8小时收取一次
                'funding_rate_calculation': 'clamp',  # 限制在±0.05%之间
                'max_funding_rate': 0.0005,  # 0.05%
                'min_funding_rate': -0.0005,
            }
        }
    },
    {
        'version': 'v2020_binance_futures',
        'effective_date': date(2020, 1, 1),
        'description': 'Binance永续合约资金费率',
        'rules': {
            'binance': {
                'funding_interval_hours': 8,
                'funding_rate_calculation': 'clamp',
                'max_funding_rate': 0.0005,
                'min_funding_rate': -0.0005,
                'avg_premium_window': 5,  # 5分钟移动平均
            },
            'okx': {
                'funding_interval_hours': 8,
                'funding_rate_calculation': 'smooth',
                'max_funding_rate': 0.0075,  # 0.75%
                'min_funding_rate': -0.0075,
            },
            'bybit': {
                'funding_interval_hours': 8,
                'funding_rate_calculation': 'clamp',
                'max_funding_rate': 0.005,  # 0.5%
                'min_funding_rate': -0.005,
            }
        }
    },
    {
        'version': 'v2023_hourly_funding',
        'effective_date': date(2023, 1, 1),
        'description': '部分交易所改为每小时资金费率',
        'rules': {
            'binance': {
                'funding_interval_hours': 8,  # Binance保持8小时
                'funding_rate_calculation': 'clamp',
                'max_funding_rate': 0.0005,
                'min_funding_rate': -0.0005,
            },
            'okx': {
                'funding_interval_hours': 1,  # 改为1小时
                'funding_rate_calculation': 'smooth',
                'max_funding_rate': 0.0075,
                'min_funding_rate': -0.0075,
            },
            'bybit': {
                'funding_interval_hours': 1,  # 改为1小时
                'funding_rate_calculation': 'clamp',
                'max_funding_rate': 0.005,
                'min_funding_rate': -0.005,
            }
        }
    },
]

# ============================================================================
# 强平规则版本
# ============================================================================
LIQUIDATION_VERSIONS = [
    {
        'version': 'v_default',
        'effective_date': date(2009, 1, 3),
        'description': '默认强平规则',
        'rules': {
            'all': {
                'liquidation_method': 'mark_price',  # 使用标记价格强平
                'maintenance_margin_rate': 0.005,  # 维持保证金率 0.5%
                'margin_call_threshold': 0.01,  # 追保阈值 1%
                'bankruptcy_price_offset': 0.005,  # 破产价格偏移
            }
        }
    },
    {
        'version': 'v2021_binance_tier',
        'effective_date': date(2021, 1, 1),
        'description': '分级保证金制度',
        'rules': {
            'binance': {
                'liquidation_method': 'mark_price',
                'maintenance_margin_rate': 0.005,
                'margin_call_threshold': 0.01,
                'use_tiered_margin': True,  # 使用分级保证金
                'margin_tiers': [
                    {'position_max': 50000, 'mmr': 0.005},
                    {'position_max': 250000, 'mmr': 0.01},
                    {'position_max': 1000000, 'mmr': 0.02},
                    {'position_max': 5000000, 'mmr': 0.05},
                    {'position_max': float('inf'), 'mmr': 0.10},
                ]
            },
            'okx': {
                'liquidation_method': 'mark_price',
                'maintenance_margin_rate': 0.01,
                'margin_call_threshold': 0.01,
                'use_tiered_margin': True,
                'margin_tiers': [
                    {'position_max': 10000, 'mmr': 0.01},
                    {'position_max': 100000, 'mmr': 0.02},
                    {'position_max': 1000000, 'mmr': 0.05},
                    {'position_max': float('inf'), 'mmr': 0.10},
                ]
            }
        }
    },
]

# ============================================================================
# 价格精度规则版本
# ============================================================================
PRICE_PRECISION_VERSIONS = [
    {
        'version': 'v_default',
        'effective_date': date(2009, 1, 3),
        'description': '默认价格精度',
        'rules': {
            'all': {
                'tick_size': 0.01,  # 默认0.01 USDT
                'price_decimals': 2,
                'quantity_decimals': 8,
                'rounding_method': 'round_half_up',
            }
        }
    },
    {
        'version': 'v_dynamic_precision',
        'effective_date': date(2017, 1, 1),
        'description': '动态价格精度（根据价格区间）',
        'rules': {
            'all': {
                'use_dynamic_tick': True,
                'tick_size_rules': [
                    # 价格区间 -> 最小价格单位
                    {'price_max': 0.5, 'tick_size': 0.00001},
                    {'price_max': 5.0, 'tick_size': 0.0001},
                    {'price_max': 50.0, 'tick_size': 0.001},
                    {'price_max': 500.0, 'tick_size': 0.01},
                    {'price_max': 5000.0, 'tick_size': 0.1},
                    {'price_max': float('inf'), 'tick_size': 1.0},
                ],
                'rounding_method': 'round_half_up',
            }
        }
    },
]

# ============================================================================
# 滑点规则版本
# ============================================================================
SLIPPAGE_VERSIONS = [
    {
        'version': 'v_default',
        'effective_date': date(2009, 1, 3),
        'description': '默认滑点设置',
        'rules': {
            'all': {
                'slip_type': 'pricerelated',
                'slip_value': 0.0005,  # 0.05% 基础滑点
                'market_order_slippage': 0.001,  # 市价单额外滑点
            }
        }
    },
]

# ============================================================================
# 加密货币交易所分类辅助函数
# ============================================================================
def get_crypto_exchange(symbol: str) -> str:
    """根据交易对代码获取交易所

    Args:
        symbol: 交易对代码，如 'BTCUSDT', 'BTC-USDT'

    Returns:
        交易所: 'binance', 'okx', 'bybit', 等
    """
    # 根据分隔符判断
    if '-' in symbol:
        # OKX风格: BTC-USDT
        return 'okx'
    elif 'PERP' in symbol.upper():
        # Bybit风格: BTCPERP
        return 'bybit'
    else:
        # Binance风格: BTCUSDT
        return 'binance'


def get_crypto_pair_type(symbol: str) -> str:
    """根据交易对代码获取交易类型

    Args:
        symbol: 交易对代码

    Returns:
        交易类型: 'spot', 'futures', 'margin'
    """
    symbol_upper = symbol.upper()

    # 检查是否为永续合约（优先检查）
    if 'PERP' in symbol_upper:
        return 'futures'

    # 检查是否为合约交易对
    # BTCUSDT可能是现货，BTCPERP才是合约
    # 使用分隔符来判断：有'-'的通常是现货（OKX格式）
    if '-' in symbol:
        return 'spot'

    # 对于Binance格式的交易对，默认为现货
    # 例如：BTCUSDT是现货，BTCPERP是永续合约
    return 'spot'


def get_base_currency(symbol: str) -> str:
    """获取基础货币

    Args:
        symbol: 交易对代码，如 'BTCUSDT', 'BTC-USDT'

    Returns:
        基础货币: 'BTC', 'ETH', 等
    """
    if '-' in symbol:
        return symbol.split('-')[0]
    else:
        # 移除稳定币后缀
        for quote in ['USDT', 'USD', 'BUSD', 'USDC', 'PERP']:
            if symbol.endswith(quote):
                return symbol[:-len(quote)]
        return symbol


def get_quote_currency(symbol: str) -> str:
    """获取计价货币

    Args:
        symbol: 交易对代码

    Returns:
        计价货币: 'USDT', 'USD', 等
    """
    if '-' in symbol:
        return symbol.split('-')[1]
    else:
        # 查找稳定币后缀
        for quote in ['USDT', 'USD', 'BUSD', 'USDC']:
            if symbol.endswith(quote):
                return quote
        return 'USDT'


def get_crypto_precision(symbol: str) -> Dict[str, int]:
    """获取交易对精度配置

    Args:
        symbol: 交易对代码

    Returns:
        精度配置: {'price_decimals': 2, 'quantity_decimals': 8}
    """
    base = get_base_currency(symbol)

    # 根据基础货币设置默认精度
    precision_defaults = {
        'BTC': {'price_decimals': 2, 'quantity_decimals': 8},
        'ETH': {'price_decimals': 2, 'quantity_decimals': 8},
        'BNB': {'price_decimals': 3, 'quantity_decimals': 8},
        'SOL': {'price_decimals': 3, 'quantity_decimals': 6},
    }

    return precision_defaults.get(base, {'price_decimals': 2, 'quantity_decimals': 8})


# ============================================================================
# 规则查询辅助类
# ============================================================================
class RuleVersion:
    """规则版本查询类"""

    @staticmethod
    def get_applicable_rule(versions: List[Dict], category: str,
                           query_date: date, exchange: str = None) -> Optional[Dict]:
        """获取指定日期适用的规则版本

        Args:
            versions: 规则版本列表
            category: 分类（交易所或'all'）
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

            # 检查分类是否存在规则
            if 'rules' in version:
                if category in version['rules'] or 'all' in version['rules']:
                    applicable_versions.append(version)

        if not applicable_versions:
            return None

        # 返回最新生效的版本（按effective_date降序排序）
        applicable_versions.sort(key=lambda x: x['effective_date'], reverse=True)

        # 合并所有适用的规则（从最新到最旧）
        merged_rules = {}
        for v in applicable_versions:
            if category in v['rules']:
                # 只添加尚未添加的键（保持最新值）
                for key, value in v['rules'][category].items():
                    if key not in merged_rules:
                        merged_rules[key] = value
            elif 'all' in v['rules']:
                for key, value in v['rules']['all'].items():
                    if key not in merged_rules:
                        merged_rules[key] = value

        return merged_rules

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
        'BTCUSDT',  # Binance
        'ETH-USDT',  # OKX
        'BTCPERP',  # Bybit
    ]

    for symbol in test_symbols:
        print(f"{symbol}: exchange={get_crypto_exchange(symbol)}, "
              f"type={get_crypto_pair_type(symbol)}, "
              f"base={get_base_currency(symbol)}, "
              f"quote={get_quote_currency(symbol)}")
