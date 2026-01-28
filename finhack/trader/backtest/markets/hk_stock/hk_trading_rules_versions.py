"""
香港股票市场交易规则版本配置

定义了港股交易规则的历史版本，支持按时间查询规则变更
"""

from datetime import date, time
from typing import Dict, List, Any, Optional

# ============================================================================
# 港股板块分类
# ============================================================================
# 港股主要板块
HK_BOARD_TYPES = {
    'MAIN_BOARD': 'main_board',        # 主板
    'GEM': 'gem',                      # 创业板 (GEM)
}

# 港股代码前缀与板块映射
# 港股代码通常是5位数字，如 00005.HK, 00700.HK, 39999.HK
HK_CODE_PREFIX_MAP = {
    # 主板代码范围（大部分港股）
    '0000': 'main_board',  # 00001-00999
    '0100': 'main_board',  # 01000-09999
    '0200': 'main_board',
    '0300': 'main_board',
    '0400': 'main_board',
    '0500': 'main_board',
    '0600': 'main_board',
    '0700': 'main_board',
    '0800': 'main_board',
    '08': 'gem',           # 8xxxx 创业板 (32xxxx 也可能是创业板，但8xxxx更常见)
    '3999': 'main_board',  # 39xxx 主板
    '39': 'main_board',    # 39xxx 主板
}

# VCM（波动调节机制）股票名单（81只大盘股）
# VCM于2016年8月22日推出，适用于81只恒指及H股指数成分股
# 以下为主要VCM股票代码示例
VCM_STOCK_CODES = [
    '00001.HK',  # 长和
    '00002.HK',  # 中电控股
    '00003.HK',  # 香港中华煤气
    '00005.HK',  # 汇丰控股
    '00006.HK',  # 电能实业
    '00011.HK',  # 恒生银行
    '00012.HK',  # 恒基地产
    '00016.HK',  # 新鸿基地产
    '00017.HK',  # 新世界发展
    '00019.HK',  # 太古股份公司A
    '00027.HK',  # 银河娱乐
    '00066.HK',  # MTR Corporation
    '00083.HK',  # 信和置业
    '00101.HK',  # 恒隆地产
    '00175.HK',  # 吉利汽车
    '00267.HK',  # 中信股份
    '00288.HK',  # 怡邦洋行
    '00386.HK',  # 中国石化
    '00388.HK',  # 港交所
    '00670.HK',  # 东方航空
    '00688.HK',  # 中国海外发展
    '00700.HK',  # 腾讯控股
    '00762.HK',  # 中国联通
    '00823.HK',  # 领展房产基金
    '00836.HK',  # 华润电力
    '00857.HK',  # 中国石油股份
    '00883.HK',  # 中国海洋石油
    '00939.HK',  # 建设银行
    '00941.HK',  # 中国移动
    '00960.HK',  # 龙湖集团
    '00981.HK',  # 中芯国际
    '00992.HK',  # 联想集团
    '01038.HK',  # 长江基建集团
    '01044.HK',  # 恒安国际
    '01093.HK',  # 石药集团
    '01093.HK',  # 中国海外宏洋集团
    '01109.HK',  # 华润置地
    '01113.HK',  # 长实集团
    '01128.HK',  # 中国神华
    '01138.HK',  # 中远海控
    '01157.HK',  # 中联重科
    '01171.HK',  # 充矿煤业
    '01201.HK',  # 港铁公司
    '01288.HK',  # 农业银行
    '01299.HK',  # 友邦保险
    '01398.HK',  # 工商银行
    '01800.HK',  # 中国交通建设
    '01810.HK',  # 小米集团
    '01828.HK',  # 中国平安
    '02007.HK',  # 碧桂园
    '02020.HK',  # 安踏体育
    '02269.HK',  # 药明生物
    '02313.HK',  # 申洲国际
    '02318.HK',  # 中国平安保险
    '02382.HK',  # 舜宇光学科技
    '02628.HK',  # 中国人寿
    '02883.HK',  # 中信证券
    '03888.HK',  # 港铁公司
    '06690.HK',  # 海天国际
    # ... 还有其他VCM股票
]

# ============================================================================
# 交易时段规则版本
# ============================================================================
TRADING_SCHEDULE_VERSIONS = [
    {
        'version': 'v19860401',
        'effective_date': date(1986, 4, 1),
        'description': '港股早期交易时间（历史参考）',
        'rules': {
            'pre_market_start': '09:00',
            'pre_market_end': '09:30',
            'morning_start': '09:30',
            'morning_end': '12:00',
            'afternoon_start': '13:00',
            'afternoon_end': '16:00',
        }
    },
    {
        'version': 'v20080526',
        'effective_date': date(2008, 5, 26),
        'description': '延长午休时段至1.5小时',
        'rules': {
            'lunch_break_start': '12:00',
            'lunch_break_end': '13:30',
        }
    },
    {
        'version': 'v20110307',
        'effective_date': date(2011, 3, 7),
        'description': '恢复午休时段为1小时',
        'rules': {
            'lunch_break_start': '12:00',
            'lunch_break_end': '13:00',
        }
    },
    {
        'version': 'v20160822_vcm',
        'effective_date': date(2016, 8, 22),
        'description': '引入VCM波动调节机制',
        'rules': {
            'vcm_enabled': True,
            'vcm_trigger_threshold': 0.10,  # 10%波动
            'vcm_cooling_period_minutes': 5,  # 5分钟冷静期
            'vcm_stocks': VCM_STOCK_CODES,
        }
    },
    {
        'version': 'v20211029_random_close',
        'effective_date': date(2021, 10, 29),
        'description': '引入收市竞价交易时段随机收市机制',
        'rules': {
            'closing_auction_start': '16:00',
            'closing_auction_end': '16:10',
            'random_close_enabled': True,
            'random_close_time_range': (time(16, 8), time(16, 10)),
        }
    },
]

# ============================================================================
# 涨跌幅限制规则版本（港股无涨跌停限制）
# ============================================================================
PRICE_LIMIT_VERSIONS = [
    {
        'version': 'v_default',
        'effective_date': date(1980, 1, 1),
        'description': '港股无涨跌停限制',
        'rules': {
            'main_board': {
                'daily_limit': None,  # 无涨跌停限制
                'is_new_stock_no_limit': True,  # 新股也无限制
            },
            'gem': {
                'daily_limit': None,
                'is_new_stock_no_limit': True,
            }
        }
    },
]

# ============================================================================
# 最小交易单位规则版本
# ============================================================================
# 港股最小买卖单位因股票而异，称为"一手"
# 不同股票的一手股数不同，范围从100到10000不等
# 2026年拟将所有股票的一手股数标准化为100

LOT_SIZE_VERSIONS = [
    {
        'version': 'v_default',
        'effective_date': date(1980, 1, 1),
        'description': '港股不同股票不同最小单位（一手）',
        'rules': {
            'main_board': {
                'lot_size_table': {
                    # 价格区间 -> 最小单位
                    (0.01, 0.25): 1000,      # 0.01-0.25港元：1000股/手
                    (0.25, 0.50): 500,       # 0.25-0.50港元：500股/手
                    (0.50, 10.00): 100,      # 0.50-10港元：100股/手
                    (10.00, 20.00): 50,      # 10-20港元：50股/手
                    (20.00, 100.00): 20,     # 20-100港元：20股/手
                    (100.00, 200.00): 10,    # 100-200港元：10股/手
                    (200.00, 500.00): 5,     # 200-500港元：5股/手
                    (500.00, 1000.00): 2,    # 500-1000港元：2股/手
                    (1000.00, float('inf')): 1,  # 1000港元以上：1股/手
                },
                'default_lot_size': 100,
                'sell_allow_fractional': False,  # 必须整手买卖
                'sell_min': 1,
            },
            'gem': {
                'lot_size_table': {
                    (0.01, 0.25): 1000,
                    (0.25, 0.50): 500,
                    (0.50, 10.00): 100,
                },
                'default_lot_size': 100,
                'sell_allow_fractional': False,
                'sell_min': 1,
            }
        }
    },
    {
        'version': 'v2026_standardization',
        'effective_date': date(2026, 1, 1),
        'description': '拟议：统一最小交易单位为100股',
        'rules': {
            'all': {
                'lot_size_table': {
                    (0.01, float('inf')): 100,  # 统一为100股/手
                },
                'default_lot_size': 100,
                'sell_allow_fractional': False,
                'sell_min': 1,
            }
        }
    },
]

# ============================================================================
# 价格笼子/VCM机制规则版本
# ============================================================================
VCM_VERSIONS = [
    {
        'version': 'v20160822_vcm',
        'effective_date': date(2016, 8, 22),
        'description': '引入VCM波动调节机制',
        'rules': {
            'all': {
                'vcm_enabled': True,
                'vcm_stocks': VCM_STOCK_CODES,
                'vcm_monitoring_period': 5,  # 5分钟监控期
                'vcm_trigger_threshold': 0.10,  # 10%波动触发
                'vcm_cooling_period': 5,  # 5分钟冷静期
                'vcm_trading_allowed': True,  # 冷静期内仍可交易，但价格受限制
                'vcm_price_limit_pct': 0.10,  # 冷静期内价格限制在触发价的±10%
            }
        }
    },
]

# ============================================================================
# 价格精度（最小价格变动单位）规则版本
# ============================================================================
# 港股最小价格变动单位（tick size）根据股价范围不同而不同
TICK_SIZE_VERSIONS = [
    {
        'version': 'v_default',
        'effective_date': date(1980, 1, 1),
        'description': '港股价格分段tick size',
        'rules': {
            'tick_size_table': [
                # (价格下限, 价格上限, tick_size)
                (0.01, 0.25, 0.001),
                (0.25, 0.50, 0.005),
                (0.50, 10.00, 0.010),
                (10.00, 20.00, 0.020),
                (20.00, 100.00, 0.050),
                (100.00, 200.00, 0.100),
                (200.00, 500.00, 0.200),
                (500.00, 1000.00, 0.500),
                (1000.00, 2000.00, 1.000),
                (2000.00, 5000.00, 2.000),
                (5000.00, 9995.00, 5.000),
            ]
        }
    },
]

# ============================================================================
# 手续费规则版本
# ============================================================================
COMMISSION_VERSIONS = [
    {
        'version': 'v2000',
        'effective_date': date(2000, 1, 1),
        'description': '港股基本费用结构',
        'rules': {
            'stock': {
                # 交易佣金（可协商，典型0.01%-0.25%）
                'commission_rate': 0.001,  # 0.1%（千分之一）
                'min_commission': 0.0,  # 无最低佣金（券商可能设置）

                # 印花税（仅卖出）
                'stamp_tax_buy': 0.0,
                'stamp_tax_sell': 0.001,  # 0.1%（千分之一，2021年从0.13%下调）

                # 交易费（买卖双方）
                'trading_fee_rate': 0.00005,  # 0.005%（十万分之五）

                # 交易征费（买卖双方）
                'trading_levy_rate': 0.000027,  # 0.0027%（十万分之2.7）

                # 交收费（买卖双方）
                'clearing_fee_rate': 0.00002,  # 0.002%（十万分之二）
                'clearing_fee_max': 200.0,  # 最高200港元

                # 其他费用
                'other_fees': 0.0,
            }
        }
    },
    {
        'version': 'v20210801_stamp_tax',
        'effective_date': date(2021, 8, 1),
        'description': '印花税从0.13%下调至0.1%',
        'rules': {
            'stock': {
                'stamp_tax_sell': 0.001,  # 从0.0013降至0.001
            }
        }
    },
]

# ============================================================================
# 结算规则版本
# ============================================================================
SETTLEMENT_VERSIONS = [
    {
        'version': 'v_default',
        'effective_date': date(1980, 1, 1),
        'description': 'T+0交易，T+2结算',
        'rules': {
            'trading_cycle': 'T+0',  # T+0交易（当天买入当天可卖）
            'settlement_cycle': 'T+2',  # T+2结算（交易日后2个交易日结算）
            'short_selling': True,  # 允许卖空
            'margin_trading': True,  # 允许保证金交易
        }
    },
]

# ============================================================================
# 港股代码分类辅助函数
# ============================================================================
def get_hk_stock_board_type(symbol: str) -> str:
    """根据港股代码获取板块类型

    Args:
        symbol: 港股代码，如 '00700.HK' 或 '700.HK'

    Returns:
        板块类型: 'main_board' 或 'gem'
    """
    # 提取代码（去掉.HK后缀）
    code_str = symbol.split('.')[0]

    # 转换为整数判断范围
    try:
        code_num = int(code_str)
    except ValueError:
        # 如果无法转换，默认主板
        return 'main_board'

    # 检查是否为创业板（80000-89999）
    if 80000 <= code_num <= 89999:
        return 'gem'

    # 其他都是主板
    return 'main_board'


def is_vcm_stock(symbol: str) -> bool:
    """判断是否为VCM股票

    Args:
        symbol: 港股代码

    Returns:
        是否为VCM股票
    """
    # 标准化代码格式
    code_str = symbol.split('.')[0]
    try:
        code_num = int(code_str)
    except ValueError:
        return False

    # 格式化为5位数字
    code_formatted = f"{code_num:05d}.HK"
    return code_formatted in VCM_STOCK_CODES


def get_exchange_from_symbol(symbol: str) -> str:
    """从代码中提取交易所

    Args:
        symbol: 港股代码，如 '00700.HK'

    Returns:
        交易所: 'hkex'（香港交易所）
    """
    return 'hkex'


def get_lot_size_by_price(price: float, board: str = 'main_board',
                          query_date: date = None) -> int:
    """根据股价获取最小交易单位（一手股数）

    Args:
        price: 股价
        board: 板块类型
        query_date: 查询日期（用于未来标准化规则）

    Returns:
        最小交易单位（股数）
    """
    if query_date and query_date >= date(2026, 1, 1):
        # 2026年拟统一为100股
        return 100

    # 根据价格区间确定lot size
    if board == 'main_board':
        lot_ranges = [
            (1000, 0.01, 0.25),
            (500, 0.25, 0.50),
            (100, 0.50, 10.00),
            (50, 10.00, 20.00),
            (20, 20.00, 100.00),
            (10, 100.00, 200.00),
            (5, 200.00, 500.00),
            (2, 500.00, 1000.00),
            (1, 1000.00, float('inf')),
        ]
    else:  # gem
        lot_ranges = [
            (1000, 0.01, 0.25),
            (500, 0.25, 0.50),
            (100, 0.50, 10.00),
        ]

    for lot_size, lower, upper in lot_ranges:
        if lower <= price < upper:
            return lot_size

    return 100  # 默认100股


def get_tick_size_by_price(price: float) -> float:
    """根据股价获取最小价格变动单位（tick size）

    Args:
        price: 股价

    Returns:
        最小价格变动单位
    """
    tick_ranges = [
        (0.001, 0.01, 0.25),
        (0.005, 0.25, 0.50),
        (0.010, 0.50, 10.00),
        (0.020, 10.00, 20.00),
        (0.050, 20.00, 100.00),
        (0.100, 100.00, 200.00),
        (0.200, 200.00, 500.00),
        (0.500, 500.00, 1000.00),
        (1.000, 1000.00, 2000.00),
        (2.000, 2000.00, 5000.00),
        (5.000, 5000.00, 9995.00),
    ]

    for tick_size, lower, upper in tick_ranges:
        if lower <= price < upper:
            return tick_size

    return 5.000  # 默认


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
        merged_rules = {}
        for v in applicable_versions:
            if board in v['rules']:
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
            if 'scope' not in version:
                dates.add(version['effective_date'])
            elif board is None:
                dates.add(version['effective_date'])

        return sorted(dates)


if __name__ == '__main__':
    # 测试代码
    test_symbols = [
        '00700.HK',  # 腾讯 - 主板，VCM股票
        '00005.HK',  # 汇丰 - 主板，VCM股票
        '08000.HK',  # 创业板股票示例
    ]

    for symbol in test_symbols:
        print(f"{symbol}: board={get_hk_stock_board_type(symbol)}, "
              f"is_vcm={is_vcm_stock(symbol)}, "
              f"exchange={get_exchange_from_symbol(symbol)}")

    # 测试lot size
    test_prices = [0.10, 0.50, 5.00, 15.00, 50.00, 150.00, 300.00]
    print("\n=== 最小交易单位测试 ===")
    for price in test_prices:
        lot = get_lot_size_by_price(price)
        tick = get_tick_size_by_price(price)
        print(f"价格 {price:8.2f}: 最小单位={lot:4d}股, tick={tick}")
