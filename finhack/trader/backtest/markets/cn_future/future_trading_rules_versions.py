"""
中国期货市场交易规则版本配置

定义了期货交易规则的历史版本，支持按时间查询规则变更
包含不同交易所、不同合约类型的规则差异
"""

from datetime import date, time
from typing import Dict, List, Any, Optional

# ============================================================================
# 交易所定义
# ============================================================================
FUTURE_EXCHANGES = {
    'CFFEX': 'cffex',  # 中国金融期货交易所（中金所）
    'SHFE': 'shfe',    # 上海期货交易所（上期所）
    'DCE': 'dce',      # 大连商品交易所（大商所）
    'CZCE': 'czce',    # 郑州商品交易所（郑商所）
    'GFEX': 'gfex',    # 广州期货交易所（广期所）
}

# 合约类型分类
FUTUREContract_TYPES = {
    'FINANCIAL': 'financial',    # 金融期货（股指、国债）
    'METAL': 'metal',            # 金属期货
    'ENERGY': 'energy',          # 能源化工
    'AGRICULTURAL': 'agricultural',  # 农产品
    'BOND': 'bond',              # 国债期货
    'INDEX': 'index',            # 股指期货
}

# 期货品种代码前缀与交易所映射
FUTURE_CODE_PREFIX_MAP = {
    # 中金所
    'IF': 'cffex',  # 沪深300股指期货
    'IH': 'cffex',  # 上证50股指期货
    'IC': 'cffex',  # 中证500股指期货
    'IM': 'cffex',  # 中证1000股指期货
    'TS': 'cffex',  # 2年期国债期货
    'TF': 'cffex',  # 5年期国债期货
    'T': 'cffex',   # 10年期国债期货
    'TL': 'cffex',  # 30年期国债期货

    # 上期所
    'CU': 'shfe',   # 铜
    'AL': 'shfe',   # 铝
    'ZN': 'shfe',   # 锌
    'PB': 'shfe',   # 铅
    'NI': 'shfe',   # 镍
    'SN': 'shfe',   # 锡
    'AU': 'shfe',   # 黄金
    'AG': 'shfe',   # 白银
    'RB': 'shfe',   # 螺纹钢
    'WR': 'shfe',   # 线材
    'HC': 'shfe',   # 热轧卷板
    'SS': 'shfe',   # 不锈钢
    'FU': 'shfe',   # 燃料油
    'BU': 'shfe',   # 沥青
    'RU': 'shfe',   # 天然橡胶
    'SP': 'shfe',   # 纸浆
    'AO': 'shfe',   # 氧化铝

    # 大商所
    'A': 'dce',     # 黄大豆
    'B': 'dce',     # 黄大豆2号
    'M': 'dce',     # 豆粕
    'Y': 'dce',     # 豆油
    'P': 'dce',     # 棕榈油
    'C': 'dce',     # 玉米
    'CS': 'dce',    # 玉米淀粉
    'JD': 'dce',    # 鸡蛋
    'L': 'dce',     # 聚乙烯
    'V': 'dce',     # 聚氯乙烯
    'PP': 'dce',    # 聚丙烯
    'FB': 'dce',    # 纤维板
    'BB': 'dce',    # 胶合板
    'J': 'dce',     # 焦炭
    'JM': 'dce',    # 焦煤
    'I': 'dce',     # 铁矿石
    'PG': 'dce',    # 液化石油气
    'EB': 'dce',    # 苯乙烯
    'EG': 'dce',    # 乙二醇
    'LH': 'dce',    # 生猪

    # 郑商所
    'SR': 'czce',   # 白糖
    'CF': 'czce',   # 棉花
    'TA': 'czce',   # PTA
    'OI': 'czce',   # 菜籽油
    'MA': 'czce',   # 甲醇
    'FG': 'czce',   # 玻璃
    'RM': 'czce',   # 菜粕
    'ZC': 'czce',   # 动力煤
    'SF': 'czce',   # 硅铁
    'SM': 'czce',   # 锰硅
    'UR': 'czce',   # 尿素
    'SA': 'czce',   # 纯碱
    'PK': 'czce',   # 花生
    'AP': 'czce',   # 苹果
    'CJ': 'czce',   # 红枣
    'RS': 'czce',   # 油菜籽
    'RI': 'czce',   # 早籼稻
    'JR': 'czce',   # 粳稻
    'LR': 'czce',   # 晚籼稻
    'WH': 'czce',   # 强麦
    'WT': 'czce',   # 硬麦
    'PM': 'czce',   # 普麦

    # 广期所
    'SI': 'gfex',   # 工业硅
    'LC': 'gfex',   # 碳酸锂
}

# 合约类型映射（基于品种代码）
FUTURE_TYPE_MAP = {
    # 金融期货
    'IF': 'index', 'IH': 'index', 'IC': 'index', 'IM': 'index',
    'TS': 'bond', 'TF': 'bond', 'T': 'bond', 'TL': 'bond',

    # 贵金属
    'AU': 'metal', 'AG': 'metal',

    # 有色金属
    'CU': 'metal', 'AL': 'metal', 'ZN': 'metal', 'PB': 'metal',
    'NI': 'metal', 'SN': 'metal', 'AO': 'metal',

    # 黑色金属
    'RB': 'metal', 'WR': 'metal', 'HC': 'metal', 'SS': 'metal',
    'I': 'metal', 'J': 'energy', 'JM': 'energy',

    # 能源化工
    'FU': 'energy', 'BU': 'energy', 'RU': 'energy', 'SP': 'energy',
    'L': 'energy', 'V': 'energy', 'PP': 'energy', 'EB': 'energy', 'EG': 'energy',
    'TA': 'energy', 'MA': 'energy', 'FG': 'energy', 'SA': 'energy',
    'PG': 'energy', 'SI': 'energy', 'LC': 'energy',

    # 农产品
    'A': 'agricultural', 'B': 'agricultural', 'M': 'agricultural',
    'Y': 'agricultural', 'P': 'agricultural', 'C': 'agricultural',
    'CS': 'agricultural', 'JD': 'agricultural', 'LH': 'agricultural',
    'SR': 'agricultural', 'CF': 'agricultural', 'OI': 'agricultural',
    'RM': 'agricultural', 'ZC': 'agricultural', 'SF': 'agricultural',
    'SM': 'agricultural', 'UR': 'agricultural', 'PK': 'agricultural',
    'AP': 'agricultural', 'CJ': 'agricultural', 'RS': 'agricultural',
    'RI': 'agricultural', 'JR': 'agricultural', 'LR': 'agricultural',
    'WH': 'agricultural', 'WT': 'agricultural', 'PM': 'agricultural',

    # 其他
    'FB': 'agricultural', 'BB': 'agricultural',
}

# ============================================================================
# 交易时段规则版本
# ============================================================================
TRADING_SCHEDULE_VERSIONS = [
    {
        'version': 'v20100416_cffex',
        'effective_date': date(2010, 4, 16),
        'description': '股指期货上市交易',
        'rules': {
            'day_session_start': '09:15',
            'day_session_end': '15:15',
            'morning_break_start': '11:30',
            'morning_break_end': '13:00',
            'night_session': False,  # 股指期货无夜盘
        },
        'scope': ['cffex']
    },
    {
        'version': 'v20131021_night',
        'effective_date': date(2013, 10, 21),
        'description': '黄金、白银开启夜盘交易',
        'rules': {
            'night_session': True,
            'night_session_start': '21:00',
            'night_session_end': '02:30',
            'night_break_start': None,
        },
        'scope': ['shfe']
    },
    {
        'version': 'v20131220_night_expansion',
        'effective_date': date(2013, 12, 20),
        'description': '夜盘交易扩展到更多品种',
        'rules': {
            'night_session': True,
            'night_session_start': '21:00',
            'night_session_end': '23:00',  # 铜、铝、锌等
            'night_break_start': None,
        },
        'scope': ['shfe', 'czce']
    },
    {
        'version': 'v20150105_night_extension',
        'effective_date': date(2015, 1, 5),
        'description': '夜盘交易延长至次日凌晨',
        'rules': {
            'night_session': True,
            'night_session_start': '21:00',
            'night_session_end': '01:00',  # 铜、铝、锌等延长至1:00
            'night_break_start': None,
        },
        'scope': ['shfe']
    },
    {
        'version': 'v20150413_night_further',
        'effective_date': date(2015, 4, 13),
        'description': '夜盘交易进一步延长',
        'rules': {
            'night_session': True,
            'night_session_start': '21:00',
            'night_session_end': '02:30',  # 恢复至2:30
            'night_break_start': None,
        },
        'scope': ['shfe', 'dce', 'czce']
    },
    {
        'version': 'v20150416_cffex_hourly',
        'effective_date': date(2015, 4, 16),
        'description': '中金所调整日盘结束时间',
        'rules': {
            'day_session_end': '15:00',  # 从15:15改为15:00
        },
        'scope': ['cffex']
    },
]

# 默认交易时段配置（按交易所和品种）
DEFAULT_TRADING_SCHEDULES = {
    'cffex': {  # 中金所（股指、国债期货）
        'day_session_start': '09:30',  # 实际数据从09:30开始
        'day_session_end': '15:00',    # 实际数据到15:00结束
        'morning_break_start': '11:30',
        'morning_break_end': '13:00',
        'night_session': False,
        'night_session_start': None,
        'night_session_end': None,
    },
    'shfe': {  # 上期所
        'day_session_start': '09:00',
        'day_session_end': '15:00',
        'morning_break_start': '10:15',
        'morning_break_end': '10:30',
        'night_session': True,  # 部分品种有夜盘
        'night_session_start': '21:00',
        'night_session_end': '02:30',
    },
    'dce': {  # 大商所
        'day_session_start': '09:00',
        'day_session_end': '15:00',
        'morning_break_start': '10:15',
        'morning_break_end': '10:30',
        'night_session': True,  # 部分品种有夜盘
        'night_session_start': '21:00',
        'night_session_end': '23:00',
    },
    'czce': {  # 郑商所
        'day_session_start': '09:00',
        'day_session_end': '15:00',
        'morning_break_start': '10:15',
        'morning_break_end': '10:30',
        'night_session': True,  # 部分品种有夜盘
        'night_session_start': '21:00',
        'night_session_end': '23:30',
    },
    'gfex': {  # 广期所
        'day_session_start': '09:00',
        'day_session_end': '15:00',
        'morning_break_start': '10:15',
        'morning_break_end': '10:30',
        'night_session': True,  # 部分品种有夜盘
        'night_session_start': '21:00',
        'night_session_end': '23:00',
    },
}

# 特殊品种夜盘时间配置
NIGHT_SESSION_SPECIAL = {
    'CU': {'end': '01:00'},  # 铜夜盘至1:00
    'AL': {'end': '01:00'},  # 铝夜盘至1:00
    'ZN': {'end': '01:00'},  # 锌夜盘至1:00
    'PB': {'end': '01:00'},  # 铅夜盘至1:00
    'NI': {'end': '01:00'},  # 镍夜盘至1:00
    'SN': {'end': '01:00'},  # 锡夜盘至1:00
    'AU': {'end': '02:30'},  # 黄金夜盘至2:30
    'AG': {'end': '02:30'},  # 白银夜盘至2:30
    'RB': {'end': '23:00'},  # 螺纹钢夜盘至23:00
    'HC': {'end': '23:00'},  # 热卷夜盘至23:00
    'FU': {'end': '23:00'},  # 燃料油夜盘至23:00
    'BU': {'end': '23:00'},  # 沥青夜盘至23:00
    'RU': {'end': '23:00'},  # 橡胶夜盘至23:00
}

# ============================================================================
# 涨跌幅限制规则版本
# ============================================================================
PRICE_LIMIT_VERSIONS = [
    {
        'version': 'v_default',
        'effective_date': date(1990, 1, 1),
        'description': '默认涨跌幅限制',
        'rules': {
            'financial': {
                'daily_limit': 0.10,  # 股指期货10%
                'first_day_limit': 0.20,  # 新合约首日20%
            },
            'metal': {
                'daily_limit': [0.04, 0.05, 0.06, 0.07],  # 梯级涨跌停
                'first_day_limit': 0.10,
            },
            'agricultural': {
                'daily_limit': 0.04,
                'first_day_limit': 0.07,
            },
            'energy': {
                'daily_limit': 0.05,
                'first_day_limit': 0.10,
            },
        }
    },
]

# 具体品种涨跌幅配置
SPECIFIC_PRICE_LIMITS = {
    # 股指期货
    'IF': {'limit': 0.10},  # 沪深300
    'IH': {'limit': 0.10},  # 上证50
    'IC': {'limit': 0.10},  # 中证500
    'IM': {'limit': 0.10},  # 中证1000

    # 国债期货（无涨跌停限制）
    'T': {'limit': None},   # 10年期国债
    'TF': {'limit': None},  # 5年期国债
    'TS': {'limit': None},  # 2年期国债
    'TL': {'limit': None},  # 30年期国债

    # 贵金属
    'AU': {'limit': 0.07},  # 黄金7%
    'AG': {'limit': 0.09},  # 白银9%

    # 有色金属
    'CU': {'limit': 0.05},  # 铜5%
    'AL': {'limit': 0.05},  # 铝5%
    'ZN': {'limit': 0.05},  # 锌5%
    'PB': {'limit': 0.05},  # 铅5%
    'NI': {'limit': 0.08},  # 镍8%
    'SN': {'limit': 0.05},  # 锡5%

    # 黑色系
    'RB': {'limit': 0.05},  # 螺纹钢5%
    'HC': {'limit': 0.05},  # 热卷5%
    'I': {'limit': 0.08},   # 铁矿石8%
    'J': {'limit': 0.08},   # 焦炭8%
    'JM': {'limit': 0.08},  # 焦煤8%

    # 能源化工
    'RU': {'limit': 0.06},  # 橡胶6%
    'FU': {'limit': 0.07},  # 燃料油7%
    'BU': {'limit': 0.07},  # 沥青7%
    'L': {'limit': 0.04},   # 塑料4%
    'PP': {'limit': 0.04},  # PP4%
    'MA': {'limit': 0.04},  # 甲醇4%
    'TA': {'limit': 0.04},  # PTA4%

    # 农产品
    'M': {'limit': 0.04},   # 豆粕4%
    'Y': {'limit': 0.04},   # 豆油4%
    'P': {'limit': 0.04},   # 棕榈油4%
    'C': {'limit': 0.04},   # 玉米4%
    'A': {'limit': 0.04},   # 大豆4%
    'SR': {'limit': 0.04},  # 白糖4%
    'CF': {'limit': 0.04},  # 棉花4%
    'JD': {'limit': 0.04},  # 鸡蛋4%
    'AP': {'limit': 0.06},  # 苹果6%
    'LH': {'limit': 0.04},  # 生猪4%
}

# ============================================================================
# 保证金比例规则版本
# ============================================================================
MARGIN_RATIO_VERSIONS = [
    {
        'version': 'v_default',
        'effective_date': date(1990, 1, 1),
        'description': '默认保证金比例',
        'rules': {
            'financial': {
                'initial_margin': 0.15,  # 15%
                'maintenance_margin': 0.10,  # 10%
            },
            'metal': {
                'initial_margin': 0.08,  # 8%
                'maintenance_margin': 0.06,  # 6%
            },
            'agricultural': {
                'initial_margin': 0.07,  # 7%
                'maintenance_margin': 0.05,  # 5%
            },
            'energy': {
                'initial_margin': 0.08,  # 8%
                'maintenance_margin': 0.06,  # 6%
            },
        }
    },
]

# 具体品种保证金配置（交易所最低标准，实际可能更高）
SPECIFIC_MARGIN_RATIOS = {
    # 股指期货
    'IF': {'initial': 0.15, 'maintenance': 0.10},
    'IH': {'initial': 0.15, 'maintenance': 0.10},
    'IC': {'initial': 0.15, 'maintenance': 0.10},
    'IM': {'initial': 0.15, 'maintenance': 0.10},

    # 国债期货
    'T': {'initial': 0.03, 'maintenance': 0.02},  # 10年期国债3%
    'TF': {'initial': 0.02, 'maintenance': 0.015},  # 5年期国债2%
    'TS': {'initial': 0.015, 'maintenance': 0.01},  # 2年期国债1.5%
    'TL': {'initial': 0.035, 'maintenance': 0.025},  # 30年期国债3.5%

    # 贵金属
    'AU': {'initial': 0.06, 'maintenance': 0.04},  # 黄金6%
    'AG': {'initial': 0.07, 'maintenance': 0.05},  # 白银7%

    # 有色金属
    'CU': {'initial': 0.07, 'maintenance': 0.05},  # 铜7%
    'AL': {'initial': 0.05, 'maintenance': 0.04},  # 铝5%
    'ZN': {'initial': 0.06, 'maintenance': 0.05},  # 锌6%
    'NI': {'initial': 0.08, 'maintenance': 0.06},  # 镍8%

    # 黑色系
    'RB': {'initial': 0.07, 'maintenance': 0.05},  # 螺纹钢7%
    'HC': {'initial': 0.07, 'maintenance': 0.05},  # 热卷7%
    'I': {'initial': 0.09, 'maintenance': 0.07},  # 铁矿石9%
    'J': {'initial': 0.09, 'maintenance': 0.07},  # 焦炭9%
    'JM': {'initial': 0.09, 'maintenance': 0.07},  # 焦煤9%

    # 农产品
    'M': {'initial': 0.06, 'maintenance': 0.05},  # 豆粕6%
    'C': {'initial': 0.06, 'maintenance': 0.05},  # 玉米6%
    'A': {'initial': 0.06, 'maintenance': 0.05},  # 大豆6%
    'Y': {'initial': 0.06, 'maintenance': 0.05},  # 豆油6%
}

# ============================================================================
# 最小变动价位（Tick Size）规则
# ============================================================================
TICK_SIZE_RULES = {
    # 股指期货（0.2点）
    'IF': {'tick_size': 0.2, 'tick_value': 60},  # 0.2点 * 300元/点
    'IH': {'tick_size': 0.2, 'tick_value': 60},  # 0.2点 * 300元/点
    'IC': {'tick_size': 0.2, 'tick_value': 100},  # 0.2点 * 200元/点
    'IM': {'tick_size': 0.2, 'tick_value': 200},  # 0.2点 * 1000元/点

    # 国债期货（0.005元）
    'T': {'tick_size': 0.005, 'tick_value': 50},   # 0.005元 * 10000元/点
    'TF': {'tick_size': 0.005, 'tick_value': 50},  # 0.005元 * 10000元/点
    'TS': {'tick_size': 0.005, 'tick_value': 20},  # 0.005元 * 20000元/点
    'TL': {'tick_size': 0.01, 'tick_value': 100},  # 0.01元 * 10000元/点

    # 贵金属
    'AU': {'tick_size': 0.02, 'tick_value': 20},   # 0.02元/克 * 1000克/手
    'AG': {'tick_size': 1, 'tick_value': 10},      # 1元/千克 * 10千克/手

    # 有色金属
    'CU': {'tick_size': 10, 'tick_value': 50},     # 10元/吨 * 5吨/手
    'AL': {'tick_size': 5, 'tick_value': 25},      # 5元/吨 * 5吨/手
    'ZN': {'tick_size': 5, 'tick_value': 25},      # 5元/吨 * 5吨/手
    'PB': {'tick_size': 5, 'tick_value': 50},      # 5元/吨 * 25吨/手
    'NI': {'tick_size': 10, 'tick_value': 10},     # 10元/吨 * 1吨/手
    'SN': {'tick_size': 10, 'tick_value': 50},     # 10元/吨 * 1吨/手

    # 黑色系
    'RB': {'tick_size': 1, 'tick_value': 10},      # 1元/吨 * 10吨/手
    'HC': {'tick_size': 1, 'tick_value': 10},      # 1元/吨 * 10吨/手
    'I': {'tick_size': 0.5, 'tick_value': 50},     # 0.5元/吨 * 100吨/手
    'J': {'tick_size': 0.5, 'tick_value': 50},     # 0.5元/吨 * 100吨/手
    'JM': {'tick_size': 0.5, 'tick_value': 60},    # 0.5元/吨 * 60吨/手

    # 能源化工
    'RU': {'tick_size': 5, 'tick_value': 50},      # 5元/吨 * 10吨/手
    'FU': {'tick_size': 1, 'tick_value': 10},      # 1元/吨 * 10吨/手
    'BU': {'tick_size': 1, 'tick_value': 10},      # 1元/吨 * 10吨/手
    'L': {'tick_size': 5, 'tick_value': 50},       # 5元/吨 * 10吨/手
    'PP': {'tick_size': 5, 'tick_value': 50},      # 5元/吨 * 10吨/手
    'MA': {'tick_size': 1, 'tick_value': 20},      # 1元/吨 * 20吨/手
    'TA': {'tick_size': 2, 'tick_value': 20},      # 2元/吨 * 10吨/手

    # 农产品
    'M': {'tick_size': 1, 'tick_value': 10},       # 1元/吨 * 10吨/手
    'Y': {'tick_size': 2, 'tick_value': 20},       # 2元/吨 * 10吨/手
    'P': {'tick_size': 2, 'tick_value': 20},       # 2元/吨 * 10吨/手
    'C': {'tick_size': 1, 'tick_value': 10},       # 1元/吨 * 10吨/手
    'A': {'tick_size': 1, 'tick_value': 10},       # 1元/吨 * 10吨/手
    'SR': {'tick_size': 1, 'tick_value': 10},      # 1元/吨 * 10吨/手
    'CF': {'tick_size': 5, 'tick_value': 50},      # 5元/吨 * 5吨/手
    'JD': {'tick_size': 1, 'tick_value': 10},      # 1元/500kg * 10手
    'AP': {'tick_size': 1, 'tick_value': 10},      # 1元/吨 * 10吨/手
    'LH': {'tick_size': 5, 'tick_value': 16},      # 5元/吨 * 16吨/手
}

# 默认Tick Size（未定义的品种使用默认值）
DEFAULT_TICK_SIZE = {
    'metal': 10,
    'energy': 1,
    'agricultural': 1,
    'financial': 0.2,
}

# ============================================================================
# 合约单位规则
# ============================================================================
CONTRACT_SIZE_RULES = {
    # 股指期货
    'IF': 300,    # 沪深300：300元/点
    'IH': 300,    # 上证50：300元/点
    'IC': 200,    # 中证500：200元/点
    'IM': 1000,   # 中证1000：1000元/点

    # 国债期货
    'T': 10000,   # 10年期国债：10000元/点
    'TF': 10000,  # 5年期国债：10000元/点
    'TS': 20000,  # 2年期国债：20000元/点
    'TL': 10000,  # 30年期国债：10000元/点

    # 贵金属
    'AU': 1000,   # 黄金：1000克/手
    'AG': 10,     # 白银：10千克/手

    # 有色金属
    'CU': 5,      # 铜：5吨/手
    'AL': 5,      # 铝：5吨/手
    'ZN': 5,      # 锌：5吨/手
    'PB': 25,     # 铅：25吨/手
    'NI': 1,      # 镍：1吨/手
    'SN': 1,      # 锡：1吨/手
    'AO': 20,     # 氧化铝：20吨/手

    # 黑色系
    'RB': 10,     # 螺纹钢：10吨/手
    'HC': 10,     # 热卷：10吨/手
    'I': 100,     # 铁矿石：100吨/手
    'J': 100,     # 焦炭：100吨/手
    'JM': 60,     # 焦煤：60吨/手

    # 能源化工
    'RU': 10,     # 橡胶：10吨/手
    'FU': 10,     # 燃料油：10吨/手
    'BU': 10,     # 沥青：10吨/手
    'L': 10,      # 塑料：10吨/手
    'PP': 10,     # PP：10吨/手
    'MA': 20,     # 甲醇：20吨/手
    'TA': 10,     # PTA：10吨/手

    # 农产品
    'M': 10,      # 豆粕：10吨/手
    'Y': 10,      # 豆油：10吨/手
    'P': 10,      # 棕榈油：10吨/手
    'C': 10,      # 玉米：10吨/手
    'A': 10,      # 大豆：10吨/手
    'SR': 10,     # 白糖：10吨/手
    'CF': 5,      # 棉花：5吨/手
    'JD': 10,     # 鸡蛋：10手（500kg/手）
    'AP': 10,     # 苹果：10吨/手
    'LH': 16,     # 生猪：16吨/手
}

# ============================================================================
# 手续费规则版本
# ============================================================================
COMMISSION_VERSIONS = [
    {
        'version': 'v_default',
        'effective_date': date(1990, 1, 1),
        'description': '默认手续费规则',
        'rules': {
            'financial': {
                'open_commission': 0.00002,  # 万分之0.2（成交额）
                'close_commission': 0.00002,
                'close_today_commission': 0.00002,  # 平今相同
                'min_commission': 5.0,
            },
            'metal': {
                'open_commission': 0.0001,   # 万分之一
                'close_commission': 0.0001,
                'close_today_commission': 0.0001,
                'min_commission': 5.0,
            },
            'agricultural': {
                'open_commission': 0.0003,   # 万分之三
                'close_commission': 0.0003,
                'close_today_commission': 0.0003,
                'min_commission': 5.0,
            },
            'energy': {
                'open_commission': 0.0002,   # 万分之二
                'close_commission': 0.0002,
                'close_today_commission': 0.0002,
                'min_commission': 5.0,
            },
        }
    },
]

# 具体品种手续费配置
SPECIFIC_COMMISSIONS = {
    # 股指期货
    'IF': {'open': 0.00002, 'close': 0.00002, 'close_today': 0.00003, 'min': 5.0},
    'IH': {'open': 0.00002, 'close': 0.00002, 'close_today': 0.00003, 'min': 5.0},
    'IC': {'open': 0.00002, 'close': 0.00002, 'close_today': 0.00003, 'min': 5.0},
    'IM': {'open': 0.00002, 'close': 0.00002, 'close_today': 0.00003, 'min': 5.0},

    # 国债期货
    'T': {'open': 0.00003, 'close': 0.00003, 'close_today': 0.00003, 'min': 5.0},
    'TF': {'open': 0.00003, 'close': 0.00003, 'close_today': 0.00003, 'min': 5.0},
    'TS': {'open': 0.00003, 'close': 0.00003, 'close_today': 0.00003, 'min': 5.0},
    'TL': {'open': 0.00003, 'close': 0.00003, 'close_today': 0.00003, 'min': 5.0},

    # 贵金属
    'AU': {'open': 0.0001, 'close': 0.0001, 'close_today': 0.0003, 'min': 10.0},
    'AG': {'open': 0.0001, 'close': 0.0001, 'close_today': 0.0003, 'min': 5.0},

    # 有色金属
    'CU': {'open': 0.0001, 'close': 0.0001, 'close_today': 0.0003, 'min': 5.0},
    'AL': {'open': 0.00003, 'close': 0.00003, 'close_today': 0.0003, 'min': 5.0},

    # 黑色系
    'RB': {'open': 0.0001, 'close': 0.0001, 'close_today': 0.0003, 'min': 5.0},
    'I': {'open': 0.0001, 'close': 0.0001, 'close_today': 0.0003, 'min': 5.0},

    # 能源化工
    'RU': {'open': 0.0001, 'close': 0.0001, 'close_today': 0.0003, 'min': 5.0},

    # 农产品
    'M': {'open': 0.0003, 'close': 0.0003, 'close_today': 0.0003, 'min': 5.0},
    'C': {'open': 0.0002, 'close': 0.0002, 'close_today': 0.0002, 'min': 5.0},
}

# ============================================================================
# 交割月限制规则
# ============================================================================
DELIVERY_MONTH_RESTRICTIONS = {
    'natural_person_ban_days': {  # 自然人最后交易日
        'cffex': 15,  # 中金所交割月前第15个交易日
        'shfe': 15,   # 上期所交割月前第15个交易日
        'dce': 15,    # 大商所交割月前第15个交易日
        'czce': 15,   # 郑商所交割月前第15个交易日
        'gfex': 15,   # 广期所交割月前第15个交易日
    },
    'last_trading_day': {
        'financial': 'contract_month_last_day',  # 合约月份最后交易日
        'commodity': 'contract_month_15th',  # 交割月份第15个交易日
    },
}

# ============================================================================
# 持仓限制规则
# ============================================================================
POSITION_LIMITS = {
    # 股指期货持仓限制（手）
    'IF': {'speculator': 5000, 'hedger': 10000, 'arbitrageur': 10000},
    'IH': {'speculator': 5000, 'hedger': 10000, 'arbitrageur': 10000},
    'IC': {'speculator': 5000, 'hedger': 10000, 'arbitrageur': 10000},
    'IM': {'speculator': 3000, 'hedger': 6000, 'arbitrageur': 6000},

    # 国债期货持仓限制
    'T': {'speculator': 2000, 'hedger': 4000},
    'TF': {'speculator': 2000, 'hedger': 4000},
    'TS': {'speculator': 2000, 'hedger': 4000},
    'TL': {'speculator': 1000, 'hedger': 2000},

    # 商品期货持仓限制
    'CU': {'speculator': 8000, 'hedger': 16000},
    'AU': {'speculator': 9000, 'hedger': 18000},
    'RB': {'speculator': 10000, 'hedger': 20000},
}

# ============================================================================
# 辅助函数
# ============================================================================

def get_future_exchange(symbol: str) -> str:
    """从期货代码中提取交易所

    Args:
        symbol: 期货代码，如 'IF2406.CFFEX' 或 'IF2406'

    Returns:
        交易所代码: 'cffex', 'shfe', 'dce', 'czce', 'gfex'
    """
    # 如果有后缀，直接提取
    if '.' in symbol:
        suffix = symbol.split('.')[1].upper()
        return FUTURE_EXCHANGES.get(suffix, 'cffex')

    # 提取品种代码
    code = symbol.split('.')[0]

    # 查找品种前缀匹配
    for prefix, exchange in FUTURE_CODE_PREFIX_MAP.items():
        if code.startswith(prefix):
            return exchange

    # 默认返回中金所
    return 'cffex'


def get_future_type(symbol: str) -> str:
    """根据期货代码获取合约类型

    Args:
        symbol: 期货代码

    Returns:
        合约类型: 'financial', 'metal', 'energy', 'agricultural', 'bond', 'index'
    """
    # 提取品种代码
    code = symbol.split('.')[0]

    # 查找品种前缀匹配
    for prefix, future_type in FUTURE_TYPE_MAP.items():
        if code.startswith(prefix):
            return future_type

    # 默认返回商品期货
    return 'agricultural'


def get_future_product(symbol: str) -> str:
    """从期货代码中提取品种代码

    Args:
        symbol: 期货代码，如 'IF2406.CFFEX'

    Returns:
        品种代码: 'IF', 'CU', 'RB' 等
    """
    code = symbol.split('.')[0]

    # 尝试匹配品种前缀
    for prefix in sorted(FUTURE_CODE_PREFIX_MAP.keys(), key=len, reverse=True):
        if code.startswith(prefix):
            return prefix

    # 如果没找到，返回前2个字符
    return code[:2].upper()


def has_night_session(symbol: str) -> bool:
    """判断品种是否有夜盘交易

    Args:
        symbol: 期货代码

    Returns:
        是否有夜盘交易
    """
    exchange = get_future_exchange(symbol)
    product = get_future_product(symbol)

    # 中金所品种无夜盘
    if exchange == 'cffex':
        return False

    # 检查特定品种是否有夜盘
    # 大部分商品期货都有夜盘，除少数品种
    no_night_products = ['WR', 'FB', 'BB', 'RR']  # 无夜盘的品种

    if product in no_night_products:
        return False

    # 检查交易所默认配置
    return DEFAULT_TRADING_SCHEDULES.get(exchange, {}).get('night_session', False)


def get_tick_size(symbol: str) -> float:
    """获取品种的最小变动价位

    Args:
        symbol: 期货代码

    Returns:
        最小变动价位
    """
    product = get_future_product(symbol)

    # 查找具体品种配置
    if product in TICK_SIZE_RULES:
        return TICK_SIZE_RULES[product]['tick_size']

    # 使用类型默认值
    future_type = get_future_type(symbol)
    return DEFAULT_TICK_SIZE.get(future_type, 1)


def get_tick_value(symbol: str) -> float:
    """获取品种的最小变动价位对应的价值

    Args:
        symbol: 期货代码

    Returns:
        最小变动价值
    """
    product = get_future_product(symbol)

    # 查找具体品种配置
    if product in TICK_SIZE_RULES:
        return TICK_SIZE_RULES[product]['tick_value']

    # 计算默认值
    tick_size = get_tick_size(symbol)
    contract_size = get_contract_size(symbol)
    return tick_size * contract_size


def get_contract_size(symbol: str) -> int:
    """获取合约单位

    Args:
        symbol: 期货代码

    Returns:
        合约单位
    """
    product = get_future_product(symbol)

    if product in CONTRACT_SIZE_RULES:
        return CONTRACT_SIZE_RULES[product]

    # 默认值
    return 10


def get_price_limit(symbol: str) -> Optional[float]:
    """获取涨跌幅限制

    Args:
        symbol: 期货代码

    Returns:
        涨跌幅限制（如0.10表示10%），None表示无限制
    """
    product = get_future_product(symbol)

    if product in SPECIFIC_PRICE_LIMITS:
        return SPECIFIC_PRICE_LIMITS[product].get('limit')

    # 使用类型默认值
    future_type = get_future_type(symbol)
    if future_type == 'bond':
        return None  # 国债期货无涨跌停
    elif future_type == 'index':
        return 0.10
    elif future_type == 'metal':
        return 0.05
    else:
        return 0.04


def get_margin_ratio(symbol: str, margin_type: str = 'initial') -> float:
    """获取保证金比例

    Args:
        symbol: 期货代码
        margin_type: 保证金类型 ('initial' or 'maintenance')

    Returns:
        保证金比例
    """
    product = get_future_product(symbol)

    if product in SPECIFIC_MARGIN_RATIOS:
        if margin_type == 'initial':
            return SPECIFIC_MARGIN_RATIOS[product]['initial']
        else:
            return SPECIFIC_MARGIN_RATIOS[product]['maintenance']

    # 使用类型默认值
    future_type = get_future_type(symbol)
    if future_type == 'financial':
        return 0.15 if margin_type == 'initial' else 0.10
    elif future_type == 'metal':
        return 0.08 if margin_type == 'initial' else 0.06
    else:
        return 0.07 if margin_type == 'initial' else 0.05


def get_commission_info(symbol: str) -> Dict[str, float]:
    """获取手续费信息

    Args:
        symbol: 期货代码

    Returns:
        手续费配置字典
    """
    product = get_future_product(symbol)

    if product in SPECIFIC_COMMISSIONS:
        return SPECIFIC_COMMISSIONS[product]

    # 使用类型默认值
    future_type = get_future_type(symbol)
    if future_type == 'financial':
        return {'open': 0.00002, 'close': 0.00002, 'close_today': 0.00002, 'min': 5.0}
    elif future_type == 'metal':
        return {'open': 0.0001, 'close': 0.0001, 'close_today': 0.0001, 'min': 5.0}
    else:
        return {'open': 0.0003, 'close': 0.0003, 'close_today': 0.0003, 'min': 5.0}


# ============================================================================
# 规则查询辅助类
# ============================================================================
class RuleVersion:
    """规则版本查询类"""

    @staticmethod
    def get_applicable_rule(versions: List[Dict], contract_type: str,
                           query_date: date, exchange: str = None) -> Optional[Dict]:
        """获取指定日期适用的规则版本

        Args:
            versions: 规则版本列表
            contract_type: 合约类型
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

            # 检查合约类型是否存在规则
            if 'rules' in version:
                if contract_type in version['rules'] or 'all' in version['rules']:
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
            if contract_type in v['rules']:
                for key, value in v['rules'][contract_type].items():
                    if key not in merged_rules:
                        merged_rules[key] = value
            elif 'all' in v['rules']:
                for key, value in v['rules']['all'].items():
                    if key not in merged_rules:
                        merged_rules[key] = value

        return merged_rules


if __name__ == '__main__':
    # 测试代码
    test_symbols = [
        'IF2406.CFFEX',  # 沪深300股指期货
        'CU2406.SHFE',   # 铜期货
        'RB2406.SHFE',   # 螺纹钢期货
        'M2406.DCE',     # 豆粕期货
        'MA406.CZCE',    # 甲醇期货
    ]

    for symbol in test_symbols:
        product = get_future_product(symbol)
        exchange = get_future_exchange(symbol)
        future_type = get_future_type(symbol)
        tick_size = get_tick_size(symbol)
        contract_size = get_contract_size(symbol)
        price_limit = get_price_limit(symbol)

        print(f"{symbol}:")
        print(f"  品种: {product}")
        print(f"  交易所: {exchange}")
        print(f"  类型: {future_type}")
        print(f"  最小变动价位: {tick_size}")
        print(f"  合约单位: {contract_size}")
        print(f"  涨跌幅限制: {price_limit}")
        print(f"  有夜盘: {has_night_session(symbol)}")
        print()
