"""
中国股票市场交易规则配置

定义了A股市场的各种交易规则和限制
"""

# A股交易规则配置
TRADING_RULES = {
    # 基本交易规则
    'basic_rules': {
        'min_order_quantity': 100,      # 最小交易单位（股）
        'tick_size': 0.01,              # 最小价格变动单位（元）
        'lot_size': 100,                # 每手股数
        't1_rule': True,                # T+1交易规则
        'short_selling': False,         # 不允许做空（普通股票）
    },
    
    # 价格限制规则
    'price_limits': {
        'limit_up_down': True,          # 涨跌停限制
        'main_board_limit': 0.10,       # 主板涨跌停幅度（10%）
        'gem_limit': 0.20,              # 创业板涨跌停幅度（20%）
        'star_limit': 0.05,             # ST股票涨跌停幅度（5%）
        'new_stock_days': 5,            # 新股前几天不设涨跌停限制
    },
    
    # 时间规则
    'time_rules': {
        'trading_days': [0, 1, 2, 3, 4],  # 周一到周五交易
        'pre_open_start': '09:15:00',      # 集合竞价开始时间
        'pre_open_end': '09:25:00',        # 集合竞价结束时间
        'morning_start': '09:30:00',       # 上午交易开始
        'morning_end': '11:30:00',         # 上午交易结束
        'afternoon_start': '13:00:00',     # 下午交易开始
        'afternoon_end': '15:00:00',       # 下午交易结束
        'closing_auction_start': '14:57:00', # 收盘集合竞价开始
    },
    
    # 费用规则
    'fee_rules': {
        'commission_rate': 0.0003,      # 佣金费率（万分之三）
        'min_commission': 5.0,          # 最低佣金（元）
        'stamp_tax_rate': 0.001,        # 印花税（千分之一，仅卖出）
        'transfer_fee_rate': 0.00002,   # 过户费（万分之0.2）
    },
    
    # 滑点规则
    'slippage_rules': {
        'default_type': 'percentage',    # 滑点类型：percentage 或 fixed
        'default_value': 0.001,         # 默认滑点值（0.1%）
        'market_order_slippage': 0.002, # 市价单额外滑点
    },
    
    # 风控规则
    'risk_rules': {
        'max_single_order_ratio': 1.0,  # 单笔订单最大占资金比例
        'max_position_ratio': 1.0,      # 单只股票最大持仓比例
        'min_trade_amount': 100,        # 最小交易金额（元）
    },
    
    # 特殊股票规则
    'special_rules': {
        'st_stocks': {
            'limit_ratio': 0.05,        # ST股票涨跌停幅度
            'risk_warning': True,       # 风险警示
        },
        'new_stocks': {
            'no_limit_days': 5,         # 新股上市前几天无涨跌停限制
            'call_auction_only': True,  # 首日仅集合竞价
        },
        'suspended_stocks': {
            'trading_allowed': False,   # 停牌股票不允许交易
        }
    },
    
    # 订单类型规则
    'order_types': {
        'supported_types': [
            'LIMIT',                    # 限价单
            'MARKET',                   # 市价单
            'BEST_PRICE',              # 最优价格单
            'FIVE_LEVEL_PRICE',        # 五档即成剩撤
        ],
        'default_type': 'LIMIT',       # 默认订单类型
    },
    
    # 撮合规则
    'matching_rules': {
        'price_priority': True,         # 价格优先
        'time_priority': True,          # 时间优先
        'pro_rata': False,             # 按比例撮合（A股不适用）
        'minimum_quantity': 100,        # 最小撮合数量
    }
} 