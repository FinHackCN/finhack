"""
中国股票市场事件配置

定义了A股市场的所有标准事件和时间点
"""

from datetime import time

# A股市场事件配置
EVENTS_CONFIG = {
    # 静态市场事件配置
    'market_events': [
        {'event_type': 'DAY_START', 'time': '00:00:00', 'description': '新的一天开始'},
        {'event_type': 'BEFORE_MARKET', 'time': '09:00:00', 'description': '盘前准备阶段'},
        {'event_type': 'PRE_OPENING_START', 'time': '09:15:00', 'description': '集合竞价开始'},
        {'event_type': 'PRE_OPENING_END', 'time': '09:20:00', 'description': '集合竞价可撤单结束'},
        {'event_type': 'MATCHING_START', 'time': '09:25:00', 'description': '集合竞价撮合开始'},
        {'event_type': 'OPENING_PRICE_DETERMINED', 'time': '09:25:00', 'description': '开盘价确定'},
        {'event_type': 'MARKET_START', 'time': '09:30:00', 'description': '上午连续竞价开始'},
        {'event_type': 'MORNING_END', 'time': '11:30:00', 'description': '上午交易结束'},
        {'event_type': 'AFTERNOON_START', 'time': '13:00:00', 'description': '下午连续竞价开始'},
        {'event_type': 'CLOSING_START', 'time': '14:57:00', 'description': '尾盘集合竞价开始'},
        {'event_type': 'CLOSING_END', 'time': '15:00:00', 'description': '尾盘集合竞价结束'},
        {'event_type': 'CLOSING_PRICE_DETERMINED', 'time': '15:00:00', 'description': '收盘价确定'},
        {'event_type': 'MARKET_END', 'time': '15:00:00', 'description': '市场交易结束'},
        {'event_type': 'DAILY_BAR_CLOSED', 'time': '15:00:00', 'description': '日线数据生成'},
        {'event_type': 'AFTER_MARKET', 'time': '18:00:00', 'description': '盘后处理阶段'},
        {'event_type': 'DAY_END', 'time': '23:59:59', 'description': '一天结束'},
    ],
    
    # 交易时间段配置
    'trading_sessions': [
        {'start': '09:30:00', 'end': '11:30:00', 'name': '上午交易'},
        {'start': '13:00:00', 'end': '15:00:00', 'name': '下午交易'},
    ],
    
    # TRY_MATCH事件配置（用于订单撮合）
    'try_match_events': {
        '1d': [
            {'time': '09:30:00', 'description': '开盘撮合'},
            {'time': '15:00:00', 'description': '收盘撮合'},
        ],
        '1m': {
            'trading_sessions': [
                {'start': '09:30:00', 'end': '11:30:00'},
                {'start': '13:00:00', 'end': '15:00:00'},
            ],
            'interval': 1  # 每分钟撮合一次
        }
    },
    
    # 特殊日期配置（节假日等）
    'holidays': [
        # 2024年法定节假日
        '2024-01-01',  # 元旦
        '2024-02-10',  # 春节假期开始
        '2024-02-11',
        '2024-02-12',
        '2024-02-13',
        '2024-02-14',
        '2024-02-15',
        '2024-02-16',
        '2024-02-17',  # 春节假期结束
        '2024-04-04',  # 清明节
        '2024-04-05',
        '2024-04-06',
        '2024-05-01',  # 劳动节
        '2024-05-02',
        '2024-05-03',
        '2024-06-10',  # 端午节
        '2024-09-15',  # 中秋节
        '2024-09-16',
        '2024-09-17',
        '2024-10-01',  # 国庆节开始
        '2024-10-02',
        '2024-10-03',
        '2024-10-04',
        '2024-10-05',
        '2024-10-06',
        '2024-10-07',  # 国庆节结束
    ]
} 