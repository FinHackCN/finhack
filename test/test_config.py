"""
数据接口测试配置

用户可以根据实际数据可用性调整以下配置
"""

# 测试用的股票代码（确保这些代码在您的数据中存在）
TEST_CODES = {
    'cn_stock': [
        "000001.SZ",  # 平安银行
        "000002.SZ",  # 万科A  
        "600000.SH",  # 浦发银行
        "600036.SH",  # 招商银行
        "000858.SZ",  # 五粮液
    ],
    'cn_index': [
        "000001.SH",  # 上证指数
        "399001.SZ",  # 深证成指
        "399006.SZ",  # 创业板指
    ],
    'cn_fund': [
        "159919.SZ",  # 沪深300ETF
        "510050.SH",  # 50ETF
        "510300.SH",  # 300ETF
    ]
}

# 测试用的市场列表
TEST_MARKETS = [
    "cn_stock",
    # "cn_index",    # 如果有指数数据，取消注释
    # "cn_fund",     # 如果有基金数据，取消注释
    # "hk_stock",    # 如果有港股数据，取消注释
]

# 测试用的日期范围
TEST_DATE_RANGES = [
    ("2023-01-01", "2023-01-31"),  # 1个月
    ("2023-01-01", "2023-03-31"),  # 3个月
    ("2023-01-01", "2023-06-30"),  # 6个月
]

# 测试用的因子名称（根据实际可用因子调整）
TEST_FACTORS = {
    'matrix': [
        "pe_ratio",      # 市盈率
        "pb_ratio",      # 市净率
        "market_cap",    # 市值
        "turnover",      # 换手率
        "roc",           # ROC
    ],
    'vector': [
        "momentum",      # 动量
        "volatility",    # 波动率
        "beta",          # Beta
        "alpha",         # Alpha
    ]
}

# 复权测试配置
ADJUSTMENT_TYPES = ['none', 'front', 'back']

# 测试用的字段组合
FIELD_COMBINATIONS = [
    ['close'],
    ['open', 'close'],
    ['open', 'high', 'low', 'close'],
    ['open', 'high', 'low', 'close', 'volume'],
    ['open', 'high', 'low', 'close', 'volume', 'amount']
]

# 性能测试配置
PERFORMANCE_CONFIG = {
    'large_stock_count': 20,        # 大量股票测试的股票数量
    'large_timerange_years': 2,     # 长时间范围测试的年数
    'concurrent_threads': 5,        # 并发测试的线程数
    'cache_test_iterations': 3,     # 缓存测试的迭代次数
}

# 超时配置（秒）
TIMEOUT_CONFIG = {
    'single_query': 30,             # 单次查询超时
    'large_query': 120,             # 大数据量查询超时
    'concurrent_query': 60,         # 并发查询总超时
}

# 错误处理测试配置
ERROR_TEST_CONFIG = {
    'invalid_codes': [
        ["INVALID.CODE"],
        ["999999.SZ"],
        [""],
        # [None],  # 可能导致程序崩溃，谨慎测试
    ],
    'invalid_date_ranges': [
        ("2023-12-31", "2023-01-01"),  # 结束日期早于开始日期
        ("invalid-date", "2023-01-31"),  # 无效日期格式
        ("2023-01-01", "invalid-date"),  # 无效日期格式
    ],
    'invalid_markets': [
        "invalid_market",
        "",
        # None,  # 可能导致程序崩溃，谨慎测试
    ],
    'invalid_factors': [
        ["non_existent_factor"],
        [""],
        # [None],  # 可能导致程序崩溃，谨慎测试
    ]
}

# 历史数据查找测试时间点
HISTORICAL_LOOKUP_TIMES = [
    ("2023-06-17", "weekend_saturday"),    # 周六
    ("2023-06-18", "weekend_sunday"),      # 周日
    ("2023-05-01", "holiday_labor_day"),   # 劳动节
    ("2023-10-01", "holiday_national"),    # 国庆节
    ("2023-06-15", "after_hours"),         # 收盘后
    ("2023-06-15", "before_hours"),        # 开盘前
]

# 缓存测试配置
CACHE_TEST_CONFIG = {
    'test_cache_hit': True,         # 测试缓存命中
    'test_cache_miss': True,        # 测试缓存未命中
    'test_cache_clear': True,       # 测试缓存清理
    'test_cache_stats': True,       # 测试缓存统计
}

def get_test_codes(market='cn_stock', count=None):
    """获取指定市场的测试代码"""
    codes = TEST_CODES.get(market, TEST_CODES['cn_stock'])
    if count:
        return codes[:count]
    return codes

def get_test_factors(factor_type='matrix', count=None):
    """获取指定类型的测试因子"""
    factors = TEST_FACTORS.get(factor_type, TEST_FACTORS['matrix'])
    if count:
        return factors[:count]
    return factors

def is_data_available(market='cn_stock'):
    """检查指定市场的数据是否可用"""
    # 这里可以添加实际的数据可用性检查逻辑
    # 例如检查数据文件是否存在
    import os
    from runtime.constant import DATA_DIR
    
    market_dir = os.path.join(DATA_DIR, 'market', 'kline', 'codebased', market)
    return os.path.exists(market_dir) and os.listdir(market_dir)

def get_available_markets():
    """获取可用的市场列表"""
    available_markets = []
    for market in TEST_MARKETS:
        if is_data_available(market):
            available_markets.append(market)
    return available_markets or ['cn_stock']  # 至少返回一个默认市场

# 动态配置：根据实际数据可用性调整测试参数
def get_dynamic_config():
    """获取动态测试配置"""
    config = {
        'available_markets': get_available_markets(),
        'test_codes_per_market': {},
        'recommended_date_range': None,
    }
    
    # 为每个可用市场准备测试代码
    for market in config['available_markets']:
        config['test_codes_per_market'][market] = get_test_codes(market)
    
    # 推荐的日期范围（最近3个月）
    from datetime import datetime, timedelta
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    config['recommended_date_range'] = (
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d')
    )
    
    return config