#!/usr/bin/env python3
"""
统一数据接口使用示例

演示如何使用新的统一数据接口来获取K线数据、因子数据和参考数据
"""

import sys
import os
from datetime import datetime, date, timedelta

# 添加finhack路径
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from finhack.library.data import get_data_interface, DataInterface
import pandas as pd

def example_basic_usage():
    """基础使用示例"""
    print("=== 基础使用示例 ===")
    
    # 获取数据接口实例
    project_path = "/path/to/your/project"  # 替换为实际项目路径
    data_interface = get_data_interface(project_path)
    
    # 查看缓存统计
    print("缓存统计:", data_interface.get_cache_stats())
    
    print()

def example_kline_data():
    """K线数据获取示例"""
    print("=== K线数据获取示例 ===")
    
    data_interface = get_data_interface()
    
    # 1. 获取单只股票的K线数据
    print("1. 获取单只股票的K线数据")
    klines = data_interface.get_klines(
        codes="000001.SZ",
        market="cn_stock",
        freq="1d",
        start_date="2023-01-01",
        end_date="2023-12-31"
    )
    print(f"数据形状: {klines.shape}")
    print(f"数据列: {klines.columns.tolist()}")
    print(f"前5行:\n{klines.head()}")
    print()
    
    # 2. 获取多只股票的K线数据
    print("2. 获取多只股票的K线数据")
    codes = ["000001.SZ", "000002.SZ", "600000.SH"]
    klines_multi = data_interface.get_klines(
        codes=codes,
        market="cn_stock",
        freq="1d",
        start_date="2023-01-01",
        end_date="2023-01-31",
        fields=["open", "high", "low", "close", "volume"]
    )
    print(f"数据形状: {klines_multi.shape}")
    print(f"股票数量: {len(klines_multi.index.get_level_values('symbol').unique())}")
    print()
    
    # 3. 获取指定时间点的行情快照（智能历史数据查找）
    print("3. 获取指定时间点的行情快照")
    quotes = data_interface.get_quotes(
        codes=codes,
        market="cn_stock",
        freq="1d",
        time=datetime(2023, 6, 30),
        fields=["open", "high", "low", "close", "volume"]
    )
    print(f"行情快照:\n{quotes}")
    
    # 4. 智能历史数据查找示例
    print("4. 智能历史数据查找示例")
    
    # 查询周末数据，自动使用最近交易日数据
    weekend_quotes = data_interface.get_quotes(
        codes=["000001.SZ"],
        market="cn_stock",
        freq="1d",
        time=datetime(2023, 6, 11),  # 周日
        fields=["close"]
    )
    print(f"周末查询结果（自动使用最近交易日）:\n{weekend_quotes}")
    
    # 查询节假日数据
    holiday_quotes = data_interface.get_quotes(
        codes=["000001.SZ"],
        market="cn_stock",
        freq="1d",
        time=datetime(2023, 5, 1),  # 劳动节
        fields=["close"]
    )
    print(f"节假日查询结果（自动使用最近交易日）:\n{holiday_quotes}")
    print()

def example_factor_data():
    """因子数据获取示例"""
    print("=== 因子数据获取示例 ===")
    
    data_interface = get_data_interface()
    
    # 1. 获取矩阵因子数据
    print("1. 获取矩阵因子数据")
    try:
        factors = data_interface.get_factors(
            factor_names=["pe_ratio", "pb_ratio", "market_cap"],
            codes=["000001.SZ", "000002.SZ"],
            market="cn_stock",
            freq="1d",
            start_date="2023-01-01",
            end_date="2023-01-31",
            factor_type="matrix"
        )
        print(f"因子数据形状: {factors.shape}")
        print(f"因子列: {factors.columns.tolist()}")
        print(f"前5行:\n{factors.head()}")
    except Exception as e:
        print(f"获取因子数据失败: {e}")
    print()
    
    # 2. 获取向量因子数据
    print("2. 获取向量因子数据")
    try:
        vector_factors = data_interface.get_factors(
            factor_names=["momentum", "volatility"],
            market="cn_stock",
            freq="1d",
            start_date="2023-01-01",
            end_date="2023-01-31",
            factor_type="vector"
        )
        print(f"向量因子数据形状: {vector_factors.shape}")
        print(f"向量因子列: {vector_factors.columns.tolist()}")
    except Exception as e:
        print(f"获取向量因子数据失败: {e}")
    print()

def example_reference_data():
    """参考数据获取示例"""
    print("=== 参考数据获取示例 ===")
    
    data_interface = get_data_interface()
    
    # 1. 获取股票列表
    print("1. 获取股票列表")
    stock_list = data_interface.get_stock_list("cn_stock")
    print(f"股票列表数量: {len(stock_list)}")
    if not stock_list.empty:
        print(f"股票列表列: {stock_list.columns.tolist()}")
        print(f"前5只股票:\n{stock_list.head()}")
    print()
    
    # 2. 获取复权因子
    print("2. 获取复权因子")
    adj_factors = data_interface.get_adj_factors(
        market="cn_stock",
        codes=["000001.SZ", "000002.SZ"],
        start_date="2023-01-01",
        end_date="2023-12-31"
    )
    print(f"复权因子数量: {len(adj_factors)}")
    if not adj_factors.empty:
        print(f"复权因子列: {adj_factors.columns.tolist()}")
        print(f"前5条记录:\n{adj_factors.head()}")
    print()
    
    # 3. 获取交易日历
    print("3. 获取交易日历")
    trading_calendar = data_interface.get_trading_calendar(
        market="cn_stock",
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31)
    )
    print(f"交易日数量: {len(trading_calendar)}")
    print(f"前10个交易日: {trading_calendar[:10]}")
    print(f"最后10个交易日: {trading_calendar[-10:]}")
    print()

def example_cache_management():
    """缓存管理示例"""
    print("=== 缓存管理示例 ===")
    
    data_interface = get_data_interface()
    
    # 查看缓存统计
    print("当前缓存统计:")
    stats = data_interface.get_cache_stats()
    for cache_name, cache_stats in stats.items():
        print(f"  {cache_name}: {cache_stats['size']}/{cache_stats['max_size']}")
    print()
    
    # 清理特定类型的缓存
    print("清理K线缓存...")
    data_interface.clear_cache('kline')
    
    # 清理所有缓存
    print("清理所有缓存...")
    data_interface.clear_cache()
    
    # 再次查看缓存统计
    print("清理后缓存统计:")
    stats = data_interface.get_cache_stats()
    for cache_name, cache_stats in stats.items():
        print(f"  {cache_name}: {cache_stats['size']}/{cache_stats['max_size']}")
    print()

def example_adjustment_features():
    """复权功能示例"""
    print("=== 复权功能示例 ===")
    
    data_interface = get_data_interface()
    
    code = "000001.SZ"
    
    # 1. 对比不同复权方式的K线数据
    print("1. 对比不同复权方式的K线数据")
    
    # 获取不复权数据
    klines_none = data_interface.get_klines(
        codes=[code],
        market="cn_stock",
        freq="1d",
        start_date="2023-01-01",
        end_date="2023-01-31",
        adj_type="none"
    )
    
    # 获取前复权数据  
    klines_front = data_interface.get_klines(
        codes=[code],
        market="cn_stock",
        freq="1d",
        start_date="2023-01-01",
        end_date="2023-01-31",
        adj_type="front"
    )
    
    # 获取后复权数据
    klines_back = data_interface.get_klines(
        codes=[code],
        market="cn_stock",
        freq="1d",
        start_date="2023-01-01",
        end_date="2023-01-31",
        adj_type="back"
    )
    
    if not klines_none.empty and not klines_front.empty and not klines_back.empty:
        print(f"数据日期范围: {klines_none.index.get_level_values('time').min()} 到 {klines_none.index.get_level_values('time').max()}")
        
        # 选择一个样本日期进行对比
        sample_date = klines_none.index.get_level_values('time')[0]
        
        try:
            none_price = klines_none.loc[(sample_date, code), 'close']
            front_price = klines_front.loc[(sample_date, code), 'close']
            back_price = klines_back.loc[(sample_date, code), 'close']
            
            print(f"样本日期: {sample_date}")
            print(f"原始价格: {none_price:.2f}")
            print(f"前复权价格: {front_price:.2f}")
            print(f"后复权价格: {back_price:.2f}")
        except KeyError as e:
            print(f"数据访问错误: {e}")
    else:
        print("未获取到K线数据，可能是数据不存在或市场不可用")
    
    print()
    
    # 2. 对比不同复权方式的行情快照
    print("2. 对比不同复权方式的行情快照")
    
    query_time = datetime(2023, 6, 15)
    
    # 不复权行情
    quotes_none = data_interface.get_quotes(
        codes=[code],
        market="cn_stock",
        freq="1d",
        time=query_time,
        adj_type="none"
    )
    
    # 前复权行情
    quotes_front = data_interface.get_quotes(
        codes=[code],
        market="cn_stock",
        freq="1d",
        time=query_time,
        adj_type="front"
    )
    
    # 后复权行情
    quotes_back = data_interface.get_quotes(
        codes=[code],
        market="cn_stock",
        freq="1d",
        time=query_time,
        adj_type="back"
    )
    
    print(f"查询时间: {query_time}")
    print(f"不复权行情: {quotes_none}")
    print(f"前复权行情: {quotes_front}")
    print(f"后复权行情: {quotes_back}")
    print()

def example_save_factors():
    """保存因子数据示例"""
    print("=== 保存因子数据示例 ===")
    
    data_interface = get_data_interface()
    
    # 创建示例因子数据
    dates = pd.date_range('2023-01-01', '2023-01-10', freq='D')
    codes = ['000001.SZ', '000002.SZ', '600000.SH']
    
    # 创建MultiIndex
    index = pd.MultiIndex.from_product([dates, codes], names=['time', 'code'])
    
    # 创建示例数据
    import numpy as np
    np.random.seed(42)
    
    df_factors = pd.DataFrame({
        'factor1': np.random.randn(len(index)),
        'factor2': np.random.randn(len(index)),
        'factor3': np.random.randn(len(index))
    }, index=index)
    
    print(f"示例因子数据形状: {df_factors.shape}")
    print(f"示例因子数据:\n{df_factors.head()}")
    
    # 保存因子数据
    try:
        success = data_interface.save_factors(
            df_factors=df_factors,
            factor_list=['factor1', 'factor2', 'factor3'],
            market='cn_stock',
            freq='1d',
            factor_type='matrix'
        )
        if success:
            print("因子数据保存成功")
        else:
            print("因子数据保存失败")
    except Exception as e:
        print(f"保存因子数据异常: {e}")
    print()

def example_performance_comparison():
    """性能对比示例"""
    print("=== 性能对比示例 ===")
    
    data_interface = get_data_interface()
    
    codes = ["000001.SZ", "000002.SZ", "600000.SH", "600036.SH", "000858.SZ"]
    
    # 第一次获取（无缓存）
    print("第一次获取K线数据（无缓存）...")
    start_time = datetime.now()
    klines1 = data_interface.get_klines(
        codes=codes,
        market="cn_stock",
        freq="1d",
        start_date="2023-01-01",
        end_date="2023-12-31",
        use_cache=True
    )
    time1 = (datetime.now() - start_time).total_seconds()
    print(f"耗时: {time1:.2f} 秒, 数据形状: {klines1.shape}")
    
    # 第二次获取（有缓存）
    print("第二次获取相同K线数据（有缓存）...")
    start_time = datetime.now()
    klines2 = data_interface.get_klines(
        codes=codes,
        market="cn_stock",
        freq="1d",
        start_date="2023-01-01",
        end_date="2023-12-31",
        use_cache=True
    )
    time2 = (datetime.now() - start_time).total_seconds()
    print(f"耗时: {time2:.2f} 秒, 数据形状: {klines2.shape}")
    
    # 性能提升
    if time1 > 0:
        speedup = time1 / time2 if time2 > 0 else float('inf')
        print(f"性能提升: {speedup:.1f}x")
    print()

def example_error_handling():
    """错误处理示例"""
    print("=== 错误处理示例 ===")
    
    data_interface = get_data_interface()
    
    # 1. 不存在的股票代码
    print("1. 获取不存在的股票代码...")
    try:
        klines = data_interface.get_klines(
            codes=["NONEXISTENT.CODE"],
            market="cn_stock",
            freq="1d",
            start_date="2023-01-01",
            end_date="2023-01-31"
        )
        print(f"结果: {klines.shape}")
    except Exception as e:
        print(f"异常: {e}")
    print()
    
    # 2. 不存在的因子
    print("2. 获取不存在的因子...")
    try:
        factors = data_interface.get_factors(
            factor_names=["nonexistent_factor"],
            market="cn_stock",
            freq="1d",
            start_date="2023-01-01",
            end_date="2023-01-31"
        )
        print(f"结果: {factors.shape}")
    except Exception as e:
        print(f"异常: {e}")
    print()
    
    # 3. 无效的日期范围
    print("3. 使用无效的日期范围...")
    try:
        klines = data_interface.get_klines(
            codes=["000001.SZ"],
            market="cn_stock",
            freq="1d",
            start_date="2025-01-01",  # 未来日期
            end_date="2025-12-31"
        )
        print(f"结果: {klines.shape}")
    except Exception as e:
        print(f"异常: {e}")
    print()

def main():
    """主函数"""
    print("统一数据接口使用示例")
    print("=" * 50)
    print()
    
    try:
        # 基础示例
        example_basic_usage()
        
        # K线数据示例
        example_kline_data()
        
        # 因子数据示例
        example_factor_data()
        
        # 参考数据示例
        example_reference_data()
        
        # 缓存管理示例
        example_cache_management()
        
        # 复权功能示例
        example_adjustment_features()
        
        # 保存因子数据示例
        example_save_factors()
        
        # 性能对比示例
        example_performance_comparison()
        
        # 错误处理示例
        example_error_handling()
        
    except Exception as e:
        print(f"示例执行异常: {e}")
        import traceback
        traceback.print_exc()
    
    print("示例执行完成")

if __name__ == "__main__":
    main()