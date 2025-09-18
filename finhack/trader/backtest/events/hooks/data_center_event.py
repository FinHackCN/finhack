"""
DataCenter专用事件钩子
处理数据中心相关的事件，包括行情数据预加载、缓存管理等
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import os


def start_day(context):
    """
    处理每日开始事件，执行数据预加载
    
    Args:
        context: 回测上下文
    """
    if not context.data_center:
        return
    
    current_date = context.current_dt
    frequency = context.trade_config.frequency
    market = context.trade_config.market
    
    if context.logger:
        context.logger.info(f"DataCenter钩子: 开始数据预加载 - {current_date.strftime('%Y-%m-%d')} (频率: {frequency})")
    
    try:
        # 获取需要预加载的股票列表
        symbols = _get_preload_symbols(context)
        
        if not symbols:
            if context.logger:
                context.logger.warning("没有找到需要预加载的股票")
            return
        
        # 根据频率确定预加载范围
        start_date, end_date = _calculate_preload_range(current_date, frequency)
        
        if context.logger:
            context.logger.info(f"预加载范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
            context.logger.info(f"预加载股票: {len(symbols)}只股票")
        
        # 预加载数据
        _preload_market_data(context, symbols, start_date, end_date, frequency)
        
        # 预加载因子数据
        _preload_factor_data(context, symbols, start_date, end_date)
        
        # 预加载基础数据
        _preload_basic_data(context, symbols, current_date)
        
        # 更新预加载状态
        _update_preload_status(context, current_date, symbols)
        
        if context.logger:
            context.logger.info(f"DataCenter钩子: 数据预加载完成")
            
    except Exception as e:
        if context.logger:
            context.logger.error(f"DataCenter钩子: 数据预加载失败 - {str(e)}")


def before_market(context):
    """
    处理盘前事件，准备实时数据
    
    Args:
        context: 回测上下文
    """
    if not context.data_center:
        return
    
    try:
        if context.logger:
            context.logger.info(f"DataCenter钩子: 盘前数据准备 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 准备实时数据缓存
        _prepare_realtime_cache(context)
        
        # 预热数据查询
        _warmup_data_queries(context)
        
        # 检查数据完整性
        _check_data_integrity(context)
        
        if context.logger:
            context.logger.debug(f"DataCenter钩子: 盘前数据准备完成")
            
    except Exception as e:
        if context.logger:
            context.logger.error(f"DataCenter钩子: 盘前数据准备失败 - {str(e)}")


def morning_start(context):
    """
    处理上午开盘事件，同步数据
    
    Args:
        context: 回测上下文
    """
    if not context.data_center:
        return
    
    try:
        if context.logger:
            context.logger.info(f"DataCenter钩子: 上午开盘数据同步 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 同步开盘数据
        _sync_opening_data(context)
        
        # 更新价格缓存
        _update_price_cache(context)
        
        if context.logger:
            context.logger.debug(f"DataCenter钩子: 上午开盘数据同步完成")
            
    except Exception as e:
        if context.logger:
            context.logger.error(f"DataCenter钩子: 上午开盘数据同步失败 - {str(e)}")


def afternoon_start(context):
    """
    处理下午开盘事件，同步数据
    
    Args:
        context: 回测上下文
    """
    if not context.data_center:
        return
    
    try:
        if context.logger:
            context.logger.info(f"DataCenter钩子: 下午开盘数据同步 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 同步下午开盘数据
        _sync_afternoon_data(context)
        
        # 更新价格缓存
        _update_price_cache(context)
        
        if context.logger:
            context.logger.debug(f"DataCenter钩子: 下午开盘数据同步完成")
            
    except Exception as e:
        if context.logger:
            context.logger.error(f"DataCenter钩子: 下午开盘数据同步失败 - {str(e)}")


def daily_bar_closed(context):
    """
    处理日线收盘事件，更新日线数据
    
    Args:
        context: 回测上下文
    """
    if not context.data_center:
        return
    
    try:
        if context.logger:
            context.logger.info(f"DataCenter钩子: 日线收盘数据更新 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 更新日线数据
        _update_daily_bar_data(context)
        
        # 计算技术指标
        _calculate_technical_indicators(context)
        
        # 更新价格缓存
        _update_price_cache(context)
        
        # 保存日线数据
        _save_daily_bar_data(context)
        
        if context.logger:
            context.logger.debug(f"DataCenter钩子: 日线数据更新完成")
            
    except Exception as e:
        if context.logger:
            context.logger.error(f"DataCenter钩子: 日线数据更新失败 - {str(e)}")


def minute_bar(context):
    """
    处理分钟Bar事件，更新分钟数据
    
    Args:
        context: 回测上下文
    """
    if not context.data_center:
        return
    
    try:
        if context.logger:
            context.logger.debug(f"DataCenter钩子: 分钟Bar数据更新 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 更新分钟数据
        _update_minute_bar_data(context)
        
        # 更新价格缓存
        _update_price_cache(context)
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"DataCenter钩子: 分钟Bar数据更新失败 - {str(e)}")


def after_market(context):
    """
    处理盘后事件，清理数据缓存
    
    Args:
        context: 回测上下文
    """
    if not context.data_center:
        return
    
    try:
        if context.logger:
            context.logger.info(f"DataCenter钩子: 盘后数据清理 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 清理临时缓存
        _cleanup_temp_cache(context)
        
        # 保存数据统计
        _save_data_statistics(context)
        
        # 压缩历史数据
        _compress_historical_data(context)
        
        if context.logger:
            context.logger.debug(f"DataCenter钩子: 盘后数据清理完成")
            
    except Exception as e:
        if context.logger:
            context.logger.error(f"DataCenter钩子: 盘后数据清理失败 - {str(e)}")


def _get_preload_symbols(context) -> List[str]:
    """
    获取需要预加载的股票列表
    
    Args:
        context: 回测上下文
        
    Returns:
        List[str]: 股票代码列表
    """
    symbols = []
    
    try:
        # 从策略管理器获取股票列表
        if hasattr(context, 'strategy_manager') and context.strategy_manager:
            strategy = context.strategy_manager.get_active_strategy()
            if strategy and hasattr(strategy, 'symbols'):
                symbols.extend(strategy.symbols)
        
        # 从配置获取股票列表
        if hasattr(context, 'trade_config') and hasattr(context.trade_config, 'symbols'):
            symbols.extend(context.trade_config.symbols)
        
        # 从基准获取股票
        if hasattr(context, 'trade_config') and hasattr(context.trade_config, 'benchmark'):
            benchmark = context.trade_config.benchmark
            if benchmark and benchmark not in symbols:
                symbols.append(benchmark)
        
        # 去重
        symbols = list(set(symbols))
        
        # 如果没有找到股票，使用默认股票
        if not symbols:
            symbols = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH']
            
    except Exception as e:
        if context.logger:
            context.logger.error(f"获取预加载股票列表失败: {str(e)}")
        # 返回默认股票
        symbols = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH']
    
    return symbols


def _calculate_preload_range(current_date: datetime, frequency: str) -> tuple:
    """
    计算预加载范围
    
    Args:
        current_date: 当前日期
        frequency: 频率
        
    Returns:
        tuple: (开始日期, 结束日期)
    """
    try:
        if frequency == '1d':
            # 日线频率：预加载当年及前一年数据
            start_date = datetime(current_date.year - 1, 1, 1)
            end_date = datetime(current_date.year, 12, 31)
        elif frequency == '1m':
            # 分钟频率：预加载当年及前一个月数据
            start_date = current_date - timedelta(days=32)
            end_date = current_date + timedelta(days=1)
        elif frequency == '1h':
            # 小时频率：预加载当年及前一周数据
            start_date = current_date - timedelta(days=8)
            end_date = current_date + timedelta(days=1)
        else:
            # 默认：预加载前后一个月数据
            start_date = current_date - timedelta(days=32)
            end_date = current_date + timedelta(days=1)
            
        return start_date, end_date
        
    except Exception as e:
        # 如果计算失败，返回默认范围
        start_date = current_date - timedelta(days=32)
        end_date = current_date + timedelta(days=1)
        return start_date, end_date


def _preload_market_data(context, symbols: List[str], start_date: datetime, end_date: datetime, frequency: str):
    """
    预加载市场数据
    
    Args:
        context: 回测上下文
        symbols: 股票代码列表
        start_date: 开始日期
        end_date: 结束日期
        frequency: 频率
    """
    try:
        if context.logger:
            context.logger.info(f"开始预加载市场数据: {len(symbols)}只股票")
        
        # 批量预加载K线数据
        for symbol in symbols:
            try:
                # 这里调用DataCenter的预加载方法
                if hasattr(context.data_center, 'preload_kline'):
                    context.data_center.preload_kline(symbol, start_date, end_date, frequency)
                
                # 预加载价格数据
                if hasattr(context.data_center, 'preload_price'):
                    context.data_center.preload_price(symbol, start_date, end_date)
                    
            except Exception as e:
                if context.logger:
                    context.logger.warning(f"预加载{symbol}数据失败: {str(e)}")
        
        if context.logger:
            context.logger.info(f"市场数据预加载完成")
            
    except Exception as e:
        if context.logger:
            context.logger.error(f"预加载市场数据失败: {str(e)}")


def _preload_factor_data(context, symbols: List[str], start_date: datetime, end_date: datetime):
    """
    预加载因子数据
    
    Args:
        context: 回测上下文
        symbols: 股票代码列表
        start_date: 开始日期
        end_date: 结束日期
    """
    try:
        if context.logger:
            context.logger.info(f"开始预加载因子数据")
        
        # 批量预加载因子数据
        for symbol in symbols:
            try:
                # 这里调用DataCenter的因子预加载方法
                if hasattr(context.data_center, 'preload_factors'):
                    context.data_center.preload_factors(symbol, start_date, end_date)
                    
            except Exception as e:
                if context.logger:
                    context.logger.warning(f"预加载{symbol}因子数据失败: {str(e)}")
        
        if context.logger:
            context.logger.info(f"因子数据预加载完成")
            
    except Exception as e:
        if context.logger:
            context.logger.error(f"预加载因子数据失败: {str(e)}")


def _preload_basic_data(context, symbols: List[str], current_date: datetime):
    """
    预加载基础数据
    
    Args:
        context: 回测上下文
        symbols: 股票代码列表
        current_date: 当前日期
    """
    try:
        if context.logger:
            context.logger.info(f"开始预加载基础数据")
        
        # 预加载股票基本信息
        if hasattr(context.data_center, 'preload_stock_info'):
            context.data_center.preload_stock_info(symbols)
        
        # 预加载交易日历
        if hasattr(context.data_center, 'preload_trading_calendar'):
            context.data_center.preload_trading_calendar(current_date.year)
        
        if context.logger:
            context.logger.info(f"基础数据预加载完成")
            
    except Exception as e:
        if context.logger:
            context.logger.error(f"预加载基础数据失败: {str(e)}")


def _update_preload_status(context, current_date: datetime, symbols: List[str]):
    """
    更新预加载状态
    
    Args:
        context: 回测上下文
        current_date: 当前日期
        symbols: 股票代码列表
    """
    try:
        if not hasattr(context, 'g'):
            context.g = type('g', (), {})()
        
        # 更新预加载状态
        context.g.data_preloaded = True
        context.g.preload_date = current_date.date()
        context.g.preload_symbols = symbols
        context.g.preload_count = len(symbols)
        
        # 缓存统计
        if hasattr(context.data_center, 'get_cache_stats'):
            cache_stats = context.data_center.get_cache_stats()
            context.g.cache_stats = cache_stats
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"更新预加载状态失败: {str(e)}")


def _prepare_realtime_cache(context):
    """
    准备实时数据缓存
    
    Args:
        context: 回测上下文
    """
    try:
        # 预热缓存
        if hasattr(context.data_center, 'warmup_cache'):
            context.data_center.warmup_cache()
        
        # 清理过期缓存
        if hasattr(context.data_center, 'cleanup_expired_cache'):
            context.data_center.cleanup_expired_cache()
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"准备实时数据缓存失败: {str(e)}")


def _warmup_data_queries(context):
    """
    预热数据查询
    
    Args:
        context: 回测上下文
    """
    try:
        # 预热常用查询
        if hasattr(context.data_center, 'warmup_queries'):
            context.data_center.warmup_queries()
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"预热数据查询失败: {str(e)}")


def _check_data_integrity(context):
    """
    检查数据完整性
    
    Args:
        context: 回测上下文
    """
    try:
        # 检查数据完整性
        if hasattr(context.data_center, 'check_data_integrity'):
            result = context.data_center.check_data_integrity()
            if not result:
                if context.logger:
                    context.logger.warning("数据完整性检查失败")
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"检查数据完整性失败: {str(e)}")


def _sync_opening_data(context):
    """
    同步开盘数据
    
    Args:
        context: 回测上下文
    """
    try:
        # 同步开盘数据
        if hasattr(context.data_center, 'sync_opening_data'):
            context.data_center.sync_opening_data()
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"同步开盘数据失败: {str(e)}")


def _sync_afternoon_data(context):
    """
    同步下午数据
    
    Args:
        context: 回测上下文
    """
    try:
        # 同步下午数据
        if hasattr(context.data_center, 'sync_afternoon_data'):
            context.data_center.sync_afternoon_data()
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"同步下午数据失败: {str(e)}")


def _update_price_cache(context):
    """
    更新价格缓存
    
    Args:
        context: 回测上下文
    """
    try:
        # 更新价格缓存
        if hasattr(context.data_center, 'update_price_cache'):
            context.data_center.update_price_cache()
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"更新价格缓存失败: {str(e)}")


def _update_daily_bar_data(context):
    """
    更新日线数据
    
    Args:
        context: 回测上下文
    """
    try:
        # 更新日线数据
        if hasattr(context.data_center, 'update_daily_bar_data'):
            context.data_center.update_daily_bar_data()
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"更新日线数据失败: {str(e)}")


def _update_minute_bar_data(context):
    """
    更新分钟数据
    
    Args:
        context: 回测上下文
    """
    try:
        # 更新分钟数据
        if hasattr(context.data_center, 'update_minute_bar_data'):
            context.data_center.update_minute_bar_data()
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"更新分钟数据失败: {str(e)}")


def _calculate_technical_indicators(context):
    """
    计算技术指标
    
    Args:
        context: 回测上下文
    """
    try:
        # 计算技术指标
        if hasattr(context.data_center, 'calculate_technical_indicators'):
            context.data_center.calculate_technical_indicators()
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"计算技术指标失败: {str(e)}")


def _save_daily_bar_data(context):
    """
    保存日线数据
    
    Args:
        context: 回测上下文
    """
    try:
        # 保存日线数据
        if hasattr(context.data_center, 'save_daily_bar_data'):
            context.data_center.save_daily_bar_data()
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"保存日线数据失败: {str(e)}")


def _cleanup_temp_cache(context):
    """
    清理临时缓存
    
    Args:
        context: 回测上下文
    """
    try:
        # 清理临时缓存
        if hasattr(context.data_center, 'cleanup_temp_cache'):
            context.data_center.cleanup_temp_cache()
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"清理临时缓存失败: {str(e)}")


def _save_data_statistics(context):
    """
    保存数据统计
    
    Args:
        context: 回测上下文
    """
    try:
        # 保存数据统计
        if hasattr(context.data_center, 'save_data_statistics'):
            context.data_center.save_data_statistics()
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"保存数据统计失败: {str(e)}")


def _compress_historical_data(context):
    """
    压缩历史数据
    
    Args:
        context: 回测上下文
    """
    try:
        # 压缩历史数据
        if hasattr(context.data_center, 'compress_historical_data'):
            context.data_center.compress_historical_data()
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"压缩历史数据失败: {str(e)}") 