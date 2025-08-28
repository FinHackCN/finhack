"""
框架级别的默认事件钩子
处理基础的事件处理功能，为所有事件提供通用的处理逻辑
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional


def start_day(context):
    """
    处理每日开始事件 - 全局默认处理
    
    Args:
        context: 回测上下文
    """
    if not context:
        return
    
    current_date = context.current_dt
    if context.logger:
        context.logger.info(f"默认钩子: 开始处理交易日 - {current_date.strftime('%Y-%m-%d')}")
    
    try:
        # 更新当前日期
        context.current_date = current_date.date()
        
        # 重置每日状态
        _reset_daily_state(context)
        
        # 初始化每日全局变量
        _init_daily_globals(context)
        
        # 同步当前时间
        _sync_current_time(context)
        
        if context.logger:
            context.logger.debug(f"默认钩子: 交易日初始化完成")
            
    except Exception as e:
        if context.logger:
            context.logger.error(f"默认钩子: 处理每日开始事件失败 - {str(e)}")


def before_market(context):
    """
    处理盘前事件 - 全局默认处理
    
    Args:
        context: 回测上下文
    """
    if not context:
        return
    
    try:
        if context.logger:
            context.logger.info(f"默认钩子: 盘前准备 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 更新市场状态
        _update_market_state(context, 'pre_market')
        
        # 同步当前时间
        _sync_current_time(context)
        
        if context.logger:
            context.logger.debug(f"默认钩子: 盘前准备完成")
            
    except Exception as e:
        if context.logger:
            context.logger.error(f"默认钩子: 处理盘前事件失败 - {str(e)}")


def morning_start(context):
    """
    处理上午开盘事件 - 全局默认处理
    
    Args:
        context: 回测上下文
    """
    if not context:
        return
    
    try:
        if context.logger:
            context.logger.info(f"默认钩子: 上午开盘 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 更新市场状态
        _update_market_state(context, 'morning_trading')
        
        # 同步当前时间
        _sync_current_time(context)
        
        if context.logger:
            context.logger.debug(f"默认钩子: 上午开盘处理完成")
            
    except Exception as e:
        if context.logger:
            context.logger.error(f"默认钩子: 处理上午开盘事件失败 - {str(e)}")


def morning_end(context):
    """
    处理上午收盘事件 - 全局默认处理
    
    Args:
        context: 回测上下文
    """
    if not context:
        return
    
    try:
        if context.logger:
            context.logger.info(f"默认钩子: 上午收盘 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 更新市场状态
        _update_market_state(context, 'midday_break')
        
        # 同步当前时间
        _sync_current_time(context)
        
        if context.logger:
            context.logger.debug(f"默认钩子: 上午收盘处理完成")
            
    except Exception as e:
        if context.logger:
            context.logger.error(f"默认钩子: 处理上午收盘事件失败 - {str(e)}")


def afternoon_start(context):
    """
    处理下午开盘事件 - 全局默认处理
    
    Args:
        context: 回测上下文
    """
    if not context:
        return
    
    try:
        if context.logger:
            context.logger.info(f"默认钩子: 下午开盘 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 更新市场状态
        _update_market_state(context, 'afternoon_trading')
        
        # 同步当前时间
        _sync_current_time(context)
        
        if context.logger:
            context.logger.debug(f"默认钩子: 下午开盘处理完成")
            
    except Exception as e:
        if context.logger:
            context.logger.error(f"默认钩子: 处理下午开盘事件失败 - {str(e)}")


def afternoon_end(context):
    """
    处理下午收盘事件 - 全局默认处理
    
    Args:
        context: 回测上下文
    """
    if not context:
        return
    
    try:
        if context.logger:
            context.logger.info(f"默认钩子: 下午收盘 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 更新市场状态
        _update_market_state(context, 'after_market')
        
        # 同步当前时间
        _sync_current_time(context)
        
        if context.logger:
            context.logger.debug(f"默认钩子: 下午收盘处理完成")
            
    except Exception as e:
        if context.logger:
            context.logger.error(f"默认钩子: 处理下午收盘事件失败 - {str(e)}")


def daily_bar_closed(context):
    """
    处理日线收盘事件 - 全局默认处理
    
    Args:
        context: 回测上下文
    """
    if not context:
        return
    
    try:
        if context.logger:
            context.logger.info(f"默认钩子: 日线收盘 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 更新市场状态
        _update_market_state(context, 'daily_closed')
        
        # 同步当前时间
        _sync_current_time(context)
        
        # 计算每日统计
        _calculate_daily_stats(context)
        
        if context.logger:
            context.logger.debug(f"默认钩子: 日线收盘处理完成")
            
    except Exception as e:
        if context.logger:
            context.logger.error(f"默认钩子: 处理日线收盘事件失败 - {str(e)}")


def after_market(context):
    """
    处理盘后事件 - 全局默认处理
    
    Args:
        context: 回测上下文
    """
    if not context:
        return
    
    try:
        if context.logger:
            context.logger.info(f"默认钩子: 盘后处理 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 更新市场状态
        _update_market_state(context, 'after_market')
        
        # 同步当前时间
        _sync_current_time(context)
        
        # 盘后清理
        _after_market_cleanup(context)
        
        if context.logger:
            context.logger.debug(f"默认钩子: 盘后处理完成")
            
    except Exception as e:
        if context.logger:
            context.logger.error(f"默认钩子: 处理盘后事件失败 - {str(e)}")


def user_daily(context):
    """
    处理用户每日事件 - 全局默认处理
    
    Args:
        context: 回测上下文
    """
    if not context:
        return
    
    try:
        if context.logger:
            context.logger.debug(f"默认钩子: 用户每日事件 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 同步当前时间
        _sync_current_time(context)
        
        # 如果没有其他处理器，这里可以添加默认的用户事件处理
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"默认钩子: 处理用户每日事件失败 - {str(e)}")


def user_hourly(context):
    """
    处理用户每小时事件 - 全局默认处理
    
    Args:
        context: 回测上下文
    """
    if not context:
        return
    
    try:
        if context.logger:
            context.logger.debug(f"默认钩子: 用户每小时事件 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 同步当前时间
        _sync_current_time(context)
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"默认钩子: 处理用户每小时事件失败 - {str(e)}")


def user_minutely(context):
    """
    处理用户每分钟事件 - 全局默认处理
    
    Args:
        context: 回测上下文
    """
    if not context:
        return
    
    try:
        if context.logger:
            context.logger.debug(f"默认钩子: 用户每分钟事件 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 同步当前时间
        _sync_current_time(context)
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"默认钩子: 处理用户每分钟事件失败 - {str(e)}")


def minute_bar(context):
    """
    处理分钟Bar事件 - 全局默认处理
    
    Args:
        context: 回测上下文
    """
    if not context:
        return
    
    try:
        if context.logger:
            context.logger.debug(f"默认钩子: 分钟Bar事件 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 同步当前时间
        _sync_current_time(context)
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"默认钩子: 处理分钟Bar事件失败 - {str(e)}")


def order_submission(context):
    """
    处理订单提交事件 - 全局默认处理
    
    Args:
        context: 回测上下文
    """
    if not context:
        return
    
    try:
        if context.logger:
            context.logger.debug(f"默认钩子: 订单提交事件 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 同步当前时间
        _sync_current_time(context)
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"默认钩子: 处理订单提交事件失败 - {str(e)}")


def order_fill(context):
    """
    处理订单成交事件 - 全局默认处理
    
    Args:
        context: 回测上下文
    """
    if not context:
        return
    
    try:
        if context.logger:
            context.logger.debug(f"默认钩子: 订单成交事件 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 同步当前时间
        _sync_current_time(context)
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"默认钩子: 处理订单成交事件失败 - {str(e)}")


def _reset_daily_state(context):
    """
    重置每日状态
    
    Args:
        context: 回测上下文
    """
    try:
        # 重置每日全局变量
        if not hasattr(context, 'g'):
            context.g = type('g', (), {})()
        
        # 重置市场状态
        context.g.market_state = 'pre_market'
        context.g.trading_day_start = context.current_dt
        context.g.daily_trades = []
        context.g.daily_orders = []
        context.g.daily_returns = 0.0
        context.g.daily_trade_count = 0
        
        # 重置日内计数器
        context.g.morning_trades = 0
        context.g.afternoon_trades = 0
        context.g.order_count = 0
        context.g.fill_count = 0
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"重置每日状态失败: {str(e)}")


def _init_daily_globals(context):
    """
    初始化每日全局变量
    
    Args:
        context: 回测上下文
    """
    try:
        if not hasattr(context, 'g'):
            context.g = type('g', (), {})()
        
        # 初始化每日统计
        context.g.daily_metrics = {
            'start_time': context.current_dt,
            'end_time': None,
            'total_trades': 0,
            'total_volume': 0.0,
            'total_amount': 0.0,
            'commission': 0.0,
            'tax': 0.0,
            'return': 0.0
        }
        
        # 初始化持仓跟踪
        context.g.position_changes = {}
        context.g.cash_changes = []
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"初始化每日全局变量失败: {str(e)}")


def _sync_current_time(context):
    """
    同步当前时间到各个组件
    
    Args:
        context: 回测上下文
    """
    try:
        # 同步到各个组件
        if hasattr(context, 'data_center') and context.data_center:
            context.data_center.current_time = context.current_dt
        
        if hasattr(context, 'trade_center') and context.trade_center:
            context.trade_center.current_time = context.current_dt
        
        if hasattr(context, 'strategy_manager') and context.strategy_manager:
            context.strategy_manager.current_time = context.current_dt
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"同步当前时间失败: {str(e)}")


def _update_market_state(context, state: str):
    """
    更新市场状态
    
    Args:
        context: 回测上下文
        state: 市场状态
    """
    try:
        if not hasattr(context, 'g'):
            context.g = type('g', (), {})()
        
        context.g.market_state = state
        context.g.market_state_time = context.current_dt
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"更新市场状态失败: {str(e)}")


def _calculate_daily_stats(context):
    """
    计算每日统计数据
    
    Args:
        context: 回测上下文
    """
    try:
        if not hasattr(context, 'g'):
            return
        
        # 更新每日统计
        if hasattr(context.g, 'daily_metrics'):
            context.g.daily_metrics['end_time'] = context.current_dt
            
            # 计算当日收益率
            if hasattr(context, 'account') and context.account:
                initial_value = getattr(context.account, 'initial_cash', 1000000)
                current_value = getattr(context.account, 'total_value', initial_value)
                
                if initial_value > 0:
                    context.g.daily_metrics['return'] = (current_value - initial_value) / initial_value
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"计算每日统计失败: {str(e)}")


def _after_market_cleanup(context):
    """
    盘后清理
    
    Args:
        context: 回测上下文
    """
    try:
        # 清理临时数据
        if hasattr(context, 'g'):
            # 保存每日统计
            if hasattr(context.g, 'daily_metrics'):
                if not hasattr(context.g, 'daily_history'):
                    context.g.daily_history = []
                context.g.daily_history.append(context.g.daily_metrics.copy())
        
        # 清理缓存
        if hasattr(context, 'temp_cache'):
            context.temp_cache.clear()
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"盘后清理失败: {str(e)}") 