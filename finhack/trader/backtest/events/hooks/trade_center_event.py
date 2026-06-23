"""
TradeCenter专用事件钩子
处理交易中心相关的事件，包括分红送股、订单撮合等
"""

import os
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any


def start_day(context):
    """
    处理每日开始事件，执行分红送股等公司行为
    
    Args:
        context: 回测上下文
    """
    if not context.trade_center:
        return
    
    current_date = context.current_dt
    market = context.trade_config.market
    
    if context.logger:
        context.logger.info(f"TradeCenter钩子: 开始处理交易日事件 - {current_date.strftime('%Y-%m-%d')}")
    
    try:
        # 对于cn_stock类型的回测，处理分红送股
        if market == 'cn_stock':
            _process_dividend_stock_split(context, current_date)
        
        # 处理T+1解冻
        _process_t1_unlock(context)
        
        # 处理待成交订单
        _process_pending_orders(context)
        
        # 重置每日交易计数
        _reset_daily_counters(context)
        
        # 更新持仓市值
        _update_positions_market_value(context)
        
        # 检查风险控制
        _check_risk_control(context)
        
        if context.logger:
            context.logger.info(f"交易日事件处理完成")
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"TradeCenter事件处理失败: {str(e)}")


def _process_dividend_stock_split(context, current_date: datetime):
    """
    处理分红送股事件
    
    Args:
        context: 回测上下文
        current_date: 当前日期
    """
    try:
        # 获取当前持仓
        positions = context.trade_center.get_positions()
        
        if not positions:
            return
        
        # 获取分红送股数据
        dividend_data = _get_dividend_data(context, current_date)
        
        if not dividend_data:
            return
        
        processed_count = 0
        
        for symbol, position in positions.items():
            if position.volume <= 0:
                continue
            
            # 检查是否有分红送股
            if symbol in dividend_data:
                div_info = dividend_data[symbol]
                _apply_dividend_stock_split(context, symbol, position, div_info)
                processed_count += 1
        
        if processed_count > 0 and context.logger:
            context.logger.info(f"处理分红送股事件: {processed_count}只股票")
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"处理分红送股失败: {str(e)}")


def _get_dividend_data(context, current_date: datetime) -> Dict[str, Any]:
    """
    从数据库获取分红送股数据

    Args:
        context: 回测上下文
        current_date: 当前日期

    Returns:
        Dict[str, Any]: 分红送股数据 {symbol: {cash_dividend, stock_dividend, ex_date, ...}}
    """
    dividend_data = {}

    try:
        # 获取数据库路径
        db_path = os.path.join(
            context.project_path,
            'data/db/tushare.sqlite'
        )

        if not os.path.exists(db_path):
            if context.logger:
                context.logger.warning(f"分红数据库不存在: {db_path}")
            return dividend_data

        # 查询当日除权的分红送股数据
        ex_date_str = current_date.strftime('%Y%m%d')

        query = f"""
        SELECT ts_code, cash_div_tax, stk_div, stk_bo_rate, ex_date, record_date
        FROM astock_finance_dividend
        WHERE ex_date = '{ex_date_str}'
          AND div_proc = '实施'
        """

        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(query, conn)
        conn.close()

        if df.empty:
            return dividend_data

        # 构建分红送股数据字典
        for _, row in df.iterrows():
            symbol = row['ts_code']

            # 现金分红（扣税后）
            cash_dividend = row.get('cash_div_tax', 0) or 0

            # 送股比例（每股送多少股）
            stk_div = row.get('stk_div', 0) or 0

            # 转增股比例（每股转增多少股）
            stk_bo_rate = row.get('stk_bo_rate', 0) or 0
            if isinstance(stk_bo_rate, str):
                try:
                    stk_bo_rate = float(stk_bo_rate)
                except:
                    stk_bo_rate = 0

            # 总送股比例 = 送股 + 转增
            stock_dividend = stk_div + (stk_bo_rate / 10 if stk_bo_rate else 0)

            # 只有有分红或送股才记录
            if cash_dividend > 0 or stock_dividend > 0:
                dividend_data[symbol] = {
                    'type': 'dividend_stock_split',
                    'cash_dividend': float(cash_dividend),  # 每股现金分红
                    'stock_dividend': float(stock_dividend),  # 每股送股
                    'ex_date': row.get('ex_date', ''),
                    'record_date': row.get('record_date', '')
                }

                if context.logger:
                    context.logger.info(
                        f"分红送股数据: {symbol} 现金{cash_dividend:.4f}元/股, "
                        f"送股{stock_dividend:.4f}股/股"
                    )

    except Exception as e:
        if context.logger:
            context.logger.error(f"获取分红送股数据失败: {str(e)}")
            import traceback
            context.logger.error(traceback.format_exc())

    return dividend_data


def _apply_dividend_stock_split(context, symbol: str, position, div_info: Dict[str, Any]):
    """
    应用分红送股
    
    Args:
        context: 回测上下文
        symbol: 股票代码
        position: 持仓对象
        div_info: 分红送股信息
    """
    try:
        cash_dividend = div_info.get('cash_dividend', 0)
        stock_dividend = div_info.get('stock_dividend', 0)
        
        # 现金分红
        if cash_dividend > 0:
            dividend_amount = position.volume * cash_dividend
            
            # 更新账户现金
            context.account.cash += dividend_amount
            context.account.available_cash += dividend_amount
            
            # 记录分红记录
            if not hasattr(context, 'logs'):
                context.logs = {'dividend_list': [], 'stock_dividend_list': []}
            
            context.logs['dividend_list'].append({
                'symbol': symbol,
                'type': 'cash_dividend',
                'amount': dividend_amount,
                'per_share': cash_dividend,
                'quantity': position.volume,
                'date': context.current_dt.strftime('%Y-%m-%d'),
                'timestamp': context.current_dt.isoformat()
            })
            
            if context.logger:
                context.logger.info(f"分红处理: {symbol} 现金分红 {dividend_amount:.2f}元 (每股{cash_dividend:.2f}元)")
        
        # 送股
        if stock_dividend > 0:
            bonus_shares = int(position.volume * stock_dividend)
            if bonus_shares > 0:
                # 增加持仓数量
                old_quantity = position.volume
                position.volume += bonus_shares
                position.available_volume += bonus_shares
                
                # 调整成本价（送股后成本价下降）
                position.cost_price = (position.cost_price * old_quantity) / position.volume
                
                # 更新兼容字段
                position.volume = position.volume
                position.available_volume = position.available_volume
                
                # 记录送股记录
                if not hasattr(context, 'logs'):
                    context.logs = {'dividend_list': [], 'stock_dividend_list': []}
                
                context.logs['stock_dividend_list'].append({
                    'symbol': symbol,
                    'type': 'stock_dividend',
                    'bonus_shares': bonus_shares,
                    'per_share_ratio': stock_dividend,
                    'old_quantity': old_quantity,
                    'new_quantity': position.volume,
                    'date': context.current_dt.strftime('%Y-%m-%d'),
                    'timestamp': context.current_dt.isoformat()
                })
                
                if context.logger:
                    context.logger.info(f"送股处理: {symbol} 送股 {bonus_shares}股 (每股送{stock_dividend:.2f}股)")
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"应用分红送股失败 {symbol}: {str(e)}")


def _process_t1_unlock(context):
    """
    处理T+1解冻
    
    Args:
        context: 回测上下文
    """
    try:
        # 获取当前持仓
        positions = context.trade_center.get_positions()
        
        if not positions:
            return
        
        unlock_count = 0
        current_date = context.current_dt.date()
        
        for symbol, position in positions.items():
            if position.frozen_volume > 0:
                # 检查是否有T+1解冻的股票
                if hasattr(position, 't1_unlock_date'):
                    unlock_date = position.t1_unlock_date
                    if isinstance(unlock_date, str):
                        unlock_date = datetime.strptime(unlock_date, '%Y-%m-%d').date()
                    
                    if current_date >= unlock_date:
                        # 解冻股票
                        unlock_quantity = position.frozen_volume
                        position.available_volume += unlock_quantity
                        position.frozen_volume = 0
                        position.available_volume = position.available_volume
                        
                        # 移除解冻日期
                        delattr(position, 't1_unlock_date')
                        
                        unlock_count += 1
                        
                        if context.logger:
                            context.logger.info(f"T+1解冻: {symbol} 解冻 {unlock_quantity}股")
        
        if unlock_count > 0 and context.logger:
            context.logger.info(f"T+1解冻完成: {unlock_count}只股票")
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"T+1解冻处理失败: {str(e)}")


def _process_pending_orders(context):
    """
    处理待成交订单
    
    Args:
        context: 回测上下文
    """
    try:
        # 获取待成交订单
        pending_orders = context.trade_center.get_pending_orders()
        
        if not pending_orders:
            return
        
        if context.logger:
            context.logger.info(f"处理待成交订单: {len(pending_orders)}个")
        
        filled_count = 0
        
        for order in pending_orders:
            if _try_match_order(context, order):
                filled_count += 1
        
        if filled_count > 0 and context.logger:
            context.logger.info(f"订单匹配完成: {filled_count}个订单成交")
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"处理待成交订单失败: {str(e)}")


def _try_match_order(context, order):
    """
    尝试匹配订单
    
    Args:
        context: 回测上下文
        order: 订单对象
        
    Returns:
        bool: 是否成功匹配
    """
    try:
        # 获取当前价格
        current_price = context.data_center.get_price(order.symbol)
        if not current_price or current_price <= 0:
            return False
        
        # 检查价格限制
        if not _check_price_limit(context, order, current_price):
            return False
        
        # 检查市场流动性
        max_fill_quantity = _get_max_fill_quantity(context, order, current_price)
        if max_fill_quantity <= 0:
            return False
        
        # 计算成交数量
        remaining_quantity = order.quantity - order.filled_quantity
        fill_quantity = min(remaining_quantity, max_fill_quantity)
        
        # 计算成交价格（考虑滑点）
        fill_price = _calculate_fill_price(context, order, current_price)
        
        # 执行成交
        success = context.trade_center._fill_order(order, fill_quantity, fill_price)
        
        if success:
            if context.logger:
                context.logger.info(f"订单成交: {order.symbol} {order.side} {fill_quantity}股 @ {fill_price:.2f}")
            return True
        
        return False
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"订单匹配失败 {order.symbol}: {str(e)}")
        return False


def _check_price_limit(context, order, current_price: float) -> bool:
    """
    检查价格限制
    
    Args:
        context: 回测上下文
        order: 订单对象
        current_price: 当前价格
        
    Returns:
        bool: 是否通过价格限制检查
    """
    try:
        # 市价单直接通过
        if order.order_type == 'market':
            return True
        
        # 限价单需要检查价格
        if order.order_type == 'limit' and order.price:
            if order.side.value == 'buy':
                # 买入限价单：当前价格 <= 限价
                return current_price <= order.price
            elif order.side.value == 'sell':
                # 卖出限价单：当前价格 >= 限价
                return current_price >= order.price
        
        # 检查涨跌停限制
        if context.trade_config.market == 'cn_stock':
            # 模拟涨跌停检查（实际应该从数据中获取）
            yesterday_price = _get_yesterday_price(context, order.symbol)
            if yesterday_price and yesterday_price > 0:
                limit_up = yesterday_price * 1.10
                limit_down = yesterday_price * 0.90
                
                if current_price >= limit_up or current_price <= limit_down:
                    if context.logger:
                        context.logger.warning(f"价格涨跌停限制: {order.symbol} 当前价格 {current_price:.2f}")
                    return False
        
        return True
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"价格限制检查失败 {order.symbol}: {str(e)}")
        return False


def _get_yesterday_price(context, symbol: str) -> float:
    """
    获取昨日收盘价
    
    Args:
        context: 回测上下文
        symbol: 股票代码
        
    Returns:
        float: 昨日收盘价
    """
    try:
        # 获取昨日K线数据
        yesterday = context.current_dt - timedelta(days=1)
        end_date = yesterday.strftime('%Y-%m-%d')
        start_date = (yesterday - timedelta(days=5)).strftime('%Y-%m-%d')
        
        data = context.data_center.get_kline(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            period='1d'
        )
        
        if data is not None and not data.empty:
            return data['close'].iloc[-1]
        
        return None
        
    except Exception as e:
        return None


def _get_max_fill_quantity(context, order, current_price: float) -> float:
    """
    获取最大成交数量
    
    Args:
        context: 回测上下文
        order: 订单对象
        current_price: 当前价格
        
    Returns:
        float: 最大成交数量
    """
    try:
        # 基础流动性检查
        base_liquidity = 100000  # 基础流动性：10万股
        
        # 根据股票代码调整流动性
        if order.symbol.startswith('000001') or order.symbol.startswith('600000'):
            base_liquidity = 500000  # 大盘股流动性更好
        elif order.symbol.startswith('300'):
            base_liquidity = 50000   # 创业板流动性较差
        
        # 考虑订单金额对流动性的影响
        order_value = order.quantity * current_price
        if order_value > 1000000:  # 大额订单
            base_liquidity = int(base_liquidity * 0.5)
        
        # 随机化流动性（模拟市场波动）
        import random
        liquidity_factor = random.uniform(0.8, 1.2)
        max_quantity = int(base_liquidity * liquidity_factor)
        
        # 不能超过订单剩余数量
        remaining_quantity = order.quantity - order.filled_quantity
        return min(max_quantity, remaining_quantity)
        
    except Exception as e:
        return 0


def _calculate_fill_price(context, order, current_price: float) -> float:
    """
    计算成交价格（考虑滑点）
    
    Args:
        context: 回测上下文
        order: 订单对象
        current_price: 当前价格
        
    Returns:
        float: 成交价格
    """
    try:
        # 获取滑点设置
        slippage = context.trade_config.slippage if hasattr(context.trade_config, 'slippage') else 0.005
        
        # 计算滑点
        if order.side.value == 'buy':
            # 买入时价格向上滑点
            fill_price = current_price * (1 + slippage)
        else:
            # 卖出时价格向下滑点
            fill_price = current_price * (1 - slippage)
        
        # 限价单不能超过限价
        if order.order_type == 'limit' and order.price:
            if order.side.value == 'buy':
                fill_price = min(fill_price, order.price)
            else:
                fill_price = max(fill_price, order.price)
        
        return round(fill_price, 2)
        
    except Exception as e:
        return current_price


def _reset_daily_counters(context):
    """
    重置每日交易计数
    
    Args:
        context: 回测上下文
    """
    try:
        # 重置交易计数
        if hasattr(context, 'g'):
            context.g.daily_trade_count = 0
            context.g.daily_order_count = 0
            context.g.daily_turnover = 0.0
        
        # 重置风险控制计数
        if hasattr(context, 'risk_control'):
            context.risk_control.daily_loss_count = 0
            context.risk_control.daily_position_changes = 0
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"重置每日计数失败: {str(e)}")


def _update_positions_market_value(context):
    """
    更新持仓市值

    Args:
        context: 回测上下文
    """
    try:
        from finhack.trader.backtest.constants import SHORT_POSITION_SUFFIX
        from finhack.trader.backtest.models.enums import PositionSide

        positions = context.trade_center.get_positions()

        if not positions:
            return

        total_market_value = 0.0

        for symbol, position in positions.items():
            # 获取实际行情代码（做空持仓key带_SHORT后缀）
            quote_symbol = symbol[:-len(SHORT_POSITION_SUFFIX)] if symbol.endswith(SHORT_POSITION_SUFFIX) else symbol
            current_price = context.data_center.get_price(quote_symbol)

            if not current_price or current_price <= 0:
                continue

            position.last_price = current_price
            position.last_sale_price = current_price

            # 使用Position模型方法更新（正确处理contract_multiplier和多空方向）
            position.update_market_price(current_price)
            position.total_value = position.market_value

            total_market_value += position.market_value

        # 更新账户总市值
        context.account.total_value = context.account.cash + total_market_value

        if context.logger:
            context.logger.debug(f"持仓市值更新: 总市值 {total_market_value:.2f}")

    except Exception as e:
        if context.logger:
            context.logger.error(f"更新持仓市值失败: {str(e)}")


def _check_risk_control(context):
    """
    检查风险控制
    
    Args:
        context: 回测上下文
    """
    try:
        # 检查现金是否为负
        if context.account.cash < 0:
            if context.logger:
                context.logger.warning(f"现金余额为负: {context.account.cash:.2f}")
        
        # 检查持仓集中度
        positions = context.trade_center.get_positions()
        if positions:
            total_value = context.account.total_value
            if total_value > 0:
                max_position_ratio = 0.0
                for symbol, position in positions.items():
                    if position.volume > 0:
                        position_ratio = position.market_value / total_value
                        if position_ratio > max_position_ratio:
                            max_position_ratio = position_ratio
                
                # 检查单个持仓是否过于集中
                if max_position_ratio > 0.3:  # 30%的集中度警告
                    if context.logger:
                        context.logger.warning(f"持仓集中度过高: {max_position_ratio:.1%}")
        
        # 检查杠杆比例
        if hasattr(context.account, 'margin') and context.account.margin > 0:
            leverage_ratio = context.account.total_value / context.account.cash
            if leverage_ratio > 2.0:  # 2倍杠杆警告
                if context.logger:
                    context.logger.warning(f"杠杆比例过高: {leverage_ratio:.1f}")
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"风险控制检查失败: {str(e)}")


def before_market(context):
    """
    处理盘前事件
    
    Args:
        context: 回测上下文
    """
    if context.logger:
        context.logger.debug(f"TradeCenter钩子: 盘前处理 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 盘前准备工作
    _prepare_for_trading(context)


def _prepare_for_trading(context):
    """
    盘前准备工作
    
    Args:
        context: 回测上下文
    """
    try:
        # 更新交易状态
        context.trade_center.is_trading = True
        
        # 检查账户状态
        _check_account_status(context)
        
        # 检查持仓状态
        _check_positions_status(context)
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"盘前准备失败: {str(e)}")


def _check_account_status(context):
    """
    检查账户状态
    
    Args:
        context: 回测上下文
    """
    try:
        account = context.account
        
        # 检查账户余额
        if account.cash < 0:
            if context.logger:
                context.logger.warning(f"账户现金为负: {account.cash:.2f}")
        
        # 检查可用资金
        if account.available_cash < 0:
            if context.logger:
                context.logger.warning(f"可用资金为负: {account.available_cash:.2f}")
        
        # 更新总资产
        context.update_portfolio_value()
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"检查账户状态失败: {str(e)}")


def _check_positions_status(context):
    """
    检查持仓状态
    
    Args:
        context: 回测上下文
    """
    try:
        positions = context.trade_center.get_positions()
        
        for symbol, position in positions.items():
            # 更新持仓市值
            current_price = context.data_center.get_current_price(symbol)
            if current_price and current_price > 0:
                position.last_price = current_price
                position.market_value = position.volume * current_price
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"检查持仓状态失败: {str(e)}")


def morning_start(context):
    """
    处理上午开盘事件
    
    Args:
        context: 回测上下文
    """
    if context.logger:
        context.logger.debug(f"TradeCenter钩子: 上午开盘 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 开盘处理
    _handle_market_open(context)


def _handle_market_open(context):
    """
    处理开盘
    
    Args:
        context: 回测上下文
    """
    try:
        # 设置交易状态
        context.trade_center.is_trading = True
        
        # 处理开盘时的待成交订单
        _process_pending_orders(context)
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"开盘处理失败: {str(e)}")


def afternoon_start(context):
    """
    处理下午开盘事件
    
    Args:
        context: 回测上下文
    """
    if context.logger:
        context.logger.debug(f"TradeCenter钩子: 下午开盘 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 下午开盘处理
    _handle_market_open(context)


def morning_end(context):
    """
    处理上午收盘事件
    
    Args:
        context: 回测上下文
    """
    if context.logger:
        context.logger.debug(f"TradeCenter钩子: 上午收盘 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 上午收盘处理
    _handle_market_close(context)


def afternoon_end(context):
    """
    处理下午收盘事件
    
    Args:
        context: 回测上下文
    """
    if context.logger:
        context.logger.debug(f"TradeCenter钩子: 下午收盘 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 下午收盘处理
    _handle_market_close(context)


def _handle_market_close(context):
    """
    处理收盘
    
    Args:
        context: 回测上下文
    """
    try:
        # 暂停交易
        context.trade_center.is_trading = False
        
        # 取消所有未成交的市价单
        _cancel_market_orders(context)
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"收盘处理失败: {str(e)}")


def _cancel_market_orders(context):
    """
    取消市价单
    
    Args:
        context: 回测上下文
    """
    try:
        pending_orders = context.trade_center.get_pending_orders()
        
        for order in pending_orders:
            if order.order_type == "market":
                context.trade_center.cancel_order(order_id=order.order_id)
                
                if context.logger:
                    context.logger.info(f"收盘取消市价单: {order.order_id}")
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"取消市价单失败: {str(e)}")


def after_market(context):
    """
    处理盘后事件
    
    Args:
        context: 回测上下文
    """
    if context.logger:
        context.logger.debug(f"TradeCenter钩子: 盘后处理 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 盘后清算
    _handle_after_market_settlement(context)


def _handle_after_market_settlement(context):
    """
    处理盘后清算
    
    Args:
        context: 回测上下文
    """
    try:
        # 更新所有持仓的市值
        _update_positions_market_value(context)
        
        # 更新账户总价值
        context.update_portfolio_value()
        
        # 计算收益率
        context.calculate_returns()
        
        # 生成每日绩效报告
        _generate_daily_performance_report(context)
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"盘后清算失败: {str(e)}")


def _update_positions_market_value(context):
    """
    更新持仓市值

    Args:
        context: 回测上下文
    """
    try:
        from finhack.trader.backtest.constants import SHORT_POSITION_SUFFIX

        positions = context.trade_center.get_positions()

        # BUG 1 fix: 收集需要清除的幽灵持仓（volume≈0）
        ghost_symbols = []
        for symbol, position in list(positions.items()):
            # 跳过零量幽灵持仓
            if position.volume <= 1e-10:
                ghost_symbols.append(symbol)
                continue

            quote_symbol = symbol[:-len(SHORT_POSITION_SUFFIX)] if symbol.endswith(SHORT_POSITION_SUFFIX) else symbol
            current_price = context.data_center.get_current_price(quote_symbol)
            if not current_price or current_price <= 0:
                continue

            position.last_price = current_price

            if position.is_short:
                # BUG 2 fix: 空头持仓市值计算补充 contract_multiplier
                position.market_value = position.volume * current_price * position.contract_multiplier
                position.unrealized_pnl = (position.cost_price - current_price) * position.volume * position.contract_multiplier
            else:
                position.market_value = position.volume * current_price * position.contract_multiplier
                position.unrealized_pnl = (current_price - position.cost_price) * position.volume * position.contract_multiplier

        # 清除幽灵持仓
        for symbol in ghost_symbols:
            del positions[symbol]

    except Exception as e:
        if context.logger:
            context.logger.error(f"更新持仓市值失败: {str(e)}")


def _generate_daily_performance_report(context):
    """
    生成每日绩效报告
    
    Args:
        context: 回测上下文
    """
    try:
        # 获取账户摘要
        account_summary = context.trade_center.get_account_summary()
        
        # 获取今日交易统计
        today_trades = [
            trade for trade in context.logs['trade_list'] 
            if trade.get('timestamp', '').startswith(context.current_dt.strftime('%Y-%m-%d'))
        ]
        
        # 记录每日绩效
        daily_performance = {
            'date': context.current_dt.strftime('%Y-%m-%d'),
            'total_value': account_summary.get('total_value', 0),
            'cash': account_summary.get('cash', 0),
            'positions_value': account_summary.get('total_market_value', 0),
            'unrealized_pnl': account_summary.get('total_unrealized_pnl', 0),
            'realized_pnl': account_summary.get('total_realized_pnl', 0),
            'trades_count': len(today_trades),
            'positions_count': account_summary.get('positions_count', 0)
        }
        
        # 添加到历史记录
        if 'daily_performance' not in context.logs:
            context.logs['daily_performance'] = []
        
        context.logs['daily_performance'].append(daily_performance)
        
        if context.logger:
            context.logger.info(f"每日绩效: 总资产={daily_performance['total_value']:,.2f}, "
                              f"现金={daily_performance['cash']:,.2f}, "
                              f"持仓市值={daily_performance['positions_value']:,.2f}, "
                              f"未实现盈亏={daily_performance['unrealized_pnl']:,.2f}")
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"生成每日绩效报告失败: {str(e)}")


def order_submission(context):
    """
    处理订单提交事件
    
    Args:
        context: 回测上下文
    """
    if context.logger:
        context.logger.debug(f"TradeCenter钩子: 订单提交 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 立即尝试撮合新提交的订单
    _process_pending_orders(context)


def order_fill(context):
    """
    处理订单成交事件
    
    Args:
        context: 回测上下文
    """
    if context.logger:
        context.logger.debug(f"TradeCenter钩子: 订单成交 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 订单成交后的后续处理
    _handle_order_fill_follow_up(context)


def _handle_order_fill_follow_up(context):
    """
    处理订单成交后续
    
    Args:
        context: 回测上下文
    """
    try:
        # 更新持仓信息
        _update_positions_market_value(context)
        
        # 更新账户价值
        context.update_portfolio_value()
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"订单成交后续处理失败: {str(e)}")


def daily_bar_closed(context):
    """
    处理日线收盘事件
    
    Args:
        context: 回测上下文
    """
    if context.logger:
        context.logger.debug(f"TradeCenter钩子: 日线收盘 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 日线收盘处理
    _handle_daily_bar_close(context)


def _handle_daily_bar_close(context):
    """
    处理日线收盘
    
    Args:
        context: 回测上下文
    """
    try:
        # 处理待成交的限价单
        _process_pending_orders(context)
        
        # 更新持仓市值
        _update_positions_market_value(context)
        
    except Exception as e:
        if context.logger:
            context.logger.error(f"日线收盘处理失败: {str(e)}")


def minute_bar(context):
    """
    处理分钟线事件
    
    Args:
        context: 回测上下文
    """
    if context.logger:
        context.logger.debug(f"TradeCenter钩子: 分钟线更新 - {context.current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 分钟线撮合
    _process_pending_orders(context) 