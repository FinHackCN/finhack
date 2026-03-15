"""
主回测引擎

协调EventCenter、TradeCenter、DataCenter、StrategyExecutor四大组件
实现完整的回测流程
"""

import asyncio
import os
import pickle
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np

import finhack.library.log as Log


# ========== 性能优化工具类 ==========

class TimeFormatter:
    """
    时间字符串格式化缓存器 - 性能优化

    避免重复调用 strftime，缓存格式化结果
    同一分钟的多个事件共享同一个格式化字符串
    """
    _cache: Dict[tuple, str] = {}
    _cache_date: Optional[date] = None

    @classmethod
    def format(cls, dt: datetime) -> str:
        """格式化时间为字符串，使用缓存加速"""
        if dt is None:
            return '--:--:--'

        current_date = dt.date()

        # 新的一天，清空缓存
        if cls._cache_date != current_date:
            cls._cache.clear()
            cls._cache_date = current_date

        # 以 (hour, minute) 为键缓存
        minute_key = (dt.hour, dt.minute)
        if minute_key not in cls._cache:
            cls._cache[minute_key] = dt.strftime('%Y-%m-%d %H:%M:%S')

        return cls._cache[minute_key]

    @classmethod
    def format_range(cls, start: datetime, end: datetime) -> tuple:
        """批量格式化开始和结束时间"""
        return cls.format(start), cls.format(end)

    @classmethod
    def clear(cls):
        """清空缓存（用于测试）"""
        cls._cache.clear()
        cls._cache_date = None


def extract_latest_prices_batch(klines_df: pd.DataFrame, symbols: List[str]) -> Dict[str, Dict]:
    """
    批量提取最新价格 - 性能优化

    使用 groupby 代替循环查询 MultiIndex，性能提升 5-10 倍

    Args:
        klines_df: K线数据，MultiIndex(time, code)
        symbols: 需要提取的股票代码列表

    Returns:
        Dict[symbol, {'close': price, 'volume': vol}]
    """
    if klines_df.empty:
        return {}

    result = {}

    try:
        # 方法1: 使用 groupby 批量获取每个股票的最后一行（最快）
        # groupby().last() 一次性获取所有股票的最新数据
        latest = klines_df.groupby(level='code').last()

        has_volume = 'volume' in latest.columns

        for symbol in symbols:
            try:
                if symbol in latest.index:
                    row = latest.loc[symbol]
                    result[symbol] = {
                        'close': row['close'],
                        'volume': row['volume'] if has_volume else 0
                    }
            except (KeyError, IndexError):
                continue

    except Exception as e:
        # 回退方案: 使用 xs 逐个查询（仍然比 get_level_values 快）
        Log.logger.debug(f"[extract_latest_prices_batch] groupby失败，回退到xs方式: {e}")
        for symbol in symbols:
            try:
                symbol_data = klines_df.xs(symbol, level='code')
                if not symbol_data.empty:
                    result[symbol] = {
                        'close': symbol_data['close'].iloc[-1],
                        'volume': symbol_data['volume'].iloc[-1] if 'volume' in symbol_data.columns else 0
                    }
            except (KeyError, IndexError):
                continue

    return result
from ..events.event_types import EventTypeEnum, BaseEvent
from ..models.enums import *
from ..models.account import Account
from ..models.position import Position
from ..models.order import Order
from ..models.trade import Trade
from ..core.context import Context


class TradeCenter:
    """交易中心 - 模拟交易所功能"""

    def __init__(self, context: Dict, market_adapter=None, data_center=None):
        self.context = context
        self.market_adapter = market_adapter
        self.data_center = data_center  # 数据中心引用

        # 事件中心（由BacktestEngine注入）
        self.event_center = None

        # 初始化账户
        self.account = Account.from_dict(context['account'])
        self.positions = {}  # symbol -> Position
        self.orders = {}     # order_id -> Order (所有订单历史)
        self.active_orders = {}  # order_id -> Order (仅活跃订单，用于撮合)
        self.trades = []     # List[Trade]

        # 订单ID计数器
        self.order_id_counter = 1
        self.trade_id_counter = 1

        # ========== 部分成交比例配置 ==========
        # 从配置文件获取，如果没有则使用默认值
        settings = context.get('settings', {})
        partial_fill_config = settings.get('partial_fill_ratio', {
            'large_order_threshold': 10000,    # 大订单阈值（股）
            'large_order_min': 0.3,             # 大订单最小成交比例
            'large_order_max': 0.5,             # 大订单最大成交比例
            'medium_order_threshold': 5000,    # 中订单阈值（股）
            'medium_order_min': 0.5,            # 中订单最小成交比例
            'medium_order_max': 0.7,            # 中订单最大成交比例
            'small_order_min': 0.7,             # 小订单最小成交比例
            'small_order_max': 1.0,             # 小订单最大成交比例
        })
        # 兼容旧格式：如果配置是单个数字，则作为统一比例
        if isinstance(partial_fill_config, (int, float)):
            ratio = partial_fill_config
            partial_fill_config = {
                'large_order_threshold': 10000,
                'large_order_min': ratio,
                'large_order_max': ratio,
                'medium_order_threshold': 5000,
                'medium_order_min': ratio,
                'medium_order_max': ratio,
                'small_order_min': ratio,
                'small_order_max': ratio,
            }
        self.partial_fill_config = partial_fill_config

        # 防重复处理：已处理的公司行为记录
        # 格式: {(symbol, ex_date, action_type): True}
        self._processed_corporate_actions: set = set()

    def set_event_center(self, event_center):
        """设置事件中心

        Args:
            event_center: 事件中心实例
        """
        self.event_center = event_center
        
    async def get_account(self, adapter_id: str, refresh: bool = False) -> Account:
        """获取账户信息"""
        return self.account

    def get_account(self) -> Account:
        """获取账户信息 - 同步版本"""
        return self.account

    async def get_positions(self, adapter_id: str, symbol: Optional[str] = None, refresh: bool = False) -> List[Position]:
        """获取持仓信息"""
        if symbol:
            return [self.positions[symbol]] if symbol in self.positions else []
        return list(self.positions.values())

    def get_all_positions(self) -> Dict[str, Position]:
        """获取所有持仓 - 同步版本

        Returns:
            Dict[str, Position]: 持仓字典 {symbol: Position}
        """
        return self.positions
        
    async def get_orders(self, adapter_id: str, symbol: Optional[str] = None, 
                        status: Optional[OrderStatus] = None, **kwargs) -> List[Order]:
        """获取订单信息"""
        orders = list(self.orders.values())
        
        if symbol:
            orders = [o for o in orders if o.symbol == symbol]
        if status:
            orders = [o for o in orders if o.status == status]
            
        return orders
        
    async def get_trades(self, adapter_id: str, symbol: Optional[str] = None, **kwargs) -> List[Trade]:
        """获取成交信息"""
        trades = self.trades.copy()
        
        if symbol:
            trades = [t for t in trades if t.symbol == symbol]
            
        return trades
        
    async def place_order(self, adapter_id: str, symbol: str, side: Side, 
                         order_type: OrderType, volume: float, price: Optional[float] = None,
                         **kwargs) -> str:
        """下单"""
        # 生成订单ID
        order_id = f"order_{self.order_id_counter:06d}"
        self.order_id_counter += 1
        
        # 创建订单对象
        order = Order(
            account_id=self.account.account_id,
            symbol=symbol,
            side=side,
            order_type=order_type,
            volume=volume,
            order_id=order_id,
            price=price,
            status=OrderStatus.PENDING_NEW,
            created_time=self.context.get('current_dt', datetime.now())
        )
        
        # 验证订单
        if not await self._validate_order(order):
            order.status = OrderStatus.REJECTED
            order.rejected_reason = "订单验证失败"
            Log.logger.warning(f"订单被拒绝: {order_id} - {order.rejected_reason}")
            return order_id
            
        # 添加到订单列表
        self.orders[order_id] = order
        self.active_orders[order_id] = order  # 同时添加到活跃订单
        order.status = OrderStatus.NEW

        # 发布订单提交事件
        if self.event_center:
            self.event_center.publish_order_event(
                event_type=EventTypeEnum.ORDER_SUBMISSION,
                order_data={
                    "order_id": order_id,
                    "symbol": symbol,
                    "side": side.value if hasattr(side, 'value') else str(side),
                    "volume": volume,
                    "price": price
                }
            )

        Log.logger.info(f"订单提交成功: {order_id} {symbol} {side} {volume}@{price}")
        return order_id
        
    async def cancel_order(self, adapter_id: str, order_id: str, **kwargs) -> bool:
        """撤单"""
        if order_id not in self.orders:
            Log.logger.warning(f"订单不存在: {order_id}")
            return False
            
        order = self.orders[order_id]
        if order.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]:
            Log.logger.warning(f"订单状态不允许撤销: {order_id} {order.status}")
            return False
            
        order.status = OrderStatus.CANCELLED
        order.updated_time = self.context.get('current_dt', datetime.now())

        # 发布订单撤销事件
        if self.event_center:
            self.event_center.publish_order_event(
                event_type=EventTypeEnum.ORDER_CANCELLATION,
                order_data={
                    "order_id": order_id,
                    "symbol": order.symbol
                }
            )

        # 从活跃订单中移除
        if order_id in self.active_orders:
            del self.active_orders[order_id]

        Log.logger.info(f"订单撤销成功: {order_id}")
        return True
        
    def try_match_orders(self, event):
        """尝试撮合订单（事件驱动接口）

        Args:
            event: TRY_MATCH 事件
        """
        # 如果事件包含 market_data，直接使用
        if hasattr(event, 'market_data') and event.market_data:
            self.try_match_orders_sync(event.market_data)
            return

        # 否则从 DataCenter 动态获取市场数据
        current_time = self.context.get('current_dt', datetime.now())
        freq = self.context.get('settings', {}).get('freq', '1d')

        # 获取所有需要行情的标的
        symbols = set(order.symbol for order in self.active_orders.values()
                     if order.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED])

        if not symbols:
            return

        # 获取市场数据
        market_data = {}
        # 优先使用 self.data_center，如果没有则尝试使用 event_center.data_center
        data_center = getattr(self, 'data_center', None) or (self.event_center.data_center if self.event_center and hasattr(self.event_center, 'data_center') else None)
        if freq == '1m' and data_center:
            from datetime import timedelta
            start_time = current_time - timedelta(minutes=5)
            end_time = current_time + timedelta(minutes=1)

            try:
                klines_df = data_center.get_klines(
                    codes=list(symbols),
                    freq=freq,
                    start_time=start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    end_time=end_time.strftime('%Y-%m-%d %H:%M:%S'),
                    fields=['close', 'volume']  # 【修复】获取成交量数据
                )

                if not klines_df.empty:
                    for symbol in symbols:
                        try:
                            symbol_klines = klines_df[klines_df.index.get_level_values('code') == symbol]
                            if not symbol_klines.empty:
                                market_data[symbol] = {
                                    'close': symbol_klines['close'].iloc[-1],
                                    'volume': symbol_klines['volume'].iloc[-1] if 'volume' in symbol_klines.columns else 0
                                }
                        except Exception:
                            pass
            except Exception:
                pass

        # 调用同步撮合逻辑
        if market_data:
            self.try_match_orders_sync(market_data)
            
    async def _execute_trade(self, order: Order, price: float, trade_time: datetime):
        """执行交易"""
        # 应用滑点
        slip_price = self._apply_slippage(price, order.side)
        
        # 计算手续费和税费
        trade_amount = order.volume * slip_price
        commission = self._calculate_commission(trade_amount, order.side)
        tax = self._calculate_tax(trade_amount, order.side)
        
        # 检查资金充足性
        if order.side == Side.BUY:
            required_cash = trade_amount + commission + tax
            if self.account.cash_available < required_cash:
                order.status = OrderStatus.REJECTED
                order.rejected_reason = "资金不足"
                Log.logger.warning(f"资金不足，订单被拒绝: {order.order_id}")
                return
                
        # 检查持仓充足性
        if order.side == Side.SELL:
            if order.symbol not in self.positions:
                order.status = OrderStatus.REJECTED
                order.rejected_reason = "无持仓"
                Log.logger.warning(f"无持仓，订单被拒绝: {order.order_id}")
                return
                
            position = self.positions[order.symbol]
            if position.available_volume < order.volume:
                order.status = OrderStatus.REJECTED
                order.rejected_reason = "持仓不足"
                Log.logger.warning(f"持仓不足，订单被拒绝: {order.order_id}")
                return
                
        # 生成成交记录
        trade_id = f"trade_{self.trade_id_counter:06d}"
        self.trade_id_counter += 1
        
        trade = Trade(
            account_id=self.account.account_id,
            symbol=order.symbol,
            order_id=order.order_id,
            trade_id=trade_id,
            side=order.side,
            volume=order.volume,
            price=slip_price,
            trade_time=trade_time,
            amount=trade_amount,
            commission=commission,
            tax=tax
        )
        
        # 更新订单状态
        order.status = OrderStatus.FILLED
        order.filled_volume = order.volume
        order.filled_amount = trade_amount
        order.avg_fill_price = slip_price
        order.updated_time = trade_time
        
        # 更新账户和持仓
        await self._update_account_and_positions(trade)
        
        # 记录成交
        self.trades.append(trade)
        self.context['logs']['all_trades'].append(trade.to_dict())
        
        Log.logger.info(f"交易执行成功: {trade_id} {order.symbol} {order.side.value} {order.volume}@{slip_price:.4f}")
        
    async def _update_account_and_positions(self, trade: Trade):
        """更新账户和持仓"""
        if trade.side == Side.BUY:
            # 买入：减少现金，增加持仓
            self.account.cash_available -= (trade.amount + trade.commission + trade.tax)
            
            if trade.symbol not in self.positions:
                # 新建持仓
                self.positions[trade.symbol] = Position(
                    account_id=self.account.account_id,
                    symbol=trade.symbol,
                    position_side=PositionSide.LONG,
                    volume=trade.volume,
                    available_volume=trade.volume,
                    cost_price=trade.price,
                    market_value=trade.volume * trade.price,
                    open_time=trade.trade_time
                )
            else:
                # 增加持仓
                position = self.positions[trade.symbol]
                old_cost = position.volume * position.cost_price
                new_cost = trade.volume * trade.price + trade.commission + trade.tax
                position.volume += trade.volume
                position.available_volume += trade.volume
                position.cost_price = (old_cost + new_cost) / position.volume
                position.market_value = position.volume * trade.price
                
        elif trade.side == Side.SELL:
            # 卖出：增加现金，减少持仓
            self.account.cash_available += (trade.amount - trade.commission - trade.tax)
            
            if trade.symbol in self.positions:
                position = self.positions[trade.symbol]
                position.volume -= trade.volume
                position.available_volume -= trade.volume
                position.market_value = position.volume * trade.price
                
                # 计算已实现盈亏
                realized_pnl = (trade.price - position.cost_price) * trade.volume - trade.commission - trade.tax
                self.account.pnl_realized += realized_pnl
                
                # 如果持仓清零，删除持仓记录
                if position.volume <= 0:
                    del self.positions[trade.symbol]
                    
        # 更新账户总资产
        self._update_account_value()
        
    def _update_account_value(self):
        """更新账户总资产"""
        # 计算持仓市值（使用已更新的市值）
        positions_value = sum(pos.market_value for pos in self.positions.values())

        # 打印持仓市值
        Log.logger.info(f"持仓市值计算: {positions_value:.2f}")

        # 计算总资产（确保不为负数）
        total_assets = self.account.cash_available + self.account.cash_frozen + positions_value

        # 防御性检查：如果总资产为负数或异常小，记录警告并修正
        initial_capital = self.context.get('settings', {}).get('initial_capital', 1000000)
        if total_assets < 0:
            Log.logger.error(f"总资产为负数({total_assets:.2f})，可能存在计算错误！"
                           f"现金={self.account.cash_available:.2f}, "
                           f"冻结={self.account.cash_frozen:.2f}, "
                           f"市值={positions_value:.2f}")
            # 修正为最小值（避免最大回撤计算异常）
            total_assets = initial_capital * 0.001  # 设为初始资金的0.1%
        elif total_assets < initial_capital * 0.01:  # 小于初始资金的1%
            Log.logger.warning(f"总资产异常低({total_assets:.2f})，请检查！")

        # 更新账户
        self.account.market_value = positions_value
        self.account.total_assets = total_assets
        self.account.timestamp_updated = self.context.get('current_dt', datetime.now())

        # 计算未实现盈亏
        self.account.pnl_unrealized = sum(pos.unrealized_pnl for pos in self.positions.values())

        # 打印更新后的账户信息
        Log.logger.info(f"账户更新: 总资产={self.account.total_assets:.2f}, 持仓市值={self.account.market_value:.2f}")
        
    async def _validate_order(self, order: Order) -> bool:
        """验证订单"""
        # 基本验证
        if order.volume <= 0:
            return False

        if order.order_type == OrderType.LIMIT and (not order.price or order.price <= 0):
            return False

        # A股特殊规则验证
        if self.context['settings']['market'] == 'cn_stock':
            # 100股整数倍
            if order.volume % 100 != 0:
                Log.logger.warning(f"A股买入必须是100股的整数倍: {order.volume}")
                return False

        # 买入订单：检查资金是否充足
        if order.side == Side.BUY:
            # 获取成交价格（限价单使用订单价格，市价单使用最新价格）
            if order.order_type == OrderType.LIMIT and order.price:
                estimated_price = order.price
            else:
                # 市价单：尝试获取最新价格
                estimated_price = self._get_last_price(order.symbol)
                if estimated_price <= 0:
                    Log.logger.warning(f"无法获取{order.symbol}的最新价格，无法验证资金")
                    return False

            # 计算预估成交金额（含手续费）
            estimated_amount = order.volume * estimated_price
            estimated_commission = self._calculate_commission(estimated_amount, Side.BUY)
            estimated_tax = self._calculate_tax(estimated_amount, Side.BUY)
            total_required = estimated_amount + estimated_commission + estimated_tax

            # 检查可用资金
            if self.account.cash_available < total_required:
                Log.logger.warning(
                    f"资金不足，订单被拒绝: {order.symbol} {order.side} {order.volume}股 "
                    f"需要{total_required:.2f}元，可用{self.account.cash_available:.2f}元"
                )
                return False

        # 卖出订单：检查持仓是否充足
        elif order.side == Side.SELL:
            position = self.positions.get(order.symbol)
            available_volume = position.available_volume if position else 0
            if available_volume < order.volume:
                Log.logger.warning(
                    f"持仓不足，订单被拒绝: {order.symbol} 需要{order.volume}股，可用{available_volume}股"
                )
                return False

        return True
        
    def _apply_slippage(self, price: float, side: Side) -> float:
        """应用滑点"""
        slip_type = self.context['settings'].get('slip_type', 'pricerelated')
        slip_value = self.context['settings'].get('slip_value', 0.001)
        
        if slip_type == 'pricerelated':
            if side == Side.BUY:
                return price * (1 + slip_value)
            else:
                return price * (1 - slip_value)
        elif slip_type == 'fixed':
            if side == Side.BUY:
                return price + slip_value
            else:
                return price - slip_value
        else:
            return price
            
    def _calculate_commission(self, amount: float, side: Side) -> float:
        """计算手续费"""
        if side == Side.BUY:
            commission_rate = self.context['settings'].get('open_commission', 0.0003)
        else:
            commission_rate = self.context['settings'].get('close_commission', 0.0003)
            
        commission = amount * commission_rate
        min_commission = self.context['settings'].get('min_commission', 5.0)
        
        return max(commission, min_commission)
        
    def _calculate_tax(self, amount: float, side: Side) -> float:
        """计算税费"""
        if side == Side.BUY:
            tax_rate = self.context['settings'].get('open_tax', 0.0)
        else:
            tax_rate = self.context['settings'].get('close_tax', 0.001)

        return amount * tax_rate

    def _get_last_price(self, symbol: str) -> float:
        """获取最新价格（同步版本）

        Args:
            symbol: 股票代码

        Returns:
            float: 最新价格，获取失败返回0
        """
        try:
            current_time = self.context.get('current_dt')
            if not current_time:
                return 0.0

            # 尝试从data_center获取最新行情
            if hasattr(self, 'data_center') and self.data_center:
                quote_df = self.data_center.get_quotes([symbol], '1d', current_time)
                if not quote_df.empty and symbol in quote_df.index:
                    return float(quote_df.loc[symbol, 'close'])

            # 尝试从持仓获取最后价格
            if symbol in self.positions:
                position = self.positions[symbol]
                if hasattr(position, 'last_price') and position.last_price > 0:
                    return float(position.last_price)
                if hasattr(position, 'cost_price') and position.cost_price > 0:
                    return float(position.cost_price)

            return 0.0
        except Exception as e:
            Log.logger.debug(f"获取{symbol}最新价格失败: {e}")
            return 0.0

    def _get_price_from_datacenter(self, symbol: str) -> float:
        """从数据中心获取股票价格（供backtest_trader使用）

        Args:
            symbol: 股票代码

        Returns:
            float: 最新价格，获取失败返回0
        """
        return self._get_last_price(symbol)

    def try_match_orders_sync(self, market_data: Dict[str, Dict]):
        """撮合订单 - 同步版本，支持部分成交"""
        matched_orders = []
        partial_orders = []
        current_time = self.context.get('current_dt')
        time_str = current_time.strftime('%Y-%m-%d %H:%M:%S') if current_time else '--:--:--'

        # 添加INFO级别日志方便调试
        Log.logger.info(f"[{time_str}] 开始撮合，共{len(self.active_orders)}个活跃订单，市场数据标的: {list(market_data.keys())[:5]}...")

        if not market_data:
            Log.logger.warning(f"[{time_str}] 市场数据为空，无法撮合")
            return

        Log.logger.debug(f"[{time_str}] 开始撮合，共{len(self.active_orders)}个活跃订单（总订单{len(self.orders)}），市场数据: {list(market_data.keys())}")

        # 使用 active_orders 替代 orders，避免遍历所有历史订单
        for order_id, order in list(self.active_orders.items()):
            # 检查订单状态
            if order.status not in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
                continue

            symbol = order.symbol
            Log.logger.debug(f"[{time_str}] 检查订单 {order_id}: {symbol}, 类型: {order.order_type}, 状态: {order.status}, 剩余: {order.remaining_volume}")

            # 检查订单是否已过等待期（延迟撮合）
            if hasattr(order, 'can_match'):
                if not order.can_match(current_time):
                    order_age = order.age_minutes(current_time) if hasattr(order, 'age_minutes') else 0
                    Log.logger.debug(f"[{time_str}] 跳过订单 {order_id}: 订单尚未到达撮合时间，已等待 {order_age:.2f} 分钟")
                    continue
            elif hasattr(order, 'created_at') and order.created_at and current_time:
                from datetime import timedelta
                time_diff = (current_time - order.created_at).total_seconds() / 60
                if time_diff < 1:  # 1分钟延迟
                    Log.logger.debug(f"[{time_str}] 跳过订单 {order_id}: 订单尚未到达撮合时间，已等待 {time_diff:.2f} 分钟")
                    continue

            if symbol not in market_data:
                Log.logger.info(f"[{time_str}] 跳过订单 {order_id}: {symbol} 不在市场数据中")
                continue

            quote = market_data[symbol]
            current_price = quote.get('close', 0)
            market_volume = quote.get('volume', 0)  # 【修复】获取市场成交量
            Log.logger.info(f"[{time_str}] 订单 {order_id} 当前价格: {current_price}, 市场成交量: {market_volume}")

            if current_price <= 0:
                Log.logger.info(f"[{time_str}] 跳过订单 {order_id}: 价格无效 {current_price}")
                continue

            # 【修复】基于市场成交量的成交限制配置
            max_fill_ratio = 0.15  # 单笔成交不超过市场成交量的15%
            min_fill_volume = 100   # 最小成交100股（A股规则）

            # 计算基于市场成交量的最大可成交数量
            if market_volume > 0:
                max_fill_by_market = max(min_fill_volume, int(market_volume * max_fill_ratio))
            else:
                # 如果市场成交量数据缺失，使用默认限制
                max_fill_by_market = 10000

            # 判断是否可以成交
            can_fill = False
            fill_price = current_price
            fill_volume = order.remaining_volume  # 默认全部成交

            if order.order_type == OrderType.MARKET:
                # 市价单部分成交：每次撮合只成交剩余量的30%-80%，但受市场成交量限制
                # 分钟级回测中每分钟都会撮合，所以可以部分成交
                import random
                # 从配置获取成交比例
                if order.remaining_volume > self.partial_fill_config['large_order_threshold']:
                    fill_ratio = random.uniform(
                        self.partial_fill_config['large_order_min'],
                        self.partial_fill_config['large_order_max']
                    )
                elif order.remaining_volume > self.partial_fill_config['medium_order_threshold']:
                    fill_ratio = random.uniform(
                        self.partial_fill_config['medium_order_min'],
                        self.partial_fill_config['medium_order_max']
                    )
                else:
                    fill_ratio = random.uniform(
                        self.partial_fill_config['small_order_min'],
                        self.partial_fill_config['small_order_max']
                    )  # 小订单更容易全部成交

                fill_volume = max(min_fill_volume, int(order.remaining_volume * fill_ratio))
                # 确保是100的整数倍（A股规则）
                fill_volume = (fill_volume // 100) * 100
                # 【修复】不超过剩余量，且不超过市场成交量限制
                fill_volume = min(fill_volume, order.remaining_volume, max_fill_by_market)

                can_fill = True
                fill_price = self._apply_slippage(current_price, order.side)
                if fill_volume < order.remaining_volume:
                    Log.logger.info(f"[{time_str}] [撮合] {order_id} {symbol} 市价单部分成交: {order.remaining_volume}->{fill_volume}股 @{fill_price:.4f} (市场成交量限制: {max_fill_by_market})")
                else:
                    Log.logger.info(f"[{time_str}] [撮合] {order_id} {symbol} 市价单成交: {fill_volume}股 @{fill_price:.4f}")
            elif order.order_type == OrderType.LIMIT:
                # 限价单需要判断价格
                if order.side == Side.BUY and order.price >= current_price:
                    can_fill = True
                    fill_price = min(order.price, current_price)
                    # 【修复】限价单也受市场成交量限制，移除不合理的按价格计算
                    fill_volume = min(order.remaining_volume, max_fill_by_market)
                    fill_volume = (fill_volume // 100) * 100
                    fill_volume = max(min_fill_volume, fill_volume)
                    Log.logger.info(f"[{time_str}] [撮合] {order_id} {symbol} 限价买单可成交: {order.price}>={current_price}, 成交{fill_volume}股 (市场成交量限制: {max_fill_by_market})")
                elif order.side == Side.SELL and order.price <= current_price:
                    can_fill = True
                    fill_price = max(order.price, current_price)
                    # 【修复】限价单也受市场成交量限制
                    fill_volume = min(order.remaining_volume, max_fill_by_market)
                    fill_volume = (fill_volume // 100) * 100
                    fill_volume = max(min_fill_volume, fill_volume)
                    Log.logger.info(f"[{time_str}] [撮合] {order_id} {symbol} 限价卖单可成交: {order.price}<={current_price}, 成交{fill_volume}股 (市场成交量限制: {max_fill_by_market})")
                else:
                    Log.logger.debug(f"[{time_str}] 订单 {order_id} 限价单价格不匹配: {order.price} vs {current_price}")

            if can_fill and fill_volume > 0:
                # 执行成交
                Log.logger.info(f"[{time_str}] [撮合] {order_id} {symbol} {order.side} 成交{fill_volume}/{order.volume}股 @{fill_price:.4f}")
                self._execute_trade_sync(order, fill_price, fill_volume)

                # 检查订单状态
                if order.status == OrderStatus.FILLED:
                    matched_orders.append(order_id)
                    Log.logger.info(f"[{time_str}] [撮合] {order_id} 全部成交 ✓")
                elif order.status == OrderStatus.PARTIALLY_FILLED:
                    partial_orders.append(order_id)
                    Log.logger.info(f"[{time_str}] [撮合] {order_id} 部分成交 {order.filled_volume}/{order.volume}")
            else:
                Log.logger.info(f"[{time_str}] 订单 {order_id} 不可成交: can_fill={can_fill}, fill_volume={fill_volume}, current_price={current_price}, order_type={order.order_type}")

        # 从活跃订单中移除已完全成交的订单
        for order_id in matched_orders:
            if order_id in self.active_orders:
                del self.active_orders[order_id]

        Log.logger.info(f"[{time_str}] [撮合] 完成: 全部成交{len(matched_orders)}单, 部分成交{len(partial_orders)}单, 剩余活跃{len(self.active_orders)}单")

        if matched_orders:
            Log.logger.info(f"[{time_str}] [撮合] 全部成交: {matched_orders}")
        if partial_orders:
            Log.logger.info(f"[{time_str}] [撮合] 部分成交: {partial_orders}")
        
    def _execute_trade_sync(self, order: Order, fill_price: float, fill_volume: float = None):
        """执行成交 - 同步版本，支持部分成交

        Args:
            order: 订单对象
            fill_price: 成交价格
            fill_volume: 成交数量，如果为None则使用order.remaining_volume
        """
        try:
            # 确定成交数量
            if fill_volume is None:
                fill_volume = order.remaining_volume

            # 确保不超过剩余量
            fill_volume = min(fill_volume, order.remaining_volume)

            # 获取事件时间
            current_time = self.context.get('current_dt', datetime.now())
            time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')

            # 计算成交金额
            fill_amount = fill_volume * fill_price

            # 计算费用
            commission = self._calculate_commission(fill_amount, order.side)
            tax = self._calculate_tax(fill_amount, order.side)
            total_cost = commission + tax

            # 买入订单：检查资金是否充足（防止超额交易）
            if order.side == Side.BUY:
                total_required = fill_amount + total_cost
                if self.account.cash_available < total_required:
                    Log.logger.error(f"[{time_str}] 资金不足，成交被拒绝: {order.symbol} "
                                   f"需要{total_required:.2f}元，可用{self.account.cash_available:.2f}元")
                    order.status = OrderStatus.REJECTED
                    order.rejected_reason = f"资金不足: 需要{total_required:.2f}, 可用{self.account.cash_available:.2f}"
                    if order.order_id in self.active_orders:
                        del self.active_orders[order.order_id]
                    return

            # 卖出订单：检查持仓是否充足
            elif order.side == Side.SELL:
                position = self.positions.get(order.symbol)
                available_volume = position.available_volume if position else 0
                if available_volume < fill_volume:
                    Log.logger.error(f"[{time_str}] 持仓不足，成交被拒绝: {order.symbol} "
                                   f"需要{fill_volume}股，可用{available_volume}股")
                    order.status = OrderStatus.REJECTED
                    order.rejected_reason = f"持仓不足: 需要{fill_volume}, 可用{available_volume}"
                    if order.order_id in self.active_orders:
                        del self.active_orders[order.order_id]
                    return

            # 生成成交ID
            trade_id = f"trade_{self.trade_id_counter:06d}"
            self.trade_id_counter += 1

            # 创建成交记录
            trade = Trade(
                account_id=order.account_id,
                symbol=order.symbol,
                order_id=order.order_id,
                trade_id=trade_id,
                side=order.side,
                volume=fill_volume,
                price=fill_price,
                amount=fill_amount,
                commission=commission,
                tax=tax,
                trade_time=current_time
            )

            # 更新订单状态（支持部分成交）
            # 更新累计成交数据
            total_amount = order.filled_amount + fill_amount
            total_volume = order.filled_volume + fill_volume

            # 计算新的平均成交价
            order.avg_fill_price = total_amount / total_volume if total_volume > 0 else 0.0
            order.filled_volume = total_volume
            order.filled_amount = total_amount
            order.updated_time = trade.trade_time

            # 更新订单状态
            if order.filled_volume >= order.volume:
                order.status = OrderStatus.FILLED
                Log.logger.info(f"[{time_str}] [订单] {order.order_id} 全部成交 ✓ ({order.filled_volume}/{order.volume}股)")
            else:
                order.status = OrderStatus.PARTIALLY_FILLED
                Log.logger.info(f"[{time_str}] [订单] {order.order_id} 部分成交 ({order.filled_volume}/{order.volume}股)")

            # 从活跃订单中移除已完全成交的订单
            if order.status == OrderStatus.FILLED and order.order_id in self.active_orders:
                del self.active_orders[order.order_id]

            # 添加成交记录
            self.trades.append(trade)

            # 更新持仓和账户
            # 先更新持仓，获取卖出时的成本价（用于准确计算盈亏）
            try:
                sell_cost_price = self._update_position_sync(trade, total_cost)
                Log.logger.info(f"[调试] _update_position_sync 返回: {sell_cost_price}")
            except Exception as e:
                Log.logger.error(f"[调试] _update_position_sync 失败: {e}")
                import traceback
                Log.logger.error(traceback.format_exc())
                sell_cost_price = 0.0
            # 再更新账户，传入成本价避免持仓删除后获取不到
            try:
                self._update_account_sync(trade, total_cost, sell_cost_price)
            except Exception as e:
                Log.logger.error(f"[调试] _update_account_sync 失败: {e}")
                import traceback
                Log.logger.error(traceback.format_exc())

            # 调试：检查持仓成本价
            if trade.symbol in self.positions:
                pos = self.positions[trade.symbol]
                Log.logger.info(f"[调试] 持仓更新后 {trade.symbol}: volume={pos.volume}, cost_price={pos.cost_price:.4f}")

            Log.logger.info(f"[{time_str}] [成交] {trade_id} {order.symbol} {order.side.value} "
                          f"{fill_volume}股@{fill_price:.4f} 费用:{total_cost:.2f} "
                          f"(累计:{order.filled_volume}/{order.volume}股)")

        except Exception as e:
            current_time = self.context.get('current_dt', datetime.now())
            time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
            Log.logger.error(f"[{time_str}] 执行成交失败: {e}")
            order.status = OrderStatus.REJECTED
            order.rejected_reason = f"成交执行失败: {str(e)}"
            # 从活跃订单中移除失败的订单
            if order.order_id in self.active_orders:
                del self.active_orders[order.order_id]
            
    def _update_position_sync(self, trade: Trade, total_cost: float = 0.0) -> float:
        """更新持仓 - 同步版本

        Args:
            trade: 成交记录
            total_cost: 交易费用（手续费+税费），用于准确计算成本价

        Returns:
            float: 卖出时的成本价（用于盈亏计算），买入时返回0
        """
        symbol = trade.symbol
        sell_cost_price = 0.0  # 用于记录卖出时的成本价

        if symbol not in self.positions:
            # 创建新持仓
            self.positions[symbol] = Position(
                account_id=trade.account_id,
                symbol=symbol,
                position_side=PositionSide.LONG,  # 股票默认多头
                volume=0,
                available_volume=0,
                cost_price=0,
                market_value=0,
                unrealized_pnl=0,
            )

        position = self.positions[symbol]

        if trade.side == Side.BUY:
            # 买入：增加持仓（成本价计算包含交易费用）
            old_volume = position.volume
            old_cost = position.volume * position.cost_price
            # 新成本 = 成交金额 + 交易费用（费用分摊到每股）
            new_cost = trade.volume * trade.price + total_cost

            Log.logger.info(f"[调试] BUY {symbol}: old_volume={old_volume}, old_cost={old_cost}, "
                           f"trade.volume={trade.volume}, trade.price={trade.price}, total_cost={total_cost}, "
                           f"new_cost={new_cost}")

            position.volume += trade.volume

            # T+1规则：当日买入的股票冻结，不可用
            if self.context.get('settings', {}).get('t1_rule', True):
                # 初始化buy_dates（如果不存在）
                if not hasattr(position, 'buy_dates'):
                    position.buy_dates = []
                # 记录买入日期时间，用于T+1解锁
                current_time = self.context.get('current_dt')
                position.buy_dates.append((current_time, trade.volume))
                # T+1规则下，新买入的股票不可用，available_volume 不变
                # 但如果是新持仓（old_volume=0），需要确保 available_volume 初始化为0
                if old_volume == 0:
                    position.available_volume = 0
                # 注意：frozen_volume 是计算属性 (volume - available_volume)，不需要直接设置
            else:
                # 非T+1市场（如美股、期货），买入立即可用
                position.available_volume += trade.volume

            # 计算新的加权平均成本价
            if position.volume > 0:
                position.cost_price = (old_cost + new_cost) / position.volume
                Log.logger.info(f"[调试] BUY {symbol}: 成本价计算 ({old_cost} + {new_cost}) / {position.volume} = {position.cost_price:.4f}")
            else:
                position.cost_price = 0
                Log.logger.warning(f"[调试] BUY {symbol}: 持仓量为0，成本价设为0")
        else:
            # 卖出：减少持仓
            # 记录卖出前的成本价（用于盈亏计算）
            sell_cost_price = position.cost_price

            position.volume -= trade.volume
            position.available_volume -= trade.volume

            # 如果持仓为0，移除持仓记录
            if position.volume <= 0:
                del self.positions[symbol]
                return sell_cost_price

        # 更新持仓市值
        if position.volume > 0:
            position.market_value = position.volume * trade.price
            position.unrealized_pnl = (trade.price - position.cost_price) * position.volume
            position.last_price = trade.price

        return sell_cost_price
        
    def _update_account_sync(self, trade: Trade, total_cost: float, sell_cost_price: float = 0.0):
        """更新账户 - 同步版本

        Args:
            trade: 成交记录
            total_cost: 交易费用（手续费+税费）
            sell_cost_price: 卖出时的成本价（从_update_position_sync返回），用于准确计算已实现盈亏
        """
        if trade.side == Side.BUY:
            # 买入：减少现金，增加持仓市值
            self.account.cash_available -= (trade.amount + total_cost)
            self.account.market_value += trade.amount
        else:
            # 卖出：增加现金，减少持仓市值
            self.account.cash_available += (trade.amount - total_cost)
            self.account.market_value -= trade.amount
            # 使用传入的成本价计算已实现盈亏（修正Bug：避免持仓删除后获取不到成本价）
            cost_price = sell_cost_price if sell_cost_price > 0 else self._get_cost_price(trade.symbol)
            realized_pnl = (trade.price - cost_price) * trade.volume - total_cost
            self.account.pnl_realized += realized_pnl

        # 更新总资产
        self._update_account_value()
        
    def _get_cost_price(self, symbol: str) -> float:
        """获取持仓成本价"""
        if symbol in self.positions:
            return self.positions[symbol].cost_price
        return 0

    def handle_corporate_action(self, event):
        """处理公司行为事件（分红、送股等）

        具有防重复处理机制，确保同一公司行为不会被重复处理。
        """
        try:
            # 获取公司行为信息（支持属性访问和字典访问）
            if hasattr(event, 'data') and event.data:
                action_type = event.data.get('action_type', '')
                symbol = event.data.get('symbol', '')
            else:
                action_type = getattr(event, 'action_type', '')
                symbol = getattr(event, 'symbol', '')

            if not symbol or not action_type:
                return

            # 获取除权除息日期（用于防重复检查）
            ex_date = None
            if hasattr(event, 'data') and event.data:
                ex_date = event.data.get('ex_date')
            if not ex_date and hasattr(event, 'event_time'):
                ex_date = event.event_time.date() if event.event_time else None

            # 创建唯一键进行防重复检查
            if ex_date:
                action_key = (symbol, str(ex_date), action_type)
                if action_key in self._processed_corporate_actions:
                    Log.logger.debug(f"公司行为已处理，跳过: {symbol} {action_type} {ex_date}")
                    return

            # 处理除权除息
            if action_type in ['dividend', 'bonus', 'split', 'rights']:
                if symbol not in self.positions:
                    return

                position = self.positions[symbol]

                # 处理分红
                if action_type == 'dividend':
                    self._handle_dividend(event, position, symbol)

                # 处理送股
                elif action_type == 'bonus':
                    self._handle_bonus(event, position, symbol)

                # 处理拆股
                elif action_type == 'split':
                    self._handle_split(event, position, symbol)

                # 更新持仓市值（公司行为后需要重新计算）
                # 获取当前价格重新计算市值
                current_price = self._get_last_price(symbol)
                if current_price and current_price > 0:
                    position.market_value = position.volume * current_price
                    position.last_price = current_price
                    position.unrealized_pnl = (current_price - position.cost_price) * position.volume

                # 更新账户总资产
                self._update_account_value()

                # 记录已处理的公司行为
                if ex_date:
                    action_key = (symbol, str(ex_date), action_type)
                    self._processed_corporate_actions.add(action_key)

        except Exception as e:
            Log.logger.error(f"处理公司行为事件失败: {e}")
            import traceback
            traceback.print_exc()

    def _handle_dividend(self, event, position, symbol):
        """处理分红事件（含除息处理）"""
        # 获取数据
        if hasattr(event, 'data') and event.data:
            dividend_per_share = event.data.get('dividend_per_share', 0)
            tax_rate = event.data.get('tax_rate', 0.10)
        else:
            dividend_per_share = getattr(event, 'dividend_per_share', 0)
            tax_rate = getattr(event, 'tax_rate', 0.10)

        if dividend_per_share <= 0:
            return

        dividend_amount = position.volume * dividend_per_share
        after_tax = dividend_amount * (1 - tax_rate)

        # 记录除息前成本
        old_cost_price = position.cost_price

        # 增加现金
        self.account.cash_available += after_tax

        # 除息处理：降低持仓成本价
        # 每股分红导致每股成本降低
        new_cost_price = max(0, old_cost_price - dividend_per_share)
        position.cost_price = new_cost_price

        Log.logger.info(f"[公司行为] {symbol} 分红: 每股{dividend_per_share:.4f}元, "
                       f"税率{tax_rate:.0%}, 实得{after_tax:.2f}元, "
                       f"除息: 成本{old_cost_price:.4f}→{new_cost_price:.4f}")

    def _handle_bonus(self, event, position, symbol):
        """处理送股事件"""
        if hasattr(event, 'data') and event.data:
            bonus_ratio = event.data.get('bonus_ratio', 0)
        else:
            bonus_ratio = getattr(event, 'bonus_ratio', 0)

        if bonus_ratio <= 0:
            return

        bonus_shares = int(position.volume * bonus_ratio)
        if bonus_shares > 0:
            # 100股整数倍
            bonus_shares = (bonus_shares // 100) * 100
            old_volume = position.volume
            position.volume += bonus_shares
            position.available_volume += bonus_shares

            # 调整成本价（送股后成本降低）
            if position.volume > 0:
                position.cost_price = position.cost_price * old_volume / position.volume

            Log.logger.info(f"[公司行为] {symbol} 送股: 比例{bonus_ratio:.2f}, "
                           f"持仓{old_volume}→{position.volume}股, "
                           f"新成本{position.cost_price:.4f}")

    def _handle_split(self, event, position, symbol):
        """处理拆股事件"""
        if hasattr(event, 'data') and event.data:
            split_ratio = event.data.get('split_ratio', 1)
        else:
            split_ratio = getattr(event, 'split_ratio', 1)

        if split_ratio <= 0 or split_ratio == 1:
            return

        old_volume = position.volume
        new_volume = int(old_volume * split_ratio)
        actual_ratio = new_volume / old_volume if old_volume > 0 else 1

        position.volume = new_volume
        position.available_volume = new_volume
        position.cost_price = position.cost_price / actual_ratio

        Log.logger.info(f"[公司行为] {symbol} 拆股: 比例{split_ratio:.2f}, "
                       f"持仓{old_volume}→{new_volume}股, "
                       f"新成本{position.cost_price:.4f}")

class BacktestEngine:
    """回测引擎主类"""

    def __init__(self, context: Dict, data_center, event_center, event_bus):
        self.context = context
        self.data_center = data_center
        self.event_center = event_center
        self.event_bus = event_bus

        # 初始化交易中心
        self.trade_center = TradeCenter(context, data_center=data_center)

        # 智能加速跳过追踪（用于合并日志）
        self._skip_total_minutes = 0.0
        self._skip_start_time = None

        # 撮合跳过追踪（用于合并无活跃订单的日志）
        self._match_skip_count = 0
        self._match_skip_start_time = None
        
        # 注册所有市场事件的默认处理器
        market_events = [
            EventTypeEnum.DAY_START,
            EventTypeEnum.BEFORE_MARKET,
            EventTypeEnum.PRE_OPENING_START,
            EventTypeEnum.PRE_OPENING_END,
            EventTypeEnum.MATCHING_START,
            EventTypeEnum.OPENING_PRICE_DETERMINED,
            EventTypeEnum.MARKET_START,
            EventTypeEnum.MORNING_END,
            EventTypeEnum.AFTERNOON_START,
            EventTypeEnum.CLOSING_START,
            EventTypeEnum.CLOSING_END,
            EventTypeEnum.CLOSING_PRICE_DETERMINED,
            EventTypeEnum.MARKET_END,
            EventTypeEnum.DAILY_BAR_CLOSED,
            EventTypeEnum.AFTER_MARKET,
            EventTypeEnum.DAY_END
        ]
        
        for event_type in market_events:
            self.event_center.event_bus.register_handler(event_type, self._process_event_sync)
        
        # 注册动态事件的默认处理器
        dynamic_events = [
            EventTypeEnum.ON_TIME,
            EventTypeEnum.TRY_MATCH,
            EventTypeEnum.ORDER_SUBMISSION,
            EventTypeEnum.ORDER_CANCELLATION,
            EventTypeEnum.ORDER_REJECT,
            EventTypeEnum.ORDER_FILL
        ]
        
        for event_type in dynamic_events:
            self.event_center.event_bus.register_handler(event_type, self._process_event_sync)
    
    def _has_pending_orders(self):
        """检查是否有挂单（未成交的订单）
        
        Returns:
            bool: True表示有挂单，False表示没有挂单
        """
        # 使用 active_orders，性能更好
        return len(self.trade_center.active_orders) > 0
        
    def _handle_order_submission(self, event: Dict):
        """处理订单提交事件"""
        # 订单提交逻辑已在place_order中处理
        pass
        
    def _handle_order_cancellation(self, event: Dict):
        """处理订单撤销事件"""
        # 订单撤销逻辑已在cancel_order中处理
        pass
        
    async def _handle_try_match(self, event: Dict):
        """处理撮合事件"""
        # 获取当前市场数据
        current_time = self.context['current_dt']
        market = self.context['settings']['market']
        freq = self.context['settings']['freq']
        
        # 获取所有需要行情的标的（使用 active_orders）
        symbols = set()
        for order in self.trade_center.active_orders.values():
            symbols.add(order.symbol)
                
        if not symbols:
            return

        # 获取行情数据 - 批量获取以提高性能
        try:
            market_data = {}
            symbols_list = list(symbols)
            
            # 批量获取行情数据而不是逐个获取
            if freq == '1m':
                # 对于1分钟数据，使用get_klines获取最近的数据
                from datetime import timedelta
                start_time = current_time - timedelta(minutes=5)
                klines_df = self.data_center.get_klines(
                    codes=symbols_list,  # 批量获取
                    freq=freq,
                    start_time=start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    end_time=(current_time + timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S'),
                    fields=['close', 'volume']  # 【修复】获取成交量数据
                )

                if not klines_df.empty:
                    # 为每个symbol提取最新数据
                    for symbol in symbols_list:
                        try:
                            symbol_klines = klines_df[klines_df.index.get_level_values('code') == symbol]
                            if not symbol_klines.empty:
                                latest_close = symbol_klines['close'].iloc[-1]
                                latest_volume = symbol_klines['volume'].iloc[-1] if 'volume' in symbol_klines.columns else 0
                                market_data[symbol] = {'close': latest_close, 'volume': latest_volume}
                        except Exception as e:
                            Log.logger.warning(f"提取 {symbol} 行情数据失败: {e}")
            else:
                # 对于日线数据，批量获取
                quote_df = self.data_center.get_quotes(symbols_list, freq=freq, time=current_time, fields=['close'])
                if not quote_df.empty:
                    for symbol in symbols_list:
                        if symbol in quote_df.index:
                            market_data[symbol] = quote_df.loc[symbol].to_dict()

            # 执行撮合
            self.trade_center.try_match_orders_sync(market_data)

        except Exception as e:
            Log.logger.error(f"撮合过程中发生错误: {e}")
            
    async def _handle_on_time(self, event):
        """处理定时任务事件"""
        try:
            # 从上下文中查找定时任务
            if not self.context or 'scheduled_tasks' not in self.context:
                return

            function_name = getattr(event, 'function_name', None)
            if not function_name:
                Log.logger.warning("ON_TIME事件缺少function_name属性")
                return

            # 查找对应的函数对象
            func = None
            for task in self.context['scheduled_tasks']:
                if task.get('function_name') == function_name:
                    func = task.get('function_object')
                    break

            if func:
                # 调用策略函数
                if asyncio.iscoroutinefunction(func):
                    await func(self.context)
                else:
                    func(self.context)

                Log.logger.info(f"执行定时任务成功: {function_name}")

                # 注意：不再立即撮合，让订单在后续的分钟级撮合事件中自然成交
                # 分钟级回测中，后续每分钟都会触发撮合事件

            else:
                Log.logger.warning(f"未找到定时任务函数: {function_name}")

        except Exception as e:
            Log.logger.error(f"执行定时任务失败: {e}")

    async def _try_match_after_scheduled_task(self):
        """定时任务执行后尝试撮合订单"""
        try:
            current_time = self.context.get('current_dt')
            if not current_time:
                return

            market = self.context.get('settings', {}).get('market', 'cn_stock')
            freq = self.context.get('settings', {}).get('freq', '1d')

            # 获取所有需要行情的标的
            symbols = set()
            for order in self.trade_center.active_orders.values():
                symbols.add(order.symbol)

            if not symbols:
                return

            # 获取行情数据
            try:
                market_data = {}
                symbols_list = list(symbols)

                if freq == '1d':
                    # 日线数据批量获取
                    quote_df = self.data_center.get_quotes(symbols_list, freq=freq, time=current_time, fields=['close'])
                    if not quote_df.empty:
                        for symbol in symbols_list:
                            if symbol in quote_df.index:
                                market_data[symbol] = quote_df.loc[symbol].to_dict()
                else:
                    # 分钟线数据
                    from datetime import timedelta
                    start_time = current_time - timedelta(minutes=5)
                    klines_df = self.data_center.get_klines(
                        codes=symbols_list,
                        freq=freq,
                        start_time=start_time.strftime('%Y-%m-%d %H:%M:%S'),
                        end_time=(current_time + timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S'),
                        fields=['close', 'volume']  # 【修复】获取成交量数据
                    )

                    if not klines_df.empty:
                        for symbol in symbols_list:
                            try:
                                symbol_klines = klines_df[klines_df.index.get_level_values('code') == symbol]
                                if not symbol_klines.empty:
                                    latest_close = symbol_klines['close'].iloc[-1]
                                    latest_volume = symbol_klines['volume'].iloc[-1] if 'volume' in symbol_klines.columns else 0
                                    market_data[symbol] = {'close': latest_close, 'volume': latest_volume}
                            except Exception:
                                pass

                # 执行撮合
                if market_data:
                    time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
                    Log.logger.info(f"[{time_str}] [定时任务后撮合] 活跃订单={len(self.trade_center.active_orders)}, 标的={list(symbols)}")
                    self.trade_center.try_match_orders_sync(market_data)

            except Exception as e:
                Log.logger.error(f"定时任务后撮合失败: {e}")

        except Exception as e:
            Log.logger.error(f"_try_match_after_scheduled_task 失败: {e}")
            
    async def run(self, start_date: str, end_date: str, strategy, scheduled_tasks: List):
        """运行回测"""
        Log.logger.info(f"开始回测: {start_date} -> {end_date}")
        
        # 生成交易日历
        calendar = await self._generate_calendar(start_date, end_date)
        
        # 设置调度任务
        self.event_center.set_scheduled_tasks(scheduled_tasks)
        
        # 主循环：遍历每个交易日
        for trade_date in calendar:
            self.context['current_dt'] = trade_date
            Log.logger.info(f"交易日: {trade_date.strftime('%Y-%m-%d')}")
            
            # 生成当日事件列表
            daily_events = self.event_center.generate_daily_events(trade_date.date())
            
            # 按时间顺序处理事件
            for event in daily_events:
                await self._process_event(event, strategy)
                
            # 更新前一交易日
            self.context['previous_date'] = trade_date.date()

        # 计算绩效
        await self._calculate_performance()

        Log.logger.info("回测完成")

    def _get_context_filepath(self) -> Optional[str]:
        """获取上下文文件路径"""
        settings = self.context.get('settings', {})
        base_dir = settings.get('base_dir', os.getcwd())
        running_dir = Path(base_dir) / "data" / "running"
        running_dir.mkdir(parents=True, exist_ok=True)

        # 使用策略名和market生成文件名
        strategy_name = settings.get('strategy_name', 'unknown')
        market = settings.get('market', 'unknown')
        filename = f"backtest_{market}_{strategy_name}.pkl"
        return str(running_dir / filename)

    def _save_context(self) -> bool:
        """保存上下文到文件（用于模拟盘续跑）

        Returns:
            bool: 是否保存成功
        """
        try:
            filepath = self._get_context_filepath()
            if not filepath:
                return False

            # 准备保存的数据
            save_data = {
                'context': self.context,
                'trade_center_state': {
                    'account': self.trade_center.account.to_dict() if hasattr(self.trade_center.account, 'to_dict') else self.trade_center.account.__dict__,
                    'positions': {k: v.to_dict() if hasattr(v, 'to_dict') else v.__dict__
                                 for k, v in self.trade_center.positions.items()},
                    'orders': {k: v.to_dict() if hasattr(v, 'to_dict') else v.__dict__
                              for k, v in self.trade_center.orders.items()},
                    'order_id_counter': self.trade_center.order_id_counter,
                    'trade_id_counter': self.trade_center.trade_id_counter,
                },
                'saved_at': datetime.now().isoformat(),
                'saved_end_date': self.context.get('settings', {}).get('end_date'),
            }

            with open(filepath, 'wb') as f:
                pickle.dump(save_data, f)

            Log.logger.info(f"上下文已保存到: {filepath}")
            return True
        except Exception as e:
            Log.logger.error(f"保存上下文失败: {e}")
            return False

    def _load_context(self) -> Optional[Dict]:
        """加载保存的上下文

        Returns:
            Optional[Dict]: 保存的上下文数据，如果不存在则返回None
        """
        try:
            filepath = self._get_context_filepath()
            if not filepath or not os.path.exists(filepath):
                return None

            with open(filepath, 'rb') as f:
                save_data = pickle.load(f)

            Log.logger.info(f"从文件加载上下文: {filepath}")
            Log.logger.info(f"保存时间: {save_data.get('saved_at')}")
            Log.logger.info(f"保存时的结束日期: {save_data.get('saved_end_date')}")
            return save_data
        except Exception as e:
            Log.logger.error(f"加载上下文失败: {e}")
            return None

    def _restore_from_saved_context(self, saved_data: Dict) -> datetime:
        """从保存的上下文恢复状态

        Args:
            saved_data: 保存的上下文数据

        Returns:
            datetime: 恢复的开始时间（从保存的结束时间的下一天开始）
        """
        # 恢复context状态
        saved_context = saved_data['context']
        self.context.update(saved_context)

        # 恢复TradeCenter状态
        tc_state = saved_data['trade_center_state']
        if 'account' in tc_state:
            from ..models.account import Account
            self.trade_center.account = Account.from_dict(tc_state['account'])

        # 恢复positions
        from ..models.position import Position
        self.trade_center.positions = {}
        for symbol, pos_data in tc_state.get('positions', {}).items():
            self.trade_center.positions[symbol] = Position.from_dict(pos_data)

        # 恢复orders
        from ..models.order import Order
        self.trade_center.orders = {}
        for order_id, order_data in tc_state.get('orders', {}).items():
            self.trade_center.orders[order_id] = Order.from_dict(order_data)

        # 恢复活跃订单
        self.trade_center.active_orders = {
            k: v for k, v in self.trade_center.orders.items()
            if v.status not in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]
        }

        # 恢复计数器
        self.trade_center.order_id_counter = tc_state.get('order_id_counter', 1)
        self.trade_center.trade_id_counter = tc_state.get('trade_id_counter', 1)

        # 计算新的开始时间：从保存结束日期的下一天开始
        saved_end_date_str = saved_data.get('saved_end_date')
        if saved_end_date_str:
            saved_end_date = datetime.strptime(saved_end_date_str, '%Y-%m-%d %H:%M:%S')
            new_start_date = saved_end_date + timedelta(days=1)
            Log.logger.info(f"从保存的上下文恢复，新的开始日期: {new_start_date.strftime('%Y-%m-%d')}")
            return new_start_date

        return None

    def run_sync(self, start_date: str, end_date: str, strategy, scheduled_tasks: List):
        """运行回测 - 同步版本"""
        Log.logger.info(f"开始回测: {start_date} -> {end_date}")

        # 保存策略引用
        self.strategy = strategy

        # 初始化全局函数
        self.initialize_global_functions()

        # 如果策略有set_order_functions函数，则调用它
        if hasattr(strategy, 'set_order_functions'):
            strategy.set_order_functions(
                globals()['order_value'],
                globals()['order_volume']
            )

        # ========== 模拟盘：检查是否有保存的上下文 ==========
        settings = self.context.get('settings', {})
        simulation_mode = settings.get('simulation_mode', False)
        context_persistence = settings.get('context_persistence', False)

        if simulation_mode and context_persistence:
            saved_data = self._load_context()
            if saved_data:
                saved_end_date_str = saved_data.get('saved_end_date')
                current_end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')

                # 如果当前结束日期 > 保存的结束日期，说明用户延长了回测时间
                if saved_end_date_str:
                    saved_end_date = datetime.strptime(saved_end_date_str, '%Y-%m-%d %H:%M:%S')
                    if current_end_date > saved_end_date:
                        Log.logger.info(f"检测到延长结束日期：{saved_end_date} -> {current_end_date}")
                        Log.logger.info("从保存的上下文恢复状态...")
                        restored_start = self._restore_from_saved_context(saved_data)
                        if restored_start:
                            start_date = restored_start.strftime('%Y-%m-%d %H:%M:%S')
                            Log.logger.info(f"新的开始日期: {start_date}")
                        else:
                            Log.logger.warning("恢复上下文失败，使用原始配置")
                    else:
                        Log.logger.info("检测到保存的上下文，但结束日期未延长，正常回测")

        # 生成交易日历
        calendar = self._generate_calendar_sync(start_date, end_date)
        Log.logger.info(f"交易日历已生成，共{len(calendar)}天")
        
        # 设置调度任务
        self.event_center.set_scheduled_tasks(scheduled_tasks)
        
        # 获取市场设置
        market = self.context.get('settings', {}).get('market', 'cn_stock')
        frequency = self.context.get('settings', {}).get('freq', '1d')
        
        # 如果是1分钟频率，预加载前两个月的数据
        if frequency == '1m':
            # 检查是否启用预加载（可通过配置禁用）
            enable_preload = True
            if hasattr(self.context, 'data_config'):
                enable_preload = getattr(self.context.data_config, 'preload_data', True)

            if enable_preload:
                # 获取额外预加载月数配置
                extra_months = 3
                if hasattr(self.context, 'data_config'):
                    extra_months = getattr(self.context.data_config, 'preload_extra_months', 3)

                Log.logger.info(f"检测到1分钟频率回测，启用预加载策略（额外预加载前{extra_months}个月）")

                # 预加载第一个交易日及其前N个月的数据
                if calendar:
                    first_trade_date = calendar[0]
                    Log.logger.info(f"开始预加载初始数据: {first_trade_date}（含前{extra_months}个月）")
                    print(f"[回测] 开始预加载 {market} 数据（含前{extra_months}个月历史数据）...", flush=True)

                    # 获取universe（股票池）
                    universe = self.context.get('universe', None)

                    self.data_center.ensure_monthly_data_loaded(
                        market=market,
                        current_date=first_trade_date,
                        universe=universe,
                        frequency=frequency
                    )
                    print(f"[回测] 初始数据预加载完成（已加载前{extra_months}个月历史数据）", flush=True)
            else:
                Log.logger.info("检测到1分钟频率回测，使用按需加载策略（禁用预加载）")
                print(f"[回测] 使用按需加载策略，数据将在策略需要时加载", flush=True)
        
        # 记录上一次处理的月份，用于检测月份变化
        last_processed_month = None
        
        # 主循环：遍历每个交易日
        for trade_date in calendar:
            self.context['current_dt'] = trade_date
            Log.logger.info(f"交易日: {trade_date.strftime('%Y-%m-%d')}")
            
            # 检查是否进入新的月份
            if frequency == '1m':
                current_month = f"{trade_date.year}-{trade_date.month:02d}"
                if current_month != last_processed_month:
                    Log.logger.info(f"检测到进入新月份: {current_month}")

                    # 检查是否启用预加载
                    enable_preload = True
                    if hasattr(self.context, 'data_config'):
                        enable_preload = getattr(self.context.data_config, 'preload_data', True)

                    if enable_preload:
                        extra_months = 3
                        if hasattr(self.context, 'data_config'):
                            extra_months = getattr(self.context.data_config, 'preload_extra_months', 3)
                        print(f"[回测] 进入新月份 {current_month}，预加载本月及前{extra_months}个月数据", flush=True)

                        # 预加载当前月及前N个月的数据
                        self.data_center.ensure_monthly_data_loaded(
                            market=market,
                            current_date=trade_date,
                            universe=self.context.get('universe', None),
                            frequency=frequency
                        )
                    else:
                        print(f"[回测] 进入新月份 {current_month}，使用按需加载", flush=True)

                    last_processed_month = current_month
            
            # 生成当日事件列表
            daily_events = self.event_center.generate_daily_events(trade_date.date())
            
            # 调试：打印前3个事件
            Log.logger.info(f"生成了 {len(daily_events)} 个事件")
            for idx in range(min(3, len(daily_events))):
                e = daily_events[idx]
                Log.logger.info(f"  [{idx}] {e.event_time} ({e.event_type.value})")
                print(f"[事件列表] [{idx}] {e.event_time} ({e.event_type.value})", flush=True)
            
            # 按时间顺序处理事件
            i = 0
            while i < len(daily_events):
                event = daily_events[i]
                self._process_event_sync(event, strategy)

                # 智能加速逻辑：仅在1分钟频率下生效
                frequency = self.context.get('settings', {}).get('freq', '1d')

                # 检查策略是否注册了MARKET_BAR_1M事件处理器
                strategy_has_bar_handler = (
                    hasattr(strategy, 'event_handlers') and
                    EventTypeEnum.MARKET_BAR_1M in strategy.event_handlers
                )

                # 如果没有注册1分钟K线处理器，在MARKET_BAR_1M事件时跳过
                if frequency == '1m' and not strategy_has_bar_handler:
                    if event.event_type == EventTypeEnum.MARKET_BAR_1M:
                        # 直接跳过MARKET_BAR_1M事件，不做任何处理
                        Log.logger.debug(f"智能加速：跳过MARKET_BAR_1M事件 {event.event_time}")
                        i += 1
                        continue
                    elif event.event_type == EventTypeEnum.TRY_MATCH:
                        # 检查是否有挂单
                        has_pending_orders = self._has_pending_orders()
                        active_count = len(self.trade_center.active_orders) if hasattr(self.trade_center, 'active_orders') else 0
                        Log.logger.debug(f"[智能加速] TRY_MATCH {event.event_time}: 有挂单={has_pending_orders}, 活跃订单数={active_count}")

                        if not has_pending_orders:
                            # 无挂单时，跳过后续的MARKET_BAR_1M，直达下一个TRY_MATCH或重要事件
                            next_event_idx = i + 1
                            while next_event_idx < len(daily_events):
                                next_event = daily_events[next_event_idx]
                                # 停在TRY_MATCH或重要事件上
                                if next_event.event_type == EventTypeEnum.TRY_MATCH:
                                    break
                                # 重要事件列表
                                important_events = [
                                    EventTypeEnum.ON_TIME,
                                    EventTypeEnum.MARKET_START,
                                    EventTypeEnum.MORNING_END,
                                    EventTypeEnum.AFTERNOON_START,
                                    EventTypeEnum.CLOSING_START,
                                    EventTypeEnum.MARKET_END,
                                ]
                                if next_event.event_type in important_events:
                                    break
                                next_event_idx += 1

                            # 只有找到了目标事件且不在列表末尾时才跳转
                            if next_event_idx > i + 1 and next_event_idx < len(daily_events):
                                target_event = daily_events[next_event_idx]
                                time_diff = (target_event.event_time - event.event_time).total_seconds() / 60

                                # 累积跳过时间，不立即输出日志
                                if self._skip_start_time is None:
                                    self._skip_start_time = event.event_time
                                self._skip_total_minutes += time_diff

                                i = next_event_idx
                                continue

                i += 1

            # 每日结束时，输出剩余的累积跳过日志
            if self._skip_total_minutes > 0:
                Log.logger.info(f"智能加速：累计跳过 {self._skip_total_minutes:.0f} 分钟 (从 {self._skip_start_time} 到交易日结束)")
                self._skip_total_minutes = 0
                self._skip_start_time = None

            # 每日结束时，输出剩余的撮合跳过日志
            if self._match_skip_count > 0:
                Log.logger.info(f"[TRY_MATCH] 累计跳过撮合 {self._match_skip_count} 次 (从 {self._match_skip_start_time} 到交易日结束)")
                self._match_skip_count = 0
                self._match_skip_start_time = None

            # 更新前一交易日
            self.context['previous_date'] = trade_date.date()

        # 计算绩效
        self._calculate_performance_sync()

        Log.logger.info("回测完成")

        # ========== 模拟盘：保存上下文 ==========
        if simulation_mode and context_persistence:
            Log.logger.info("模拟盘模式：保存上下文状态...")
            self._save_context()

        # 返回回测结果
        return self.context['performance']
    
    def initialize_global_functions(self):
        """初始化全局函数"""
        global order_value, order_volume
        
        def order_value(security, amount, side, order_type=OrderType.MARKET, 
                         order_cost=None, slippage=None):
            """下单函数 - 按金额下单"""
            try:
                # 获取当前价格
                current_time = self.context['current_dt']
                freq = self.context['settings']['freq']
                
                # 获取价格数据
                quote_df = self.data_center.get_quotes([security], freq, current_time)
                if quote_df.empty or security not in quote_df.index:
                    Log.logger.warning(f"无法获取 {security} 的价格数据")
                    return None
                
                price = quote_df.loc[security, 'close']
                
                # 计算数量
                volume = amount / price
                
                # A股特殊规则：买入必须是100股的整数倍
                if self.context['settings']['market'] == 'cn_stock' and side.lower() == 'buy':
                    volume = int(volume / 100) * 100  # 向下取整到100的倍数
                    if volume <= 0:
                        volume = 100  # 至少买入100股
                
                # 转换订单类型
                from finhack.trader.backtest.models.enums import Side
                order_side = Side.BUY if side.lower() == "buy" else Side.SELL
                
                # 调用TradeCenter下单（同步版本）
                import asyncio
                try:
                    # 尝试获取当前事件循环
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    # 如果没有事件循环，创建一个新的
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

                # 调用异步方法
                order_id = loop.run_until_complete(
                    self.trade_center.place_order(
                        adapter_id="default",
                        symbol=security,
                        side=order_side,
                        order_type=order_type,
                        volume=volume,
                        price=price
                    )
                )

                current_time = self.context.get('current_dt')
                time_str = TimeFormatter.format(current_time)
                Log.logger.info(f"[{time_str}] 下单成功: {security} {side} {volume:.2f} @ {price:.2f}, 订单ID: {order_id}")
                return order_id

            except Exception as e:
                current_time = self.context.get('current_dt')
                time_str = TimeFormatter.format(current_time)
                Log.logger.error(f"[{time_str}] 下单失败: {e}")
                return None

        def order_volume(security, volume, side, order_type=OrderType.MARKET,
                          order_cost=None, slippage=None):
            """下单函数 - 按数量下单"""
            try:
                # 获取当前价格
                current_time = self.context['current_dt']
                freq = self.context['settings']['freq']
                
                # 获取价格数据
                quote_df = self.data_center.get_quotes([security], freq, current_time)
                if quote_df.empty or security not in quote_df.index:
                    Log.logger.warning(f"无法获取 {security} 的价格数据")
                    return None
                
                price = quote_df.loc[security, 'close']
                
                # A股特殊规则：买入必须是100股的整数倍
                if self.context['settings']['market'] == 'cn_stock' and side.lower() == 'buy':
                    volume = int(volume / 100) * 100  # 向下取整到100的倍数
                    if volume <= 0:
                        volume = 100  # 至少买入100股
                
                # 转换订单类型
                from finhack.trader.backtest.models.enums import Side
                order_side = Side.BUY if side.lower() == "buy" else Side.SELL
                
                # 调用TradeCenter下单（同步版本）
                import asyncio
                try:
                    # 尝试获取当前事件循环
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    # 如果没有事件循环，创建一个新的
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                # 调用异步方法
                order_id = loop.run_until_complete(
                    self.trade_center.place_order(
                        adapter_id="default",
                        symbol=security,
                        side=order_side,
                        order_type=order_type,
                        volume=volume,
                        price=price
                    )
                )

                current_time = self.context.get('current_dt')
                time_str = TimeFormatter.format(current_time)
                Log.logger.info(f"[{time_str}] 下单成功: {security} {side} {volume:.2f} @ {price:.2f}, 订单ID: {order_id}")
                return order_id

            except Exception as e:
                current_time = self.context.get('current_dt')
                time_str = TimeFormatter.format(current_time)
                Log.logger.error(f"[{time_str}] 下单失败: {e}")
                return None

        # 设置全局函数
        globals()['order_value'] = order_value
        globals()['order_volume'] = order_volume
        
        # 设置全局变量'engine'
        globals()['engine'] = self
        
        # 如果策略有set_order_functions函数，则调用它
        if hasattr(self, 'strategy') and hasattr(self.strategy, 'set_order_functions'):
            self.strategy.set_order_functions(order_value, order_volume)
    
    def _generate_calendar_sync(self, start_date: str, end_date: str) -> List[datetime]:
        """生成交易日历 - 同步版本

        使用真实交易日历过滤非交易日（节假日等）
        """
        # 尝试多种日期格式
        date_formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']

        start_dt = None
        end_dt = None

        # 尝试解析开始日期
        for fmt in date_formats:
            try:
                start_dt = datetime.strptime(start_date, fmt)
                break
            except ValueError:
                continue

        # 尝试解析结束日期
        for fmt in date_formats:
            try:
                end_dt = datetime.strptime(end_date, fmt)
                break
            except ValueError:
                continue

        if start_dt is None or end_dt is None:
            raise ValueError(f"无法解析日期: start_date={start_date}, end_date={end_date}")

        # 获取市场类型
        market_name = self.context.get('settings', {}).get('market', 'cn_stock')

        # 加密货币市场是7x24交易，不跳过周末和节假日
        if market_name.startswith('global_crypto'):
            calendar = []
            current_dt = start_dt
            while current_dt <= end_dt:
                calendar.append(current_dt)
                current_dt += timedelta(days=1)
            return calendar

        # 尝试从交易日历文件加载真实交易日
        trade_dates = self._load_trade_calendar(market_name, start_dt, end_dt)

        if trade_dates:
            # 使用真实交易日历
            calendar = []
            for trade_date in trade_dates:
                dt = datetime.combine(trade_date, datetime.min.time())
                if start_dt <= dt <= end_dt:
                    calendar.append(dt)
            Log.logger.info(f"使用交易日历，共{len(calendar)}个交易日")
            return calendar
        else:
            # 回退：只过滤周末
            Log.logger.warning("未找到交易日历文件，使用简单工作日过滤")
            calendar = []
            current_dt = start_dt
            while current_dt <= end_dt:
                if current_dt.weekday() < 5:  # 跳过周末
                    calendar.append(current_dt)
                current_dt += timedelta(days=1)
            return calendar

    def _load_trade_calendar(self, market: str, start_dt: datetime, end_dt: datetime) -> List[date]:
        """从交易日历文件加载交易日列表

        Args:
            market: 市场名称
            start_dt: 开始日期
            end_dt: 结束日期

        Returns:
            交易日列表（date对象），如果加载失败返回空列表
        """
        import os

        try:
            # 尝试多个可能的路径
            possible_paths = [
                # demo_project 路径
                os.path.join(self.context.get('project_path', ''), 'data', 'market', 'reference', market, f'{market}_calendar.csv'),
                # mysql_project 路径
                os.path.join(os.path.dirname(self.context.get('project_path', '')), 'mysql_project', 'data', 'market', 'reference', market, f'{market}_calendar.csv'),
            ]

            calendar_file = None
            for path in possible_paths:
                if os.path.exists(path):
                    calendar_file = path
                    break

            if not calendar_file:
                Log.logger.debug(f"未找到交易日历文件: {possible_paths[0]}")
                return []

            # 读取交易日历
            import pandas as pd
            df = pd.read_csv(calendar_file)

            # 将 cal_date 转换为整数进行比较（CSV中是int64类型）
            start_date_int = int(start_dt.strftime('%Y%m%d'))
            end_date_int = int(end_dt.strftime('%Y%m%d'))

            trade_df = df[
                (df['is_open'] == 1) &
                (df['cal_date'] >= start_date_int) &
                (df['cal_date'] <= end_date_int)
            ]

            # 转换为date列表
            trade_dates = []
            for date_val in trade_df['cal_date'].values:
                trade_dates.append(datetime.strptime(str(int(date_val)), '%Y%m%d').date())

            Log.logger.info(f"从交易日历加载 {len(trade_dates)} 个交易日")
            return sorted(trade_dates)

        except Exception as e:
            Log.logger.warning(f"加载交易日历失败: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _process_event_sync(self, event: BaseEvent, strategy):
        """处理单个事件 - 同步版本"""
        event_name = event.event_type.value
        event_time = event.event_time
        
        # 更新当前时间
        self.context['current_dt'] = event_time
        
        # 添加调试日志
        if event.event_type == EventTypeEnum.TRY_MATCH:
            Log.logger.debug(f"处理撮合事件: {event_time}")
        
        # ====== 新增：调用策略注册的事件处理器 ======
        if hasattr(strategy, 'event_handlers'):
            # 尝试通过枚举值和枚举对象本身查找处理器
            handlers = None
            
            # 添加详细调试
            if event.event_type.value == 'DAY_START' or event.event_type.value == 'day_start':
                print(f"[DEBUG] 处理DAY_START事件", flush=True)
                print(f"[DEBUG] event.event_type = {event.event_type}, type = {type(event.event_type)}", flush=True)
                print(f"[DEBUG] event.event_type.value = {event.event_type.value}", flush=True)
                print(f"[DEBUG] strategy.event_handlers.keys() = {list(strategy.event_handlers.keys())}", flush=True)
                for k in strategy.event_handlers.keys():
                    print(f"[DEBUG] key = {k}, value = {k.value}, type = {type(k)}", flush=True)
                    print(f"[DEBUG] k == event.event_type: {k == event.event_type}", flush=True)
                    print(f"[DEBUG] k.value == event.event_type.value: {k.value == event.event_type.value}", flush=True)
            
            # 方法1：直接用枚举对象查找
            if event.event_type in strategy.event_handlers:
                handlers = strategy.event_handlers[event.event_type]
            else:
                # 方法2：通过枚举值查找（防止不同模块的枚举对象不相等）
                for registered_event_type, registered_handlers in strategy.event_handlers.items():
                    if registered_event_type.value == event.event_type.value:
                        handlers = registered_handlers
                        print(f"[DEBUG] 通过枚举值匹配找到处理器: {event.event_type.value}", flush=True)
                        break
            
            if handlers:
                Log.logger.info(f"[事件分发] 调用策略事件处理器: {event.event_type.value}，共{len(handlers)}个处理器")
                print(f"[事件分发] 调用策略事件处理器: {event.event_type.value}", flush=True)
                for handler in handlers:
                    try:
                        handler(self.context, event)
                    except Exception as e:
                        Log.logger.error(f"调用策略事件处理器失败 ({event.event_type.value}): {e}")
                        import traceback
                        traceback.print_exc()
            else:
                Log.logger.debug(f"事件 {event.event_type.value} 没有注册处理器")
        else:
            Log.logger.warning("策略没有event_handlers属性！")
        
        # 调用策略的handle_bar函数（如果有）
        if hasattr(strategy, 'handle_bar') and event.event_type in [
            EventTypeEnum.MARKET_BAR_1D, EventTypeEnum.MARKET_BAR_1M, 
            EventTypeEnum.MARKET_BAR_30M, EventTypeEnum.MARKET_BAR_120M,
            EventTypeEnum.DAILY_BAR_CLOSED
        ]:
            try:
                # 创建bar_dict
                bar_dict = {
                    'datetime': event_time,
                    'open': 0,  # 这里可以获取实际价格
                    'high': 0,
                    'low': 0,
                    'close': 0,
                    'volume': 0
                }
                strategy.handle_bar(self.context, bar_dict)
            except Exception as e:
                Log.logger.error(f"调用策略handle_bar失败: {e}")
        
        # 根据事件类型直接处理
        if event.event_type == EventTypeEnum.ON_TIME:
            self._handle_on_time_sync(event)
        elif event.event_type == EventTypeEnum.TRY_MATCH:
            self._handle_try_match_sync(event)
        elif event.event_type == EventTypeEnum.MARKET_END:
            self._handle_market_end_sync(event)
        elif event.event_type == EventTypeEnum.DAILY_BAR_CLOSED:
            self._handle_day_end_sync(event)
        elif event.event_type == EventTypeEnum.DAY_END:
            self._handle_day_end_sync(event)
        elif event.event_type == EventTypeEnum.BEFORE_MARKET:
            self._handle_before_market_sync(event)
        elif event.event_type == EventTypeEnum.CORPORATE_ACTION:
            self._handle_corporate_action_sync(event)
        else:
            # 其他市场事件的默认处理
            Log.logger.debug(f"市场事件 {event.event_type.value} 已处理")
    
    def _handle_on_time_sync(self, event):
        """处理定时任务事件 - 同步版本"""
        try:
            # 从上下文中查找定时任务
            if not self.context or 'scheduled_tasks' not in self.context:
                return

            function_name = getattr(event, 'function_name', None)
            if not function_name:
                Log.logger.warning("ON_TIME事件缺少function_name属性")
                return

            # 查找对应的函数对象
            func = None
            for task in self.context['scheduled_tasks']:
                if task.get('function_name') == function_name:
                    func = task.get('function_object')
                    break

            if func:
                # 调用策略函数 - 强制同步调用
                func(self.context)
                Log.logger.info(f"执行定时任务成功: {function_name}")

                # 注意：不再立即撮合，让订单在后续的分钟级撮合事件中自然成交
                # 分钟级回测中，后续每分钟都会触发撮合事件

            else:
                Log.logger.warning(f"未找到定时任务函数: {function_name}")

        except Exception as e:
            Log.logger.error(f"执行定时任务失败: {e}")

    def _try_match_after_scheduled_task_sync(self):
        """定时任务执行后尝试撮合订单 - 同步版本

        性能优化版本:
        - 使用 TimeFormatter 缓存时间格式化
        - 使用 extract_latest_prices_batch 批量提取价格
        """
        try:
            current_time = self.context.get('current_dt')
            if not current_time:
                return

            market = self.context.get('settings', {}).get('market', 'cn_stock')
            freq = self.context.get('settings', {}).get('freq', '1d')

            # 获取所有需要行情的标的
            symbols = set()
            for order in self.trade_center.active_orders.values():
                symbols.add(order.symbol)

            if not symbols:
                return

            # 获取行情数据
            try:
                market_data = {}
                symbols_list = list(symbols)

                if freq == '1d':
                    # 日线数据批量获取
                    quote_df = self.data_center.get_quotes(symbols_list, freq=freq, time=current_time, fields=['close'])
                    if not quote_df.empty:
                        for symbol in symbols_list:
                            if symbol in quote_df.index:
                                market_data[symbol] = quote_df.loc[symbol].to_dict()
                else:
                    # 分钟线数据
                    start_time = current_time - timedelta(minutes=5)
                    end_time = current_time + timedelta(minutes=1)

                    # 【优化】使用缓存的时间格式化器
                    start_time_str, end_time_str = TimeFormatter.format_range(start_time, end_time)

                    klines_df = self.data_center.get_klines(
                        codes=symbols_list,
                        freq=freq,
                        start_time=start_time_str,
                        end_time=end_time_str,
                        fields=['close', 'volume']
                    )

                    if not klines_df.empty:
                        # 【性能优化】使用批量提取函数
                        market_data = extract_latest_prices_batch(klines_df, symbols_list)

                # 执行撮合
                if market_data:
                    # 【优化】使用缓存的时间格式化器
                    time_str = TimeFormatter.format(current_time)
                    Log.logger.info(f"[{time_str}] [定时任务后撮合] 活跃订单={len(self.trade_center.active_orders)}, 标的={list(symbols)}")
                    self.trade_center.try_match_orders_sync(market_data)

            except Exception as e:
                Log.logger.error(f"定时任务后撮合失败: {e}")

        except Exception as e:
            Log.logger.error(f"_try_match_after_scheduled_task_sync 失败: {e}")
            
    def _handle_try_match_sync(self, event):
        """处理撮合事件 - 同步版本

        性能优化版本:
        - 使用 TimeFormatter 缓存时间格式化
        - 使用 extract_latest_prices_batch 批量提取价格
        """
        # 获取当前市场数据
        current_time = self.context['current_dt']
        # 【优化】使用缓存的时间格式化器
        time_str = TimeFormatter.format(current_time)
        market = self.context['settings']['market']
        freq = self.context['settings']['freq']

        # 获取所有需要行情的标的（使用 active_orders）
        symbols = set()
        for order_id, order in self.trade_center.active_orders.items():
            symbols.add(order.symbol)

        # 如果没有活跃订单，合并日志输出
        if not symbols:
            self._match_skip_count += 1
            if self._match_skip_start_time is None:
                self._match_skip_start_time = current_time
            # 不立即输出日志，等待有订单或结束时再输出
            return

        # 如果之前有累积的跳过记录，先输出合并后的日志
        if self._match_skip_count > 0:
            # 【优化】使用缓存的时间格式化器
            start_str = TimeFormatter.format(self._match_skip_start_time)
            Log.logger.info(f"[{time_str}] [TRY_MATCH] 累计跳过撮合 {self._match_skip_count} 次 ({start_str} -> {time_str})")
            self._match_skip_count = 0
            self._match_skip_start_time = None

        Log.logger.info(f"[{time_str}] [TRY_MATCH] 活跃订单={len(self.trade_center.active_orders)}, 标的={list(symbols)}")

        # 获取行情数据 - 批量获取以提高性能
        try:
            market_data = {}
            symbols_list = list(symbols)

            # 批量获取行情数据而不是逐个获取
            if freq == '1m':
                # 对于1分钟数据，使用get_klines获取最近的数据
                start_time = current_time - timedelta(minutes=5)
                end_time = current_time + timedelta(minutes=1)

                # 【优化】使用缓存的时间格式化器
                start_time_str, end_time_str = TimeFormatter.format_range(start_time, end_time)
                Log.logger.info(f"[{time_str}] [TRY_MATCH] 查询K线: codes={len(symbols_list)}个, start={start_time_str}, end={end_time_str}")

                klines_df = self.data_center.get_klines(
                    codes=symbols_list,  # 批量获取
                    freq=freq,
                    start_time=start_time_str,
                    end_time=end_time_str,
                    fields=['close', 'volume']  # 获取成交量数据
                )

                Log.logger.info(f"[{time_str}] [TRY_MATCH] get_klines返回: empty={klines_df.empty}, shape={klines_df.shape if not klines_df.empty else 'N/A'}")

                if not klines_df.empty:
                    # 【性能优化】使用批量提取函数，避免循环查询MultiIndex
                    market_data = extract_latest_prices_batch(klines_df, symbols_list)
                    found_count = len(market_data)
                    Log.logger.info(f"[{time_str}] [TRY_MATCH] 共找到{found_count}个标的的价格")
                    if found_count <= 3 and found_count > 0:
                        for symbol, data in list(market_data.items())[:3]:
                            Log.logger.info(f"[{time_str}] [TRY_MATCH] 找到{symbol}价格: {data['close']}, 成交量: {data['volume']}")
            else:
                # 对于日线数据，批量获取
                quote_df = self.data_center.get_quotes(symbols_list, freq=freq, time=current_time, fields=['close'])
                if not quote_df.empty:
                    for symbol in symbols_list:
                        if symbol in quote_df.index:
                            market_data[symbol] = quote_df.loc[symbol].to_dict()

            # 执行撮合
            Log.logger.info(f"[{time_str}] [TRY_MATCH] 获取到市场数据: {len(market_data)}个标的, 数据内容: {list(market_data.items())[:3]}...")
            self.trade_center.try_match_orders_sync(market_data)

        except Exception as e:
            Log.logger.error(f"[{time_str}] 撮合过程中发生错误: {e}")
            import traceback
            traceback.print_exc()
            
    def _handle_market_end_sync(self, event):
        """处理收盘事件 - 同步版本"""
        # 取消未成交的市价单（使用 active_orders）
        for order_id, order in list(self.trade_center.active_orders.items()):
            if order.order_type == OrderType.MARKET:
                order.status = OrderStatus.CANCELLED
                order.rejected_reason = "收盘时未成交自动撤销"
                # 从活跃订单中移除
                del self.trade_center.active_orders[order_id]
                
    def _handle_day_end_sync(self, event):
        """处理日终事件 - 同步版本"""
        try:
            current_time = self.context['current_dt']
            Log.logger.info(f"处理日终事件: {current_time}")
            
            # 更新所有持仓的市值
            market = self.context['settings']['market']
            freq = self.context['settings']['freq']
            
            # 获取所有持仓的标的
            symbols = list(self.trade_center.positions.keys())
            if symbols:
                try:
                    # 获取最新价格
                    quote_df = self.data_center.get_quotes(symbols, freq, current_time)
                    if not quote_df.empty:
                        for symbol in symbols:
                            if symbol in quote_df.index:
                                latest_price = quote_df.loc[symbol, 'close']
                                position = self.trade_center.positions[symbol]
                                position.last_price = latest_price
                                # 更新持仓市值
                                position.market_value = position.volume * latest_price
                                position.unrealized_pnl = (latest_price - position.cost_price) * position.volume
                                Log.logger.debug(f"更新持仓市值: {symbol} 数量:{position.volume} 价格:{latest_price} 市值:{position.market_value}")
                except Exception as e:
                    Log.logger.warning(f"更新持仓市值失败: {e}")
                
            # 更新账户价值
            self.trade_center._update_account_value()
            
            # 调试信息
            Log.logger.info(f"调试 - 更新后账户总资产: {self.trade_center.account.total_assets:.2f}")
            Log.logger.info(f"调试 - 更新后持仓市值: {self.trade_center.account.market_value:.2f}")
            Log.logger.info(f"调试 - 更新后现金: {self.trade_center.account.cash_available:.2f}")
            
            # 记录每日净值
            daily_record = {
                'date': current_time.strftime('%Y-%m-%d'),
                'total_assets': self.trade_center.account.total_assets,
                'cash': self.trade_center.account.cash_available,
                'positions_value': self.trade_center.account.market_value,
                'pnl_realized': self.trade_center.account.pnl_realized,
                'pnl_unrealized': sum(pos.unrealized_pnl for pos in self.trade_center.positions.values())
            }
            self.context['logs']['daily_history'].append(daily_record)
            
            Log.logger.info(f"记录每日净值: {daily_record['date']}, 总资产: {daily_record['total_assets']:.2f}")
            
            # 打印每日资产情况
            Log.logger.info(f"日期: {daily_record['date']}, 总资产: {daily_record['total_assets']:.2f}, "
                           f"现金: {daily_record['cash']:.2f}, 持仓市值: {daily_record['positions_value']:.2f}, "
                           f"已实现盈亏: {daily_record['pnl_realized']:.2f}, 未实现盈亏: {daily_record['pnl_unrealized']:.2f}")
            
        except Exception as e:
            Log.logger.error(f"处理日终事件失败: {e}")
            
    def _handle_before_market_sync(self, event):
        """处理盘前事件 - 同步版本

        注意：分红送股等公司行为由 CORPORATE_ACTION 事件统一处理，
        不在此处处理，以避免重复计算。
        """
        try:
            current_time = self.context['current_dt']
            current_date = current_time.date()

            Log.logger.debug(f"[盘前事件] {current_date} 盘前准备完成")

            # T+1规则：日始时解冻昨日买入的持仓
            if self.context.get('settings', {}).get('t1_rule', True):
                for symbol, position in self.trade_center.positions.items():
                    if hasattr(position, 'buy_dates') and position.buy_dates:
                        # 解冻昨日及之前买入的持仓
                        newly_available = 0.0
                        remaining_buy_dates = []
                        for buy_time, buy_volume in position.buy_dates:
                            if buy_time.date() < current_date:
                                # 昨日及之前买入的，解冻
                                newly_available += buy_volume
                            else:
                                # 今日买入的，保持冻结
                                remaining_buy_dates.append((buy_time, buy_volume))

                        if newly_available > 0:
                            position.available_volume += newly_available
                            # 注意：frozen_volume 是计算属性 (volume - available_volume)，不需要直接设置
                            position.buy_dates = remaining_buy_dates
                            Log.logger.info(f"[T+1解冻] {symbol}: 解冻{newly_available}股, 可用{position.available_volume}股, 冻结{position.frozen_volume}股")

            # 更新持仓市值
            for symbol, position in self.trade_center.positions.items():
                try:
                    quote = self.data_center.get_quotes([symbol], '1d', current_time)
                    if not quote.empty and symbol in quote.index:
                        current_price = quote.loc[symbol, 'close']
                        position.market_value = position.volume * current_price
                        position.unrealized_pnl = (current_price - position.cost_price) * position.volume
                except Exception as e:
                    Log.logger.warning(f"更新{symbol}持仓市值失败: {e}")

            # 更新账户总资产
            self.trade_center._update_account_value()

        except Exception as e:
            Log.logger.error(f"处理盘前事件失败: {e}")
            import traceback
            traceback.print_exc()
            
    def _handle_corporate_action_sync(self, event):
        """处理公司行为事件 - 同步版本

        只处理与持仓相关的公司行为，非持仓股票的公司行为不会输出日志。
        """
        try:
            # 获取基本信息（需要先获取symbol来检查持仓）
            if hasattr(event, 'data') and event.data:
                symbol = event.data.get('symbol', '')
                action_type = event.data.get('action_type', '')
            else:
                symbol = getattr(event, 'symbol', '')
                action_type = getattr(event, 'action_type', '')

            # 先检查是否在持仓中，不在持仓中则直接返回（不处理也不输出日志）
            if not symbol or symbol not in self.trade_center.positions:
                return

            # 调用TradeCenter处理公司行为事件
            self.trade_center.handle_corporate_action(event)

            # 记录事件 - 构建详细的日志信息
            log_parts = []

            log_parts.append(f"{symbol}")

            # 根据事件类型添加详细信息
            action_type_lower = action_type.lower() if action_type else ''

            if action_type_lower == 'dividend':
                # 分红事件
                dividend = getattr(event, 'dividend_per_share', None) or (event.data.get('dividend_per_share') if hasattr(event, 'data') else 0)
                if dividend:
                    log_parts.append(f"现金分红 每股{float(dividend):.4f}元")

            elif action_type_lower == 'bonus':
                # 送股事件
                ratio = getattr(event, 'bonus_ratio', None) or (event.data.get('bonus_ratio') if hasattr(event, 'data') else 0)
                if ratio:
                    # 转换显示格式：0.1 -> 10送1
                    log_parts.append(f"送股 每10股送{float(ratio)*10:.0f}股")

            elif action_type_lower == 'dividend_bonus':
                # 分红送股事件
                dividend = getattr(event, 'dividend_per_share', None) or (event.data.get('dividend_per_share') if hasattr(event, 'data') else 0)
                ratio = getattr(event, 'bonus_ratio', None) or (event.data.get('bonus_ratio') if hasattr(event, 'data') else 0)
                parts = []
                if dividend:
                    parts.append(f"分红{float(dividend):.4f}元")
                if ratio:
                    parts.append(f"送{float(ratio)*10:.0f}股")
                if parts:
                    log_parts.append(f"10股: {' '.join(parts)}")

            elif action_type_lower == 'transfer':
                # 转增事件
                ratio = getattr(event, 'transfer_ratio', None) or (event.data.get('transfer_ratio') if hasattr(event, 'data') else 0)
                if ratio:
                    log_parts.append(f"转增 每10股转增{float(ratio)*10:.0f}股")

            elif action_type_lower == 'split':
                # 拆股事件
                ratio = getattr(event, 'split_ratio', None) or (event.data.get('split_ratio') if hasattr(event, 'data') else 0)
                if ratio:
                    # 转换显示格式：2 -> 1拆2
                    log_parts.append(f"拆股 1拆{float(ratio):.0f}")

            elif action_type_lower == 'rights':
                # 配股事件
                ratio = getattr(event, 'rights_ratio', None) or (event.data.get('rights_ratio') if hasattr(event, 'data') else 0)
                price = getattr(event, 'rights_price', None) or (event.data.get('rights_price') if hasattr(event, 'data') else 0)
                if ratio:
                    log_parts.append(f"配股 每10股配{float(ratio)*10:.0f}股 配股价{float(price):.2f}元")

            else:
                # 其他类型或未知类型
                log_parts.append(f"{action_type}")

            # 获取除权除息日期
            ex_date = getattr(event, 'ex_date', None) or (event.data.get('ex_date') if hasattr(event, 'data') else None)
            if ex_date:
                log_parts.append(f"除权除息日{ex_date}")

            Log.logger.info(f"公司行为: {' | '.join(log_parts)}")

        except Exception as e:
            Log.logger.error(f"处理公司行为事件失败: {e}")
            
    def _calculate_performance_sync(self):
        """计算绩效指标 - 同步版本"""
        Log.logger.info("开始计算绩效指标")
        
        daily_history = self.context['logs']['daily_history']
        Log.logger.info(f"每日历史记录数量: {len(daily_history)}")
        
        if len(daily_history) < 2:
            Log.logger.warning("每日历史记录不足，无法计算绩效指标")
            return
            
        # 计算日收益率
        returns = []
        for i in range(1, len(daily_history)):
            prev_value = daily_history[i-1]['total_assets']
            curr_value = daily_history[i]['total_assets']
            # 防御性检查：避免除以0或负数
            if prev_value > 0:
                daily_return = (curr_value - prev_value) / prev_value
            else:
                Log.logger.warning(f"第{i-1}天总资产异常({prev_value:.2f})，无法计算收益率")
                daily_return = 0.0
            returns.append(daily_return)
            
        self.context['performance']['returns'] = returns
        
        # 计算基本统计指标
        if returns:
            import numpy as np
            returns_array = np.array(returns)
            
            # 年化收益率
            total_return = (daily_history[-1]['total_assets'] / daily_history[0]['total_assets']) - 1
            # 确保是实数
            if isinstance(total_return, complex):
                total_return = total_return.real
            trading_days = len(returns)
            annual_return = (1 + total_return) ** (252 / trading_days) - 1
            # 确保是实数
            if isinstance(annual_return, complex):
                annual_return = annual_return.real
            
            # 年化波动率
            annual_volatility = np.std(returns_array) * np.sqrt(252)
            
            # 夏普比率
            risk_free_rate = 0.03  # 假设无风险利率3%
            sharpe_ratio = (annual_return - risk_free_rate) / annual_volatility if annual_volatility > 0 else 0
            
            # 最大回撤
            cumulative_returns = np.cumprod(1 + returns_array)
            peak = np.maximum.accumulate(cumulative_returns)
            drawdown = (cumulative_returns - peak) / peak
            max_drawdown = np.min(drawdown)

            # 打印历史净值曲线（用于排查回撤问题）
            self._print_equity_curve(daily_history, cumulative_returns, drawdown, max_drawdown)

            # 胜率
            win_trades = len([r for r in returns if r > 0])
            win_ratio = win_trades / len(returns) if returns else 0
            
            # 更新绩效指标
            self.context['performance']['indicators'] = {
                'total_return': total_return,
                'annual_return': annual_return,
                'annual_volatility': annual_volatility,
                'sharpe_ratio': sharpe_ratio,
                'max_drawdown': max_drawdown
            }
            self.context['performance']['win_ratio'] = win_ratio
            self.context['performance']['trade_num'] = len(self.trade_center.trades)
            
            Log.logger.info(f"回测绩效 - 总收益: {total_return:.2%}, 年化收益: {annual_return:.2%}, "
                          f"夏普比率: {sharpe_ratio:.2f}, 最大回撤: {max_drawdown:.2%}, "
                          f"胜率: {win_ratio:.2%}, 交易次数: {len(self.trade_center.trades)}")

            # ========== 计算基准收益率和超额收益 ==========
            self._calculate_benchmark_performance(daily_history, returns_array, trading_days)

    def _print_equity_curve(self, daily_history: List[Dict], cumulative_returns: np.ndarray,
                            drawdown: np.ndarray, max_drawdown: float):
        """打印历史净值曲线和回撤曲线，用于排查回撤问题

        Args:
            daily_history: 每日历史记录列表
            cumulative_returns: 累计收益率数组
            drawdown: 回撤数组
            max_drawdown: 最大回撤值
        """
        if not daily_history or len(daily_history) < 2:
            return

        Log.logger.info("=" * 80)
        Log.logger.info("【净值曲线分析】")

        # 基本信息
        start_date = daily_history[0].get('date', 'N/A')
        end_date = daily_history[-1].get('date', 'N/A')
        start_value = daily_history[0].get('total_assets', 0)
        end_value = daily_history[-1].get('total_assets', 0)

        Log.logger.info(f"回测区间: {start_date} ~ {end_date}")
        Log.logger.info(f"初始资金: {start_value:,.2f}, 最终资金: {end_value:,.2f}")

        # 找到关键点位
        max_dd_idx = np.argmin(drawdown)
        peak_idx = np.argmax(cumulative_returns[:max_dd_idx + 1]) if max_dd_idx > 0 else 0

        # 累计净值曲线关键点
        Log.logger.info("-" * 80)
        Log.logger.info("【累计净值关键点】")
        Log.logger.info(f"起点    : 日期={start_date}, 净值=1.0000, 资金={start_value:,.2f}")

        # 最高点
        max_nav_idx = np.argmax(cumulative_returns)
        max_nav_date = daily_history[min(max_nav_idx + 1, len(daily_history) - 1)].get('date', 'N/A')
        max_nav_value = daily_history[min(max_nav_idx + 1, len(daily_history) - 1)].get('total_assets', 0)
        Log.logger.info(f"最高点  : 日期={max_nav_date}, 净值={cumulative_returns[max_nav_idx]:.4f}, 资金={max_nav_value:,.2f}")

        # 最低点
        min_nav_idx = np.argmin(cumulative_returns)
        min_nav_date = daily_history[min(min_nav_idx + 1, len(daily_history) - 1)].get('date', 'N/A')
        min_nav_value = daily_history[min(min_nav_idx + 1, len(daily_history) - 1)].get('total_assets', 0)
        Log.logger.info(f"最低点  : 日期={min_nav_date}, 净值={cumulative_returns[min_nav_idx]:.4f}, 资金={min_nav_value:,.2f}")

        # 终点
        Log.logger.info(f"终点    : 日期={end_date}, 净值={cumulative_returns[-1]:.4f}, 资金={end_value:,.2f}")

        # 最大回撤详情
        Log.logger.info("-" * 80)
        Log.logger.info("【最大回撤详情】")
        if max_drawdown < 0:
            peak_date = daily_history[min(peak_idx + 1, len(daily_history) - 1)].get('date', 'N/A')
            trough_date = daily_history[min(max_dd_idx + 1, len(daily_history) - 1)].get('date', 'N/A')
            peak_nav = cumulative_returns[peak_idx]
            trough_nav = cumulative_returns[max_dd_idx]
            Log.logger.info(f"回撤区间: {peak_date} ~ {trough_date}")
            Log.logger.info(f"峰值净值: {peak_nav:.4f} (日期: {peak_date})")
            Log.logger.info(f"谷值净值: {trough_nav:.4f} (日期: {trough_date})")
            Log.logger.info(f"最大回撤: {max_drawdown:.2%}")
            Log.logger.info(f"回撤天数: {max_dd_idx - peak_idx} 天")
        else:
            Log.logger.info("无回撤（最大回撤=0）")

        # 如果回撤异常（>50%），打印详细的历史数据
        if max_drawdown < -0.5:
            Log.logger.info("-" * 80)
            Log.logger.info("【关键】回撤区间详细记录（峰值前5天 ~ 谷值后5天）：")
            Log.logger.info(f"{'序号':>6} | {'日期':>12} | {'总资产':>15} | {'累计净值':>10} | {'回撤':>10} | {'备注':>10}")
            Log.logger.info("-" * 80)

            # 计算回撤区间的起始和结束索引（peak_idx 和 max_dd_idx 是 returns 数组的索引，需要 +1 对应 daily_history）
            start_idx = max(0, peak_idx - 5)  # 峰值前5天
            end_idx = min(len(daily_history) - 2, max_dd_idx + 5)  # 谷值后5天

            for i in range(start_idx, end_idx + 1):
                date = daily_history[i + 1].get('date', 'N/A')
                total = daily_history[i + 1].get('total_assets', 0)
                nav = cumulative_returns[i] if i < len(cumulative_returns) else 0
                dd = drawdown[i] if i < len(drawdown) else 0

                # 添加备注标记关键点位
                remark = ""
                if i == peak_idx:
                    remark = "【峰值】"
                elif i == max_dd_idx:
                    remark = "【谷值】"

                Log.logger.info(f"{i + 1:>6} | {date:>12} | {total:>15,.2f} | {nav:>10.4f} | {dd:>9.2%} | {remark:>10}")

            Log.logger.info("-" * 80)
            Log.logger.info("【警告】最大回撤超过50%，同时打印完整净值历史（前20条和后20条）：")
            Log.logger.info(f"{'序号':>6} | {'日期':>12} | {'总资产':>15} | {'累计净值':>10} | {'回撤':>10}")
            Log.logger.info("-" * 80)

            # 打印前20条
            for i in range(min(20, len(daily_history) - 1)):
                date = daily_history[i + 1].get('date', 'N/A')
                total = daily_history[i + 1].get('total_assets', 0)
                nav = cumulative_returns[i] if i < len(cumulative_returns) else 0
                dd = drawdown[i] if i < len(drawdown) else 0
                Log.logger.info(f"{i + 1:>6} | {date:>12} | {total:>15,.2f} | {nav:>10.4f} | {dd:>9.2%}")

            if len(daily_history) > 40:
                Log.logger.info(f"{'...':>6} | {'...':>12} | {'...':>15} | {'...':>10} | {'...':>10}")

            # 打印后20条
            for i in range(max(20, len(daily_history) - 20), len(daily_history) - 1):
                date = daily_history[i + 1].get('date', 'N/A')
                total = daily_history[i + 1].get('total_assets', 0)
                nav = cumulative_returns[i] if i < len(cumulative_returns) else 0
                dd = drawdown[i] if i < len(drawdown) else 0
                Log.logger.info(f"{i + 1:>6} | {date:>12} | {total:>15,.2f} | {nav:>10.4f} | {dd:>9.2%}")

        # 月度/季度统计摘要
        Log.logger.info("-" * 80)
        Log.logger.info("【收益分布统计】")
        returns_array = np.diff([h.get('total_assets', 0) for h in daily_history]) / \
                       np.array([h.get('total_assets', 1) for h in daily_history[:-1]])
        positive_days = len([r for r in returns_array if r > 0])
        negative_days = len([r for r in returns_array if r < 0])
        zero_days = len(returns_array) - positive_days - negative_days
        Log.logger.info(f"盈利天数: {positive_days} ({positive_days/len(returns_array):.1%})")
        Log.logger.info(f"亏损天数: {negative_days} ({negative_days/len(returns_array):.1%})")
        Log.logger.info(f"持平天数: {zero_days} ({zero_days/len(returns_array):.1%})")

        if len(returns_array) > 0:
            Log.logger.info(f"最大单日盈利: {np.max(returns_array):.2%}")
            Log.logger.info(f"最大单日亏损: {np.min(returns_array):.2%}")

        Log.logger.info("=" * 80)

    def _calculate_benchmark_performance(self, daily_history: List[Dict], strategy_returns_array, trading_days: int):
        """计算基准收益率和超额收益指标

        Args:
            daily_history: 每日历史记录
            strategy_returns_array: 策略日收益率数组
            trading_days: 交易日数量
        """
        benchmark = self.context.get('benchmark')
        if not benchmark:
            Log.logger.info("未设置基准，跳过基准收益率计算")
            return

        try:
            # 获取回测起止日期
            start_date = daily_history[0].get('date') if isinstance(daily_history[0], dict) else daily_history[0]
            end_date = daily_history[-1].get('date') if isinstance(daily_history[-1], dict) else daily_history[-1]

            # 如果是 datetime 对象，转为字符串
            if hasattr(start_date, 'strftime'):
                start_date = start_date.strftime('%Y-%m-%d')
            if hasattr(end_date, 'strftime'):
                end_date = end_date.strftime('%Y-%m-%d')

            # 获取基准市场：优先使用配置，否则根据代码后缀智能推断
            benchmark_market = self.context.get('settings', {}).get('benchmark_market')
            if not benchmark_market:
                # 根据基准代码后缀推断市场
                benchmark_market = self._infer_benchmark_market(benchmark)

            Log.logger.info(f"获取基准数据: {benchmark}, 市场: {benchmark_market}, {start_date} -> {end_date}")

            # 获取基准K线数据 - 基准通常是指数，直接调用 data_interface
            # 不能使用 data_center.get_klines()，因为后者会从 context 获取市场（默认 cn_stock）
            benchmark_df = self.data_center.data_interface.get_klines(
                codes=benchmark,
                market=benchmark_market,
                freq='1d',
                start_date=start_date,
                end_date=end_date,
                fields=['close'],
                adj_type='none',
                use_cache=True
            )

            if benchmark_df is None or benchmark_df.empty:
                Log.logger.warning(f"无法获取基准数据: {benchmark}")
                return

            # 提取基准收盘价（处理 MultiIndex）
            if hasattr(benchmark_df.index, 'levels') and len(benchmark_df.index.levels) > 0:
                # MultiIndex (time, symbol)，按 symbol 过滤
                if benchmark in benchmark_df.index.get_level_values(1):
                    benchmark_prices = benchmark_df.loc[(slice(None), benchmark), 'close']
                else:
                    Log.logger.warning(f"基准 {benchmark} 不在返回数据中")
                    return
            else:
                # 单一 index，直接使用
                benchmark_prices = benchmark_df['close']

            # 计算基准日收益率
            benchmark_returns = benchmark_prices.pct_change().dropna().values

            # 对齐收益率序列长度（取较小值）
            min_len = min(len(strategy_returns_array), len(benchmark_returns))
            if min_len < 2:
                Log.logger.warning("数据点不足，无法计算基准指标")
                return

            aligned_strategy_returns = strategy_returns_array[:min_len]
            aligned_benchmark_returns = benchmark_returns[:min_len]

            # 计算基准指标
            benchmark_total_return = (benchmark_prices.iloc[-1] / benchmark_prices.iloc[0]) - 1
            benchmark_annual_return = (1 + benchmark_total_return) ** (252 / trading_days) - 1
            benchmark_volatility = np.std(aligned_benchmark_returns) * np.sqrt(252)

            # 超额收益
            strategy_total_return = self.context['performance']['indicators']['total_return']
            excess_return = strategy_total_return - benchmark_total_return

            # 跟踪误差（策略收益与基准收益的差值的标准差）
            excess_returns_daily = aligned_strategy_returns - aligned_benchmark_returns
            tracking_error = np.std(excess_returns_daily) * np.sqrt(252)

            # 信息比率（年化超额收益 / 跟踪误差）
            information_ratio = excess_return / tracking_error if tracking_error > 0 else 0

            # 更新绩效指标
            self.context['performance']['benchmark'] = {
                'total_return': float(benchmark_total_return),
                'annual_return': float(benchmark_annual_return),
                'volatility': float(benchmark_volatility),
            }

            self.context['performance']['indicators'].update({
                'excess_return': float(excess_return),
                'tracking_error': float(tracking_error),
                'information_ratio': float(information_ratio),
            })

            Log.logger.info(f"基准收益率 - 总收益: {benchmark_total_return:.2%}, 年化收益: {benchmark_annual_return:.2%}")
            Log.logger.info(f"超额收益 - 超额收益: {excess_return:.2%}, 跟踪误差: {tracking_error:.2%}, 信息比率: {information_ratio:.2f}")

        except Exception as e:
            Log.logger.error(f"计算基准收益率失败: {e}")

    def _infer_benchmark_market(self, benchmark: str) -> str:
        """根据基准代码后缀推断市场类型

        Args:
            benchmark: 基准代码，如 000001.SH, 000300.SZ, HSI.HK

        Returns:
            str: 市场类型 (cn_index, hk_index, global_index等)
        """
        # 后缀到市场的映射
        suffix_to_market = {
            '.SH': 'cn_index',   # 上证指数
            '.SZ': 'cn_index',   # 深证指数
            '.HK': 'hk_index',   # 香港指数
            '.US': 'us_index',   # 美国指数
            '.UK': 'global_index',
            '.JP': 'global_index',
        }

        # 检查后缀
        for suffix, market in suffix_to_market.items():
            if benchmark.endswith(suffix):
                Log.logger.debug(f"基准 {benchmark} 后缀 {suffix} 推断为市场: {market}")
                return market

        # 默认返回 cn_index
        Log.logger.debug(f"基准 {benchmark} 无法推断市场，使用默认: cn_index")
        return 'cn_index'
    
    def _handle_market_event(self, event):
        """通用市场事件处理器"""
        try:
            Log.logger.debug(f"处理市场事件: {event.event_type.value} at {event.event_time}")
            
            # 根据不同的事件类型执行相应的处理
            if event.event_type == EventTypeEnum.BEFORE_MARKET:
                asyncio.create_task(self._handle_before_market(event))
            elif event.event_type == EventTypeEnum.MARKET_END:
                asyncio.create_task(self._handle_market_end(event))
            elif event.event_type == EventTypeEnum.DAY_END:
                asyncio.create_task(self._handle_day_end(event))
            elif event.event_type == EventTypeEnum.TRY_MATCH:
                asyncio.create_task(self._handle_try_match(event))
            elif event.event_type == EventTypeEnum.CORPORATE_ACTION:
                asyncio.create_task(self._handle_corporate_action(event))
            else:
                # 其他市场事件的默认处理（主要是记录日志）
                Log.logger.debug(f"市场事件 {event.event_type.value} 已处理")
                
        except Exception as e:
            Log.logger.error(f"处理市场事件失败 {event.event_type.value}: {e}")
    
    async def _handle_before_market(self, event):
        """处理盘前事件"""
        try:
            # 处理除权除息等盘前事件
            Log.logger.debug(f"处理盘前事件: {event.event_time}")
        except Exception as e:
            Log.logger.error(f"处理盘前事件失败: {e}") 