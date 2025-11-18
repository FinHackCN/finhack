"""
主回测引擎

协调EventCenter、TradeCenter、DataCenter、StrategyExecutor四大组件
实现完整的回测流程
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pandas as pd

import finhack.library.log as Log
from ..events.event_types import EventTypeEnum, BaseEvent
from ..models.enums import *
from ..models.account import Account
from ..models.position import Position
from ..models.order import Order
from ..models.trade import Trade


class TradeCenter:
    """交易中心 - 模拟交易所功能"""
    
    def __init__(self, context: Dict, market_adapter=None):
        self.context = context
        self.market_adapter = market_adapter
        
        # 初始化账户
        self.account = Account.from_dict(context['account'])
        self.positions = {}  # symbol -> Position
        self.orders = {}     # order_id -> Order
        self.trades = []     # List[Trade]
        
        # 订单ID计数器
        self.order_id_counter = 1
        self.trade_id_counter = 1
        
    async def get_account(self, adapter_id: str, refresh: bool = False) -> Account:
        """获取账户信息"""
        return self.account
        
    async def get_positions(self, adapter_id: str, symbol: Optional[str] = None, refresh: bool = False) -> List[Position]:
        """获取持仓信息"""
        if symbol:
            return [self.positions[symbol]] if symbol in self.positions else []
        return list(self.positions.values())
        
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
        order.status = OrderStatus.NEW
        
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
        
        Log.logger.info(f"订单撤销成功: {order_id}")
        return True
        
    async def try_match_orders(self, market_data: Dict):
        """尝试撮合订单"""
        current_time = self.context.get('current_dt', datetime.now())
        
        for order_id, order in list(self.orders.items()):
            if order.status != OrderStatus.NEW:
                continue
                
            symbol = order.symbol
            if symbol not in market_data:
                continue
                
            # 获取市场价格
            market_price = None
            if order.order_type == OrderType.MARKET:
                market_price = market_data[symbol].get('close', market_data[symbol].get('price'))
            elif order.order_type == OrderType.LIMIT and order.price:
                current_price = market_data[symbol].get('close', market_data[symbol].get('price'))
                # 简单撮合逻辑：买单价格大于等于市价，卖单价格小于等于市价
                if ((order.side == Side.BUY and order.price >= current_price) or 
                    (order.side == Side.SELL and order.price <= current_price)):
                    market_price = order.price
                    
            if market_price is None or market_price <= 0:
                continue
                
            # 执行撮合
            await self._execute_trade(order, market_price, current_time)
            
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
        
        # 更新账户
        self.account.market_value = positions_value
        self.account.total_assets = self.account.cash_available + self.account.cash_frozen + positions_value
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

    def try_match_orders_sync(self, market_data: Dict[str, Dict]):
        """撮合订单 - 同步版本"""
        matched_orders = []
        
        Log.logger.debug(f"开始撮合，共{len(self.orders)}个订单，市场数据: {list(market_data.keys())}")
        
        for order_id, order in self.orders.items():
            if order.status != OrderStatus.NEW:
                continue
                
            symbol = order.symbol
            Log.logger.debug(f"检查订单 {order_id}: {symbol}, 类型: {order.order_type}, 方向: {order.side}, 价格: {order.price}")
            
            if symbol not in market_data:
                Log.logger.debug(f"跳过订单 {order_id}: {symbol} 不在市场数据中")
                continue
                
            quote = market_data[symbol]
            current_price = quote.get('close', 0)
            Log.logger.debug(f"订单 {order_id} 当前价格: {current_price}")
            
            if current_price <= 0:
                Log.logger.debug(f"跳过订单 {order_id}: 价格无效 {current_price}")
                continue
                
            # 判断是否可以成交
            can_fill = False
            fill_price = current_price
            
            if order.order_type == OrderType.MARKET:
                # 市价单直接成交
                can_fill = True
                fill_price = self._apply_slippage(current_price, order.side)
                Log.logger.debug(f"订单 {order_id} 市价单直接成交，价格: {fill_price}")
            elif order.order_type == OrderType.LIMIT:
                # 限价单需要判断价格
                if order.side == Side.BUY and order.price >= current_price:
                    can_fill = True
                    fill_price = min(order.price, current_price)
                    Log.logger.debug(f"订单 {order_id} 限价买单可成交: 订单价格{order.price} >= 当前价格{current_price}, 成交价格: {fill_price}")
                elif order.side == Side.SELL and order.price <= current_price:
                    can_fill = True
                    fill_price = max(order.price, current_price)
                    Log.logger.debug(f"订单 {order_id} 限价卖单可成交: 订单价格{order.price} <= 当前价格{current_price}, 成交价格: {fill_price}")
                else:
                    Log.logger.debug(f"订单 {order_id} 限价单价格不匹配: 订单价格{order.price}, 当前价格{current_price}")
                    
            if can_fill:
                # 执行成交
                Log.logger.info(f"订单 {order_id} 准备成交: {symbol} {order.side} {order.volume} @ {fill_price}")
                self._execute_trade_sync(order, fill_price)
                matched_orders.append(order_id)
            else:
                Log.logger.debug(f"订单 {order_id} 不可成交")
                
        Log.logger.info(f"撮合完成，成交订单数: {len(matched_orders)}，详细: {matched_orders}")
        
    def _execute_trade_sync(self, order: Order, fill_price: float):
        """执行成交 - 同步版本"""
        try:
            # 生成成交ID
            trade_id = f"trade_{self.trade_id_counter:06d}"
            self.trade_id_counter += 1
            
            # 计算成交金额
            fill_amount = order.volume * fill_price
            
            # 计算费用
            commission = self._calculate_commission(fill_amount, order.side)
            tax = self._calculate_tax(fill_amount, order.side)
            total_cost = commission + tax
            
            # 创建成交记录
            trade = Trade(
                account_id=order.account_id,
                symbol=order.symbol,
                order_id=order.order_id,
                trade_id=trade_id,
                side=order.side,
                volume=order.volume,
                price=fill_price,
                amount=fill_amount,
                commission=commission,
                tax=tax,
                trade_time=self.context.get('current_dt', datetime.now())
            )
            
            # 更新订单状态
            order.status = OrderStatus.FILLED
            order.filled_volume = order.volume
            order.filled_amount = fill_amount
            order.avg_fill_price = fill_price
            order.updated_time = trade.trade_time
            
            # 添加成交记录
            self.trades.append(trade)
            
            # 更新持仓和账户
            self._update_position_sync(trade)
            self._update_account_sync(trade, total_cost)
            
            Log.logger.info(f"成交执行完成: {trade_id} {order.symbol} {order.side} "
                          f"{order.volume}@{fill_price} 费用:{total_cost:.2f}")
            
        except Exception as e:
            Log.logger.error(f"执行成交失败: {e}")
            order.status = OrderStatus.REJECTED
            order.rejected_reason = f"成交执行失败: {str(e)}"
            
    def _update_position_sync(self, trade: Trade):
        """更新持仓 - 同步版本"""
        symbol = trade.symbol
        
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
            # 买入：增加持仓
            total_cost = position.volume * position.cost_price + trade.volume * trade.price
            position.volume += trade.volume
            position.available_volume += trade.volume
            position.cost_price = total_cost / position.volume if position.volume > 0 else 0
        else:
            # 卖出：减少持仓
            position.volume -= trade.volume
            position.available_volume -= trade.volume
            
            # 如果持仓为0，移除持仓记录
            if position.volume <= 0:
                del self.positions[symbol]
                return
                
        # 更新持仓市值
        position.market_value = position.volume * trade.price
        position.unrealized_pnl = (trade.price - position.cost_price) * position.volume
        position.last_price = trade.price
        
    def _update_account_sync(self, trade: Trade, total_cost: float):
        """更新账户 - 同步版本"""
        if trade.side == Side.BUY:
            # 买入：减少现金，增加持仓市值
            self.account.cash_available -= (trade.amount + total_cost)
            self.account.market_value += trade.amount
        else:
            # 卖出：增加现金，减少持仓市值
            self.account.cash_available += (trade.amount - total_cost)
            self.account.market_value -= trade.amount
            self.account.pnl_realized += (trade.price - self._get_cost_price(trade.symbol)) * trade.volume
            
        # 更新总资产
        self._update_account_value()
        
    def _get_cost_price(self, symbol: str) -> float:
        """获取持仓成本价"""
        if symbol in self.positions:
            return self.positions[symbol].cost_price
        return 0


class BacktestEngine:
    """回测引擎主类"""
    
    def __init__(self, context: Dict, data_center, event_center, event_bus):
        self.context = context
        self.data_center = data_center
        self.event_center = event_center
        self.event_bus = event_bus
        
        # 初始化交易中心
        self.trade_center = TradeCenter(context)
        
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
        for order in self.trade_center.orders.values():
            if order.status == OrderStatus.NEW:
                return True
        return False
        
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
        
        # 获取所有需要行情的标的
        symbols = set()
        for order in self.trade_center.orders.values():
            if order.status == OrderStatus.NEW:
                symbols.add(order.symbol)
                
        if not symbols:
            return
            
        # 获取行情数据
        try:
            market_data = {}
            for symbol in symbols:
                Log.logger.debug(f"获取 {symbol} 的行情数据，频率: {freq}, 时间: {current_time}")
                quote_df = self.data_center.get_quotes([symbol], freq=freq, time=current_time, fields=['close'])
                Log.logger.debug(f"{symbol} 行情数据结果: DataFrame形状={quote_df.shape}, 是否为空={quote_df.empty}")
                if not quote_df.empty:
                    market_data[symbol] = quote_df.loc[symbol].to_dict()
                    Log.logger.debug(f"{symbol} 行情数据内容: {market_data[symbol]}")
                else:
                    Log.logger.warning(f"无法获取 {symbol} 的行情数据")
                    
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
            else:
                Log.logger.warning(f"未找到定时任务函数: {function_name}")
                
        except Exception as e:
            Log.logger.error(f"执行定时任务失败: {e}")
            
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
            Log.logger.info("检测到1分钟频率回测，启动按月预加载策略")
            
            # 获取股票池
            universe = self.context.get('universe', None)
            
            # 预加载第一个交易日所在月份和上个月的数据（已优化）
            if calendar:
                first_trade_date = calendar[0]
                Log.logger.info(f"开始预加载初始数据: {first_trade_date}")
                print(f"[回测] 开始预加载 {market} 数据...", flush=True)
                self.data_center.ensure_monthly_data_loaded(
                    market=market,
                    current_date=first_trade_date,
                    universe=universe,
                    frequency=frequency
                )
                print(f"[回测] 初始数据预加载完成", flush=True)
        
        # 记录上一次处理的月份，用于检测月份变化
        last_processed_month = None
        
        # 主循环：遍历每个交易日
        for trade_date in calendar:
            self.context['current_dt'] = trade_date
            Log.logger.info(f"交易日: {trade_date.strftime('%Y-%m-%d')}")
            
            # 检查是否进入新的月份，如果是则预加载下个月数据（已优化）
            if frequency == '1m':
                current_month = f"{trade_date.year}-{trade_date.month:02d}"
                if current_month != last_processed_month:
                    Log.logger.info(f"检测到进入新月份: {current_month}")
                    print(f"[回测] 进入新月份 {current_month}，预加载数据...", flush=True)
                    
                    # 确保当前月份和下个月份数据已预加载
                    self.data_center.ensure_monthly_data_loaded(
                        market=market,
                        current_date=trade_date,
                        universe=self.context.get('universe', None),
                        frequency=frequency
                    )
                    
                    last_processed_month = current_month
                    print(f"[回测] 新月份数据预加载完成", flush=True)
            
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
                Log.logger.debug(f"智能加速检查：当前频率 {frequency}, 事件类型 {event.event_type}")
                
                if frequency == '1m' and event.event_type == EventTypeEnum.TRY_MATCH:
                    # 检查是否有挂单（未成交的订单）
                    has_pending_orders = self._has_pending_orders()
                    Log.logger.debug(f"智能加速检查：当前时间 {event.event_time}, 有挂单: {has_pending_orders}")
                    
                    if not has_pending_orders:
                        # 没有挂单，查找下一个非撮合事件
                        next_event_idx = i + 1
                        
                        # 添加详细的事件序列日志
                        Log.logger.debug(f"事件序列：当前事件索引 {i}, 当前事件 {event.event_time} ({event.event_type})")
                        for j in range(max(0, i-2), min(len(daily_events), i+10)):
                            ev = daily_events[j]
                            marker = " -> " if j == i else "    "
                            Log.logger.debug(f"{marker}[{j}] {ev.event_time} ({ev.event_type}) - {ev.event_description if hasattr(ev, 'event_description') else ''}")
                        
                        # 优化的跳跃逻辑：寻找重要事件进行更大跳跃
                        target_event_idx = None
                        target_event = None
                        
                        # 寻找下一个重要事件，允许更大的跳跃
                        while next_event_idx < len(daily_events):
                            next_event = daily_events[next_event_idx]
                            Log.logger.debug(f"检查下一个事件：索引 {next_event_idx}, {next_event.event_time} ({next_event.event_type})")
                            
                            # 重要事件优先级列表
                            important_events = [
                                EventTypeEnum.ON_TIME,      # 策略事件
                                EventTypeEnum.MORNING_END,  # 中午休市
                                EventTypeEnum.AFTERNOON_START,  # 下午开盘
                                EventTypeEnum.CLOSING_START, # 收盘开始
                                EventTypeEnum.MARKET_END,   # 市场结束
                                EventTypeEnum.DAY_START,    # 新一天开始
                                EventTypeEnum.BEFORE_MARKET # 盘前准备
                            ]
                            
                            if next_event.event_type in important_events:
                                Log.logger.debug(f"找到重要事件：索引 {next_event_idx}, {next_event.event_time} ({next_event.event_type})")
                                target_event_idx = next_event_idx
                                target_event = next_event
                                break
                            elif next_event.event_type != EventTypeEnum.TRY_MATCH:
                                # 如果不是撮合事件，也可以作为跳跃目标
                                target_event_idx = next_event_idx
                                target_event = next_event
                                break
                            
                            next_event_idx += 1
                        
                        # 执行跳跃 - 降低跳跃阈值并增加更灵活的跳跃条件
                        if target_event_idx is not None and target_event is not None:
                            time_diff = (target_event.event_time - event.event_time).total_seconds() / 60
                            
                            # 大幅降低跳跃阈值到10秒，任何有意义的跳跃都执行
                            # 如果跳跃超过30分钟，更是优先执行
                            if time_diff > 0.17 or time_diff > 30:  # 10秒或者30分钟以上
                                Log.logger.info(f"智能加速：无挂单，当前事件 {event.event_time} ({event.event_type})，跳转 {time_diff:.1f} 分钟到 {target_event.event_time} ({target_event.event_type})")
                                # 直接跳到目标事件
                                i = target_event_idx
                                continue
                        else:
                            Log.logger.debug("未找到下一个非撮合事件，继续正常处理")
                
                i += 1
                
            # 更新前一交易日
            self.context['previous_date'] = trade_date.date()
            
        # 计算绩效
        self._calculate_performance_sync()
        
        Log.logger.info("回测完成")
        
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
                
                Log.logger.info(f"下单成功: {security} {side} {volume:.2f} @ {price:.2f}, 订单ID: {order_id}")
                return order_id
                
            except Exception as e:
                Log.logger.error(f"下单失败: {e}")
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
                
                Log.logger.info(f"下单成功: {security} {side} {volume:.2f} @ {price:.2f}, 订单ID: {order_id}")
                return order_id
                
            except Exception as e:
                Log.logger.error(f"下单失败: {e}")
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
        """生成交易日历 - 同步版本"""
        # 简单实现：生成所有工作日
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
        
        calendar = []
        current_dt = start_dt
        
        while current_dt <= end_dt:
            # 跳过周末
            if current_dt.weekday() < 5:  # 0-4为周一到周五
                calendar.append(current_dt)
            current_dt += timedelta(days=1)
            
        return calendar
    
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
            else:
                Log.logger.warning(f"未找到定时任务函数: {function_name}")
                
        except Exception as e:
            Log.logger.error(f"执行定时任务失败: {e}")
            
    def _handle_try_match_sync(self, event):
        """处理撮合事件 - 同步版本"""
        # 获取当前市场数据
        current_time = self.context['current_dt']
        market = self.context['settings']['market']
        freq = self.context['settings']['freq']
        
        # 获取所有需要行情的标的
        symbols = set()
        for order_id, order in self.trade_center.orders.items():
            if hasattr(order, 'status') and order.status == OrderStatus.NEW:
                symbols.add(order.symbol)
                
        if not symbols:
            return
            
        # 获取行情数据
        try:
            market_data = {}
            for symbol in symbols:
                Log.logger.debug(f"获取 {symbol} 的行情数据，频率: {freq}, 时间: {current_time}")
                quote_df = self.data_center.get_quotes([symbol], freq=freq, time=current_time, fields=['close'])
                Log.logger.debug(f"{symbol} 行情数据结果: DataFrame形状={quote_df.shape}, 是否为空={quote_df.empty}")
                if not quote_df.empty:
                    market_data[symbol] = quote_df.loc[symbol].to_dict()
                    Log.logger.debug(f"{symbol} 行情数据内容: {market_data[symbol]}")
                else:
                    Log.logger.warning(f"无法获取 {symbol} 的行情数据")
                    
            # 执行撮合
            self.trade_center.try_match_orders_sync(market_data)
            
        except Exception as e:
            Log.logger.error(f"撮合过程中发生错误: {e}")
            
    def _handle_market_end_sync(self, event):
        """处理收盘事件 - 同步版本"""
        # 取消未成交的市价单
        for order in list(self.trade_center.orders.values()):
            if order.status == OrderStatus.NEW and order.order_type == OrderType.MARKET:
                order.status = OrderStatus.CANCELLED
                order.rejected_reason = "收盘时未成交自动撤销"
                
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
        """处理盘前事件 - 同步版本"""
        try:
            # 处理除权除息等盘前事件
            Log.logger.debug(f"处理盘前事件: {event.event_time}")
        except Exception as e:
            Log.logger.error(f"处理盘前事件失败: {e}")
            
    def _handle_corporate_action_sync(self, event):
        """处理公司行为事件 - 同步版本"""
        try:
            # 调用TradeCenter处理公司行为事件
            self.trade_center.handle_corporate_action(event)
            
            # 记录事件
            Log.logger.info(f"处理公司行为事件: {event.symbol} {event.action_type}")
            
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
            daily_return = (curr_value - prev_value) / prev_value
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