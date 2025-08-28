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
        # 计算持仓市值
        positions_value = sum(pos.market_value for pos in self.positions.values())
        
        # 更新账户
        self.account.market_value = positions_value
        self.account.total_assets = self.account.cash_available + self.account.cash_frozen + positions_value
        self.account.timestamp_updated = self.context.get('current_dt', datetime.now())
        
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
        
        for order_id, order in self.orders.items():
            if order.status != OrderStatus.NEW:
                continue
                
            symbol = order.symbol
            if symbol not in market_data:
                continue
                
            quote = market_data[symbol]
            current_price = quote.get('close', 0)
            
            if current_price <= 0:
                continue
                
            # 判断是否可以成交
            can_fill = False
            fill_price = current_price
            
            if order.order_type == OrderType.MARKET:
                # 市价单直接成交
                can_fill = True
                fill_price = self._apply_slippage(current_price, order.side)
            elif order.order_type == OrderType.LIMIT:
                # 限价单需要判断价格
                if order.side == Side.BUY and order.price >= current_price:
                    can_fill = True
                    fill_price = min(order.price, current_price)
                elif order.side == Side.SELL and order.price <= current_price:
                    can_fill = True
                    fill_price = max(order.price, current_price)
                    
            if can_fill:
                # 执行成交
                self._execute_trade_sync(order, fill_price)
                matched_orders.append(order_id)
                
        Log.logger.info(f"撮合完成，成交订单数: {len(matched_orders)}")
        
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
        
    def _update_account_value(self):
        """更新账户总资产"""
        self.account.total_assets = self.account.cash_available + self.account.market_value
        self.account.pnl_unrealized = sum(pos.unrealized_pnl for pos in self.positions.values())
        
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
            EventTypeEnum.DAY_END,
        ]
        
        # 为所有市场事件注册默认处理器
        for event_type in market_events:
            self.event_bus.register_handler(event_type, self._handle_market_event)
        
        # 注册交易相关事件处理器
        self.event_bus.register_handler(EventTypeEnum.ORDER_SUBMISSION, self._handle_order_submission)
        self.event_bus.register_handler(EventTypeEnum.ORDER_CANCELLATION, self._handle_order_cancellation)
        self.event_bus.register_handler(EventTypeEnum.TRY_MATCH, self._handle_try_match_sync)
        self.event_bus.register_handler(EventTypeEnum.ON_TIME, self._handle_on_time_sync)
        
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
                quote_df = self.data_center.get_quotes([symbol], freq, current_time)
                if not quote_df.empty:
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
        
        # 生成交易日历
        calendar = self._generate_calendar_sync(start_date, end_date)
        
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
                self._process_event_sync(event, strategy)
                
            # 更新前一交易日
            self.context['previous_date'] = trade_date.date()
            
        # 计算绩效
        self._calculate_performance_sync()
        
        Log.logger.info("回测完成")
        
    def _generate_calendar_sync(self, start_date: str, end_date: str) -> List[datetime]:
        """生成交易日历 - 同步版本"""
        # 简单实现：生成所有工作日
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
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
        
        # 根据事件类型直接处理
        if event.event_type == EventTypeEnum.ON_TIME:
            self._handle_on_time_sync(event)
        elif event.event_type == EventTypeEnum.TRY_MATCH:
            self._handle_try_match_sync(event)
        elif event.event_type == EventTypeEnum.MARKET_END:
            self._handle_market_end_sync(event)
        elif event.event_type == EventTypeEnum.DAY_END:
            self._handle_day_end_sync(event)
        elif event.event_type == EventTypeEnum.BEFORE_MARKET:
            self._handle_before_market_sync(event)
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
        for order in self.trade_center.orders.values():
            if order.status == OrderStatus.NEW:
                symbols.add(order.symbol)
                
        if not symbols:
            return
            
        # 获取行情数据
        try:
            market_data = {}
            for symbol in symbols:
                quote_df = self.data_center.get_quotes([symbol], freq, current_time)
                if not quote_df.empty:
                    market_data[symbol] = quote_df.loc[symbol].to_dict()
                    
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
        # 更新持仓市值
        current_time = self.context['current_dt']
        market = self.context['settings']['market']
        freq = self.context['settings']['freq']
        
        # 获取所有持仓的最新价格
        if self.trade_center.positions:
            symbols = list(self.trade_center.positions.keys())
            try:
                for symbol in symbols:
                    quote_df = self.data_center.get_quotes([symbol], freq, current_time)
                    if not quote_df.empty:
                        latest_price = quote_df.loc[symbol, 'close']
                        position = self.trade_center.positions[symbol]
                        position.last_price = latest_price
                        position.market_value = position.volume * latest_price
                        position.unrealized_pnl = (latest_price - position.cost_price) * position.volume
                        
            except Exception as e:
                Log.logger.warning(f"更新持仓市值失败: {e}")
                
        # 更新账户总资产
        self.trade_center._update_account_value()
        
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
        
    def _handle_before_market_sync(self, event):
        """处理盘前事件 - 同步版本"""
        try:
            # 处理除权除息等盘前事件
            Log.logger.debug(f"处理盘前事件: {event.event_time}")
        except Exception as e:
            Log.logger.error(f"处理盘前事件失败: {e}")
            
    def _calculate_performance_sync(self):
        """计算绩效指标 - 同步版本"""
        daily_history = self.context['logs']['daily_history']
        if len(daily_history) < 2:
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
            trading_days = len(returns)
            annual_return = (1 + total_return) ** (252 / trading_days) - 1
            
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