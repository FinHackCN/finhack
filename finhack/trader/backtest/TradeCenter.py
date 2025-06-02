import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import logging
import pandas as pd
import numpy as np
from collections import defaultdict

# 导入规则模块
from .Rules import RuleEngine


class OrderStatus(Enum):
    """订单状态枚举"""
    PENDING = "pending"          # 待提交
    SUBMITTED = "submitted"      # 已提交
    PARTIAL_FILLED = "partial_filled"  # 部分成交
    FILLED = "filled"           # 全部成交
    CANCELLED = "cancelled"     # 已撤销
    REJECTED = "rejected"       # 被拒绝


class OrderType(Enum):
    """订单类型枚举"""
    MARKET = "market"           # 市价单
    LIMIT = "limit"             # 限价单
    STOP = "stop"               # 止损单
    STOP_LIMIT = "stop_limit"   # 止损限价单


class OrderSide(Enum):
    """订单方向枚举"""
    BUY = "buy"                 # 买入
    SELL = "sell"               # 卖出
    BUY_TO_COVER = "buy_to_cover"  # 买入平仓（做空）
    SELL_SHORT = "sell_short"   # 卖出开仓（做空）


@dataclass
class Order:
    """订单对象"""
    order_id: str
    adapter_id: str
    code: str
    side: OrderSide
    order_type: OrderType
    quantity: float
    price: Optional[float] = None
    stop_price: Optional[float] = None
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: float = 0.0
    avg_filled_price: float = 0.0
    commission: float = 0.0
    tax: float = 0.0
    slippage: float = 0.0
    create_time: datetime = field(default_factory=datetime.now)
    update_time: datetime = field(default_factory=datetime.now)
    tags: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Trade:
    """成交记录"""
    trade_id: str
    order_id: str
    adapter_id: str
    code: str
    side: OrderSide
    quantity: float
    price: float
    commission: float
    tax: float
    slippage: float
    trade_time: datetime
    tags: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Position:
    """持仓对象"""
    adapter_id: str
    code: str
    quantity: float = 0.0              # 持仓数量
    available_quantity: float = 0.0    # 可用数量
    avg_cost: float = 0.0              # 平均成本
    market_value: float = 0.0          # 市值
    unrealized_pnl: float = 0.0        # 未实现盈亏
    realized_pnl: float = 0.0          # 已实现盈亏
    last_price: float = 0.0            # 最新价格
    # 期货/期权相关字段
    margin: float = 0.0                # 保证金
    long_quantity: float = 0.0         # 多头数量
    short_quantity: float = 0.0        # 空头数量
    today_long: float = 0.0            # 今日多头
    today_short: float = 0.0           # 今日空头
    contract_multiplier: float = 1.0   # 合约乘数
    # 更新时间
    update_time: datetime = field(default_factory=datetime.now)


@dataclass
class Account:
    """账户对象"""
    adapter_id: str
    cash: float = 0.0                  # 现金
    available_cash: float = 0.0        # 可用现金
    market_value: float = 0.0          # 持仓市值
    total_value: float = 0.0           # 总资产
    initial_cash: float = 0.0          # 初始资金
    # 保证金相关
    margin_occupied: float = 0.0       # 占用保证金
    margin_available: float = 0.0      # 可用保证金
    # 盈亏统计
    unrealized_pnl: float = 0.0        # 未实现盈亏
    realized_pnl: float = 0.0          # 已实现盈亏
    # 手续费统计
    total_commission: float = 0.0      # 总手续费
    total_tax: float = 0.0             # 总税费
    total_slippage: float = 0.0        # 总滑点
    # 更新时间
    update_time: datetime = field(default_factory=datetime.now)


class TradeCenter:
    """交易中心，管理所有交易相关的对象和操作"""
    
    def __init__(self, data_center: Any):
        """
        初始化交易中心
        
        Args:
            data_center: 数据中心实例
        """
        self.data_center = data_center
        self.logger = logging.getLogger(__name__)
        
        # 全局对象存储
        self.g = {}
        
        # 账户管理
        self._accounts: Dict[str, Account] = {}
        
        # 持仓管理
        self._positions: Dict[str, Dict[str, Position]] = defaultdict(dict)
        
        # 订单管理
        self._orders: Dict[str, List[Order]] = defaultdict(list)
        self._active_orders: Dict[str, List[Order]] = defaultdict(list)
        
        # 成交记录
        self._trades: Dict[str, List[Trade]] = defaultdict(list)
        
        # 规则引擎
        self.rule_engine = RuleEngine()
        
        # 当前时间
        self.current_time: Optional[datetime] = None
        
        # 市场状态
        self.market_open = False
        
        # 手续费配置
        self.commission_config = {
            'cn_stock': {
                'buy_commission_rate': 0.0003,
                'sell_commission_rate': 0.0003,
                'min_commission': 5,
                'stamp_tax_rate': 0.001,  # 印花税，仅卖出
            }
        }
        
        # 滑点配置
        self.slippage_config = {
            'cn_stock': {
                'type': 'percentage',  # percentage, fixed, tick
                'value': 0.001,        # 0.1%
            }
        }
        
    def create_account(self, adapter_id: str, initial_cash: float, **kwargs) -> Account:
        """创建账户"""
        account = Account(
            adapter_id=adapter_id,
            cash=initial_cash,
            available_cash=initial_cash,
            initial_cash=initial_cash,
            total_value=initial_cash,
            **kwargs
        )
        self._accounts[adapter_id] = account
        return account
        
    def get_account(self, adapter_id: str) -> Optional[Account]:
        """获取账户信息"""
        return self._accounts.get(adapter_id)
        
    def get_positions(self, adapter_id: str) -> Dict[str, Position]:
        """获取所有持仓"""
        return dict(self._positions.get(adapter_id, {}))
        
    def get_position(self, adapter_id: str, code: str) -> Optional[Position]:
        """获取单个持仓"""
        return self._positions.get(adapter_id, {}).get(code)
        
    def get_orders(self, adapter_id: str, active_only: bool = False) -> List[Order]:
        """获取订单列表"""
        if active_only:
            return list(self._active_orders.get(adapter_id, []))
        return list(self._orders.get(adapter_id, []))
        
    def get_trades(self, adapter_id: str) -> List[Trade]:
        """获取成交记录"""
        return list(self._trades.get(adapter_id, []))
        
    def place_order(self, adapter_id: str, code: str, quantity: float, 
                   side: Union[str, OrderSide], order_type: Union[str, OrderType] = OrderType.MARKET,
                   price: Optional[float] = None, stop_price: Optional[float] = None,
                   **kwargs) -> Optional[Order]:
        """
        下单
        
        Args:
            adapter_id: 适配器ID
            code: 股票代码
            quantity: 数量
            side: 买卖方向
            order_type: 订单类型
            price: 限价
            stop_price: 止损价
            **kwargs: 其他参数
            
        Returns:
            订单对象，如果下单失败返回None
        """
        # 参数转换
        if isinstance(side, str):
            side = OrderSide(side)
        if isinstance(order_type, str):
            order_type = OrderType(order_type)
            
        # 创建订单
        order = Order(
            order_id=str(uuid.uuid4()),
            adapter_id=adapter_id,
            code=code,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            stop_price=stop_price,
            create_time=self.current_time or datetime.now(),
            tags=kwargs
        )
        
        # 规则检查
        if not self.rule_engine.check_order(order, self):
            order.status = OrderStatus.REJECTED
            self._orders[adapter_id].append(order)
            self.logger.warning(f"Order rejected by rules: {order.order_id}")
            return None
            
        # 市价单立即执行
        if order_type == OrderType.MARKET and self.market_open:
            self._execute_order(order)
        else:
            # 加入待执行队列
            order.status = OrderStatus.SUBMITTED
            self._active_orders[adapter_id].append(order)
            
        self._orders[adapter_id].append(order)
        return order
        
    def cancel_order(self, adapter_id: str, order_id: str) -> bool:
        """撤销订单"""
        active_orders = self._active_orders.get(adapter_id, [])
        for i, order in enumerate(active_orders):
            if order.order_id == order_id:
                if order.status in [OrderStatus.PENDING, OrderStatus.SUBMITTED]:
                    order.status = OrderStatus.CANCELLED
                    order.update_time = self.current_time or datetime.now()
                    del active_orders[i]
                    return True
        return False
        
    def get_price(self, adapter_id: str, code: str, 
                 price_type: str = "current") -> Optional[float]:
        """
        获取价格
        
        Args:
            adapter_id: 适配器ID
            code: 股票代码
            price_type: 价格类型 (current, open, high, low, close)
        """
        try:
            # 获取最新的K线数据
            df = self.data_center.get_kline(
                adapter_id, code, 
                frequency="1d",
                end_time=self.current_time,
                count=1
            )
            
            if df.empty:
                return None
                
            if price_type == "current":
                # 交易时间返回最新价，否则返回收盘价
                if self.market_open:
                    return float(df.iloc[-1]['close'])
                else:
                    return float(df.iloc[-1]['close'])
            else:
                return float(df.iloc[-1][price_type])
                
        except Exception as e:
            self.logger.error(f"Error getting price for {code}: {str(e)}")
            return None
            
    def on_event(self, event: Any):
        """处理事件"""
        from .EventCenter import EventType
        
        self.current_time = event.time
        
        # 处理市场开关事件
        if event.event_type == EventType.MARKET_OPEN:
            self.market_open = True
            self._process_pending_orders()
        elif event.event_type == EventType.MARKET_CLOSE:
            self.market_open = False
        elif event.event_type == EventType.BAR:
            self._update_positions()
            self._process_active_orders()
        elif event.event_type == EventType.DIVIDEND:
            self._process_dividend(event.data)
        elif event.event_type == EventType.STOCK_SPLIT:
            self._process_stock_split(event.data)
            
    def _execute_order(self, order: Order):
        """执行订单"""
        # 获取当前价格
        current_price = self.get_price(order.adapter_id, order.code)
        if current_price is None:
            order.status = OrderStatus.REJECTED
            return
            
        # 计算成交价格（考虑滑点）
        execution_price = self._calculate_execution_price(
            order, current_price
        )
        
        # 计算手续费和税费
        commission, tax = self._calculate_fees(order, execution_price)
        
        # 检查资金/持仓是否足够
        if not self._check_execution_feasibility(order, execution_price, commission, tax):
            order.status = OrderStatus.REJECTED
            return
            
        # 更新订单状态
        order.status = OrderStatus.FILLED
        order.filled_quantity = order.quantity
        order.avg_filled_price = execution_price
        order.commission = commission
        order.tax = tax
        order.slippage = abs(execution_price - current_price) * order.quantity
        order.update_time = self.current_time or datetime.now()
        
        # 创建成交记录
        trade = Trade(
            trade_id=str(uuid.uuid4()),
            order_id=order.order_id,
            adapter_id=order.adapter_id,
            code=order.code,
            side=order.side,
            quantity=order.quantity,
            price=execution_price,
            commission=commission,
            tax=tax,
            slippage=order.slippage,
            trade_time=self.current_time or datetime.now(),
            tags=order.tags
        )
        self._trades[order.adapter_id].append(trade)
        
        # 更新账户和持仓
        self._update_account_position(order, trade)
        
        # 从活跃订单中移除
        if order in self._active_orders[order.adapter_id]:
            self._active_orders[order.adapter_id].remove(order)
            
    def _calculate_execution_price(self, order: Order, market_price: float) -> float:
        """计算成交价格（含滑点）"""
        slippage_config = self.slippage_config.get('cn_stock', {})
        
        if slippage_config['type'] == 'percentage':
            slippage_rate = slippage_config['value']
            if order.side in [OrderSide.BUY, OrderSide.BUY_TO_COVER]:
                return market_price * (1 + slippage_rate)
            else:
                return market_price * (1 - slippage_rate)
        elif slippage_config['type'] == 'fixed':
            slippage_amount = slippage_config['value']
            if order.side in [OrderSide.BUY, OrderSide.BUY_TO_COVER]:
                return market_price + slippage_amount
            else:
                return market_price - slippage_amount
        else:
            return market_price
            
    def _calculate_fees(self, order: Order, price: float) -> tuple:
        """计算手续费和税费"""
        commission_config = self.commission_config.get('cn_stock', {})
        value = price * order.quantity
        
        # 计算佣金
        if order.side in [OrderSide.BUY, OrderSide.BUY_TO_COVER]:
            commission = max(
                value * commission_config['buy_commission_rate'],
                commission_config['min_commission']
            )
            tax = 0  # 买入无印花税
        else:
            commission = max(
                value * commission_config['sell_commission_rate'],
                commission_config['min_commission']
            )
            tax = value * commission_config['stamp_tax_rate']  # 卖出印花税
            
        return commission, tax
        
    def _check_execution_feasibility(self, order: Order, price: float, 
                                   commission: float, tax: float) -> bool:
        """检查订单是否可执行"""
        account = self.get_account(order.adapter_id)
        if not account:
            return False
            
        total_cost = price * order.quantity + commission + tax
        
        if order.side in [OrderSide.BUY]:
            # 买入检查资金
            return account.available_cash >= total_cost
        else:
            # 卖出检查持仓
            position = self.get_position(order.adapter_id, order.code)
            if not position:
                return False
            return position.available_quantity >= order.quantity
            
    def _update_account_position(self, order: Order, trade: Trade):
        """更新账户和持仓"""
        account = self.get_account(order.adapter_id)
        if not account:
            return
            
        # 更新账户
        if order.side == OrderSide.BUY:
            # 买入
            total_cost = trade.price * trade.quantity + trade.commission + trade.tax
            account.cash -= total_cost
            account.available_cash -= total_cost
            account.total_commission += trade.commission
            account.total_tax += trade.tax
            account.total_slippage += trade.slippage
            
            # 更新或创建持仓
            position = self.get_position(order.adapter_id, order.code)
            if position:
                # 更新现有持仓
                total_quantity = position.quantity + trade.quantity
                position.avg_cost = (
                    (position.avg_cost * position.quantity + trade.price * trade.quantity) /
                    total_quantity
                )
                position.quantity = total_quantity
            else:
                # 创建新持仓
                position = Position(
                    adapter_id=order.adapter_id,
                    code=order.code,
                    quantity=trade.quantity,
                    available_quantity=0,  # T+1，当日买入不可卖
                    avg_cost=trade.price,
                    last_price=trade.price
                )
                self._positions[order.adapter_id][order.code] = position
                
        else:
            # 卖出
            total_value = trade.price * trade.quantity - trade.commission - trade.tax
            account.cash += total_value
            account.available_cash += total_value
            account.total_commission += trade.commission
            account.total_tax += trade.tax
            account.total_slippage += trade.slippage
            
            # 更新持仓
            position = self.get_position(order.adapter_id, order.code)
            if position:
                # 计算已实现盈亏
                realized_pnl = (trade.price - position.avg_cost) * trade.quantity - trade.commission - trade.tax
                position.realized_pnl += realized_pnl
                account.realized_pnl += realized_pnl
                
                position.quantity -= trade.quantity
                position.available_quantity -= trade.quantity
                
                # 如果持仓为0，删除持仓
                if position.quantity == 0:
                    del self._positions[order.adapter_id][order.code]
                    
        # 更新账户时间
        account.update_time = self.current_time or datetime.now()
        
    def _update_positions(self):
        """更新所有持仓的市值和盈亏"""
        for adapter_id, positions in self._positions.items():
            account = self.get_account(adapter_id)
            if not account:
                continue
                
            total_market_value = 0
            total_unrealized_pnl = 0
            
            for code, position in positions.items():
                # 获取最新价格
                current_price = self.get_price(adapter_id, code)
                if current_price:
                    position.last_price = current_price
                    position.market_value = current_price * position.quantity
                    position.unrealized_pnl = (current_price - position.avg_cost) * position.quantity
                    
                    total_market_value += position.market_value
                    total_unrealized_pnl += position.unrealized_pnl
                    
                # T+1更新可用数量
                if position.quantity > position.available_quantity:
                    position.available_quantity = position.quantity
                    
            # 更新账户
            account.market_value = total_market_value
            account.unrealized_pnl = total_unrealized_pnl
            account.total_value = account.cash + total_market_value
            
    def _process_pending_orders(self):
        """处理待执行订单"""
        for adapter_id, orders in self._active_orders.items():
            for order in orders[:]:  # 复制列表避免迭代时修改
                if order.status == OrderStatus.SUBMITTED:
                    if order.order_type == OrderType.MARKET:
                        self._execute_order(order)
                        
    def _process_active_orders(self):
        """处理活跃订单（限价单、止损单等）"""
        for adapter_id, orders in self._active_orders.items():
            for order in orders[:]:
                if order.status != OrderStatus.SUBMITTED:
                    continue
                    
                current_price = self.get_price(adapter_id, order.code)
                if not current_price:
                    continue
                    
                # 限价单
                if order.order_type == OrderType.LIMIT and order.price:
                    if order.side in [OrderSide.BUY, OrderSide.BUY_TO_COVER]:
                        if current_price <= order.price:
                            self._execute_order(order)
                    else:
                        if current_price >= order.price:
                            self._execute_order(order)
                            
                # 止损单
                elif order.order_type == OrderType.STOP and order.stop_price:
                    if order.side in [OrderSide.BUY, OrderSide.BUY_TO_COVER]:
                        if current_price >= order.stop_price:
                            self._execute_order(order)
                    else:
                        if current_price <= order.stop_price:
                            self._execute_order(order)
                            
    def _process_dividend(self, dividend_data: Dict[str, Any]):
        """处理分红事件"""
        code = dividend_data.get('code')
        cash_dividend = dividend_data.get('cash_dividend', 0)
        
        for adapter_id, positions in self._positions.items():
            if code in positions:
                position = positions[code]
                account = self.get_account(adapter_id)
                if account and cash_dividend > 0:
                    dividend_amount = position.quantity * cash_dividend
                    account.cash += dividend_amount
                    account.available_cash += dividend_amount
                    self.logger.info(f"Dividend received: {code} {dividend_amount}")
                    
    def _process_stock_split(self, split_data: Dict[str, Any]):
        """处理送股事件"""
        code = split_data.get('code')
        split_ratio = split_data.get('split_ratio', 0)
        
        for adapter_id, positions in self._positions.items():
            if code in positions:
                position = positions[code]
                if split_ratio > 0:
                    new_shares = position.quantity * split_ratio
                    position.quantity += new_shares
                    position.available_quantity += new_shares
                    # 调整成本价
                    position.avg_cost = position.avg_cost / (1 + split_ratio)
                    self.logger.info(f"Stock split: {code} ratio={split_ratio}")
