"""
交易中心实现
"""

import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum

from finhack.core.classes.dictobj import DictObj


class OrderStatus(Enum):
    """订单状态枚举"""
    PENDING = "pending"       # 待处理
    SUBMITTED = "submitted"   # 已提交
    FILLED = "filled"        # 已成交
    PARTIALLY_FILLED = "partially_filled"  # 部分成交
    CANCELLED = "cancelled"   # 已取消
    REJECTED = "rejected"     # 已拒绝


class OrderSide(Enum):
    """订单方向枚举"""
    BUY = "buy"
    SELL = "sell"


@dataclass
class Order:
    """订单对象"""
    order_id: str
    symbol: str
    side: OrderSide
    quantity: float
    price: Optional[float] = None  # None表示市价单
    order_type: str = "market"  # market, limit
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: float = 0.0
    avg_fill_price: float = 0.0
    commission: float = 0.0
    created_at: datetime = None
    updated_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()


@dataclass 
class Trade:
    """成交对象"""
    trade_id: str
    order_id: str
    symbol: str
    side: OrderSide
    quantity: float
    price: float
    commission: float
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class Position:
    """持仓对象"""
    symbol: str
    quantity: float = 0.0
    available_quantity: float = 0.0  # 可用数量（考虑T+1等规则）
    avg_cost: float = 0.0
    market_value: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    last_price: float = 0.0
    updated_at: datetime = None
    
    def __post_init__(self):
        if self.updated_at is None:
            self.updated_at = datetime.now()


class TradeCenter:
    """交易中心，负责账户、持仓、订单、成交管理"""
    
    def __init__(self, market: str = "cn_stock"):
        self.market = market
        
        # 交易对象存储
        self.g = DictObj()  # 全局变量，类似原系统
        self.orders: Dict[str, Order] = {}
        self.trades: Dict[str, Trade] = {}
        self.positions: Dict[str, Position] = {}
        
        # 账户信息（通过context获取）
        self._context = None
        
        # 交易规则
        self.trading_rules = {}
        self._load_trading_rules()
    
    def initialize(self, context):
        """
        初始化交易中心
        
        Args:
            context: 回测上下文
        """
        self._context = context
        
        # 将持仓信息同步到context
        context.portfolio.positions = self.positions
        
        # 初始化g变量
        self.g = context.g
    
    def _load_trading_rules(self):
        """加载交易规则"""
        # A股交易规则
        if self.market == "cn_stock":
            self.trading_rules = {
                "min_order_quantity": 100,  # 最小交易单位（股）
                "tick_size": 0.01,          # 最小价格变动单位
                "t1_rule": True,            # T+1交易规则
                "limit_up_down": True,      # 涨跌停限制
                "trading_hours": [
                    ("09:30", "11:30"),
                    ("13:00", "15:00")
                ]
            }
    
    def get_account(self, adapter_id: str = "default") -> Optional[Dict[str, Any]]:
        """
        获取账户信息
        
        Args:
            adapter_id: 适配器ID
            
        Returns:
            Dict: 账户信息
        """
        if not self._context:
            return None
        
        return {
            "account_id": self._context.account.account_id,
            "cash": self._context.account.cash,
            "available_cash": self._context.account.available_cash,
            "total_value": self._context.account.total_value,
            "locked_cash": self._context.account.locked_cash,
            "margin": self._context.account.margin
        }
    
    def get_positions(self, adapter_id: str = "default") -> Dict[str, Position]:
        """
        获取持仓信息
        
        Args:
            adapter_id: 适配器ID
            
        Returns:
            Dict: 持仓信息字典
        """
        return self.positions.copy()
    
    def get_position(self, symbol: str, adapter_id: str = "default") -> Optional[Position]:
        """
        获取单个持仓信息
        
        Args:
            symbol: 股票代码
            adapter_id: 适配器ID
            
        Returns:
            Optional[Position]: 持仓信息，如果不存在则返回None
        """
        return self.positions.get(symbol)
    
    def get_orders(self, adapter_id: str = "default", 
                  status: Optional[OrderStatus] = None) -> List[Order]:
        """
        获取订单信息
        
        Args:
            adapter_id: 适配器ID
            status: 订单状态过滤
            
        Returns:
            List[Order]: 订单列表
        """
        orders = list(self.orders.values())
        
        if status:
            orders = [order for order in orders if order.status == status]
        
        return orders
    
    def get_trades(self, adapter_id: str = "default") -> List[Trade]:
        """
        获取成交信息
        
        Args:
            adapter_id: 适配器ID
            
        Returns:
            List[Trade]: 成交列表
        """
        return list(self.trades.values())
    
    def place_order(self, adapter_id: str = "default", symbol: str = "", 
                   side: str = "buy", quantity: float = 0, 
                   price: Optional[float] = None, 
                   order_type: str = "market") -> Optional[str]:
        """
        下单
        
        Args:
            adapter_id: 适配器ID
            symbol: 股票代码
            side: 买卖方向 (buy/sell)
            quantity: 数量
            price: 价格（None表示市价单）
            order_type: 订单类型 (market/limit)
            
        Returns:
            str: 订单ID，失败返回None
        """
        try:
            # 参数验证
            if not symbol or quantity <= 0:
                if self._context:
                    self._context.logger.error(f"下单参数错误: symbol={symbol}, quantity={quantity}")
                return None
            
            # 转换订单方向
            order_side = OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL
            
            # 生成订单ID
            order_id = str(uuid.uuid4())
            
            # 创建订单
            order = Order(
                order_id=order_id,
                symbol=symbol,
                side=order_side,
                quantity=quantity,
                price=price,
                order_type=order_type
            )
            
            # 验证订单
            if not self._validate_order(order):
                return None
            
            # 提交订单
            self.orders[order_id] = order
            order.status = OrderStatus.SUBMITTED
            
            # 记录订单日志
            if self._context:
                self._context.log_order({
                    "order_id": order_id,
                    "symbol": symbol,
                    "side": side,
                    "quantity": quantity,
                    "price": price,
                    "order_type": order_type
                })
            
            # 尝试立即成交（简化的撮合逻辑）
            self._try_fill_order(order)
            
            return order_id
            
        except Exception as e:
            if self._context:
                self._context.logger.error(f"下单失败: {str(e)}")
            return None
    
    def cancel_order(self, adapter_id: str = "default", order_id: str = "") -> bool:
        """
        撤单
        
        Args:
            adapter_id: 适配器ID
            order_id: 订单ID
            
        Returns:
            bool: 是否成功
        """
        try:
            if order_id not in self.orders:
                return False
            
            order = self.orders[order_id]
            
            # 只有未成交或部分成交的订单可以撤销
            if order.status in [OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED]:
                order.status = OrderStatus.CANCELLED
                order.updated_at = datetime.now()
                
                if self._context:
                    self._context.logger.info(f"订单撤销成功: {order_id}")
                
                return True
            
            return False
            
        except Exception as e:
            if self._context:
                self._context.logger.error(f"撤单失败: {str(e)}")
            return False
    
    def get_price(self, adapter_id: str = "default", symbol: str = "") -> Optional[float]:
        """
        获取价格
        
        Args:
            adapter_id: 适配器ID
            symbol: 股票代码
            
        Returns:
            float: 价格
        """
        if not self._context or not self._context.data_center:
            return None
        
        # DataCenter.get_price只接受symbol和date参数
        date_str = self._context.current_dt.strftime('%Y-%m-%d') if self._context.current_dt else None
        return self._context.data_center.get_price(symbol, date_str)
    
    def _validate_order(self, order: Order) -> bool:
        """验证订单"""
        try:
            # 检查最小交易单位
            min_quantity = self.trading_rules.get("min_order_quantity", 1)
            if order.quantity < min_quantity:
                if self._context:
                    self._context.logger.error(f"订单数量低于最小交易单位: {order.quantity} < {min_quantity}")
                return False
            
            # 检查资金是否足够（买入时）
            if order.side == OrderSide.BUY:
                estimated_cost = self._estimate_order_cost(order)
                if estimated_cost > self._context.account.available_cash:
                    if self._context:
                        self._context.logger.error(f"资金不足: 需要{estimated_cost}, 可用{self._context.account.available_cash}")
                    return False
            
            # 检查持仓是否足够（卖出时）
            elif order.side == OrderSide.SELL:
                position = self.positions.get(order.symbol)
                if not position or position.available_quantity < order.quantity:
                    available = position.available_quantity if position else 0
                    if self._context:
                        self._context.logger.error(f"持仓不足: 需要{order.quantity}, 可用{available}")
                    return False
            
            return True
            
        except Exception as e:
            if self._context:
                self._context.logger.error(f"订单验证失败: {str(e)}")
            return False
    
    def _estimate_order_cost(self, order: Order) -> float:
        """估算订单成本"""
        # 获取当前价格
        current_price = self.get_price(symbol=order.symbol)
        if not current_price:
            return 0.0
        
        # 使用当前价格估算（市价单）或使用限价（限价单）
        price = current_price if order.order_type == "market" else (order.price or current_price)
        
        # 计算成本（包含手续费）
        trade_value = price * order.quantity
        commission = self._calculate_commission(trade_value, order.side)
        
        return trade_value + commission
    
    def _calculate_commission(self, trade_value: float, side: OrderSide) -> float:
        """计算手续费"""
        if not self._context:
            return 0.0
        
        # 获取费率配置
        if side == OrderSide.BUY:
            commission_rate = self._context.account.open_commission
            tax_rate = self._context.account.open_tax
        else:
            commission_rate = self._context.account.close_commission
            tax_rate = self._context.account.close_tax
        
        # 计算手续费
        commission = trade_value * commission_rate
        tax = trade_value * tax_rate
        
        # 最小手续费
        min_commission = self._context.account.min_commission
        if commission < min_commission:
            commission = min_commission
        
        return commission + tax
    
    def _try_fill_order(self, order: Order):
        """尝试成交订单（简化逻辑）"""
        try:
            # 获取当前价格
            current_price = self.get_price(symbol=order.symbol)
            if not current_price:
                return
            
            # 简化的成交逻辑：市价单立即成交，限价单检查价格
            can_fill = False
            fill_price = current_price
            
            if order.order_type == "market":
                can_fill = True
            elif order.order_type == "limit" and order.price:
                if order.side == OrderSide.BUY and current_price <= order.price:
                    can_fill = True
                    fill_price = min(order.price, current_price)
                elif order.side == OrderSide.SELL and current_price >= order.price:
                    can_fill = True
                    fill_price = max(order.price, current_price)
            
            if can_fill:
                self._fill_order(order, order.quantity, fill_price)
        
        except Exception as e:
            if self._context:
                self._context.logger.error(f"订单成交处理失败: {str(e)}")
    
    def _fill_order(self, order: Order, fill_quantity: float, fill_price: float):
        """成交订单"""
        try:
            # 计算手续费
            trade_value = fill_quantity * fill_price
            commission = self._calculate_commission(trade_value, order.side)
            
            # 生成成交记录
            trade_id = str(uuid.uuid4())
            trade = Trade(
                trade_id=trade_id,
                order_id=order.order_id,
                symbol=order.symbol,
                side=order.side,
                quantity=fill_quantity,
                price=fill_price,
                commission=commission
            )
            
            self.trades[trade_id] = trade
            
            # 更新订单状态
            order.filled_quantity += fill_quantity
            order.avg_fill_price = ((order.avg_fill_price * (order.filled_quantity - fill_quantity) + 
                                   fill_price * fill_quantity) / order.filled_quantity)
            order.commission += commission
            order.updated_at = datetime.now()
            
            if order.filled_quantity >= order.quantity:
                order.status = OrderStatus.FILLED
            else:
                order.status = OrderStatus.PARTIALLY_FILLED
            
            # 更新持仓
            self._update_position(order.symbol, order.side, fill_quantity, fill_price, commission)
            
            # 更新账户资金
            self._update_account(order.side, trade_value, commission)
            
            # 记录成交日志
            if self._context:
                self._context.log_trade({
                    "trade_id": trade_id,
                    "order_id": order.order_id,
                    "symbol": order.symbol,
                    "side": order.side.value,
                    "quantity": fill_quantity,
                    "price": fill_price,
                    "commission": commission
                })
                
                self._context.logger.info(
                    f"订单成交: {order.symbol} {order.side.value} {fill_quantity}@{fill_price}, "
                    f"手续费: {commission}"
                )
        
        except Exception as e:
            if self._context:
                self._context.logger.error(f"订单成交处理失败: {str(e)}")
    
    def _update_position(self, symbol: str, side: OrderSide, quantity: float, 
                        price: float, commission: float):
        """更新持仓"""
        if symbol not in self.positions:
            self.positions[symbol] = Position(symbol=symbol)
        
        position = self.positions[symbol]
        
        if side == OrderSide.BUY:
            # 买入：增加持仓
            total_cost = position.quantity * position.avg_cost + quantity * price + commission
            position.quantity += quantity
            position.avg_cost = total_cost / position.quantity if position.quantity > 0 else 0
            
            # T+1规则：当日买入的股票不能卖出
            if self.trading_rules.get("t1_rule", False):
                # 这里需要更复杂的逻辑来跟踪可用数量
                pass
            else:
                position.available_quantity = position.quantity
        
        else:
            # 卖出：减少持仓
            position.quantity -= quantity
            position.available_quantity = max(0, position.available_quantity - quantity)
            
            # 计算已实现盈亏
            realized_pnl = (price - position.avg_cost) * quantity - commission
            position.realized_pnl += realized_pnl
        
        # 更新市值和未实现盈亏
        current_price = self.get_price(symbol=symbol)
        if current_price:
            position.last_price = current_price
            position.market_value = position.quantity * current_price
            position.unrealized_pnl = (current_price - position.avg_cost) * position.quantity
        
        position.updated_at = datetime.now()
        
        # 如果持仓为0，移除持仓记录
        if position.quantity <= 0:
            if symbol in self.positions:
                del self.positions[symbol]
    
    def _update_account(self, side: OrderSide, trade_value: float, commission: float):
        """更新账户资金"""
        if not self._context:
            return
        
        if side == OrderSide.BUY:
            # 买入：减少现金
            self._context.account.cash -= (trade_value + commission)
            self._context.account.available_cash -= (trade_value + commission)
            self._context.portfolio.cash = self._context.account.cash
        else:
            # 卖出：增加现金
            self._context.account.cash += (trade_value - commission)
            self._context.account.available_cash += (trade_value - commission)
            self._context.portfolio.cash = self._context.account.cash
        
        # 更新总资产
        self._context.update_portfolio_value()
    
    # 事件处理器方法
    def handle_morning_start(self, context, event):
        """处理上午开盘事件"""
        context.logger.info("交易中心: 上午开盘")
        
    def handle_afternoon_start(self, context, event):
        """处理下午开盘事件"""
        context.logger.info("交易中心: 下午开盘")
        
    def handle_after_market(self, context, event):
        """处理盘后事件 - 执行清算"""
        context.logger.info("交易中心: 盘后清算")
        
        # 更新所有持仓的市值
        for symbol, position in self.positions.items():
            current_price = self.get_price(symbol=symbol)
            if current_price:
                position.last_price = current_price
                position.market_value = position.quantity * current_price
                position.unrealized_pnl = (current_price - position.avg_cost) * position.quantity
        
        # 更新账户总价值
        context.update_portfolio_value()
        context.calculate_returns() 