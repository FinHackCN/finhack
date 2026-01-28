"""
交易中心实现
"""

import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from finhack.core.classes.dictobj import DictObj
from finhack.trader.backtest.models.enums import (
    Side as OrderSide,
    OrderStatus,
    OrderType,
    AssetTypeEnum,
    ExchangeEnum,
)
from finhack.trader.backtest.models.instrument import Instrument
from finhack.trader.backtest.events.event_types import EventTypeEnum


@dataclass
class Order:
    """订单对象"""
    order_id: str
    symbol: str
    side: OrderSide
    quantity: float
    price: Optional[float] = None  # None表示市价单
    order_type: OrderType = OrderType.MARKET  # MARKET, LIMIT
    status: OrderStatus = OrderStatus.PENDING_NEW
    filled_quantity: float = 0.0
    avg_fill_price: float = 0.0
    commission: float = 0.0
    created_at: datetime = None
    updated_at: datetime = None
    # 延迟撮合配置：订单创建后需要等待多少分钟才能撮合
    match_delay_minutes: int = 1

    def __post_init__(self):
        # 不再自动设置时间，由调用方在创建时传入回测模拟时间
        pass

    def age_minutes(self, current_time: datetime) -> float:
        """计算订单从创建到当前时间的分钟数"""
        if self.created_at is None:
            return 999  # 如果没有创建时间，认为订单很老
        return (current_time - self.created_at).total_seconds() / 60

    def can_match(self, current_time: datetime) -> bool:
        """判断订单是否可以撮合（已超过等待时间）"""
        return self.age_minutes(current_time) >= self.match_delay_minutes


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
    
    # 新增字段以兼容旧接口
    frozen_quantity: float = 0.0      # 冻结数量
    total_cost: float = 0.0           # 总成本
    total_value: float = 0.0          # 总价值
    amount: float = 0.0               # 兼容字段：总数量
    enable_amount: float = 0.0        # 兼容字段：可用数量
    last_sale_price: float = 0.0      # 兼容字段：最新价格
    cost_basis: float = 0.0           # 兼容字段：成本基础
    
    def __post_init__(self):
        if self.updated_at is None:
            self.updated_at = datetime.now()
        
        # 设置兼容字段
        self.amount = self.quantity
        self.enable_amount = self.available_quantity
        self.last_sale_price = self.last_price
        self.cost_basis = self.avg_cost
        self.total_value = self.market_value


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

        # 事件中心（由BacktestEngine注入）
        self.event_center = None

        # 交易状态
        self.is_trading = False

        # 交易规则
        self.trading_rules = {}
        self._load_trading_rules()

        # 性能优化：价格缓存
        self._price_cache = {}  # {symbol: price} 当前bar的价格缓存
        self._current_bar_time = None  # 当前bar时间戳
    
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

    def set_event_center(self, event_center):
        """设置事件中心

        Args:
            event_center: 事件中心实例
        """
        self.event_center = event_center

    def _load_trading_rules(self):
        """加载交易规则"""
        # 根据市场类型加载不同的交易规则
        market = self.market.lower()
        
        if market in ["cn_stock", "cn_fund"]:
            # A股/基金交易规则
            self.trading_rules = {
                "min_order_quantity": 100,  # 最小交易单位（股）
                "tick_size": 0.01,          # 最小价格变动单位
                "t1_rule": True,            # T+1交易规则
                "limit_up_down": True,      # 涨跌停限制
                "price_limit_ratio": 0.10,  # 涨跌停比例（10%）
                "st_price_limit_ratio": 0.05, # ST股涨跌停比例（5%）
                "star_price_limit_ratio": 0.20, # 科创板/创业板涨跌停比例（20%）
                "trading_hours": [
                    ("09:30", "11:30"),
                    ("13:00", "15:00")
                ],
                "commission_rate": {
                    "open": 0.0003,  # 买入佣金率
                    "close": 0.0003, # 卖出佣金率
                    "min_commission": 5.0  # 最低佣金
                },
                "tax_rate": {
                    "open": 0.0,    # 买入税率
                    "close": 0.001  # 卖出税率（印花税）
                }
            }
        elif market == "cn_future":
            # 期货交易规则
            self.trading_rules = {
                "min_order_quantity": 1,    # 最小交易单位（手）
                "tick_size": 0.01,           # 最小价格变动单位
                "t0_rule": True,             # T+0交易规则
                "margin_enabled": True,       # 保证金制度
                "margin_ratio": 0.10,        # 保证金比例
                "trading_hours": [
                    ("09:00", "10:15"),
                    ("10:30", "11:30"),
                    ("13:30", "15:00"),
                    ("21:00", "02:30")  # 夜盘
                ],
                "commission_rate": {
                    "open": 0.0001,      # 买入佣金率
                    "close": 0.0001,     # 卖出佣金率
                    "close_today": 0.0001, # 平今佣金率
                    "min_commission": 5.0   # 最低佣金
                },
                "tax_rate": {
                    "open": 0.0,        # 买入税率
                    "close": 0.0        # 卖出税率
                }
            }
        elif market == "global_cryptospot":
            # 加密货币现货交易规则
            self.trading_rules = {
                "min_order_quantity": 0.00000001,  # 最小交易单位
                "tick_size": 0.00000001,           # 最小价格变动单位
                "t0_rule": True,                  # T+0交易规则
                "trading_24_7": True,             # 24小时交易
                "trading_hours": [
                    ("00:00", "23:59:59")
                ],
                "commission_rate": {
                    "open": 0.001,       # 买入佣金率
                    "close": 0.001,      # 卖出佣金率
                    "min_commission": 0.0  # 无最低佣金
                },
                "tax_rate": {
                    "open": 0.0,         # 买入税率
                    "close": 0.0         # 卖出税率
                }
            }
        else:
            # 默认交易规则
            self.trading_rules = {
                "min_order_quantity": 1,
                "tick_size": 0.01,
                "t1_rule": False,
                "limit_up_down": False,
                "trading_hours": [
                    ("09:30", "15:00")
                ],
                "commission_rate": {
                    "open": 0.0003,
                    "close": 0.0003,
                    "min_commission": 5.0
                },
                "tax_rate": {
                    "open": 0.0,
                    "close": 0.0
                }
            }
    
    def get_account(self, adapter_id: str = "default") -> Optional[Dict[str, Any]]:
        """
        获取账户信息 - 兼容UniTrader接口
        
        Args:
            adapter_id: 适配器ID
            
        Returns:
            Dict: 账户信息，兼容UniTrader.Account格式
        """
        if not self._context:
            return None
        
        # 计算总持仓市值
        total_market_value = sum(pos.market_value for pos in self.positions.values())
        
        # 更新账户总价值
        total_value = self._context.account.cash + total_market_value
        
        return {
            # UniTrader标准字段
            "account_id": getattr(self._context.account, 'account_id', 'backtest_account'),
            "platform": "BACKTEST",
            "account_type": "CASH",
            "currency": "CNY",
            "total_assets": total_value,
            "cash_available": self._context.account.cash,
            "cash_frozen": getattr(self._context.account, 'locked_cash', 0),
            "market_value": total_market_value,
            "pnl_unrealized": sum(pos.unrealized_pnl for pos in self.positions.values()),
            "pnl_realized": getattr(self._context.account, 'realized_pnl', 0),
            "status": "DATA_READY",
            
            # 兼容字段
            "cash": self._context.account.cash,
            "available_cash": self._context.account.cash,
            "total_value": total_value,
            "locked_cash": getattr(self._context.account, 'locked_cash', 0),
            "margin": getattr(self._context.account, 'margin', 0)
        }

    # ========== Instrument 管理 ==========

    def get_instrument(self, symbol: str) -> Optional[Instrument]:
        """获取合约信息（动态构造）

        Args:
            symbol: 合约代码，如 000001.SZ

        Returns:
            Optional[Instrument]: 合约信息对象
        """
        try:
            # 解析 symbol
            code, exchange_str = self._parse_symbol(symbol)

            # 获取交易规则
            rules = self.trading_rules

            # 动态构造 Instrument
            return Instrument(
                symbol=symbol,
                exchange=self._get_exchange_enum(exchange_str),
                asset_type=self._get_asset_type(self.market),
                name="",  # 可选：后续可从数据源获取
                currency=self._get_currency(self.market),
                contract_multiplier=self._get_contract_multiplier(self.market),
                tick_size=rules.get('tick_size', 0.01),
                lot_size=rules.get('min_order_quantity', 100),
                min_order_volume=rules.get('min_order_quantity', 100),
                max_order_volume=1e9,
                volume_step=self._get_volume_step(self.market),
                margin_ratio=rules.get('margin_ratio', 0.0),
            )
        except Exception as e:
            if self._context:
                self._context.logger.warning(f"构造 Instrument 失败: {symbol}, {e}")
            return None

    def _parse_symbol(self, symbol: str) -> tuple:
        """解析合约代码

        Args:
            symbol: 合约代码，如 000001.SZ

        Returns:
            tuple: (code, exchange) 如 (000001, SZ)
        """
        if '.' in symbol:
            code, exchange = symbol.split('.', 1)
            return code, exchange
        return symbol, None

    def _get_exchange_enum(self, exchange_str: Optional[str]) -> ExchangeEnum:
        """将交易所字符串转换为枚举

        Args:
            exchange_str: 交易所字符串，如 SH, SZ, SHFE, CFFEX 等

        Returns:
            ExchangeEnum: 交易所枚举
        """
        if not exchange_str:
            return ExchangeEnum.SSE  # 默认上交所

        exchange_map = {
            'SH': ExchangeEnum.SSE,
            'SZ': ExchangeEnum.SZSE,
            'SSE': ExchangeEnum.SSE,
            'SZSE': ExchangeEnum.SZSE,
            'SHFE': ExchangeEnum.SHFE,
            'CFFEX': ExchangeEnum.CFFEX,
            'DCE': ExchangeEnum.DCE,
            'CZCE': ExchangeEnum.CZCE,
            'INE': ExchangeEnum.INE,
            'HKEX': ExchangeEnum.HKEX,
            'NYSE': ExchangeEnum.NYSE,
            'NASDAQ': ExchangeEnum.NASDAQ,
            'CME': ExchangeEnum.CME,
            'ICE': ExchangeEnum.ICE,
            'LME': ExchangeEnum.LME,
        }

        return exchange_map.get(exchange_str.upper(), ExchangeEnum.OTHER)

    def _get_asset_type(self, market: str) -> AssetTypeEnum:
        """根据市场类型获取资产类型

        Args:
            market: 市场类型

        Returns:
            AssetTypeEnum: 资产类型枚举
        """
        market = market.lower()
        asset_type_map = {
            'cn_stock': AssetTypeEnum.STOCK,
            'cn_fund': AssetTypeEnum.FUND,
            'cn_future': AssetTypeEnum.FUTURE,
            'cn_index': AssetTypeEnum.OTHER,  # 指数映射到 OTHER
            'global_cryptospot': AssetTypeEnum.CRYPTO,
            'global_forex': AssetTypeEnum.FX,
        }
        return asset_type_map.get(market, AssetTypeEnum.STOCK)

    def _get_currency(self, market: str) -> str:
        """根据市场类型获取币种

        Args:
            market: 市场类型

        Returns:
            str: 币种代码
        """
        market = market.lower()
        if market.startswith('cn_'):
            return 'CNY'
        elif market == 'global_cryptospot' or market == 'global_forex':
            return 'USD'
        return 'CNY'

    def _get_contract_multiplier(self, market: str) -> float:
        """根据市场类型获取合约乘数

        Args:
            market: 市场类型

        Returns:
            float: 合约乘数
        """
        market = market.lower()
        if market == 'cn_future':
            return 1.0  # 期货合约乘数根据品种不同，这里简化为1
        return 1.0

    def _get_volume_step(self, market: str) -> float:
        """根据市场类型获取数量步长

        Args:
            market: 市场类型

        Returns:
            float: 数量步长
        """
        market = market.lower()
        if market in ['cn_stock', 'cn_fund']:
            return 100.0  # A股100股为一手
        elif market == 'cn_future':
            return 1.0  # 期货1手为单位
        return 1.0

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
                   side: str = "buy", volume: float = 0,
                   price: Optional[float] = None,
                   order_type: OrderType = OrderType.MARKET, **kwargs) -> Optional[str]:
        """
        下单 - 兼容UniTrader接口

        Args:
            adapter_id: 适配器ID
            symbol: 股票代码
            side: 买卖方向 (buy/sell)
            volume: 数量
            price: 价格（None表示市价单）
            order_type: 订单类型 (market/limit)
            **kwargs: 其他UniTrader兼容参数

        Returns:
            str: 订单ID，失败返回None
        """
        try:
            # 参数验证
            if not symbol or volume <= 0:
                if self._context:
                    self._context.logger.error(f"下单参数错误: symbol={symbol}, volume={volume}")
                return None

            # 转换订单方向
            order_side = OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL

            # 生成订单ID
            order_id = str(uuid.uuid4())

            # 获取当前模拟时间
            current_time = None
            if self._context and hasattr(self._context, 'current_dt'):
                current_time = self._context.current_dt

            # 对于市价单，立即获取并固定当前价格
            market_price = None
            if order_type == OrderType.MARKET:
                current_price = self._get_price_from_datacenter(symbol)
                if current_price and current_price > 0:
                    market_price = current_price
                    if self._context:
                        self._context.logger.debug(f"市价单固定价格: {symbol} = {market_price:.2f}")

            # 创建订单 - 使用模拟时间
            order = Order(
                order_id=order_id,
                symbol=symbol,
                side=order_side,
                quantity=volume,  # Order类使用quantity字段
                price=price,
                order_type=order_type,
                created_at=current_time,  # 使用模拟时间
                updated_at=current_time,  # 使用模拟时间
                market_price=market_price  # 市价单的固定成交价
            )

            # 验证订单
            if not self._validate_order(order):
                if self._context:
                    self._context.logger.error(f"订单验证失败: {symbol} {side} {volume}")
                return None

            # 提交订单
            self.orders[order_id] = order
            order.status = OrderStatus.NEW

            # 发布订单提交事件
            if self.event_center:
                self.event_center.publish_order_event(
                    event_type=EventTypeEnum.ORDER_SUBMISSION,
                    order_data={
                        "order_id": order_id,
                        "symbol": symbol,
                        "side": side,
                        "volume": volume,
                        "price": price
                    }
                )

            # 记录订单日志
            if self._context:
                order_log = {
                    "order_id": order_id,
                    "symbol": symbol,
                    "side": side,
                    "volume": volume,
                    "price": price,
                    "order_type": order_type,
                    "status": "submitted",
                    "created_time": current_time.isoformat() if current_time else datetime.now().isoformat()
                }
                self._context.logger.info(f"订单提交成功: {order_log}")

            return order_id

        except Exception as e:
            if self._context:
                self._context.logger.error(f"下单失败: {e}")
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
            if order.status in [OrderStatus.PENDING_NEW, OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
                order.status = OrderStatus.CANCELLED
                order.updated_at = datetime.now()

                # 发布订单撤销事件
                if self.event_center:
                    self.event_center.publish_order_event(
                        event_type=EventTypeEnum.ORDER_CANCELLATION,
                        order_data={
                            "order_id": order_id,
                            "symbol": order.symbol
                        }
                    )

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
        # 直接调用_get_price_from_datacenter，使用统一的逻辑
        return self._get_price_from_datacenter(symbol)
    
    def _validate_order(self, order: Order) -> bool:
        """验证订单"""
        try:
            # 检查交易时间
            if not self._is_trading_time():
                if self._context:
                    self._context.logger.error("当前不在交易时间内")
                return False
            
            # 检查最小交易单位
            min_quantity = self.trading_rules.get("min_order_quantity", 1)
            if order.quantity < min_quantity:
                if self._context:
                    self._context.logger.error(f"订单数量低于最小交易单位: {order.quantity} < {min_quantity}")
                return False
            
            # 检查交易数量是否为最小单位的整数倍
            if order.quantity % min_quantity != 0:
                if self._context:
                    self._context.logger.error(f"订单数量必须是{min_quantity}的整数倍: {order.quantity}")
                return False
            
            # 检查股票代码格式
            if not self._validate_symbol(order.symbol):
                if self._context:
                    self._context.logger.error(f"无效的股票代码: {order.symbol}")
                return False
            
            # 检查限价单价格
            if order.order_type == OrderType.LIMIT and (not order.price or order.price <= 0):
                if self._context:
                    self._context.logger.error("限价单必须指定有效价格")
                return False
            
            # 检查资金是否足够（买入时）
            if order.side == OrderSide.BUY:
                estimated_cost = self._estimate_order_cost(order)
                available_cash = self._context.account.available_cash if self._context else 0
                if estimated_cost > available_cash:
                    if self._context:
                        self._context.logger.error(f"资金不足: 需要{estimated_cost:.2f}, 可用{available_cash:.2f}")
                    return False
            
            # 检查持仓是否足够（卖出时）
            elif order.side == OrderSide.SELL:
                position = self.positions.get(order.symbol)
                available_quantity = position.available_quantity if position else 0
                if available_quantity < order.quantity:
                    if self._context:
                        self._context.logger.error(f"持仓不足: 需要{order.quantity}, 可用{available_quantity}")
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
        price = current_price if order.order_type == OrderType.MARKET else (order.price or current_price)
        
        # 计算成本（包含手续费）
        trade_value = price * order.quantity
        commission = self._calculate_commission(trade_value, order.side)
        
        return trade_value + commission
    
    def _calculate_commission(self, trade_value: float, side: OrderSide) -> float:
        """计算手续费"""
        if not self._context:
            return 0.0
        
        # 获取费率配置
        commission_rate = 0.0
        tax_rate = 0.0
        min_commission = 0.0
        
        if side == OrderSide.BUY:
            commission_rate = self.trading_rules["commission_rate"]["open"]
            tax_rate = self.trading_rules["tax_rate"]["open"]
        else:
            commission_rate = self.trading_rules["commission_rate"]["close"]
            tax_rate = self.trading_rules["tax_rate"]["close"]
        
        # 计算手续费
        commission = trade_value * commission_rate
        tax = trade_value * tax_rate
        
        # 最低手续费
        min_commission = self.trading_rules["commission_rate"].get("min_commission", 0.0)
        if commission < min_commission:
            commission = min_commission
        
        return commission + tax
    
    def _try_fill_order(self, order: Order):
        """尝试成交订单（增强的撮合逻辑）"""
        try:
            if order.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]:
                return  # 已处理的订单不再尝试成交

            # 获取成交价格：
            # - 市价单：使用下单时固定的价格（order.market_price）
            # - 限价单：使用订单价格或当前价格
            if order.order_type == OrderType.MARKET:
                # 市价单：使用下单时固定的价格
                current_price = order.market_price
                if not current_price or current_price <= 0:
                    # 如果下单时没有获取到价格，尝试现在获取（向后兼容）
                    current_price = self._get_price_from_datacenter(order.symbol)
                    if current_price and current_price > 0:
                        # 更新订单的固定价格，避免重复获取
                        order.market_price = current_price
                    else:
                        if self._context:
                            self._context.logger.warning(f"无法获取 {order.symbol} 的价格({current_price})，订单暂时无法成交")
                        # 将订单标记为待处理，等待下次尝试
                        if order.status != OrderStatus.NEW:
                            order.status = OrderStatus.NEW
                        return
            else:
                # 限价单：从DataCenter获取当前价格
                current_price = self._get_price_from_datacenter(order.symbol)
                if not current_price or current_price <= 0:
                    if self._context:
                        self._context.logger.warning(f"无法获取 {order.symbol} 的价格({current_price})，订单暂时无法成交")
                    # 将订单标记为待处理，等待下次尝试
                    if order.status != OrderStatus.NEW:
                        order.status = OrderStatus.NEW
                    return

            # 检查涨跌停限制（仅限价单需要检查）
            if self.trading_rules.get("limit_up_down", False) and order.order_type == OrderType.LIMIT:
                if not self._check_price_limit(order.symbol, current_price, order.price):
                    order.status = OrderStatus.REJECTED
                    if self._context:
                        self._context.logger.warning(f"订单价格超出涨跌停限制: {order.symbol}")
                    return

            # 增强的撮合逻辑
            can_fill = False
            fill_price = current_price
            fill_quantity = order.quantity - order.filled_quantity

            if order.order_type == OrderType.MARKET:
                # 市价单：立即成交，但增加滑点（基于固定价格）
                can_fill = True
                slippage = self._calculate_slippage(order)
                if order.side == OrderSide.BUY:
                    fill_price = current_price * (1 + slippage)
                else:
                    fill_price = current_price * (1 - slippage)
                    
            elif order.order_type == OrderType.LIMIT and order.price:
                # 限价单：价格合适时成交
                if order.side == OrderSide.BUY and current_price <= order.price:
                    can_fill = True
                    fill_price = order.price  # 以限价成交
                elif order.side == OrderSide.SELL and current_price >= order.price:
                    can_fill = True
                    fill_price = order.price  # 以限价成交
            
            # 模拟部分成交（大额订单）
            if can_fill and fill_quantity > 0:
                # 对于大额订单，模拟分批成交
                max_single_fill = self._get_max_single_fill(order, current_price)
                actual_fill_quantity = min(fill_quantity, max_single_fill)
                
                if actual_fill_quantity > 0:
                    if self._context:
                        self._context.logger.info(f"订单成交: {order.symbol} {order.side.value} {actual_fill_quantity}@{fill_price:.2f}")
                    self._fill_order(order, actual_fill_quantity, fill_price)
                    
                    # 如果还有剩余未成交，设置为部分成交状态，将在下次事件中继续尝试
                    if order.filled_quantity < order.quantity:
                        order.status = OrderStatus.PARTIALLY_FILLED
                        if self._context:
                            remaining = order.quantity - order.filled_quantity
                            self._context.logger.info(f"订单部分成交: {order.symbol} 已成交{order.filled_quantity}/{order.quantity}, 剩余{remaining}")
                    else:
                        order.status = OrderStatus.FILLED
                        if self._context:
                            self._context.logger.info(f"订单完全成交: {order.symbol} {order.side.value} {order.quantity}")
                else:
                    if self._context:
                        self._context.logger.debug(f"订单暂未成交: {order.symbol} {order.side.value} - 成交量为0")
            else:
                if self._context:
                    reason = "价格不合适" if not can_fill else "无剩余数量"
                    self._context.logger.debug(f"订单暂未成交: {order.symbol} {order.side.value} - {reason}")
                    
                # 确保订单状态正确，以便下次继续尝试
                if order.status != OrderStatus.NEW and order.status != OrderStatus.PARTIALLY_FILLED:
                    order.status = OrderStatus.NEW
        
        except Exception as e:
            if self._context:
                self._context.logger.error(f"订单撮合处理失败: {order.symbol} - {str(e)}")
    
    def _check_price_limit(self, symbol: str, current_price: float, order_price: Optional[float]) -> bool:
        """检查价格是否在涨跌停限制内"""
        if not order_price:
            return True
        
        # 如果没有启用涨跌停限制，直接返回True
        if not self.trading_rules.get("limit_up_down", False):
            return True
        
        # 获取涨跌停比例
        limit_ratio = self.trading_rules.get("price_limit_ratio", 0.10)
        
        # 根据股票代码判断是否为特殊股票
        if symbol.startswith('688') or symbol.startswith('300'):  # 科创板或创业板
            limit_ratio = self.trading_rules.get("star_price_limit_ratio", 0.20)
        elif 'ST' in symbol or 'st' in symbol:  # ST股票
            limit_ratio = self.trading_rules.get("st_price_limit_ratio", 0.05)
        
        # 计算涨跌停价格
        price_upper_limit = current_price * (1 + limit_ratio)
        price_lower_limit = current_price * (1 - limit_ratio)
        
        # 价格精度处理（保留2位小数）
        price_upper_limit = round(price_upper_limit, 2)
        price_lower_limit = round(price_lower_limit, 2)
        
        # 检查订单价格是否在涨跌停范围内
        return price_lower_limit <= order_price <= price_upper_limit
    
    def _calculate_slippage(self, order: Order) -> float:
        """计算滑点"""
        if not self._context:
            return 0.0
        
        # 获取基础滑点配置
        base_slippage = self._context.trade_config.get('slippage', 0.001) if hasattr(self._context, 'trade_config') else 0.001
        
        # 根据市场类型调整滑点
        market = self.market.lower()
        if market in ["cn_stock", "cn_fund"]:
            # A股/基金滑点相对较小
            market_adjustment = 1.0
        elif market == "cn_future":
            # 期货滑点中等
            market_adjustment = 1.2
        elif market == "global_cryptospot":
            # 加密货币滑点较大
            market_adjustment = 1.5
        else:
            market_adjustment = 1.0
        
        # 根据订单大小调整滑点
        volume_adjustment = 1.0
        if order.quantity > 10000:
            volume_adjustment = 1.2  # 大额订单增加20%滑点
        elif order.quantity > 50000:
            volume_adjustment = 1.5  # 超大额订单增加50%滑点
        elif order.quantity > 100000:
            volume_adjustment = 2.0  # 巨额订单增加100%滑点
        
        # 根据订单类型调整滑点
        type_adjustment = 1.0
        if order.order_type == OrderType.MARKET:
            type_adjustment = 1.5  # 市价单滑点更大
        
        # 根据当前市场波动性调整滑点（简化处理）
        volatility_adjustment = 1.0
        
        # 计算最终滑点
        final_slippage = base_slippage * market_adjustment * volume_adjustment * type_adjustment * volatility_adjustment
        
        # 限制滑点范围（0.01%到5%）
        final_slippage = max(0.0001, min(0.05, final_slippage))
        
        return final_slippage
    
    def _get_max_single_fill(self, order: Order, current_price: float) -> float:
        """获取单次最大成交量"""
        # 模拟市场流动性限制
        if order.order_type == OrderType.MARKET:
            # 市价单可以成交更多
            return min(order.quantity, 100000)
        else:
            # 限价单成交量相对较小
            return min(order.quantity, 50000)
    
    def _schedule_next_fill_attempt(self, order: Order):
        """安排下次撮合尝试"""
        # 这里可以将订单加入到下一个时间点的撮合队列
        # 暂时简化处理
        pass
    
    def _is_trading_time(self) -> bool:
        """检查是否在交易时间内"""
        if not self._context or not self._context.current_dt:
            return True  # 回测模式下默认允许交易
        
        current_time = self._context.current_dt.time()
        trading_hours = self.trading_rules.get("trading_hours", [])
        
        if not trading_hours:
            return True
        
        # 检查是否在交易时间段内
        for start_time_str, end_time_str in trading_hours:
            start_time = datetime.strptime(start_time_str, "%H:%M").time()
            end_time = datetime.strptime(end_time_str, "%H:%M").time()
            
            if start_time <= current_time <= end_time:
                return True
        
        return False
    
    def _validate_symbol(self, symbol: str) -> bool:
        """验证股票代码格式"""
        if not symbol:
            return False
        
        market = self.market.lower()
        
        # A股/基金代码验证
        if market in ["cn_stock", "cn_fund"]:
            if len(symbol) != 9 or '.' not in symbol:
                return False
            code, exchange = symbol.split('.')
            if len(code) != 6 or not code.isdigit():
                return False
            if exchange not in ['SH', 'SZ']:
                return False
        
        # 期货代码验证
        elif market == "cn_future":
            # 期货代码格式较复杂，这里简化处理
            if len(symbol) < 5:
                return False
        
        # 加密货币代码验证
        elif market == "global_cryptospot":
            # 加密货币代码通常是字母组合，如BTC, ETH等
            if not symbol.replace('_', '').replace('-', '').isalnum():
                return False
        
        return True
    
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
        """更新持仓（增强版）"""
        if symbol not in self.positions:
            self.positions[symbol] = Position(symbol=symbol)
        
        position = self.positions[symbol]
        
        if side == OrderSide.BUY:
            # 买入：增加持仓
            if position.quantity > 0:
                # 已有持仓，计算新的平均成本
                total_cost = position.quantity * position.avg_cost + quantity * price + commission
                position.quantity += quantity
                position.avg_cost = total_cost / position.quantity
            else:
                # 新建持仓
                position.quantity = quantity
                position.avg_cost = price + commission / quantity
            
            # T+1规则处理
            if self.trading_rules.get("t1_rule", False):
                # 当日买入的股票不能卖出
                position.available_quantity = position.quantity - quantity
                position.frozen_quantity = quantity
                # 记录买入日期，用于T+1解锁
                if not hasattr(position, 'buy_dates'):
                    position.buy_dates = []
                position.buy_dates.append({
                    'date': self._context.current_dt.date() if self._context and self._context.current_dt else datetime.now().date(),
                    'quantity': quantity
                })
            else:
                position.available_quantity = position.quantity
        
        else:
            # 卖出：减少持仓
            if position.quantity >= quantity:
                position.quantity -= quantity
                
                # T+1规则处理：检查可用数量
                if self.trading_rules.get("t1_rule", False):
                    # 只有非冻结的股票可以卖出
                    available_before = position.available_quantity
                    position.available_quantity = max(0, position.available_quantity - quantity)
                    
                    # 更新冻结数量
                    position.frozen_quantity = position.quantity - position.available_quantity
                    
                    # 如果卖出了今日买入的股票，需要更新buy_dates记录
                    if hasattr(position, 'buy_dates'):
                        remaining_sell = quantity
                        updated_buy_dates = []
                        for buy_record in position.buy_dates:
                            if remaining_sell <= 0:
                                updated_buy_dates.append(buy_record)
                                continue
                            
                            if buy_record['quantity'] <= remaining_sell:
                                remaining_sell -= buy_record['quantity']
                                # 完全卖出了该批买入的股票，不保留记录
                            else:
                                # 部分卖出了该批买入的股票
                                buy_record['quantity'] -= remaining_sell
                                updated_buy_dates.append(buy_record)
                                remaining_sell = 0
                        
                        position.buy_dates = updated_buy_dates
                else:
                    position.available_quantity = max(0, position.available_quantity - quantity)
                
                # 计算已实现盈亏
                realized_pnl = (price - position.avg_cost) * quantity - commission
                position.realized_pnl += realized_pnl
                
                # 更新账户的已实现盈亏
                if self._context:
                    self._context.account.realized_pnl += realized_pnl
        
        # 更新市值和未实现盈亏
        current_price = self.get_price(symbol=symbol)
        if current_price and position.quantity > 0:
            position.last_price = current_price
            position.market_value = position.quantity * current_price
            position.unrealized_pnl = (current_price - position.avg_cost) * position.quantity
        
        position.updated_at = datetime.now()
        
        # 同步兼容字段
        position.amount = position.quantity
        position.enable_amount = position.available_quantity
        position.last_sale_price = position.last_price
        position.cost_basis = position.avg_cost
        position.total_value = position.market_value
        position.total_cost = position.avg_cost * position.quantity
        
        # 如果持仓为0，移除持仓记录
        if position.quantity <= 0:
            if symbol in self.positions:
                del self.positions[symbol]
    
    def _schedule_t1_unlock(self, symbol: str, quantity: float):
        """安排T+1解锁"""
        # 在实际系统中，这里需要设置定时器在下一个交易日解锁
        # 暂时简化处理
        pass
    
    def _update_account(self, side: OrderSide, trade_value: float, commission: float):
        """更新账户资金（增强版）"""
        if not self._context:
            return
        
        if side == OrderSide.BUY:
            # 买入：减少现金
            total_cost = trade_value + commission
            self._context.account.cash -= total_cost
            self._context.account.available_cash -= total_cost
            
            # 记录交易成本
            self._context.account.total_commission += commission
            
        else:
            # 卖出：增加现金
            net_proceeds = trade_value - commission
            self._context.account.cash += net_proceeds
            self._context.account.available_cash += net_proceeds
            
            # 记录交易成本
            self._context.account.total_commission += commission
        
        # 更新组合现金
        self._context.portfolio.cash = self._context.account.cash
        
        # 更新总资产
        self._context.update_portfolio_value()
    
    # 事件处理器方法
    def handle_start_interval(self, context, event):
        """处理交易日开始事件"""
        context.logger.info("交易中心: 交易日开始")
        
        # 初始化交易状态
        self.is_trading = False
        
        # 重置日内交易相关状态
        self.daily_orders = []
        self.daily_trades = []
        
        # 更新T+1规则的可用数量
        self._update_t1_available_quantity()
        
        # 检查并处理待处理的订单
        self._process_pending_orders()
        
        # 同步持仓信息
        self._sync_positions()
        
        context.logger.info(f"交易中心初始化完成 - 持仓数量: {len(self.positions)}, 待处理订单: {len(self.get_pending_orders())}")
    
    def handle_before_market(self, context, event):
        """处理盘前事件"""
        context.logger.info("交易中心: 盘前准备")
        
        # 验证所有待处理订单
        self._validate_pending_orders()
        
        # 更新价格信息
        self._update_position_prices()
        
        # 计算预期的资金需求
        self._calculate_required_cash()
        
        # 检查账户资金是否充足
        self._check_account_balance()
        
        # 准备交易引擎
        self._prepare_trading_engine()
        
        context.logger.info(f"盘前准备完成 - 现金: {context.account.cash:.2f}, 可用现金: {context.account.available_cash:.2f}")
    
    def handle_morning_start(self, context, event):
        """处理上午开盘事件"""
        context.logger.info("交易中心: 上午开盘")
        
        # 开启交易
        self.is_trading = True
        
        # 处理市价单
        self._process_market_orders()
        
        # 开始订单撮合
        self._start_order_matching()
        
        # 主动处理订单撮合
        self.process_order_matching()
        
        context.logger.info("上午交易时段开始")
    
    def handle_morning_end(self, context, event):
        """处理上午收盘事件"""
        context.logger.info("交易中心: 上午收盘")
        
        # 暂停交易
        self.is_trading = False
        
        # 处理未成交的订单
        self._handle_unfilled_orders()
        
        # 更新上午交易统计
        self._update_morning_stats()
        
        # 保存交易记录
        self._save_trading_records()
        
        context.logger.info("上午交易时段结束")
    
    def handle_afternoon_start(self, context, event):
        """处理下午开盘事件"""
        context.logger.info("交易中心: 下午开盘")
        
        # 重新开启交易
        self.is_trading = True
        
        # 处理午间积累的订单
        self._process_market_orders()
        
        # 恢复订单撮合
        self._start_order_matching()
        
        # 主动处理订单撮合
        self.process_order_matching()
        
        context.logger.info("下午交易时段开始")
    
    def handle_afternoon_end(self, context, event):
        """处理下午收盘事件"""
        context.logger.info("交易中心: 下午收盘")
        
        # 结束交易
        self.is_trading = False
        
        # 处理收盘前的订单
        self._handle_closing_orders()
        
        # 更新下午交易统计
        self._update_afternoon_stats()
        
        # 计算当日交易汇总
        self._calculate_daily_summary()
        
        context.logger.info("下午交易时段结束")
    
    def handle_daily_bar_closed(self, context, event):
        """处理日线数据完成事件"""
        context.logger.info("交易中心: 日线数据完成")
        
        # 更新所有持仓的最新价格
        self._update_position_prices()
        
        # 在数据更新后，重试未成交订单
        self.retry_unfilled_orders()
        
        # 计算持仓盈亏
        self._calculate_position_pnl()
        
        # 更新账户价值
        self._update_account_value()
        
        # 执行分红除权处理
        self._process_dividend_split()
        
        # 生成交易报告
        self._generate_trading_report()
        
        context.logger.info("日线数据处理完成")
    
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
        
        # 清理过期订单
        self._cleanup_expired_orders()
        
        # 保存交易状态
        self._save_trading_state()
        
        # 生成日终报告
        self._generate_daily_report()
        
        context.logger.info("盘后清算完成")

    # 增加一些便捷方法
    def submit_order(self, symbol: str, side: str, volume: float, 
                    price: Optional[float] = None, order_type: OrderType = OrderType.MARKET,
                    **kwargs) -> Optional[str]:
        """提交订单 - 兼容旧接口"""
        return self.place_order(
            symbol=symbol, side=side, volume=volume, 
            price=price, order_type=order_type, **kwargs
        )
    
    def get_order_status(self, order_id: str) -> Optional[OrderStatus]:
        """获取订单状态"""
        order = self.orders.get(order_id)
        return order.status if order else None
    
    def get_pending_orders(self) -> List[Order]:
        """获取待处理订单"""
        return [order for order in self.orders.values() 
                if order.status in [OrderStatus.PENDING_NEW, OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]]
    
    def get_filled_orders(self) -> List[Order]:
        """获取已成交订单"""
        return [order for order in self.orders.values() 
                if order.status == OrderStatus.FILLED]
    
    def get_account_summary(self) -> Dict[str, Any]:
        """获取账户摘要"""
        if not self._context:
            return {}
        
        total_market_value = sum(pos.market_value for pos in self.positions.values())
        total_unrealized_pnl = sum(pos.unrealized_pnl for pos in self.positions.values())
        
        return {
            "cash": self._context.account.cash,
            "available_cash": self._context.account.available_cash,
            "frozen_cash": getattr(self._context.account, 'frozen_cash', 0),
            "total_market_value": total_market_value,
            "total_value": self._context.account.cash + total_market_value,
            "total_unrealized_pnl": total_unrealized_pnl,
            "total_realized_pnl": getattr(self._context.account, 'realized_pnl', 0),
            "total_commission": getattr(self._context.account, 'total_commission', 0),
            "positions_count": len(self.positions),
            "pending_orders": len(self.get_pending_orders()),
            "filled_orders": len(self.get_filled_orders())
        } 

    def _update_t1_available_quantity(self):
        """更新T+1规则的可用数量"""
        if not self.trading_rules.get("t1_rule", False):
            return
        
        current_date = self._context.current_dt.date() if self._context and self._context.current_dt else datetime.now().date()
        
        for symbol, position in self.positions.items():
            if not hasattr(position, 'buy_dates') or not position.buy_dates:
                continue
            
            # 检查是否有需要解冻的股票
            unlock_quantity = 0
            updated_buy_dates = []
            
            for buy_record in position.buy_dates:
                # 检查是否已经超过T+1限制（即买入日期早于当前日期）
                if buy_record['date'] < current_date:
                    # 可以解冻
                    unlock_quantity += buy_record['quantity']
                else:
                    # 仍需冻结
                    updated_buy_dates.append(buy_record)
            
            # 更新buy_dates记录
            position.buy_dates = updated_buy_dates
            
            # 解冻股票
            if unlock_quantity > 0:
                position.available_quantity += unlock_quantity
                position.frozen_quantity = max(0, position.frozen_quantity - unlock_quantity)
                position.enable_amount = position.available_quantity
                
                if self._context:
                    self._context.logger.debug(f"T+1解冻: {symbol} 解冻数量 {unlock_quantity}")
    
    def _process_pending_orders(self):
        """处理待处理的订单"""
        pending_orders = self.get_pending_orders()
        for order in pending_orders:
            # 重新验证订单
            if self._validate_order(order):
                order.status = OrderStatus.NEW
            else:
                order.status = OrderStatus.REJECTED
                if self._context:
                    self._context.logger.warning(f"订单验证失败: {order.order_id}")
    
    def _sync_positions(self):
        """同步持仓信息"""
        # 更新所有持仓的市值
        for symbol, position in self.positions.items():
            current_price = self.get_price(symbol=symbol)
            if current_price:
                position.last_price = current_price
                position.market_value = position.quantity * current_price
                position.unrealized_pnl = (current_price - position.avg_cost) * position.quantity
                
                # 更新兼容字段
                position.last_sale_price = current_price
                position.total_value = position.market_value
    
    def _validate_pending_orders(self):
        """验证所有待处理订单"""
        pending_orders = self.get_pending_orders()
        validated_count = 0
        
        for order in pending_orders:
            if self._validate_order(order):
                validated_count += 1
            else:
                order.status = OrderStatus.REJECTED
                if self._context:
                    self._context.logger.warning(f"订单验证失败: {order.order_id} - {order.symbol}")
        
        if self._context:
            self._context.logger.info(f"订单验证完成: {validated_count}/{len(pending_orders)} 个订单通过验证")
    
    def _update_position_prices(self):
        """更新持仓价格"""
        updated_count = 0
        for symbol, position in self.positions.items():
            current_price = self.get_price(symbol=symbol)
            if current_price and current_price != position.last_price:
                position.last_price = current_price
                position.market_value = position.quantity * current_price
                position.unrealized_pnl = (current_price - position.avg_cost) * position.quantity
                position.last_sale_price = current_price
                position.total_value = position.market_value
                updated_count += 1
        
        if self._context and updated_count > 0:
            self._context.logger.info(f"更新了 {updated_count} 个持仓的价格")
    
    def _calculate_required_cash(self):
        """计算预期的资金需求"""
        required_cash = 0
        pending_buy_orders = [order for order in self.get_pending_orders() 
                             if order.side == OrderSide.BUY]
        
        for order in pending_buy_orders:
            estimated_cost = self._estimate_order_cost(order)
            required_cash += estimated_cost
        
        if self._context:
            self._context.logger.info(f"待处理买单预计需要资金: {required_cash:.2f}")
        
        return required_cash
    
    def _check_account_balance(self):
        """检查账户资金是否充足"""
        if not self._context:
            return
        
        required_cash = self._calculate_required_cash()
        available_cash = self._context.account.available_cash
        
        if required_cash > available_cash:
            self._context.logger.warning(
                f"资金不足: 需要 {required_cash:.2f}, 可用 {available_cash:.2f}"
            )
            
            # 取消部分订单以释放资金
            self._cancel_orders_to_free_cash(required_cash - available_cash)
    
    def _prepare_trading_engine(self):
        """准备交易引擎"""
        # 设置交易参数
        self.is_trading = False  # 开盘前先设为False
        
        # 初始化日内统计
        self.daily_orders = []
        self.daily_trades = []
        
        if self._context:
            self._context.logger.info("交易引擎准备完成")
    
    def _process_market_orders(self):
        """处理市价单"""
        market_orders = [order for order in self.get_pending_orders() 
                        if order.order_type == OrderType.MARKET]
        
        for order in market_orders:
            self._try_fill_order(order)
    
    def _start_order_matching(self):
        """开始订单撮合"""
        # 处理所有待处理订单
        pending_orders = self.get_pending_orders()
        for order in pending_orders:
            self._try_fill_order(order)
    
    def _handle_unfilled_orders(self):
        """处理未成交的订单"""
        unfilled_orders = [order for order in self.get_pending_orders() 
                          if order.status == OrderStatus.NEW]
        
        for order in unfilled_orders:
            # 限价单继续保留，市价单取消
            if order.order_type == OrderType.MARKET:
                order.status = OrderStatus.CANCELLED
                if self._context:
                    self._context.logger.info(f"取消未成交的市价单: {order.order_id}")
    
    def _update_morning_stats(self):
        """更新上午交易统计"""
        if not hasattr(self, 'morning_stats'):
            self.morning_stats = {}
        
        filled_orders = [order for order in self.orders.values() 
                        if order.status == OrderStatus.FILLED]
        
        self.morning_stats = {
            'filled_orders': len(filled_orders),
            'total_volume': sum(order.filled_quantity for order in filled_orders),
            'total_value': sum(order.filled_quantity * order.avg_fill_price for order in filled_orders)
        }
    
    def _save_trading_records(self):
        """保存交易记录"""
        if self._context:
            self._context.logger.info("保存交易记录")
    
    def _handle_closing_orders(self):
        """处理收盘前的订单"""
        # 强制成交所有市价单
        market_orders = [order for order in self.get_pending_orders() 
                        if order.order_type == OrderType.MARKET]
        
        for order in market_orders:
            self._try_fill_order(order)
    
    def _update_afternoon_stats(self):
        """更新下午交易统计"""
        if not hasattr(self, 'afternoon_stats'):
            self.afternoon_stats = {}
        
        # 计算下午的交易统计
        filled_orders = [order for order in self.orders.values() 
                        if order.status == OrderStatus.FILLED]
        
        self.afternoon_stats = {
            'filled_orders': len(filled_orders),
            'total_volume': sum(order.filled_quantity for order in filled_orders),
            'total_value': sum(order.filled_quantity * order.avg_fill_price for order in filled_orders)
        }
    
    def _calculate_daily_summary(self):
        """计算当日交易汇总"""
        if not hasattr(self, 'daily_summary'):
            self.daily_summary = {}
        
        all_trades = list(self.trades.values())
        buy_trades = [t for t in all_trades if t.side == OrderSide.BUY]
        sell_trades = [t for t in all_trades if t.side == OrderSide.SELL]
        
        self.daily_summary = {
            'total_trades': len(all_trades),
            'buy_trades': len(buy_trades),
            'sell_trades': len(sell_trades),
            'buy_volume': sum(t.quantity for t in buy_trades),
            'sell_volume': sum(t.quantity for t in sell_trades),
            'buy_value': sum(t.quantity * t.price for t in buy_trades),
            'sell_value': sum(t.quantity * t.price for t in sell_trades),
            'total_commission': sum(t.commission for t in all_trades)
        }
    
    def _calculate_position_pnl(self):
        """计算持仓盈亏"""
        total_unrealized_pnl = 0
        
        for symbol, position in self.positions.items():
            current_price = self.get_price(symbol=symbol)
            if current_price:
                position.last_price = current_price
                position.market_value = position.quantity * current_price
                position.unrealized_pnl = (current_price - position.avg_cost) * position.quantity
                total_unrealized_pnl += position.unrealized_pnl
        
        if self._context:
            self._context.logger.info(f"总未实现盈亏: {total_unrealized_pnl:.2f}")
        
        return total_unrealized_pnl
    
    def _update_account_value(self):
        """更新账户价值"""
        if not self._context:
            return
        
        total_market_value = sum(pos.market_value for pos in self.positions.values())
        self._context.account.total_value = self._context.account.cash + total_market_value
        
        # 更新组合价值
        self._context.portfolio.total_value = self._context.account.total_value
        self._context.portfolio.positions_value = total_market_value
    
    def _process_dividend_split(self):
        """执行分红除权处理"""
        # 这里应该根据实际的分红除权数据进行处理
        # 简化实现：暂时跳过
        pass
    
    def handle_dividend_distribution(self, symbol: str, dividend_per_share: float, 
                                   ex_dividend_date: str, record_date: str):
        """
        处理现金分红
        
        Args:
            symbol: 股票代码
            dividend_per_share: 每股分红金额
            ex_dividend_date: 除权除息日
            record_date: 股权登记日
        """
        try:
            if symbol not in self.positions:
                return
            
            position = self.positions[symbol]
            
            # 计算分红金额
            dividend_amount = position.quantity * dividend_per_share
            
            if dividend_amount > 0:
                # 增加现金
                if self._context:
                    self._context.account.cash += dividend_amount
                    self._context.account.available_cash += dividend_amount
                
                # 更新持仓的已实现盈亏
                position.realized_pnl += dividend_amount
                
                # 记录分红日志
                if self._context:
                    self._context.logger.info(
                        f"现金分红: {symbol} 分红金额 {dividend_amount:.2f} "
                        f"(持仓 {position.quantity} 股, 每股分红 {dividend_per_share:.4f})"
                    )
                
                # 记录分红事件
                self._record_dividend_event(symbol, dividend_amount, "cash_dividend")
                
        except Exception as e:
            if self._context:
                self._context.logger.error(f"处理现金分红失败: {symbol}, {e}")
    
    def handle_stock_split(self, symbol: str, split_ratio: float, 
                          ex_split_date: str, record_date: str):
        """
        处理股票分拆/送股
        
        Args:
            symbol: 股票代码
            split_ratio: 拆分比例（例如2.0表示1拆2）
            ex_split_date: 除权日
            record_date: 股权登记日
        """
        try:
            if symbol not in self.positions:
                return
            
            position = self.positions[symbol]
            
            # 计算新的股数
            old_quantity = position.quantity
            new_quantity = old_quantity * split_ratio
            
            # 计算新的平均成本
            new_avg_cost = position.avg_cost / split_ratio
            
            # 更新持仓信息
            position.quantity = new_quantity
            position.amount = new_quantity  # 兼容字段
            position.avg_cost = new_avg_cost
            position.cost_basis = new_avg_cost  # 兼容字段
            
            # 更新可用数量
            position.available_quantity = position.available_quantity * split_ratio
            position.enable_amount = position.available_quantity  # 兼容字段
            
            # 更新冻结数量
            position.frozen_quantity = position.frozen_quantity * split_ratio
            
            # 更新总成本保持不变
            position.total_cost = position.quantity * position.avg_cost
            
            # 更新时间戳
            position.updated_at = datetime.now()
            
            # 记录股票分拆日志
            if self._context:
                self._context.logger.info(
                    f"股票分拆: {symbol} 分拆比例 {split_ratio:.2f} "
                    f"(原持仓 {old_quantity} 股 -> 新持仓 {new_quantity} 股, "
                    f"原成本 {position.avg_cost * split_ratio:.4f} -> 新成本 {new_avg_cost:.4f})"
                )
            
            # 记录分拆事件
            self._record_dividend_event(symbol, split_ratio, "stock_split")
            
        except Exception as e:
            if self._context:
                self._context.logger.error(f"处理股票分拆失败: {symbol}, {e}")
    
    def handle_stock_dividend(self, symbol: str, dividend_ratio: float, 
                            ex_dividend_date: str, record_date: str):
        """
        处理股票股利（送股）
        
        Args:
            symbol: 股票代码
            dividend_ratio: 送股比例（例如0.1表示10送1）
            ex_dividend_date: 除权日
            record_date: 股权登记日
        """
        try:
            if symbol not in self.positions:
                return
            
            position = self.positions[symbol]
            
            # 计算送股数量
            dividend_shares = position.quantity * dividend_ratio
            
            if dividend_shares > 0:
                # 增加股票数量
                position.quantity += dividend_shares
                position.amount = position.quantity  # 兼容字段
                
                # 调整平均成本（保持总成本不变）
                total_cost = position.quantity * position.avg_cost
                position.avg_cost = total_cost / (position.quantity + dividend_shares)
                position.cost_basis = position.avg_cost  # 兼容字段
                
                # 更新可用数量（送股通常立即可用）
                position.available_quantity += dividend_shares
                position.enable_amount = position.available_quantity  # 兼容字段
                
                # 更新时间戳
                position.updated_at = datetime.now()
                
                # 记录送股日志
                if self._context:
                    self._context.logger.info(
                        f"股票送股: {symbol} 送股比例 {dividend_ratio:.4f} "
                        f"(获得 {dividend_shares} 股送股, 总持仓 {position.quantity} 股)"
                    )
                
                # 记录送股事件
                self._record_dividend_event(symbol, dividend_shares, "stock_dividend")
                
        except Exception as e:
            if self._context:
                self._context.logger.error(f"处理股票送股失败: {symbol}, {e}")
    
    def handle_rights_offering(self, symbol: str, rights_ratio: float, 
                             rights_price: float, ex_rights_date: str, record_date: str):
        """
        处理配股
        
        Args:
            symbol: 股票代码
            rights_ratio: 配股比例（例如0.3表示10配3）
            rights_price: 配股价格
            ex_rights_date: 除权日
            record_date: 股权登记日
        """
        try:
            if symbol not in self.positions:
                return
            
            position = self.positions[symbol]
            
            # 计算配股数量
            rights_shares = position.quantity * rights_ratio
            
            if rights_shares > 0:
                # 计算配股所需资金
                rights_cost = rights_shares * rights_price
                
                # 检查资金是否充足
                if self._context and self._context.account.available_cash >= rights_cost:
                    # 扣减现金
                    self._context.account.cash -= rights_cost
                    self._context.account.available_cash -= rights_cost
                    
                    # 增加股票数量
                    old_quantity = position.quantity
                    position.quantity += rights_shares
                    position.amount = position.quantity  # 兼容字段
                    
                    # 重新计算平均成本
                    total_cost = old_quantity * position.avg_cost + rights_cost
                    position.avg_cost = total_cost / position.quantity
                    position.cost_basis = position.avg_cost  # 兼容字段
                    position.total_cost = total_cost
                    
                    # 更新可用数量
                    position.available_quantity += rights_shares
                    position.enable_amount = position.available_quantity  # 兼容字段
                    
                    # 更新时间戳
                    position.updated_at = datetime.now()
                    
                    # 记录配股日志
                    if self._context:
                        self._context.logger.info(
                            f"配股: {symbol} 配股比例 {rights_ratio:.4f} "
                            f"(获得 {rights_shares} 股配股, 配股价 {rights_price:.4f}, "
                            f"总成本 {rights_cost:.2f}, 总持仓 {position.quantity} 股)"
                        )
                    
                    # 记录配股事件
                    self._record_dividend_event(symbol, rights_shares, "rights_offering")
                    
                else:
                    if self._context:
                        self._context.logger.warning(
                            f"配股资金不足: {symbol} 需要 {rights_cost:.2f}, "
                            f"可用资金 {self._context.account.available_cash:.2f}"
                        )
                
        except Exception as e:
            if self._context:
                self._context.logger.error(f"处理配股失败: {symbol}, {e}")
    
    def handle_bonus_issue(self, symbol: str, bonus_ratio: float, 
                          ex_bonus_date: str, record_date: str):
        """
        处理转增股本
        
        Args:
            symbol: 股票代码
            bonus_ratio: 转增比例（例如0.5表示10转5）
            ex_bonus_date: 除权日
            record_date: 股权登记日
        """
        try:
            if symbol not in self.positions:
                return
            
            position = self.positions[symbol]
            
            # 计算转增数量
            bonus_shares = position.quantity * bonus_ratio
            
            if bonus_shares > 0:
                # 增加股票数量
                old_quantity = position.quantity
                position.quantity += bonus_shares
                position.amount = position.quantity  # 兼容字段
                
                # 调整平均成本（保持总成本不变）
                position.avg_cost = position.avg_cost * old_quantity / position.quantity
                position.cost_basis = position.avg_cost  # 兼容字段
                
                # 更新可用数量
                position.available_quantity += bonus_shares
                position.enable_amount = position.available_quantity  # 兼容字段
                
                # 更新时间戳
                position.updated_at = datetime.now()
                
                # 记录转增日志
                if self._context:
                    self._context.logger.info(
                        f"转增股本: {symbol} 转增比例 {bonus_ratio:.4f} "
                        f"(获得 {bonus_shares} 股转增, 总持仓 {position.quantity} 股)"
                    )
                
                # 记录转增事件
                self._record_dividend_event(symbol, bonus_shares, "bonus_issue")
                
        except Exception as e:
            if self._context:
                self._context.logger.error(f"处理转增股本失败: {symbol}, {e}")
    
    def _record_dividend_event(self, symbol: str, value: float, event_type: str):
        """
        记录分红除权事件
        
        Args:
            symbol: 股票代码
            value: 事件数值
            event_type: 事件类型
        """
        try:
            if not hasattr(self, 'dividend_events'):
                self.dividend_events = []
            
            event = {
                'symbol': symbol,
                'value': value,
                'event_type': event_type,
                'timestamp': datetime.now(),
                'date': datetime.now().strftime('%Y-%m-%d')
            }
            
            self.dividend_events.append(event)
            
            # 保持事件记录不超过1000条
            if len(self.dividend_events) > 1000:
                self.dividend_events = self.dividend_events[-1000:]
                
        except Exception as e:
            if self._context:
                self._context.logger.error(f"记录分红除权事件失败: {e}")
    
    def get_dividend_events(self, symbol: str = None, event_type: str = None) -> List[Dict]:
        """
        获取分红除权事件记录
        
        Args:
            symbol: 股票代码（可选）
            event_type: 事件类型（可选）
            
        Returns:
            List[Dict]: 事件记录列表
        """
        try:
            if not hasattr(self, 'dividend_events'):
                return []
            
            events = self.dividend_events
            
            # 按股票代码过滤
            if symbol:
                events = [e for e in events if e['symbol'] == symbol]
            
            # 按事件类型过滤
            if event_type:
                events = [e for e in events if e['event_type'] == event_type]
            
            return events
            
        except Exception as e:
            if self._context:
                self._context.logger.error(f"获取分红除权事件失败: {e}")
            return []
    
    def _generate_trading_report(self):
        """生成交易报告"""
        if not self._context:
            return
        
        report = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'positions_count': len(self.positions),
            'total_value': getattr(self._context.account, 'total_value', 0),
            'cash': self._context.account.cash,
            'trades_count': len(self.trades),
            'orders_count': len(self.orders)
        }
        
        self._context.logger.info(f"交易报告: {report}")
    
    def _cleanup_expired_orders(self):
        """清理过期订单"""
        expired_orders = []
        
        for order_id, order in self.orders.items():
            # 简化实现：取消所有未成交的订单
            if order.status in [OrderStatus.PENDING_NEW, OrderStatus.NEW]:
                order.status = OrderStatus.CANCELLED
                expired_orders.append(order_id)
        
        if self._context and expired_orders:
            self._context.logger.info(f"清理了 {len(expired_orders)} 个过期订单")
    
    def _save_trading_state(self):
        """保存交易状态"""
        if self._context:
            self._context.logger.info("保存交易状态")
    
    def _generate_daily_report(self):
        """生成日终报告"""
        if not self._context:
            return
        
        # 计算日终统计
        total_market_value = sum(pos.market_value for pos in self.positions.values())
        total_unrealized_pnl = sum(pos.unrealized_pnl for pos in self.positions.values())
        
        report = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'total_value': self._context.account.cash + total_market_value,
            'cash': self._context.account.cash,
            'market_value': total_market_value,
            'unrealized_pnl': total_unrealized_pnl,
            'positions_count': len(self.positions),
            'trades_count': len(self.trades),
            'commission_paid': sum(t.commission for t in self.trades.values())
        }
        
        self._context.logger.info(f"日终报告: {report}")
    
    def _cancel_orders_to_free_cash(self, cash_needed: float):
        """取消订单以释放资金"""
        pending_buy_orders = [order for order in self.get_pending_orders() 
                             if order.side == OrderSide.BUY]
        
        # 按订单金额降序排列，优先取消大额订单
        pending_buy_orders.sort(key=lambda x: self._estimate_order_cost(x), reverse=True)
        
        freed_cash = 0
        for order in pending_buy_orders:
            if freed_cash >= cash_needed:
                break
            
            order_cost = self._estimate_order_cost(order)
            order.status = OrderStatus.CANCELLED
            freed_cash += order_cost
            
            if self._context:
                self._context.logger.info(f"取消订单释放资金: {order.order_id}, 金额: {order_cost:.2f}")
    
    def _get_price_from_datacenter(self, symbol: str) -> Optional[float]:
        """从DataCenter获取股票价格（带缓存优化）"""
        try:
            if not self._context or not self._context.data_center:
                return None
            
            # 获取当前回测时间
            current_time = self._context.current_dt
            if not current_time:
                return None
            
            # 获取当前频率
            freq = '1d'
            if hasattr(self._context, 'trade_config'):
                freq = self._context.trade_config.get('frequency', '1d')
            elif hasattr(self._context, 'settings'):
                freq = self._context.settings.get('freq', '1d')
            
            # === 性能优化：价格缓存 ===
            # 生成缓存键（精确到bar级别）
            if freq == '1m':
                # 1分钟数据：缓存键精确到分钟
                cache_key = current_time.replace(second=0, microsecond=0)
            else:
                # 日线数据：缓存键精确到日期
                cache_key = current_time.date()
            
            # 如果bar时间变化，清空缓存
            if cache_key != self._current_bar_time:
                self._price_cache = {}
                self._current_bar_time = cache_key
            
            # 检查缓存
            if symbol in self._price_cache:
                return self._price_cache[symbol]
            
            # === 缓存未命中，从DataCenter获取 ===
            
            # 获取复权类型
            adj_type = 'none'
            if hasattr(self._context, 'trade_config'):
                adj_type = self._context.trade_config.get('adj_type', 'none')
            
            # 使用get_quotes方法获取价格
            quotes_df = self._context.data_center.get_quotes(
                codes=[symbol],
                freq=freq,
                time=current_time,
                fields=['close'],
                adj_type=adj_type
            )
            
            price = None
            if not quotes_df.empty and symbol in quotes_df.index:
                price = float(quotes_df.loc[symbol, 'close'])
            
            # 如果get_quotes失败，尝试使用get_klines获取最近的价格
            if price is None:
                from datetime import timedelta
                start_time = (current_time - timedelta(days=5)).strftime('%Y-%m-%d %H:%M:%S')
                end_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
                
                klines_df = self._context.data_center.get_klines(
                    codes=[symbol],
                    freq=freq,
                    start_time=start_time,
                    end_time=end_time,
                    fields=['close'],
                    adj_type=adj_type
                )
                
                if not klines_df.empty:
                    # 获取最接近当前时间的数据
                    symbol_klines = klines_df[klines_df.index.get_level_values('symbol') == symbol]
                    if not symbol_klines.empty:
                        price = float(symbol_klines['close'].iloc[-1])
            
            # 缓存价格（无论成功与否都缓存，避免重复查询）
            if price is not None:
                self._price_cache[symbol] = price
            
            return price
            
        except Exception as e:
            if self._context and hasattr(self._context, 'logger'):
                self._context.logger.error(f"从DataCenter获取价格失败: {symbol} - {str(e)}")
            return None
    
    def process_order_matching(self):
        """
        主动处理订单撮合 - 在每个事件中调用
        """
        try:
            # 获取所有未完全成交的订单
            pending_orders = [order for order in self.orders.values() 
                            if order.status in [OrderStatus.PENDING_NEW, OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]]
            
            if not pending_orders:
                return
            
            # 按优先级排序：时间优先
            pending_orders.sort(key=lambda x: x.created_at)
            
            matched_count = 0
            partial_matched_count = 0
            
            # 逐个处理订单
            for order in pending_orders:
                try:
                    old_status = order.status
                    old_filled_quantity = order.filled_quantity
                    
                    # 尝试撮合
                    self._try_fill_order(order)
                    
                    # 统计撮合结果
                    if order.status == OrderStatus.FILLED and old_status != OrderStatus.FILLED:
                        matched_count += 1
                    elif order.filled_quantity > old_filled_quantity:
                        partial_matched_count += 1
                        
                except Exception as e:
                    if self._context:
                        self._context.logger.error(f"处理订单撮合失败: {order.order_id}, {e}")
            
            # 记录撮合结果
            if self._context and len(pending_orders) > 0:
                self._context.logger.debug(f"订单撮合完成：{len(pending_orders)}个待处理订单，{matched_count}个完全成交，{partial_matched_count}个部分成交")
                
        except Exception as e:
            if self._context:
                self._context.logger.error(f"订单撮合处理失败: {e}")
    
    def retry_unfilled_orders(self):
        """
        重试未成交的订单 - 在每个事件前调用
        """
        try:
            if self._context:
                self._context.logger.debug("开始重试未成交订单...")
            
            # 调用撮合逻辑
            self.process_order_matching()
            
            # 统计未成交订单
            pending_orders = self.get_pending_orders()
            if self._context and len(pending_orders) > 0:
                self._context.logger.debug(f"仍有{len(pending_orders)}个订单未完全成交")
            
        except Exception as e:
            if self._context:
                self._context.logger.error(f"重试未成交订单失败: {str(e)}")
    
    def update_position_market_value(self):
        """
        更新持仓市值
        """
        try:
            for symbol, position in self.positions.items():
                # 获取最新价格
                current_price = self.get_price(symbol=symbol)
                
                if current_price and current_price > 0:
                    # 更新持仓价格和市值
                    position.last_price = current_price
                    position.last_sale_price = current_price  # 兼容字段
                    position.market_value = position.quantity * current_price
                    position.total_value = position.market_value  # 兼容字段
                    
                    # 计算未实现盈亏
                    if position.avg_cost > 0:
                        position.unrealized_pnl = (current_price - position.avg_cost) * position.quantity
                    
                    position.updated_at = datetime.now()
            
            # 更新账户总资产
            self._update_account_value()
            
            if self._context:
                self._context.logger.debug("持仓市值更新完成")
                
        except Exception as e:
            if self._context:
                self._context.logger.error(f"更新持仓市值失败: {e}")
    
    def process_market_open_orders(self):
        """
        处理开盘时的订单
        """
        try:
            # 重新验证所有pending订单
            self._validate_pending_orders()
            
            # 处理市价单优先
            market_orders = [order for order in self.orders.values() 
                           if order.status in [OrderStatus.PENDING_NEW, OrderStatus.PARTIALLY_FILLED] 
                           and order.order_type == OrderType.MARKET]
            
            for order in market_orders:
                self._try_fill_order(order)
            
            if self._context:
                self._context.logger.debug(f"开盘订单处理完成，处理了{len(market_orders)}个市价单")
                
        except Exception as e:
            if self._context:
                self._context.logger.error(f"开盘订单处理失败: {e}")
    
    def handle_t1_unlock(self):
        """
        处理T+1解冻
        """
        try:
            if not self.trading_rules.get("t1_rule", False):
                return
            
            # 检查是否有需要解冻的持仓
            for symbol, position in self.positions.items():
                if position.frozen_quantity > 0:
                    # 简化处理：直接解冻所有冻结的股票
                    unlock_quantity = position.frozen_quantity
                    position.available_quantity += unlock_quantity
                    position.enable_amount = position.available_quantity  # 兼容字段
                    position.frozen_quantity = 0
                    
                    if self._context:
                        self._context.logger.debug(f"T+1解冻: {symbol} 解冻数量 {unlock_quantity}")
            
        except Exception as e:
            if self._context:
                self._context.logger.error(f"T+1解冻处理失败: {e}")
    
    def check_risk_limits(self):
        """
        检查风险控制限制
        """
        try:
            if not self._context:
                return True
            
            account = self.get_account()
            if not account:
                return False
            
            # 检查资金是否充足
            if account['available_cash'] < 0:
                if self._context:
                    self._context.logger.warning("可用资金不足")
                return False
            
            # 检查总资产
            if account['total_value'] <= 0:
                if self._context:
                    self._context.logger.warning("总资产为零或负数")
                return False
            
            # 检查持仓集中度
            total_value = account['total_value']
            for symbol, position in self.positions.items():
                if position.market_value > 0:
                    concentration = position.market_value / total_value
                    if concentration > 0.5:  # 单只股票不超过50%
                        if self._context:
                            self._context.logger.warning(f"持仓集中度过高: {symbol} {concentration:.2%}")
            
            return True
            
        except Exception as e:
            if self._context:
                self._context.logger.error(f"风险控制检查失败: {e}")
            return False
    
    def cancel_pending_market_orders(self):
        """
        取消待成交的市价单
        """
        try:
            market_orders = [order for order in self.orders.values() 
                           if order.status in [OrderStatus.PENDING_NEW, OrderStatus.PARTIALLY_FILLED] 
                           and order.order_type == OrderType.MARKET]
            
            cancelled_count = 0
            for order in market_orders:
                order.status = OrderStatus.CANCELLED
                order.updated_at = datetime.now()
                cancelled_count += 1
            
            if cancelled_count > 0 and self._context:
                self._context.logger.info(f"取消了{cancelled_count}个市价单")
                
        except Exception as e:
            if self._context:
                self._context.logger.error(f"取消市价单失败: {e}")
    
    def generate_performance_summary(self) -> Dict[str, Any]:
        """
        生成绩效汇总
        """
        try:
            account = self.get_account()
            if not account:
                return {}
            
            # 计算总体绩效
            initial_cash = getattr(self._context.account, 'initial_cash', 1000000.0)
            current_value = account['total_value']
            total_return = (current_value - initial_cash) / initial_cash if initial_cash > 0 else 0
            
            # 计算持仓统计
            position_count = len([p for p in self.positions.values() if p.quantity > 0])
            total_unrealized_pnl = sum(p.unrealized_pnl for p in self.positions.values())
            total_realized_pnl = sum(p.realized_pnl for p in self.positions.values())
            
            # 计算交易统计
            total_trades = len(self.trades)
            total_commission = sum(t.commission for t in self.trades.values())
            
            # 计算今日统计
            today = datetime.now().date()
            today_trades = [t for t in self.trades.values() if t.timestamp.date() == today]
            today_trade_count = len(today_trades)
            today_volume = sum(t.quantity for t in today_trades)
            today_amount = sum(t.quantity * t.price for t in today_trades)
            
            return {
                'account_value': current_value,
                'available_cash': account['available_cash'],
                'total_return': total_return,
                'position_count': position_count,
                'total_unrealized_pnl': total_unrealized_pnl,
                'total_realized_pnl': total_realized_pnl,
                'total_trades': total_trades,
                'total_commission': total_commission,
                'today_trades': today_trade_count,
                'today_volume': today_volume,
                'today_amount': today_amount
            }
            
        except Exception as e:
            if self._context:
                self._context.logger.error(f"生成绩效汇总失败: {e}")
            return {} 

    def handle_corporate_action(self, event):
        """处理公司行为事件（分红、送股等）"""
        try:
            if not hasattr(event, 'action_type') or not hasattr(event, 'symbol'):
                return
            
            action_type = event.action_type.lower()
            symbol = event.symbol
            
            # 获取当前持仓
            position = self.positions.get(symbol)
            if not position or position.volume <= 0:
                return
            
            # 处理不同类型的公司行为
            if action_type == 'dividend':
                self._handle_dividend(event, position)
            elif action_type == 'bonus':
                self._handle_bonus_shares(event, position)
            elif action_type == 'split':
                self._handle_stock_split(event, position)
            elif action_type == 'rights':
                self._handle_rights_issue(event, position)
            
            # 更新持仓信息
            self._update_position_after_corporate_action(position, event)
            
            if self._context:
                self._context.logger.info(f"处理公司行为事件: {symbol} {action_type}")
        
        except Exception as e:
            if self._context:
                self._context.logger.error(f"处理公司行为事件失败: {e}")
    
    def _handle_dividend(self, event, position):
        """处理分红事件"""
        try:
            if not hasattr(event, 'dividend_per_share'):
                return
            
            dividend_per_share = event.dividend_per_share
            dividend_amount = position.volume * dividend_per_share
            
            # 计算分红税（A股分红需要缴税）
            tax_rate = 0.10  # A股分红税率10%
            if hasattr(event, 'tax_rate'):
                tax_rate = event.tax_rate
            
            tax_amount = dividend_amount * tax_rate
            net_dividend = dividend_amount - tax_amount
            
            # 更新账户现金
            if self._context and hasattr(self._context, 'account'):
                self._context.account.cash += net_dividend
                self._context.account.cash_available += net_dividend
                
                # 记录已实现收益
                self._context.account.realized_pnl += net_dividend
            
            if self._context:
                self._context.logger.info(f"分红处理: {position.symbol} 每股{dividend_per_share:.4f}, "
                                       f"税后{net_dividend:.2f}, 税率{tax_rate:.2%}")
        
        except Exception as e:
            if self._context:
                self._context.logger.error(f"处理分红事件失败: {e}")
    
    def _handle_bonus_shares(self, event, position):
        """处理送股事件"""
        try:
            if not hasattr(event, 'bonus_ratio'):
                return
            
            bonus_ratio = event.bonus_ratio  # 送股比例，如0.1表示每10股送1股
            bonus_shares = position.volume * bonus_ratio
            
            # 更新持仓数量
            position.volume += bonus_shares
            position.available_quantity += bonus_shares
            
            # 调整成本价
            if position.volume > 0:
                position.avg_cost = position.avg_cost * (position.volume - bonus_shares) / position.volume
            
            if self._context:
                self._context.logger.info(f"送股处理: {position.symbol} 送股比例{bonus_ratio:.2f}, "
                                       f"送股数量{bonus_shares:.2f}, 新持仓{position.volume:.2f}")
        
        except Exception as e:
            if self._context:
                self._context.logger.error(f"处理送股事件失败: {e}")
    
    def _handle_stock_split(self, event, position):
        """处理拆股事件"""
        try:
            if not hasattr(event, 'split_ratio'):
                return
            
            split_ratio = event.split_ratio  # 拆股比例，如2表示1拆2
            old_volume = position.volume
            old_cost = position.avg_cost
            
            # 更新持仓数量和成本价
            position.volume *= split_ratio
            position.available_quantity *= split_ratio
            position.avg_cost /= split_ratio
            
            if self._context:
                self._context.logger.info(f"拆股处理: {position.symbol} 拆股比例{split_ratio:.2f}, "
                                       f"原持仓{old_volume:.2f}@{old_cost:.2f}, "
                                       f"新持仓{position.volume:.2f}@{position.avg_cost:.2f}")
        
        except Exception as e:
            if self._context:
                self._context.logger.error(f"处理拆股事件失败: {e}")
    
    def _handle_rights_issue(self, event, position):
        """处理配股事件"""
        try:
            if not hasattr(event, 'rights_ratio') or not hasattr(event, 'rights_price'):
                return
            
            rights_ratio = event.rights_ratio  # 配股比例
            rights_price = event.rights_price  # 配股价格
            
            # 计算可配股数量
            rights_shares = position.volume * rights_ratio
            
            # 检查是否有足够资金认购
            total_cost = rights_shares * rights_price
            if self._context and hasattr(self._context, 'account'):
                if self._context.account.cash_available < total_cost:
                    if self._context:
                        self._context.logger.warning(f"资金不足，无法认购配股: {position.symbol}")
                    return
                
                # 扣除资金
                self._context.account.cash -= total_cost
                self._context.account.cash_available -= total_cost
                
                # 更新持仓
                old_volume = position.volume
                old_cost_basis = position.avg_cost * position.volume
                
                position.volume += rights_shares
                position.available_quantity += rights_shares
                
                # 重新计算平均成本
                position.avg_cost = (old_cost_basis + total_cost) / position.volume
                
                if self._context:
                    self._context.logger.info(f"配股处理: {position.symbol} 配股比例{rights_ratio:.2f}, "
                                           f"配股价格{rights_price:.2f}, 配股数量{rights_shares:.2f}, "
                                           f"原持仓{old_volume:.2f}, 新持仓{position.volume:.2f}@{position.avg_cost:.2f}")
        
        except Exception as e:
            if self._context:
                self._context.logger.error(f"处理配股事件失败: {e}")
    
    def _update_position_after_corporate_action(self, position, event):
        """公司行为后更新持仓信息"""
        try:
            # 更新市值和未实现盈亏
            current_price = self.get_price(symbol=position.symbol)
            if current_price and position.volume > 0:
                position.last_price = current_price
                position.market_value = position.volume * current_price
                position.unrealized_pnl = (current_price - position.avg_cost) * position.volume
            
            # 更新时间戳
            position.updated_at = datetime.now()
            
            # 同步兼容字段
            position.amount = position.volume
            position.enable_amount = position.available_quantity
            position.last_sale_price = position.last_price
            position.cost_basis = position.avg_cost
            position.total_value = position.market_value
            position.total_cost = position.avg_cost * position.volume
        
        except Exception as e:
            if self._context:
                self._context.logger.error(f"更新持仓信息失败: {e}")
    
    def process_corporate_actions(self, current_date):
        """处理指定日期的所有公司行为事件"""
        try:
            # 从数据接口获取公司行为数据
            if not self._context or not hasattr(self._context, 'data_center'):
                return
            
            data_center = self._context.data_center
            
            # 获取当前日期的所有公司行为事件
            corporate_actions = data_center.get_corporate_actions(
                date=current_date,
                market=self.market
            )
            
            if not corporate_actions:
                return
            
            # 处理每个公司行为事件
            for action in corporate_actions:
                # 创建事件对象
                event = type('CorporateActionEvent', (), {
                    'event_type': type('EventType', (), {'value': 'CORPORATE_ACTION'})(),
                    'event_time': datetime.combine(current_date, datetime.min.time()),
                    'symbol': action.get('symbol', ''),
                    'action_type': action.get('action_type', ''),
                    'dividend_per_share': action.get('dividend_per_share', 0),
                    'tax_rate': action.get('tax_rate', 0.10),
                    'bonus_ratio': action.get('bonus_ratio', 0),
                    'split_ratio': action.get('split_ratio', 1),
                    'rights_ratio': action.get('rights_ratio', 0),
                    'rights_price': action.get('rights_price', 0)
                })()
                
                # 处理公司行为事件
                self.handle_corporate_action(event)
            
            if self._context and corporate_actions:
                self._context.logger.info(f"处理了{len(corporate_actions)}个公司行为事件")

        except Exception as e:
            if self._context:
                self._context.logger.error(f"处理公司行为事件失败: {e}")

    # ========================================================================
    # 订单撮合方法（事件驱动接口）
    # ========================================================================

    def try_match_orders(self, event):
        """尝试撮合订单（事件驱动接口）

        Args:
            event: 包含市场数据的 TRY_MATCH 事件
        """
        try:
            if not hasattr(event, 'market_data'):
                if self._context:
                    self._context.logger.warning("TRY_MATCH 事件缺少 market_data")
                return

            market_data = event.market_data
            current_time = self._context.current_dt if self._context and hasattr(self._context, 'current_dt') else datetime.now()

            # 获取所有待撮合订单
            pending_orders = [
                order for order in self.orders.values()
                if order.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]
            ]

            if not pending_orders:
                return

            if self._context:
                self._context.logger.debug(f"[撮合] 开始处理 {len(pending_orders)} 个待撮合订单")

            # 处理每个订单
            for order in pending_orders:
                self._try_fill_order(order)

        except Exception as e:
            if self._context:
                self._context.logger.error(f"订单撮合失败: {e}")

    def try_match_orders_sync(self, market_data: Dict[str, Dict]):
        """尝试撮合订单（同步接口，供回测引擎直接调用）

        Args:
            market_data: 市场数据字典 {symbol: {field: value}}
        """
        try:
            current_time = self._context.current_dt if self._context and hasattr(self._context, 'current_dt') else datetime.now()

            # 更新价格缓存
            if market_data:
                self._price_cache = {
                    symbol: data.get('close', 0)
                    for symbol, data in market_data.items()
                }
                self._current_bar_time = current_time

            # 获取所有待撮合订单
            pending_orders = [
                order for order in self.orders.values()
                if order.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]
            ]

            if not pending_orders:
                return

            # 处理每个订单
            for order in pending_orders:
                self._try_fill_order(order)

        except Exception as e:
            if self._context:
                self._context.logger.error(f"订单撮合失败: {e}")

    def handle_order_submission(self, event):
        """处理订单提交事件

        Args:
            event: ORDER_SUBMISSION 事件
        """
        try:
            # 订单已在下单时提交到 self.orders
            # 这里可以添加额外的处理逻辑，如日志记录
            if self._context and hasattr(event, 'order_id'):
                order = self.orders.get(event.order_id)
                if order:
                    self._context.logger.debug(f"订单提交事件处理: {event.order_id} - {order.symbol} {order.side.value}")
        except Exception as e:
            if self._context:
                self._context.logger.error(f"处理订单提交事件失败: {e}")

    def handle_order_cancellation(self, event):
        """处理订单撤销事件

        Args:
            event: ORDER_CANCELLATION 事件
        """
        try:
            if hasattr(event, 'order_id'):
                order_id = event.order_id
                # 撤销订单
                if order_id in self.orders:
                    order = self.orders[order_id]
                    if order.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
                        order.status = OrderStatus.CANCELLED
                        order.updated_at = datetime.now()
                        if self._context:
                            self._context.logger.info(f"订单撤销成功: {order_id}")
                    else:
                        if self._context:
                            self._context.logger.warning(f"订单无法撤销，当前状态: {order.status.value}")
        except Exception as e:
            if self._context:
                self._context.logger.error(f"处理订单撤销事件失败: {e}")

    def cancel_pending_orders(self, event):
        """取消所有待处理订单（市场收盘时调用）

        Args:
            event: MARKET_END 事件
        """
        try:
            cancelled_count = 0
            for order in list(self.orders.values()):
                if order.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
                    order.status = OrderStatus.CANCELLED
                    order.updated_at = datetime.now()
                    cancelled_count += 1

            if self._context and cancelled_count > 0:
                self._context.logger.info(f"收盘取消待处理订单: {cancelled_count} 个")

        except Exception as e:
            if self._context:
                self._context.logger.error(f"取消待处理订单失败: {e}")

    def daily_settlement(self, event):
        """日终清算

        Args:
            event: MARKET_END 事件
        """
        try:
            # 更新持仓市值
            current_time = self._context.current_dt if self._context and hasattr(self._context, 'current_dt') else datetime.now()

            for position in self.positions.values():
                if position.last_price > 0:
                    position.market_value = position.quantity * position.last_price
                    position.updated_at = current_time

            # 计算账户总值
            if self._context:
                total_market_value = sum(pos.market_value for pos in self.positions.values())
                total_assets = self._context.account.cash + total_market_value

                self._context.logger.info(f"日终清算 - 总资产: {total_assets:.2f}, 现金: {self._context.account.cash:.2f}, 市值: {total_market_value:.2f}")

        except Exception as e:
            if self._context:
                self._context.logger.error(f"日终清算失败: {e}")