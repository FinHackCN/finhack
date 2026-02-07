"""
订单模型定义

委托订单数据结构，包含订单的所有信息
"""

import dataclasses
import logging
from datetime import datetime
from typing import Optional, Dict, Any, Set, Tuple

from .enums import (
    Side, OrderType, PositionEffect, OrderStatus,
    TimeInForceEnum, AssetTypeEnum
)

logger = logging.getLogger(__name__)


# 订单状态转换矩阵：定义合法的状态转换
# 格式: {当前状态: {可转换到的状态集合}}
_ORDER_STATE_TRANSITIONS: Dict[OrderStatus, Set[OrderStatus]] = {
    OrderStatus.PENDING_NEW: {
        OrderStatus.NEW,           # 提交成功
        OrderStatus.REJECTED,      # 被拒绝
    },
    OrderStatus.NEW: {
        OrderStatus.PARTIALLY_FILLED,  # 部分成交
        OrderStatus.FILLED,            # 完全成交
        OrderStatus.CANCELLED,         # 用户撤销
        OrderStatus.EXPIRED,           # 过期
        OrderStatus.REJECTED,          # 系统拒绝（特殊情况）
    },
    OrderStatus.PARTIALLY_FILLED: {
        OrderStatus.PARTIALLY_FILLED,  # 继续部分成交
        OrderStatus.FILLED,            # 完全成交
        OrderStatus.CANCELLED,         # 用户撤销剩余部分
        OrderStatus.EXPIRED,           # 过期
    },
    # 终态不允许转换到任何其他状态
    OrderStatus.FILLED: set(),
    OrderStatus.CANCELLED: set(),
    OrderStatus.REJECTED: set(),
    OrderStatus.EXPIRED: set(),
}


@dataclasses.dataclass
class Order:
    """委托订单模型"""

    # 必需字段
    account_id: str                 # 委托所属账户
    symbol: str                     # 统一标的代码
    side: Side                      # 委托方向
    order_type: OrderType           # 委托类型
    volume: float                   # 委托数量 (总是正数)
    order_id: str                   # 系统生成的唯一委托订单标识

    # 可选字段
    price: Optional[float] = None   # 委托价格 (限价单必填)
    market_price: Optional[float] = None  # 市价单的固定成交价（下单时获取并固定）
    position_effect: Optional[PositionEffect] = None # 开平仓类型
    client_order_id: Optional[str] = None # 用户自定义订单标识
    broker_order_id: Optional[str] = None # 券商/交易所订单标识
    status: OrderStatus = OrderStatus.PENDING_NEW # 订单状态
    filled_volume: float = 0.0      # 已成交数量
    filled_amount: float = 0.0      # 已成交金额
    avg_fill_price: float = 0.0     # 平均成交价
    created_time: Optional[datetime] = None # 订单创建时间
    updated_time: Optional[datetime] = None # 订单最后更新时间
    rejected_reason: Optional[str] = None # 拒绝原因描述
    asset_type: Optional[AssetTypeEnum] = None # 资产类型
    time_in_force: TimeInForceEnum = TimeInForceEnum.DAY # 委托有效期
    strategy_id: Optional[str] = None # 关联的策略ID
    order_tag: Optional[str] = None   # 用户自定义标签/备注
    stop_price: float = 0.0          # 触发价格（止损单）
    kwargs: Dict[str, Any] = dataclasses.field(default_factory=dict) # 扩展参数

    def __post_init__(self):
        """初始化后处理"""
        if self.created_time is None:
            self.created_time = datetime.now()
        if self.updated_time is None:
            self.updated_time = self.created_time

    def _is_valid_transition(self, new_status: OrderStatus) -> bool:
        """检查状态转换是否合法

        Args:
            new_status: 新状态

        Returns:
            bool: 转换是否合法
        """
        current_status = self.status
        allowed_transitions = _ORDER_STATE_TRANSITIONS.get(current_status, set())
        return new_status in allowed_transitions
    
    @property
    def remaining_volume(self) -> float:
        """剩余未成交数量"""
        return self.volume - self.filled_volume
    
    @property
    def fill_ratio(self) -> float:
        """成交比例"""
        return self.filled_volume / self.volume if self.volume > 0 else 0.0
    
    @property
    def is_buy(self) -> bool:
        """是否为买单"""
        return self.side == Side.BUY
    
    @property
    def is_sell(self) -> bool:
        """是否为卖单"""
        return self.side == Side.SELL
    
    @property
    def is_limit(self) -> bool:
        """是否为限价单"""
        return self.order_type == OrderType.LIMIT
    
    @property
    def is_market(self) -> bool:
        """是否为市价单"""
        return self.order_type == OrderType.MARKET
    
    @property
    def is_active(self) -> bool:
        """订单是否处于活跃状态（可成交）"""
        return self.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]
    
    @property
    def is_finished(self) -> bool:
        """订单是否已结束"""
        return self.status in [
            OrderStatus.FILLED, OrderStatus.CANCELLED, 
            OrderStatus.REJECTED, OrderStatus.EXPIRED
        ]
    
    def update_status(self, status: OrderStatus, reason: str = "", force: bool = False) -> bool:
        """更新订单状态（带状态转换验证）

        Args:
            status: 新状态
            reason: 状态变更原因
            force: 是否强制转换（跳过验证，仅用于特殊情况）

        Returns:
            bool: 是否成功更新状态

        Raises:
            ValueError: 如果状态转换不合法且不是强制转换
        """
        current_status = self.status

        # 如果是同一状态，直接返回成功
        if current_status == status:
            return True

        # 检查状态转换是否合法
        if not force and not self._is_valid_transition(status):
            error_msg = (f"非法的订单状态转换: {current_status.value} -> {status.value}. "
                        f"订单ID: {self.order_id}, 标的: {self.symbol}")
            logger.error(error_msg)
            raise ValueError(error_msg)

        # 执行状态转换
        old_status = self.status
        self.status = status
        self.updated_time = datetime.now()

        # 如果是拒绝状态，记录拒绝原因
        if status == OrderStatus.REJECTED and reason:
            self.rejected_reason = reason

        logger.debug(f"订单状态变更: {self.order_id} {old_status.value} -> {status.value}{f' (原因: {reason})' if reason else ''}")
        return True
    
    def add_fill(self, fill_volume: float, fill_price: float) -> float:
        """添加成交记录（带状态转换验证）

        Args:
            fill_volume: 成交数量
            fill_price: 成交价格

        Returns:
            float: 本次成交金额

        Raises:
            ValueError: 如果订单状态不允许成交
        """
        # 检查订单是否可以成交
        if not self.is_active:
            raise ValueError(f"订单 {self.order_id} 状态为 {self.status.value}，不允许成交")

        # 确保成交数量不超过剩余数量
        actual_fill_volume = min(fill_volume, self.remaining_volume)
        fill_amount = actual_fill_volume * fill_price

        # 更新累计成交数据
        total_amount = self.filled_amount + fill_amount
        total_volume = self.filled_volume + actual_fill_volume

        # 计算新的平均成交价
        self.avg_fill_price = total_amount / total_volume if total_volume > 0 else 0.0
        self.filled_volume = total_volume
        self.filled_amount = total_amount

        # 更新订单状态（使用带验证的方法）
        if self.filled_volume >= self.volume:
            self.update_status(OrderStatus.FILLED, reason="完全成交")
        else:
            self.update_status(OrderStatus.PARTIALLY_FILLED, reason=f"部分成交 {actual_fill_volume}/{self.volume}")

        self.updated_time = datetime.now()

        return fill_amount
    
    def cancel(self, reason: str = "用户撤单") -> bool:
        """撤销订单（带状态转换验证）

        Args:
            reason: 撤单原因

        Returns:
            bool: 是否成功撤销

        Raises:
            ValueError: 如果订单状态不允许撤销
        """
        if not self.is_active:
            raise ValueError(f"订单 {self.order_id} 状态为 {self.status.value}，不允许撤销")

        return self.update_status(OrderStatus.CANCELLED, reason=reason)

    def reject(self, reason: str) -> bool:
        """拒绝订单（带状态转换验证）

        Args:
            reason: 拒绝原因

        Returns:
            bool: 是否成功拒绝

        Raises:
            ValueError: 如果订单状态不允许拒绝
        """
        # 只有 PENDING_NEW 和 NEW 状态可以被拒绝
        if self.status not in [OrderStatus.PENDING_NEW, OrderStatus.NEW]:
            raise ValueError(f"订单 {self.order_id} 状态为 {self.status.value}，不允许拒绝")

        return self.update_status(OrderStatus.REJECTED, reason=reason)
    
    def to_dict(self) -> dict:
        """转换为字典格式"""
        return {
            'account_id': self.account_id,
            'symbol': self.symbol,
            'side': self.side.value,
            'order_type': self.order_type.value,
            'volume': self.volume,
            'order_id': self.order_id,
            'price': self.price,
            'position_effect': self.position_effect.value if self.position_effect else None,
            'client_order_id': self.client_order_id,
            'broker_order_id': self.broker_order_id,
            'status': self.status.value,
            'filled_volume': self.filled_volume,
            'filled_amount': self.filled_amount,
            'avg_fill_price': self.avg_fill_price,
            'created_time': self.created_time.isoformat() if self.created_time else None,
            'updated_time': self.updated_time.isoformat() if self.updated_time else None,
            'rejected_reason': self.rejected_reason,
            'asset_type': self.asset_type.value if self.asset_type else None,
            'time_in_force': self.time_in_force.value,
            'strategy_id': self.strategy_id,
            'order_tag': self.order_tag,
            'stop_price': self.stop_price,
            'kwargs': self.kwargs
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Order':
        """从字典创建Order对象（类型安全）"""
        data = data.copy()

        # 处理枚举类型 - 使用 from_value 进行类型安全转换
        if 'side' in data and isinstance(data['side'], str):
            data['side'] = Side.from_value(data['side'])
        if 'order_type' in data and isinstance(data['order_type'], str):
            data['order_type'] = OrderType.from_value(data['order_type'])
        if 'position_effect' in data and data['position_effect'] and isinstance(data['position_effect'], str):
            data['position_effect'] = PositionEffect.from_value(data['position_effect'])
        if 'status' in data and isinstance(data['status'], str):
            data['status'] = OrderStatus.from_value(data['status'])
        if 'asset_type' in data and data['asset_type'] and isinstance(data['asset_type'], str):
            data['asset_type'] = AssetTypeEnum.from_value(data['asset_type'])
        if 'time_in_force' in data and isinstance(data['time_in_force'], str):
            data['time_in_force'] = TimeInForceEnum.from_value(data['time_in_force'])

        # 处理时间字段
        if 'created_time' in data and isinstance(data['created_time'], str):
            data['created_time'] = datetime.fromisoformat(data['created_time'])
        if 'updated_time' in data and isinstance(data['updated_time'], str):
            data['updated_time'] = datetime.fromisoformat(data['updated_time'])

        return cls(**data) 