"""
订单模型定义

委托订单数据结构，包含订单的所有信息
"""

import dataclasses
from datetime import datetime
from typing import Optional, Dict, Any

from .enums import (
    Side, OrderType, PositionEffect, OrderStatus, 
    TimeInForceEnum, AssetTypeEnum
)


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
    
    def update_status(self, status: OrderStatus, reason: str = ""):
        """更新订单状态
        
        Args:
            status: 新状态
            reason: 状态变更原因
        """
        self.status = status
        self.updated_time = datetime.now()
        
        # 如果是拒绝状态，记录拒绝原因
        if status == OrderStatus.REJECTED and reason:
            self.rejected_reason = reason
    
    def add_fill(self, fill_volume: float, fill_price: float) -> float:
        """添加成交记录
        
        Args:
            fill_volume: 成交数量
            fill_price: 成交价格
            
        Returns:
            float: 本次成交金额
        """
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
        
        # 更新订单状态
        if self.filled_volume >= self.volume:
            self.status = OrderStatus.FILLED
        else:
            self.status = OrderStatus.PARTIALLY_FILLED
            
        self.updated_time = datetime.now()
        
        return fill_amount
    
    def cancel(self, reason: str = "用户撤单"):
        """撤销订单
        
        Args:
            reason: 撤单原因
        """
        if self.is_active:
            self.status = OrderStatus.CANCELLED
            self.rejected_reason = reason
            self.updated_time = datetime.now()
    
    def reject(self, reason: str):
        """拒绝订单
        
        Args:
            reason: 拒绝原因
        """
        self.status = OrderStatus.REJECTED
        self.rejected_reason = reason
        self.updated_time = datetime.now()
    
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
        """从字典创建Order对象"""
        data = data.copy()
        
        # 处理枚举类型
        if 'side' in data and isinstance(data['side'], str):
            data['side'] = Side(data['side'])
        if 'order_type' in data and isinstance(data['order_type'], str):
            data['order_type'] = OrderType(data['order_type'])
        if 'position_effect' in data and data['position_effect'] and isinstance(data['position_effect'], str):
            data['position_effect'] = PositionEffect(data['position_effect'])
        if 'status' in data and isinstance(data['status'], str):
            data['status'] = OrderStatus(data['status'])
        if 'asset_type' in data and data['asset_type'] and isinstance(data['asset_type'], str):
            data['asset_type'] = AssetTypeEnum(data['asset_type'])
        if 'time_in_force' in data and isinstance(data['time_in_force'], str):
            data['time_in_force'] = TimeInForceEnum(data['time_in_force'])
            
        # 处理时间字段
        if 'created_time' in data and isinstance(data['created_time'], str):
            data['created_time'] = datetime.fromisoformat(data['created_time'])
        if 'updated_time' in data and isinstance(data['updated_time'], str):
            data['updated_time'] = datetime.fromisoformat(data['updated_time'])
            
        return cls(**data) 