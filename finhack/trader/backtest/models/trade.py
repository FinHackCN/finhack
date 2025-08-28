"""
成交模型定义

成交记录数据结构，记录订单的成交信息
"""

import dataclasses
from datetime import datetime
from typing import Optional

from .enums import Side, PositionEffect


@dataclasses.dataclass
class Trade:
    """成交记录模型"""
    
    # 必需字段
    account_id: str                 # 成交所属账户
    symbol: str                     # 统一标的代码
    order_id: str                   # 对应的系统订单标识
    trade_id: str                   # 成交标识
    side: Side                      # 成交方向
    volume: float                   # 成交数量 (总是正数)
    price: float                    # 成交价格
    trade_time: Optional[datetime] = None # 成交时间

    # 计算字段
    amount: float = 0.0             # 成交金额 (price * volume * multiplier)

    # 可选字段
    position_effect: Optional[PositionEffect] = None # 持仓影响
    commission: float = 0.0         # 手续费
    commission_asset: Optional[str] = None # 手续费收取币种/资产
    tax: float = 0.0                # 税费
    is_maker: Optional[bool] = None # 是否为 Maker 成交
    
    def __post_init__(self):
        """初始化后处理"""
        if self.trade_time is None:
            self.trade_time = datetime.now()
        
        # 计算成交金额
        if self.amount == 0.0:
            self.amount = self.price * self.volume
    
    @property
    def is_buy(self) -> bool:
        """是否为买入成交"""
        return self.side == Side.BUY
    
    @property
    def is_sell(self) -> bool:
        """是否为卖出成交"""
        return self.side == Side.SELL
    
    @property
    def net_amount(self) -> float:
        """净成交金额（扣除费用后）"""
        total_cost = self.commission + self.tax
        if self.is_buy:
            return self.amount + total_cost  # 买入时费用增加成本
        else:
            return self.amount - total_cost  # 卖出时费用减少收入
    
    @property
    def total_cost(self) -> float:
        """总费用（手续费+税费）"""
        return self.commission + self.tax
    
    def calculate_commission(self, commission_rate: float, min_commission: float = 0.0) -> float:
        """计算手续费
        
        Args:
            commission_rate: 手续费率
            min_commission: 最低手续费
            
        Returns:
            float: 计算出的手续费
        """
        commission = self.amount * commission_rate
        self.commission = max(commission, min_commission)
        return self.commission
    
    def calculate_tax(self, tax_rate: float) -> float:
        """计算税费
        
        Args:
            tax_rate: 税率
            
        Returns:
            float: 计算出的税费
        """
        self.tax = self.amount * tax_rate
        return self.tax
    
    def calculate_fees(self, commission_rate: float, tax_rate: float = 0.0, 
                      min_commission: float = 0.0):
        """计算所有费用
        
        Args:
            commission_rate: 手续费率
            tax_rate: 税率
            min_commission: 最低手续费
        """
        self.calculate_commission(commission_rate, min_commission)
        self.calculate_tax(tax_rate)
    
    def to_dict(self) -> dict:
        """转换为字典格式"""
        return {
            'account_id': self.account_id,
            'symbol': self.symbol,
            'order_id': self.order_id,
            'trade_id': self.trade_id,
            'side': self.side.value,
            'volume': self.volume,
            'price': self.price,
            'trade_time': self.trade_time.isoformat() if self.trade_time else None,
            'amount': self.amount,
            'position_effect': self.position_effect.value if self.position_effect else None,
            'commission': self.commission,
            'commission_asset': self.commission_asset,
            'tax': self.tax,
            'is_maker': self.is_maker
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Trade':
        """从字典创建Trade对象"""
        data = data.copy()
        
        # 处理枚举类型
        if 'side' in data and isinstance(data['side'], str):
            data['side'] = Side(data['side'])
        if 'position_effect' in data and data['position_effect'] and isinstance(data['position_effect'], str):
            data['position_effect'] = PositionEffect(data['position_effect'])
            
        # 处理时间字段
        if 'trade_time' in data and isinstance(data['trade_time'], str):
            data['trade_time'] = datetime.fromisoformat(data['trade_time'])
            
        return cls(**data)
    
    def __str__(self) -> str:
        """字符串表示"""
        return (f"Trade(symbol={self.symbol}, side={self.side.value}, "
                f"volume={self.volume}, price={self.price}, amount={self.amount})")
    
    def __repr__(self) -> str:
        """详细字符串表示"""
        return (f"Trade(trade_id={self.trade_id}, order_id={self.order_id}, "
                f"symbol={self.symbol}, side={self.side.value}, "
                f"volume={self.volume}, price={self.price}, "
                f"trade_time={self.trade_time})") 