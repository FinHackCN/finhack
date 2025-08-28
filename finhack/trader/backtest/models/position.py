"""
持仓模型定义

持仓信息数据结构，记录持仓的详细信息
"""

import dataclasses
from datetime import datetime
from typing import Optional

from .enums import PositionSide


@dataclasses.dataclass
class Position:
    """持仓信息模型"""
    
    # 必需字段
    account_id: str                 # 持仓所属账户
    symbol: str                     # 统一标的代码
    position_side: PositionSide     # 持仓方向
    volume: float                   # 总持仓量 (正数)

    # 可选/计算字段
    available_volume: float = 0.0   # 可用持仓量 (可平仓数量)
    cost_price: float = 0.0         # 持仓成本价
    market_value: float = 0.0       # 当前市值
    unrealized_pnl: float = 0.0     # 浮动盈亏
    margin_used: float = 0.0        # 占用保证金
    open_time: Optional[datetime] = None # 建仓时间
    close_today_volume: float = 0.0 # 今仓数量 (期货)
    close_yesterday_volume: float = 0.0 # 昨仓数量 (期货)
    last_price: float = 0.0         # 当前最新市场价格
    contract_multiplier: float = 1.0 # 合约乘数
    timestamp_updated: Optional[datetime] = None # 更新时间
    
    def __post_init__(self):
        """初始化后处理"""
        if self.open_time is None:
            self.open_time = datetime.now()
        if self.timestamp_updated is None:
            self.timestamp_updated = datetime.now()
        if self.available_volume == 0.0:
            self.available_volume = self.volume
    
    @property
    def is_long(self) -> bool:
        """是否为多头持仓"""
        return self.position_side == PositionSide.LONG
    
    @property
    def is_short(self) -> bool:
        """是否为空头持仓"""
        return self.position_side == PositionSide.SHORT
    
    @property
    def frozen_volume(self) -> float:
        """冻结持仓量"""
        return self.volume - self.available_volume
    
    @property
    def pnl_ratio(self) -> float:
        """盈亏比例"""
        if self.cost_price > 0:
            return self.unrealized_pnl / (self.cost_price * self.volume)
        return 0.0
    
    def update_market_price(self, price: float):
        """更新市场价格并重新计算市值和盈亏
        
        Args:
            price: 最新市场价格
        """
        self.last_price = price
        self.market_value = self.volume * price * self.contract_multiplier
        
        # 计算浮动盈亏
        if self.cost_price > 0:
            if self.is_long:
                self.unrealized_pnl = (price - self.cost_price) * self.volume * self.contract_multiplier
            else:  # 空头
                self.unrealized_pnl = (self.cost_price - price) * self.volume * self.contract_multiplier
        
        self.timestamp_updated = datetime.now()
    
    def add_position(self, volume: float, price: float) -> float:
        """增加持仓
        
        Args:
            volume: 增加的持仓量
            price: 成交价格
            
        Returns:
            float: 新的成本价
        """
        if volume <= 0:
            return self.cost_price
        
        # 计算新的成本价（加权平均）
        total_cost = self.cost_price * self.volume + price * volume
        total_volume = self.volume + volume
        
        self.cost_price = total_cost / total_volume if total_volume > 0 else price
        self.volume = total_volume
        self.available_volume += volume
        
        # 更新市值和盈亏
        if self.last_price > 0:
            self.update_market_price(self.last_price)
        else:
            self.market_value = self.volume * price * self.contract_multiplier
            
        self.timestamp_updated = datetime.now()
        return self.cost_price
    
    def reduce_position(self, volume: float) -> bool:
        """减少持仓
        
        Args:
            volume: 减少的持仓量
            
        Returns:
            bool: 是否成功减少
        """
        if volume <= 0 or volume > self.available_volume:
            return False
        
        self.volume -= volume
        self.available_volume -= volume
        
        # 如果持仓清零，重置成本价
        if self.volume <= 0:
            self.cost_price = 0.0
            self.volume = 0.0
            self.available_volume = 0.0
            self.market_value = 0.0
            self.unrealized_pnl = 0.0
        else:
            # 更新市值和盈亏
            if self.last_price > 0:
                self.update_market_price(self.last_price)
        
        self.timestamp_updated = datetime.now()
        return True
    
    def freeze_volume(self, volume: float) -> bool:
        """冻结持仓
        
        Args:
            volume: 需要冻结的持仓量
            
        Returns:
            bool: 是否冻结成功
        """
        if volume <= 0 or volume > self.available_volume:
            return False
        
        self.available_volume -= volume
        self.timestamp_updated = datetime.now()
        return True
    
    def unfreeze_volume(self, volume: float):
        """解冻持仓
        
        Args:
            volume: 需要解冻的持仓量
        """
        unfreeze_amount = min(volume, self.frozen_volume)
        self.available_volume += unfreeze_amount
        self.timestamp_updated = datetime.now()
    
    def to_dict(self) -> dict:
        """转换为字典格式"""
        return {
            'account_id': self.account_id,
            'symbol': self.symbol,
            'position_side': self.position_side.value,
            'volume': self.volume,
            'available_volume': self.available_volume,
            'cost_price': self.cost_price,
            'market_value': self.market_value,
            'unrealized_pnl': self.unrealized_pnl,
            'margin_used': self.margin_used,
            'open_time': self.open_time.isoformat() if self.open_time else None,
            'close_today_volume': self.close_today_volume,
            'close_yesterday_volume': self.close_yesterday_volume,
            'last_price': self.last_price,
            'contract_multiplier': self.contract_multiplier,
            'timestamp_updated': self.timestamp_updated.isoformat() if self.timestamp_updated else None
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Position':
        """从字典创建Position对象"""
        data = data.copy()
        
        # 处理枚举类型
        if 'position_side' in data and isinstance(data['position_side'], str):
            data['position_side'] = PositionSide(data['position_side'])
            
        # 处理时间字段
        if 'open_time' in data and isinstance(data['open_time'], str):
            data['open_time'] = datetime.fromisoformat(data['open_time'])
        if 'timestamp_updated' in data and isinstance(data['timestamp_updated'], str):
            data['timestamp_updated'] = datetime.fromisoformat(data['timestamp_updated'])
            
        return cls(**data)
    
    def __str__(self) -> str:
        """字符串表示"""
        return (f"Position(symbol={self.symbol}, side={self.position_side.value}, "
                f"volume={self.volume}, cost_price={self.cost_price}, "
                f"market_value={self.market_value}, pnl={self.unrealized_pnl})")
    
    def __repr__(self) -> str:
        """详细字符串表示"""
        return (f"Position(account_id={self.account_id}, symbol={self.symbol}, "
                f"side={self.position_side.value}, volume={self.volume}, "
                f"available={self.available_volume}, cost_price={self.cost_price})") 