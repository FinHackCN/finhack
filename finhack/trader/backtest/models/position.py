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
    realized_pnl: float = 0.0       # 已实现盈亏
    margin_used: float = 0.0        # 占用保证金
    open_time: Optional[datetime] = None # 建仓时间
    close_today_volume: float = 0.0 # 今仓数量 (期货)
    close_yesterday_volume: float = 0.0 # 昨仓数量 (期货)
    last_price: float = 0.0         # 当前最新市场价格
    contract_multiplier: float = 1.0 # 合约乘数
    timestamp_updated: Optional[datetime] = None # 更新时间

    # T+1 规则相关字段
    buy_dates: list = None          # 记录每批买入的日期和数量 [(datetime, volume), ...]
    
    def __post_init__(self):
        """初始化后处理"""
        if self.open_time is None:
            self.open_time = datetime.now()
        if self.timestamp_updated is None:
            self.timestamp_updated = datetime.now()
        if self.available_volume == 0.0 and self.volume > 0:
            self.available_volume = self.volume
        # 初始化 T+1 规则相关字段
        if self.buy_dates is None:
            self.buy_dates = []
    
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

        return True

    # ========== 兼容属性（兼容 core/trade_center.py 的字段名） ==========

    @property
    def quantity(self) -> float:
        """兼容字段：总持仓量（同 volume）"""
        return self.volume

    @quantity.setter
    def quantity(self, value: float):
        """设置持仓量（同时更新兼容字段）"""
        self.volume = value

    @property
    def available_quantity(self) -> float:
        """兼容字段：可用持仓量（同 available_volume）"""
        return self.available_volume

    @available_quantity.setter
    def available_quantity(self, value: float):
        """设置可用持仓量（同时更新兼容字段）"""
        self.available_volume = value

    @property
    def avg_cost(self) -> float:
        """兼容字段：平均成本（同 cost_price）"""
        return self.cost_price

    @avg_cost.setter
    def avg_cost(self, value: float):
        """设置平均成本（同时更新兼容字段）"""
        self.cost_price = value

    @property
    def amount(self) -> float:
        """兼容字段：总数量（同 volume）"""
        return self.volume

    @property
    def enable_amount(self) -> float:
        """兼容字段：可用数量（同 available_volume）"""
        return self.available_volume

    @property
    def cost_basis(self) -> float:
        """兼容字段：成本基础（同 cost_price）"""
        return self.cost_price

    @property
    def last_sale_price(self) -> float:
        """兼容字段：最新售价（同 last_price）"""
        return self.last_price

    @last_sale_price.setter
    def last_sale_price(self, value: float):
        """设置最新售价（同时更新 last_price）"""
        self.last_price = value

    @property
    def total_value(self) -> float:
        """兼容字段：总市值（同 market_value）"""
        return self.market_value

    @property
    def total_cost(self) -> float:
        """兼容字段：总成本"""
        return self.cost_price * self.volume

    @property
    def updated_at(self) -> Optional[datetime]:
        """兼容字段：更新时间（同 timestamp_updated）"""
        return self.timestamp_updated

    @updated_at.setter
    def updated_at(self, value: Optional[datetime]):
        """设置更新时间"""
        self.timestamp_updated = value

    def get_sellable_quantity(self, current_date: datetime) -> float:
        """
        获取可卖出数量（考虑T+1规则）

        Args:
            current_date: 当前日期时间

        Returns:
            可卖出数量
        """
        sellable = 0.0
        for buy_time, qty in self.buy_dates:
            # 【修复】确保时区一致后再比较日期
            # 将两个时间都转换为naive datetime或都转换为aware datetime
            buy_date_for_compare = buy_time
            current_date_for_compare = current_date

            # 如果buy_time有时区信息但current_date没有，去除buy_time的时区
            if buy_time.tzinfo is not None and current_date.tzinfo is None:
                buy_date_for_compare = buy_time.replace(tzinfo=None)
            # 如果current_date有时区信息但buy_time没有，去除current_date的时区
            elif current_date.tzinfo is not None and buy_time.tzinfo is None:
                current_date_for_compare = current_date.replace(tzinfo=None)
            # 如果两者都有时区但不同，统一转换为UTC
            elif buy_time.tzinfo is not None and current_date.tzinfo is not None:
                if buy_time.tzinfo != current_date.tzinfo:
                    buy_date_for_compare = buy_time.astimezone(current_date.tzinfo)

            # 判断是否是昨日及之前买入的（T+1：当日买入不可卖）
            if buy_date_for_compare and buy_date_for_compare.date() < current_date_for_compare.date():
                sellable += qty
        return sellable

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
        """从字典创建Position对象（类型安全）"""
        data = data.copy()

        # 处理枚举类型 - 使用 from_value 进行类型安全转换
        if 'position_side' in data and isinstance(data['position_side'], str):
            data['position_side'] = PositionSide.from_value(data['position_side'])

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