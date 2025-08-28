"""
合约/标的信息模型定义

包含交易标的的基本信息和交易规则
"""

import dataclasses
from typing import Optional

from .enums import AssetTypeEnum, OptionTypeEnum


@dataclasses.dataclass
class Instrument:
    """合约/标的信息模型"""
    
    # 必需字段
    symbol: str                     # 统一标的代码
    exchange: str                   # 交易所代码
    asset_type: AssetTypeEnum       # 资产类型
    name: str                       # 标的名称
    currency: str                   # 交易货币

    # 可选字段
    contract_multiplier: float = 1.0 # 合约乘数
    tick_size: float = 0.0          # 最小价格变动单位
    lot_size: float = 1.0           # 每手数量
    min_order_volume: float = 0.0   # 最小委托数量
    max_order_volume: float = 0.0   # 最大委托数量
    volume_step: float = 0.0        # 委托数量步长
    margin_ratio: float = 0.0       # 基础保证金比例
    expiry_date: Optional[str] = None # 到期日
    underlying_symbol: Optional[str] = None # 标的资产代码
    is_active: bool = True          # 合约是否活跃/可交易
    option_type: OptionTypeEnum = OptionTypeEnum.NONE # 期权类型
    strike_price: float = 0.0       # 行权价格
    
    @property
    def is_stock(self) -> bool:
        """是否为股票"""
        return self.asset_type == AssetTypeEnum.STOCK
    
    @property
    def is_future(self) -> bool:
        """是否为期货"""
        return self.asset_type == AssetTypeEnum.FUTURE
    
    @property
    def is_option(self) -> bool:
        """是否为期权"""
        return self.asset_type == AssetTypeEnum.OPTION
    
    @property
    def is_crypto(self) -> bool:
        """是否为数字货币"""
        return self.asset_type == AssetTypeEnum.CRYPTO
    
    @property
    def is_fx(self) -> bool:
        """是否为外汇"""
        return self.asset_type == AssetTypeEnum.FX
    
    @property
    def is_call_option(self) -> bool:
        """是否为看涨期权"""
        return self.is_option and self.option_type == OptionTypeEnum.CALL
    
    @property
    def is_put_option(self) -> bool:
        """是否为看跌期权"""
        return self.is_option and self.option_type == OptionTypeEnum.PUT
    
    def normalize_price(self, price: float) -> float:
        """标准化价格到最小变动单位
        
        Args:
            price: 原始价格
            
        Returns:
            float: 标准化后的价格
        """
        if self.tick_size > 0:
            return round(price / self.tick_size) * self.tick_size
        return price
    
    def normalize_volume(self, volume: float) -> float:
        """标准化委托数量
        
        Args:
            volume: 原始数量
            
        Returns:
            float: 标准化后的数量
        """
        # 首先处理手数
        if self.lot_size > 1:
            volume = round(volume / self.lot_size) * self.lot_size
        
        # 然后处理步长
        if self.volume_step > 0:
            volume = round(volume / self.volume_step) * self.volume_step
        
        return volume
    
    def validate_order_volume(self, volume: float) -> tuple[bool, str]:
        """验证委托数量是否合法
        
        Args:
            volume: 委托数量
            
        Returns:
            tuple[bool, str]: (是否合法, 错误信息)
        """
        if volume <= 0:
            return False, "委托数量必须大于0"
        
        if self.min_order_volume > 0 and volume < self.min_order_volume:
            return False, f"委托数量不能小于最小数量 {self.min_order_volume}"
        
        if self.max_order_volume > 0 and volume > self.max_order_volume:
            return False, f"委托数量不能大于最大数量 {self.max_order_volume}"
        
        # 检查数量步长
        if self.volume_step > 0:
            remainder = volume % self.volume_step
            if abs(remainder) > 1e-8:  # 考虑浮点精度
                return False, f"委托数量必须是 {self.volume_step} 的整数倍"
        
        # 检查手数
        if self.lot_size > 1:
            remainder = volume % self.lot_size
            if abs(remainder) > 1e-8:
                return False, f"委托数量必须是 {self.lot_size} 手的整数倍"
        
        return True, ""
    
    def calculate_margin(self, volume: float, price: float) -> float:
        """计算保证金需求
        
        Args:
            volume: 持仓数量
            price: 价格
            
        Returns:
            float: 保证金金额
        """
        if self.margin_ratio > 0:
            return volume * price * self.contract_multiplier * self.margin_ratio
        return 0.0
    
    def to_dict(self) -> dict:
        """转换为字典格式"""
        return {
            'symbol': self.symbol,
            'exchange': self.exchange,
            'asset_type': self.asset_type.value,
            'name': self.name,
            'currency': self.currency,
            'contract_multiplier': self.contract_multiplier,
            'tick_size': self.tick_size,
            'lot_size': self.lot_size,
            'min_order_volume': self.min_order_volume,
            'max_order_volume': self.max_order_volume,
            'volume_step': self.volume_step,
            'margin_ratio': self.margin_ratio,
            'expiry_date': self.expiry_date,
            'underlying_symbol': self.underlying_symbol,
            'is_active': self.is_active,
            'option_type': self.option_type.value,
            'strike_price': self.strike_price
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Instrument':
        """从字典创建Instrument对象"""
        data = data.copy()
        
        # 处理枚举类型
        if 'asset_type' in data and isinstance(data['asset_type'], str):
            data['asset_type'] = AssetTypeEnum(data['asset_type'])
        if 'option_type' in data and isinstance(data['option_type'], str):
            data['option_type'] = OptionTypeEnum(data['option_type'])
            
        return cls(**data)
    
    def __str__(self) -> str:
        """字符串表示"""
        return f"Instrument(symbol={self.symbol}, name={self.name}, type={self.asset_type.value})"
    
    def __repr__(self) -> str:
        """详细字符串表示"""
        return (f"Instrument(symbol={self.symbol}, exchange={self.exchange}, "
                f"asset_type={self.asset_type.value}, name={self.name})") 