"""
账户模型定义

账户信息数据结构，包含账户资金、状态等信息
"""

import dataclasses
from datetime import datetime
from typing import Optional

from .enums import PlatformEnum, AccountTypeEnum, AccountStatusEnum


@dataclasses.dataclass
class Account:
    """账户信息模型"""
    
    # 必需字段
    account_id: str                 # 账户唯一标识
    platform: PlatformEnum          # 交易平台/券商
    account_type: AccountTypeEnum    # 账户类型
    currency: str                   # 账户基础货币 (如 "CNY", "USD", "USDT")
    total_assets: float             # 总资产 (净值)
    cash_available: float           # 可用资金
    cash_frozen: float = 0.0        # 冻结资金
    market_value: float = 0.0       # 持仓市值总和
    pnl_unrealized: float = 0.0     # 浮动盈亏总和
    pnl_realized: float = 0.0       # 已实现盈亏总和
    status: AccountStatusEnum = AccountStatusEnum.DISCONNECTED # 账户状态
    timestamp_updated: Optional[datetime] = None # 账户数据最后更新时间

    # 可选字段
    margin_used: float = 0.0        # 已用保证金 (期货/保证金账户)
    margin_free: float = 0.0        # 可用保证金 (期货/保证金账户)
    risk_level: Optional[str] = None # 风险度/风险等级描述
    
    def __post_init__(self):
        """初始化后处理"""
        if self.timestamp_updated is None:
            self.timestamp_updated = datetime.now()
    
    def update_cash(self, amount: float, reason: str = ""):
        """更新现金
        
        Args:
            amount: 变动金额，正数为增加，负数为减少
            reason: 变动原因说明
        """
        self.cash_available += amount
        self.total_assets += amount
        self.timestamp_updated = datetime.now()
    
    def freeze_cash(self, amount: float) -> bool:
        """冻结资金
        
        Args:
            amount: 需要冻结的金额
            
        Returns:
            bool: 是否冻结成功
        """
        if self.cash_available >= amount:
            self.cash_available -= amount
            self.cash_frozen += amount
            self.timestamp_updated = datetime.now()
            return True
        return False
    
    def unfreeze_cash(self, amount: float):
        """解冻资金
        
        Args:
            amount: 需要解冻的金额
        """
        unfreeze_amount = min(amount, self.cash_frozen)
        self.cash_frozen -= unfreeze_amount
        self.cash_available += unfreeze_amount
        self.timestamp_updated = datetime.now()
    
    def update_market_value(self, market_value: float):
        """更新持仓市值
        
        Args:
            market_value: 新的持仓市值
        """
        old_market_value = self.market_value
        self.market_value = market_value
        self.total_assets = self.cash_available + self.cash_frozen + market_value
        self.timestamp_updated = datetime.now()
    
    def update_pnl(self, unrealized_pnl: float, realized_pnl: float = None):
        """更新盈亏
        
        Args:
            unrealized_pnl: 浮动盈亏
            realized_pnl: 已实现盈亏（可选）
        """
        self.pnl_unrealized = unrealized_pnl
        if realized_pnl is not None:
            self.pnl_realized += realized_pnl
        self.timestamp_updated = datetime.now()
    
    def to_dict(self) -> dict:
        """转换为字典格式"""
        return {
            'account_id': self.account_id,
            'platform': self.platform.value,
            'account_type': self.account_type.value,
            'currency': self.currency,
            'total_assets': self.total_assets,
            'cash_available': self.cash_available,
            'cash_frozen': self.cash_frozen,
            'market_value': self.market_value,
            'pnl_unrealized': self.pnl_unrealized,
            'pnl_realized': self.pnl_realized,
            'status': self.status.value,
            'timestamp_updated': self.timestamp_updated.isoformat() if self.timestamp_updated else None,
            'margin_used': self.margin_used,
            'margin_free': self.margin_free,
            'risk_level': self.risk_level
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Account':
        """从字典创建Account对象"""
        data = data.copy()
        
        # 处理枚举类型
        if 'platform' in data and isinstance(data['platform'], str):
            data['platform'] = PlatformEnum(data['platform'])
        if 'account_type' in data and isinstance(data['account_type'], str):
            data['account_type'] = AccountTypeEnum(data['account_type'])
        if 'status' in data and isinstance(data['status'], str):
            data['status'] = AccountStatusEnum(data['status'])
            
        # 处理时间字段
        if 'timestamp_updated' in data and isinstance(data['timestamp_updated'], str):
            data['timestamp_updated'] = datetime.fromisoformat(data['timestamp_updated'])
            
        return cls(**data) 