"""
数据模型定义模块

包含所有回测系统使用的数据结构：
- Account: 账户模型
- Order: 订单模型
- Trade: 成交模型
- Position: 持仓模型
- Instrument: 合约模型
- 以及各种枚举定义
"""

from .account import Account
from .order import Order
from .trade import Trade
from .position import Position
from .instrument import Instrument
from .enums import (
    PlatformEnum, ExchangeEnum, AccountTypeEnum, AssetTypeEnum,
    Side, OrderSide, PositionEffect, PositionSide, OrderType, OrderStatus,
    TimeInForceEnum, AccountStatusEnum, OptionTypeEnum,
    normalize_enum_value, validate_enum_value,
)

__all__ = [
    # 数据模型
    "Account",
    "Order",
    "Trade",
    "Position",
    "Instrument",

    # 枚举类型
    "PlatformEnum",
    "ExchangeEnum",
    "AccountTypeEnum",
    "AssetTypeEnum",
    "Side",
    "OrderSide",  # Side 的别名
    "PositionEffect",
    "PositionSide",
    "OrderType",
    "OrderStatus",
    "TimeInForceEnum",
    "AccountStatusEnum",
    "OptionTypeEnum",

    # 工具函数
    "normalize_enum_value",
    "validate_enum_value",
] 