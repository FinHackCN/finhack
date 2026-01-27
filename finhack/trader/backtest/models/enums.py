"""
回测系统枚举定义

定义了回测系统中使用的所有枚举类型，包括：
- 交易平台、交易所、账户类型
- 资产类型、订单类型、订单状态
- 持仓方向、委托方向等

设计原则：
1. 所有枚举值使用大写字符串（符合金融行业标准）
2. 提供类型安全的枚举转换方法
3. 支持向后兼容的小写值转换
4. 提供中文显示名称
5. 支持JSON序列化/反序列化
"""

from __future__ import annotations

from enum import Enum, EnumMeta
from typing import Any, Dict, List, Optional, Type, TypeVar, Union
from functools import lru_cache


class BaseEnumMeta(EnumMeta):
    """枚举元类，提供额外的类型转换功能"""

    def __getitem__(self, key: str) -> 'BaseEnum':
        """支持大小写不敏感的枚举访问"""
        try:
            return super().__getitem__(key.upper())
        except (KeyError, AttributeError):
            # 尝试通过值查找
            for member in self:
                if member.value == key or member.value.lower() == key.lower():
                    return member
            raise ValueError(f"{self.__name__} 中没有找到枚举值: {key}")

    def from_value(cls, value: Any, default: Optional['BaseEnum'] = None) -> Optional['BaseEnum']:
        """
        从值获取枚举成员（类型安全的转换方法）

        Args:
            value: 可以是枚举成员、枚举值字符串或任何可比较的值
            default: 当找不到时返回的默认值，如果为None则抛出异常

        Returns:
            对应的枚举成员，如果未找到且指定了default则返回default

        Raises:
            ValueError: 当找不到对应枚举且未指定default时
        """
        if value is None:
            return default

        # 如果已经是枚举成员，直接返回
        if isinstance(value, cls):
            return value

        # 转换为字符串进行比较
        value_str = str(value).upper()

        # 先尝试通过枚举名查找
        try:
            return cls[value_str]
        except (KeyError, AttributeError):
            pass

        # 尝试通过值查找
        for member in cls:
            if member.value == value or member.value.upper() == value_str:
                return member
            # 支持小写值向后兼容
            if member.value.lower() == value_str.lower():
                return member

        if default is not None:
            return default

        raise ValueError(f"{cls.__name__} 中没有找到与 '{value}' 匹配的枚举值")

    def values(cls) -> List[str]:
        """获取所有枚举值的列表"""
        return [member.value for member in cls]

    def names(cls) -> List[str]:
        """获取所有枚举名称的列表"""
        return [member.name for member in cls]

    def members(cls) -> Dict[str, 'BaseEnum']:
        """获取所有枚举成员的字典"""
        return {member.name: member for member in cls}


class BaseEnum(str, Enum, metaclass=BaseEnumMeta):
    """
    基础枚举类，所有枚举都应继承此类

    特性：
    1. 继承自str，支持字符串操作
    2. 使用自定义元类，提供类型安全的转换
    3. 支持大小写不敏感的值匹配
    """

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}.{self.name}"

    @classmethod
    def from_value(cls, value: Any, default: Optional['BaseEnum'] = None) -> Optional['BaseEnum']:
        """从值获取枚举成员（类型安全的转换方法）"""
        return cls.__class__.from_value(value, default)

    @property
    def label(self) -> str:
        """获取枚举的中文显示标签"""
        return getattr(self, '_label', self.name)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，用于序列化"""
        return {
            'name': self.name,
            'value': self.value,
            'label': self.label,
        }


T = TypeVar('T', bound=BaseEnum)


# ============================================================================
# 平台与交易所相关枚举
# ============================================================================


class PlatformEnum(BaseEnum):
    """交易平台枚举

    支持的交易平台：
    - 国内券商平台：QMT、PTRADE、JUEJIN、TQSDK、CTP
    - 国际平台：BINANCE、IBKR
    - 测试环境：SIMULATION、BACKTEST
    """

    QMT = "QMT"
    PTRADE = "PTRADE"
    JUEJIN = "JUEJIN"
    TQSDK = "TQSDK"
    BINANCE = "BINANCE"
    IBKR = "IBKR"
    CTP = "CTP"
    SIMULATION = "SIMULATION"   # 模拟盘环境
    BACKTEST = "BACKTEST"       # 回测环境
    OTHER = "OTHER"

    # 分类属性
    @property
    def is_real_trading(self) -> bool:
        """是否为实盘交易平台"""
        return self in {
            PlatformEnum.QMT,
            PlatformEnum.PTRADE,
            PlatformEnum.JUEJIN,
            PlatformEnum.TQSDK,
            PlatformEnum.BINANCE,
            PlatformEnum.IBKR,
            PlatformEnum.CTP,
        }

    @property
    def is_simulation(self) -> bool:
        """是否为模拟/回测环境"""
        return self in {
            PlatformEnum.SIMULATION,
            PlatformEnum.BACKTEST,
        }


class ExchangeEnum(BaseEnum):
    """交易所枚举

    支持的交易所分类：
    - 中国市场：上交所(SSE)、深交所(SZSE)、期货交易所
    - 香港市场：港交所(HKEX)
    - 美国市场：纽交所(NYSE)、纳斯达克(NASDAQ)
    - 国际期货：CME、ICE、LME
    """

    # 中国市场
    SSE = "SSE"           # 上海证券交易所
    SZSE = "SZSE"         # 深圳证券交易所
    SHFE = "SHFE"         # 上海期货交易所
    DCE = "DCE"           # 大连商品交易所
    CZCE = "CZCE"         # 郑州商品交易所
    CFFEX = "CFFEX"       # 中国金融期货交易所
    INE = "INE"           # 上海国际能源交易中心

    # 香港市场
    HKEX = "HKEX"         # 香港交易所

    # 美国市场
    NYSE = "NYSE"         # 纽约证券交易所
    NASDAQ = "NASDAQ"     # 纳斯达克交易所

    # 国际期货
    CME = "CME"           # 芝加哥商品交易所
    ICE = "ICE"           # 洲际交易所
    LME = "LME"           # 伦敦金属交易所

    OTHER = "OTHER"

    # 分类方法
    @classmethod
    def get_cn_exchanges(cls) -> List['ExchangeEnum']:
        """获取中国交易所列表"""
        return [
            ExchangeEnum.SSE, ExchangeEnum.SZSE,
            ExchangeEnum.SHFE, ExchangeEnum.DCE,
            ExchangeEnum.CZCE, ExchangeEnum.CFFEX,
            ExchangeEnum.INE,
        ]

    @classmethod
    def get_us_exchanges(cls) -> List['ExchangeEnum']:
        """获取美国交易所列表"""
        return [ExchangeEnum.NYSE, ExchangeEnum.NASDAQ]

    @classmethod
    def get_futures_exchanges(cls) -> List['ExchangeEnum']:
        """获取期货交易所列表"""
        return [
            ExchangeEnum.SHFE, ExchangeEnum.DCE,
            ExchangeEnum.CZCE, ExchangeEnum.CFFEX,
            ExchangeEnum.INE, ExchangeEnum.CME,
            ExchangeEnum.ICE, ExchangeEnum.LME,
        ]

    @property
    def is_cn(self) -> bool:
        """是否为中国交易所"""
        return self in self.get_cn_exchanges()

    @property
    def is_us(self) -> bool:
        """是否为美国交易所"""
        return self in self.get_us_exchanges()

    @property
    def is_futures(self) -> bool:
        """是否为期货交易所"""
        return self in self.get_futures_exchanges()


# ============================================================================
# 账户相关枚举
# ============================================================================


class AccountTypeEnum(BaseEnum):
    """账户类型枚举

    账户类型说明：
    - CASH: 现金账户，只能使用自有资金交易，无杠杆
    - MARGIN: 保证金账户，可以融资融券
    - FUTURES: 期货账户，用于期货交易
    - OPTION: 期权账户，用于期权交易
    - CRYPTO: 数字货币账户
    - UNIFIED: 综合账户，支持多种资产类型
    - CREDIT: 信用账户
    """

    CASH = "CASH"           # 现金账户
    MARGIN = "MARGIN"       # 保证金账户
    FUTURES = "FUTURES"     # 期货账户
    OPTION = "OPTION"       # 期权账户
    CRYPTO = "CRYPTO"       # 数字货币账户
    UNIFIED = "UNIFIED"     # 综合账户
    CREDIT = "CREDIT"       # 信用账户
    OTHER = "OTHER"

    @property
    def supports_leverage(self) -> bool:
        """是否支持杠杆交易"""
        return self in {
            AccountTypeEnum.MARGIN,
            AccountTypeEnum.FUTURES,
            AccountTypeEnum.OPTION,
            AccountTypeEnum.CREDIT,
        }

    @property
    def supports_short(self) -> bool:
        """是否支持做空"""
        return self in {
            AccountTypeEnum.MARGIN,
            AccountTypeEnum.FUTURES,
            AccountTypeEnum.OPTION,
            AccountTypeEnum.CREDIT,
        }


class AccountStatusEnum(BaseEnum):
    """账户状态枚举

    状态流转：
    CONNECTING -> CONNECTED -> AUTHENTICATED -> INITIALIZING -> DATA_READY
                |                                         |
                v                                         v
            FAILED/ERROR <---------------------------- DISCONNECTED
    """

    CONNECTING = "CONNECTING"           # 连接中
    CONNECTED = "CONNECTED"             # 已连接
    AUTHENTICATED = "AUTHENTICATED"     # 已认证
    INITIALIZING = "INITIALIZING"       # 初始化中
    DATA_READY = "DATA_READY"           # 数据就绪，可以交易
    DISCONNECTED = "DISCONNECTED"       # 已断开
    FAILED = "FAILED"                   # 连接/认证失败
    ERROR = "ERROR"                     # 运行时错误
    OTHER = "OTHER"

    @property
    def is_active(self) -> bool:
        """是否为活跃状态（可以进行交易）"""
        return self == AccountStatusEnum.DATA_READY

    @property
    def is_connecting(self) -> bool:
        """是否正在连接"""
        return self in {
            AccountStatusEnum.CONNECTING,
            AccountStatusEnum.CONNECTED,
            AccountStatusEnum.AUTHENTICATED,
            AccountStatusEnum.INITIALIZING,
        }

    @property
    def is_error(self) -> bool:
        """是否为错误状态"""
        return self in {
            AccountStatusEnum.FAILED,
            AccountStatusEnum.ERROR,
        }


# ============================================================================
# 资产与交易相关枚举
# ============================================================================


class AssetTypeEnum(BaseEnum):
    """资产类型枚举

    资产类型分类：
    - 权益类：STOCK、FUND
    - 衍生品类：FUTURE、OPTION、WARRANT、COMBO
    - 其他：FX、CRYPTO、BOND
    """

    STOCK = "STOCK"         # 股票
    FUTURE = "FUTURE"       # 期货
    OPTION = "OPTION"       # 期权
    FX = "FX"               # 外汇
    CRYPTO = "CRYPTO"       # 数字货币
    FUND = "FUND"           # 基金
    BOND = "BOND"           # 债券
    WARRANT = "WARRANT"     # 权证
    COMBO = "COMBO"         # 组合合约
    OTHER = "OTHER"

    @property
    def is_derivative(self) -> bool:
        """是否为衍生品"""
        return self in {
            AssetTypeEnum.FUTURE,
            AssetTypeEnum.OPTION,
            AssetTypeEnum.WARRANT,
            AssetTypeEnum.COMBO,
        }

    @property
    def supports_short(self) -> bool:
        """是否支持做空"""
        return self in {
            AssetTypeEnum.FUTURE,
            AssetTypeEnum.OPTION,
            AssetTypeEnum.FX,
            AssetTypeEnum.CRYPTO,
        }

    @property
    def supports_margin(self) -> bool:
        """是否支持保证金交易"""
        return self in {
            AssetTypeEnum.FUTURE,
            AssetTypeEnum.OPTION,
            AssetTypeEnum.FX,
        }


class Side(BaseEnum):
    """委托方向枚举

    交易方向说明：
    - BUY: 买入/做多
    - SELL: 卖出/做空
    """

    BUY = "BUY"   # 买入
    SELL = "SELL" # 卖出

    @property
    def opposite(self) -> 'Side':
        """获取相反的方向"""
        return Side.SELL if self == Side.BUY else Side.BUY


class PositionEffect(BaseEnum):
    """开平仓类型枚举

    主要用于期货和期权交易：
    - OPEN: 开仓，建立新头寸
    - CLOSE: 平仓，关闭现有头寸
    - CLOSETODAY: 平今仓（平当日开仓的头寸）
    - CLOSEYESTERDAY: 平昨仓（平昨日之前的头寸）
    """

    OPEN = "OPEN"                        # 开仓
    CLOSE = "CLOSE"                      # 平仓
    CLOSETODAY = "CLOSETODAY"            # 平当日仓
    CLOSEYESTERDAY = "CLOSEYESTERDAY"    # 平昨日仓
    OTHER = "OTHER"

    @property
    def is_open(self) -> bool:
        """是否为开仓操作"""
        return self == PositionEffect.OPEN

    @property
    def is_close(self) -> bool:
        """是否为平仓操作"""
        return self in {
            PositionEffect.CLOSE,
            PositionEffect.CLOSETODAY,
            PositionEffect.CLOSEYESTERDAY,
        }


class PositionSide(BaseEnum):
    """持仓方向枚举

    持仓方向说明：
    - LONG: 多头持仓，看好价格上涨
    - SHORT: 空头持仓，看好价格下跌
    """

    LONG = "LONG"   # 多头
    SHORT = "SHORT" # 空头

    @property
    def opposite(self) -> 'PositionSide':
        """获取相反的方向"""
        return PositionSide.SHORT if self == PositionSide.LONG else PositionSide.LONG


class OrderType(BaseEnum):
    """订单类型枚举

    订单类型说明：
    - LIMIT: 限价单，指定价格成交
    - MARKET: 市价单，按当前市场价成交
    - BEST_PRICE: 最优价，按最优价格成交
    - FIVE_LEVEL_PRICE: 五档即成剩撤（国内市场特有）
    """

    LIMIT = "LIMIT"                      # 限价单
    MARKET = "MARKET"                    # 市价单
    BEST_PRICE = "BEST_PRICE"            # 最优价
    FIVE_LEVEL_PRICE = "FIVE_LEVEL_PRICE"  # 五档即成剩撤
    OTHER = "OTHER"

    @property
    def is_market_order(self) -> bool:
        """是否为市价单（立即成交的订单类型）"""
        return self in {
            OrderType.MARKET,
            OrderType.BEST_PRICE,
            OrderType.FIVE_LEVEL_PRICE,
        }


class OrderStatus(BaseEnum):
    """订单状态枚举

    订单生命周期：
    1. PENDING_NEW -> 订单创建，等待提交
    2. NEW -> 订单已提交到交易所
    3. PARTIALLY_FILLED -> 部分成交
    4. FILLED -> 全部成交（最终状态）
    5. CANCELLING -> 撤单请求中
    6. CANCELLED -> 已撤销（最终状态）
    7. REJECTED -> 订单被拒绝（最终状态）
    8. EXPIRED -> 订单过期（最终状态）
    """

    PENDING_NEW = "PENDING_NEW"          # 待报/待申报
    NEW = "NEW"                          # 已报/已申报
    PARTIALLY_FILLED = "PARTIALLY_FILLED"  # 部分成交
    FILLED = "FILLED"                    # 全部成交
    CANCELLING = "CANCELLING"            # 撤单中
    CANCELLED = "CANCELLED"              # 已撤销
    REJECTED = "REJECTED"                # 已拒绝
    EXPIRED = "EXPIRED"                  # 已过期
    OTHER = "OTHER"

    # 常用状态集合（避免代码中重复创建列表）
    PENDING_STATUSES = frozenset({PENDING_NEW, NEW})
    ACTIVE_STATUSES = frozenset({NEW, PARTIALLY_FILLED})
    FILLED_STATUSES = frozenset({PARTIALLY_FILLED, FILLED})
    TERMINAL_STATUSES = frozenset({FILLED, CANCELLED, REJECTED, EXPIRED})
    CANCELLABLE_STATUSES = frozenset({PENDING_NEW, NEW, PARTIALLY_FILLED})

    # 兼容旧版本的小写值
    _COMPAT_MAP = {
        "pending": PENDING_NEW,
        "submitted": NEW,
        "filled": FILLED,
        "partially_filled": PARTIALLY_FILLED,
        "cancelled": CANCELLED,
        "rejected": REJECTED,
    }

    @classmethod
    def from_value(cls, value: Any, default: Optional['OrderStatus'] = None) -> Optional['OrderStatus']:
        """从值获取订单状态，支持旧版本小写值的兼容"""
        if value is None:
            return default

        value_str = str(value).lower()

        # 检查兼容映射
        if value_str in cls._COMPAT_MAP:
            return cls._COMPAT_MAP[value_str]

        # 使用父类方法
        return super().from_value(value, default)

    def can_transition_to(self, new_status: 'OrderStatus') -> bool:
        """
        检查是否可以转换到新状态

        Args:
            new_status: 目标状态

        Returns:
            是否允许转换

        Examples:
            >>> OrderStatus.NEW.can_transition_to(OrderStatus.FILLED)
            True
            >>> OrderStatus.FILLED.can_transition_to(OrderStatus.CANCELLED)
            False
        """
        # 终止状态不能转换
        if self in self.TERMINAL_STATUSES:
            return False

        # 相同状态不需要转换
        if self == new_status:
            return True

        # 定义允许的转换
        valid_transitions = {
            self.PENDING_NEW: {self.NEW, self.CANCELLED, self.REJECTED},
            self.NEW: {self.PARTIALLY_FILLED, self.FILLED, self.CANCELLING, self.CANCELLED, self.REJECTED},
            self.PARTIALLY_FILLED: {self.PARTIALLY_FILLED, self.FILLED, self.CANCELLING, self.CANCELLED},
            self.CANCELLING: {self.CANCELLED, self.FILLED},
        }

        return new_status in valid_transitions.get(self, set())

    @property
    def is_pending(self) -> bool:
        """是否为等待状态"""
        return self in self.PENDING_STATUSES

    @property
    def is_active(self) -> bool:
        """是否为活跃状态（可能成交）"""
        return self in self.ACTIVE_STATUSES

    @property
    def is_terminal(self) -> bool:
        """是否为终止状态（不会再变化）"""
        return self in self.TERMINAL_STATUSES

    @property
    def is_filled(self) -> bool:
        """是否已成交（完全或部分）"""
        return self in self.FILLED_STATUSES

    @property
    def is_cancellable(self) -> bool:
        """是否可以撤销"""
        return self in self.CANCELLABLE_STATUSES


class TimeInForceEnum(BaseEnum):
    """订单有效期枚举

    订单有效期类型：
    - DAY: 当日有效，交易日结束后自动撤销
    - GTC: 撤单前有效（Good Til Canceled）
    - GTD: 日期前有效（Good Til Date）
    - IOC: 立即成交或撤销（Immediate Or Cancel）
    - FOK: 全成或全撤（Fill Or Kill）
    """

    DAY = "DAY"       # 当日有效
    GTC = "GTC"       # Good Til Canceled - 撤单前有效
    GTD = "GTD"       # Good Til Date - 日期前有效
    IOC = "IOC"       # Immediate Or Cancel - 立即成交或撤销
    FOK = "FOK"       # Fill Or Kill - 全成或全撤
    OTHER = "OTHER"

    @property
    def is_immediate(self) -> bool:
        """是否为立即执行类型（不会挂单）"""
        return self in {
            TimeInForceEnum.IOC,
            TimeInForceEnum.FOK,
        }


class OptionTypeEnum(BaseEnum):
    """期权类型枚举

    期权类型：
    - CALL: 看涨期权（认购期权），买方有权在约定时间以约定价格买入标的资产
    - PUT: 看跌期权（认沽期权），买方有权在约定时间以约定价格卖出标的资产
    - NONE: 非期权类资产
    """

    CALL = "CALL"   # 看涨期权
    PUT = "PUT"     # 看跌期权
    NONE = "NONE"   # 非期权类资产

    @property
    def is_option(self) -> bool:
        """是否为期权类型"""
        return self in {OptionTypeEnum.CALL, OptionTypeEnum.PUT}


# ============================================================================
# 枚举工具函数
# ============================================================================


@lru_cache(maxsize=128)
def normalize_enum_value(value: Any, enum_class: Type[BaseEnum]) -> Optional[BaseEnum]:
    """
    规范化枚举值，支持多种输入格式

    Args:
        value: 可以是枚举成员、字符串、或None
        enum_class: 目标枚举类

    Returns:
        规范化后的枚举成员，如果无法转换则返回None

    Examples:
        >>> normalize_enum_value("BUY", Side)
        <Side.BUY: 'BUY'>
        >>> normalize_enum_value("buy", Side)  # 支持小写
        <Side.BUY: 'BUY'>
        >>> normalize_enum_value(Side.BUY, Side)
        <Side.BUY: 'BUY'>
    """
    if value is None:
        return None

    if isinstance(value, enum_class):
        return value

    try:
        return enum_class.from_value(value)
    except (ValueError, KeyError):
        return None


def validate_enum_value(value: Any, enum_class: Type[BaseEnum], field_name: str = "value") -> None:
    """
    验证枚举值是否有效

    Args:
        value: 要验证的值
        enum_class: 目标枚举类
        field_name: 字段名称，用于错误消息

    Raises:
        ValueError: 当值无效时
        TypeError: 当值类型不正确时

    Examples:
        >>> validate_enum_value("BUY", Side)
        >>> validate_enum_value("INVALID", Side)  # 抛出 ValueError
    """
    if value is None:
        return

    normalized = normalize_enum_value(value, enum_class)
    if normalized is None:
        valid_values = ", ".join(enum_class.values())
        raise ValueError(
            f"无效的 {field_name} 值: '{value}'. "
            f"有效值为: {valid_values}"
        )


# ============================================================================
# 类型别名（向后兼容）
# ============================================================================

# 向后兼容：OrderSide 别名
OrderSide = Side


# ============================================================================
# 导出所有公共接口
# ============================================================================


__all__ = [
    # 基础类
    "BaseEnum",
    "BaseEnumMeta",
    "normalize_enum_value",
    "validate_enum_value",

    # 平台与交易所
    "PlatformEnum",
    "ExchangeEnum",

    # 账户相关
    "AccountTypeEnum",
    "AccountStatusEnum",

    # 资产与交易
    "AssetTypeEnum",
    "Side",
    "OrderSide",  # 别名
    "PositionEffect",
    "PositionSide",
    "OrderType",
    "OrderStatus",
    "TimeInForceEnum",
    "OptionTypeEnum",
] 