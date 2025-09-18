"""
回测系统枚举定义

定义了回测系统中使用的所有枚举类型，包括：
- 交易平台、交易所、账户类型
- 资产类型、订单类型、订单状态
- 持仓方向、委托方向等
"""

from enum import Enum


class PlatformEnum(Enum):
    """交易平台枚举"""
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


class ExchangeEnum(Enum):
    """交易所枚举"""
    SSE = "SSE"           # 上海证券交易所
    SZSE = "SZSE"         # 深圳证券交易所
    SHFE = "SHFE"         # 上海期货交易所
    DCE = "DCE"           # 大连商品交易所
    CZCE = "CZCE"         # 郑州商品交易所
    CFFEX = "CFFEX"       # 中国金融期货交易所
    INE = "INE"           # 上海国际能源交易中心
    HKEX = "HKEX"         # 香港交易所
    NYSE = "NYSE"         # 纽约证券交易所
    NASDAQ = "NASDAQ"     # 纳斯达克交易所
    CME = "CME"           # 芝加哥商品交易所
    ICE = "ICE"           # 洲际交易所
    LME = "LME"           # 伦敦金属交易所
    OTHER = "OTHER"


class AccountTypeEnum(Enum):
    """账户类型枚举"""
    CASH = "CASH"       # 现金账户
    MARGIN = "MARGIN"   # 保证金账户
    FUTURES = "FUTURES" # 期货账户
    OPTION = "OPTION"   # 期权账户
    CRYPTO = "CRYPTO"   # 数字货币账户
    UNIFIED = "UNIFIED" # 统一账户
    CREDIT = "CREDIT"   # 信用账户
    OTHER = "OTHER"


class AssetTypeEnum(Enum):
    """资产类型枚举"""
    STOCK = "STOCK"     # 股票
    FUTURE = "FUTURE"   # 期货
    OPTION = "OPTION"   # 期权
    FX = "FX"           # 外汇
    CRYPTO = "CRYPTO"   # 数字货币
    FUND = "FUND"       # 基金
    BOND = "BOND"       # 债券
    WARRANT = "WARRANT" # 权证
    COMBO = "COMBO"     # 组合合约
    OTHER = "OTHER"


class Side(Enum):
    """委托方向枚举"""
    BUY = "BUY"   # 买入
    SELL = "SELL" # 卖出


class PositionEffect(Enum):
    """持仓影响枚举"""
    OPEN = "OPEN"                    # 开仓
    CLOSE = "CLOSE"                  # 平仓
    CLOSETODAY = "CLOSETODAY"        # 平当日仓
    CLOSEYESTERDAY = "CLOSEYESTERDAY" # 平昨日仓
    OTHER = "OTHER"


class PositionSide(Enum):
    """持仓方向枚举"""
    LONG = "LONG"   # 多头
    SHORT = "SHORT" # 空头


class OrderType(Enum):
    """委托类型枚举"""
    LIMIT = "LIMIT"                      # 限价
    MARKET = "MARKET"                    # 市价
    BEST_PRICE = "BEST_PRICE"            # 最优价
    FIVE_LEVEL_PRICE = "FIVE_LEVEL_PRICE" # 五档即成剩撤
    OTHER = "OTHER"


class OrderStatus(Enum):
    """委托状态枚举"""
    PENDING_NEW = "PENDING_NEW"        # 待报
    NEW = "NEW"                        # 已报
    PARTIALLY_FILLED = "PARTIALLY_FILLED" # 部分成交
    FILLED = "FILLED"                  # 完全成交
    CANCELLING = "CANCELLING"          # 撤单中
    CANCELLED = "CANCELLED"            # 已撤单
    REJECTED = "REJECTED"              # 已拒绝
    EXPIRED = "EXPIRED"                # 已过期
    OTHER = "OTHER"


class TimeInForceEnum(Enum):
    """委托有效期枚举"""
    DAY = "DAY"   # 当日有效
    GTC = "GTC"   # Good Til Canceled
    GTD = "GTD"   # Good Til Date
    IOC = "IOC"   # Immediate Or Cancel
    FOK = "FOK"   # Fill Or Kill
    OTHER = "OTHER"


class AccountStatusEnum(Enum):
    """账户状态枚举"""
    CONNECTING = "CONNECTING"           # 连接中
    CONNECTED = "CONNECTED"             # 已连接
    AUTHENTICATED = "AUTHENTICATED"     # 已认证
    INITIALIZING = "INITIALIZING"       # 初始化中
    DATA_READY = "DATA_READY"           # 数据就绪
    DISCONNECTED = "DISCONNECTED"       # 已断开
    FAILED = "FAILED"                   # 连接/认证失败
    ERROR = "ERROR"                     # 运行时错误
    OTHER = "OTHER"


class OptionTypeEnum(Enum):
    """期权类型枚举"""
    CALL = "CALL" # 看涨期权
    PUT = "PUT"   # 看跌期权
    NONE = "NONE" # 非期权类资产 