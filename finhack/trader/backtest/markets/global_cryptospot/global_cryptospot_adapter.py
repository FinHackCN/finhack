"""
全球加密货币现货市场适配器

实现全球加密货币现货市场的交易规则和事件生成，支持多频次
支持基于时间的规则版本控制，集成计算器模块
"""

from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, date, time, timedelta
import logging

from ..base_market import BaseMarket
from ...events.event_types import BaseEvent, MarketEvent, EventTypeEnum
from .crypto_trading_rules_versions import (
    TRADING_SCHEDULE_VERSIONS,
    RuleVersion,
    get_crypto_exchange,
    get_crypto_pair_type,
    get_base_currency,
    get_quote_currency,
)
from .crypto_calculator import (
    CryptoPriceCalculator,
    CryptoLotSizeCalculator,
    CryptoCommissionCalculator,
    CryptoFundingRateCalculator,
    CryptoLeverageCalculator,
    CryptoLiquidationCalculator,
)

logger = logging.getLogger(__name__)


class GlobalCryptoSpotAdapter(BaseMarket):
    """全球加密货币现货市场适配器

    支持加密货币的交易规则，包含：
    - 24/7 交易时间（全年不休市）
    - 无涨跌幅限制（但有极端行情保护机制）
    - T+0 结算（即时清算）
    - 资金费率机制（合约交易）
    - 高杠杆支持（最高125倍）
    - 极小交易单位（支持小数交易）
    """

    def __init__(self, config: Dict[str, Any] = None, current_date: date = None):
        """初始化全球加密货币现货市场适配器

        Args:
            config: 市场配置，如果为空则加载默认配置
            current_date: 当前日期（用于规则版本查询）
        """
        # 如果没有提供配置，使用默认配置
        if not config:
            config = self._get_default_config()

        super().__init__('global_cryptospot', config)

        # 当前日期（用于规则版本查询）
        self._current_date = current_date or date.today()

        # 加密货币市场特有配置
        self.t_plus_one = config.get('t_plus_one', False)  # T+0制度
        self.price_limit_enabled = config.get('price_limit_enabled', False)  # 无涨跌停限制
        self.funding_rate_enabled = config.get('funding_rate_enabled', True)  # 支持资金费率

        logger.info(f"全球加密货币现货市场适配器初始化完成，支持频率: {self.supported_frequencies}, 当前日期: {self._current_date}")

    def set_current_date(self, current_date: date):
        """设置当前日期（用于回测时动态切换规则版本）

        Args:
            current_date: 当前日期
        """
        self._current_date = current_date

    def get_current_date(self) -> date:
        """获取当前日期

        Returns:
            当前日期
        """
        return self._current_date
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取全球加密货币现货市场默认配置"""
        return {
            'market_name': 'global_cryptospot',
            'supported_frequencies': ['1d', '1m', '30m', '120m'],
            'timezone': 'UTC',
            'currency': 'USD',
            'trading_schedule': {
                '1d': {
                    'morning_start': '00:00',
                    'morning_end': '23:59',
                    'afternoon_start': '00:00',
                    'afternoon_end': '23:59'
                },
                '1m': {
                    'morning_start': '00:00',
                    'morning_end': '23:59',
                    'afternoon_start': '00:00',
                    'afternoon_end': '23:59'
                },
                '30m': {
                    'morning_start': '00:00',
                    'morning_end': '23:59',
                    'afternoon_start': '00:00',
                    'afternoon_end': '23:59'
                },
                '120m': {
                    'morning_start': '00:00',
                    'morning_end': '23:59',
                    'afternoon_start': '00:00',
                    'afternoon_end': '23:59'
                }
            },
            'trading_rules': {
                'commission': {
                    'crypto': {
                        'open_commission': 0.001,  # 0.1% 开仓手续费
                        'close_commission': 0.001,  # 0.1% 平仓手续费
                        'open_tax': 0.0,
                        'close_tax': 0.0,  # 加密货币无印花税
                        'min_commission': 0.0
                    }
                },
                'slippage': {
                    'slip_type': 'pricerelated',
                    'slip_value': 0.0005  # 0.05% 滑点
                },
                'limits': {
                    'lot_size': 0.00000001,  # 最小交易单位
                    'min_order_volume': 0.00000001,  # 最小下单数量
                    'max_order_volume': 1000000  # 最大下单数量
                },
                'leverage': {
                    'max_leverage': 125,  # 最高125倍
                    'default_leverage': 10,
                    'leverage_increment': 1
                },
                'funding_rate': {
                    'enabled': True,
                    'interval_hours': 8,  # 每8小时收取一次
                }
            }
        }
    
    def generate_daily_events(self, trade_date: date, frequency: str = '1d') -> List[BaseEvent]:
        """生成指定日期的市场事件列表
        
        Args:
            trade_date: 交易日期
            frequency: 数据频率
            
        Returns:
            List[BaseEvent]: 事件列表
        """
        events = []
        
        if frequency == '1d':
            # 日频事件
            # 交易前事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.BEFORE_MARKET,
                event_time=datetime.combine(trade_date, time(0, 0)), # 假设UTC 00:00 对应本地 08:00
                market=self.market_name,
                frequency=frequency,
                event_description="交易前准备"
            ))
            
            # 开盘事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_START,
                event_time=datetime.combine(trade_date, time(0, 0)), # 假设UTC 00:00 对应本地 08:00
                market=self.market_name,
                frequency=frequency,
                event_description="开盘"
            ))
            
            # 撮合事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.TRY_MATCH,
                event_time=datetime.combine(trade_date, time(0, 0)), # 假设UTC 00:00 对应本地 08:00
                market=self.market_name,
                frequency=frequency,
                event_description="日级撮合"
            ))
            
            # 收盘事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_END,
                event_time=datetime.combine(trade_date, time(23, 59)), # 假设UTC 23:59 对应本地 07:59
                market=self.market_name,
                frequency=frequency,
                event_description="收盘"
            ))
            
            # 交易后事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.AFTER_MARKET,
                event_time=datetime.combine(trade_date, time(23, 59)), # 假设UTC 23:59 对应本地 07:59
                market=self.market_name,
                frequency=frequency,
                event_description="交易后处理"
            ))
            
            # 日K线事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.DAILY_BAR_CLOSED,
                event_time=datetime.combine(trade_date, time(23, 59)), # 假设UTC 23:59 对应本地 07:59
                market=self.market_name,
                frequency=frequency,
                event_description="日K线生成"
            ))
            
        elif frequency in ['1m', '30m', '120m']:
            # 分钟频事件
            # 日开始事件 - 在交易前触发，供策略初始化和调仓
            events.append(MarketEvent(
                event_type=EventTypeEnum.DAY_START,
                event_time=datetime.combine(trade_date, time(0, 0)), # 假设UTC 00:00 对应本地 08:00
                market=self.market_name,
                frequency=frequency,
                event_description="日开始"
            ))
            
            # 交易前事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.BEFORE_MARKET,
                event_time=datetime.combine(trade_date, time(0, 0)), # 假设UTC 00:00 对应本地 08:00
                market=self.market_name,
                frequency=frequency,
                event_description="交易前准备"
            ))
            
            # 开盘事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_START,
                event_time=datetime.combine(trade_date, time(0, 0)), # 假设UTC 00:00 对应本地 08:00
                market=self.market_name,
                frequency=frequency,
                event_description="开盘"
            ))
            
            # 根据频率生成K线事件
            interval_minutes = 1 if frequency == '1m' else (30 if frequency == '30m' else 120)
            
            # 上午交易时段
            morning_start = datetime.combine(trade_date, time(0, 0)) # 假设UTC 00:00 对应本地 08:00
            morning_end = datetime.combine(trade_date, time(23, 59)) # 假设UTC 23:59 对应本地 07:59
            current_time = morning_start
            
            while current_time <= morning_end:
                # 先生成K线事件
                if frequency == '1m':
                    events.append(MarketEvent(
                        event_type=EventTypeEnum.MARKET_BAR_1M,
                        event_time=current_time,
                        market=self.market_name,
                        frequency=frequency,
                        event_description="1分钟K线"
                    ))
                elif frequency == '30m':
                    events.append(MarketEvent(
                        event_type=EventTypeEnum.MARKET_BAR_30M,
                        event_time=current_time,
                        market=self.market_name,
                        frequency=frequency,
                        event_description="30分钟K线"
                    ))
                elif frequency == '120m':
                    events.append(MarketEvent(
                        event_type=EventTypeEnum.MARKET_BAR_120M,
                        event_time=current_time,
                        market=self.market_name,
                        frequency=frequency,
                        event_description="120分钟K线"
                    ))
                
                # 再生成撮合事件
                events.append(MarketEvent(
                    event_type=EventTypeEnum.TRY_MATCH,
                    event_time=current_time,
                    market=self.market_name,
                    frequency=frequency,
                    event_description=f"{frequency}级撮合"
                ))
                current_time += timedelta(minutes=interval_minutes)
            
            # 下午交易时段
            afternoon_start = datetime.combine(trade_date, time(0, 0)) # 假设UTC 00:00 对应本地 08:00
            afternoon_end = datetime.combine(trade_date, time(23, 59)) # 假设UTC 23:59 对应本地 07:59
            current_time = afternoon_start
            
            while current_time <= afternoon_end:
                # 先生成K线事件
                if frequency == '1m':
                    events.append(MarketEvent(
                        event_type=EventTypeEnum.MARKET_BAR_1M,
                        event_time=current_time,
                        market=self.market_name,
                        frequency=frequency,
                        event_description="1分钟K线"
                    ))
                elif frequency == '30m':
                    events.append(MarketEvent(
                        event_type=EventTypeEnum.MARKET_BAR_30M,
                        event_time=current_time,
                        market=self.market_name,
                        frequency=frequency,
                        event_description="30分钟K线"
                    ))
                elif frequency == '120m':
                    events.append(MarketEvent(
                        event_type=EventTypeEnum.MARKET_BAR_120M,
                        event_time=current_time,
                        market=self.market_name,
                        frequency=frequency,
                        event_description="120分钟K线"
                    ))
                
                # 再生成撮合事件
                events.append(MarketEvent(
                    event_type=EventTypeEnum.TRY_MATCH,
                    event_time=current_time,
                    market=self.market_name,
                    frequency=frequency,
                    event_description=f"{frequency}级撮合"
                ))
                current_time += timedelta(minutes=interval_minutes)
            
            # 收盘事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_END,
                event_time=datetime.combine(trade_date, time(23, 59)), # 假设UTC 23:59 对应本地 07:59
                market=self.market_name,
                frequency=frequency,
                event_description="收盘"
            ))
            
            # 交易后事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.AFTER_MARKET,
                event_time=datetime.combine(trade_date, time(23, 59)), # 假设UTC 23:59 对应本地 07:59
                market=self.market_name,
                frequency=frequency,
                event_description="交易后处理"
            ))
            
            # 日终事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.DAY_END,
                event_time=datetime.combine(trade_date, time(23, 59)), # 假设UTC 23:59 对应本地 07:59
                market=self.market_name,
                frequency=frequency,
                event_description="日终处理"
            ))
            
            # 日K线事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.DAILY_BAR_CLOSED,
                event_time=datetime.combine(trade_date, time(23, 59)), # 假设UTC 23:59 对应本地 07:59
                market=self.market_name,
                frequency=frequency,
                event_description="日K线生成"
            ))
        
        return events
    
    def is_trading_time(self, dt: datetime, frequency: str = '1d', symbol: str = None) -> bool:
        """判断指定时间是否为交易时间

        加密货币市场是24/7交易，包括周末和节假日

        Args:
            dt: 时间
            frequency: 数据频率
            symbol: 标的代码（可选）

        Returns:
            bool: 是否为交易时间
        """
        # 加密货币市场全年无休，24小时交易
        # 获取交易时段
        trading_sessions = self.get_trading_sessions(dt.date(), frequency, symbol)

        # 检查是否在任一交易时段内
        for session_start, session_end in trading_sessions:
            if session_start <= dt.time() <= session_end:
                return True

        return False

    def get_trading_sessions(self, trade_date: date, frequency: str = '1d',
                            symbol: str = None) -> List[Tuple[time, time]]:
        """获取交易时段

        加密货币市场24/7交易，全年无休

        Args:
            trade_date: 交易日期
            frequency: 数据频率
            symbol: 标的代码（可选）

        Returns:
            List[Tuple[time, time]]: 交易时段列表，每个元素为(开始时间, 结束时间)
        """
        # 加密货币市场交易时段 (24/7)
        return [(time(0, 0), time(23, 59))]

    # ========================================================================
    # 涨跌幅限制（使用新的计算器）
    # ========================================================================

    def get_price_limits(self, symbol: str, prev_close: float = None,
                        listing_date: date = None) -> Dict[str, Any]:
        """获取价格限制（使用新的计算器）

        加密货币市场通常无涨跌幅限制

        Args:
            symbol: 标的代码
            prev_close: 前收盘价
            listing_date: 上市日期

        Returns:
            价格限制字典
        """
        if prev_close is None:
            # 如果没有提供前收盘价，返回无限制
            return {
                'upper_limit': float('inf'),
                'lower_limit': 0.0,
                'limit_ratio': None,
                'has_circuit_breaker': False,
            }

        result = CryptoPriceCalculator.calculate_limit_prices(
            symbol, prev_close, self._current_date, listing_date
        )

        return {
            'upper_limit': result['upper_limit'],
            'lower_limit': result['lower_limit'],
            'limit_ratio': result['limit_ratio'],
            'has_circuit_breaker': result['has_circuit_breaker'],
            'circuit_breaker_threshold': result.get('circuit_breaker_threshold'),
        }

    # ========================================================================
    # 最小交易单位
    # ========================================================================

    def get_lot_size(self, symbol: str) -> float:
        """获取交易单位

        Args:
            symbol: 标的代码

        Returns:
            交易单位
        """
        lot_info = CryptoLotSizeCalculator.get_lot_size_info(symbol, self._current_date)
        return lot_info['min_quantity']

    def get_position_limits(self, symbol: str) -> Dict[str, Any]:
        """获取持仓限制

        Args:
            symbol: 标的代码

        Returns:
            持仓限制字典
        """
        lot_info = CryptoLotSizeCalculator.get_lot_size_info(symbol, self._current_date)
        return {
            'min_notional': lot_info['min_notional'],
            'min_quantity': lot_info['min_quantity'],
            'quantity_increment': lot_info['quantity_increment'],
            'price_increment': lot_info['price_increment'],
            'allow_fractional': lot_info['allow_fractional'],
        }

    # ========================================================================
    # 手续费计算
    # ========================================================================

    def get_commission_rate(self, symbol: str, direction: str = 'buy',
                           order_type: str = 'limit', vip_level: str = 'vip0') -> Dict[str, float]:
        """获取手续费率

        Args:
            symbol: 交易标的
            direction: 买卖方向
            order_type: 订单类型 ('limit' or 'market')
            vip_level: VIP等级

        Returns:
            手续费率字典
        """
        commission_info = CryptoCommissionCalculator.get_commission_info(
            symbol, self._current_date, vip_level
        )

        # 根据订单类型确定费率
        if order_type == 'limit':
            fee_rate = commission_info['maker_fee']
        else:
            fee_rate = commission_info['taker_fee']

        return {
            'commission': fee_rate,
            'tax': 0.0,  # 加密货币无印花税
            'min_commission': commission_info['min_commission']
        }

    # ========================================================================
    # 资金费率
    # ========================================================================

    def get_funding_rate_info(self, symbol: str) -> Dict[str, Any]:
        """获取资金费率信息

        Args:
            symbol: 标的代码

        Returns:
            资金费率信息
        """
        return CryptoFundingRateCalculator.get_funding_rate_info(symbol, self._current_date)

    def calculate_funding_rate(self, symbol: str, mark_price: float,
                              index_price: float) -> Dict[str, float]:
        """计算资金费率

        Args:
            symbol: 标的代码
            mark_price: 标记价格
            index_price: 指数价格

        Returns:
            资金费率信息
        """
        return CryptoFundingRateCalculator.calculate_funding_rate(
            symbol, mark_price, index_price, self._current_date
        )

    # ========================================================================
    # 杠杆和强平
    # ========================================================================

    def get_leverage_info(self, symbol: str) -> Dict[str, Any]:
        """获取杠杆信息

        Args:
            symbol: 标的代码

        Returns:
            杠杆信息
        """
        return CryptoLeverageCalculator.get_leverage_info(symbol, self._current_date)

    def calculate_liquidation_price(self, symbol: str, entry_price: float,
                                   leverage: int, position_side: str = 'long') -> Dict[str, float]:
        """计算强平价格

        Args:
            symbol: 标的代码
            entry_price: 开仓价格
            leverage: 杠杆倍数
            position_side: 持仓方向

        Returns:
            强平价格信息
        """
        return CryptoLiquidationCalculator.calculate_liquidation_price(
            symbol, entry_price, leverage, position_side, self._current_date
        )

    # ========================================================================
    # 其他基础方法
    # ========================================================================

    def get_trading_calendar(self, start_date: date, end_date: date) -> List[date]:
        """获取交易日历

        加密货币市场全年无休
        """
        delta = end_date - start_date
        return [start_date + timedelta(days=i) for i in range(delta.days + 1)]

    def get_settlement_cycle(self) -> str:
        """获取结算周期"""
        return 'T+0'

    def get_timezone(self) -> str:
        """获取时区"""
        return 'UTC'

    def get_currency(self) -> str:
        """获取货币"""
        return 'USD'

    def get_market_name(self) -> str:
        """获取市场名称"""
        return self.market_name

    def get_slippage_rate(self, symbol: str = None) -> float:
        """获取滑点率"""
        return self.config['trading_rules']['slippage']['slip_value']

    def get_rounding_method(self, symbol: str = None) -> str:
        """获取取整方法"""
        return 'round_half_up'

    def get_market_info(self, symbol: str = None) -> Dict[str, Any]:
        """获取市场信息"""
        exchange = get_crypto_exchange(symbol) if symbol else 'binance'
        pair_type = get_crypto_pair_type(symbol) if symbol else 'spot'

        # 获取涨跌幅信息
        price_limit_info = self.get_price_limits(symbol or 'BTCUSDT', 45000)

        return {
            'market_name': self.market_name,
            'timezone': 'UTC',
            'currency': 'USD',
            'trading_sessions': [
                (time(0, 0), time(23, 59))
            ],
            'settlement_cycle': 'T+0',
            'price_limits': False,
            'price_limit_ratio': price_limit_info['limit_ratio'],
            'short_selling': True,
            'margin_trading': True,
            'max_leverage': 125,
            'exchange': exchange,
            'pair_type': pair_type,
        }

    def get_supported_frequencies(self) -> List[str]:
        """获取支持的频率"""
        return ['1d', '1m', '30m', '120m']

    def get_trading_days(self, start_date: date, end_date: date) -> List[date]:
        """获取交易日

        加密货币市场全年无休
        """
        return self.get_trading_calendar(start_date, end_date)


# 向后兼容的别名
GlobalCryptoSpotAdapter = GlobalCryptoSpotAdapter
 