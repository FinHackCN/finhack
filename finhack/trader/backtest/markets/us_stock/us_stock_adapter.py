"""
美股市场适配器

实现美股市场的交易规则和事件生成
支持盘前盘后交易、熔断机制、PDT规则、零碎股等
"""

from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, date, time, timedelta
import logging

from ..base_market import BaseMarket
from ...events.event_types import BaseEvent, MarketEvent, EventTypeEnum
from .us_trading_rules_versions import (
    TRADING_SCHEDULE_VERSIONS,
    get_exchange_from_symbol,
    get_stock_type,
    RuleVersion,
)
from .us_calculator import (
    USCircuitBreakerCalculator,
    USLotSizeCalculator,
    USPriceCalculator,
    USCommissionCalculator,
    USSettlementCalculator,
    USPDTChecker,
    USShortSaleCalculator,
)

logger = logging.getLogger(__name__)


class USStockMarketAdapter(BaseMarket):
    """美股市场适配器

    支持美股的交易规则，包含：
    - 盘前盘后交易时段
    - 熔断机制（市场级和个股级）
    - PDT规则（日内回转交易限制）
    - 零碎股交易支持
    - 卖空规则（Rule 201）
    - T+1/T+2结算周期
    """

    def __init__(self, config: Dict[str, Any] = None, current_date: date = None):
        """初始化美股市场适配器

        Args:
            config: 市场配置，如果为空则加载默认配置
            current_date: 当前日期（用于规则版本查询）
        """
        # 如果没有提供配置，使用默认配置
        if not config:
            config = self._get_default_config()

        super().__init__('us_stock', config)

        # 当前日期（用于规则版本查询）
        self._current_date = current_date or date.today()

        # 美股市场特有配置
        self.price_limit_enabled = config.get('price_limit_enabled', True)
        self.circuit_breaker_enabled = config.get('circuit_breaker_enabled', True)
        self.fractional_shares_enabled = config.get('fractional_shares_enabled', True)

        logger.info(f"美股市场适配器初始化完成，支持频率: {self.supported_frequencies}, 当前日期: {self._current_date}")

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
        """获取美股市场默认配置"""
        return {
            'market_name': 'us_stock',
            'supported_frequencies': ['1d', '1m', '5m', '15m', '30m', '60m'],
            'timezone': 'America/New_York',
            'currency': 'USD',
            'trading_schedule': {
                '1d': {
                    'pre_market_start': '04:00',
                    'pre_market_end': '09:30',
                    'regular_start': '09:30',
                    'regular_end': '16:00',
                    'post_market_start': '16:00',
                    'post_market_end': '20:00',
                },
                '1m': {
                    'pre_market_start': '04:00',
                    'pre_market_end': '09:30',
                    'regular_start': '09:30',
                    'regular_end': '16:00',
                    'post_market_start': '16:00',
                    'post_market_end': '20:00',
                },
                '5m': {
                    'pre_market_start': '04:00',
                    'pre_market_end': '09:30',
                    'regular_start': '09:30',
                    'regular_end': '16:00',
                    'post_market_start': '16:00',
                    'post_market_end': '20:00',
                },
                '15m': {
                    'pre_market_start': '04:00',
                    'pre_market_end': '09:30',
                    'regular_start': '09:30',
                    'regular_end': '16:00',
                    'post_market_start': '16:00',
                    'post_market_end': '20:00',
                },
                '30m': {
                    'pre_market_start': '04:00',
                    'pre_market_end': '09:30',
                    'regular_start': '09:30',
                    'regular_end': '16:00',
                    'post_market_start': '16:00',
                    'post_market_end': '20:00',
                },
                '60m': {
                    'pre_market_start': '04:00',
                    'pre_market_end': '09:30',
                    'regular_start': '09:30',
                    'regular_end': '16:00',
                    'post_market_start': '16:00',
                    'post_market_end': '20:00',
                }
            },
            'trading_rules': {
                'commission': {
                    'stock': {
                        'open_commission': 0.0,
                        'close_commission': 0.0,
                        'open_tax': 0.0,
                        'close_tax': 0.0,
                        'min_commission': 0.0,
                        'sec_fee': 0.0000131,
                        'trading_activity_fee': 0.000119,
                    },
                    'etf': {
                        'open_commission': 0.0,
                        'close_commission': 0.0,
                        'open_tax': 0.0,
                        'close_tax': 0.0,
                        'min_commission': 0.0,
                        'sec_fee': 0.0000131,
                        'trading_activity_fee': 0.000119,
                    }
                },
                'slippage': {
                    'slip_type': 'pricerelated',
                    'slip_value': 0.0005  # 美股流动性较好，滑点较小
                },
                'limits': {
                    'lot_size': 1,  # 美股支持1股
                    'min_order_volume': 0.0001,  # 支持零碎股
                    'max_order_volume': 10000000
                }
            }
        }

    # ========================================================================
    # 交易时段相关（支持盘前盘后）
    # ========================================================================

    def get_trading_sessions_for_date(self, trade_date: date, symbol: str = None,
                                     frequency: str = '1d') -> List[Dict[str, Any]]:
        """获取指定日期的交易时段

        Args:
            trade_date: 交易日期
            symbol: 标的代码（可选）
            frequency: 频率

        Returns:
            交易时段列表
            [
                {'type': 'pre_market', 'start': time(4,0), 'end': time(9,30)},
                {'type': 'regular', 'start': time(9,30), 'end': time(16,0)},
                {'type': 'post_market', 'start': time(16,0), 'end': time(20,0)},
            ]
        """
        sessions = []

        # 盘前交易时段
        sessions.append({
            'type': 'pre_market',
            'start': time(4, 0),
            'end': time(9, 30),
            'description': '盘前交易'
        })

        # 正常交易时段
        sessions.append({
            'type': 'regular',
            'start': time(9, 30),
            'end': time(16, 0),
            'description': '正常交易'
        })

        # 盘后交易时段
        sessions.append({
            'type': 'post_market',
            'start': time(16, 0),
            'end': time(20, 0),
            'description': '盘后交易'
        })

        return sessions

    # ========================================================================
    # 事件生成
    # ========================================================================

    def generate_daily_events(self, trade_date: date, frequency: str = '1d',
                             symbol: str = None) -> List[BaseEvent]:
        """生成指定日期的市场事件列表

        Args:
            trade_date: 交易日期
            frequency: 数据频率
            symbol: 标的代码（可选）

        Returns:
            List[BaseEvent]: 事件列表
        """
        events = []

        if frequency == '1d':
            # 日频事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.BEFORE_MARKET,
                event_time=datetime.combine(trade_date, time(4, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="交易日开始"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_START,
                event_time=datetime.combine(trade_date, time(9, 30)),
                market=self.market_name,
                frequency=frequency,
                event_description="开盘"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.TRY_MATCH,
                event_time=datetime.combine(trade_date, time(9, 30)),
                market=self.market_name,
                frequency=frequency,
                event_description="日级撮合"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_END,
                event_time=datetime.combine(trade_date, time(16, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="收盘"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.AFTER_MARKET,
                event_time=datetime.combine(trade_date, time(20, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="盘后交易结束"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.DAILY_BAR_CLOSED,
                event_time=datetime.combine(trade_date, time(16, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="日K线生成"
            ))

        elif frequency in ['1m', '5m', '15m', '30m', '60m']:
            # 分钟频事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.DAY_START,
                event_time=datetime.combine(trade_date, time(4, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="日开始"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.BEFORE_MARKET,
                event_time=datetime.combine(trade_date, time(4, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="交易前准备"
            ))

            # 盘前交易事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_START,
                event_time=datetime.combine(trade_date, time(4, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="盘前交易开始"
            ))

            # 生成盘前K线事件
            interval_minutes = self._get_interval_minutes(frequency)
            pre_market_start = datetime.combine(trade_date, time(4, 0))
            pre_market_end = datetime.combine(trade_date, time(9, 30))
            current_time = pre_market_start

            while current_time < pre_market_end:
                self._add_bar_event(events, current_time, frequency)
                events.append(MarketEvent(
                    event_type=EventTypeEnum.TRY_MATCH,
                    event_time=current_time,
                    market=self.market_name,
                    frequency=frequency,
                    event_description=f"{frequency}级撮合"
                ))
                current_time += timedelta(minutes=interval_minutes)

            # 正常交易开始
            events.append(MarketEvent(
                event_type=EventTypeEnum.MATCHING_START,
                event_time=datetime.combine(trade_date, time(9, 30)),
                market=self.market_name,
                frequency=frequency,
                event_description="正常交易开始"
            ))

            # 正常交易时段K线
            regular_start = datetime.combine(trade_date, time(9, 30))
            regular_end = datetime.combine(trade_date, time(16, 0))
            current_time = regular_start

            while current_time <= regular_end:
                self._add_bar_event(events, current_time, frequency)
                events.append(MarketEvent(
                    event_type=EventTypeEnum.TRY_MATCH,
                    event_time=current_time,
                    market=self.market_name,
                    frequency=frequency,
                    event_description=f"{frequency}级撮合"
                ))
                current_time += timedelta(minutes=interval_minutes)

            # 收盘
            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_END,
                event_time=datetime.combine(trade_date, time(16, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="收盘"
            ))

            # 盘后交易事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.POST_TRADING_START,
                event_time=datetime.combine(trade_date, time(16, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="盘后交易开始"
            ))

            # 盘后交易时段K线
            post_market_start = datetime.combine(trade_date, time(16, 0))
            post_market_end = datetime.combine(trade_date, time(20, 0))
            current_time = post_market_start

            while current_time <= post_market_end:
                self._add_bar_event(events, current_time, frequency)
                events.append(MarketEvent(
                    event_type=EventTypeEnum.TRY_MATCH,
                    event_time=current_time,
                    market=self.market_name,
                    frequency=frequency,
                    event_description=f"{frequency}级撮合"
                ))
                current_time += timedelta(minutes=interval_minutes)

            events.append(MarketEvent(
                event_type=EventTypeEnum.POST_TRADING_END,
                event_time=datetime.combine(trade_date, time(20, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="盘后交易结束"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.AFTER_MARKET,
                event_time=datetime.combine(trade_date, time(20, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="交易后处理"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.DAY_END,
                event_time=datetime.combine(trade_date, time(23, 59, 59)),
                market=self.market_name,
                frequency=frequency,
                event_description="日终处理"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.DAILY_BAR_CLOSED,
                event_time=datetime.combine(trade_date, time(16, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="日K线生成"
            ))

        return events

    def _get_interval_minutes(self, frequency: str) -> int:
        """获取频率对应的分钟数"""
        freq_map = {
            '1m': 1,
            '5m': 5,
            '15m': 15,
            '30m': 30,
            '60m': 60,
        }
        return freq_map.get(frequency, 1)

    def _add_bar_event(self, events: List[BaseEvent], event_time: datetime, frequency: str):
        """添加K线事件"""
        if frequency == '1m':
            event_type = EventTypeEnum.MARKET_BAR_1M
            desc = "1分钟K线"
        elif frequency == '5m':
            event_type = EventTypeEnum.MARKET_BAR_1M  # 复用1分钟类型
            desc = "5分钟K线"
        elif frequency == '15m':
            event_type = EventTypeEnum.MARKET_BAR_1M
            desc = "15分钟K线"
        elif frequency == '30m':
            event_type = EventTypeEnum.MARKET_BAR_30M
            desc = "30分钟K线"
        elif frequency == '60m':
            event_type = EventTypeEnum.MARKET_BAR_120M  # 复用120分钟类型
            desc = "60分钟K线"
        else:
            return

        events.append(MarketEvent(
            event_type=event_type,
            event_time=event_time,
            market=self.market_name,
            frequency=frequency,
            event_description=desc
        ))

    # ========================================================================
    # 交易时间判断
    # ========================================================================

    def is_trading_time(self, dt: datetime, frequency: str = '1d', symbol: str = None) -> bool:
        """判断指定时间是否为交易时间

        Args:
            dt: 日期时间
            frequency: 频率
            symbol: 标的代码（可选）

        Returns:
            是否为交易时间
        """
        # 检查是否为工作日（周一至周五）
        if dt.weekday() >= 5:  # 周六、周日
            return False

        trade_date = dt.date()
        sessions = self.get_trading_sessions_for_date(trade_date, symbol, frequency)

        for session in sessions:
            if session['start'] <= dt.time() <= session['end']:
                return True

        return False

    def get_trading_sessions(self, trade_date: date, frequency: str = '1d',
                            symbol: str = None) -> List[Tuple[time, time]]:
        """获取交易时段

        Args:
            trade_date: 交易日期
            frequency: 频率
            symbol: 标的代码（可选）

        Returns:
            交易时段列表
        """
        sessions = self.get_trading_sessions_for_date(trade_date, symbol, frequency)
        return [(s['start'], s['end']) for s in sessions]

    # ========================================================================
    # 熔断机制
    # ========================================================================

    def check_circuit_breaker(self, index_level: float, previous_close: float,
                             trigger_time: time) -> Dict:
        """检查市场级熔断

        Args:
            index_level: S&P 500当前点位
            previous_close: S&P 500前收盘价
            trigger_time: 触发时间

        Returns:
            熔断信息
        """
        return USCircuitBreakerCalculator.check_circuit_breaker(
            index_level, previous_close, trigger_time, self._current_date
        )

    def check_individual_pause(self, symbol: str, current_price: float,
                              reference_price: float) -> Dict:
        """检查个股限制暂停

        Args:
            symbol: 股票代码
            current_price: 当前价格
            reference_price: 参考价

        Returns:
            暂停信息
        """
        return USCircuitBreakerCalculator.check_individual_pause(
            symbol, current_price, reference_price, self._current_date
        )

    # ========================================================================
    # 价格限制（美股无涨跌停，但有个股熔断）
    # ========================================================================

    def get_price_limits(self, symbol: str, prev_close: float = None,
                        listing_date: date = None) -> Dict[str, Any]:
        """获取价格限制

        Args:
            symbol: 标的代码
            prev_close: 前收盘价
            listing_date: 上市日期

        Returns:
            价格限制字典
        """
        # 美股无涨跌停限制，但有个股熔断
        return {
            'upper_limit': float('inf'),
            'lower_limit': 0.0,
            'limit_ratio': None,
            'is_new_stock': False,
            'circuit_breaker_enabled': True,
            'individual_pause_threshold': 0.05,  # 5%
        }

    # ========================================================================
    # 卖空规则
    # ========================================================================

    def check_short_sale_restriction(self, symbol: str, daily_change_pct: float,
                                    bid_price: float) -> Dict:
        """检查卖空限制（Rule 201）

        Args:
            symbol: 股票代码
            daily_change_pct: 当日涨跌幅
            bid_price: 最优买价

        Returns:
            限制信息
        """
        return USShortSaleCalculator.check_short_sale_restriction(
            symbol, daily_change_pct, bid_price, self._current_date
        )

    # ========================================================================
    # PDT规则
    # ========================================================================

    def check_pdt_restriction(self, account_equity: float, day_trades_count: int) -> Dict:
        """检查PDT规则限制

        Args:
            account_equity: 账户净值
            day_trades_count: 日内交易次数

        Returns:
            限制信息
        """
        return USPDTChecker.check_pdt_restriction(
            account_equity, day_trades_count, self._current_date
        )

    # ========================================================================
    # 最小交易单位（支持零碎股）
    # ========================================================================

    def get_lot_size(self, symbol: str) -> int:
        """获取交易单位（手数）

        Args:
            symbol: 标的代码

        Returns:
            交易单位
        """
        lot_info = USLotSizeCalculator.get_lot_size_info(symbol, self._current_date)
        return lot_info['min_buy']

    def get_position_limits(self, symbol: str) -> Dict[str, Any]:
        """获取持仓限制

        Args:
            symbol: 标的代码

        Returns:
            持仓限制字典
        """
        lot_info = USLotSizeCalculator.get_lot_size_info(symbol, self._current_date)
        return {
            'lot_size': lot_info['min_buy'],
            'buy_increment': lot_info['buy_increment'],
            'min_order_volume': lot_info['min_buy'],
            'sell_allow_fractional': lot_info['sell_allow_fractional'],
            'sell_min': lot_info['sell_min'],
            'fractional_shares': lot_info.get('fractional_shares', False),
        }

    # ========================================================================
    # 手续费计算
    # ========================================================================

    def get_commission_rate(self, symbol: str, direction: str = 'buy') -> Dict[str, float]:
        """获取手续费率

        Args:
            symbol: 交易标的
            direction: 买卖方向

        Returns:
            手续费率字典
        """
        commission_info = USCommissionCalculator.get_commission_info(symbol, self._current_date)

        return {
            'commission': commission_info['commission_rate'],
            'tax': commission_info['stamp_tax_sell'] if direction == 'sell' else commission_info['stamp_tax_buy'],
            'min_commission': commission_info['min_commission'],
            'sec_fee': commission_info.get('sec_fee', 0.0),
            'trading_activity_fee': commission_info.get('trading_activity_fee', 0.0),
        }

    # ========================================================================
    # 结算周期
    # ========================================================================

    def get_settlement_cycle(self) -> str:
        """获取结算周期"""
        info = USSettlementCalculator.get_settlement_info(self._current_date)
        return info['settlement_cycle']

    def get_settlement_date(self, trade_date: date) -> date:
        """获取结算日期

        Args:
            trade_date: 交易日期

        Returns:
            结算日期
        """
        info = USSettlementCalculator.get_settlement_info(trade_date)
        return info['settlement_date']

    # ========================================================================
    # 其他基础方法
    # ========================================================================

    def get_trading_calendar(self, start_date: date, end_date: date) -> List[date]:
        """获取交易日历"""
        # TODO: 实现美股交易日历
        # 美股节假日：元旦、马丁路德金日、总统日、耶稣受难日、阵亡将士纪念日、
        # 独立日、劳动节、感恩节、圣诞节
        from ...data.calendar.us_stock_calendar import USStockCalendar
        calendar = USStockCalendar()
        return calendar.get_trading_days(start_date, end_date)

    def get_timezone(self) -> str:
        """获取时区"""
        return 'America/New_York'

    def get_currency(self) -> str:
        """获取货币"""
        return 'USD'

    def get_market_name(self) -> str:
        """获取市场名称"""
        return self.market_name

    def get_slippage_rate(self, symbol: str) -> float:
        """获取滑点率"""
        return self.config['trading_rules']['slippage']['slip_value']

    def get_rounding_method(self, symbol: str) -> str:
        """获取取整方法"""
        return 'round'  # 美股使用四舍五入

    def get_market_info(self, symbol: str = None) -> Dict[str, Any]:
        """获取市场信息"""
        stock_type = get_stock_type(symbol) if symbol else 'common'
        exchange = get_exchange_from_symbol(symbol) if symbol else 'nasdaq'

        return {
            'market_name': self.market_name,
            'timezone': 'America/New_York',
            'currency': 'USD',
            'trading_sessions': [
                (time(4, 0), time(9, 30)),   # 盘前
                (time(9, 30), time(16, 0)),  # 正常
                (time(16, 0), time(20, 0)),  # 盘后
            ],
            'settlement_cycle': self.get_settlement_cycle(),
            'price_limits': False,  # 无涨跌停
            'price_limit_ratio': None,
            'circuit_breaker': True,  # 有熔断
            'short_selling': True,
            'margin_trading': True,
            'fractional_shares': True,  # 支持零碎股
            'stock_type': stock_type,
            'exchange': exchange,
        }

    def get_supported_frequencies(self) -> List[str]:
        """获取支持的频率"""
        return ['1d', '1m', '5m', '15m', '30m', '60m']

    def get_trading_days(self, start_date: date, end_date: date) -> List[date]:
        """获取交易日"""
        from ...data.calendar.us_stock_calendar import USStockCalendar
        calendar = USStockCalendar()
        return calendar.get_trading_days(start_date, end_date)


# 向后兼容的别名
USStockAdapter = USStockMarketAdapter
