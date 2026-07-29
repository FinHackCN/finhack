"""
香港股票市场适配器

实现香港股票市场的交易规则和事件生成，支持多频次
支持基于时间的规则版本控制、VCM机制、随机收市等港股特有规则
"""

from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, date, time, timedelta
import logging
import hashlib

from ..base_market import BaseMarket
from ...events.event_types import BaseEvent, MarketEvent, EventTypeEnum, EventPriorityEnum
from .hk_trading_rules_versions import (
    TRADING_SCHEDULE_VERSIONS,
    PRICE_LIMIT_VERSIONS,
    SETTLEMENT_VERSIONS,
    get_hk_stock_board_type,
    get_exchange_from_symbol,
    is_vcm_stock,
    RuleVersion,
)
from .hk_calculator import (
    HKStockPriceCalculator,
    HKStockCommissionCalculator,
    HKStockLotSizeCalculator,
    HKVCMValidator,
    HKRandomClosingCalculator,
)

logger = logging.getLogger(__name__)


class HKStockMarketAdapter(BaseMarket):
    """香港股票市场适配器

    支持港股的交易规则，包含：
    - 盘前交易时段（09:00-09:30）
    - 上午交易时段（09:30-12:00）
    - 下午交易时段（13:00-16:00）
    - 收市竞价时段（16:00-16:10，随机收市）
    - T+0交易，T+2结算
    - 无涨跌停限制
    - VCM波动调节机制（81只大盘股）
    - 印花税（仅卖出0.1%）
    - 最小交易单位（一手，根据股价不同）
    """

    def __init__(self, config: Dict[str, Any] = None, current_date: date = None):
        """初始化香港股票市场适配器

        Args:
            config: 市场配置，如果为空则加载默认配置
            current_date: 当前日期（用于规则版本查询）
        """
        # 如果没有提供配置，使用默认配置
        if not config:
            config = self._get_default_config()

        super().__init__('hk_stock', config)

        # 当前日期（用于规则版本查询）
        self._current_date = current_date or date.today()

        # 港股市场特有配置
        self.t_plus_zero = True  # T+0制度
        self.price_limit_enabled = False  # 无涨跌停限制
        self.vcm_enabled = config.get('vcm_enabled', True)  # VCM机制
        self.random_close_enabled = config.get('random_close_enabled', True)  # 随机收市

        logger.info(f"香港股票市场适配器初始化完成，支持频率: {self.supported_frequencies}, "
                   f"当前日期: {self._current_date}")

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
        """获取香港股票市场默认配置"""
        return {
            'market_name': 'hk_stock',
            'supported_frequencies': ['1d', '1m', '30m', '120m'],
            'timezone': 'Asia/Hong_Kong',
            'currency': 'HKD',
            'trading_schedule': {
                '1d': {
                    'pre_market_start': '09:00',
                    'pre_market_end': '09:30',
                    'morning_start': '09:30',
                    'morning_end': '12:00',
                    'lunch_break_start': '12:00',
                    'lunch_break_end': '13:00',
                    'afternoon_start': '13:00',
                    'afternoon_end': '16:00',
                    'closing_auction_start': '16:00',
                    'closing_auction_end': '16:10',
                },
                '1m': {
                    'pre_market_start': '09:00',
                    'pre_market_end': '09:30',
                    'morning_start': '09:30',
                    'morning_end': '12:00',
                    'lunch_break_start': '12:00',
                    'lunch_break_end': '13:00',
                    'afternoon_start': '13:00',
                    'afternoon_end': '16:00',
                    'closing_auction_start': '16:00',
                    'closing_auction_end': '16:10',
                },
                '30m': {
                    'pre_market_start': '09:00',
                    'pre_market_end': '09:30',
                    'morning_start': '09:30',
                    'morning_end': '12:00',
                    'lunch_break_start': '12:00',
                    'lunch_break_end': '13:00',
                    'afternoon_start': '13:00',
                    'afternoon_end': '16:00',
                    'closing_auction_start': '16:00',
                    'closing_auction_end': '16:10',
                },
                '120m': {
                    'pre_market_start': '09:00',
                    'pre_market_end': '09:30',
                    'morning_start': '09:30',
                    'morning_end': '12:00',
                    'lunch_break_start': '12:00',
                    'lunch_break_end': '13:00',
                    'afternoon_start': '13:00',
                    'afternoon_end': '16:00',
                    'closing_auction_start': '16:00',
                    'closing_auction_end': '16:10',
                }
            },
            'trading_rules': {
                'commission': {
                    'stock': {
                        'open_commission': 0.001,  # 0.1%
                        'close_commission': 0.001,
                        'open_tax': 0.0,
                        'close_tax': 0.001,  # 印花税（卖出）
                        'min_commission': 0.0,
                        'trading_fee_rate': 0.00005,  # 交易费
                        'trading_levy_rate': 0.000027,  # 交易征费
                        'clearing_fee_rate': 0.00002,  # 交收费
                        'clearing_fee_max': 200.0,
                    }
                },
                'slippage': {
                    'slip_type': 'pricerelated',
                    'slip_value': 0.001
                },
                'limits': {
                    'lot_size': 100,  # 默认最小交易单位
                    'min_order_volume': 100,
                    'max_order_volume': 1000000
                },
                'settlement': {
                    'trading_cycle': 'T+0',
                    'settlement_cycle': 'T+2',
                }
            },
            'vcm_enabled': True,
            'random_close_enabled': True,
        }

    # ========================================================================
    # 交易时段相关
    # ========================================================================

    def get_trading_sessions_for_date(self, trade_date: date, symbol: str = None,
                                     frequency: str = '1d') -> List[Dict[str, Any]]:
        """获取指定日期的交易时段（考虑规则版本）

        Args:
            trade_date: 交易日期
            symbol: 标的代码（可选，用于判断板块）
            frequency: 频率

        Returns:
            交易时段列表，每个元素包含时段类型和时间范围
        """
        sessions = []
        exchange = get_exchange_from_symbol(symbol) if symbol else 'hkex'

        # 盘前交易时段
        sessions.append({
            'type': 'pre_market',
            'start': time(9, 0),
            'end': time(9, 30),
            'description': '盘前交易'
        })

        # 上午交易时段
        sessions.append({
            'type': 'morning',
            'start': time(9, 30),
            'end': time(12, 0),
            'description': '上午交易'
        })

        # 午休时段
        sessions.append({
            'type': 'lunch_break',
            'start': time(12, 0),
            'end': time(13, 0),
            'description': '午休'
        })

        # 下午交易时段
        sessions.append({
            'type': 'afternoon',
            'start': time(13, 0),
            'end': time(16, 0),
            'description': '下午交易'
        })

        # 收市竞价时段（2021年10月29日起引入随机收市）
        if trade_date >= date(2021, 10, 29):
            sessions.append({
                'type': 'closing_auction',
                'start': time(16, 0),
                'end': time(16, 10),
                'random_close': True,
                'description': '收市竞价（随机收市）'
            })
        else:
            sessions.append({
                'type': 'closing_auction',
                'start': time(16, 0),
                'end': time(16, 10),
                'random_close': False,
                'description': '收市竞价'
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

        # 确定是否启用随机收市
        random_close_time = None
        trade_day = trade_date.date() if isinstance(trade_date, datetime) else trade_date
        if trade_day >= date(2021, 10, 29) and self.random_close_enabled:
            # 计算随机收市时间（16:08-16:10之间）
            seed = int(hashlib.md5(str(trade_date).encode()).hexdigest(), 16)
            random_seconds = seed % 121  # 0-120秒
            random_close_time = (datetime.combine(trade_day, time(16, 8, 0)) + timedelta(seconds=random_seconds)).time()

        if frequency == '1d':
            # 日频事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.DAY_START,
                event_time=datetime.combine(trade_date, time(0, 0)),
                market=self.market_name,
                frequency=frequency,
                priority=EventPriorityEnum.HIGH,
                event_description="日开始"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.BEFORE_MARKET,
                event_time=datetime.combine(trade_date, time(9, 0)),
                market=self.market_name,
                frequency=frequency,
                priority=EventPriorityEnum.HIGH,
                event_description="盘前交易开始"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_START,
                event_time=datetime.combine(trade_date, time(9, 30)),
                market=self.market_name,
                frequency=frequency,
                priority=EventPriorityEnum.HIGH,
                event_description="开盘"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.TRY_MATCH,
                event_time=datetime.combine(trade_date, time(9, 30)),
                market=self.market_name,
                frequency=frequency,
                priority=EventPriorityEnum.HIGH,
                event_description="日级撮合"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.MORNING_END,
                event_time=datetime.combine(trade_date, time(12, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="上午收盘"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.AFTERNOON_START,
                event_time=datetime.combine(trade_date, time(13, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="下午开盘"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_END,
                event_time=datetime.combine(trade_date, time(16, 0)),
                market=self.market_name,
                frequency=frequency,
                priority=EventPriorityEnum.HIGH,
                event_description="收盘"
            ))

            # 收市竞价事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.CLOSING_START,
                event_time=datetime.combine(trade_date, time(16, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="收市竞价开始"
            ))

            close_time = random_close_time or time(16, 10)
            events.append(MarketEvent(
                event_type=EventTypeEnum.CLOSING_END,
                event_time=datetime.combine(trade_date, close_time),
                market=self.market_name,
                frequency=frequency,
                priority=EventPriorityEnum.HIGH,
                event_description="收市竞价结束（随机收市）" if random_close_time else "收市竞价结束"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.CLOSING_PRICE_DETERMINED,
                event_time=datetime.combine(trade_date, close_time),
                market=self.market_name,
                frequency=frequency,
                event_description="收盘价确定"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.AFTER_MARKET,
                event_time=datetime.combine(trade_date, time(18, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="交易后处理"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.DAILY_BAR_CLOSED,
                event_time=datetime.combine(trade_date, close_time),
                market=self.market_name,
                frequency=frequency,
                event_description="日K线生成"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.DAY_END,
                event_time=datetime.combine(trade_date, time(23, 59, 59)),
                market=self.market_name,
                frequency=frequency,
                priority=EventPriorityEnum.HIGH,
                event_description="日终处理"
            ))

        elif frequency in ['1m', '30m', '120m']:
            # 分钟频事件
            interval_minutes = 1 if frequency == '1m' else (30 if frequency == '30m' else 120)

            events.append(MarketEvent(
                event_type=EventTypeEnum.DAY_START,
                event_time=datetime.combine(trade_date, time(0, 0)),
                market=self.market_name,
                frequency=frequency,
                priority=EventPriorityEnum.HIGH,
                event_description="日开始"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.BEFORE_MARKET,
                event_time=datetime.combine(trade_date, time(9, 0)),
                market=self.market_name,
                frequency=frequency,
                priority=EventPriorityEnum.HIGH,
                event_description="盘前交易开始"
            ))

            # 盘前交易时段（09:00-09:30）
            if interval_minutes < 30:
                pre_market_start = datetime.combine(trade_date, time(9, 0))
                pre_market_end = datetime.combine(trade_date, time(9, 30))
                current_time = pre_market_start
                while current_time < pre_market_end:
                    events.append(MarketEvent(
                        event_type=EventTypeEnum.TRY_MATCH,
                        event_time=current_time,
                        market=self.market_name,
                        frequency=frequency,
                        event_description=f"{frequency}级撮合"
                    ))
                    current_time += timedelta(minutes=interval_minutes)

            # 开盘
            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_START,
                event_time=datetime.combine(trade_date, time(9, 30)),
                market=self.market_name,
                frequency=frequency,
                priority=EventPriorityEnum.HIGH,
                event_description="开盘"
            ))

            # 上午交易时段（09:30-12:00）
            morning_start = datetime.combine(trade_date, time(9, 30))
            morning_end = datetime.combine(trade_date, time(12, 0))
            current_time = morning_start

            while current_time <= morning_end:
                self._add_bar_event(events, current_time, frequency)
                events.append(MarketEvent(
                    event_type=EventTypeEnum.TRY_MATCH,
                    event_time=current_time,
                    market=self.market_name,
                    frequency=frequency,
                    event_description=f"{frequency}级撮合"
                ))
                current_time += timedelta(minutes=interval_minutes)

            # 上午收盘
            events.append(MarketEvent(
                event_type=EventTypeEnum.MORNING_END,
                event_time=datetime.combine(trade_date, time(12, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="上午收盘"
            ))

            # 下午开盘
            events.append(MarketEvent(
                event_type=EventTypeEnum.AFTERNOON_START,
                event_time=datetime.combine(trade_date, time(13, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="下午开盘"
            ))

            # 下午交易时段（13:00-16:00）
            afternoon_start = datetime.combine(trade_date, time(13, 0))
            afternoon_end = datetime.combine(trade_date, time(16, 0))
            current_time = afternoon_start

            while current_time <= afternoon_end:
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
                priority=EventPriorityEnum.HIGH,
                event_description="收盘"
            ))

            # 收市竞价时段（16:00-16:10）
            events.append(MarketEvent(
                event_type=EventTypeEnum.CLOSING_START,
                event_time=datetime.combine(trade_date, time(16, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="收市竞价开始"
            ))

            close_time = random_close_time or time(16, 10)
            events.append(MarketEvent(
                event_type=EventTypeEnum.CLOSING_END,
                event_time=datetime.combine(trade_date, close_time),
                market=self.market_name,
                frequency=frequency,
                priority=EventPriorityEnum.HIGH,
                event_description="收市竞价结束（随机收市）" if random_close_time else "收市竞价结束"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.CLOSING_PRICE_DETERMINED,
                event_time=datetime.combine(trade_date, close_time),
                market=self.market_name,
                frequency=frequency,
                event_description="收盘价确定"
            ))

            # 交易后
            events.append(MarketEvent(
                event_type=EventTypeEnum.AFTER_MARKET,
                event_time=datetime.combine(trade_date, time(18, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="交易后处理"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.DAILY_BAR_CLOSED,
                event_time=datetime.combine(trade_date, close_time),
                market=self.market_name,
                frequency=frequency,
                event_description="日K线生成"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.DAY_END,
                event_time=datetime.combine(trade_date, time(23, 59, 59)),
                market=self.market_name,
                frequency=frequency,
                priority=EventPriorityEnum.HIGH,
                event_description="日终处理"
            ))

        # 按时间和优先级排序
        events.sort(key=lambda x: (x.event_time, x.priority.value))

        return events

    def _add_bar_event(self, events: List[BaseEvent], event_time: datetime, frequency: str):
        """添加K线事件"""
        if frequency == '1m':
            event_type = EventTypeEnum.MARKET_BAR_1M
            desc = "1分钟K线"
        elif frequency == '30m':
            event_type = EventTypeEnum.MARKET_BAR_30M
            desc = "30分钟K线"
        elif frequency == '120m':
            event_type = EventTypeEnum.MARKET_BAR_120M
            desc = "120分钟K线"
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
        # 检查是否为工作日（港股周六、周日休市）
        if dt.weekday() >= 5:
            return False

        trade_date = dt.date()
        sessions = self.get_trading_sessions_for_date(trade_date, symbol, frequency)

        for session in sessions:
            # 跳过午休时段
            if session['type'] == 'lunch_break':
                continue
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
        return [(s['start'], s['end']) for s in sessions if s['type'] != 'lunch_break']

    # ========================================================================
    # 价格限制（港股无涨跌停）
    # ========================================================================

    def get_price_limits(self, symbol: str, prev_close: float = None,
                        listing_date: date = None) -> Dict[str, Any]:
        """获取价格限制（港股无涨跌停限制）

        Args:
            symbol: 标的代码
            prev_close: 前收盘价
            listing_date: 上市日期

        Returns:
            价格限制字典
        """
        return {
            'upper_limit': float('inf'),
            'lower_limit': 0.01,
            'limit_ratio': None,
            'is_new_stock': False,
            'has_vcm': is_vcm_stock(symbol) and self._current_date >= date(2016, 8, 22),
        }

    # ========================================================================
    # VCM机制验证
    # ========================================================================

    def check_vcm_trigger(self, symbol: str, current_price: float,
                         reference_price: float) -> Dict:
        """检查VCM是否触发

        Args:
            symbol: 标的代码
            current_price: 当前价格
            reference_price: 参考价

        Returns:
            VCM触发状态
        """
        if not self.vcm_enabled:
            return {
                'is_vcm_stock': False,
                'triggered': False,
                'change_pct': 0.0,
                'cooling_period_minutes': 0,
                'price_limit': None,
            }

        return HKVCMValidator.check_vcm_trigger(
            symbol, current_price, reference_price, self._current_date
        )

    def validate_order_in_vcm_cooling_period(self, symbol: str, order_price: float,
                                           side: str, vcm_trigger_price: float) -> Dict:
        """验证VCM冷静期内的订单价格

        Args:
            symbol: 标的代码
            order_price: 订单价格
            side: 订单方向
            vcm_trigger_price: VCM触发时的参考价

        Returns:
            验证结果
        """
        if not self.vcm_enabled:
            return {'valid': True, 'message': ''}

        return HKVCMValidator.validate_order_in_cooling_period(
            symbol, order_price, side, vcm_trigger_price, self._current_date
        )

    # ========================================================================
    # 最小交易单位
    # ========================================================================

    def get_lot_size(self, symbol: str, price: float = None) -> int:
        """获取交易单位（手数）

        Args:
            symbol: 标的代码
            price: 股价（用于确定lot size）

        Returns:
            交易单位
        """
        if price is None:
            # 默认价格
            price = 10.0

        lot_info = HKStockLotSizeCalculator.get_lot_size_info(symbol, price, self._current_date)
        return lot_info['min_buy']

    def get_position_limits(self, symbol: str, price: float = None) -> Dict[str, Any]:
        """获取持仓限制

        Args:
            symbol: 标的代码
            price: 股价

        Returns:
            持仓限制字典
        """
        if price is None:
            price = 10.0

        lot_info = HKStockLotSizeCalculator.get_lot_size_info(symbol, price, self._current_date)
        return {
            'lot_size': lot_info['lot_size'],
            'min_order_volume': lot_info['min_buy'],
            'sell_allow_fractional': lot_info['sell_allow_fractional'],
            'sell_min': lot_info['sell_min'],
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
        commission_info = HKStockCommissionCalculator.get_commission_info(symbol, self._current_date)

        if direction == 'buy':
            return {
                'commission': commission_info['commission_rate'],
                'tax': commission_info['stamp_tax_buy'],
                'trading_fee': commission_info['trading_fee_rate'],
                'trading_levy': commission_info['trading_levy_rate'],
                'clearing_fee': commission_info['clearing_fee_rate'],
                'min_commission': commission_info['min_commission'],
            }
        else:
            return {
                'commission': commission_info['commission_rate'],
                'tax': commission_info['stamp_tax_sell'],
                'trading_fee': commission_info['trading_fee_rate'],
                'trading_levy': commission_info['trading_levy_rate'],
                'clearing_fee': commission_info['clearing_fee_rate'],
                'min_commission': commission_info['min_commission'],
            }

    # ========================================================================
    # 其他基础方法
    # ========================================================================

    def get_trading_calendar(self, start_date: date, end_date: date) -> List[date]:
        """获取交易日历"""
        # TODO: 实现港股交易日历
        # 暂时返回工作日（排除周六日）
        trading_days = []
        current = start_date
        while current <= end_date:
            if current.weekday() < 5:  # 周一到周五
                trading_days.append(current)
            current += timedelta(days=1)
        return trading_days

    def get_settlement_cycle(self) -> str:
        """获取结算周期"""
        return 'T+2'

    def get_timezone(self) -> str:
        """获取时区"""
        return 'Asia/Hong_Kong'

    def get_currency(self) -> str:
        """获取货币"""
        return 'HKD'

    def get_market_name(self) -> str:
        """获取市场名称"""
        return self.market_name

    def get_slippage_rate(self, symbol: str) -> float:
        """获取滑点率"""
        return self.config['trading_rules']['slippage']['slip_value']

    def get_market_info(self, symbol: str = None, price: float = None) -> Dict[str, Any]:
        """获取市场信息"""
        return {
            'market_name': self.market_name,
            'timezone': 'Asia/Hong_Kong',
            'currency': 'HKD',
            'trading_sessions': [
                (time(9, 30), time(12, 0)),
                (time(13, 0), time(16, 0))
            ],
            'settlement_cycle': 'T+2',
            'trading_cycle': 'T+0',
            'price_limits': False,
            'price_limit_ratio': None,
            'vcm_enabled': self.vcm_enabled,
            'random_close_enabled': self.random_close_enabled,
            'short_selling': True,
            'margin_trading': True,
        }

    def get_supported_frequencies(self) -> List[str]:
        """获取支持的频率"""
        return ['1d', '1m', '30m', '120m']

    def get_trading_days(self, start_date: date, end_date: date) -> List[date]:
        """获取交易日"""
        return self.get_trading_calendar(start_date, end_date)


# 向后兼容的别名
HKStockAdapter = HKStockMarketAdapter
