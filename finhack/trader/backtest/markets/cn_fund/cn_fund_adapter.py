"""
中国基金市场适配器

实现中国ETF市场的交易规则和事件生成，支持多频次
支持基于时间的规则版本控制
"""

from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, date, time, timedelta
import logging

from ..base_market import BaseMarket
from ...events.event_types import BaseEvent, MarketEvent, EventTypeEnum
from .etf_trading_rules_versions import (
    TRADING_SCHEDULE_VERSIONS,
    get_etf_board_type,
    get_etf_type,
    get_exchange_from_symbol,
    RuleVersion,
)
from .etf_calculator import (
    ETFPriceCalculator,
    ETFPriceCageValidator,
    ETFLotSizeCalculator,
    ETFCommissionCalculator,
    ETFDividendAdjuster,
)

logger = logging.getLogger(__name__)


class CnFundMarketAdapter(BaseMarket):
    """中国基金市场适配器

    支持ETF的交易规则，包含：
    - 基于时间的规则版本控制
    - 不同板块的涨跌幅限制
    - 集合竞价时段
    - 收盘集合竞价
    - 盘后定价交易（科创板/创业板）
    - 价格笼子机制
    - 最小交易单位规则
    """

    def __init__(self, config: Dict[str, Any] = None, current_date: date = None):
        """初始化中国基金市场适配器

        Args:
            config: 市场配置，如果为空则加载默认配置
            current_date: 当前日期（用于规则版本查询）
        """
        # 如果没有提供配置，使用默认配置
        if not config:
            config = self._get_default_config()

        super().__init__('cn_fund', config)

        # 当前日期（用于规则版本查询）
        self._current_date = current_date or date.today()

        # 中国基金市场特有配置
        self.t_plus_one = config.get('t_plus_one', True)  # T+1制度
        self.price_limit_enabled = config.get('price_limit_enabled', True)  # 涨跌停限制
        self.daily_price_limit = config.get('daily_price_limit', 0.10)  # 10%涨跌停

        logger.info(f"中国基金市场适配器初始化完成，支持频率: {self.supported_frequencies}, 当前日期: {self._current_date}")

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
        """获取中国基金市场默认配置"""
        return {
            'market_name': 'cn_fund',
            'supported_frequencies': ['1d', '1m', '30m', '120m'],
            'timezone': 'Asia/Shanghai',
            'currency': 'CNY',
            'trading_schedule': {
                '1d': {
                    'morning_start': '09:30',
                    'morning_end': '11:30',
                    'afternoon_start': '13:00',
                    'afternoon_end': '15:00'
                },
                '1m': {
                    'morning_start': '09:30',
                    'morning_end': '11:30',
                    'afternoon_start': '13:00',
                    'afternoon_end': '15:00'
                },
                '30m': {
                    'morning_start': '09:30',
                    'morning_end': '11:30',
                    'afternoon_start': '13:00',
                    'afternoon_end': '15:00'
                },
                '120m': {
                    'morning_start': '09:30',
                    'morning_end': '11:30',
                    'afternoon_start': '13:00',
                    'afternoon_end': '15:00'
                }
            },
            'trading_rules': {
                'commission': {
                    'stock_etf': {
                        'open_commission': 0.0003,
                        'close_commission': 0.0003,
                        'open_tax': 0.0,
                        'close_tax': 0.001,  # 印花税（卖出）
                        'min_commission': 5.0
                    },
                    'bond_etf': {
                        'open_commission': 0.0003,
                        'close_commission': 0.0003,
                        'open_tax': 0.0,
                        'close_tax': 0.0,  # 债券ETF免印花税
                        'min_commission': 5.0
                    },
                    'money_etf': {
                        'open_commission': 0.0,
                        'close_commission': 0.0,
                        'open_tax': 0.0,
                        'close_tax': 0.0,  # 货币ETF免印花税
                        'min_commission': 0.0
                    }
                },
                'slippage': {
                    'slip_type': 'pricerelated',
                    'slip_value': 0.001
                },
                'limits': {
                    'lot_size': 100,  # 默认最小交易单位
                    'min_order_volume': 100,  # 最小下单数量
                    'max_order_volume': 1000000  # 最大下单数量
                }
            }
        }

    # ========================================================================
    # 交易时段相关（支持集合竞价、收盘集合竞价、盘后交易）
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
            [
                {'type': 'pre_opening', 'start': time(9,15), 'end': time(9,25)},
                {'type': 'morning', 'start': time(9,30), 'end': time(11,30)},
                {'type': 'afternoon', 'start': time(13,0), 'end': time(15,0)},
                {'type': 'closing_auction', 'start': time(14,57), 'end': time(15,0)},  # 如果适用
                {'type': 'post_trading', 'start': time(15,5), 'end': time(15,30)},  # 如果适用
            ]
        """
        sessions = []
        board = get_etf_board_type(symbol) if symbol else 'main_board'
        exchange = get_exchange_from_symbol(symbol) if symbol else 'sse'

        # 获取适用的交易时段规则
        applicable_rules = []
        for version in TRADING_SCHEDULE_VERSIONS:
            if version['effective_date'] <= trade_date:
                # 检查scope
                if 'scope' in version:
                    if exchange not in version['scope']:
                        continue
                applicable_rules.append(version)

        # 基础连续竞价时段
        sessions.append({
            'type': 'morning',
            'start': time(9, 30),
            'end': time(11, 30),
            'description': '上午连续竞价'
        })
        sessions.append({
            'type': 'afternoon',
            'start': time(13, 0),
            'end': time(15, 0),
            'description': '下午连续竞价'
        })

        # 集合竞价时段（1991年7月3日起）
        if trade_date >= date(1991, 7, 3):
            sessions.insert(0, {
                'type': 'pre_opening',
                'start': time(9, 15),
                'end': time(9, 25),
                'cancel_deadline': time(9, 20),
                'description': '开盘集合竞价'
            })

        # 收盘集合竞价
        # 深市2006年7月1日起
        if exchange == 'szse' and trade_date >= date(2006, 7, 1):
            sessions.append({
                'type': 'closing_auction',
                'start': time(14, 57),
                'end': time(15, 0),
                'description': '收盘集合竞价'
            })
        # 沪市2018年8月20日起
        elif exchange == 'sse' and trade_date >= date(2018, 8, 20):
            sessions.append({
                'type': 'closing_auction',
                'start': time(14, 57),
                'end': time(15, 0),
                'description': '收盘集合竞价'
            })

        # 盘后定价交易（科创板2019年7月22日起，创业板2020年8月24日起）
        if board == 'star_market' and trade_date >= date(2019, 7, 22):
            sessions.append({
                'type': 'post_trading',
                'start': time(15, 5),
                'end': time(15, 30),
                'description': '盘后定价交易'
            })
        elif board == 'gem' and trade_date >= date(2020, 8, 24):
            sessions.append({
                'type': 'post_trading',
                'start': time(15, 5),
                'end': time(15, 30),
                'description': '盘后定价交易'
            })

        return sessions

    # ========================================================================
    # 事件生成（支持集合竞价等新事件）
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
        board = get_etf_board_type(symbol) if symbol else 'main_board'
        exchange = get_exchange_from_symbol(symbol) if symbol else 'sse'

        if frequency == '1d':
            # 日频事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.BEFORE_MARKET,
                event_time=datetime.combine(trade_date, time(9, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="交易前准备"
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
                event_time=datetime.combine(trade_date, time(15, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="收盘"
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
                event_time=datetime.combine(trade_date, time(15, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="日K线生成"
            ))

        elif frequency in ['1m', '30m', '120m']:
            # 分钟频事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.DAY_START,
                event_time=datetime.combine(trade_date, time(9, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="日开始"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.BEFORE_MARKET,
                event_time=datetime.combine(trade_date, time(9, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="交易前准备"
            ))

            # 集合竞价事件（1991年7月3日起）
            if trade_date >= date(1991, 7, 3):
                events.append(MarketEvent(
                    event_type=EventTypeEnum.PRE_OPENING_START,
                    event_time=datetime.combine(trade_date, time(9, 15)),
                    market=self.market_name,
                    frequency=frequency,
                    event_description="集合竞价开始"
                ))

                events.append(MarketEvent(
                    event_type=EventTypeEnum.PRE_OPENING_END,
                    event_time=datetime.combine(trade_date, time(9, 20)),
                    market=self.market_name,
                    frequency=frequency,
                    event_description="集合竞价不可撤单"
                ))

                events.append(MarketEvent(
                    event_type=EventTypeEnum.MATCHING_START,
                    event_time=datetime.combine(trade_date, time(9, 25)),
                    market=self.market_name,
                    frequency=frequency,
                    event_description="集合竞价撮合"
                ))

                events.append(MarketEvent(
                    event_type=EventTypeEnum.OPENING_PRICE_DETERMINED,
                    event_time=datetime.combine(trade_date, time(9, 25)),
                    market=self.market_name,
                    frequency=frequency,
                    event_description="开盘价确定"
                ))

            # 开盘
            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_START,
                event_time=datetime.combine(trade_date, time(9, 30)),
                market=self.market_name,
                frequency=frequency,
                event_description="开盘"
            ))

            # 生成K线事件
            interval_minutes = 1 if frequency == '1m' else (30 if frequency == '30m' else 120)

            # 上午交易时段
            morning_start = datetime.combine(trade_date, time(9, 30))
            morning_end = datetime.combine(trade_date, time(11, 30))
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
                event_time=datetime.combine(trade_date, time(11, 30)),
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

            # 下午交易时段
            afternoon_start = datetime.combine(trade_date, time(13, 0))
            afternoon_end = datetime.combine(trade_date, time(15, 0))
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

            # 收盘集合竞价事件
            closing_auction_date = date(2006, 7, 1) if exchange == 'szse' else date(2018, 8, 20)
            if trade_date >= closing_auction_date:
                events.append(MarketEvent(
                    event_type=EventTypeEnum.CLOSING_START,
                    event_time=datetime.combine(trade_date, time(14, 57)),
                    market=self.market_name,
                    frequency=frequency,
                    event_description="收盘集合竞价开始"
                ))

                events.append(MarketEvent(
                    event_type=EventTypeEnum.CLOSING_END,
                    event_time=datetime.combine(trade_date, time(15, 0)),
                    market=self.market_name,
                    frequency=frequency,
                    event_description="收盘集合竞价结束"
                ))

                events.append(MarketEvent(
                    event_type=EventTypeEnum.CLOSING_PRICE_DETERMINED,
                    event_time=datetime.combine(trade_date, time(15, 0)),
                    market=self.market_name,
                    frequency=frequency,
                    event_description="收盘价确定"
                ))

            # 收盘
            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_END,
                event_time=datetime.combine(trade_date, time(15, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="收盘"
            ))

            # 盘后定价交易事件
            post_trading_enabled = False
            if board == 'star_market' and trade_date >= date(2019, 7, 22):
                post_trading_enabled = True
            elif board == 'gem' and trade_date >= date(2020, 8, 24):
                post_trading_enabled = True

            if post_trading_enabled:
                events.append(MarketEvent(
                    event_type=EventTypeEnum.POST_TRADING_START,
                    event_time=datetime.combine(trade_date, time(15, 5)),
                    market=self.market_name,
                    frequency=frequency,
                    event_description="盘后定价交易开始"
                ))

                events.append(MarketEvent(
                    event_type=EventTypeEnum.POST_TRADING_END,
                    event_time=datetime.combine(trade_date, time(15, 30)),
                    market=self.market_name,
                    frequency=frequency,
                    event_description="盘后定价交易结束"
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
                event_type=EventTypeEnum.DAY_END,
                event_time=datetime.combine(trade_date, time(23, 59, 59)),
                market=self.market_name,
                frequency=frequency,
                event_description="日终处理"
            ))

            events.append(MarketEvent(
                event_type=EventTypeEnum.DAILY_BAR_CLOSED,
                event_time=datetime.combine(trade_date, time(15, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="日K线生成"
            ))

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
        # 检查是否为工作日
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
    # 涨跌幅限制（使用新的计算器）
    # ========================================================================

    def get_price_limits(self, symbol: str, prev_close: float = None,
                        listing_date: date = None) -> Dict[str, Any]:
        """获取价格限制（使用新的计算器）

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
                'is_new_stock': False,
            }

        result = ETFPriceCalculator.calculate_limit_prices(
            symbol, prev_close, self._current_date, listing_date
        )

        return {
            'upper_limit': result['upper_limit'],
            'lower_limit': result['lower_limit'],
            'limit_ratio': result['limit_ratio'],
            'is_new_stock': result['is_new_stock'],
            'new_stock_days_left': result['new_stock_days_left'],
        }

    # ========================================================================
    # 价格笼子验证
    # ========================================================================

    def validate_price_cage(self, symbol: str, order_price: float, side: str,
                           reference_price: float, bid_price: float = None,
                           ask_price: float = None) -> Dict:
        """验证订单价格是否符合价格笼子限制

        Args:
            symbol: 标的代码
            order_price: 订单价格
            side: 订单方向 ('buy' or 'sell')
            reference_price: 参考价
            bid_price: 买一价
            ask_price: 卖一价

        Returns:
            验证结果
        """
        return ETFPriceCageValidator.validate_order_price(
            symbol, order_price, side, reference_price,
            bid_price, ask_price, self._current_date
        )

    # ========================================================================
    # 最小交易单位
    # ========================================================================

    def get_lot_size(self, symbol: str) -> int:
        """获取交易单位（手数）

        Args:
            symbol: 标的代码

        Returns:
            交易单位
        """
        lot_info = ETFLotSizeCalculator.get_lot_size_info(symbol, self._current_date)
        return lot_info['min_buy']

    def get_position_limits(self, symbol: str) -> Dict[str, Any]:
        """获取持仓限制

        Args:
            symbol: 标的代码

        Returns:
            持仓限制字典
        """
        lot_info = ETFLotSizeCalculator.get_lot_size_info(symbol, self._current_date)
        return {
            'lot_size': lot_info['min_buy'],
            'buy_increment': lot_info['buy_increment'],
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
        commission_info = ETFCommissionCalculator.get_commission_info(symbol, self._current_date)

        if direction == 'buy':
            return {
                'commission': commission_info['commission_rate'],
                'tax': commission_info['stamp_tax_buy'],
                'min_commission': commission_info['min_commission']
            }
        else:
            return {
                'commission': commission_info['commission_rate'],
                'tax': commission_info['stamp_tax_sell'],
                'min_commission': commission_info['min_commission']
            }

    # ========================================================================
    # 其他基础方法
    # ========================================================================

    def get_trading_calendar(self, start_date: date, end_date: date) -> List[date]:
        """获取交易日历"""
        from ...data.calendar.cn_stock_calendar import CnStockCalendar
        calendar = CnStockCalendar()
        return calendar.get_trading_days(start_date, end_date)

    def get_settlement_cycle(self) -> str:
        """获取结算周期"""
        return 'T+1'

    def get_timezone(self) -> str:
        """获取时区"""
        return 'Asia/Shanghai'

    def get_currency(self) -> str:
        """获取货币"""
        return 'CNY'

    def get_market_name(self) -> str:
        """获取市场名称"""
        return self.market_name

    def get_slippage_rate(self, symbol: str) -> float:
        """获取滑点率"""
        return self.config['trading_rules']['slippage']['slip_value']

    def get_rounding_method(self, symbol: str) -> str:
        """获取取整方法"""
        board = get_etf_board_type(symbol)
        if board in ['star_market', 'gem']:
            return 'ceil'  # 向上进位
        elif board == 'bse':
            return 'truncate'  # 直接截断
        else:
            return 'round'  # 四舍五入

    def get_market_info(self, symbol: str = None) -> Dict[str, Any]:
        """获取市场信息"""
        board = get_etf_board_type(symbol) if symbol else 'main_board'

        # 获取涨跌幅信息
        price_limit_info = self.get_price_limits(symbol or '510300.SH', 4.5)

        return {
            'market_name': self.market_name,
            'timezone': 'Asia/Shanghai',
            'currency': 'CNY',
            'trading_sessions': [
                (time(9, 30), time(11, 30)),
                (time(13, 0), time(15, 0))
            ],
            'settlement_cycle': 'T+1',
            'price_limits': True,
            'price_limit_ratio': price_limit_info['limit_ratio'],
            'short_selling': False,
            'margin_trading': False,
            'board_type': board,
        }

    def get_supported_frequencies(self) -> List[str]:
        """获取支持的频率"""
        return ['1d', '1m', '30m', '120m']

    def get_trading_days(self, start_date: date, end_date: date) -> List[date]:
        """获取交易日"""
        from ...data.calendar.cn_stock_calendar import CnStockCalendar
        calendar = CnStockCalendar()
        return calendar.get_trading_days(start_date, end_date)


# 向后兼容的别名
CnFundAdapter = CnFundMarketAdapter
