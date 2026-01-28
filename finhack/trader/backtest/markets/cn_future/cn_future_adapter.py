"""
中国期货市场适配器

实现中国期货市场的交易规则和事件生成，支持多频次
支持基于时间的规则版本控制，包含保证金、涨跌停、交割月等规则
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, date, time, timedelta
import logging

from ..base_market import BaseMarket
from ...events.event_types import BaseEvent, MarketEvent, EventTypeEnum
from .future_trading_rules_versions import (
    get_future_exchange,
    get_future_type,
    get_future_product,
    has_night_session,
    DEFAULT_TRADING_SCHEDULES,
    NIGHT_SESSION_SPECIAL,
    RuleVersion,
)
from .future_calculator import (
    FuturePriceCalculator,
    FutureMarginCalculator,
    FutureLotSizeCalculator,
    FutureCommissionCalculator,
    FutureDeliveryValidator,
    FuturePositionValidator,
)

logger = logging.getLogger(__name__)


class CnFutureMarketAdapter(BaseMarket):
    """中国期货市场适配器

    支持期货市场的交易规则，包含：
    - 基于时间的规则版本控制
    - 日盘和夜盘交易时段
    - 涨跌幅限制（不同品种不同限制）
    - 保证金制度（初始保证金、维持保证金）
    - T+0交易制度
    - 交割月限制
    - 持仓限制
    - 手续费计算（开仓、平仓、平今）
    """

    def __init__(self, config: Dict[str, Any] = None, current_date: date = None):
        """初始化中国期货市场适配器

        Args:
            config: 市场配置，如果为空则加载默认配置
            current_date: 当前日期（用于规则版本查询）
        """
        # 如果没有提供配置，使用默认配置
        if not config:
            config = self._get_default_config()

        super().__init__('cn_future', config)

        # 当前日期（用于规则版本查询）
        self._current_date = current_date or date.today()

        # 中国期货市场特有配置
        self.margin_enabled = config.get('margin_enabled', True)  # 保证金制度
        self.t_plus_zero = config.get('t_plus_zero', True)  # T+0制度

        logger.info(f"中国期货市场适配器初始化完成，支持频率: {self.supported_frequencies}, 当前日期: {self._current_date}")

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
        """获取默认配置"""
        return {
            'supported_frequencies': ['1d', '1m'],
            'timezone': 'Asia/Shanghai',
            'currency': 'CNY',
            'trading_schedule': {
                '1d': {
                    'DAY_START': '00:00:00',
                    'BEFORE_MARKET': '08:55:00',
                    'AUCTION_START': '08:59:00',
                    'DAY_SESSION_START': '09:00:00',
                    'DAY_SESSION_END': '15:00:00',
                    'SETTLEMENT_PRICE_DETERMINED': '15:15:00',
                    'MARGIN_CALL_CHECK': '16:00:00',
                    'BEFORE_NIGHT_SESSION': '20:55:00',
                    'NIGHT_AUCTION_START': '20:59:00',
                    'NIGHT_SESSION_START': '21:00:00',
                    'NIGHT_SESSION_END': '02:30:00',
                    'DAY_END': '23:59:59'
                },
                '1m': {
                    'DAY_START': '00:00:00',
                    'BEFORE_MARKET': '08:55:00',
                    'DAY_SESSION_START': '09:00:00',
                    'DAY_SESSION_END': '15:00:00',
                    'NIGHT_SESSION_START': '21:00:00',
                    'NIGHT_SESSION_END': '02:30:00',
                    'DAY_END': '23:59:59'
                }
            },
            'trading_rules': {
                'commission': {
                    'future': {
                        'open_commission': 0.0001,    # 万分之一
                        'close_commission': 0.0001,   # 万分之一
                        'close_today_commission': 0.0001,  # 平今手续费
                        'open_tax': 0.0,              # 免税
                        'close_tax': 0.0,             # 免税
                        'min_commission': 5.0         # 最低5元
                    }
                },
                'slippage': {
                    'slip_type': 'pricerelated',
                    'slip_value': 0.0005            # 0.05%滑点
                },
                'limits': {
                    'lot_size': 1,                  # 1手
                    'min_order_volume': 1,          # 最小1手
                    'max_order_volume': 1000        # 最大1000手
                },
                'margin': {
                    'margin_ratio': 0.10           # 10%保证金比例
                }
            },
            't_plus_zero': True,
            'margin_enabled': True
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
        
        if frequency not in self.supported_frequencies:
            logger.warning(f"不支持的频率: {frequency}")
            return events
        
        schedule = self.trading_schedule.get(frequency, {})
        
        if frequency == '1d':
            # 日线频率的完整事件列表
            events.extend(self._generate_daily_events_1d(trade_date, schedule))
        elif frequency == '1m':
            # 分钟线频率的事件列表
            events.extend(self._generate_daily_events_1m(trade_date, schedule))
        
        # 按时间排序
        events.sort(key=lambda x: x.event_time)
        
        logger.debug(f"生成 {trade_date} {frequency} 频率事件 {len(events)} 个")
        return events
    
    def _generate_daily_events_1d(self, trade_date: date, schedule: Dict[str, time]) -> List[BaseEvent]:
        """生成日线频率的事件列表"""
        events = []
        
        # 生成静态市场事件
        for event_name, event_time in schedule.items():
            if event_name in ['DAY_START', 'DAY_END']:
                continue  # 这些事件在EventCenter中统一处理
                
            # 将事件名称转换为EventTypeEnum
            event_type_map = {
                'BEFORE_MARKET': EventTypeEnum.BEFORE_MARKET,
                'AUCTION_START': EventTypeEnum.PRE_OPENING_START,
                'DAY_SESSION_START': EventTypeEnum.DAY_SESSION_START,
                'DAY_SESSION_END': EventTypeEnum.DAY_SESSION_END,
                'SETTLEMENT_PRICE_DETERMINED': EventTypeEnum.SETTLEMENT_PRICE_DETERMINED,
                'MARGIN_CALL_CHECK': EventTypeEnum.MARGIN_CALL_CHECK,
                'BEFORE_NIGHT_SESSION': EventTypeEnum.BEFORE_NIGHT_SESSION,
                'NIGHT_AUCTION_START': EventTypeEnum.NIGHT_AUCTION_START,
                'NIGHT_SESSION_START': EventTypeEnum.NIGHT_SESSION_START,
                'NIGHT_SESSION_END': EventTypeEnum.NIGHT_SESSION_END,
            }
            
            event_type = event_type_map.get(event_name, EventTypeEnum.BEFORE_MARKET)
            event_dt = datetime.combine(trade_date, event_time)
            
            # 处理跨日事件（夜盘）
            if event_name == 'NIGHT_SESSION_END':
                # 夜盘结束时间是第二天
                event_dt = datetime.combine(trade_date + timedelta(days=1), event_time)
            
            event = MarketEvent(
                event_type=event_type,
                event_time=event_dt,
                market=self.market_name,
                frequency='1d',
                event_description=self._get_event_description(event_name)
            )
            events.append(event)
        
        return events
    
    def _generate_daily_events_1m(self, trade_date: date, schedule: Dict[str, time]) -> List[BaseEvent]:
        """生成分钟线频率的事件列表"""
        events = []
        
        # 基础市场事件
        basic_events = ['BEFORE_MARKET', 'DAY_SESSION_START', 'DAY_SESSION_END', 
                       'NIGHT_SESSION_START']
        
        # 将事件名称转换为EventTypeEnum
        event_type_map = {
            'BEFORE_MARKET': EventTypeEnum.BEFORE_MARKET,
            'DAY_SESSION_START': EventTypeEnum.DAY_SESSION_START,
            'DAY_SESSION_END': EventTypeEnum.DAY_SESSION_END,
            'NIGHT_SESSION_START': EventTypeEnum.NIGHT_SESSION_START,
        }
        
        for event_name in basic_events:
            if event_name in schedule:
                event_type = event_type_map.get(event_name, EventTypeEnum.BEFORE_MARKET)
                event_dt = datetime.combine(trade_date, schedule[event_name])
                
                event = MarketEvent(
                    event_type=event_type,
                    event_time=event_dt,
                    market=self.market_name,
                    frequency='1m',
                    event_description=self._get_event_description(event_name)
                )
                events.append(event)
        
        # 生成分钟级TRY_MATCH事件
        trading_sessions = self.get_trading_sessions(trade_date, '1m')
        for session_start, session_end in trading_sessions:
            current_time = session_start
            while current_time < session_end:
                # 每分钟生成一个TRY_MATCH事件
                match_event = MarketEvent(
                    event_type=EventTypeEnum.TRY_MATCH,
                    event_time=current_time,
                    market=self.market_name,
                    frequency='1m',
                    event_description="分钟级撮合"
                )
                events.append(match_event)
                current_time += timedelta(minutes=1)
        
        # 处理夜盘结束事件（跨日）
        if 'NIGHT_SESSION_END' in schedule:
            event_type = EventTypeEnum.NIGHT_SESSION_END
            event_dt = datetime.combine(trade_date + timedelta(days=1), schedule['NIGHT_SESSION_END'])
            
            event = MarketEvent(
                event_type=event_type,
                event_time=event_dt,
                market=self.market_name,
                frequency='1m',
                event_description=self._get_event_description('NIGHT_SESSION_END')
            )
            events.append(event)
        
        return events
    
    def _get_event_description(self, event_name: str) -> str:
        """获取事件描述"""
        descriptions = {
            'BEFORE_MARKET': '盘前准备',
            'AUCTION_START': '开盘集合竞价开始',
            'DAY_SESSION_START': '日盘开始',
            'DAY_SESSION_END': '日盘结束',
            'SETTLEMENT_PRICE_DETERMINED': '结算价确定',
            'MARGIN_CALL_CHECK': '保证金检查',
            'BEFORE_NIGHT_SESSION': '夜盘盘前准备',
            'NIGHT_AUCTION_START': '夜盘集合竞价开始',
            'NIGHT_SESSION_START': '夜盘开始',
            'NIGHT_SESSION_END': '夜盘结束',
            'TRY_MATCH': '撮合处理'
        }
        return descriptions.get(event_name, event_name)
    
    def get_trading_sessions(self, trade_date: date, frequency: str = '1d',
                            symbol: str = None) -> List[tuple]:
        """获取交易时段

        Args:
            trade_date: 交易日期
            frequency: 数据频率
            symbol: 期货代码（可选，用于判断是否有夜盘）

        Returns:
            List[tuple]: 交易时段列表，每个元素为 (start_dt, end_dt, session_type)
        """
        sessions = []

        # 获取交易所
        exchange = get_future_exchange(symbol) if symbol else 'cffex'
        product = get_future_product(symbol) if symbol else 'IF'

        # 获取默认交易时段配置
        default_schedule = DEFAULT_TRADING_SCHEDULES.get(exchange, DEFAULT_TRADING_SCHEDULES['cffex'])

        # 检查是否有夜盘
        has_night = has_night_session(symbol) if symbol else False

        # 日盘时段
        day_start_str = default_schedule.get('day_session_start', '09:00')
        day_end_str = default_schedule.get('day_session_end', '15:00')

        day_start = datetime.strptime(day_start_str, '%H:%M').time()
        day_end = datetime.strptime(day_end_str, '%H:%M').time()

        # 构建日盘时段（考虑中间休息）
        if exchange == 'cffex':
            # 中金所（股指、国债期货）
            day_start_dt = datetime.combine(trade_date, day_start)
            day_end_dt = datetime.combine(trade_date, day_end)
            sessions.append((day_start_dt, day_end_dt, 'day'))
        else:
            # 商品期货（有中间休息）
            break_start_str = default_schedule.get('morning_break_start', '10:15')
            break_end_str = default_schedule.get('morning_break_end', '10:30')
            break_start = datetime.strptime(break_start_str, '%H:%M').time()
            break_end = datetime.strptime(break_end_str, '%H:%M').time()

            # 上午时段
            morning_start_dt = datetime.combine(trade_date, day_start)
            morning_end_dt = datetime.combine(trade_date, break_start)
            sessions.append((morning_start_dt, morning_end_dt, 'morning'))

            # 下午时段
            afternoon_start_dt = datetime.combine(trade_date, break_end)
            afternoon_end_dt = datetime.combine(trade_date, day_end)
            sessions.append((afternoon_start_dt, afternoon_end_dt, 'afternoon'))

        # 夜盘时段
        if has_night:
            # 检查是否有特殊夜盘时间配置
            night_end_str = default_schedule.get('night_session_end', '23:00')
            if product in NIGHT_SESSION_SPECIAL:
                night_end_str = NIGHT_SESSION_SPECIAL[product]['end']

            night_start_str = default_schedule.get('night_session_start', '21:00')
            night_start = datetime.strptime(night_start_str, '%H:%M').time()
            night_end = datetime.strptime(night_end_str, '%H:%M').time()

            # 夜盘跨越到次日
            night_start_dt = datetime.combine(trade_date, night_start)
            night_end_dt = datetime.combine(trade_date + timedelta(days=1), night_end)
            sessions.append((night_start_dt, night_end_dt, 'night'))

        return sessions
    
    def is_trading_time(self, dt: datetime, frequency: str = '1d',
                       symbol: str = None) -> bool:
        """判断指定时间是否为交易时间

        Args:
            dt: 时间
            frequency: 数据频率
            symbol: 期货代码（可选）

        Returns:
            bool: 是否为交易时间
        """
        # 获取交易时段
        trading_sessions = self.get_trading_sessions(dt.date(), frequency, symbol)

        # 检查是否在任一交易时段内
        for session_start, session_end, _ in trading_sessions:
            if session_start <= dt <= session_end:
                return True

        return False
    
    def validate_order(self, symbol: str, volume: float, price: float,
                      side: str, prev_settlement: float = None) -> tuple[bool, str]:
        """验证订单是否符合中国期货市场规则

        Args:
            symbol: 交易标的
            volume: 交易数量
            price: 交易价格
            side: 交易方向
            prev_settlement: 前结算价（用于涨跌停检查）

        Returns:
            tuple[bool, str]: (是否有效, 错误信息)
        """
        # 调用基类验证
        is_valid, error_msg = super().validate_order(symbol, volume, price, side)
        if not is_valid:
            return is_valid, error_msg

        # 期货特有验证

        # 1. 检查手数（必须为整数手）
        lot_info = FutureLotSizeCalculator.get_lot_size_info(symbol)
        normalized_volume, msg = FutureLotSizeCalculator.normalize_order_volume(symbol, volume)
        if normalized_volume == 0:
            return False, msg

        # 2. 检查价格是否符合Tick Size
        tick_size = lot_info['tick_size']
        tick_count = price / tick_size
        if not abs(tick_count - round(tick_count)) < 1e-6:
            return False, f"价格必须是{tick_size}的整数倍"

        # 3. 检查涨跌停限制
        if prev_settlement:
            price_validation = FuturePriceCalculator.validate_order_price(
                symbol, price, prev_settlement=prev_settlement
            )
            if not price_validation['valid']:
                return False, price_validation['message']

        # 4. 检查交割月限制（自然人不能进入交割月）
        delivery_check = FutureDeliveryValidator.check_natural_person_ban(
            symbol, self._current_date
        )
        if delivery_check['is_banned']:
            return False, delivery_check['reason']

        return True, ""
    
    def is_t_plus_zero_allowed(self, symbol: str) -> bool:
        """检查是否允许T+0交易

        Args:
            symbol: 交易标的

        Returns:
            bool: 是否允许T+0交易
        """
        # 中国期货市场均支持T+0
        return self.t_plus_zero

    def get_margin_ratio(self, symbol: str, margin_type: str = 'initial') -> float:
        """获取保证金比例

        Args:
            symbol: 交易标的
            margin_type: 保证金类型 ('initial' or 'maintenance')

        Returns:
            float: 保证金比例
        """
        if not self.margin_enabled:
            return 0.0

        # 使用计算器获取保证金比例
        return get_margin_ratio(symbol, margin_type)

    def calculate_margin(self, symbol: str, volume: int, price: float,
                        margin_type: str = 'initial') -> Dict[str, float]:
        """计算保证金

        Args:
            symbol: 期货代码
            volume: 手数
            price: 价格
            margin_type: 保证金类型

        Returns:
            保证金信息字典
        """
        return FutureMarginCalculator.calculate_margin(
            symbol, volume, price, margin_type, self._current_date
        )

    def calculate_commission(self, symbol: str, volume: int, price: float,
                            offset_flag: str = 'open') -> Dict[str, float]:
        """计算手续费

        Args:
            symbol: 期货代码
            volume: 手数
            price: 价格
            offset_flag: 开平标志 ('open', 'close', 'close_today')

        Returns:
            手续费信息字典
        """
        return FutureCommissionCalculator.calculate_commission(
            symbol, volume, price, offset_flag, self._current_date
        )

    def get_price_limits(self, symbol: str, prev_close: float = None,
                        is_new_contract: bool = False) -> Dict[str, Any]:
        """获取价格限制

        Args:
            symbol: 期货代码
            prev_close: 前结算价
            is_new_contract: 是否为新合约

        Returns:
            价格限制字典
        """
        if prev_close is None:
            return {
                'upper_limit': float('inf'),
                'lower_limit': 0.0,
                'limit_ratio': None,
                'has_limit': False,
            }

        return FuturePriceCalculator.calculate_limit_prices(
            symbol, prev_close, self._current_date, is_new_contract
        )

    def get_lot_size(self, symbol: str) -> int:
        """获取交易单位（手数）

        Args:
            symbol: 期货代码

        Returns:
            交易单位
        """
        # 期货最小交易单位为1手
        return 1

    def get_tick_size(self, symbol: str) -> float:
        """获取最小变动价位

        Args:
            symbol: 期货代码

        Returns:
            最小变动价位
        """
        return get_tick_size(symbol)

    def get_contract_size(self, symbol: str) -> int:
        """获取合约单位

        Args:
            symbol: 期货代码

        Returns:
            合约单位
        """
        return get_contract_size(symbol)

    def get_delivery_info(self, symbol: str) -> Dict[str, Any]:
        """获取交割信息

        Args:
            symbol: 期货代码

        Returns:
            交割信息字典
        """
        delivery_year, delivery_month = FutureDeliveryValidator.parse_delivery_month(symbol)

        return {
            'delivery_year': delivery_year,
            'delivery_month': delivery_month,
            'is_delivery_month': FutureDeliveryValidator.is_delivery_month(symbol, self._current_date),
            'is_near_delivery': FutureDeliveryValidator.is_near_delivery_month(symbol, self._current_date),
            'natural_person_ban': FutureDeliveryValidator.check_natural_person_ban(symbol, self._current_date),
        }

    def get_position_limit(self, symbol: str, position_type: str = 'speculator') -> int:
        """获取持仓限制

        Args:
            symbol: 期货代码
            position_type: 持仓类型

        Returns:
            持仓限制（手数）
        """
        return FuturePositionValidator.get_position_limit(symbol, position_type)

    def get_market_info(self, symbol: str = None) -> Dict[str, Any]:
        """获取市场信息

        Args:
            symbol: 期货代码（可选）

        Returns:
            市场信息字典
        """
        if symbol:
            exchange = get_future_exchange(symbol)
            future_type = get_future_type(symbol)
            product = get_future_product(symbol)
            tick_size = get_tick_size(symbol)
            contract_size = get_contract_size(symbol)
            has_night = has_night_session(symbol)

            # 获取涨跌幅信息
            price_limit = get_price_limit(symbol)
        else:
            exchange = 'cffex'
            future_type = 'financial'
            product = 'IF'
            tick_size = 0.2
            contract_size = 300
            has_night = False
            price_limit = 0.10

        return {
            'market_name': self.market_name,
            'exchange': exchange,
            'product': product,
            'contract_type': future_type,
            'timezone': 'Asia/Shanghai',
            'currency': 'CNY',
            'tick_size': tick_size,
            'contract_size': contract_size,
            'has_night_session': has_night,
            't_plus_zero': True,
            'margin_enabled': self.margin_enabled,
            'price_limit': price_limit,
        }
