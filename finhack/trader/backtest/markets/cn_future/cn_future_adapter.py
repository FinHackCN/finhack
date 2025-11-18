"""
中国期货市场适配器

实现中国期货市场的交易规则和事件生成，支持多频次
"""

from typing import List, Dict, Any
from datetime import datetime, date, time, timedelta
import logging

from ..base_market import BaseMarket
from ...events.event_types import BaseEvent, MarketEvent, EventTypeEnum

logger = logging.getLogger(__name__)


class CnFutureMarketAdapter(BaseMarket):
    """中国期货市场适配器
    
    支持期货市场的交易规则
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """初始化中国期货市场适配器
        
        Args:
            config: 市场配置，如果为空则加载默认配置
        """
        # 如果没有提供配置，使用默认配置
        if not config:
            config = self._get_default_config()
            
        super().__init__('cn_future', config)
        
        # 中国期货市场特有配置
        self.margin_enabled = config.get('margin_enabled', True)  # 保证金制度
        self.t_plus_zero = config.get('t_plus_zero', True)  # T+0制度
        
        logger.info(f"中国期货市场适配器初始化完成，支持频率: {self.supported_frequencies}")
    
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
    
    def get_trading_sessions(self, trade_date: date, frequency: str = '1d') -> List[tuple]:
        """获取交易时段
        
        Args:
            trade_date: 交易日期
            frequency: 数据频率
            
        Returns:
            List[tuple]: 交易时段列表
        """
        schedule = self.trading_schedule.get(frequency, {})
        sessions = []
        
        if frequency == '1d':
            # 日线：整个交易日为一个时段
            day_start = schedule.get('DAY_SESSION_START')
            day_end = schedule.get('DAY_SESSION_END')
            
            if day_start and day_end:
                start_dt = datetime.combine(trade_date, day_start)
                end_dt = datetime.combine(trade_date, day_end)
                sessions.append((start_dt, end_dt))
                
            # 夜盘时段（跨日）
            night_start = schedule.get('NIGHT_SESSION_START')
            night_end = schedule.get('NIGHT_SESSION_END')
            
            if night_start and night_end:
                # 夜盘开始当天
                night_start_dt = datetime.combine(trade_date, night_start)
                # 夜盘结束第二天
                night_end_dt = datetime.combine(trade_date + timedelta(days=1), night_end)
                sessions.append((night_start_dt, night_end_dt))
                
        elif frequency == '1m':
            # 分钟线：日盘和夜盘两个时段
            day_start = schedule.get('DAY_SESSION_START')
            day_end = schedule.get('DAY_SESSION_END')
            
            if day_start and day_end:
                # 日盘时段
                day_start_dt = datetime.combine(trade_date, day_start)
                day_end_dt = datetime.combine(trade_date, day_end)
                sessions.append((day_start_dt, day_end_dt))
                
            # 夜盘时段（跨日）
            night_start = schedule.get('NIGHT_SESSION_START')
            night_end = schedule.get('NIGHT_SESSION_END')
            
            if night_start and night_end:
                # 夜盘开始当天
                night_start_dt = datetime.combine(trade_date, night_start)
                # 夜盘结束第二天
                night_end_dt = datetime.combine(trade_date + timedelta(days=1), night_end)
                sessions.append((night_start_dt, night_end_dt))
        
        return sessions
    
    def is_trading_time(self, dt: datetime, frequency: str = '1d') -> bool:
        """判断指定时间是否为交易时间
        
        Args:
            dt: 时间
            frequency: 数据频率
            
        Returns:
            bool: 是否为交易时间
        """
        # 获取交易时段
        trading_sessions = self.get_trading_sessions(dt.date(), frequency)
        
        # 检查是否在任一交易时段内
        for session_start, session_end in trading_sessions:
            if session_start <= dt <= session_end:
                return True
        
        return False
    
    def validate_order(self, symbol: str, volume: float, price: float, 
                      side: str) -> tuple[bool, str]:
        """验证订单是否符合中国期货市场规则
        
        Args:
            symbol: 交易标的
            volume: 交易数量
            price: 交易价格
            side: 交易方向
            
        Returns:
            tuple[bool, str]: (是否有效, 错误信息)
        """
        # 调用基类验证
        is_valid, error_msg = super().validate_order(symbol, volume, price, side)
        if not is_valid:
            return is_valid, error_msg
        
        # 期货特有验证
        # 检查手数（必须是1的整数倍）
        lot_size = self.get_lot_size(symbol)
        if volume % lot_size != 0:
            return False, f"交易数量必须是{lot_size}手的整数倍"
        
        return True, ""
    
    def is_t_plus_zero_allowed(self, symbol: str) -> bool:
        """检查是否允许T+0交易
        
        Args:
            symbol: 交易标的
            
        Returns:
            bool: 是否允许T+0交易
        """
        return self.t_plus_zero
    
    def get_margin_ratio(self, symbol: str) -> float:
        """获取保证金比例
        
        Args:
            symbol: 交易标的
            
        Returns:
            float: 保证金比例
        """
        if not self.margin_enabled:
            return 0.0
        
        # 默认保证金比例，实际应该根据合约类型和交易所规则确定
        return self.trading_rules.get('margin', {}).get('margin_ratio', 0.10)
