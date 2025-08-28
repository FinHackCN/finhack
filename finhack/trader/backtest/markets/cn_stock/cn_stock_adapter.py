"""
中国股票市场适配器

实现中国A股市场的交易规则和事件生成，支持多频次
"""

from typing import List, Dict, Any
from datetime import datetime, date, time, timedelta
import logging

from ..base_market import BaseMarket
from ...events.event_types import BaseEvent, MarketEvent, EventTypeEnum

logger = logging.getLogger(__name__)


class CnStockAdapter(BaseMarket):
    """中国股票市场适配器
    
    支持A股、基金、可转债等的交易规则
    """
    
    def __init__(self, config: Dict[str, Any]):
        """初始化中国股票市场适配器
        
        Args:
            config: 市场配置，如果为空则加载默认配置
        """
        # 如果没有提供配置，使用默认配置
        if not config:
            config = self._get_default_config()
            
        super().__init__('cn_stock', config)
        
        # 中国股票市场特有配置
        self.t_plus_one = config.get('t_plus_one', True)  # T+1制度
        self.price_limit_enabled = config.get('price_limit_enabled', True)  # 涨跌停限制
        self.daily_price_limit = config.get('daily_price_limit', 0.10)  # 10%涨跌停
        
        logger.info(f"中国股票市场适配器初始化完成，支持频率: {self.supported_frequencies}")
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            'supported_frequencies': ['1d', '1m'],
            'timezone': 'Asia/Shanghai',
            'currency': 'CNY',
            'trading_schedule': {
                '1d': {
                    'DAY_START': '00:00:00',
                    'BEFORE_MARKET': '09:00:00',
                    'PRE_OPENING_START': '09:15:00',
                    'PRE_OPENING_END': '09:20:00',
                    'MATCHING_START': '09:25:00',
                    'OPENING_PRICE_DETERMINED': '09:25:00',
                    'MARKET_START': '09:30:00',
                    'MORNING_END': '11:30:00',
                    'AFTERNOON_START': '13:00:00',
                    'CLOSING_START': '14:57:00',
                    'CLOSING_END': '15:00:00',
                    'CLOSING_PRICE_DETERMINED': '15:00:00',
                    'MARKET_END': '15:00:00',
                    'DAILY_BAR_CLOSED': '15:00:00',
                    'AFTER_MARKET': '18:00:00',
                    'DAY_END': '23:59:59'
                },
                '1m': {
                    'DAY_START': '00:00:00',
                    'BEFORE_MARKET': '09:00:00',
                    'MARKET_START': '09:30:00',
                    'MORNING_END': '11:30:00',
                    'AFTERNOON_START': '13:00:00',
                    'MARKET_END': '15:00:00',
                    'AFTER_MARKET': '18:00:00',
                    'DAY_END': '23:59:59'
                }
            },
            'trading_rules': {
                'commission': {
                    'stock': {
                        'open_commission': 0.0003,    # 万分之三
                        'close_commission': 0.0003,   # 万分之三
                        'open_tax': 0.0,              # 买入免税
                        'close_tax': 0.001,           # 卖出印花税千分之一
                        'min_commission': 5.0         # 最低5元
                    },
                    'fund': {
                        'open_commission': 0.0003,
                        'close_commission': 0.0003,
                        'open_tax': 0.0,
                        'close_tax': 0.0,             # 基金免印花税
                        'min_commission': 5.0
                    }
                },
                'slippage': {
                    'slip_type': 'pricerelated',
                    'slip_value': 0.001            # 0.1%滑点
                },
                'limits': {
                    'lot_size': 100,               # 100股一手
                    'min_order_volume': 100,       # 最小100股
                    'max_order_volume': 1000000    # 最大100万股
                }
            },
            't_plus_one': True,
            'price_limit_enabled': True,
            'daily_price_limit': 0.10
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
                
            event_type = EventTypeEnum(event_name)
            event_dt = datetime.combine(trade_date, event_time)
            
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
        basic_events = ['BEFORE_MARKET', 'MARKET_START', 'MORNING_END', 
                       'AFTERNOON_START', 'MARKET_END', 'AFTER_MARKET']
        
        for event_name in basic_events:
            if event_name in schedule:
                event_type = EventTypeEnum(event_name)
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
        for start_time, end_time in trading_sessions:
            current_time = start_time
            while current_time < end_time:
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
        
        return events
    
    def _get_event_description(self, event_name: str) -> str:
        """获取事件描述"""
        descriptions = {
            'BEFORE_MARKET': '盘前准备阶段',
            'PRE_OPENING_START': '集合竞价开始',
            'PRE_OPENING_END': '集合竞价不可撤单',
            'MATCHING_START': '集合竞价撮合',
            'OPENING_PRICE_DETERMINED': '开盘价确定',
            'MARKET_START': '上午连续竞价开始',
            'MORNING_END': '上午交易结束',
            'AFTERNOON_START': '下午连续竞价开始',
            'CLOSING_START': '尾盘集合竞价开始',
            'CLOSING_END': '尾盘集合竞价结束',
            'CLOSING_PRICE_DETERMINED': '收盘价确定',
            'MARKET_END': '当日交易结束',
            'DAILY_BAR_CLOSED': '日线数据生成',
            'AFTER_MARKET': '盘后处理阶段',
            'TRY_MATCH': '撮合处理'
        }
        return descriptions.get(event_name, event_name)
    
    def is_trading_time(self, dt: datetime, frequency: str = '1d') -> bool:
        """判断指定时间是否为交易时间
        
        Args:
            dt: 时间
            frequency: 数据频率
            
        Returns:
            bool: 是否为交易时间
        """
        schedule = self.trading_schedule.get(frequency, {})
        if not schedule:
            return False
        
        current_time = dt.time()
        
        if frequency == '1d':
            # 日线频率：开盘到收盘
            market_start = schedule.get('MARKET_START')
            market_end = schedule.get('MARKET_END')
            
            if market_start and market_end:
                return market_start <= current_time <= market_end
                
        elif frequency == '1m':
            # 分钟线频率：需要检查具体的交易时段
            trading_sessions = self.get_trading_sessions(dt.date(), frequency)
            for start_dt, end_dt in trading_sessions:
                if start_dt <= dt <= end_dt:
                    return True
        
        return False
    
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
            market_start = schedule.get('MARKET_START')
            market_end = schedule.get('MARKET_END')
            
            if market_start and market_end:
                start_dt = datetime.combine(trade_date, market_start)
                end_dt = datetime.combine(trade_date, market_end)
                sessions.append((start_dt, end_dt))
                
        elif frequency == '1m':
            # 分钟线：上午和下午两个时段
            market_start = schedule.get('MARKET_START')
            morning_end = schedule.get('MORNING_END')
            afternoon_start = schedule.get('AFTERNOON_START')
            market_end = schedule.get('MARKET_END')
            
            if all([market_start, morning_end, afternoon_start, market_end]):
                # 上午时段
                morning_start = datetime.combine(trade_date, market_start)
                morning_end_dt = datetime.combine(trade_date, morning_end)
                sessions.append((morning_start, morning_end_dt))
                
                # 下午时段
                afternoon_start_dt = datetime.combine(trade_date, afternoon_start)
                afternoon_end = datetime.combine(trade_date, market_end)
                sessions.append((afternoon_start_dt, afternoon_end))
        
        return sessions
    
    def validate_order(self, symbol: str, volume: float, price: float, 
                      side: str) -> tuple[bool, str]:
        """验证订单是否符合中国股票市场规则
        
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
        
        # A股特有验证
        
        # 检查手数（必须是100的整数倍）
        lot_size = self.get_lot_size(symbol)
        if volume % lot_size != 0:
            return False, f"交易数量必须是{lot_size}股的整数倍"
        
        # 检查涨跌停（需要昨日收盘价，这里暂时跳过，在实际撮合时检查）
        
        return True, ""
    
    def is_t_plus_one_restricted(self, symbol: str, trade_date: date, buy_date: date) -> bool:
        """检查是否受T+1限制
        
        Args:
            symbol: 交易标的
            trade_date: 交易日期
            buy_date: 买入日期
            
        Returns:
            bool: 是否受T+1限制（当日买入的股票当日不能卖出）
        """
        if not self.t_plus_one:
            return False
        
        return trade_date == buy_date
    
    def get_price_limits(self, symbol: str, prev_close: float) -> tuple[float, float]:
        """获取涨跌停价格限制
        
        Args:
            symbol: 交易标的
            prev_close: 昨日收盘价
            
        Returns:
            tuple[float, float]: (跌停价, 涨停价)
        """
        if not self.price_limit_enabled:
            return 0.0, float('inf')
        
        limit_ratio = self.daily_price_limit
        
        # 特殊板块的涨跌停限制
        if symbol.startswith('30') or symbol.startswith('68'):  # 创业板、科创板
            limit_ratio = 0.20  # 20%
        elif 'ST' in symbol or 'st' in symbol:  # ST股票
            limit_ratio = 0.05  # 5%
        
        down_limit = prev_close * (1 - limit_ratio)
        up_limit = prev_close * (1 + limit_ratio)
        
        # 价格精度处理（保留2位小数）
        down_limit = round(down_limit, 2)
        up_limit = round(up_limit, 2)
        
        return down_limit, up_limit 