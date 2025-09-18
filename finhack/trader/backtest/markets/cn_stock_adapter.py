"""
CN股票市场适配器

负责生成A股市场的事件列表
"""

from datetime import datetime, time, timedelta
from typing import List, Dict, Any
from ..events.event_types import EventTypeEnum, MarketEvent, EventPriorityEnum


class CnStockMarketAdapter:
    """A股市场适配器"""
    
    def __init__(self):
        """初始化市场适配器"""
        self.market_name = "cn_stock"
        
        # A股市场事件时间表
        self.event_schedule = {
            EventTypeEnum.DAY_START: time(0, 0, 0),
            EventTypeEnum.BEFORE_MARKET: time(9, 0, 0),
            EventTypeEnum.PRE_OPENING_START: time(9, 15, 0),
            EventTypeEnum.PRE_OPENING_END: time(9, 20, 0),
            EventTypeEnum.MATCHING_START: time(9, 25, 0),
            EventTypeEnum.OPENING_PRICE_DETERMINED: time(9, 25, 0),
            EventTypeEnum.MARKET_START: time(9, 30, 0),
            EventTypeEnum.MORNING_END: time(11, 30, 0),
            EventTypeEnum.AFTERNOON_START: time(13, 0, 0),
            EventTypeEnum.CLOSING_START: time(14, 57, 0),
            EventTypeEnum.CLOSING_END: time(15, 0, 0),
            EventTypeEnum.CLOSING_PRICE_DETERMINED: time(15, 0, 0),
            EventTypeEnum.MARKET_END: time(15, 0, 0),
            EventTypeEnum.DAILY_BAR_CLOSED: time(15, 0, 0),
            EventTypeEnum.AFTER_MARKET: time(18, 0, 0),
            EventTypeEnum.DAY_END: time(23, 59, 59),
        }
        
        # 撮合时间点 (1d频率)
        self.match_times_1d = [
            time(9, 30, 0),   # 开盘撮合
            time(15, 0, 0),   # 收盘撮合
        ]
        
    def generate_daily_events(self, trade_date: datetime, frequency: str = '1d') -> List[MarketEvent]:
        """生成指定日期的市场事件列表
        
        Args:
            trade_date: 交易日期
            frequency: 数据频率
            
        Returns:
            List[MarketEvent]: 事件列表
        """
        events = []
        
        # 生成静态市场事件
        for event_type, event_time in self.event_schedule.items():
            event_datetime = datetime.combine(trade_date.date(), event_time)
            
            event = MarketEvent(
                event_type=event_type,
                event_time=event_datetime,
                market=self.market_name,
                frequency=frequency,
                priority=self._get_event_priority(event_type),
                data={'description': f"A股市场{event_type.value}事件"}
            )
            events.append(event)
        
        # 生成撮合事件
        if frequency == '1d':
            for match_time in self.match_times_1d:
                match_datetime = datetime.combine(trade_date.date(), match_time)
                
                match_event = MarketEvent(
                    event_type=EventTypeEnum.TRY_MATCH,
                    event_time=match_datetime,
                    market=self.market_name,
                    frequency=frequency,
                    priority=EventPriorityEnum.HIGH,
                    data={'description': "订单撮合事件"}
                )
                events.append(match_event)
                
        elif frequency == '1m':
            # 分钟级撮合：上午9:30-11:30，下午13:00-15:00，每分钟一次
            morning_start = datetime.combine(trade_date.date(), time(9, 30, 0))
            morning_end = datetime.combine(trade_date.date(), time(11, 30, 0))
            afternoon_start = datetime.combine(trade_date.date(), time(13, 0, 0))
            afternoon_end = datetime.combine(trade_date.date(), time(15, 0, 0))
            
            # 上午时段
            current_time = morning_start
            while current_time <= morning_end:
                match_event = MarketEvent(
                    event_type=EventTypeEnum.TRY_MATCH,
                    event_time=current_time,
                    market=self.market_name,
                    frequency=frequency,
                    priority=EventPriorityEnum.HIGH,
                    data={'description': "分钟级撮合事件"}
                )
                events.append(match_event)
                current_time += timedelta(minutes=1)
            
            # 下午时段
            current_time = afternoon_start
            while current_time <= afternoon_end:
                match_event = MarketEvent(
                    event_type=EventTypeEnum.TRY_MATCH,
                    event_time=current_time,
                    market=self.market_name,
                    frequency=frequency,
                    priority=EventPriorityEnum.HIGH,
                    data={'description': "分钟级撮合事件"}
                )
                events.append(match_event)
                current_time += timedelta(minutes=1)
        
        # 按时间和优先级排序
        events.sort(key=lambda x: (x.event_time, x.priority.value))
        
        return events
    
    def _get_event_priority(self, event_type: EventTypeEnum) -> EventPriorityEnum:
        """获取事件优先级"""
        high_priority_events = {
            EventTypeEnum.DAY_START,
            EventTypeEnum.BEFORE_MARKET,
            EventTypeEnum.MARKET_START,
            EventTypeEnum.MARKET_END,
            EventTypeEnum.DAILY_BAR_CLOSED,
            EventTypeEnum.AFTER_MARKET,
            EventTypeEnum.DAY_END,
        }
        
        if event_type in high_priority_events:
            return EventPriorityEnum.HIGH
        else:
            return EventPriorityEnum.NORMAL
    
    def is_trading_time(self, current_time: datetime, frequency: str = '1d') -> bool:
        """判断是否在交易时间内
        
        Args:
            current_time: 当前时间
            frequency: 数据频率
            
        Returns:
            bool: 是否在交易时间内
        """
        time_obj = current_time.time()
        
        # A股交易时间：9:30-11:30, 13:00-15:00
        morning_start = time(9, 30, 0)
        morning_end = time(11, 30, 0)
        afternoon_start = time(13, 0, 0)
        afternoon_end = time(15, 0, 0)
        
        return (morning_start <= time_obj <= morning_end) or \
               (afternoon_start <= time_obj <= afternoon_end)
    
    def get_trading_sessions(self, trade_date: datetime) -> List[Dict[str, datetime]]:
        """获取交易时段
        
        Args:
            trade_date: 交易日期
            
        Returns:
            List[Dict]: 交易时段列表
        """
        sessions = [
            {
                'name': 'morning',
                'start': datetime.combine(trade_date.date(), time(9, 30, 0)),
                'end': datetime.combine(trade_date.date(), time(11, 30, 0))
            },
            {
                'name': 'afternoon', 
                'start': datetime.combine(trade_date.date(), time(13, 0, 0)),
                'end': datetime.combine(trade_date.date(), time(15, 0, 0))
            }
        ]
        return sessions 