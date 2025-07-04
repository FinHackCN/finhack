"""
事件中心实现
"""

import heapq
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Callable
from collections import defaultdict

from ..events.base_event import BaseEvent, EventType
from ..events.market_events import (
    MarketEvent, StartIntervalEvent, BeforeMarketEvent, 
    MorningStartEvent, MorningEndEvent, AfternoonStartEvent, 
    AfternoonEndEvent, AfterMarketEvent, DailyBarClosedEvent, MinuteBarEvent
)
from ..events.user_events import (
    UserEvent, UserDailyEvent, UserHourlyEvent, UserMinutelyEvent,
    UserWeeklyEvent, UserMonthlyEvent, UserIntervalEvent
)
from ..events.trade_events import (
    TradeEvent, OrderSubmissionEvent, OrderFillEvent, 
    OrderCancellationEvent, PositionUpdateEvent
)


class EventCenter:
    """事件中心，负责事件的生成、调度和分发"""
    
    def __init__(self, market: str = "cn_stock"):
        self.market = market
        self.event_queue: List[BaseEvent] = []  # 事件队列（最小堆）
        self.user_schedules: Dict[str, List[Dict]] = defaultdict(list)  # 用户定时任务
        self.current_time: Optional[datetime] = None
        self.trading_sessions = self._get_trading_sessions()
        self.context = None  # 上下文引用
    
    def initialize(self, context):
        """初始化事件中心"""
        self.context = context
        context.logger.info("事件中心初始化完成")
    
    def _get_trading_sessions(self) -> Dict[str, List[Dict]]:
        """获取交易时段配置"""
        sessions = {
            "cn_stock": [
                {
                    "name": "morning",
                    "start_time": "09:30:00",
                    "end_time": "11:30:00",
                    "pre_open": "09:15:00",
                    "pre_close": "11:25:00"
                },
                {
                    "name": "afternoon", 
                    "start_time": "13:00:00",
                    "end_time": "15:00:00",
                    "pre_open": "12:55:00",
                    "pre_close": "14:57:00"
                }
            ],
            "hk_stock": [
                {
                    "name": "morning",
                    "start_time": "09:30:00",
                    "end_time": "12:00:00",
                    "pre_open": "09:15:00",
                    "pre_close": "11:55:00"
                },
                {
                    "name": "afternoon",
                    "start_time": "13:00:00", 
                    "end_time": "16:00:00",
                    "pre_open": "12:55:00",
                    "pre_close": "15:55:00"
                }
            ]
        }
        return sessions.get(self.market, sessions["cn_stock"])
    
    def add_event(self, event: BaseEvent):
        """添加事件到队列"""
        heapq.heappush(self.event_queue, event)
    
    def get_next_event(self) -> Optional[BaseEvent]:
        """获取下一个事件"""
        if self.event_queue:
            return heapq.heappop(self.event_queue)
        return None
    
    def has_events(self) -> bool:
        """检查是否还有事件"""
        return len(self.event_queue) > 0
    
    def clear_events(self):
        """清空事件队列"""
        self.event_queue.clear()
    
    def generate_market_events(self, date: datetime, frequency: str = "1d") -> List[BaseEvent]:
        """
        生成市场事件
        
        Args:
            date: 交易日期
            frequency: 频率 (1d, 1h, 1m, 1s)
            
        Returns:
            List[BaseEvent]: 生成的事件列表
        """
        events = []
        
        # 生成日开始事件
        start_time = date.replace(hour=0, minute=0, second=0, microsecond=0)
        events.append(StartIntervalEvent(start_time, self.market))
        
        # 生成盘前事件
        before_market_time = date.replace(hour=9, minute=0, second=0, microsecond=0)
        events.append(BeforeMarketEvent(before_market_time, self.market))
        
        # 生成交易时段事件
        for session in self.trading_sessions:
            session_name = session["name"]
            start_time_str = session["start_time"]
            end_time_str = session["end_time"]
            
            # 解析时间
            start_hour, start_minute, start_second = map(int, start_time_str.split(':'))
            end_hour, end_minute, end_second = map(int, end_time_str.split(':'))
            
            # 开盘事件
            session_start = date.replace(hour=start_hour, minute=start_minute, second=start_second, microsecond=0)
            if session_name == "morning":
                events.append(MorningStartEvent(session_start, self.market))
            else:
                events.append(AfternoonStartEvent(session_start, self.market))
            
            # 根据频率生成Bar事件
            if frequency in ["1m", "1s"]:
                bar_events = self._generate_bar_events(date, session, frequency)
                events.extend(bar_events)
            
            # 收盘事件
            session_end = date.replace(hour=end_hour, minute=end_minute, second=end_second, microsecond=0)
            if session_name == "morning":
                events.append(MorningEndEvent(session_end, self.market))
            else:
                events.append(AfternoonEndEvent(session_end, self.market))
        
        # 生成盘后事件
        after_market_time = date.replace(hour=18, minute=0, second=0, microsecond=0)
        events.append(AfterMarketEvent(after_market_time, self.market))
        
        # 生成日线收盘事件
        daily_close_time = date.replace(hour=15, minute=0, second=0, microsecond=0)
        
        # 获取当日的K线数据
        bar_data = self._get_daily_bar_data(date)
        events.append(DailyBarClosedEvent(daily_close_time, self.market, bar_data=bar_data))
        
        return events
    
    def _generate_bar_events(self, date: datetime, session: Dict, frequency: str) -> List[BaseEvent]:
        """生成Bar事件"""
        events = []
        
        start_time_str = session["start_time"]
        end_time_str = session["end_time"]
        session_name = session["name"]
        
        # 解析时间
        start_hour, start_minute, start_second = map(int, start_time_str.split(':'))
        end_hour, end_minute, end_second = map(int, end_time_str.split(':'))
        
        current_time = date.replace(hour=start_hour, minute=start_minute, second=start_second, microsecond=0)
        end_time = date.replace(hour=end_hour, minute=end_minute, second=end_second, microsecond=0)
        
        # 计算时间增量
        if frequency == "1m":
            delta = timedelta(minutes=1)
        elif frequency == "1s":
            delta = timedelta(seconds=1)
        else:
            return events
        
        # 生成Bar事件
        while current_time < end_time:
            events.append(MinuteBarEvent(current_time, self.market, session=session_name))
            current_time += delta
        
        return events
    
    def register_user_schedule(self, schedule_type: str, time_str: str, 
                              callback: Callable, **kwargs):
        """
        注册用户定时任务
        
        Args:
            schedule_type: 调度类型 (daily, hourly, minutely, weekly, monthly, interval)
            time_str: 时间字符串 (如 "09:30:00")
            callback: 回调函数
            **kwargs: 其他参数
        """
        schedule_info = {
            "type": schedule_type,
            "time_str": time_str,
            "callback": callback,
            "kwargs": kwargs
        }
        self.user_schedules[schedule_type].append(schedule_info)
    
    def generate_user_events(self, date: datetime) -> List[BaseEvent]:
        """
        生成用户事件
        
        Args:
            date: 交易日期
            
        Returns:
            List[BaseEvent]: 生成的用户事件列表
        """
        events = []
        
        # 生成日级别事件
        for schedule in self.user_schedules["daily"]:
            time_str = schedule["time_str"]
            callback = schedule["callback"]
            
            # 解析时间
            if ":" in time_str:
                time_parts = time_str.split(":")
                hour = int(time_parts[0])
                minute = int(time_parts[1]) if len(time_parts) > 1 else 0
                second = int(time_parts[2]) if len(time_parts) > 2 else 0
            else:
                hour, minute, second = 9, 30, 0  # 默认开盘时间
            
            event_time = date.replace(hour=hour, minute=minute, second=second, microsecond=0)
            events.append(UserDailyEvent(event_time, callback, self.market))
        
        # 生成小时级别事件
        for schedule in self.user_schedules["hourly"]:
            callback = schedule["callback"]
            minute = schedule["kwargs"].get("minute", 0)
            
            # 在交易时间内每小时生成事件
            for session in self.trading_sessions:
                start_hour = int(session["start_time"].split(':')[0])
                end_hour = int(session["end_time"].split(':')[0])
                
                for hour in range(start_hour, end_hour + 1):
                    event_time = date.replace(hour=hour, minute=minute, second=0, microsecond=0)
                    # 检查是否在交易时间内
                    if self._is_trading_time(event_time, session):
                        events.append(UserHourlyEvent(event_time, callback, self.market))
        
        # 生成分钟级别事件
        for schedule in self.user_schedules["minutely"]:
            callback = schedule["callback"]
            interval = schedule["kwargs"].get("interval", 1)  # 默认每分钟
            
            # 在交易时间内按间隔生成事件
            for session in self.trading_sessions:
                start_time_str = session["start_time"]
                end_time_str = session["end_time"]
                
                start_hour, start_minute, _ = map(int, start_time_str.split(':'))
                end_hour, end_minute, _ = map(int, end_time_str.split(':'))
                
                current_time = date.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
                end_time = date.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0)
                
                while current_time < end_time:
                    events.append(UserMinutelyEvent(current_time, callback, self.market))
                    current_time += timedelta(minutes=interval)
        
        return events
    
    def _is_trading_time(self, check_time: datetime, session: Dict) -> bool:
        """检查是否在交易时间内"""
        start_time_str = session["start_time"]
        end_time_str = session["end_time"]
        
        start_hour, start_minute, start_second = map(int, start_time_str.split(':'))
        end_hour, end_minute, end_second = map(int, end_time_str.split(':'))
        
        start_time = check_time.replace(hour=start_hour, minute=start_minute, second=start_second)
        end_time = check_time.replace(hour=end_hour, minute=end_minute, second=end_second)
        
        return start_time <= check_time <= end_time
    
    def generate_daily_events(self, date: datetime, frequency: str = "1d") -> List[BaseEvent]:
        """
        生成某日的所有事件
        
        Args:
            date: 交易日期
            frequency: 频率
            
        Returns:
            List[BaseEvent]: 生成的事件列表
        """
        events = []
        
        # 生成市场事件
        market_events = self.generate_market_events(date, frequency)
        events.extend(market_events)
        
        # 生成用户事件
        user_events = self.generate_user_events(date)
        events.extend(user_events)
        
        # 按时间排序
        events.sort(key=lambda x: x.event_time)
        
        return events
    
    def add_events_to_queue(self, events: List[BaseEvent]):
        """批量添加事件到队列"""
        for event in events:
            self.add_event(event)
    
    # 事件处理器方法
    def handle_start_interval(self, context, event: BaseEvent):
        """处理开始区间事件"""
        context.logger.info(f"新的交易日开始: {event.event_time.strftime('%Y-%m-%d')}")
        # 更新交易日状态
        context.current_date = event.event_time.date()
        
    def handle_before_market(self, context, event: BaseEvent):
        """处理盘前事件"""
        context.logger.info(f"盘前准备: {event.event_time.strftime('%Y-%m-%d %H:%M:%S')}")
        # 盘前准备工作，如数据预加载等
        
    def handle_morning_start(self, context, event: BaseEvent):
        """处理上午开盘事件"""
        context.logger.info(f"上午开盘: {event.event_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
    def handle_morning_end(self, context, event: BaseEvent):
        """处理上午收盘事件"""
        context.logger.info(f"上午收盘: {event.event_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
    def handle_afternoon_start(self, context, event: BaseEvent):
        """处理下午开盘事件"""
        context.logger.info(f"下午开盘: {event.event_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
    def handle_afternoon_end(self, context, event: BaseEvent):
        """处理下午收盘事件"""
        context.logger.info(f"下午收盘: {event.event_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
    def handle_after_market(self, context, event: BaseEvent):
        """处理盘后事件"""
        context.logger.info(f"盘后处理: {event.event_time.strftime('%Y-%m-%d %H:%M:%S')}")
        # 盘后处理工作，如清算、结算等
        
    def handle_daily_bar_closed(self, context, event: BaseEvent):
        """处理日线收盘事件"""
        context.logger.info(f"日线数据完成: {event.event_time.strftime('%Y-%m-%d %H:%M:%S')}")
        # 更新日线数据，计算收益等
    
    def _get_daily_bar_data(self, date: datetime) -> Dict[str, Any]:
        """
        获取当日K线数据
        
        Args:
            date: 交易日期
            
        Returns:
            Dict[str, Any]: K线数据
        """
        try:
            if self.context and self.context.data_center:
                # 从数据中心获取K线数据
                date_str = date.strftime('%Y-%m-%d')
                
                # 这里可以根据实际需要获取多个股票的数据
                # 暂时返回模拟的日线数据
                bar_data = {
                    'date': date_str,
                    'open': 10.0,
                    'high': 10.5,
                    'low': 9.5,
                    'close': 10.2,
                    'volume': 1000000,
                    'amount': 10200000
                }
                
                return bar_data
            else:
                # 返回默认的模拟数据
                return {
                    'date': date.strftime('%Y-%m-%d'),
                    'open': 10.0,
                    'high': 10.5,
                    'low': 9.5,
                    'close': 10.2,
                    'volume': 1000000,
                    'amount': 10200000
                }
        except Exception as e:
            # 如果获取数据失败，返回默认数据
            return {
                'date': date.strftime('%Y-%m-%d'),
                'open': 10.0,
                'high': 10.5,
                'low': 9.5,
                'close': 10.2,
                'volume': 1000000,
                'amount': 10200000
            } 