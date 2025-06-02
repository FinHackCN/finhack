import heapq
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set
import logging
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd


class EventType(Enum):
    """事件类型枚举"""
    BAR = "bar"
    MARKET_OPEN = "market_open"
    MARKET_CLOSE = "market_close"
    PRE_MARKET = "pre_market"
    AFTER_MARKET = "after_market"
    AUCTION_START = "auction_start"
    AUCTION_END = "auction_end"
    DIVIDEND = "dividend"
    STOCK_SPLIT = "stock_split"
    TIMER = "timer"
    CUSTOM = "custom"


@dataclass(order=True)
class Event:
    """事件对象"""
    time: datetime
    event_type: EventType = field(compare=False)
    data: Dict[str, Any] = field(default_factory=dict, compare=False)
    handler: Optional[str] = field(default=None, compare=False)
    adapter_id: Optional[str] = field(default=None, compare=False)


class EventCenter:
    """事件中心，负责事件的生成、管理和分发"""
    
    def __init__(self, start_date: str, end_date: str, frequency: str = "1d"):
        """
        初始化事件中心
        
        Args:
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD  
            frequency: 回测频率，1m/5m/15m/30m/60m/1d等
        """
        self.start_date = datetime.strptime(start_date, "%Y%m%d")
        self.end_date = datetime.strptime(end_date, "%Y%m%d")
        self.frequency = frequency
        
        # 事件队列（最小堆）
        self.event_queue: List[Event] = []
        
        # 事件处理器注册
        self.event_handlers: Dict[EventType, Dict[str, List[Callable]]] = {}
        
        # 时间处理器注册
        self.timer_handlers: Dict[str, List[Callable]] = {}
        
        # 交易日历
        self.trading_calendar: List[datetime] = []
        
        # 当前时间
        self.current_time: Optional[datetime] = None
        
        # 适配器列表
        self.adapters: Set[str] = set()
        
        # 日志
        self.logger = logging.getLogger(__name__)
        
    def initialize(self, trading_calendar: List[str]):
        """
        初始化事件中心
        
        Args:
            trading_calendar: 交易日历，日期格式为YYYYMMDD
        """
        # 转换交易日历
        self.trading_calendar = [datetime.strptime(date, "%Y%m%d") for date in trading_calendar]
        
        # 生成预定义事件
        self._generate_market_events()
        self._generate_bar_events()
        
    def _generate_market_events(self):
        """生成市场事件（开盘、收盘、集合竞价等）"""
        for date in self.trading_calendar:
            if date < self.start_date or date > self.end_date:
                continue
                
            # A股市场时间表
            events = [
                (date.replace(hour=9, minute=0), EventType.PRE_MARKET),
                (date.replace(hour=9, minute=15), EventType.AUCTION_START),
                (date.replace(hour=9, minute=25), EventType.AUCTION_END),
                (date.replace(hour=9, minute=30), EventType.MARKET_OPEN),
                (date.replace(hour=11, minute=30), EventType.MARKET_CLOSE),  # 上午收盘
                (date.replace(hour=13, minute=0), EventType.MARKET_OPEN),   # 下午开盘
                (date.replace(hour=15, minute=0), EventType.MARKET_CLOSE),  # 下午收盘
                (date.replace(hour=15, minute=30), EventType.AFTER_MARKET),
            ]
            
            for event_time, event_type in events:
                event = Event(time=event_time, event_type=event_type)
                heapq.heappush(self.event_queue, event)
                
    def _generate_bar_events(self):
        """根据回测频率生成Bar事件"""
        freq_minutes = self._parse_frequency()
        
        for date in self.trading_calendar:
            if date < self.start_date or date > self.end_date:
                continue
                
            # 生成交易时段的Bar事件
            # 上午：9:30 - 11:30
            current = date.replace(hour=9, minute=30)
            morning_end = date.replace(hour=11, minute=30)
            
            while current <= morning_end:
                event = Event(
                    time=current,
                    event_type=EventType.BAR,
                    data={"frequency": self.frequency}
                )
                heapq.heappush(self.event_queue, event)
                current += timedelta(minutes=freq_minutes)
                
            # 下午：13:00 - 15:00
            current = date.replace(hour=13, minute=0)
            afternoon_end = date.replace(hour=15, minute=0)
            
            while current <= afternoon_end:
                event = Event(
                    time=current,
                    event_type=EventType.BAR,
                    data={"frequency": self.frequency}
                )
                heapq.heappush(self.event_queue, event)
                current += timedelta(minutes=freq_minutes)
                
    def _parse_frequency(self) -> int:
        """解析频率字符串，返回分钟数"""
        if self.frequency.endswith('m'):
            return int(self.frequency[:-1])
        elif self.frequency.endswith('d'):
            return 240  # 日线用240分钟表示（4小时交易时间）
        else:
            raise ValueError(f"Unsupported frequency: {self.frequency}")
            
    def register_adapter(self, adapter_id: str):
        """注册适配器"""
        self.adapters.add(adapter_id)
        
    def register_event_handler(self, adapter_id: str, event_type: EventType, handler: Callable):
        """
        注册事件处理器
        
        Args:
            adapter_id: 适配器ID
            event_type: 事件类型
            handler: 处理函数
        """
        if event_type not in self.event_handlers:
            self.event_handlers[event_type] = {}
        if adapter_id not in self.event_handlers[event_type]:
            self.event_handlers[event_type][adapter_id] = []
        self.event_handlers[event_type][adapter_id].append(handler)
        
    def register_timer(self, adapter_id: str, time_str: str, handler: Callable):
        """
        注册定时器
        
        Args:
            adapter_id: 适配器ID
            time_str: 时间字符串，格式 "HH:MM" 或 "HH:MM:SS"
            handler: 处理函数
        """
        key = f"{adapter_id}:{time_str}"
        if key not in self.timer_handlers:
            self.timer_handlers[key] = []
        self.timer_handlers[key].append(handler)
        
        # 为每个交易日生成定时事件
        for date in self.trading_calendar:
            if date < self.start_date or date > self.end_date:
                continue
                
            # 解析时间
            time_parts = time_str.split(":")
            hour = int(time_parts[0])
            minute = int(time_parts[1])
            second = int(time_parts[2]) if len(time_parts) > 2 else 0
            
            event_time = date.replace(hour=hour, minute=minute, second=second)
            event = Event(
                time=event_time,
                event_type=EventType.TIMER,
                data={"time_str": time_str},
                handler=key,
                adapter_id=adapter_id
            )
            heapq.heappush(self.event_queue, event)
            
    def add_custom_event(self, time: datetime, event_type: EventType, data: Dict[str, Any] = None):
        """添加自定义事件"""
        event = Event(time=time, event_type=event_type, data=data or {})
        heapq.heappush(self.event_queue, event)
        
    def add_dividend_event(self, code: str, ex_date: str, dividend_info: Dict[str, Any]):
        """
        添加分红事件
        
        Args:
            code: 股票代码
            ex_date: 除权除息日，格式YYYYMMDD
            dividend_info: 分红信息
        """
        ex_datetime = datetime.strptime(ex_date, "%Y%m%d").replace(hour=9, minute=0)
        event = Event(
            time=ex_datetime,
            event_type=EventType.DIVIDEND,
            data={"code": code, **dividend_info}
        )
        heapq.heappush(self.event_queue, event)
        
    def add_stock_split_event(self, code: str, split_date: str, split_info: Dict[str, Any]):
        """
        添加送股事件
        
        Args:
            code: 股票代码
            split_date: 送股日期，格式YYYYMMDD
            split_info: 送股信息
        """
        split_datetime = datetime.strptime(split_date, "%Y%m%d").replace(hour=9, minute=0)
        event = Event(
            time=split_datetime,
            event_type=EventType.STOCK_SPLIT,
            data={"code": code, **split_info}
        )
        heapq.heappush(self.event_queue, event)
        
    def get_next_event(self) -> Optional[Event]:
        """获取下一个事件"""
        if self.event_queue:
            return heapq.heappop(self.event_queue)
        return None
        
    def dispatch_event(self, event: Event, trade_center: Any, strategies: Dict[str, Any]):
        """
        分发事件给相应的处理器
        
        Args:
            event: 事件对象
            trade_center: 交易中心实例
            strategies: 策略字典 {adapter_id: strategy_instance}
        """
        self.current_time = event.time
        
        # 分发给交易中心
        if hasattr(trade_center, 'on_event'):
            trade_center.on_event(event)
            
        # 处理定时器事件
        if event.event_type == EventType.TIMER and event.handler:
            if event.handler in self.timer_handlers:
                for handler in self.timer_handlers[event.handler]:
                    handler(event.time)
                    
        # 分发给注册的事件处理器
        if event.event_type in self.event_handlers:
            for adapter_id, handlers in self.event_handlers[event.event_type].items():
                if adapter_id in strategies:
                    strategy = strategies[adapter_id]
                    for handler in handlers:
                        handler(event.time, event.data)
                        
    def check_order_trade_updates(self, trade_center: Any, strategies: Dict[str, Any], 
                                 last_orders: Dict[str, List], last_trades: Dict[str, List]):
        """
        检查订单和成交更新，并通知策略
        
        Args:
            trade_center: 交易中心实例
            strategies: 策略字典
            last_orders: 上次的订单状态
            last_trades: 上次的成交状态
        """
        for adapter_id in self.adapters:
            if adapter_id not in strategies:
                continue
                
            strategy = strategies[adapter_id]
            
            # 检查订单更新
            current_orders = trade_center.get_orders(adapter_id)
            if adapter_id in last_orders:
                # 比较订单状态变化
                for order in current_orders:
                    if order not in last_orders[adapter_id]:
                        if hasattr(strategy, 'on_order'):
                            strategy.on_order(order)
            last_orders[adapter_id] = current_orders.copy()
            
            # 检查成交更新
            current_trades = trade_center.get_trades(adapter_id)
            if adapter_id in last_trades:
                # 比较成交记录变化
                for trade in current_trades:
                    if trade not in last_trades[adapter_id]:
                        if hasattr(strategy, 'on_trade'):
                            strategy.on_trade(trade)
            last_trades[adapter_id] = current_trades.copy()
