"""
事件引擎实现
负责管理和生成所有类型的事件，包括市场特定事件和公司行为事件
"""

import heapq
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Callable, Set
from collections import defaultdict

from .base_event import BaseEvent, EventType
from .event_factory import EventFactory
from .event_manager import EventManager


class EventEngine:
    """事件引擎，负责事件的生成、调度和分发"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化事件引擎
        
        Args:
            config: 配置参数
        """
        self.config = config or {}
        self.logger = logging.getLogger("EventEngine")
        
        # 事件队列
        self.event_queue: List[BaseEvent] = []
        
        # 事件工厂和管理器
        self.event_factory = EventFactory()
        self.event_manager = EventManager()
        
        # 事件监听器
        self.event_listeners: Dict[EventType, List[Callable]] = defaultdict(list)
        
        # 事件过滤器
        self.event_filters: List[Callable] = []
        
        # 事件统计
        self.event_stats = {
            'total_generated': 0,
            'total_processed': 0,
            'error_count': 0,
            'type_stats': defaultdict(int)
        }
        
        # 运行状态
        self.is_running = False
        self.context = None
        
        # 初始化
        self._initialize()
    
    def _initialize(self):
        """初始化事件引擎"""
        # 加载事件配置
        self._load_event_config()
        
        # 初始化事件工厂
        self.event_factory.initialize(self.config)
        
        # 初始化事件管理器
        self.event_manager.initialize(self.config)
        
        self.logger.info("事件引擎初始化完成")
    
    def _load_event_config(self):
        """加载事件配置"""
        # 从配置中加载事件相关设置
        self.market = self.config.get('market', 'cn_stock')
        self.frequency = self.config.get('frequency', '1d')
        self.enable_corporate_actions = self.config.get('enable_corporate_actions', True)
        self.enable_market_events = self.config.get('enable_market_events', True)
        self.enable_trade_events = self.config.get('enable_trade_events', True)
        
        # 事件过滤配置
        self.event_filters_config = self.config.get('event_filters', [])
        
        # 加载过滤器
        self._load_event_filters()
    
    def _load_event_filters(self):
        """加载事件过滤器"""
        for filter_config in self.event_filters_config:
            filter_type = filter_config.get('type')
            if filter_type == 'time_range':
                start_time = filter_config.get('start_time')
                end_time = filter_config.get('end_time')
                self.add_time_filter(start_time, end_time)
            elif filter_type == 'event_type':
                allowed_types = filter_config.get('allowed_types', [])
                self.add_event_type_filter(allowed_types)
    
    def initialize(self, context):
        """
        初始化事件引擎
        
        Args:
            context: 回测上下文
        """
        self.context = context
        self.event_factory.context = context
        self.event_manager.context = context
        
        # 设置日志记录器
        if hasattr(context, 'logger'):
            self.logger = context.logger
        
        self.logger.info("事件引擎上下文初始化完成")
    
    def generate_events(self, start_date: datetime, end_date: datetime, 
                       symbols: List[str] = None) -> List[BaseEvent]:
        """
        生成指定时间范围内的所有事件
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            symbols: 股票代码列表
            
        Returns:
            List[BaseEvent]: 生成的事件列表
        """
        events = []
        
        # 生成交易日列表
        trading_days = self._get_trading_days(start_date, end_date)
        
        for trade_date in trading_days:
            # 生成市场事件
            if self.enable_market_events:
                market_events = self.event_factory.create_market_events(
                    trade_date, self.market, self.frequency
                )
                events.extend(market_events)
            
            # 生成公司行为事件
            if self.enable_corporate_actions and symbols:
                corporate_events = self.event_factory.create_corporate_action_events(
                    trade_date, symbols, self.market
                )
                events.extend(corporate_events)
            
            # 生成交易事件（如果有挂单等）
            if self.enable_trade_events:
                trade_events = self.event_factory.create_trade_events(
                    trade_date, self.market
                )
                events.extend(trade_events)
        
        # 应用事件过滤器
        filtered_events = self._apply_filters(events)
        
        # 按时间排序
        filtered_events.sort(key=lambda x: x.event_time)
        
        # 更新统计
        self.event_stats['total_generated'] += len(filtered_events)
        for event in filtered_events:
            self.event_stats['type_stats'][event.event_type.value] += 1
        
        self.logger.info(f"生成事件完成，共 {len(filtered_events)} 个事件")
        
        return filtered_events
    
    def _get_trading_days(self, start_date: datetime, end_date: datetime) -> List[datetime]:
        """获取交易日列表"""
        trading_days = []
        current_date = start_date
        
        while current_date <= end_date:
            # 简化实现：排除周末
            if current_date.weekday() < 5:  # 周一到周五
                trading_days.append(current_date)
            current_date += timedelta(days=1)
        
        return trading_days
    
    def _apply_filters(self, events: List[BaseEvent]) -> List[BaseEvent]:
        """应用事件过滤器"""
        filtered_events = events
        
        for event_filter in self.event_filters:
            filtered_events = [event for event in filtered_events if event_filter(event)]
        
        return filtered_events
    
    def add_event(self, event: BaseEvent):
        """添加事件到队列"""
        heapq.heappush(self.event_queue, event)
    
    def add_events(self, events: List[BaseEvent]):
        """批量添加事件到队列"""
        for event in events:
            self.add_event(event)
    
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
    
    def process_event(self, event: BaseEvent) -> bool:
        """
        处理单个事件
        
        Args:
            event: 要处理的事件
            
        Returns:
            bool: 是否处理成功
        """
        try:
            # 更新统计
            self.event_stats['total_processed'] += 1
            
            # 调用事件监听器
            self._notify_listeners(event)
            
            # 让事件自己处理
            result = event.process(self.context)
            
            if not result:
                self.event_stats['error_count'] += 1
                self.logger.warning(f"事件处理失败: {event.event_type.value}")
            
            return result
            
        except Exception as e:
            self.event_stats['error_count'] += 1
            self.logger.error(f"处理事件异常: {event.event_type.value}, 错误: {str(e)}")
            return False
    
    def _notify_listeners(self, event: BaseEvent):
        """通知事件监听器"""
        listeners = self.event_listeners.get(event.event_type, [])
        for listener in listeners:
            try:
                listener(event)
            except Exception as e:
                self.logger.error(f"事件监听器异常: {str(e)}")
    
    def add_event_listener(self, event_type: EventType, listener: Callable):
        """添加事件监听器"""
        self.event_listeners[event_type].append(listener)
    
    def remove_event_listener(self, event_type: EventType, listener: Callable):
        """移除事件监听器"""
        if listener in self.event_listeners[event_type]:
            self.event_listeners[event_type].remove(listener)
    
    def add_time_filter(self, start_time: str, end_time: str):
        """添加时间过滤器"""
        def time_filter(event: BaseEvent) -> bool:
            event_time_str = event.event_time.strftime('%H:%M:%S')
            return start_time <= event_time_str <= end_time
        
        self.event_filters.append(time_filter)
    
    def add_event_type_filter(self, allowed_types: List[str]):
        """添加事件类型过滤器"""
        allowed_event_types = {EventType(event_type) for event_type in allowed_types}
        
        def type_filter(event: BaseEvent) -> bool:
            return event.event_type in allowed_event_types
        
        self.event_filters.append(type_filter)
    
    def add_market_filter(self, allowed_markets: List[str]):
        """添加市场过滤器"""
        def market_filter(event: BaseEvent) -> bool:
            return event.market in allowed_markets
        
        self.event_filters.append(market_filter)
    
    def get_event_statistics(self) -> Dict[str, Any]:
        """获取事件统计信息"""
        total_generated = self.event_stats['total_generated']
        total_processed = self.event_stats['total_processed']
        
        stats = {
            'total_generated': total_generated,
            'total_processed': total_processed,
            'pending_events': len(self.event_queue),
            'error_count': self.event_stats['error_count'],
            'success_rate': (total_processed - self.event_stats['error_count']) / max(1, total_processed),
            'type_statistics': dict(self.event_stats['type_stats']),
            'listeners_count': {event_type.value: len(listeners) 
                              for event_type, listeners in self.event_listeners.items()},
            'filters_count': len(self.event_filters)
        }
        
        return stats
    
    def export_events_log(self, events: List[BaseEvent], file_path: str):
        """导出事件日志"""
        try:
            import json
            from pathlib import Path
            
            events_data = []
            for event in events:
                events_data.append(event.to_dict())
            
            log_data = {
                'events': events_data,
                'statistics': self.get_event_statistics(),
                'config': self.config,
                'export_time': datetime.now().isoformat()
            }
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(log_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"事件日志已导出到: {file_path}")
            
        except Exception as e:
            self.logger.error(f"导出事件日志失败: {str(e)}")
    
    def start(self):
        """启动事件引擎"""
        self.is_running = True
        self.logger.info("事件引擎已启动")
    
    def stop(self):
        """停止事件引擎"""
        self.is_running = False
        self.logger.info("事件引擎已停止")
    
    def reset(self):
        """重置事件引擎"""
        self.clear_events()
        self.event_stats = {
            'total_generated': 0,
            'total_processed': 0,
            'error_count': 0,
            'type_stats': defaultdict(int)
        }
        self.logger.info("事件引擎已重置")
    
    def __str__(self):
        return f"EventEngine(market={self.market}, frequency={self.frequency}, events={len(self.event_queue)})" 