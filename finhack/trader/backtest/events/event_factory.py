"""
事件工厂实现
负责创建各种类型的事件，包括市场事件、公司行为事件、交易事件等
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Callable
from collections import defaultdict

from .base_event import BaseEvent, EventType
from .market_events import MarketEvent
from .trade_events import TradeEvent
from .corporate_action_events import CorporateActionEvent
from .markets.cn_stock_events import CnStockEventFactory
from .markets.hk_stock_events import HkStockEventFactory
from .markets.us_stock_events import UsStockEventFactory


class EventFactory:
    """事件工厂，负责创建各种类型的事件"""
    
    def __init__(self):
        self.logger = logging.getLogger("EventFactory")
        self.context = None
        self.config = {}
        
        # 市场特定事件工厂
        self.market_factories = {
            'cn_stock': CnStockEventFactory(),
            'hk_stock': HkStockEventFactory(),
            'us_stock': UsStockEventFactory()
        }
        
        # 事件创建统计
        self.creation_stats = defaultdict(int)
    
    def initialize(self, config: Dict[str, Any]):
        """
        初始化事件工厂
        
        Args:
            config: 配置参数
        """
        self.config = config
        
        # 初始化市场特定工厂
        for market, factory in self.market_factories.items():
            factory.initialize(config)
        
        self.logger.info("事件工厂初始化完成")
    
    def create_market_events(self, trade_date: datetime, market: str, 
                           frequency: str) -> List[BaseEvent]:
        """
        创建市场事件
        
        Args:
            trade_date: 交易日期
            market: 市场标识
            frequency: 频率
            
        Returns:
            List[BaseEvent]: 市场事件列表
        """
        events = []
        
        try:
            # 使用市场特定工厂
            if market in self.market_factories:
                factory = self.market_factories[market]
                market_events = factory.create_market_events(trade_date, frequency)
                events.extend(market_events)
            else:
                # 使用通用市场事件
                events.extend(self._create_common_market_events(trade_date, market, frequency))
            
            # 更新统计
            self.creation_stats['market_events'] += len(events)
            
            return events
            
        except Exception as e:
            self.logger.error(f"创建市场事件失败: {str(e)}")
            return []
    
    def create_corporate_action_events(self, trade_date: datetime, symbols: List[str], 
                                     market: str) -> List[BaseEvent]:
        """
        创建公司行为事件
        
        Args:
            trade_date: 交易日期
            symbols: 股票代码列表
            market: 市场标识
            
        Returns:
            List[BaseEvent]: 公司行为事件列表
        """
        events = []
        
        try:
            # 使用市场特定工厂
            if market in self.market_factories:
                factory = self.market_factories[market]
                corporate_events = factory.create_corporate_action_events(trade_date, symbols)
                events.extend(corporate_events)
            else:
                # 使用通用公司行为事件
                events.extend(self._create_common_corporate_action_events(trade_date, symbols, market))
            
            # 更新统计
            self.creation_stats['corporate_action_events'] += len(events)
            
            return events
            
        except Exception as e:
            self.logger.error(f"创建公司行为事件失败: {str(e)}")
            return []
    
    def create_trade_events(self, trade_date: datetime, market: str) -> List[BaseEvent]:
        """
        创建交易事件
        
        Args:
            trade_date: 交易日期
            market: 市场标识
            
        Returns:
            List[BaseEvent]: 交易事件列表
        """
        events = []
        
        try:
            # 使用市场特定工厂
            if market in self.market_factories:
                factory = self.market_factories[market]
                trade_events = factory.create_trade_events(trade_date)
                events.extend(trade_events)
            else:
                # 使用通用交易事件
                events.extend(self._create_common_trade_events(trade_date, market))
            
            # 更新统计
            self.creation_stats['trade_events'] += len(events)
            
            return events
            
        except Exception as e:
            self.logger.error(f"创建交易事件失败: {str(e)}")
            return []
    
    def create_user_events(self, trade_date: datetime, market: str, 
                          user_schedules: Dict[str, List[Dict]]) -> List[BaseEvent]:
        """
        创建用户自定义事件
        
        Args:
            trade_date: 交易日期
            market: 市场标识
            user_schedules: 用户调度配置
            
        Returns:
            List[BaseEvent]: 用户事件列表
        """
        events = []
        
        try:
            from .user_events import UserDailyEvent, UserHourlyEvent, UserMinutelyEvent
            
            # 创建日级别事件
            for schedule in user_schedules.get('daily', []):
                time_str = schedule.get('time_str', '09:30:00')
                callback = schedule.get('callback')
                
                if callback:
                    event_time = self._parse_time_string(trade_date, time_str)
                    events.append(UserDailyEvent(event_time, callback, market))
            
            # 创建小时级别事件
            for schedule in user_schedules.get('hourly', []):
                callback = schedule.get('callback')
                minute = schedule.get('minute', 0)
                
                if callback:
                    # 在交易时间内每小时生成事件
                    trading_sessions = self._get_trading_sessions(market)
                    for session in trading_sessions:
                        start_hour = int(session['start_time'].split(':')[0])
                        end_hour = int(session['end_time'].split(':')[0])
                        
                        for hour in range(start_hour, end_hour + 1):
                            event_time = trade_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
                            events.append(UserHourlyEvent(event_time, callback, market))
            
            # 创建分钟级别事件
            for schedule in user_schedules.get('minutely', []):
                callback = schedule.get('callback')
                interval = schedule.get('interval', 1)
                
                if callback:
                    # 在交易时间内按间隔生成事件
                    trading_sessions = self._get_trading_sessions(market)
                    for session in trading_sessions:
                        start_time = self._parse_time_string(trade_date, session['start_time'])
                        end_time = self._parse_time_string(trade_date, session['end_time'])
                        
                        current_time = start_time
                        while current_time < end_time:
                            events.append(UserMinutelyEvent(current_time, callback, market))
                            current_time += timedelta(minutes=interval)
            
            # 更新统计
            self.creation_stats['user_events'] += len(events)
            
            return events
            
        except Exception as e:
            self.logger.error(f"创建用户事件失败: {str(e)}")
            return []
    
    def _create_common_market_events(self, trade_date: datetime, market: str, 
                                   frequency: str) -> List[BaseEvent]:
        """创建通用市场事件"""
        from .market_events import (
            StartIntervalEvent, BeforeMarketEvent, MorningStartEvent, 
            MorningEndEvent, AfternoonStartEvent, AfternoonEndEvent, 
            AfterMarketEvent, DailyBarClosedEvent, MinuteBarEvent
        )
        
        events = []
        
        # 生成日开始事件
        start_time = trade_date.replace(hour=0, minute=0, second=0, microsecond=0)
        events.append(StartIntervalEvent(start_time, market))
        
        # 生成盘前事件
        before_market_time = trade_date.replace(hour=9, minute=0, second=0, microsecond=0)
        events.append(BeforeMarketEvent(before_market_time, market))
        
        # 获取交易时段
        trading_sessions = self._get_trading_sessions(market)
        
        # 生成交易时段事件
        for session in trading_sessions:
            start_time = self._parse_time_string(trade_date, session['start_time'])
            end_time = self._parse_time_string(trade_date, session['end_time'])
            
            # 开盘事件
            if session['name'] == 'morning':
                events.append(MorningStartEvent(start_time, market))
            else:
                events.append(AfternoonStartEvent(start_time, market))
            
            # 根据频率生成Bar事件
            if frequency in ['1m', '1s']:
                bar_events = self._create_bar_events(trade_date, session, frequency, market)
                events.extend(bar_events)
            
            # 收盘事件
            if session['name'] == 'morning':
                events.append(MorningEndEvent(end_time, market))
            else:
                events.append(AfternoonEndEvent(end_time, market))
        
        # 生成盘后事件
        after_market_time = trade_date.replace(hour=18, minute=0, second=0, microsecond=0)
        events.append(AfterMarketEvent(after_market_time, market))
        
        # 生成日线收盘事件
        daily_close_time = trade_date.replace(hour=15, minute=0, second=0, microsecond=0)
        events.append(DailyBarClosedEvent(daily_close_time, market))
        
        return events
    
    def _create_common_corporate_action_events(self, trade_date: datetime, symbols: List[str], 
                                             market: str) -> List[BaseEvent]:
        """创建通用公司行为事件"""
        events = []
        
        # 这里可以从数据库或配置文件中获取公司行为数据
        # 暂时返回空列表，实际实现需要查询数据源
        
        return events
    
    def _create_common_trade_events(self, trade_date: datetime, market: str) -> List[BaseEvent]:
        """创建通用交易事件"""
        events = []
        
        # 这里可以根据现有订单创建交易事件
        # 暂时返回空列表，实际实现需要查询交易状态
        
        return events
    
    def _create_bar_events(self, trade_date: datetime, session: Dict, frequency: str, 
                          market: str) -> List[BaseEvent]:
        """创建Bar事件"""
        from .market_events import MinuteBarEvent
        
        events = []
        
        start_time = self._parse_time_string(trade_date, session['start_time'])
        end_time = self._parse_time_string(trade_date, session['end_time'])
        session_name = session['name']
        
        # 计算时间增量
        if frequency == '1m':
            delta = timedelta(minutes=1)
        elif frequency == '1s':
            delta = timedelta(seconds=1)
        else:
            return events
        
        # 生成Bar事件
        current_time = start_time
        while current_time < end_time:
            events.append(MinuteBarEvent(current_time, market, session=session_name))
            current_time += delta
        
        return events
    
    def _get_trading_sessions(self, market: str) -> List[Dict]:
        """获取交易时段配置"""
        sessions = {
            'cn_stock': [
                {
                    'name': 'morning',
                    'start_time': '09:30:00',
                    'end_time': '11:30:00'
                },
                {
                    'name': 'afternoon',
                    'start_time': '13:00:00',
                    'end_time': '15:00:00'
                }
            ],
            'hk_stock': [
                {
                    'name': 'morning',
                    'start_time': '09:30:00',
                    'end_time': '12:00:00'
                },
                {
                    'name': 'afternoon',
                    'start_time': '13:00:00',
                    'end_time': '16:00:00'
                }
            ],
            'us_stock': [
                {
                    'name': 'regular',
                    'start_time': '09:30:00',
                    'end_time': '16:00:00'
                }
            ]
        }
        
        return sessions.get(market, sessions['cn_stock'])
    
    def _parse_time_string(self, date: datetime, time_str: str) -> datetime:
        """解析时间字符串"""
        time_parts = time_str.split(':')
        hour = int(time_parts[0])
        minute = int(time_parts[1]) if len(time_parts) > 1 else 0
        second = int(time_parts[2]) if len(time_parts) > 2 else 0
        
        return date.replace(hour=hour, minute=minute, second=second, microsecond=0)
    
    def add_market_factory(self, market: str, factory):
        """添加市场特定工厂"""
        self.market_factories[market] = factory
        factory.initialize(self.config)
    
    def get_creation_statistics(self) -> Dict[str, int]:
        """获取事件创建统计"""
        return dict(self.creation_stats)
    
    def reset_statistics(self):
        """重置统计信息"""
        self.creation_stats.clear()
    
    def __str__(self):
        return f"EventFactory(markets={list(self.market_factories.keys())})" 