"""
美股市场特定事件实现
包括美股特有的公司行为事件和市场事件
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from ..base_event import BaseEvent, EventType
from ..market_events import MarketEvent
from ..corporate_action_events import (
    DividendStockSplitEvent, StockSplitEvent, TradingSuspensionEvent
)


class UsStockEventFactory:
    """美股市场事件工厂"""
    
    def __init__(self):
        self.logger = logging.getLogger("UsStockEventFactory")
        self.config = {}
        
        # 美股交易时段（东部时间）
        self.trading_sessions = [
            {
                'name': 'pre_market',
                'start_time': '04:00:00',
                'end_time': '09:30:00',
            },
            {
                'name': 'regular',
                'start_time': '09:30:00',
                'end_time': '16:00:00',
            },
            {
                'name': 'after_hours',
                'start_time': '16:00:00',
                'end_time': '20:00:00',
            }
        ]
        
        # 美股节假日
        self.holidays = [
            '2024-01-01',  # New Year's Day
            '2024-01-15',  # Martin Luther King Jr. Day
            '2024-02-19',  # Presidents' Day
            '2024-03-29',  # Good Friday
            '2024-05-27',  # Memorial Day
            '2024-06-19',  # Juneteenth
            '2024-07-04',  # Independence Day
            '2024-09-02',  # Labor Day
            '2024-11-28',  # Thanksgiving
            '2024-12-25',  # Christmas Day
        ]
    
    def initialize(self, config: Dict[str, Any]):
        """初始化工厂"""
        self.config = config
        custom_holidays = config.get('us_stock_holidays', [])
        if custom_holidays:
            self.holidays.extend(custom_holidays)
        self.logger.info("美股事件工厂初始化完成")
    
    def create_market_events(self, trade_date: datetime, frequency: str) -> List[BaseEvent]:
        """创建美股市场事件"""
        events = []
        
        if not self._is_trading_day(trade_date):
            return events
        
        # 创建美股特有的市场事件
        events.extend(self._create_us_market_events(trade_date, frequency))
        
        return events
    
    def create_corporate_action_events(self, trade_date: datetime, symbols: List[str]) -> List[BaseEvent]:
        """创建美股公司行为事件"""
        events = []
        
        # 美股的公司行为事件处理
        for symbol in symbols:
            if self._should_create_dividend_event(trade_date, symbol):
                event = self._create_us_dividend_event(trade_date, symbol)
                if event:
                    events.append(event)
            
            if self._should_create_split_event(trade_date, symbol):
                event = self._create_us_split_event(trade_date, symbol)
                if event:
                    events.append(event)
        
        return events
    
    def create_trade_events(self, trade_date: datetime) -> List[BaseEvent]:
        """创建美股交易事件"""
        return []
    
    def _is_trading_day(self, date: datetime) -> bool:
        """检查是否为美股交易日"""
        if date.weekday() >= 5:
            return False
        
        date_str = date.strftime('%Y-%m-%d')
        if date_str in self.holidays:
            return False
        
        return True
    
    def _create_us_market_events(self, trade_date: datetime, frequency: str) -> List[BaseEvent]:
        """创建美股市场事件"""
        from ..market_events import StartIntervalEvent, BeforeMarketEvent, AfterMarketEvent
        
        events = []
        
        # 基本市场事件
        start_time = trade_date.replace(hour=0, minute=0, second=0, microsecond=0)
        events.append(StartIntervalEvent(start_time, 'us_stock'))
        
        # 盘前交易开始
        pre_market_time = trade_date.replace(hour=4, minute=0, second=0, microsecond=0)
        events.append(UsPreMarketStartEvent(pre_market_time))
        
        # 常规交易时间
        before_market_time = trade_date.replace(hour=9, minute=30, second=0, microsecond=0)
        events.append(BeforeMarketEvent(before_market_time, 'us_stock'))
        
        # 盘后交易
        after_hours_time = trade_date.replace(hour=16, minute=0, second=0, microsecond=0)
        events.append(UsAfterHoursStartEvent(after_hours_time))
        
        after_market_time = trade_date.replace(hour=20, minute=0, second=0, microsecond=0)
        events.append(AfterMarketEvent(after_market_time, 'us_stock'))
        
        return events
    
    def _should_create_dividend_event(self, trade_date: datetime, symbol: str) -> bool:
        """判断是否创建美股分红事件"""
        import random
        return random.random() < 0.0008
    
    def _create_us_dividend_event(self, trade_date: datetime, symbol: str) -> Optional[DividendStockSplitEvent]:
        """创建美股分红事件"""
        try:
            return DividendStockSplitEvent(
                event_time=trade_date,
                symbol=symbol,
                dividend_ratio=0.25,  # 美元
                split_ratio=0.0,      # 美股通常分红不送股
                ex_date=trade_date,
                market='us_stock'
            )
        except Exception as e:
            self.logger.error(f"创建美股分红事件失败: {str(e)}")
            return None
    
    def _should_create_split_event(self, trade_date: datetime, symbol: str) -> bool:
        """判断是否创建美股拆股事件"""
        import random
        return random.random() < 0.0002
    
    def _create_us_split_event(self, trade_date: datetime, symbol: str) -> Optional[StockSplitEvent]:
        """创建美股拆股事件"""
        try:
            return StockSplitEvent(
                event_time=trade_date,
                symbol=symbol,
                split_ratio=2.0,  # 1拆2
                ex_date=trade_date,
                market='us_stock'
            )
        except Exception as e:
            self.logger.error(f"创建美股拆股事件失败: {str(e)}")
            return None


class UsPreMarketStartEvent(MarketEvent):
    """美股盘前交易开始事件"""
    
    def __init__(self, event_time: datetime, adapter_id: str = "default"):
        super().__init__(EventType.PRE_OPENING_START, event_time, 'us_stock', adapter_id)


class UsAfterHoursStartEvent(MarketEvent):
    """美股盘后交易开始事件"""
    
    def __init__(self, event_time: datetime, adapter_id: str = "default"):
        super().__init__(EventType.AFTER_MARKET, event_time, 'us_stock', adapter_id) 