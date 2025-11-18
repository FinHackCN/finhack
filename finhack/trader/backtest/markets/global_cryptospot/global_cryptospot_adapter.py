"""
全球加密货币现货市场适配器

实现全球加密货币现货市场的交易规则和事件生成，支持多频次
"""

from typing import List, Dict, Any, Tuple
from datetime import datetime, date, time, timedelta
import logging

from ..base_market import BaseMarket
from ...events.event_types import BaseEvent, MarketEvent, EventTypeEnum

logger = logging.getLogger(__name__)


class GlobalCryptoSpotAdapter(BaseMarket):
    """全球加密货币现货市场适配器"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.market_name = 'global_cryptospot'
        
        # 加密货币市场配置 - 24/7交易
        self.config = {
            'market': 'global_cryptospot',
            'trading_hours': {
                '1d': {
                    'morning_start': '00:00',
                    'morning_end': '23:59',
                    'afternoon_start': '00:00',
                    'afternoon_end': '23:59'
                },
                '1m': {
                    'morning_start': '00:00',
                    'morning_end': '23:59',
                    'afternoon_start': '00:00',
                    'afternoon_end': '23:59'
                },
                '30m': {
                    'morning_start': '00:00',
                    'morning_end': '23:59',
                    'afternoon_start': '00:00',
                    'afternoon_end': '23:59'
                },
                '120m': {
                    'morning_start': '00:00',
                    'morning_end': '23:59',
                    'afternoon_start': '00:00',
                    'afternoon_end': '23:59'
                }
            },
            'trading_rules': {
                'commission': {
                    'crypto': {
                        'open_commission': 0.001,  # 0.1% 开仓手续费
                        'close_commission': 0.001,  # 0.1% 平仓手续费
                        'open_tax': 0.0,
                        'close_tax': 0.0,  # 加密货币无印花税
                        'min_commission': 0.0
                    }
                },
                'slippage': {
                    'slip_type': 'pricerelated',
                    'slip_value': 0.0005  # 0.05% 滑点
                },
                'limits': {
                    'lot_size': 0.00000001,  # 最小交易单位
                    'min_order_volume': 0.00000001,  # 最小下单数量
                    'max_order_volume': 1000000  # 最大下单数量
                }
            }
        }
        
        super().__init__(self.market_name, self.config)
        
        logger.info(f"全球加密货币现货市场适配器初始化完成，支持频率: {self.supported_frequencies}")
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取全球加密货币现货市场默认配置"""
        return {
            'market_name': 'global_cryptospot',
            'supported_frequencies': ['1d', '1m', '30m', '120m'],
            'timezone': 'UTC',
            'currency': 'USD',
            'trading_schedule': {
                '1d': {
                    'morning_start': '00:00',
                    'morning_end': '23:59',
                    'afternoon_start': '00:00',
                    'afternoon_end': '23:59'
                },
                '1m': {
                    'morning_start': '00:00',
                    'morning_end': '23:59',
                    'afternoon_start': '00:00',
                    'afternoon_end': '23:59'
                },
                '30m': {
                    'morning_start': '00:00',
                    'morning_end': '23:59',
                    'afternoon_start': '00:00',
                    'afternoon_end': '23:59'
                },
                '120m': {
                    'morning_start': '00:00',
                    'morning_end': '23:59',
                    'afternoon_start': '00:00',
                    'afternoon_end': '23:59'
                }
            },
            'trading_rules': {
                'commission': {
                    'crypto': {
                        'open_commission': 0.001,  # 0.1% 开仓手续费
                        'close_commission': 0.001,  # 0.1% 平仓手续费
                        'open_tax': 0.0,
                        'close_tax': 0.0,  # 加密货币无印花税
                        'min_commission': 0.0
                    }
                },
                'slippage': {
                    'slip_type': 'pricerelated',
                    'slip_value': 0.0005  # 0.05% 滑点
                },
                'limits': {
                    'lot_size': 0.00000001,  # 最小交易单位
                    'min_order_volume': 0.00000001,  # 最小下单数量
                    'max_order_volume': 1000000  # 最大下单数量
                }
            }
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
        
        if frequency == '1d':
            # 日频事件
            # 交易前事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.BEFORE_MARKET,
                event_time=datetime.combine(trade_date, time(0, 0)), # 假设UTC 00:00 对应本地 08:00
                market=self.market_name,
                frequency=frequency,
                event_description="交易前准备"
            ))
            
            # 开盘事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_START,
                event_time=datetime.combine(trade_date, time(0, 0)), # 假设UTC 00:00 对应本地 08:00
                market=self.market_name,
                frequency=frequency,
                event_description="开盘"
            ))
            
            # 撮合事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.TRY_MATCH,
                event_time=datetime.combine(trade_date, time(0, 0)), # 假设UTC 00:00 对应本地 08:00
                market=self.market_name,
                frequency=frequency,
                event_description="日级撮合"
            ))
            
            # 收盘事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_END,
                event_time=datetime.combine(trade_date, time(23, 59)), # 假设UTC 23:59 对应本地 07:59
                market=self.market_name,
                frequency=frequency,
                event_description="收盘"
            ))
            
            # 交易后事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.AFTER_MARKET,
                event_time=datetime.combine(trade_date, time(23, 59)), # 假设UTC 23:59 对应本地 07:59
                market=self.market_name,
                frequency=frequency,
                event_description="交易后处理"
            ))
            
            # 日K线事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.DAILY_BAR_CLOSED,
                event_time=datetime.combine(trade_date, time(23, 59)), # 假设UTC 23:59 对应本地 07:59
                market=self.market_name,
                frequency=frequency,
                event_description="日K线生成"
            ))
            
        elif frequency in ['1m', '30m', '120m']:
            # 分钟频事件
            # 日开始事件 - 在交易前触发，供策略初始化和调仓
            events.append(MarketEvent(
                event_type=EventTypeEnum.DAY_START,
                event_time=datetime.combine(trade_date, time(0, 0)), # 假设UTC 00:00 对应本地 08:00
                market=self.market_name,
                frequency=frequency,
                event_description="日开始"
            ))
            
            # 交易前事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.BEFORE_MARKET,
                event_time=datetime.combine(trade_date, time(0, 0)), # 假设UTC 00:00 对应本地 08:00
                market=self.market_name,
                frequency=frequency,
                event_description="交易前准备"
            ))
            
            # 开盘事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_START,
                event_time=datetime.combine(trade_date, time(0, 0)), # 假设UTC 00:00 对应本地 08:00
                market=self.market_name,
                frequency=frequency,
                event_description="开盘"
            ))
            
            # 根据频率生成K线事件
            interval_minutes = 1 if frequency == '1m' else (30 if frequency == '30m' else 120)
            
            # 上午交易时段
            morning_start = datetime.combine(trade_date, time(0, 0)) # 假设UTC 00:00 对应本地 08:00
            morning_end = datetime.combine(trade_date, time(23, 59)) # 假设UTC 23:59 对应本地 07:59
            current_time = morning_start
            
            while current_time <= morning_end:
                # 先生成K线事件
                if frequency == '1m':
                    events.append(MarketEvent(
                        event_type=EventTypeEnum.MARKET_BAR_1M,
                        event_time=current_time,
                        market=self.market_name,
                        frequency=frequency,
                        event_description="1分钟K线"
                    ))
                elif frequency == '30m':
                    events.append(MarketEvent(
                        event_type=EventTypeEnum.MARKET_BAR_30M,
                        event_time=current_time,
                        market=self.market_name,
                        frequency=frequency,
                        event_description="30分钟K线"
                    ))
                elif frequency == '120m':
                    events.append(MarketEvent(
                        event_type=EventTypeEnum.MARKET_BAR_120M,
                        event_time=current_time,
                        market=self.market_name,
                        frequency=frequency,
                        event_description="120分钟K线"
                    ))
                
                # 再生成撮合事件
                events.append(MarketEvent(
                    event_type=EventTypeEnum.TRY_MATCH,
                    event_time=current_time,
                    market=self.market_name,
                    frequency=frequency,
                    event_description=f"{frequency}级撮合"
                ))
                current_time += timedelta(minutes=interval_minutes)
            
            # 下午交易时段
            afternoon_start = datetime.combine(trade_date, time(0, 0)) # 假设UTC 00:00 对应本地 08:00
            afternoon_end = datetime.combine(trade_date, time(23, 59)) # 假设UTC 23:59 对应本地 07:59
            current_time = afternoon_start
            
            while current_time <= afternoon_end:
                # 先生成K线事件
                if frequency == '1m':
                    events.append(MarketEvent(
                        event_type=EventTypeEnum.MARKET_BAR_1M,
                        event_time=current_time,
                        market=self.market_name,
                        frequency=frequency,
                        event_description="1分钟K线"
                    ))
                elif frequency == '30m':
                    events.append(MarketEvent(
                        event_type=EventTypeEnum.MARKET_BAR_30M,
                        event_time=current_time,
                        market=self.market_name,
                        frequency=frequency,
                        event_description="30分钟K线"
                    ))
                elif frequency == '120m':
                    events.append(MarketEvent(
                        event_type=EventTypeEnum.MARKET_BAR_120M,
                        event_time=current_time,
                        market=self.market_name,
                        frequency=frequency,
                        event_description="120分钟K线"
                    ))
                
                # 再生成撮合事件
                events.append(MarketEvent(
                    event_type=EventTypeEnum.TRY_MATCH,
                    event_time=current_time,
                    market=self.market_name,
                    frequency=frequency,
                    event_description=f"{frequency}级撮合"
                ))
                current_time += timedelta(minutes=interval_minutes)
            
            # 收盘事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_END,
                event_time=datetime.combine(trade_date, time(23, 59)), # 假设UTC 23:59 对应本地 07:59
                market=self.market_name,
                frequency=frequency,
                event_description="收盘"
            ))
            
            # 交易后事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.AFTER_MARKET,
                event_time=datetime.combine(trade_date, time(23, 59)), # 假设UTC 23:59 对应本地 07:59
                market=self.market_name,
                frequency=frequency,
                event_description="交易后处理"
            ))
            
            # 日终事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.DAY_END,
                event_time=datetime.combine(trade_date, time(23, 59)), # 假设UTC 23:59 对应本地 07:59
                market=self.market_name,
                frequency=frequency,
                event_description="日终处理"
            ))
            
            # 日K线事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.DAILY_BAR_CLOSED,
                event_time=datetime.combine(trade_date, time(23, 59)), # 假设UTC 23:59 对应本地 07:59
                market=self.market_name,
                frequency=frequency,
                event_description="日K线生成"
            ))
        
        return events
    
    def is_trading_time(self, dt: datetime, frequency: str = '1d') -> bool:
        """判断指定时间是否为交易时间
        
        Args:
            dt: 时间
            frequency: 数据频率
            
        Returns:
            bool: 是否为交易时间
        """
        # 检查是否为工作日
        if dt.weekday() >= 5:  # 周六、周日
            return False
        
        # 获取交易时段
        trading_sessions = self.get_trading_sessions(dt.date(), frequency)
        
        # 检查是否在任一交易时段内
        for session_start, session_end in trading_sessions:
            if session_start <= dt.time() <= session_end:
                return True
        
        return False
    
    def get_trading_sessions(self, trade_date: date, frequency: str = '1d') -> List[Tuple[time, time]]:
        """获取交易时段
        
        Args:
            trade_date: 交易日期
            frequency: 数据频率
            
        Returns:
            List[Tuple[time, time]]: 交易时段列表，每个元素为(开始时间, 结束时间)
        """
        # 检查是否为工作日
        if trade_date.weekday() >= 5:  # 周六、周日
            return []
        
        # 加密货币市场交易时段 (24/7)
        return [(time(0, 0), time(23, 59))]


# 向后兼容的别名
GlobalCryptoSpotAdapter = GlobalCryptoSpotAdapter


class CnFundMarketAdapter(CnStockMarketAdapter):
    """中国基金市场适配器
    
    基于中国股票市场规则，但有一些特殊差异
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """初始化中国基金市场适配器"""
        # 如果没有提供配置，使用默认配置
        if not config:
            config = self._get_default_config()
            
        # 修改市场名称
        config['market_name'] = 'cn_fund'
        
        # 基金特有的配置
        config['trading_rules']['commission']['fund'] = {
            'open_commission': 0.0003,
            'close_commission': 0.0003,
            'open_tax': 0.0,
            'close_tax': 0.0,             # 基金免印花税
            'min_commission': 5.0
        }
        
        super().__init__(config)
        
        # 基金特有属性
        self.market_name = 'cn_fund'
        
        logger.info(f"中国基金市场适配器初始化完成，支持频率: {self.supported_frequencies}")
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取基金市场默认配置"""
        config = super()._get_default_config()
        config['market_name'] = 'cn_fund'
        return config
    
    def generate_daily_events(self, trade_date: datetime.date, frequency: str) -> List[BaseEvent]:
        """生成指定交易日和频率的事件列表"""
        if frequency == '1d':
            return self._generate_daily_events_1d(trade_date)
        elif frequency in ['1m', '30m', '120m']:
            return self._generate_daily_events_min(trade_date, frequency)
        else:
            raise ValueError(f"Unsupported frequency for cn_fund: {frequency}")
    
    def _generate_daily_events_1d(self, trade_date: datetime.date) -> List[BaseEvent]:
        """生成1d频率的日内事件"""
        events = []
        
        # 交易前事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.BEFORE_MARKET,
            event_time=datetime.combine(trade_date, time(9, 0)),
            market=self.market_name,
            frequency='1d',
            event_description="基金交易前准备"
        ))
        
        # 开盘事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.MARKET_START,
            event_time=datetime.combine(trade_date, time(9, 30)),
            market=self.market_name,
            frequency='1d',
            event_description="基金开盘"
        ))
        
        # 撮合事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.TRY_MATCH,
            event_time=datetime.combine(trade_date, time(9, 30)),
            market=self.market_name,
            frequency='1d',
            event_description="基金日级撮合"
        ))
        
        # 收盘事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.MARKET_END,
            event_time=datetime.combine(trade_date, time(15, 0)),
            market=self.market_name,
            frequency='1d',
            event_description="基金收盘"
        ))
        
        return events 