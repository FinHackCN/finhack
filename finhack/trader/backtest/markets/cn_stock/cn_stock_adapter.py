"""
中国股票市场适配器

实现中国A股市场的交易规则和事件生成，支持多频次
"""

from typing import List, Dict, Any, Tuple
from datetime import datetime, date, time, timedelta
import logging

from ..base_market import BaseMarket
from ..base_minutely_events import BaseMinutelyEventGenerator
from ..base_daily_events import BaseDailyEventGenerator
from ...events.event_types import BaseEvent, MarketEvent, EventTypeEnum

logger = logging.getLogger(__name__)


class CnStockMarketAdapter(BaseMarket):
    """中国股票市场适配器
    
    支持A股、基金、可转债等的交易规则
    """
    
    def __init__(self, config: Dict[str, Any] = None):
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
        """获取中国股票市场默认配置"""
        return {
            'market_name': 'cn_stock',
            'supported_frequencies': ['1d', '1m', '30m', '120m'],
            'timezone': 'Asia/Shanghai',
            'currency': 'CNY',
            'trading_schedule': {
                '1d': {
                    'morning_start': '09:30',
                    'morning_end': '11:30',
                    'afternoon_start': '13:00',
                    'afternoon_end': '15:00'
                },
                '1m': {
                    'morning_start': '09:30',
                    'morning_end': '11:30',
                    'afternoon_start': '13:00',
                    'afternoon_end': '15:00'
                },
                '30m': {
                    'morning_start': '09:30',
                    'morning_end': '11:30',
                    'afternoon_start': '13:00',
                    'afternoon_end': '15:00'
                },
                '120m': {
                    'morning_start': '09:30',
                    'morning_end': '11:30',
                    'afternoon_start': '13:00',
                    'afternoon_end': '15:00'
                }
            },
            'trading_rules': {
                'commission': {
                    'stock': {
                        'open_commission': 0.0003,
                        'close_commission': 0.0003,
                        'open_tax': 0.0,
                        'close_tax': 0.001,  # 印花税
                        'min_commission': 5.0
                    }
                },
                'slippage': {
                    'slip_type': 'pricerelated',
                    'slip_value': 0.001
                },
                'limits': {
                    'lot_size': 100,  # 最小交易单位
                    'min_order_volume': 100,  # 最小下单数量
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
            # ========== 日频事件 - 使用1d精简事件序列 ==========
            # 1d频率仅加载日频数据，只有开盘和收盘进行撮合
            events = self._generate_1d_events(trade_date)

        elif frequency in ['1m', '30m', '120m']:
            # ========== 分钟频事件 - 使用统一框架 ==========
            # 基础事件由统一框架生成
            events = BaseMinutelyEventGenerator.generate_minutely_events(
                adapter=self,
                trade_date=trade_date,
                frequency=frequency
            )

            # 添加中国A股特有的集合竞价事件
            additional_events = self._get_auction_events(trade_date, frequency)

            # 添加细分时段事件
            additional_events.extend(self._get_session_events(trade_date, frequency))

            # 添加收盘相关事件
            additional_events.extend(self._get_closing_events(trade_date, frequency))

            # 添加K线事件（如果需要）
            kline_events = self._get_kline_events(trade_date, frequency)
            additional_events.extend(kline_events)

            events.extend(additional_events)
            events.sort(key=lambda x: x.event_time)

        return events

    def _generate_1d_events(self, trade_date: date) -> List[BaseEvent]:
        """生成1d频率的精简事件序列

        1d频率回测特点：
        - 仅加载日频数据
        - 只有开盘和收盘进行撮合
        - 不生成盘中分钟事件
        - 不生成午休分段事件

        事件序列：
        09:00 DAY_START              每日开始（初始化、分红送股处理等）
        09:00 BEFORE_MARKET          盘前准备（可自定义事件）
        09:25 OPENING_PRICE_DETERMINED 开盘价确定
        09:25 TRY_MATCH              开盘集合竞价撮合 ← 可交易
        -- 盘中无事件 --
        14:55 CLOSING_START          收盘集合竞价开始
        15:00 CLOSING_PRICE_DETERMINED 收盘价确定
        15:00 TRY_MATCH              收盘集合竞价撮合 ← 可交易
        15:05 AFTER_MARKET           盘后处理（可自定义事件）
        15:05 DAY_END                每日结束（净值记录等）

        Args:
            trade_date: 交易日期

        Returns:
            List[BaseEvent]: 精简的事件列表
        """
        events = []

        # 1. 每日开始（初始化、分红送股处理等）
        events.append(MarketEvent(
            event_type=EventTypeEnum.DAY_START,
            event_time=datetime.combine(trade_date, time(9, 0)),
            market=self.market_name,
            frequency='1d',
            event_description="每日开始"
        ))

        # 2. 盘前准备（策略初始化、调仓准备）
        events.append(MarketEvent(
            event_type=EventTypeEnum.BEFORE_MARKET,
            event_time=datetime.combine(trade_date, time(9, 0)),
            market=self.market_name,
            frequency='1d',
            event_description="盘前准备"
        ))

        # 3. 开盘集合竞价开始
        events.append(MarketEvent(
            event_type=EventTypeEnum.PRE_OPENING_START,
            event_time=datetime.combine(trade_date, time(9, 15)),
            market=self.market_name,
            frequency='1d',
            event_description="集合竞价开始"
        ))

        # 4. 开盘集合竞价不可撤单
        events.append(MarketEvent(
            event_type=EventTypeEnum.PRE_OPENING_END,
            event_time=datetime.combine(trade_date, time(9, 20)),
            market=self.market_name,
            frequency='1d',
            event_description="集合竞价不可撤单"
        ))

        # 5. 开盘价确定
        events.append(MarketEvent(
            event_type=EventTypeEnum.OPENING_PRICE_DETERMINED,
            event_time=datetime.combine(trade_date, time(9, 25)),
            market=self.market_name,
            frequency='1d',
            event_description="开盘价确定"
        ))

        # 6. 开盘集合竞价撮合 ← 可交易
        events.append(MarketEvent(
            event_type=EventTypeEnum.TRY_MATCH,
            event_time=datetime.combine(trade_date, time(9, 25)),
            market=self.market_name,
            frequency='1d',
            event_description="开盘集合竞价撮合"
        ))

        # -- 盘中无事件 --

        # 7. 收盘集合竞价开始
        events.append(MarketEvent(
            event_type=EventTypeEnum.CLOSING_START,
            event_time=datetime.combine(trade_date, time(14, 57)),
            market=self.market_name,
            frequency='1d',
            event_description="收盘集合竞价开始"
        ))

        # 8. 收盘价确定
        events.append(MarketEvent(
            event_type=EventTypeEnum.CLOSING_PRICE_DETERMINED,
            event_time=datetime.combine(trade_date, time(15, 0)),
            market=self.market_name,
            frequency='1d',
            event_description="收盘价确定"
        ))

        # 9. 收盘集合竞价撮合 ← 可交易
        events.append(MarketEvent(
            event_type=EventTypeEnum.TRY_MATCH,
            event_time=datetime.combine(trade_date, time(15, 0)),
            market=self.market_name,
            frequency='1d',
            event_description="收盘集合竞价撮合"
        ))

        # 10. 收盘结束
        events.append(MarketEvent(
            event_type=EventTypeEnum.CLOSING_END,
            event_time=datetime.combine(trade_date, time(15, 0)),
            market=self.market_name,
            frequency='1d',
            event_description="收盘集合竞价结束"
        ))

        # 11. 盘后处理（可自定义事件）
        events.append(MarketEvent(
            event_type=EventTypeEnum.AFTER_MARKET,
            event_time=datetime.combine(trade_date, time(15, 5)),
            market=self.market_name,
            frequency='1d',
            event_description="盘后处理"
        ))

        # 12. 每日结束（净值记录等）
        events.append(MarketEvent(
            event_type=EventTypeEnum.DAY_END,
            event_time=datetime.combine(trade_date, time(15, 5)),
            market=self.market_name,
            frequency='1d',
            event_description="每日结束"
        ))

        return events

    def _get_auction_events(self, trade_date: date, frequency: str) -> List[BaseEvent]:
        """生成A股集合竞价相关事件"""
        events = []

        # 集合竞价开始
        events.append(MarketEvent(
            event_type=EventTypeEnum.PRE_OPENING_START,
            event_time=datetime.combine(trade_date, time(9, 15)),
            market=self.market_name,
            frequency=frequency,
            event_description="集合竞价开始"
        ))

        # 集合竞价可撤单结束
        events.append(MarketEvent(
            event_type=EventTypeEnum.PRE_OPENING_END,
            event_time=datetime.combine(trade_date, time(9, 20)),
            market=self.market_name,
            frequency=frequency,
            event_description="集合竞价不可撤单"
        ))

        # 集合竞价撮合
        events.append(MarketEvent(
            event_type=EventTypeEnum.MATCHING_START,
            event_time=datetime.combine(trade_date, time(9, 25)),
            market=self.market_name,
            frequency=frequency,
            event_description="集合竞价撮合"
        ))

        # 开盘价确定
        events.append(MarketEvent(
            event_type=EventTypeEnum.OPENING_PRICE_DETERMINED,
            event_time=datetime.combine(trade_date, time(9, 25)),
            market=self.market_name,
            frequency=frequency,
            event_description="开盘价确定"
        ))

        return events

    def _get_session_events(self, trade_date: date, frequency: str) -> List[BaseEvent]:
        """生成分段时段事件（午休分段）"""
        events = []

        # 上午收盘
        events.append(MarketEvent(
            event_type=EventTypeEnum.MORNING_END,
            event_time=datetime.combine(trade_date, time(11, 30)),
            market=self.market_name,
            frequency=frequency,
            event_description="上午收盘"
        ))

        # 下午开盘
        events.append(MarketEvent(
            event_type=EventTypeEnum.AFTERNOON_START,
            event_time=datetime.combine(trade_date, time(13, 0)),
            market=self.market_name,
            frequency=frequency,
            event_description="下午开盘"
        ))

        return events

    def _get_closing_events(self, trade_date: date, frequency: str) -> List[BaseEvent]:
        """生成收盘相关事件"""
        events = []

        # 收盘集合竞价开始
        events.append(MarketEvent(
            event_type=EventTypeEnum.CLOSING_START,
            event_time=datetime.combine(trade_date, time(14, 57)),
            market=self.market_name,
            frequency=frequency,
            event_description="收盘集合竞价开始"
        ))

        # 收盘结束
        events.append(MarketEvent(
            event_type=EventTypeEnum.CLOSING_END,
            event_time=datetime.combine(trade_date, time(15, 0)),
            market=self.market_name,
            frequency=frequency,
            event_description="收盘集合竞价结束"
        ))

        # 收盘价确定
        events.append(MarketEvent(
            event_type=EventTypeEnum.CLOSING_PRICE_DETERMINED,
            event_time=datetime.combine(trade_date, time(15, 0)),
            market=self.market_name,
            frequency=frequency,
            event_description="收盘价确定"
        ))

        return events

    def _get_kline_events(self, trade_date: date, frequency: str) -> List[BaseEvent]:
        """生成K线事件（可选，某些策略可能依赖这些事件）"""
        events = []

        # 确定间隔
        interval_minutes = 1 if frequency == '1m' else (30 if frequency == '30m' else 120)

        # 获取交易时段
        trading_sessions = self.get_trading_sessions(trade_date, frequency)

        # 为每个交易时段生成K线事件
        for session_start, session_end in trading_sessions:
            start_dt = datetime.combine(trade_date, session_start)
            end_dt = datetime.combine(trade_date, session_end)
            current_time = start_dt

            while current_time <= end_dt:
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
                current_time += timedelta(minutes=interval_minutes)

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
        
        # 中国A股交易时段
        morning_session = (time(9, 30), time(11, 30))
        afternoon_session = (time(13, 0), time(15, 0))
        
        return [morning_session, afternoon_session]


# 向后兼容的别名
CnStockAdapter = CnStockMarketAdapter


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