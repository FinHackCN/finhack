"""
中国基金市场适配器

实现中国A股市场的交易规则和事件生成，支持多频次
"""

from typing import List, Dict, Any, Tuple
from datetime import datetime, date, time, timedelta
import logging

from ..base_market import BaseMarket
from ...events.event_types import BaseEvent, MarketEvent, EventTypeEnum

logger = logging.getLogger(__name__)


class CnFundMarketAdapter(BaseMarket):
    """中国基金市场适配器
    
    支持A股、基金、可转债等的交易规则
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """初始化中国基金市场适配器
        
        Args:
            config: 市场配置，如果为空则加载默认配置
        """
        # 如果没有提供配置，使用默认配置
        if not config:
            config = self._get_default_config()
            
        super().__init__('cn_fund', config)
        
        # 中国股票市场特有配置
        self.t_plus_one = config.get('t_plus_one', True)  # T+1制度
        self.price_limit_enabled = config.get('price_limit_enabled', True)  # 涨跌停限制
        self.daily_price_limit = config.get('daily_price_limit', 0.10)  # 10%涨跌停
        
        logger.info(f"中国基金市场适配器初始化完成，支持频率: {self.supported_frequencies}")
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取中国股票市场默认配置"""
        return {
            'market_name': 'cn_fund',
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
            # 日频事件
            # 交易前事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.BEFORE_MARKET,
                event_time=datetime.combine(trade_date, time(9, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="交易前准备"
            ))
            
            # 开盘事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_START,
                event_time=datetime.combine(trade_date, time(9, 30)),
                market=self.market_name,
                frequency=frequency,
                event_description="开盘"
            ))
            
            # 撮合事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.TRY_MATCH,
                event_time=datetime.combine(trade_date, time(9, 30)),
                market=self.market_name,
                frequency=frequency,
                event_description="日级撮合"
            ))
            
            # 收盘事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_END,
                event_time=datetime.combine(trade_date, time(15, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="收盘"
            ))
            
            # 交易后事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.AFTER_MARKET,
                event_time=datetime.combine(trade_date, time(15, 30)),
                market=self.market_name,
                frequency=frequency,
                event_description="交易后处理"
            ))
            
            # 日K线事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.DAILY_BAR_CLOSED,
                event_time=datetime.combine(trade_date, time(15, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="日K线生成"
            ))
            
        elif frequency in ['1m', '30m', '120m']:
            # 分钟频事件
            # 日开始事件 - 在交易前触发，供策略初始化和调仓
            events.append(MarketEvent(
                event_type=EventTypeEnum.DAY_START,
                event_time=datetime.combine(trade_date, time(9, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="日开始"
            ))
            
            # 交易前事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.BEFORE_MARKET,
                event_time=datetime.combine(trade_date, time(9, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="交易前准备"
            ))
            
            # 开盘事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_START,
                event_time=datetime.combine(trade_date, time(9, 30)),
                market=self.market_name,
                frequency=frequency,
                event_description="开盘"
            ))
            
            # 根据频率生成K线事件
            interval_minutes = 1 if frequency == '1m' else (30 if frequency == '30m' else 120)
            
            # 上午交易时段
            morning_start = datetime.combine(trade_date, time(9, 30))
            morning_end = datetime.combine(trade_date, time(11, 30))
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
            afternoon_start = datetime.combine(trade_date, time(13, 0))
            afternoon_end = datetime.combine(trade_date, time(15, 0))
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
                event_time=datetime.combine(trade_date, time(15, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="收盘"
            ))
            
            # 交易后事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.AFTER_MARKET,
                event_time=datetime.combine(trade_date, time(15, 30)),
                market=self.market_name,
                frequency=frequency,
                event_description="交易后处理"
            ))
            
            # 日终事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.DAY_END,
                event_time=datetime.combine(trade_date, time(23, 59)),
                market=self.market_name,
                frequency=frequency,
                event_description="日终处理"
            ))
            
            # 日K线事件
            events.append(MarketEvent(
                event_type=EventTypeEnum.DAILY_BAR_CLOSED,
                event_time=datetime.combine(trade_date, time(15, 0)),
                market=self.market_name,
                frequency=frequency,
                event_description="日K线生成"
            ))
        
        return events
    
    def generate_minute_events(self, trade_date: date, frequency: str = '1m') -> List[BaseEvent]:
        """生成分钟级事件
        
        Args:
            trade_date: 交易日期
            frequency: 频率
            
        Returns:
            事件列表
        """
        events = []
        
        # 添加DAY_START事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.DAY_START,
            event_time=datetime.combine(trade_date, time(9, 0)),
            market=self.market_name,
            frequency=frequency,
            event_description="基金交易日开始"
        ))
        
        # 添加BEFORE_MARKET事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.BEFORE_MARKET,
            event_time=datetime.combine(trade_date, time(9, 0)),
            market=self.market_name,
            frequency=frequency,
            event_description="基金盘前"
        ))
        
        # 添加MARKET_START事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.MARKET_START,
            event_time=datetime.combine(trade_date, time(9, 30)),
            market=self.market_name,
            frequency=frequency,
            event_description="基金开盘"
        ))
        
        # 添加撮合事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.TRY_MATCH,
            event_time=datetime.combine(trade_date, time(9, 30)),
            market=self.market_name,
            frequency=frequency,
            event_description="基金分钟级撮合"
        ))
        
        # 上午交易时段
        morning_start = datetime.combine(trade_date, time(9, 30))
        morning_end = datetime.combine(trade_date, time(11, 30))
        current_time = morning_start
        
        while current_time < morning_end:
            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_BAR_1M,
                event_time=current_time,
                market=self.market_name,
                frequency=frequency,
                event_description="基金1分钟K线"
            ))
            current_time += timedelta(minutes=1)
        
        # 下午交易时段
        afternoon_start = datetime.combine(trade_date, time(13, 0))
        afternoon_end = datetime.combine(trade_date, time(15, 0))
        current_time = afternoon_start
        
        while current_time < afternoon_end:
            events.append(MarketEvent(
                event_type=EventTypeEnum.MARKET_BAR_1M,
                event_time=current_time,
                market=self.market_name,
                frequency=frequency,
                event_description="基金1分钟K线"
            ))
            current_time += timedelta(minutes=1)
        
        # 添加MARKET_END事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.MARKET_END,
            event_time=datetime.combine(trade_date, time(15, 0)),
            market=self.market_name,
            frequency=frequency,
            event_description="基金收盘"
        ))
        
        # 添加AFTER_MARKET事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.AFTER_MARKET,
            event_time=datetime.combine(trade_date, time(15, 30)),
            market=self.market_name,
            frequency=frequency,
            event_description="基金盘后"
        ))
        
        # 添加DAY_END事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.DAY_END,
            event_time=datetime.combine(trade_date, time(23, 59)),
            market=self.market_name,
            frequency=frequency,
            event_description="基金交易日结束"
        ))
        
        # 添加DAILY_BAR_CLOSED事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.DAILY_BAR_CLOSED,
            event_time=datetime.combine(trade_date, time(15, 0)),
            market=self.market_name,
            frequency=frequency,
            event_description="基金日K线收盘"
        ))
        
        return events
    
    def is_trading_time(self, dt: datetime, frequency: str = '1d') -> bool:
        """判断指定时间是否为交易时间
        
        Args:
            dt: 日期时间
            frequency: 频率
            
        Returns:
            是否为交易时间
        """
        # 检查是否为工作日
        if dt.weekday() >= 5:  # 周六、周日
            return False
        
        # 基金交易时段
        morning_session = (time(9, 30), time(11, 30))
        afternoon_session = (time(13, 0), time(15, 0))
        
        # 检查是否在任一交易时段内
        for session_start, session_end in [morning_session, afternoon_session]:
            if session_start <= dt.time() <= session_end:
                return True
        
        return False
    
    def get_trading_sessions(self, trade_date: date, frequency: str = '1d') -> List[Tuple[time, time]]:
        """获取交易时段
        
        Args:
            trade_date: 交易日期
            frequency: 频率
            
        Returns:
            交易时段列表
        """
        # 基金交易时段
        morning_session = (time(9, 30), time(11, 30))
        afternoon_session = (time(13, 0), time(15, 0))
        
        return [morning_session, afternoon_session]
    
    def get_trading_calendar(self, start_date: date, end_date: date) -> List[date]:
        """获取交易日历
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            交易日列表
        """
        # 使用中国A股交易日历（基金与A股相同）
        from ...data.calendar.cn_stock_calendar import CnStockCalendar
        calendar = CnStockCalendar()
        return calendar.get_trading_days(start_date, end_date)
    
    def get_settlement_cycle(self) -> str:
        """获取结算周期
        
        Returns:
            结算周期
        """
        return 'T+1'
    
    def get_timezone(self) -> str:
        """获取时区
        
        Returns:
            时区
        """
        return 'Asia/Shanghai'
    
    def get_currency(self) -> str:
        """获取货币
        
        Returns:
            货币
        """
        return 'CNY'
    
    def get_market_name(self) -> str:
        """获取市场名称
        
        Returns:
            市场名称
        """
        return self.market_name
    
    def get_commission_rate(self, symbol: str, direction: str = 'buy') -> Dict[str, float]:
        """获取手续费率
        
        Args:
            symbol: 交易标的
            direction: 买卖方向
            
        Returns:
            手续费率字典
        """
        # 基金手续费率
        commission_config = self.config['trading_rules']['commission']['fund']
        
        return {
            'commission': commission_config['open_commission'] if direction == 'buy' else commission_config['close_commission'],
            'tax': commission_config['open_tax'] if direction == 'buy' else commission_config['close_tax'],
            'min_commission': commission_config['min_commission']
        }
    
    def get_slippage_rate(self, symbol: str) -> float:
        """获取滑点率
        
        Args:
            symbol: 交易标的
            
        Returns:
            滑点率
        """
        # 基金滑点率
        return self.config['trading_rules']['slippage']['slip_value']
    
    def get_position_limits(self, symbol: str) -> Dict[str, int]:
        """获取持仓限制
        
        Args:
            symbol: 交易标的
            
        Returns:
            持仓限制字典
        """
        # 基金持仓限制
        return self.config['trading_rules']['limits']
    
    def get_price_limits(self, symbol: str) -> Dict[str, float]:
        """获取价格限制
        
        Args:
            symbol: 交易标的
            
        Returns:
            价格限制字典
        """
        # 基金无涨跌停限制
        return {
            'upper_limit': float('inf'),
            'lower_limit': 0.0
        }
    
    def get_rounding_method(self, symbol: str) -> str:
        """获取取整方法
        
        Args:
            symbol: 交易标的
            
        Returns:
            取整方法
        """
        # 基金取整方法
        return 'round'
    
    def get_market_info(self) -> Dict[str, Any]:
        """获取市场信息
        
        Returns:
            市场信息字典
        """
        return {
            'market_name': self.market_name,
            'timezone': 'Asia/Shanghai',
            'currency': 'CNY',
            'trading_sessions': [
                (time(9, 30), time(11, 30)),
                (time(13, 0), time(15, 0))
            ],
            'settlement_cycle': 'T+1',
            'price_limits': False,
            'short_selling': False,
            'margin_trading': False
        }
    
    def get_supported_frequencies(self) -> List[str]:
        """获取支持的频率
        
        Returns:
            支持的频率列表
        """
        return ['1d', '1m', '30m', '120m']
    
    def get_trading_days(self, start_date: date, end_date: date) -> List[date]:
        """获取交易日
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            交易日列表
        """
        # 使用中国A股交易日历（基金与A股相同）
        from ...data.calendar.cn_stock_calendar import CnStockCalendar
        calendar = CnStockCalendar()
        return calendar.get_trading_days(start_date, end_date)


# 向后兼容的别名
CnFundAdapter = CnFundMarketAdapter 