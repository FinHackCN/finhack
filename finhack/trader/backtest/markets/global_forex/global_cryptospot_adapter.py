"""
全球加密货币现货市场适配器

实现全球加密货币现货市场的交易规则和事件生成，支持多频次
"""

from typing import List, Dict, Any
from datetime import datetime, date, time, timedelta
import logging

from ..base_market import BaseMarket
from ...events.event_types import BaseEvent, MarketEvent, EventTypeEnum

logger = logging.getLogger(__name__)


class GlobalCryptoSpotMarketAdapter(BaseMarket):
    """全球加密货币现货市场适配器
    
    支持7x24小时交易的加密货币现货市场
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """初始化全球加密货币现货市场适配器
        
        Args:
            config: 市场配置，如果为空则加载默认配置
        """
        # 如果没有提供配置，使用默认配置
        if not config:
            config = self._get_default_config()
            
        super().__init__('global_cryptospot', config)
        
        # 加密货币市场特有配置
        self.t_plus_zero = config.get('t_plus_zero', True)  # T+0制度
        self.trading_24_7 = config.get('trading_24_7', True)  # 7x24小时交易
        
        logger.info(f"全球加密货币现货市场适配器初始化完成，支持频率: {self.supported_frequencies}")
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            'supported_frequencies': ['1d', '1m', '30m', '120m'],
            'timezone': 'UTC',
            'currency': 'USDT',
            'trading_schedule': {
                '1d': {
                    'DAY_START': '00:00:00',
                    'DAY_END': '23:59:59'
                },
                '1m': {
                    'DAY_START': '00:00:00',
                    'DAY_END': '23:59:59'
                },
                '30m': {
                    'DAY_START': '00:00:00',
                    'DAY_END': '23:59:59'
                },
                '120m': {
                    'DAY_START': '00:00:00',
                    'DAY_END': '23:59:59'
                }
            },
            'trading_rules': {
                'commission': {
                    'crypto': {
                        'open_commission': 0.001,     # 0.1%
                        'close_commission': 0.001,    # 0.1%
                        'open_tax': 0.0,              # 免税
                        'close_tax': 0.0,             # 免税
                        'min_commission': 0.0         # 无最低手续费
                    }
                },
                'slippage': {
                    'slip_type': 'pricerelated',
                    'slip_value': 0.0005            # 0.05%滑点
                },
                'limits': {
                    'lot_size': 0.00000001,         # 最小单位
                    'min_order_volume': 0.00000001,  # 最小数量
                    'max_order_volume': 1000000      # 最大数量
                }
            },
            't_plus_zero': True,
            'trading_24_7': True
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
            events.extend(self._generate_daily_events_1d(trade_date))
        elif frequency in ['1m', '30m', '120m']:
            # 分钟线频率的事件列表
            events.extend(self._generate_daily_events_min(trade_date, schedule, frequency))
        
        # 按时间排序
        events.sort(key=lambda x: x.event_time)
        
        logger.debug(f"生成 {trade_date} {frequency} 频率事件 {len(events)} 个")
        return events
    
    def _generate_daily_events_1d(self, trade_date: datetime.date) -> List[BaseEvent]:
        """生成1d频率的日内事件"""
        events = []
        
        # 加密货币市场是7x24连续交易，事件生成逻辑与股票期货有所不同
        # 简化处理，主要关注日K线和撮合
        
        # 交易前 (概念性，实际可能不严格区分)
        events.append(MarketEvent(
            event_type=EventTypeEnum.BEFORE_MARKET,
            event_time=datetime.combine(trade_date, time(0, 0)),
            market=self.market_name,
            frequency='1d',
            event_description="加密货币交易前准备"
        ))
        
        # 开盘 (概念性，实际是连续的)
        events.append(MarketEvent(
            event_type=EventTypeEnum.MARKET_START,
            event_time=datetime.combine(trade_date, time(0, 0)),
            market=self.market_name,
            frequency='1d',
            event_description="加密货币开盘"
        ))
        
        # 撮合事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.TRY_MATCH,
            event_time=datetime.combine(trade_date, time(0, 0)),
            market=self.market_name,
            frequency='1d',
            event_description="加密货币日级撮合"
        ))
        
        # 尝试撮合 (全天候)
        # 对于1d频率，可以在一天结束时触发一次撮合和K线生成
        end_of_day = datetime.datetime.combine(trade_date, time(23, 59, 59))
        events.append(MarketEvent(
            event_type=EventTypeEnum.TRY_MATCH,
            datetime=end_of_day,
            trade_date=trade_date,
            market=self.market_name,
            event_description="加密货币日级撮合"
        ))
        
        # 日K线生成
        events.append(MarketEvent(
            event_type=EventTypeEnum.MARKET_BAR_1D,
            datetime=end_of_day,
            trade_date=trade_date,
            market=self.market_name,
            event_description="加密货币日K线生成"
        ))
        
        # 收盘 (概念性，实际是连续的)
        events.append(MarketEvent(
            event_type=EventTypeEnum.MARKET_END,
            datetime=end_of_day,
            trade_date=trade_date,
            market=self.market_name,
            event_description="加密货币收盘"
        ))
        
        # 交易后 (概念性)
        events.append(MarketEvent(
            event_type=EventTypeEnum.AFTER_MARKET,
            trade_date=trade_date,
            market=self.market_name,
            event_description="加密货币交易后"
        ))
        
        # 结算 (加密货币通常没有每日结算的概念，但可以用于内部账户更新)
        events.append(MarketEvent(
            event_type=EventTypeEnum.MARKET_SETTLEMENT,
            trade_date=trade_date,
            market=self.market_name,
            event_description="加密货币结算"
        ))
        
        return events
    
    def _generate_daily_events_min(self, trade_date: date, schedule: Dict[str, time], frequency: str) -> List[BaseEvent]:
        """生成分钟线频率的事件列表"""
        events = []
        
        # 加密货币市场是24小时交易，所以只需要生成TRY_MATCH事件
        # 根据频率确定事件间隔
        if frequency == '1m':
            interval = timedelta(minutes=1)
        elif frequency == '30m':
            interval = timedelta(minutes=30)
        elif frequency == '120m':
            interval = timedelta(minutes=120)
        else:
            interval = timedelta(minutes=1)
        
        # 生成一天内的TRY_MATCH事件
        start_time = datetime.combine(trade_date, schedule['DAY_START'])
        end_time = datetime.combine(trade_date, schedule['DAY_END'])
        
        current_time = start_time
        while current_time <= end_time:
            match_event = MarketEvent(
                event_type=EventTypeEnum.TRY_MATCH,
                event_time=current_time,
                market=self.market_name,
                frequency=frequency,
                event_description=f"{frequency}级撮合"
            )
            events.append(match_event)
            current_time += interval
        
        return events
    
    def _get_event_description(self, event_name: str) -> str:
        """获取事件描述"""
        descriptions = {
            'DAY_START': '新的一天开始',
            'DAY_END': '一天结束',
            'TRY_MATCH': '撮合处理'
        }
        return descriptions.get(event_name, event_name)
    
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
        
        # 加密货币市场是24小时交易
        if 'DAY_START' in schedule and 'DAY_END' in schedule:
            start_time = schedule['DAY_START']
            end_time = schedule['DAY_END']
            
            start_dt = datetime.combine(trade_date, start_time)
            end_dt = datetime.combine(trade_date, end_time)
            
            sessions.append((start_dt, end_dt))
        
        return sessions
    
    def is_trading_time(self, dt: datetime, frequency: str = '1d') -> bool:
        """判断指定时间是否为交易时间
        
        Args:
            dt: 时间
            frequency: 数据频率
            
        Returns:
            bool: 是否为交易时间
        """
        # 加密货币市场是24小时交易，所以任何时间都是交易时间
        if self.trading_24_7:
            return True
        
        # 如果不是24小时交易，则检查交易时段
        trading_sessions = self.get_trading_sessions(dt.date(), frequency)
        
        # 检查是否在任一交易时段内
        for session_start, session_end in trading_sessions:
            if session_start <= dt <= session_end:
                return True
        
        return False
    
    def validate_order(self, symbol: str, volume: float, price: float, 
                      side: str) -> tuple[bool, str]:
        """验证订单是否符合加密货币市场规则
        
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
        
        # 加密货币特有验证
        # 检查最小交易单位
        lot_size = self.get_lot_size(symbol)
        if volume < lot_size:
            return False, f"交易数量不能小于最小单位{lot_size}"
        
        return True, ""
    
    def is_t_plus_zero_allowed(self, symbol: str) -> bool:
        """检查是否允许T+0交易
        
        Args:
            symbol: 交易标的
            
        Returns:
            bool: 是否允许T+0交易
        """
        return self.t_plus_zero
    
    def is_24_7_trading(self) -> bool:
        """检查是否是24小时交易
        
        Returns:
            bool: 是否是24小时交易
        """
        return self.trading_24_7
