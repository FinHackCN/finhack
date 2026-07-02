"""
基础市场适配器

定义市场适配器的基础接口，支持多市场多频次
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from datetime import datetime, time, date
import json
import os

from ..events.event_types import BaseEvent, MarketEvent, EventTypeEnum


class BaseMarket(ABC):
    """基础市场适配器
    
    定义所有市场适配器必须实现的接口
    """
    
    def __init__(self, market_name: str, config: Dict[str, Any]):
        """初始化市场适配器
        
        Args:
            market_name: 市场名称
            config: 市场配置
        """
        self.market_name = market_name
        self.config = config

        # 回测当前日期（由引擎通过 set_current_date 同步）
        self._current_date = date.today()

        # 数据中心引用（由引擎通过 set_data_center 同步；供 validate_order 取前收盘等）
        self._data_center = None

        # 从配置加载市场参数
        self.supported_frequencies = config.get('supported_frequencies', ['1d'])
        self.timezone = config.get('timezone', 'Asia/Shanghai')
        self.currency = config.get('currency', 'CNY')
        
        # 加载交易时间配置
        self._load_trading_schedule()
        
        # 加载交易规则配置
        self._load_trading_rules()
    
    def _load_trading_schedule(self):
        """加载交易时间配置"""
        schedule_config = self.config.get('trading_schedule', {})
        self.trading_schedule = {}
        
        for freq in self.supported_frequencies:
            freq_schedule = schedule_config.get(freq, {})
            self.trading_schedule[freq] = self._parse_schedule(freq_schedule)
    
    def _parse_schedule(self, schedule_config: Dict[str, Any]) -> Dict[str, Any]:
        """解析时间配置
        
        Args:
            schedule_config: 时间配置字典
            
        Returns:
            Dict[str, Any]: 解析后的时间配置
        """
        parsed = {}
        
        for event_name, time_str in schedule_config.items():
            if isinstance(time_str, str):
                try:
                    parsed[event_name] = datetime.strptime(time_str, '%H:%M:%S').time()
                except ValueError:
                    parsed[event_name] = datetime.strptime(time_str, '%H:%M').time()
            else:
                parsed[event_name] = time_str
                
        return parsed
    
    def _load_trading_rules(self):
        """加载交易规则配置"""
        rules_config = self.config.get('trading_rules', {})
        
        # 手续费配置
        self.commission_config = rules_config.get('commission', {})
        
        # 滑点配置
        self.slippage_config = rules_config.get('slippage', {})
        
        # 交易限制配置
        self.trading_limits = rules_config.get('limits', {})
        
        # 风控配置
        self.risk_controls = rules_config.get('risk_controls', {})

    def set_current_date(self, current_date: date):
        """设置回测当前日期（由引擎调用）

        Args:
            current_date: 回测当前日期
        """
        self._current_date = current_date

    def get_current_date(self) -> date:
        """获取回测当前日期

        Returns:
            date: 回测当前日期
        """
        return self._current_date

    def set_data_center(self, data_center):
        """注入数据中心引用（由引擎调用，供 validate_order 等取前收盘/标的元数据）

        Args:
            data_center: DataCenter 实例
        """
        self._data_center = data_center
    
    @abstractmethod
    def generate_daily_events(self, trade_date: date, frequency: str = '1d') -> List[BaseEvent]:
        """生成指定日期的市场事件列表
        
        Args:
            trade_date: 交易日期
            frequency: 数据频率
            
        Returns:
            List[BaseEvent]: 事件列表
        """
        pass
    
    @abstractmethod
    def is_trading_time(self, dt: datetime, frequency: str = '1d') -> bool:
        """判断指定时间是否为交易时间
        
        Args:
            dt: 时间
            frequency: 数据频率
            
        Returns:
            bool: 是否为交易时间
        """
        pass
    
    @abstractmethod
    def get_trading_sessions(self, trade_date: date, frequency: str = '1d') -> List[tuple]:
        """获取交易时段
        
        Args:
            trade_date: 交易日期
            frequency: 数据频率
            
        Returns:
            List[tuple]: 交易时段列表，每个元素为(开始时间, 结束时间)
        """
        pass
    
    def calculate_commission(self, volume: float, price: float, side: str, 
                           asset_type: str = 'stock') -> float:
        """计算手续费
        
        Args:
            volume: 交易数量
            price: 交易价格
            side: 交易方向 ('buy' or 'sell')
            asset_type: 资产类型
            
        Returns:
            float: 手续费金额
        """
        commission_rates = self.commission_config.get(asset_type, {})
        
        if side == 'buy':
            rate = commission_rates.get('open_commission', 0.0003)
        else:
            rate = commission_rates.get('close_commission', 0.0003)
        
        amount = volume * price
        commission = amount * rate
        
        # 最低手续费
        min_commission = commission_rates.get('min_commission', 5.0)
        return max(commission, min_commission)
    
    def calculate_tax(self, volume: float, price: float, side: str,
                     asset_type: str = 'stock') -> float:
        """计算税费
        
        Args:
            volume: 交易数量
            price: 交易价格
            side: 交易方向
            asset_type: 资产类型
            
        Returns:
            float: 税费金额
        """
        commission_rates = self.commission_config.get(asset_type, {})
        
        if side == 'buy':
            rate = commission_rates.get('open_tax', 0.0)
        else:
            rate = commission_rates.get('close_tax', 0.001)  # 印花税
        
        amount = volume * price
        return amount * rate
    
    def calculate_slippage(self, price: float, volume: float, side: str) -> float:
        """计算滑点
        
        Args:
            price: 原始价格
            volume: 交易数量
            side: 交易方向
            
        Returns:
            float: 滑点后的价格
        """
        slip_type = self.slippage_config.get('slip_type', 'pricerelated')
        slip_value = self.slippage_config.get('slip_value', 0.001)
        
        if slip_type == 'pricerelated':
            # 按比例滑点
            if side == 'buy':
                return price * (1 + slip_value)
            else:
                return price * (1 - slip_value)
        elif slip_type == 'fixed':
            # 固定滑点
            if side == 'buy':
                return price + slip_value
            else:
                return price - slip_value
        else:
            return price
    
    def validate_order(self, symbol: str, volume: float, price: float, 
                      side: str) -> tuple[bool, str]:
        """验证订单是否符合市场规则
        
        Args:
            symbol: 交易标的
            volume: 交易数量
            price: 交易价格
            side: 交易方向
            
        Returns:
            tuple[bool, str]: (是否有效, 错误信息)
        """
        # 检查数量限制
        min_volume = self.trading_limits.get('min_order_volume', 0)
        if volume < min_volume:
            return False, f"交易数量不能小于 {min_volume}"
        
        max_volume = self.trading_limits.get('max_order_volume', float('inf'))
        if volume > max_volume:
            return False, f"交易数量不能大于 {max_volume}"
        
        # 检查价格限制
        if price <= 0:
            return False, "交易价格必须大于0"
        
        return True, ""
    
    def get_lot_size(self, symbol: str) -> int:
        """获取交易单位（手数）
        
        Args:
            symbol: 交易标的
            
        Returns:
            int: 交易单位
        """
        return self.trading_limits.get('lot_size', 100)
    
    def normalize_volume(self, volume: float, symbol: str) -> float:
        """标准化交易数量
        
        Args:
            volume: 原始数量
            symbol: 交易标的
            
        Returns:
            float: 标准化后的数量
        """
        lot_size = self.get_lot_size(symbol)
        return round(volume / lot_size) * lot_size
    
    @classmethod
    def load_from_config_file(cls, market_name: str, config_file: str):
        """从配置文件加载市场适配器
        
        Args:
            market_name: 市场名称
            config_file: 配置文件路径
            
        Returns:
            BaseMarket: 市场适配器实例
        """
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"市场配置文件不存在: {config_file}")
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        return cls(market_name, config)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'market_name': self.market_name,
            'supported_frequencies': self.supported_frequencies,
            'timezone': self.timezone,
            'currency': self.currency,
            'trading_schedule': self.trading_schedule,
            'commission_config': self.commission_config,
            'slippage_config': self.slippage_config,
            'trading_limits': self.trading_limits
        } 