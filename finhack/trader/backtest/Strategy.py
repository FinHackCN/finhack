from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging


class Strategy(ABC):
    """策略基类"""
    
    def __init__(self, adapter_id: str):
        """
        初始化策略
        
        Args:
            adapter_id: 适配器ID，用于标识策略实例
        """
        self.adapter_id = adapter_id
        self.trade_center = None
        self.data_center = None
        self.event_center = None
        self.logger = logging.getLogger(f"{__name__}.{adapter_id}")
        
    def initialize(self, trade_center, data_center, event_center):
        """
        初始化策略依赖
        
        Args:
            trade_center: 交易中心实例
            data_center: 数据中心实例
            event_center: 事件中心实例
        """
        self.trade_center = trade_center
        self.data_center = data_center
        self.event_center = event_center
        
        # 注册事件处理器
        self._register_handlers()
        
        # 调用用户初始化
        self.on_init()
        
    def _register_handlers(self):
        """注册事件处理器"""
        # 检查并注册各种处理器
        if hasattr(self, 'on_bar'):
            self.event_center.register_event_handler(
                self.adapter_id, 
                self.event_center.EventType.BAR,
                self.on_bar
            )
            
        if hasattr(self, 'on_market_open'):
            self.event_center.register_event_handler(
                self.adapter_id,
                self.event_center.EventType.MARKET_OPEN,
                self.on_market_open
            )
            
        if hasattr(self, 'on_market_close'):
            self.event_center.register_event_handler(
                self.adapter_id,
                self.event_center.EventType.MARKET_CLOSE,
                self.on_market_close
            )
            
    def register_timer(self, time_str: str):
        """
        注册定时器
        
        Args:
            time_str: 时间字符串，格式 "HH:MM" 或 "HH:MM:SS"
        """
        if hasattr(self, 'on_timer'):
            self.event_center.register_timer(
                self.adapter_id,
                time_str,
                self.on_timer
            )
            
    @abstractmethod
    def on_init(self):
        """策略初始化，子类必须实现"""
        pass
        
    # 便捷方法，直接调用trade_center的方法
    def get_account(self):
        """获取账户信息"""
        return self.trade_center.get_account(self.adapter_id)
        
    def get_positions(self):
        """获取所有持仓"""
        return self.trade_center.get_positions(self.adapter_id)
        
    def get_position(self, code: str):
        """获取单个持仓"""
        return self.trade_center.get_position(self.adapter_id, code)
        
    def get_orders(self, active_only: bool = False):
        """获取订单列表"""
        return self.trade_center.get_orders(self.adapter_id, active_only)
        
    def get_trades(self):
        """获取成交记录"""
        return self.trade_center.get_trades(self.adapter_id)
        
    def place_order(self, code: str, quantity: float, side: str, 
                   order_type: str = "market", price: Optional[float] = None,
                   stop_price: Optional[float] = None, **kwargs):
        """下单"""
        return self.trade_center.place_order(
            self.adapter_id, code, quantity, side, order_type,
            price, stop_price, **kwargs
        )
        
    def cancel_order(self, order_id: str):
        """撤销订单"""
        return self.trade_center.cancel_order(self.adapter_id, order_id)
        
    def get_price(self, code: str, price_type: str = "current"):
        """获取价格"""
        return self.trade_center.get_price(self.adapter_id, code, price_type)
        
    # 便捷方法，直接调用data_center的方法
    def get_kline(self, codes, frequency: str = "1d", 
                  end_time: Optional[datetime] = None, count: int = 100):
        """获取K线数据"""
        return self.data_center.get_kline(
            self.adapter_id, codes, frequency, end_time, count
        )
        
    def get_factors(self, codes, factor_names: List[str],
                   end_time: Optional[datetime] = None, count: int = 100):
        """获取因子数据"""
        return self.data_center.get_factors(
            self.adapter_id, codes, factor_names, end_time, count
        )
        
    def compute_factors(self, factor_list: List[str], data):
        """计算因子"""
        return self.data_center.compute_factors(
            self.adapter_id, factor_list, data
        )
        
    def ml_predict(self, model_id: str, features):
        """机器学习预测"""
        return self.data_center.ml_predict(model_id, features)
        
    # 全局变量访问
    @property
    def g(self):
        """获取全局变量字典"""
        return self.trade_center.g 