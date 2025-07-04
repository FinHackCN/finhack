"""
策略基类实现
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime

from ..events.base_event import BaseEvent, EventType
from ..events.market_events import MarketEvent
from ..events.trade_events import TradeEvent
from ..events.user_events import UserEvent


class BaseStrategy(ABC):
    """策略基类，提供所有策略必须实现的接口"""
    
    def __init__(self, name: str = "BaseStrategy"):
        """
        初始化策略
        
        Args:
            name: 策略名称
        """
        self.name = name
        self.enabled = True
        self.params: Dict[str, Any] = {}
        self.context = None
        self.logger = None
        
    def initialize(self, context):
        """
        策略初始化
        
        Args:
            context: 回测上下文
        """
        self.context = context
        self.logger = context.logger if context else None
        self.on_initialize(context)
    
    def set_params(self, params: Dict[str, Any]):
        """设置策略参数"""
        self.params.update(params)
    
    def get_param(self, key: str, default: Any = None) -> Any:
        """获取策略参数"""
        return self.params.get(key, default)
    
    # 抽象方法，子类必须实现
    @abstractmethod
    def on_initialize(self, context):
        """策略初始化回调"""
        pass
    
    @abstractmethod
    def on_bar(self, context, bar_data: Dict[str, Any]):
        """Bar数据回调"""
        pass
    
    # 可选实现的事件回调方法
    def on_start_interval(self, context, event: BaseEvent):
        """交易日开始回调"""
        pass
    
    def on_before_market(self, context, event: BaseEvent):
        """盘前回调"""
        pass
    
    def on_morning_start(self, context, event: BaseEvent):
        """上午开盘回调"""
        pass
    
    def on_morning_end(self, context, event: BaseEvent):
        """上午收盘回调"""
        pass
    
    def on_afternoon_start(self, context, event: BaseEvent):
        """下午开盘回调"""
        pass
    
    def on_afternoon_end(self, context, event: BaseEvent):
        """下午收盘回调"""
        pass
    
    def on_after_market(self, context, event: BaseEvent):
        """盘后回调"""
        pass
    
    def on_end_interval(self, context, event: BaseEvent):
        """交易日结束回调"""
        pass
    
    def on_minute_bar(self, context, event: BaseEvent):
        """分钟级Bar回调"""
        bar_data = event.data.get("bar_data", {})
        if bar_data:
            self.on_bar(context, bar_data)
    
    def on_daily_bar_closed(self, context, event: BaseEvent):
        """日线数据完成回调"""
        bar_data = event.data.get("bar_data", {})
        if bar_data:
            self.on_bar(context, bar_data)
    
    def on_order_submission(self, context, event: BaseEvent):
        """订单提交回调"""
        pass
    
    def on_order_fill(self, context, event: BaseEvent):
        """订单成交回调"""
        pass
    
    def on_order_cancellation(self, context, event: BaseEvent):
        """订单取消回调"""
        pass
    
    def on_position_update(self, context, event: BaseEvent):
        """持仓更新回调"""
        pass
    
    def on_time(self, context, event: BaseEvent):
        """定时回调"""
        pass
    
    def on_event(self, context, event: BaseEvent):
        """自定义事件回调"""
        pass
    
    def on_user_daily(self, context, event: BaseEvent):
        """用户每日回调"""
        pass
    
    def on_user_hourly(self, context, event: BaseEvent):
        """用户每小时回调"""
        pass
    
    def on_user_minutely(self, context, event: BaseEvent):
        """用户每分钟回调"""
        pass
    
    def on_user_weekly(self, context, event: BaseEvent):
        """用户每周回调"""
        pass
    
    def on_user_monthly(self, context, event: BaseEvent):
        """用户每月回调"""
        pass
    
    def on_user_interval(self, context, event: BaseEvent):
        """用户自定义间隔回调"""
        pass
    
    # 便利方法
    def log_info(self, message: str):
        """记录信息日志"""
        if self.logger:
            self.logger.info(f"[{self.name}] {message}")
    
    def log_error(self, message: str):
        """记录错误日志"""
        if self.logger:
            self.logger.error(f"[{self.name}] {message}")
    
    def log_warning(self, message: str):
        """记录警告日志"""
        if self.logger:
            self.logger.warning(f"[{self.name}] {message}")
    
    def buy(self, symbol: str, amount: int, price: Optional[float] = None, 
            order_type: str = "market", **kwargs) -> Optional[str]:
        """
        买入股票
        
        Args:
            symbol: 股票代码
            amount: 数量
            price: 价格（限价单时使用）
            order_type: 订单类型 (market, limit)
            **kwargs: 其他参数
            
        Returns:
            Optional[str]: 订单ID
        """
        if not self.context or not self.context.trade_center:
            self.log_error("交易中心未初始化")
            return None
        
        return self.context.trade_center.place_order(
            symbol=symbol,
            side="buy",
            quantity=amount,  # 修改参数名：amount -> quantity
            price=price,
            order_type=order_type,
            **kwargs
        )
    
    def sell(self, symbol: str, amount: int, price: Optional[float] = None,
             order_type: str = "market", **kwargs) -> Optional[str]:
        """
        卖出股票
        
        Args:
            symbol: 股票代码
            amount: 数量
            price: 价格（限价单时使用）
            order_type: 订单类型 (market, limit)
            **kwargs: 其他参数
            
        Returns:
            Optional[str]: 订单ID
        """
        if not self.context or not self.context.trade_center:
            self.log_error("交易中心未初始化")
            return None
        
        return self.context.trade_center.place_order(
            symbol=symbol,
            side="sell",
            quantity=amount,  # 修改参数名：amount -> quantity
            price=price,
            order_type=order_type,
            **kwargs
        )
    
    def cancel_order(self, order_id: str) -> bool:
        """
        取消订单
        
        Args:
            order_id: 订单ID
            
        Returns:
            bool: 是否成功
        """
        if not self.context or not self.context.trade_center:
            self.log_error("交易中心未初始化")
            return False
        
        return self.context.trade_center.cancel_order(order_id)
    
    def get_position(self, symbol: str) -> Optional[Any]:
        """获取持仓信息"""
        if not self.context or not self.context.trade_center:
            return None
        
        return self.context.trade_center.get_position(symbol)
    
    def get_price(self, symbol: str) -> Optional[float]:
        """获取当前价格"""
        if not self.context or not self.context.trade_center:
            return None
        
        return self.context.trade_center.get_price(symbol)
    
    def get_account(self) -> Optional[Any]:
        """获取账户信息"""
        if not self.context or not self.context.trade_center:
            return None
        
        return self.context.trade_center.get_account()
    
    def get_kline(self, symbol: str, start_date: str, end_date: str, 
                  period: str = "1d") -> Optional[Any]:
        """获取K线数据"""
        if not self.context or not self.context.data_center:
            return None
        
        return self.context.data_center.get_kline(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            period=period
        )
    
    def get_factors(self, symbol: str, start_date: str, end_date: str,
                   factors: List[str]) -> Optional[Any]:
        """获取因子数据"""
        if not self.context or not self.context.data_center:
            return None
        
        return self.context.data_center.get_factors(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            factors=factors
        )
    
    def compute_factors(self, symbol: str, start_date: str, end_date: str,
                       factor_names: List[str]) -> Optional[Any]:
        """计算因子"""
        if not self.context or not self.context.data_center:
            return None
        
        return self.context.data_center.compute_factors(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            factor_names=factor_names
        )
    
    def ml_predict(self, model_name: str, features: Dict[str, Any]) -> Optional[Any]:
        """机器学习预测"""
        if not self.context or not self.context.data_center:
            return None
        
        return self.context.data_center.ml_predict(
            model_name=model_name,
            features=features
        )
    
    def schedule_daily(self, time_str: str, callback):
        """注册每日定时任务"""
        if not self.context or not self.context.event_center:
            self.log_error("事件中心未初始化")
            return
        
        self.context.event_center.register_user_schedule(
            schedule_type="daily",
            time_str=time_str,
            callback=callback
        )
    
    def schedule_hourly(self, minute: int, callback):
        """注册每小时定时任务"""
        if not self.context or not self.context.event_center:
            self.log_error("事件中心未初始化")
            return
        
        self.context.event_center.register_user_schedule(
            schedule_type="hourly",
            time_str=f"{minute:02d}:00",
            callback=callback
        )
    
    def schedule_minutely(self, second: int, callback):
        """注册每分钟定时任务"""
        if not self.context or not self.context.event_center:
            self.log_error("事件中心未初始化")
            return
        
        self.context.event_center.register_user_schedule(
            schedule_type="minutely",
            time_str=f"00:{second:02d}",
            callback=callback
        )
    
    def schedule_weekly(self, day: str, time_str: str, callback):
        """注册每周定时任务"""
        if not self.context or not self.context.event_center:
            self.log_error("事件中心未初始化")
            return
        
        self.context.event_center.register_user_schedule(
            schedule_type="weekly",
            time_str=time_str,
            callback=callback,
            day=day
        )
    
    def schedule_monthly(self, day: int, time_str: str, callback):
        """注册每月定时任务"""
        if not self.context or not self.context.event_center:
            self.log_error("事件中心未初始化")
            return
        
        self.context.event_center.register_user_schedule(
            schedule_type="monthly",
            time_str=time_str,
            callback=callback,
            day=day
        )
    
    def schedule_interval(self, interval: int, callback):
        """注册自定义间隔定时任务"""
        if not self.context or not self.context.event_center:
            self.log_error("事件中心未初始化")
            return
        
        self.context.event_center.register_user_schedule(
            schedule_type="interval",
            time_str="",
            callback=callback,
            interval=interval
        )
    
    def __str__(self) -> str:
        return f"Strategy({self.name})"
    
    def __repr__(self) -> str:
        return self.__str__() 