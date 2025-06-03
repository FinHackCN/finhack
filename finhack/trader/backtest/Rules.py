from typing import Dict, List, Any, Optional, Callable
from abc import ABC, abstractmethod
import logging
import pandas as pd
import numpy as np


class Rule(ABC):
    """规则基类"""
    
    @abstractmethod
    def check(self, order: Any, trade_center: Any) -> bool:
        """
        检查规则是否通过
        
        Args:
            order: 订单对象
            trade_center: 交易中心实例
            
        Returns:
            是否通过规则检查
        """
        pass
        
    @abstractmethod
    def get_name(self) -> str:
        """获取规则名称"""
        pass


class StopTradingRule(Rule):
    """停牌检查规则"""
    
    def check(self, order: Any, trade_center: Any) -> bool:
        price = trade_center.get_price(order.adapter_id, order.code)
        if price is None or price == 0:
            logging.warning(f"{order.code} is suspended, order rejected")
            return False
        return True
        
    def get_name(self) -> str:
        return "stop_trading"


class LimitUpDownRule(Rule):
    """涨跌停检查规则"""
    
    def __init__(self, limit_ratio: float = 0.1):
        self.limit_ratio = limit_ratio
        
    def check(self, order: Any, trade_center: Any) -> bool:
        # 获取今日和昨日价格
        df = trade_center.data_center.get_kline(
            order.adapter_id, order.code, "1d", 
            end_time=trade_center.current_time, count=2
        )
        
        if len(df) < 2:
            return True
            
        yesterday_close = float(df.iloc[-2]['close'])
        today_price = float(df.iloc[-1]['close'])
        
        # 计算涨跌幅
        change_ratio = (today_price - yesterday_close) / yesterday_close
        
        # 涨停不能买入
        if change_ratio >= self.limit_ratio and order.side.value == "buy":
            logging.warning(f"{order.code} hit limit up, buy order rejected")
            return False
            
        # 跌停不能卖出
        if change_ratio <= -self.limit_ratio and order.side.value == "sell":
            logging.warning(f"{order.code} hit limit down, sell order rejected")
            return False
            
        return True
        
    def get_name(self) -> str:
        return "limit_up_down"


class MinOrderAmountRule(Rule):
    """最小下单量规则"""
    
    def __init__(self, min_amount: int = 100):
        self.min_amount = min_amount
        
    def check(self, order: Any, trade_center: Any) -> bool:
        # A股买入必须是100的整数倍
        if order.side.value == "buy" and order.quantity % self.min_amount != 0:
            logging.warning(f"Order quantity {order.quantity} must be multiple of {self.min_amount}")
            return False
        return True
        
    def get_name(self) -> str:
        return "min_order_amount"


class STStockRule(Rule):
    """ST股票限制规则"""
    
    def __init__(self, allow_st: bool = False):
        self.allow_st = allow_st
        
    def check(self, order: Any, trade_center: Any) -> bool:
        if not self.allow_st:
            # TODO: 检查股票名称是否包含ST
            # 这里需要获取股票信息的接口
            pass
        return True
        
    def get_name(self) -> str:
        return "st_stock"


class VolumeRatioRule(Rule):
    """成交量比例限制规则"""
    
    def __init__(self, max_ratio: float = 0.25):
        self.max_ratio = max_ratio
        
    def check(self, order: Any, trade_center: Any) -> bool:
        # 获取最近的成交量
        df = trade_center.data_center.get_kline(
            order.adapter_id, order.code, "1d",
            end_time=trade_center.current_time, count=5
        )
        
        if df.empty:
            return True
            
        avg_volume = df['volume'].mean()
        if order.quantity > avg_volume * self.max_ratio:
            logging.warning(
                f"Order quantity {order.quantity} exceeds "
                f"{self.max_ratio * 100}% of average volume {avg_volume}"
            )
            return False
            
        return True
        
    def get_name(self) -> str:
        return "volume_ratio"


class CashAvailableRule(Rule):
    """资金可用性检查规则"""
    
    def check(self, order: Any, trade_center: Any) -> bool:
        if order.side.value != "buy":
            return True
            
        account = trade_center.get_account(order.adapter_id)
        if not account:
            return False
            
        # 估算所需资金
        price = trade_center.get_price(order.adapter_id, order.code)
        if not price:
            return False
            
        # 计算手续费
        value = price * order.quantity
        commission_config = trade_center.commission_config.get('cn_stock', {})
        commission = max(
            value * commission_config['buy_commission_rate'],
            commission_config['min_commission']
        )
        
        total_cost = value + commission
        
        if account.available_cash < total_cost:
            logging.warning(
                f"Insufficient cash: need {total_cost}, "
                f"available {account.available_cash}"
            )
            return False
            
        return True
        
    def get_name(self) -> str:
        return "cash_available"


class PositionAvailableRule(Rule):
    """持仓可用性检查规则"""
    
    def check(self, order: Any, trade_center: Any) -> bool:
        if order.side.value != "sell":
            return True
            
        position = trade_center.get_position(order.adapter_id, order.code)
        if not position:
            logging.warning(f"No position for {order.code}")
            return False
            
        if position.available_quantity < order.quantity:
            logging.warning(
                f"Insufficient position: need {order.quantity}, "
                f"available {position.available_quantity}"
            )
            return False
            
        return True
        
    def get_name(self) -> str:
        return "position_available"


class MarketCapRule(Rule):
    """市值限制规则"""
    
    def __init__(self, min_cap: float = 0, max_cap: float = float('inf')):
        self.min_cap = min_cap
        self.max_cap = max_cap
        
    def check(self, order: Any, trade_center: Any) -> bool:
        # TODO: 获取股票市值数据
        # 这里需要从数据源获取股票的总市值信息
        return True
        
    def get_name(self) -> str:
        return "market_cap"


class CustomRule(Rule):
    """自定义规则"""
    
    def __init__(self, name: str, check_func: Callable[[Any, Any], bool]):
        self.name = name
        self.check_func = check_func
        
    def check(self, order: Any, trade_center: Any) -> bool:
        return self.check_func(order, trade_center)
        
    def get_name(self) -> str:
        return self.name


class RuleEngine:
    """规则引擎"""
    
    def __init__(self):
        self.rules: List[Rule] = []
        self.logger = logging.getLogger(__name__)
        
        # 默认规则
        self._init_default_rules()
        
    def _init_default_rules(self):
        """初始化默认规则"""
        self.add_rule(StopTradingRule())
        self.add_rule(LimitUpDownRule())
        self.add_rule(MinOrderAmountRule())
        self.add_rule(VolumeRatioRule())
        self.add_rule(CashAvailableRule())
        self.add_rule(PositionAvailableRule())
        
    def add_rule(self, rule: Rule):
        """添加规则"""
        self.rules.append(rule)
        
    def remove_rule(self, rule_name: str):
        """移除规则"""
        self.rules = [r for r in self.rules if r.get_name() != rule_name]
        
    def clear_rules(self):
        """清空所有规则"""
        self.rules.clear()
        
    def check_order(self, order: Any, trade_center: Any) -> bool:
        """
        检查订单是否通过所有规则
        
        Args:
            order: 订单对象
            trade_center: 交易中心实例
            
        Returns:
            是否通过所有规则检查
        """
        for rule in self.rules:
            try:
                if not rule.check(order, trade_center):
                    self.logger.info(
                        f"Order {order.order_id} failed rule: {rule.get_name()}"
                    )
                    return False
            except Exception as e:
                self.logger.error(
                    f"Error checking rule {rule.get_name()}: {str(e)}"
                )
                # 规则检查出错时，保守起见返回False
                return False
                
        return True
        
    def get_active_rules(self) -> List[str]:
        """获取当前激活的规则列表"""
        return [rule.get_name() for rule in self.rules] 