"""
市场规则实现
包括涨跌停、停牌、退市、ST标记等规则
"""

from typing import Dict, Any, Optional
from datetime import datetime, time
import math

from ..base_rule import BaseRule, RuleResult, RuleType


class PriceLimitRule(BaseRule):
    """涨跌停规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("price_limit", RuleType.MARKET, config=config)
        self.description = "检查涨跌停限制"
        
        # 涨跌停配置
        self.limit_ratio = self.get_config('limit_ratio', 0.10)  # 10%涨跌停
        self.st_limit_ratio = self.get_config('st_limit_ratio', 0.05)  # ST股票5%涨跌停
        self.star_limit_ratio = self.get_config('star_limit_ratio', 0.20)  # 科创板20%涨跌停
        self.gem_limit_ratio = self.get_config('gem_limit_ratio', 0.20)  # 创业板20%涨跌停
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查涨跌停限制"""
        symbol = data.get('symbol', '')
        order_price = data.get('price', 0)
        
        # 获取前一日收盘价
        prev_close = getattr(context.data_center, 'get_prev_close', lambda x: 0)(symbol)
        if prev_close <= 0:
            return RuleResult(passed=True, message="无前一日收盘价数据")
        
        # 确定涨跌停比例
        limit_ratio = self.limit_ratio
        if self._is_st_stock(symbol):
            limit_ratio = self.st_limit_ratio
        elif self._is_star_market(symbol):
            limit_ratio = self.star_limit_ratio
        elif self._is_gem_stock(symbol):
            limit_ratio = self.gem_limit_ratio
        
        # 计算涨跌停价格
        up_limit = prev_close * (1 + limit_ratio)
        down_limit = prev_close * (1 - limit_ratio)
        
        # 检查订单价格是否超过涨跌停
        if order_price > up_limit:
            return RuleResult(
                passed=False,
                message=f"订单价格 {order_price} 超过涨停价 {up_limit:.2f}"
            )
        elif order_price < down_limit:
            return RuleResult(
                passed=False,
                message=f"订单价格 {order_price} 低于跌停价 {down_limit:.2f}"
            )
        
        return RuleResult(passed=True, message="涨跌停检查通过")
    
    def _is_st_stock(self, symbol: str) -> bool:
        """检查是否为ST股票"""
        # 简化实现，实际应该查询股票信息
        return 'ST' in symbol
    
    def _is_star_market(self, symbol: str) -> bool:
        """检查是否为科创板股票"""
        # 科创板代码以688开头
        return symbol.startswith('688')
    
    def _is_gem_stock(self, symbol: str) -> bool:
        """检查是否为创业板股票"""
        # 创业板代码以300开头
        return symbol.startswith('300')


class SuspensionRule(BaseRule):
    """停牌规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("suspension", RuleType.MARKET, config=config)
        self.description = "检查股票是否停牌"
        
        # 停牌配置
        self.check_suspension = self.get_config('check_suspension', True)
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查停牌状态"""
        if not self.check_suspension:
            return RuleResult(passed=True, message="停牌检查已禁用")
        
        symbol = data.get('symbol', '')
        
        # 获取停牌状态
        is_suspended = getattr(context.data_center, 'is_suspended', lambda x: False)(symbol)
        
        if is_suspended:
            return RuleResult(
                passed=False,
                message=f"股票 {symbol} 已停牌"
            )
        
        return RuleResult(passed=True, message="停牌检查通过")


class DelistingRule(BaseRule):
    """退市规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("delisting", RuleType.MARKET, config=config)
        self.description = "检查股票是否退市"
        
        # 退市配置
        self.check_delisting = self.get_config('check_delisting', True)
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查退市状态"""
        if not self.check_delisting:
            return RuleResult(passed=True, message="退市检查已禁用")
        
        symbol = data.get('symbol', '')
        
        # 获取退市状态
        is_delisted = getattr(context.data_center, 'is_delisted', lambda x: False)(symbol)
        
        if is_delisted:
            return RuleResult(
                passed=False,
                message=f"股票 {symbol} 已退市"
            )
        
        return RuleResult(passed=True, message="退市检查通过")


class STMarkRule(BaseRule):
    """ST标记规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("st_mark", RuleType.MARKET, config=config)
        self.description = "检查ST股票交易限制"
        
        # ST股票配置
        self.allow_st_trading = self.get_config('allow_st_trading', True)
        self.st_max_position_ratio = self.get_config('st_max_position_ratio', 0.05)  # ST股票最大持仓比例
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查ST股票交易限制"""
        symbol = data.get('symbol', '')
        
        # 检查是否为ST股票
        is_st = self._is_st_stock(symbol)
        
        if is_st:
            if not self.allow_st_trading:
                return RuleResult(
                    passed=False,
                    message=f"ST股票 {symbol} 交易已禁用"
                )
            
            # 检查ST股票持仓比例
            order_side = data.get('side', 'buy')
            if order_side == 'buy':
                order_amount = data.get('amount', 0)
                current_position = context.portfolio.positions.get(symbol, 0)
                new_position = current_position + order_amount
                
                current_price = getattr(context.data_center, 'get_current_price', lambda x: 0)(symbol)
                position_value = new_position * current_price
                total_value = context.portfolio.total_value
                
                if total_value > 0:
                    position_ratio = position_value / total_value
                    if position_ratio > self.st_max_position_ratio:
                        return RuleResult(
                            passed=False,
                            message=f"ST股票持仓比例 {position_ratio:.2%} 超过最大限制 {self.st_max_position_ratio:.2%}"
                        )
        
        return RuleResult(passed=True, message="ST股票检查通过")
    
    def _is_st_stock(self, symbol: str) -> bool:
        """检查是否为ST股票"""
        # 简化实现，实际应该查询股票信息
        return 'ST' in symbol or '*ST' in symbol


class NewStockRule(BaseRule):
    """新股规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("new_stock", RuleType.MARKET, config=config)
        self.description = "检查新股交易限制"
        
        # 新股配置
        self.allow_new_stock = self.get_config('allow_new_stock', True)
        self.new_stock_days = self.get_config('new_stock_days', 30)  # 新股定义天数
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查新股交易限制"""
        if not self.allow_new_stock:
            symbol = data.get('symbol', '')
            
            # 获取上市日期
            list_date = getattr(context.data_center, 'get_list_date', lambda x: None)(symbol)
            if list_date:
                current_date = context.current_date
                days_since_list = (current_date - list_date).days
                
                if days_since_list <= self.new_stock_days:
                    return RuleResult(
                        passed=False,
                        message=f"新股 {symbol} 上市仅 {days_since_list} 天，禁止交易"
                    )
        
        return RuleResult(passed=True, message="新股检查通过")


class TradingDayRule(BaseRule):
    """交易日规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("trading_day", RuleType.MARKET, config=config)
        self.description = "检查是否为交易日"
        
        # 交易日配置
        self.check_trading_day = self.get_config('check_trading_day', True)
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查交易日"""
        if not self.check_trading_day:
            return RuleResult(passed=True, message="交易日检查已禁用")
        
        current_date = context.current_date
        
        # 检查是否为交易日
        is_trading_day = getattr(context.data_center, 'is_trading_day', lambda x: True)(current_date)
        
        if not is_trading_day:
            return RuleResult(
                passed=False,
                message=f"{current_date} 不是交易日"
            )
        
        return RuleResult(passed=True, message="交易日检查通过")


class MarketHoursRule(BaseRule):
    """市场开闭市规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("market_hours", RuleType.MARKET, config=config)
        self.description = "检查市场开闭市时间"
        
        # 市场时间配置
        self.market_open = time(9, 30)
        self.market_close = time(15, 0)
        self.lunch_start = time(11, 30)
        self.lunch_end = time(13, 0)
        
        # 从配置中读取
        if config:
            if 'market_open' in config:
                self.market_open = time(*config['market_open'])
            if 'market_close' in config:
                self.market_close = time(*config['market_close'])
            if 'lunch_start' in config:
                self.lunch_start = time(*config['lunch_start'])
            if 'lunch_end' in config:
                self.lunch_end = time(*config['lunch_end'])
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查市场开闭市时间"""
        current_time = context.current_dt.time()
        
        # 检查是否在开市时间
        if current_time < self.market_open or current_time > self.market_close:
            return RuleResult(
                passed=False,
                message=f"不在开市时间内（{self.market_open}-{self.market_close}）"
            )
        
        # 检查是否在午休时间
        if self.lunch_start <= current_time <= self.lunch_end:
            return RuleResult(
                passed=False,
                message=f"在午休时间内（{self.lunch_start}-{self.lunch_end}）"
            )
        
        return RuleResult(passed=True, message="市场时间检查通过")


class LiquidityRule(BaseRule):
    """流动性规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("liquidity", RuleType.MARKET, config=config)
        self.description = "检查股票流动性"
        
        # 流动性配置
        self.min_turnover = self.get_config('min_turnover', 0.01)  # 最小换手率
        self.min_volume = self.get_config('min_volume', 1000000)  # 最小成交量
        self.min_market_value = self.get_config('min_market_value', 1000000000)  # 最小市值
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查流动性"""
        symbol = data.get('symbol', '')
        
        # 获取流动性指标
        turnover = getattr(context.data_center, 'get_turnover', lambda x: 0)(symbol)
        volume = getattr(context.data_center, 'get_volume', lambda x: 0)(symbol)
        market_value = getattr(context.data_center, 'get_market_value', lambda x: 0)(symbol)
        
        # 检查换手率
        if turnover < self.min_turnover:
            return RuleResult(
                passed=False,
                message=f"换手率 {turnover:.2%} 低于最小要求 {self.min_turnover:.2%}"
            )
        
        # 检查成交量
        if volume < self.min_volume:
            return RuleResult(
                passed=False,
                message=f"成交量 {volume} 低于最小要求 {self.min_volume}"
            )
        
        # 检查市值
        if market_value < self.min_market_value:
            return RuleResult(
                passed=False,
                message=f"市值 {market_value} 低于最小要求 {self.min_market_value}"
            )
        
        return RuleResult(passed=True, message="流动性检查通过")


class VolatilityRule(BaseRule):
    """波动率规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("volatility", RuleType.MARKET, config=config)
        self.description = "检查股票波动率"
        
        # 波动率配置
        self.max_volatility = self.get_config('max_volatility', 0.05)  # 最大日波动率
        self.min_volatility = self.get_config('min_volatility', 0.005)  # 最小日波动率
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查波动率"""
        symbol = data.get('symbol', '')
        
        # 获取波动率
        volatility = getattr(context.data_center, 'get_volatility', lambda x: 0)(symbol)
        
        # 检查最大波动率
        if volatility > self.max_volatility:
            return RuleResult(
                passed=False,
                message=f"波动率 {volatility:.2%} 超过最大限制 {self.max_volatility:.2%}"
            )
        
        # 检查最小波动率
        if volatility < self.min_volatility:
            return RuleResult(
                passed=False,
                message=f"波动率 {volatility:.2%} 低于最小要求 {self.min_volatility:.2%}"
            )
        
        return RuleResult(passed=True, message="波动率检查通过")


class PriceRangeRule(BaseRule):
    """价格范围规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("price_range", RuleType.MARKET, config=config)
        self.description = "检查股票价格范围"
        
        # 价格范围配置
        self.min_price = self.get_config('min_price', 1.0)  # 最小价格
        self.max_price = self.get_config('max_price', 1000.0)  # 最大价格
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查价格范围"""
        symbol = data.get('symbol', '')
        current_price = getattr(context.data_center, 'get_current_price', lambda x: 0)(symbol)
        
        # 检查最小价格
        if current_price < self.min_price:
            return RuleResult(
                passed=False,
                message=f"股价 {current_price} 低于最小价格 {self.min_price}"
            )
        
        # 检查最大价格
        if current_price > self.max_price:
            return RuleResult(
                passed=False,
                message=f"股价 {current_price} 超过最大价格 {self.max_price}"
            )
        
        return RuleResult(passed=True, message="价格范围检查通过")


class MarketCapRule(BaseRule):
    """市值规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("market_cap", RuleType.MARKET, config=config)
        self.description = "检查股票市值"
        
        # 市值配置
        self.min_market_cap = self.get_config('min_market_cap', 1000000000)  # 最小市值10亿
        self.max_market_cap = self.get_config('max_market_cap', 1000000000000)  # 最大市值1万亿
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查市值"""
        symbol = data.get('symbol', '')
        market_cap = getattr(context.data_center, 'get_market_cap', lambda x: 0)(symbol)
        
        # 检查最小市值
        if market_cap < self.min_market_cap:
            return RuleResult(
                passed=False,
                message=f"市值 {market_cap/100000000:.2f}亿 低于最小要求 {self.min_market_cap/100000000:.2f}亿"
            )
        
        # 检查最大市值
        if market_cap > self.max_market_cap:
            return RuleResult(
                passed=False,
                message=f"市值 {market_cap/100000000:.2f}亿 超过最大限制 {self.max_market_cap/100000000:.2f}亿"
            )
        
        return RuleResult(passed=True, message="市值检查通过") 