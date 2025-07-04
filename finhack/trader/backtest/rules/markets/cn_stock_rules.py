"""
A股市场特定规则
包括T+1规则、科创板规则、创业板规则等
"""

from typing import Dict, Any, Optional
from datetime import datetime, time
import math

from ..base_rule import MarketRule, RuleResult, RuleType


class T1Rule(MarketRule):
    """T+1规则 - A股特定"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("t1_rule", "cn_stock", RuleType.TRADING, config=config)
        self.description = "A股T+1交易规则"
        
        # T+1配置
        self.enable_t1 = self.get_config('enable_t1', True)  # 是否启用T+1规则
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查T+1规则"""
        if not self.enable_t1:
            return RuleResult(passed=True, message="T+1规则已禁用")
        
        symbol = data.get('symbol', '')
        order_side = data.get('side', 'buy')
        order_amount = data.get('amount', 0)
        
        if order_side == 'sell':
            # 检查今日买入的股票不能卖出
            current_date = context.current_date
            
            # 获取今日买入的数量
            today_buy_amount = 0
            for trade in context.logs.get('trade_list', []):
                if (trade.get('date') == current_date and 
                    trade.get('symbol') == symbol and 
                    trade.get('side') == 'buy'):
                    today_buy_amount += trade.get('amount', 0)
            
            # 计算可卖出数量
            current_position = context.portfolio.positions.get(symbol, 0)
            sellable_amount = current_position - today_buy_amount
            
            if order_amount > sellable_amount:
                return RuleResult(
                    passed=False,
                    message=f"T+1限制：今日买入股票不能当日卖出，可卖数量：{sellable_amount}"
                )
        
        return RuleResult(passed=True, message="T+1规则检查通过")


class StarMarketRule(MarketRule):
    """科创板规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("star_market", "cn_stock", RuleType.MARKET, config=config)
        self.description = "科创板特殊规则"
        
        # 科创板配置
        self.allow_star_market = self.get_config('allow_star_market', True)  # 是否允许科创板交易
        self.star_limit_ratio = self.get_config('star_limit_ratio', 0.20)  # 科创板涨跌停比例
        self.min_order_amount = self.get_config('min_order_amount', 200)  # 最小委托数量
        self.order_step = self.get_config('order_step', 1)  # 委托数量步长
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查科创板规则"""
        symbol = data.get('symbol', '')
        
        # 检查是否为科创板股票（688开头）
        if not self._is_star_market_stock(symbol):
            return RuleResult(passed=True, message="非科创板股票")
        
        if not self.allow_star_market:
            return RuleResult(
                passed=False,
                message="科创板交易已禁用"
            )
        
        # 检查委托数量规则
        order_amount = data.get('amount', 0)
        
        # 科创板首次委托最小200股
        if order_amount < self.min_order_amount:
            return RuleResult(
                passed=False,
                message=f"科创板最小委托数量为 {self.min_order_amount} 股"
            )
        
        # 超过200股部分可以1股为单位委托
        if order_amount >= self.min_order_amount:
            excess_amount = order_amount - self.min_order_amount
            if excess_amount % self.order_step != 0:
                return RuleResult(
                    passed=False,
                    message=f"科创板超过 {self.min_order_amount} 股部分需以 {self.order_step} 股为单位"
                )
        
        return RuleResult(passed=True, message="科创板规则检查通过")
    
    def _is_star_market_stock(self, symbol: str) -> bool:
        """检查是否为科创板股票"""
        return symbol.startswith('688')


class GemRule(MarketRule):
    """创业板规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("gem_rule", "cn_stock", RuleType.MARKET, config=config)
        self.description = "创业板特殊规则"
        
        # 创业板配置
        self.allow_gem = self.get_config('allow_gem', True)  # 是否允许创业板交易
        self.gem_limit_ratio = self.get_config('gem_limit_ratio', 0.20)  # 创业板涨跌停比例
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查创业板规则"""
        symbol = data.get('symbol', '')
        
        # 检查是否为创业板股票（300开头）
        if not self._is_gem_stock(symbol):
            return RuleResult(passed=True, message="非创业板股票")
        
        if not self.allow_gem:
            return RuleResult(
                passed=False,
                message="创业板交易已禁用"
            )
        
        return RuleResult(passed=True, message="创业板规则检查通过")
    
    def _is_gem_stock(self, symbol: str) -> bool:
        """检查是否为创业板股票"""
        return symbol.startswith('300')


class BseRule(MarketRule):
    """北交所规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("bse_rule", "cn_stock", RuleType.MARKET, config=config)
        self.description = "北交所特殊规则"
        
        # 北交所配置
        self.allow_bse = self.get_config('allow_bse', True)  # 是否允许北交所交易
        self.bse_limit_ratio = self.get_config('bse_limit_ratio', 0.30)  # 北交所涨跌停比例
        self.min_order_amount = self.get_config('min_order_amount', 100)  # 最小委托数量
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查北交所规则"""
        symbol = data.get('symbol', '')
        
        # 检查是否为北交所股票（8开头或43开头）
        if not self._is_bse_stock(symbol):
            return RuleResult(passed=True, message="非北交所股票")
        
        if not self.allow_bse:
            return RuleResult(
                passed=False,
                message="北交所交易已禁用"
            )
        
        return RuleResult(passed=True, message="北交所规则检查通过")
    
    def _is_bse_stock(self, symbol: str) -> bool:
        """检查是否为北交所股票"""
        return symbol.startswith('8') or symbol.startswith('43')


class MarginTradingRule(MarketRule):
    """融资融券规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("margin_trading", "cn_stock", RuleType.TRADING, config=config)
        self.description = "融资融券交易规则"
        
        # 融资融券配置
        self.enable_margin = self.get_config('enable_margin', False)  # 是否启用融资融券
        self.margin_ratio = self.get_config('margin_ratio', 0.50)  # 保证金比例
        self.max_margin_ratio = self.get_config('max_margin_ratio', 2.0)  # 最大融资比例
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查融资融券规则"""
        if not self.enable_margin:
            return RuleResult(passed=True, message="未启用融资融券")
        
        order_side = data.get('side', 'buy')
        order_value = data.get('value', 0)
        
        if order_side == 'buy':
            # 检查融资买入
            available_cash = context.portfolio.cash
            
            if order_value > available_cash:
                # 需要融资
                margin_needed = order_value - available_cash
                margin_collateral = context.portfolio.total_value * self.margin_ratio
                
                if margin_needed > margin_collateral:
                    return RuleResult(
                        passed=False,
                        message=f"融资额度不足，需要 {margin_needed}，可用 {margin_collateral}"
                    )
        
        return RuleResult(passed=True, message="融资融券规则检查通过")


class CallAuctionRule(MarketRule):
    """集合竞价规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("call_auction", "cn_stock", RuleType.TRADING, config=config)
        self.description = "集合竞价规则"
        
        # 集合竞价时间配置
        self.morning_auction_start = time(9, 15)
        self.morning_auction_end = time(9, 25)
        self.closing_auction_start = time(14, 57)
        self.closing_auction_end = time(15, 0)
        
        # 集合竞价配置
        self.allow_cancel_in_auction = self.get_config('allow_cancel_in_auction', False)  # 集合竞价期间是否允许撤单
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查集合竞价规则"""
        current_time = context.current_dt.time()
        order_type = data.get('order_type', 'limit')
        
        # 检查是否在集合竞价时间
        in_morning_auction = self.morning_auction_start <= current_time <= self.morning_auction_end
        in_closing_auction = self.closing_auction_start <= current_time <= self.closing_auction_end
        
        if in_morning_auction or in_closing_auction:
            # 集合竞价期间只允许限价委托
            if order_type != 'limit':
                return RuleResult(
                    passed=False,
                    message="集合竞价期间只允许限价委托"
                )
            
            # 检查撤单限制
            if data.get('action') == 'cancel' and not self.allow_cancel_in_auction:
                auction_period = "开盘" if in_morning_auction else "收盘"
                return RuleResult(
                    passed=False,
                    message=f"{auction_period}集合竞价期间不允许撤单"
                )
        
        return RuleResult(passed=True, message="集合竞价规则检查通过")


class BlockTradingRule(MarketRule):
    """大宗交易规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("block_trading", "cn_stock", RuleType.TRADING, config=config)
        self.description = "大宗交易规则"
        
        # 大宗交易配置
        self.enable_block_trading = self.get_config('enable_block_trading', False)  # 是否启用大宗交易
        self.min_block_amount = self.get_config('min_block_amount', 500000)  # 最小大宗交易数量（股）
        self.min_block_value = self.get_config('min_block_value', 3000000)  # 最小大宗交易金额
        
        # 大宗交易时间
        self.block_trading_start = time(15, 0)
        self.block_trading_end = time(15, 30)
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查大宗交易规则"""
        if not self.enable_block_trading:
            return RuleResult(passed=True, message="未启用大宗交易")
        
        order_amount = data.get('amount', 0)
        order_value = data.get('value', 0)
        current_time = context.current_dt.time()
        
        # 检查是否满足大宗交易条件
        is_block_trade = (order_amount >= self.min_block_amount or 
                         order_value >= self.min_block_value)
        
        if is_block_trade:
            # 检查大宗交易时间
            if not (self.block_trading_start <= current_time <= self.block_trading_end):
                return RuleResult(
                    passed=False,
                    message=f"大宗交易时间为 {self.block_trading_start}-{self.block_trading_end}"
                )
        
        return RuleResult(passed=True, message="大宗交易规则检查通过")


class StockConnectRule(MarketRule):
    """沪深港通规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("stock_connect", "cn_stock", RuleType.TRADING, config=config)
        self.description = "沪深港通交易规则"
        
        # 港股通配置
        self.enable_hk_connect = self.get_config('enable_hk_connect', False)  # 是否启用港股通
        self.hk_connect_quota = self.get_config('hk_connect_quota', 52000000000)  # 港股通每日额度
        self.min_order_value = self.get_config('min_order_value', 10000)  # 最小委托金额（港币）
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查港股通规则"""
        symbol = data.get('symbol', '')
        
        # 检查是否为港股通标的
        if not self._is_hk_connect_stock(symbol):
            return RuleResult(passed=True, message="非港股通标的")
        
        if not self.enable_hk_connect:
            return RuleResult(
                passed=False,
                message="港股通交易已禁用"
            )
        
        # 检查最小委托金额
        order_value = data.get('value', 0)
        if order_value < self.min_order_value:
            return RuleResult(
                passed=False,
                message=f"港股通最小委托金额为 {self.min_order_value} 港币"
            )
        
        return RuleResult(passed=True, message="港股通规则检查通过")
    
    def _is_hk_connect_stock(self, symbol: str) -> bool:
        """检查是否为港股通标的"""
        # 简化实现，实际需要查询港股通标的名单
        return symbol.endswith('.HK')


class ChinaClearRule(MarketRule):
    """中国结算规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("china_clear", "cn_stock", RuleType.TRADING, config=config)
        self.description = "中国结算交易规则"
        
        # 结算配置
        self.settlement_period = self.get_config('settlement_period', 1)  # 结算周期T+1
        self.enable_dvp = self.get_config('enable_dvp', True)  # 是否启用券款对付
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查结算规则"""
        # 简化实现
        return RuleResult(passed=True, message="结算规则检查通过")


class CNYExchangeRule(MarketRule):
    """人民币汇率规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("cny_exchange", "cn_stock", RuleType.TRADING, config=config)
        self.description = "人民币汇率规则"
        
        # 汇率配置
        self.enable_currency_hedge = self.get_config('enable_currency_hedge', False)  # 是否启用汇率对冲
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查汇率规则"""
        # 对于A股，通常不涉及汇率转换
        return RuleResult(passed=True, message="汇率规则检查通过") 