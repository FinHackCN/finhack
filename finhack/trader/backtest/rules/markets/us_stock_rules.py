"""
美股市场特定规则
包括T+0规则、PDT规则、盘前盘后交易规则等
"""

from typing import Dict, Any, Optional
from datetime import datetime, time
import math

from ..base_rule import MarketRule, RuleResult, RuleType


class UsT0Rule(MarketRule):
    """T+0规则 - 美股特定"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("us_t0_rule", "us_stock", RuleType.TRADING, config=config)
        self.description = "美股T+0交易规则"
        
        # T+0配置
        self.enable_t0 = self.get_config('enable_t0', True)  # 是否启用T+0规则
        self.settlement_period = self.get_config('settlement_period', 2)  # T+2结算
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查T+0规则"""
        if not self.enable_t0:
            return RuleResult(passed=True, message="T+0规则已禁用")
        
        order_side = data.get('side', 'buy')
        symbol = data.get('symbol', '')
        order_amount = data.get('amount', 0)
        
        # 检查是否为现金账户
        if context.account_type == 'cash':
            # 现金账户的Good Faith规则
            if order_side == 'buy':
                # 需要检查是否有足够的已结算资金
                settled_cash = getattr(context.portfolio, 'settled_cash', context.portfolio.cash)
                order_value = data.get('value', 0)
                
                if order_value > settled_cash:
                    return RuleResult(
                        passed=False,
                        message=f"现金账户买入需要已结算资金，需要${order_value:.2f}，可用${settled_cash:.2f}"
                    )
            
            elif order_side == 'sell':
                # 检查是否有已结算的持仓
                settled_position = self._get_settled_position(context, symbol)
                
                if order_amount > settled_position:
                    return RuleResult(
                        passed=False,
                        message=f"现金账户卖出需要已结算持仓，需要{order_amount}股，可用{settled_position}股"
                    )
                
                # 当日买入后立即卖出会触发Good Faith Violation
                if self._is_same_day_buy_sell(context, symbol):
                    return RuleResult(
                        passed=False,
                        message=f"现金账户当日买入后立即卖出违反Good Faith规则"
                    )
        
        return RuleResult(passed=True, message="T+0规则检查通过")
    
    def _get_settled_position(self, context: Any, symbol: str) -> int:
        """获取已结算的持仓"""
        # 简化实现，实际需要跟踪T+2结算
        current_position = context.portfolio.positions.get(symbol, 0)
        unsettled_buys = self._get_unsettled_buys(context, symbol)
        return max(0, current_position - unsettled_buys)
    
    def _get_unsettled_buys(self, context: Any, symbol: str) -> int:
        """获取未结算的买入数量"""
        # 简化实现，实际需要查询T+2内的买入记录
        return 0
    
    def _is_same_day_buy_sell(self, context: Any, symbol: str) -> bool:
        """检查是否为当日买入后卖出"""
        current_date = context.current_date
        
        # 检查当日是否有买入记录
        for trade in context.logs.get('trade_list', []):
            if (trade.get('date') == current_date and 
                trade.get('symbol') == symbol and 
                trade.get('side') == 'buy'):
                return True
        
        return False


class PdtRule(MarketRule):
    """Pattern Day Trader规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("pdt", "us_stock", RuleType.RISK, config=config)
        self.description = "美股日内交易者规则"
        
        # PDT规则配置
        self.enable_pdt = self.get_config('enable_pdt', True)
        self.min_equity = self.get_config('min_equity', 25000)  # 最小资产25000美元
        self.max_day_trades_per_week = self.get_config('max_day_trades_per_week', 3)  # 每周最多3次日内交易
        self.day_trade_buying_power_ratio = self.get_config('day_trade_buying_power_ratio', 4.0)  # 日内交易购买力比例
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查PDT规则"""
        if not self.enable_pdt:
            return RuleResult(passed=True, message="PDT规则已禁用")
        
        # 检查账户资产
        account_equity = context.portfolio.total_value
        
        if account_equity < self.min_equity:
            # 小账户，检查日内交易次数限制
            week_day_trades = self._count_week_day_trades(context)
            
            if self._is_day_trade(context, data) and week_day_trades >= self.max_day_trades_per_week:
                return RuleResult(
                    passed=False,
                    message=f"违反PDT规则：账户资产 ${account_equity:.2f} 低于 ${self.min_equity}，本周已进行 {week_day_trades} 次日内交易"
                )
        else:
            # 大账户，检查日内交易购买力
            if self._is_day_trade(context, data):
                available_buying_power = account_equity * self.day_trade_buying_power_ratio
                order_value = data.get('value', 0)
                
                if order_value > available_buying_power:
                    return RuleResult(
                        passed=False,
                        message=f"超过日内交易购买力，可用: ${available_buying_power:.2f}，需要: ${order_value:.2f}"
                    )
        
        return RuleResult(passed=True, message="PDT规则检查通过")
    
    def _is_day_trade(self, context: Any, data: Dict[str, Any]) -> bool:
        """检查是否为日内交易"""
        symbol = data.get('symbol', '')
        order_side = data.get('side', 'buy')
        current_date = context.current_date
        
        if order_side == 'sell':
            # 检查当日是否有买入该股票
            for trade in context.logs.get('trade_list', []):
                if (trade.get('date') == current_date and 
                    trade.get('symbol') == symbol and 
                    trade.get('side') == 'buy'):
                    return True
        
        return False
    
    def _count_week_day_trades(self, context: Any) -> int:
        """统计本周日内交易次数"""
        current_date = context.current_date
        week_start = current_date - pd.Timedelta(days=current_date.weekday())
        
        day_trades = 0
        daily_symbols = {}
        
        for trade in context.logs.get('trade_list', []):
            trade_date = trade.get('date')
            if trade_date >= week_start and trade_date <= current_date:
                symbol = trade.get('symbol', '')
                side = trade.get('side', '')
                
                if trade_date not in daily_symbols:
                    daily_symbols[trade_date] = {}
                
                if symbol not in daily_symbols[trade_date]:
                    daily_symbols[trade_date][symbol] = {'buy': 0, 'sell': 0}
                
                daily_symbols[trade_date][symbol][side] += 1
        
        # 计算日内交易次数
        for date_symbols in daily_symbols.values():
            for symbol_trades in date_symbols.values():
                day_trades += min(symbol_trades['buy'], symbol_trades['sell'])
        
        return day_trades


class ExtendedHoursRule(MarketRule):
    """盘前盘后交易规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("extended_hours", "us_stock", RuleType.TRADING, config=config)
        self.description = "美股盘前盘后交易规则"
        
        # 交易时间配置（美东时间）
        self.pre_market_start = time(4, 0)    # 盘前开始
        self.pre_market_end = time(9, 30)     # 盘前结束
        self.regular_start = time(9, 30)      # 正常交易开始
        self.regular_end = time(16, 0)        # 正常交易结束
        self.after_hours_start = time(16, 0)  # 盘后开始
        self.after_hours_end = time(20, 0)    # 盘后结束
        
        # 盘前盘后交易配置
        self.enable_extended_hours = self.get_config('enable_extended_hours', False)
        self.min_extended_order_value = self.get_config('min_extended_order_value', 1000)  # 最小订单金额
        self.max_extended_order_value = self.get_config('max_extended_order_value', 100000)  # 最大订单金额
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查盘前盘后交易规则"""
        current_time = context.current_dt.time()
        order_value = data.get('value', 0)
        order_type = data.get('order_type', 'limit')
        
        # 判断当前交易时段
        trading_session = self._get_trading_session(current_time)
        
        if trading_session == 'regular':
            return RuleResult(passed=True, message="正常交易时间")
        
        elif trading_session in ['pre_market', 'after_hours']:
            if not self.enable_extended_hours:
                return RuleResult(
                    passed=False,
                    message=f"盘前盘后交易已禁用，当前时段: {trading_session}"
                )
            
            # 盘前盘后只允许限价单
            if order_type != 'limit':
                return RuleResult(
                    passed=False,
                    message=f"盘前盘后只允许限价单，当前订单类型: {order_type}"
                )
            
            # 检查订单金额限制
            if order_value < self.min_extended_order_value:
                return RuleResult(
                    passed=False,
                    message=f"盘前盘后订单金额 ${order_value:.2f} 低于最小金额 ${self.min_extended_order_value}"
                )
            
            if order_value > self.max_extended_order_value:
                return RuleResult(
                    passed=False,
                    message=f"盘前盘后订单金额 ${order_value:.2f} 超过最大金额 ${self.max_extended_order_value}"
                )
            
            return RuleResult(passed=True, message=f"{trading_session}交易检查通过")
        
        else:
            return RuleResult(
                passed=False,
                message="不在交易时间内"
            )
    
    def _get_trading_session(self, current_time: time) -> str:
        """获取当前交易时段"""
        if self.pre_market_start <= current_time < self.pre_market_end:
            return 'pre_market'
        elif self.regular_start <= current_time < self.regular_end:
            return 'regular'
        elif self.after_hours_start <= current_time < self.after_hours_end:
            return 'after_hours'
        else:
            return 'closed'


class UsCurrencyConversionRule(MarketRule):
    """美元汇率转换规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("us_currency_conversion", "us_stock", RuleType.TRADING, config=config)
        self.description = "美元汇率转换规则"
        
        # 汇率配置
        self.enable_currency_conversion = self.get_config('enable_currency_conversion', True)
        self.default_exchange_rate = self.get_config('default_exchange_rate', 7.2)  # 美元兑人民币
        self.conversion_fee_rate = self.get_config('conversion_fee_rate', 0.002)  # 汇率转换费率
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查汇率转换规则"""
        if not self.enable_currency_conversion:
            return RuleResult(passed=True, message="汇率转换已禁用")
        
        order_value_usd = data.get('value', 0)
        
        # 获取实时汇率（简化实现）
        exchange_rate = getattr(context.data_center, 'get_usd_exchange_rate', 
                               lambda: self.default_exchange_rate)()
        
        # 计算人民币金额
        order_value_cny = order_value_usd * exchange_rate
        
        # 计算汇率转换费用
        conversion_fee = order_value_usd * self.conversion_fee_rate
        
        # 修改订单数据
        modified_data = {
            'exchange_rate': exchange_rate,
            'value_cny': order_value_cny,
            'conversion_fee': conversion_fee
        }
        
        return RuleResult(
            passed=True,
            message=f"汇率转换: ${order_value_usd:.2f} USD = {order_value_cny:.2f} CNY (汇率: {exchange_rate:.4f})",
            modified_data=modified_data
        )


class UsHolidayRule(MarketRule):
    """美股休市规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("us_holiday", "us_stock", RuleType.MARKET, config=config)
        self.description = "美股休市日规则"
        
        # 休市配置
        self.check_holidays = self.get_config('check_holidays', True)
        self.us_holidays = self.get_config('us_holidays', [])  # 美股休市日
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查美股休市规则"""
        if not self.check_holidays:
            return RuleResult(passed=True, message="休市检查已禁用")
        
        current_date = context.current_date
        
        # 检查是否为美股休市日
        if self._is_us_holiday(current_date):
            return RuleResult(
                passed=False,
                message=f"{current_date} 是美股休市日"
            )
        
        # 检查周末
        if current_date.weekday() >= 5:  # 周六、周日
            return RuleResult(
                passed=False,
                message=f"{current_date} 是周末，美股休市"
            )
        
        return RuleResult(passed=True, message="美股休市检查通过")
    
    def _is_us_holiday(self, date) -> bool:
        """检查是否为美股休市日"""
        date_str = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)
        return date_str in self.us_holidays


class UsMarketHoursRule(MarketRule):
    """美股交易时间规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("us_market_hours", "us_stock", RuleType.MARKET, config=config)
        self.description = "美股交易时间规则"
        
        # 美股交易时间（美东时间）
        self.market_start = time(9, 30)
        self.market_end = time(16, 0)
        
        # 从配置中读取
        if config:
            if 'market_start' in config:
                self.market_start = time(*config['market_start'])
            if 'market_end' in config:
                self.market_end = time(*config['market_end'])
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查美股交易时间"""
        current_time = context.current_dt.time()
        
        # 检查是否在美股正常交易时间内
        if self.market_start <= current_time <= self.market_end:
            return RuleResult(passed=True, message="在美股交易时间内")
        else:
            return RuleResult(passed=False, message="不在美股正常交易时间内")


class UsCommissionRule(MarketRule):
    """美股手续费规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("us_commission", "us_stock", RuleType.TRADING, config=config)
        self.description = "美股手续费规则"
        
        # 美股手续费配置
        self.commission_per_share = self.get_config('commission_per_share', 0.005)  # 每股手续费
        self.min_commission = self.get_config('min_commission', 1.0)  # 最小手续费
        self.max_commission = self.get_config('max_commission', 10.0)  # 最大手续费
        self.sec_fee_rate = self.get_config('sec_fee_rate', 0.000022)  # SEC费率
        self.taf_fee_rate = self.get_config('taf_fee_rate', 0.000119)  # TAF费率
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """计算美股手续费"""
        order_amount = data.get('amount', 0)
        order_value = data.get('value', 0)
        order_side = data.get('side', 'buy')
        
        # 计算佣金
        commission = max(
            min(order_amount * self.commission_per_share, self.max_commission),
            self.min_commission
        )
        
        # 计算SEC费用（卖出时收取）
        sec_fee = 0
        if order_side == 'sell':
            sec_fee = order_value * self.sec_fee_rate
        
        # 计算TAF费用（买入时收取）
        taf_fee = 0
        if order_side == 'buy':
            taf_fee = order_amount * self.taf_fee_rate
        
        total_fees = commission + sec_fee + taf_fee
        
        # 修改订单数据
        modified_data = {
            'commission': commission,
            'sec_fee': sec_fee,
            'taf_fee': taf_fee,
            'total_fees': total_fees
        }
        
        return RuleResult(
            passed=True,
            message=f"美股费用: 佣金${commission:.2f}, SEC${sec_fee:.4f}, TAF${taf_fee:.4f}, 总计${total_fees:.2f}",
            modified_data=modified_data
        )


class UsOrderTypeRule(MarketRule):
    """美股订单类型规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("us_order_type", "us_stock", RuleType.TRADING, config=config)
        self.description = "美股订单类型规则"
        
        # 订单类型配置
        self.allowed_order_types = self.get_config('allowed_order_types', 
            ['market', 'limit', 'stop', 'stop_limit'])
        self.min_stop_distance = self.get_config('min_stop_distance', 0.01)  # 最小止损距离
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查美股订单类型规则"""
        order_type = data.get('order_type', 'limit')
        order_price = data.get('price', 0)
        stop_price = data.get('stop_price', 0)
        symbol = data.get('symbol', '')
        
        # 检查订单类型是否允许
        if order_type not in self.allowed_order_types:
            return RuleResult(
                passed=False,
                message=f"不支持的订单类型: {order_type}"
            )
        
        # 检查止损单距离
        if order_type in ['stop', 'stop_limit'] and stop_price > 0:
            current_price = getattr(context.data_center, 'get_current_price', lambda x: 0)(symbol)
            if current_price > 0:
                distance = abs(stop_price - current_price) / current_price
                if distance < self.min_stop_distance:
                    return RuleResult(
                        passed=False,
                        message=f"止损距离 {distance:.2%} 小于最小距离 {self.min_stop_distance:.2%}"
                    )
        
        return RuleResult(passed=True, message="订单类型检查通过")


class UsPennyStockRule(MarketRule):
    """美股仙股规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("us_penny_stock", "us_stock", RuleType.RISK, config=config)
        self.description = "美股仙股交易规则"
        
        # 仙股配置
        self.penny_stock_threshold = self.get_config('penny_stock_threshold', 5.0)  # 仙股价格阈值
        self.allow_penny_stock = self.get_config('allow_penny_stock', False)  # 是否允许仙股交易
        self.max_penny_position_ratio = self.get_config('max_penny_position_ratio', 0.05)  # 仙股最大持仓比例
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查仙股规则"""
        symbol = data.get('symbol', '')
        order_side = data.get('side', 'buy')
        
        # 获取当前价格
        current_price = getattr(context.data_center, 'get_current_price', lambda x: 0)(symbol)
        
        # 检查是否为仙股
        if current_price <= self.penny_stock_threshold:
            if not self.allow_penny_stock:
                return RuleResult(
                    passed=False,
                    message=f"股票 {symbol} 价格 ${current_price:.2f} 为仙股，交易已禁用"
                )
            
            # 检查仙股持仓比例
            if order_side == 'buy':
                order_value = data.get('value', 0)
                total_value = context.portfolio.total_value
                
                if total_value > 0:
                    position_ratio = order_value / total_value
                    if position_ratio > self.max_penny_position_ratio:
                        return RuleResult(
                            passed=False,
                            message=f"仙股持仓比例 {position_ratio:.2%} 超过最大限制 {self.max_penny_position_ratio:.2%}"
                        )
        
        return RuleResult(passed=True, message="仙股规则检查通过")


class UsShortSellingRule(MarketRule):
    """美股卖空规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("us_short_selling", "us_stock", RuleType.TRADING, config=config)
        self.description = "美股卖空规则"
        
        # 卖空配置
        self.enable_short_selling = self.get_config('enable_short_selling', False)
        self.uptick_rule = self.get_config('uptick_rule', True)  # 上涨价规则
        self.max_short_ratio = self.get_config('max_short_ratio', 0.10)  # 最大卖空比例
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查卖空规则"""
        order_side = data.get('side', 'buy')
        
        if order_side == 'short':
            if not self.enable_short_selling:
                return RuleResult(
                    passed=False,
                    message="卖空交易已禁用"
                )
            
            symbol = data.get('symbol', '')
            order_value = data.get('value', 0)
            
            # 检查上涨价规则
            if self.uptick_rule and not self._check_uptick_rule(context, symbol):
                return RuleResult(
                    passed=False,
                    message=f"违反上涨价规则，股票 {symbol} 不能卖空"
                )
            
            # 检查卖空比例
            total_value = context.portfolio.total_value
            if total_value > 0:
                short_ratio = order_value / total_value
                if short_ratio > self.max_short_ratio:
                    return RuleResult(
                        passed=False,
                        message=f"卖空比例 {short_ratio:.2%} 超过最大限制 {self.max_short_ratio:.2%}"
                    )
        
        return RuleResult(passed=True, message="卖空规则检查通过")
    
    def _check_uptick_rule(self, context: Any, symbol: str) -> bool:
        """检查上涨价规则"""
        # 简化实现，实际需要检查最近一次交易价格变化
        return True 