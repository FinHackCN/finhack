"""
港股市场特定规则
包括T+0规则、港股通规则、碎股交易规则等
"""

from typing import Dict, Any, Optional
from datetime import datetime, time
import math
import pandas as pd

from ..base_rule import MarketRule, RuleResult, RuleType


class T0Rule(MarketRule):
    """T+0规则 - 港股特定"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("t0_rule", "hk_stock", RuleType.TRADING, config=config)
        self.description = "港股T+0交易规则"
        
        # T+0配置
        self.enable_t0 = self.get_config('enable_t0', True)  # 是否启用T+0规则
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查T+0规则"""
        if not self.enable_t0:
            return RuleResult(passed=True, message="T+0规则已禁用")
        
        # 港股支持T+0交易，即当日买入可当日卖出
        return RuleResult(passed=True, message="港股T+0规则检查通过")


class StockConnectRule(MarketRule):
    """沪深港通规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("stock_connect", "hk_stock", RuleType.TRADING, config=config)
        self.description = "沪深港通交易规则"
        
        # 港股通配置
        self.enable_stock_connect = self.get_config('enable_stock_connect', True)
        self.daily_quota = self.get_config('daily_quota', 52000000000)  # 港股通每日额度（港币）
        self.min_order_value = self.get_config('min_order_value', 10000)  # 最小委托金额（港币）
        self.max_single_order_value = self.get_config('max_single_order_value', 100000000)  # 单笔最大金额
        
        # 交易时间配置
        self.morning_start = time(9, 30)
        self.morning_end = time(12, 0)
        self.afternoon_start = time(13, 0)
        self.afternoon_end = time(16, 0)
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查港股通规则"""
        if not self.enable_stock_connect:
            return RuleResult(passed=True, message="港股通交易已禁用")
        
        symbol = data.get('symbol', '')
        order_value = data.get('value', 0)
        current_time = context.current_dt.time()
        
        # 检查是否为港股通标的
        if not self._is_stock_connect_eligible(symbol):
            return RuleResult(
                passed=False,
                message=f"股票 {symbol} 不在港股通标的范围内"
            )
        
        # 检查交易时间
        in_morning = self.morning_start <= current_time <= self.morning_end
        in_afternoon = self.afternoon_start <= current_time <= self.afternoon_end
        
        if not (in_morning or in_afternoon):
            return RuleResult(
                passed=False,
                message="不在港股通交易时间内"
            )
        
        # 检查最小委托金额
        if order_value < self.min_order_value:
            return RuleResult(
                passed=False,
                message=f"委托金额 {order_value} 小于最小金额 {self.min_order_value} 港币"
            )
        
        # 检查单笔最大金额
        if order_value > self.max_single_order_value:
            return RuleResult(
                passed=False,
                message=f"委托金额 {order_value} 超过单笔最大金额 {self.max_single_order_value} 港币"
            )
        
        # 检查每日额度（简化实现）
        daily_used = self._get_daily_used_quota(context)
        if daily_used + order_value > self.daily_quota:
            return RuleResult(
                passed=False,
                message=f"超过港股通每日额度，已用: {daily_used}，限额: {self.daily_quota}"
            )
        
        return RuleResult(passed=True, message="港股通规则检查通过")
    
    def _is_stock_connect_eligible(self, symbol: str) -> bool:
        """检查是否为港股通标的"""
        # 简化实现，实际需要查询港股通标的名单
        return symbol.endswith('.HK')
    
    def _get_daily_used_quota(self, context: Any) -> float:
        """获取当日已使用的港股通额度"""
        current_date = context.current_date
        daily_used = 0
        
        for trade in context.logs.get('trade_list', []):
            if (trade.get('date') == current_date and 
                trade.get('symbol', '').endswith('.HK') and
                trade.get('side') == 'buy'):
                daily_used += trade.get('value', 0)
        
        return daily_used


class OddLotRule(MarketRule):
    """碎股交易规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("odd_lot", "hk_stock", RuleType.TRADING, config=config)
        self.description = "港股碎股交易规则"
        
        # 碎股交易配置
        self.board_lot = self.get_config('board_lot', 100)  # 一手股数，默认100股
        self.allow_odd_lot_buy = self.get_config('allow_odd_lot_buy', False)  # 是否允许碎股买入
        self.odd_lot_discount = self.get_config('odd_lot_discount', 0.05)  # 碎股折价5%
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查碎股交易规则"""
        order_amount = data.get('amount', 0)
        order_side = data.get('side', 'buy')
        symbol = data.get('symbol', '')
        
        # 获取该股票的一手股数
        board_lot = self._get_board_lot(symbol)
        
        # 检查是否为碎股
        is_odd_lot = order_amount % board_lot != 0
        
        if is_odd_lot:
            if order_side == 'buy' and not self.allow_odd_lot_buy:
                return RuleResult(
                    passed=False,
                    message=f"不允许碎股买入，一手为 {board_lot} 股"
                )
            
            if order_side == 'sell':
                # 碎股卖出需要折价
                current_price = data.get('price', 0)
                discounted_price = current_price * (1 - self.odd_lot_discount)
                
                modified_data = {
                    'odd_lot_discount_price': discounted_price,
                    'is_odd_lot': True
                }
                
                return RuleResult(
                    passed=True,
                    message=f"碎股卖出，折价 {self.odd_lot_discount:.1%}",
                    modified_data=modified_data
                )
        
        return RuleResult(passed=True, message="碎股规则检查通过")
    
    def _get_board_lot(self, symbol: str) -> int:
        """获取股票的一手股数"""
        # 简化实现，实际需要查询股票信息
        return self.board_lot


class CurrencyConversionRule(MarketRule):
    """汇率转换规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("currency_conversion", "hk_stock", RuleType.TRADING, config=config)
        self.description = "港币汇率转换规则"
        
        # 汇率配置
        self.base_currency = self.get_config('base_currency', 'CNY')  # 基础货币
        self.target_currency = self.get_config('target_currency', 'HKD')  # 目标货币
        self.exchange_rate = self.get_config('exchange_rate', 0.85)  # 汇率CNY/HKD
        self.auto_convert = self.get_config('auto_convert', True)  # 自动汇率转换
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查汇率转换"""
        if not self.auto_convert:
            return RuleResult(passed=True, message="汇率转换已禁用")
        
        order_value = data.get('value', 0)
        symbol = data.get('symbol', '')
        
        # 检查是否为港股
        if symbol.endswith('.HK'):
            # 获取实时汇率
            current_rate = self._get_current_exchange_rate(context)
            
            # 转换货币
            if self.base_currency == 'CNY' and self.target_currency == 'HKD':
                converted_value = order_value * current_rate
            elif self.base_currency == 'HKD' and self.target_currency == 'CNY':
                converted_value = order_value / current_rate
            else:
                converted_value = order_value
            
            modified_data = {
                'original_value': order_value,
                'converted_value': converted_value,
                'exchange_rate': current_rate,
                'base_currency': self.base_currency,
                'target_currency': self.target_currency
            }
            
            return RuleResult(
                passed=True,
                message=f"汇率转换: {order_value:.2f} {self.base_currency} -> {converted_value:.2f} {self.target_currency}",
                modified_data=modified_data
            )
        
        return RuleResult(passed=True, message="汇率转换检查通过")
    
    def _get_current_exchange_rate(self, context: Any) -> float:
        """获取实时汇率"""
        # 简化实现，实际需要调用汇率API
        return getattr(context, 'exchange_rate', self.exchange_rate)


class HkHolidayRule(MarketRule):
    """港股休市规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("hk_holiday", "hk_stock", RuleType.MARKET, config=config)
        self.description = "港股休市日规则"
        
        # 休市配置
        self.check_holidays = self.get_config('check_holidays', True)
        self.hk_holidays = self.get_config('hk_holidays', [])  # 港股特殊休市日
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查港股休市规则"""
        if not self.check_holidays:
            return RuleResult(passed=True, message="休市检查已禁用")
        
        current_date = context.current_date
        
        # 检查是否为港股特殊休市日
        if self._is_hk_holiday(current_date):
            return RuleResult(
                passed=False,
                message=f"{current_date} 是港股休市日"
            )
        
        # 检查台风黄色警告等特殊情况
        if self._has_typhoon_warning(context):
            return RuleResult(
                passed=False,
                message="台风黄色警告，港股休市"
            )
        
        return RuleResult(passed=True, message="港股休市检查通过")
    
    def _is_hk_holiday(self, date) -> bool:
        """检查是否为港股休市日"""
        date_str = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)
        return date_str in self.hk_holidays
    
    def _has_typhoon_warning(self, context: Any) -> bool:
        """检查是否有台风警告"""
        # 简化实现，实际需要查询天气API
        return getattr(context, 'typhoon_warning', False)


class HkMarketHoursRule(MarketRule):
    """港股交易时间规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("hk_market_hours", "hk_stock", RuleType.MARKET, config=config)
        self.description = "港股交易时间规则"
        
        # 港股交易时间（香港时间）
        self.morning_start = time(9, 30)
        self.morning_end = time(12, 0)
        self.afternoon_start = time(13, 0)
        self.afternoon_end = time(16, 0)
        
        # 从配置中读取
        if config:
            if 'morning_start' in config:
                self.morning_start = time(*config['morning_start'])
            if 'morning_end' in config:
                self.morning_end = time(*config['morning_end'])
            if 'afternoon_start' in config:
                self.afternoon_start = time(*config['afternoon_start'])
            if 'afternoon_end' in config:
                self.afternoon_end = time(*config['afternoon_end'])
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查港股交易时间"""
        current_time = context.current_dt.time()
        
        # 检查是否在港股交易时间内
        in_morning = self.morning_start <= current_time <= self.morning_end
        in_afternoon = self.afternoon_start <= current_time <= self.afternoon_end
        
        if in_morning or in_afternoon:
            return RuleResult(passed=True, message="在港股交易时间内")
        else:
            return RuleResult(passed=False, message="不在港股交易时间内")


class HkPriceLimitRule(MarketRule):
    """港股价格限制规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("hk_price_limit", "hk_stock", RuleType.MARKET, config=config)
        self.description = "港股价格限制规则"
        
        # 港股没有涨跌停限制，但有最小价位规定
        self.min_spread_config = {
            (0, 0.25): 0.001,      # 0-0.25港币，最小价位0.001港币
            (0.25, 0.5): 0.005,    # 0.25-0.5港币，最小价位0.005港币
            (0.5, 10): 0.01,       # 0.5-10港币，最小价位0.01港币
            (10, 20): 0.02,        # 10-20港币，最小价位0.02港币
            (20, 100): 0.05,       # 20-100港币，最小价位0.05港币
            (100, 200): 0.1,       # 100-200港币，最小价位0.1港币
            (200, 500): 0.2,       # 200-500港币，最小价位0.2港币
            (500, 1000): 0.5,      # 500-1000港币，最小价位0.5港币
            (1000, 2000): 1.0,     # 1000-2000港币，最小价位1.0港币
            (2000, 5000): 2.0,     # 2000-5000港币，最小价位2.0港币
            (5000, 9995): 5.0      # 5000-9995港币，最小价位5.0港币
        }
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查港股价格限制"""
        order_price = data.get('price', 0)
        
        # 获取该价位对应的最小价位
        min_spread = self._get_min_spread(order_price)
        
        # 检查价格是否为最小价位的整数倍
        if min_spread > 0:
            price_remainder = order_price % min_spread
            if abs(price_remainder) > 1e-6:  # 考虑浮点数精度
                adjusted_price = round(order_price / min_spread) * min_spread
                
                modified_data = {'adjusted_price': adjusted_price}
                
                return RuleResult(
                    passed=True,
                    message=f"价格调整为最小价位整数倍: {adjusted_price:.3f}",
                    modified_data=modified_data
                )
        
        return RuleResult(passed=True, message="港股价格检查通过")
    
    def _get_min_spread(self, price: float) -> float:
        """获取指定价格对应的最小价位"""
        for (min_price, max_price), spread in self.min_spread_config.items():
            if min_price <= price < max_price:
                return spread
        return 5.0  # 默认最小价位


class HkSettlementRule(MarketRule):
    """港股结算规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("hk_settlement", "hk_stock", RuleType.TRADING, config=config)
        self.description = "港股结算规则"
        
        # 结算配置
        self.settlement_period = self.get_config('settlement_period', 2)  # T+2结算
        self.enable_dvp = self.get_config('enable_dvp', True)  # 券款对付
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查港股结算规则"""
        # 港股实行T+2结算制度
        settlement_date = context.current_date + pd.Timedelta(days=self.settlement_period)
        
        modified_data = {
            'settlement_date': settlement_date,
            'settlement_period': self.settlement_period
        }
        
        return RuleResult(
            passed=True,
            message=f"港股T+{self.settlement_period}结算，结算日: {settlement_date}",
            modified_data=modified_data
        )


class HkCommissionRule(MarketRule):
    """港股手续费规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("hk_commission", "hk_stock", RuleType.TRADING, config=config)
        self.description = "港股手续费规则"
        
        # 港股手续费配置
        self.commission_rate = self.get_config('commission_rate', 0.0025)  # 佣金费率0.25%
        self.min_commission = self.get_config('min_commission', 50.0)  # 最小佣金50港币
        self.transaction_levy = self.get_config('transaction_levy', 0.0000565)  # 交易征费0.00565%
        self.trading_fee = self.get_config('trading_fee', 0.000055)  # 交易费0.0055%
        self.stamp_duty_rate = self.get_config('stamp_duty_rate', 0.001)  # 印花税0.1%
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """计算港股手续费"""
        order_value = data.get('value', 0)
        
        # 计算各项费用
        commission = max(order_value * self.commission_rate, self.min_commission)
        transaction_levy = order_value * self.transaction_levy
        trading_fee = order_value * self.trading_fee
        stamp_duty = math.ceil(order_value * self.stamp_duty_rate)  # 印花税向上取整
        
        total_fees = commission + transaction_levy + trading_fee + stamp_duty
        
        # 修改订单数据
        modified_data = {
            'commission': commission,
            'transaction_levy': transaction_levy,
            'trading_fee': trading_fee,
            'stamp_duty': stamp_duty,
            'total_fees': total_fees
        }
        
        return RuleResult(
            passed=True,
            message=f"港股费用: 佣金{commission:.2f}, 征费{transaction_levy:.2f}, 交易费{trading_fee:.2f}, 印花税{stamp_duty:.2f}, 总计{total_fees:.2f}港币",
            modified_data=modified_data
        ) 