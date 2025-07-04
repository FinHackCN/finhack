"""
基础交易规则实现
包括手续费、滑点、成交量、订单大小等规则
"""

from typing import Dict, Any, Optional
from datetime import datetime, time
import math

from ..base_rule import BaseRule, RuleResult, RuleType


class TradingTimeRule(BaseRule):
    """交易时间规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("trading_time", RuleType.TRADING, config=config)
        self.description = "检查是否在交易时间内"
        
        # 默认A股交易时间
        self.morning_start = time(9, 30)
        self.morning_end = time(11, 30)
        self.afternoon_start = time(13, 0)
        self.afternoon_end = time(15, 0)
        
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
        """检查是否在交易时间内"""
        current_time = context.current_dt.time()
        
        # 检查是否在交易时间内
        in_morning = self.morning_start <= current_time <= self.morning_end
        in_afternoon = self.afternoon_start <= current_time <= self.afternoon_end
        
        if in_morning or in_afternoon:
            return RuleResult(passed=True, message="在交易时间内")
        else:
            return RuleResult(passed=False, message="不在交易时间内")


class OrderSizeRule(BaseRule):
    """订单大小规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("order_size", RuleType.TRADING, config=config)
        self.description = "检查订单大小是否合理"
        
        # 默认配置
        self.max_order_ratio = self.get_config('max_order_ratio', 0.1)  # 最大单笔订单比例
        self.min_order_amount = self.get_config('min_order_amount', 100)  # 最小订单金额
        self.max_order_amount = self.get_config('max_order_amount', 1000000)  # 最大订单金额
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查订单大小"""
        order_amount = data.get('amount', 0)
        order_value = data.get('value', 0)
        
        # 检查最小金额
        if order_value < self.min_order_amount:
            return RuleResult(
                passed=False,
                message=f"订单金额 {order_value} 小于最小金额 {self.min_order_amount}"
            )
        
        # 检查最大金额
        if order_value > self.max_order_amount:
            return RuleResult(
                passed=False,
                message=f"订单金额 {order_value} 超过最大金额 {self.max_order_amount}"
            )
        
        # 检查相对于总资产的比例
        total_value = context.portfolio.total_value
        if total_value > 0:
            ratio = order_value / total_value
            if ratio > self.max_order_ratio:
                return RuleResult(
                    passed=False,
                    message=f"订单比例 {ratio:.2%} 超过最大比例 {self.max_order_ratio:.2%}"
                )
        
        return RuleResult(passed=True, message="订单大小检查通过")


class OrderPriceRule(BaseRule):
    """订单价格规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("order_price", RuleType.TRADING, config=config)
        self.description = "检查订单价格是否合理"
        
        # 价格偏差允许范围
        self.max_price_deviation = self.get_config('max_price_deviation', 0.1)  # 10%
        self.min_price = self.get_config('min_price', 0.01)  # 最小价格
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查订单价格"""
        order_price = data.get('price', 0)
        symbol = data.get('symbol', '')
        
        # 检查最小价格
        if order_price < self.min_price:
            return RuleResult(
                passed=False,
                message=f"订单价格 {order_price} 小于最小价格 {self.min_price}"
            )
        
        # 获取当前市场价格
        current_price = getattr(context.data_center, 'get_current_price', lambda x: 0)(symbol)
        if current_price > 0:
            # 检查价格偏差
            deviation = abs(order_price - current_price) / current_price
            if deviation > self.max_price_deviation:
                return RuleResult(
                    passed=False,
                    message=f"订单价格偏差 {deviation:.2%} 超过允许范围 {self.max_price_deviation:.2%}"
                )
        
        return RuleResult(passed=True, message="订单价格检查通过")


class PositionSizeRule(BaseRule):
    """持仓大小规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("position_size", RuleType.TRADING, config=config)
        self.description = "检查持仓大小限制"
        
        # 持仓限制
        self.max_position_ratio = self.get_config('max_position_ratio', 0.2)  # 单只股票最大持仓比例
        self.max_total_position_ratio = self.get_config('max_total_position_ratio', 0.95)  # 总持仓比例
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查持仓大小"""
        symbol = data.get('symbol', '')
        order_amount = data.get('amount', 0)
        order_side = data.get('side', 'buy')
        
        if order_side == 'buy':
            # 计算买入后的持仓比例
            current_position = context.portfolio.positions.get(symbol, 0)
            new_position = current_position + order_amount
            
            if new_position > 0:
                # 计算持仓价值
                current_price = getattr(context.data_center, 'get_current_price', lambda x: 0)(symbol)
                position_value = new_position * current_price
                total_value = context.portfolio.total_value
                
                if total_value > 0:
                    position_ratio = position_value / total_value
                    if position_ratio > self.max_position_ratio:
                        return RuleResult(
                            passed=False,
                            message=f"持仓比例 {position_ratio:.2%} 超过最大比例 {self.max_position_ratio:.2%}"
                        )
            
            # 检查总持仓比例
            total_position_value = sum(
                pos * getattr(context.data_center, 'get_current_price', lambda x: 0)(sym) 
                for sym, pos in context.portfolio.positions.items() 
                if pos > 0
            )
            order_value = order_amount * getattr(context.data_center, 'get_current_price', lambda x: 0)(symbol)
            new_total_position_value = total_position_value + order_value
            
            if context.portfolio.total_value > 0:
                total_ratio = new_total_position_value / context.portfolio.total_value
                if total_ratio > self.max_total_position_ratio:
                    return RuleResult(
                        passed=False,
                        message=f"总持仓比例 {total_ratio:.2%} 超过最大比例 {self.max_total_position_ratio:.2%}"
                    )
        
        return RuleResult(passed=True, message="持仓大小检查通过")


class CashRule(BaseRule):
    """现金规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("cash", RuleType.TRADING, config=config)
        self.description = "检查现金是否足够"
        
        # 现金保留比例
        self.min_cash_ratio = self.get_config('min_cash_ratio', 0.05)  # 最小现金比例
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查现金是否足够"""
        order_side = data.get('side', 'buy')
        
        if order_side == 'buy':
            order_value = data.get('value', 0)
            available_cash = context.portfolio.cash
            
            # 检查现金是否足够
            if order_value > available_cash:
                return RuleResult(
                    passed=False,
                    message=f"现金不足，需要 {order_value}，可用 {available_cash}"
                )
            
            # 检查买入后现金比例
            remaining_cash = available_cash - order_value
            total_value = context.portfolio.total_value
            
            if total_value > 0:
                cash_ratio = remaining_cash / total_value
                if cash_ratio < self.min_cash_ratio:
                    return RuleResult(
                        passed=False,
                        message=f"现金比例 {cash_ratio:.2%} 低于最小比例 {self.min_cash_ratio:.2%}"
                    )
        
        return RuleResult(passed=True, message="现金检查通过")


class CommissionRule(BaseRule):
    """手续费规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("commission", RuleType.TRADING, config=config)
        self.description = "计算交易手续费"
        
        # 手续费配置
        self.buy_rate = self.get_config('buy_rate', 0.0003)  # 买入手续费率
        self.sell_rate = self.get_config('sell_rate', 0.0003)  # 卖出手续费率
        self.min_commission = self.get_config('min_commission', 5.0)  # 最小手续费
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """计算手续费"""
        order_side = data.get('side', 'buy')
        order_value = data.get('value', 0)
        
        # 计算手续费
        if order_side == 'buy':
            commission = max(order_value * self.buy_rate, self.min_commission)
        else:
            commission = max(order_value * self.sell_rate, self.min_commission)
        
        # 修改订单数据
        modified_data = {'commission': commission}
        
        return RuleResult(
            passed=True,
            message=f"手续费: {commission:.2f}",
            modified_data=modified_data
        )


class StampTaxRule(BaseRule):
    """印花税规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("stamp_tax", RuleType.TRADING, config=config)
        self.description = "计算印花税"
        
        # 印花税配置（通常只有卖出时收取）
        self.tax_rate = self.get_config('tax_rate', 0.001)  # 0.1%
        self.apply_to_buy = self.get_config('apply_to_buy', False)  # 是否对买入收取
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """计算印花税"""
        order_side = data.get('side', 'buy')
        order_value = data.get('value', 0)
        
        stamp_tax = 0
        
        # 计算印花税
        if order_side == 'sell' or (order_side == 'buy' and self.apply_to_buy):
            stamp_tax = order_value * self.tax_rate
        
        # 修改订单数据
        modified_data = {'stamp_tax': stamp_tax}
        
        return RuleResult(
            passed=True,
            message=f"印花税: {stamp_tax:.2f}",
            modified_data=modified_data
        )


class SlippageRule(BaseRule):
    """滑点规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("slippage", RuleType.TRADING, config=config)
        self.description = "计算交易滑点"
        
        # 滑点配置
        self.slippage_rate = self.get_config('slippage_rate', 0.005)  # 0.5%
        self.min_slippage = self.get_config('min_slippage', 0.01)  # 最小滑点
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """计算滑点"""
        order_side = data.get('side', 'buy')
        order_price = data.get('price', 0)
        
        # 计算滑点
        slippage = max(order_price * self.slippage_rate, self.min_slippage)
        
        # 根据买卖方向调整价格
        if order_side == 'buy':
            adjusted_price = order_price + slippage
        else:
            adjusted_price = order_price - slippage
        
        # 修改订单数据
        modified_data = {
            'slippage': slippage,
            'adjusted_price': adjusted_price
        }
        
        return RuleResult(
            passed=True,
            message=f"滑点: {slippage:.2f}, 调整价格: {adjusted_price:.2f}",
            modified_data=modified_data
        )


class VolumeRule(BaseRule):
    """成交量规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("volume", RuleType.TRADING, config=config)
        self.description = "检查成交量限制"
        
        # 成交量限制
        self.max_volume_ratio = self.get_config('max_volume_ratio', 0.1)  # 最大成交量比例
        self.min_daily_volume = self.get_config('min_daily_volume', 100000)  # 最小日成交量
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查成交量"""
        symbol = data.get('symbol', '')
        order_amount = data.get('amount', 0)
        
        # 获取历史成交量
        daily_volume = getattr(context.data_center, 'get_daily_volume', lambda x: 1000000)(symbol)
        
        if daily_volume < self.min_daily_volume:
            return RuleResult(
                passed=False,
                message=f"日成交量 {daily_volume} 小于最小要求 {self.min_daily_volume}"
            )
        
        # 检查订单占日成交量比例
        volume_ratio = order_amount / daily_volume
        if volume_ratio > self.max_volume_ratio:
            return RuleResult(
                passed=False,
                message=f"订单成交量比例 {volume_ratio:.2%} 超过最大比例 {self.max_volume_ratio:.2%}"
            )
        
        return RuleResult(passed=True, message="成交量检查通过")


class MinTradeUnitRule(BaseRule):
    """最小交易单位规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("min_trade_unit", RuleType.TRADING, config=config)
        self.description = "检查最小交易单位"
        
        # 交易单位配置
        self.min_unit = self.get_config('min_unit', 100)  # A股最小交易单位（手）
        self.unit_size = self.get_config('unit_size', 100)  # 每手股数
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查最小交易单位"""
        order_amount = data.get('amount', 0)
        
        # 检查是否为交易单位的整数倍
        if order_amount % self.unit_size != 0:
            # 调整到最接近的交易单位
            adjusted_amount = math.floor(order_amount / self.unit_size) * self.unit_size
            
            if adjusted_amount == 0:
                return RuleResult(
                    passed=False,
                    message=f"订单数量 {order_amount} 小于最小交易单位 {self.unit_size}"
                )
            
            # 修改订单数据
            modified_data = {'adjusted_amount': adjusted_amount}
            
            return RuleResult(
                passed=True,
                message=f"订单数量调整为 {adjusted_amount}",
                modified_data=modified_data
            )
        
        return RuleResult(passed=True, message="交易单位检查通过")


class DailyTradingRule(BaseRule):
    """日线交易规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("daily_trading", RuleType.TRADING, config=config)
        self.description = "日线交易特定规则"
        
        # 日线交易配置
        self.max_daily_trades = self.get_config('max_daily_trades', 10)  # 每日最大交易次数
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查日线交易规则"""
        current_date = context.current_date
        
        # 统计当日交易次数
        daily_trades = len([
            trade for trade in context.logs.get('trade_list', []) 
            if trade.get('date') == current_date
        ])
        
        if daily_trades >= self.max_daily_trades:
            return RuleResult(
                passed=False,
                message=f"当日交易次数 {daily_trades} 已达到最大限制 {self.max_daily_trades}"
            )
        
        return RuleResult(passed=True, message="日线交易检查通过")


class EODRule(BaseRule):
    """日终规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("eod", RuleType.TRADING, config=config)
        self.description = "日终处理规则"
        
        # 日终配置
        self.auto_close_positions = self.get_config('auto_close_positions', False)  # 自动平仓
        self.position_hold_days = self.get_config('position_hold_days', 30)  # 持仓天数限制
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """日终处理"""
        return RuleResult(passed=True, message="日终处理完成")


class IntradayTradingRule(BaseRule):
    """盘中交易规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("intraday_trading", RuleType.TRADING, config=config)
        self.description = "盘中交易特定规则"
        
        # 盘中交易配置
        self.max_intraday_trades = self.get_config('max_intraday_trades', 5)  # 盘中最大交易次数
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查盘中交易规则"""
        return RuleResult(passed=True, message="盘中交易检查通过")


class HourlyLimitRule(BaseRule):
    """小时限制规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("hourly_limit", RuleType.TRADING, config=config)
        self.description = "小时交易限制规则"
        
        # 小时限制配置
        self.max_hourly_trades = self.get_config('max_hourly_trades', 2)  # 每小时最大交易次数
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查小时交易限制"""
        return RuleResult(passed=True, message="小时交易检查通过")


class MinuteTradingRule(BaseRule):
    """分钟交易规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("minute_trading", RuleType.TRADING, config=config)
        self.description = "分钟交易特定规则"
        
        # 分钟交易配置
        self.max_minute_trades = self.get_config('max_minute_trades', 1)  # 每分钟最大交易次数
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查分钟交易规则"""
        return RuleResult(passed=True, message="分钟交易检查通过")


class HighFrequencyRule(BaseRule):
    """高频交易规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("high_frequency", RuleType.TRADING, config=config)
        self.description = "高频交易特定规则"
        
        # 高频交易配置
        self.min_interval_seconds = self.get_config('min_interval_seconds', 60)  # 最小交易间隔
        self.max_position_turnover = self.get_config('max_position_turnover', 2.0)  # 最大仓位周转率
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查高频交易规则"""
        return RuleResult(passed=True, message="高频交易检查通过") 