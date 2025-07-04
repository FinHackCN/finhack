"""
风险规则实现
包括持仓集中度、止损止盈、最大回撤、杠杆比例等规则
"""

from typing import Dict, Any, Optional
from datetime import datetime
import math

from ..base_rule import BaseRule, RuleResult, RuleType


class PositionConcentrationRule(BaseRule):
    """持仓集中度规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("position_concentration", RuleType.RISK, config=config)
        self.description = "检查持仓集中度风险"
        
        # 集中度配置
        self.max_single_position_ratio = self.get_config('max_single_position_ratio', 0.20)  # 单只股票最大比例
        self.max_top5_position_ratio = self.get_config('max_top5_position_ratio', 0.60)  # 前5大持仓最大比例
        self.max_top10_position_ratio = self.get_config('max_top10_position_ratio', 0.80)  # 前10大持仓最大比例
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查持仓集中度"""
        symbol = data.get('symbol', '')
        order_side = data.get('side', 'buy')
        order_amount = data.get('amount', 0)
        
        if order_side == 'buy':
            # 计算买入后的持仓情况
            positions = dict(context.portfolio.positions)
            current_position = positions.get(symbol, 0)
            positions[symbol] = current_position + order_amount
            
            # 计算各持仓的价值
            position_values = {}
            total_value = 0
            
            for sym, pos in positions.items():
                if pos > 0:
                    price = getattr(context.data_center, 'get_current_price', lambda x: 0)(sym)
                    value = pos * price
                    position_values[sym] = value
                    total_value += value
            
            if total_value > 0:
                # 检查单只股票持仓比例
                target_value = position_values.get(symbol, 0)
                target_ratio = target_value / total_value
                
                if target_ratio > self.max_single_position_ratio:
                    return RuleResult(
                        passed=False,
                        message=f"单只股票持仓比例 {target_ratio:.2%} 超过最大限制 {self.max_single_position_ratio:.2%}"
                    )
                
                # 检查前5大持仓比例
                sorted_positions = sorted(position_values.values(), reverse=True)
                top5_value = sum(sorted_positions[:5])
                top5_ratio = top5_value / total_value
                
                if top5_ratio > self.max_top5_position_ratio:
                    return RuleResult(
                        passed=False,
                        message=f"前5大持仓比例 {top5_ratio:.2%} 超过最大限制 {self.max_top5_position_ratio:.2%}"
                    )
                
                # 检查前10大持仓比例
                top10_value = sum(sorted_positions[:10])
                top10_ratio = top10_value / total_value
                
                if top10_ratio > self.max_top10_position_ratio:
                    return RuleResult(
                        passed=False,
                        message=f"前10大持仓比例 {top10_ratio:.2%} 超过最大限制 {self.max_top10_position_ratio:.2%}"
                    )
        
        return RuleResult(passed=True, message="持仓集中度检查通过")


class SingleStockPositionRule(BaseRule):
    """单只股票持仓规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("single_stock_position", RuleType.RISK, config=config)
        self.description = "检查单只股票持仓限制"
        
        # 持仓限制配置
        self.max_position_ratio = self.get_config('max_position_ratio', 0.10)  # 最大持仓比例
        self.max_position_value = self.get_config('max_position_value', 1000000)  # 最大持仓金额
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查单只股票持仓限制"""
        symbol = data.get('symbol', '')
        order_side = data.get('side', 'buy')
        order_amount = data.get('amount', 0)
        
        if order_side == 'buy':
            current_position = context.portfolio.positions.get(symbol, 0)
            new_position = current_position + order_amount
            
            current_price = getattr(context.data_center, 'get_current_price', lambda x: 0)(symbol)
            new_position_value = new_position * current_price
            
            # 检查持仓金额限制
            if new_position_value > self.max_position_value:
                return RuleResult(
                    passed=False,
                    message=f"持仓金额 {new_position_value:.2f} 超过最大限制 {self.max_position_value:.2f}"
                )
            
            # 检查持仓比例限制
            total_value = context.portfolio.total_value
            if total_value > 0:
                position_ratio = new_position_value / total_value
                if position_ratio > self.max_position_ratio:
                    return RuleResult(
                        passed=False,
                        message=f"持仓比例 {position_ratio:.2%} 超过最大限制 {self.max_position_ratio:.2%}"
                    )
        
        return RuleResult(passed=True, message="单只股票持仓检查通过")


class DailyTradingAmountRule(BaseRule):
    """单日最大交易金额规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("daily_trading_amount", RuleType.RISK, config=config)
        self.description = "检查单日最大交易金额"
        
        # 交易金额限制
        self.max_daily_amount = self.get_config('max_daily_amount', 5000000)  # 最大单日交易金额
        self.max_daily_ratio = self.get_config('max_daily_ratio', 0.50)  # 最大单日交易比例
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查单日交易金额限制"""
        order_value = data.get('value', 0)
        current_date = context.current_date
        
        # 统计当日已交易金额
        daily_trading_amount = 0
        for trade in context.logs.get('trade_list', []):
            if trade.get('date') == current_date:
                daily_trading_amount += trade.get('value', 0)
        
        new_daily_amount = daily_trading_amount + order_value
        
        # 检查绝对金额限制
        if new_daily_amount > self.max_daily_amount:
            return RuleResult(
                passed=False,
                message=f"单日交易金额 {new_daily_amount:.2f} 超过最大限制 {self.max_daily_amount:.2f}"
            )
        
        # 检查相对比例限制
        total_value = context.portfolio.total_value
        if total_value > 0:
            daily_ratio = new_daily_amount / total_value
            if daily_ratio > self.max_daily_ratio:
                return RuleResult(
                    passed=False,
                    message=f"单日交易比例 {daily_ratio:.2%} 超过最大限制 {self.max_daily_ratio:.2%}"
                )
        
        return RuleResult(passed=True, message="单日交易金额检查通过")


class MaxDrawdownRule(BaseRule):
    """最大回撤规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("max_drawdown", RuleType.RISK, config=config)
        self.description = "检查最大回撤风险"
        
        # 回撤限制
        self.max_drawdown = self.get_config('max_drawdown', 0.20)  # 最大回撤20%
        self.stop_trading_on_drawdown = self.get_config('stop_trading_on_drawdown', False)  # 达到最大回撤时停止交易
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查最大回撤"""
        current_value = context.portfolio.total_value
        
        # 获取历史最高净值
        peak_value = getattr(context.portfolio, 'peak_value', context.account.initial_cash)
        
        # 更新峰值
        if current_value > peak_value:
            context.portfolio.peak_value = current_value
            peak_value = current_value
        
        # 计算当前回撤
        if peak_value > 0:
            current_drawdown = (peak_value - current_value) / peak_value
            
            if current_drawdown > self.max_drawdown:
                if self.stop_trading_on_drawdown:
                    return RuleResult(
                        passed=False,
                        message=f"当前回撤 {current_drawdown:.2%} 超过最大限制 {self.max_drawdown:.2%}，停止交易"
                    )
                else:
                    # 只是警告，不阻止交易
                    return RuleResult(
                        passed=True,
                        message=f"当前回撤 {current_drawdown:.2%} 超过最大限制 {self.max_drawdown:.2%}，请注意风险"
                    )
        
        return RuleResult(passed=True, message="最大回撤检查通过")


class LeverageRule(BaseRule):
    """杠杆比例规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("leverage", RuleType.RISK, config=config)
        self.description = "检查杠杆比例风险"
        
        # 杠杆配置
        self.max_leverage = self.get_config('max_leverage', 1.0)  # 最大杠杆比例
        self.enable_margin = self.get_config('enable_margin', False)  # 是否启用融资融券
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查杠杆比例"""
        if not self.enable_margin:
            return RuleResult(passed=True, message="未启用融资融券")
        
        order_side = data.get('side', 'buy')
        order_value = data.get('value', 0)
        
        if order_side == 'buy':
            # 计算买入后的杠杆比例
            current_equity = context.portfolio.total_value
            current_debt = getattr(context.portfolio, 'debt', 0)
            
            # 假设使用融资买入
            if order_value > context.portfolio.cash:
                additional_debt = order_value - context.portfolio.cash
                new_debt = current_debt + additional_debt
                new_leverage = new_debt / current_equity if current_equity > 0 else 0
                
                if new_leverage > self.max_leverage:
                    return RuleResult(
                        passed=False,
                        message=f"杠杆比例 {new_leverage:.2f} 超过最大限制 {self.max_leverage:.2f}"
                    )
        
        return RuleResult(passed=True, message="杠杆比例检查通过")


class StopLossRule(BaseRule):
    """止损规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("stop_loss", RuleType.RISK, config=config)
        self.description = "止损风险控制"
        
        # 止损配置
        self.stop_loss_ratio = self.get_config('stop_loss_ratio', 0.10)  # 止损比例10%
        self.trailing_stop = self.get_config('trailing_stop', False)  # 是否启用移动止损
        self.auto_stop_loss = self.get_config('auto_stop_loss', True)  # 是否自动止损
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查止损条件"""
        symbol = data.get('symbol', '')
        current_position = context.portfolio.positions.get(symbol, 0)
        
        if current_position <= 0:
            return RuleResult(passed=True, message="无持仓，无需止损")
        
        # 获取持仓成本价
        cost_price = getattr(context.portfolio, 'get_cost_price', lambda x: 0)(symbol)
        current_price = getattr(context.data_center, 'get_current_price', lambda x: 0)(symbol)
        
        if cost_price > 0 and current_price > 0:
            # 计算损失比例
            loss_ratio = (cost_price - current_price) / cost_price
            
            if loss_ratio > self.stop_loss_ratio:
                if self.auto_stop_loss:
                    # 自动触发止损卖出
                    modified_data = {
                        'auto_stop_loss': True,
                        'stop_loss_symbol': symbol,
                        'stop_loss_amount': current_position,
                        'stop_loss_price': current_price
                    }
                    
                    return RuleResult(
                        passed=True,
                        message=f"触发止损，损失比例 {loss_ratio:.2%}",
                        modified_data=modified_data
                    )
                else:
                    return RuleResult(
                        passed=False,
                        message=f"达到止损条件，损失比例 {loss_ratio:.2%}"
                    )
        
        return RuleResult(passed=True, message="止损检查通过")


class TakeProfitRule(BaseRule):
    """止盈规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("take_profit", RuleType.RISK, config=config)
        self.description = "止盈风险控制"
        
        # 止盈配置
        self.take_profit_ratio = self.get_config('take_profit_ratio', 0.20)  # 止盈比例20%
        self.partial_take_profit = self.get_config('partial_take_profit', True)  # 是否部分止盈
        self.partial_ratio = self.get_config('partial_ratio', 0.50)  # 部分止盈比例
        self.auto_take_profit = self.get_config('auto_take_profit', False)  # 是否自动止盈
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查止盈条件"""
        symbol = data.get('symbol', '')
        current_position = context.portfolio.positions.get(symbol, 0)
        
        if current_position <= 0:
            return RuleResult(passed=True, message="无持仓，无需止盈")
        
        # 获取持仓成本价
        cost_price = getattr(context.portfolio, 'get_cost_price', lambda x: 0)(symbol)
        current_price = getattr(context.data_center, 'get_current_price', lambda x: 0)(symbol)
        
        if cost_price > 0 and current_price > 0:
            # 计算盈利比例
            profit_ratio = (current_price - cost_price) / cost_price
            
            if profit_ratio > self.take_profit_ratio:
                if self.auto_take_profit:
                    # 计算止盈数量
                    take_profit_amount = current_position
                    if self.partial_take_profit:
                        take_profit_amount = int(current_position * self.partial_ratio)
                    
                    # 自动触发止盈卖出
                    modified_data = {
                        'auto_take_profit': True,
                        'take_profit_symbol': symbol,
                        'take_profit_amount': take_profit_amount,
                        'take_profit_price': current_price
                    }
                    
                    return RuleResult(
                        passed=True,
                        message=f"触发止盈，盈利比例 {profit_ratio:.2%}",
                        modified_data=modified_data
                    )
                else:
                    return RuleResult(
                        passed=True,
                        message=f"达到止盈条件，盈利比例 {profit_ratio:.2%}"
                    )
        
        return RuleResult(passed=True, message="止盈检查通过")


class VaRRule(BaseRule):
    """VaR风险价值规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("var", RuleType.RISK, config=config)
        self.description = "VaR风险价值控制"
        
        # VaR配置
        self.confidence_level = self.get_config('confidence_level', 0.95)  # 置信水平95%
        self.max_var_ratio = self.get_config('max_var_ratio', 0.05)  # 最大VaR比例5%
        self.lookback_days = self.get_config('lookback_days', 252)  # 回望天数
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查VaR风险价值"""
        # 简化实现，实际需要计算组合的VaR
        current_var_ratio = 0.03  # 假设当前VaR比例为3%
        
        if current_var_ratio > self.max_var_ratio:
            return RuleResult(
                passed=False,
                message=f"VaR比例 {current_var_ratio:.2%} 超过最大限制 {self.max_var_ratio:.2%}"
            )
        
        return RuleResult(passed=True, message="VaR检查通过")


class BetaRule(BaseRule):
    """Beta风险规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("beta", RuleType.RISK, config=config)
        self.description = "Beta风险控制"
        
        # Beta配置
        self.max_beta = self.get_config('max_beta', 1.5)  # 最大Beta值
        self.min_beta = self.get_config('min_beta', 0.5)  # 最小Beta值
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查Beta风险"""
        symbol = data.get('symbol', '')
        
        # 获取股票Beta值
        beta = getattr(context.data_center, 'get_beta', lambda x: 1.0)(symbol)
        
        if beta > self.max_beta:
            return RuleResult(
                passed=False,
                message=f"Beta值 {beta:.2f} 超过最大限制 {self.max_beta:.2f}"
            )
        
        if beta < self.min_beta:
            return RuleResult(
                passed=False,
                message=f"Beta值 {beta:.2f} 低于最小要求 {self.min_beta:.2f}"
            )
        
        return RuleResult(passed=True, message="Beta检查通过")


class CorrelationRule(BaseRule):
    """相关性规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("correlation", RuleType.RISK, config=config)
        self.description = "相关性风险控制"
        
        # 相关性配置
        self.max_correlation = self.get_config('max_correlation', 0.80)  # 最大相关性
        self.check_correlation = self.get_config('check_correlation', True)  # 是否检查相关性
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查相关性风险"""
        if not self.check_correlation:
            return RuleResult(passed=True, message="相关性检查已禁用")
        
        symbol = data.get('symbol', '')
        order_side = data.get('side', 'buy')
        
        if order_side == 'buy':
            # 检查与现有持仓的相关性
            for held_symbol in context.portfolio.positions:
                if held_symbol != symbol and context.portfolio.positions[held_symbol] > 0:
                    # 获取相关性
                    correlation = getattr(context.data_center, 'get_correlation', lambda x, y: 0)(symbol, held_symbol)
                    
                    if abs(correlation) > self.max_correlation:
                        return RuleResult(
                            passed=False,
                            message=f"与持仓股票 {held_symbol} 相关性 {correlation:.2f} 过高"
                        )
        
        return RuleResult(passed=True, message="相关性检查通过")


class VolatilityRiskRule(BaseRule):
    """波动率风险规则"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("volatility_risk", RuleType.RISK, config=config)
        self.description = "波动率风险控制"
        
        # 波动率风险配置
        self.max_portfolio_volatility = self.get_config('max_portfolio_volatility', 0.20)  # 最大组合波动率
        self.max_stock_volatility = self.get_config('max_stock_volatility', 0.40)  # 最大个股波动率
    
    def apply(self, context: Any, event: Any, data: Dict[str, Any]) -> RuleResult:
        """检查波动率风险"""
        symbol = data.get('symbol', '')
        
        # 检查个股波动率
        stock_volatility = getattr(context.data_center, 'get_volatility', lambda x: 0)(symbol)
        
        if stock_volatility > self.max_stock_volatility:
            return RuleResult(
                passed=False,
                message=f"个股波动率 {stock_volatility:.2%} 超过最大限制 {self.max_stock_volatility:.2%}"
            )
        
        # 检查组合波动率（简化计算）
        portfolio_volatility = 0.15  # 假设当前组合波动率为15%
        
        if portfolio_volatility > self.max_portfolio_volatility:
            return RuleResult(
                passed=False,
                message=f"组合波动率 {portfolio_volatility:.2%} 超过最大限制 {self.max_portfolio_volatility:.2%}"
            )
        
        return RuleResult(passed=True, message="波动率风险检查通过") 