"""
绩效分析器实现
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import math


class PerformanceAnalyzer:
    """绩效分析器，计算各种绩效指标"""
    
    def __init__(self):
        self.risk_free_rate = 0.03  # 无风险利率，默认3%
        self.trading_days_per_year = 252  # 年交易日数
        
    def set_risk_free_rate(self, rate: float):
        """设置无风险利率"""
        self.risk_free_rate = rate
    
    def calculate_returns(self, prices: List[float]) -> List[float]:
        """
        计算收益率序列
        
        Args:
            prices: 价格序列
            
        Returns:
            List[float]: 收益率序列
        """
        if len(prices) < 2:
            return []
        
        returns = []
        for i in range(1, len(prices)):
            if prices[i-1] > 0:
                ret = (prices[i] - prices[i-1]) / prices[i-1]
                returns.append(ret)
            else:
                returns.append(0.0)
        
        return returns
    
    def calculate_cumulative_returns(self, returns: List[float]) -> List[float]:
        """
        计算累计收益率序列
        
        Args:
            returns: 收益率序列
            
        Returns:
            List[float]: 累计收益率序列
        """
        if not returns:
            return []
        
        cumulative_returns = [0.0]  # 初始为0
        cumulative_value = 1.0
        
        for ret in returns:
            cumulative_value *= (1 + ret)
            cumulative_returns.append(cumulative_value - 1)
        
        return cumulative_returns
    
    def calculate_total_return(self, returns: List[float]) -> float:
        """
        计算总收益率
        
        Args:
            returns: 收益率序列
            
        Returns:
            float: 总收益率
        """
        if not returns:
            return 0.0
        
        total_return = 1.0
        for ret in returns:
            total_return *= (1 + ret)
        
        return total_return - 1
    
    def calculate_annualized_return(self, returns: List[float]) -> float:
        """
        计算年化收益率
        
        Args:
            returns: 收益率序列
            
        Returns:
            float: 年化收益率
        """
        if not returns:
            return 0.0
        
        total_return = self.calculate_total_return(returns)
        
        # 计算年化
        trading_days = len(returns)
        if trading_days > 0:
            years = trading_days / self.trading_days_per_year
            return (1 + total_return) ** (1/years) - 1
        
        return 0.0
    
    def calculate_volatility(self, returns: List[float]) -> float:
        """
        计算波动率（年化）
        
        Args:
            returns: 收益率序列
            
        Returns:
            float: 年化波动率
        """
        if len(returns) < 2:
            return 0.0
        
        # 计算标准差
        mean_return = sum(returns) / len(returns)
        variance = sum((ret - mean_return) ** 2 for ret in returns) / (len(returns) - 1)
        
        # 年化波动率
        return math.sqrt(variance * self.trading_days_per_year)
    
    def calculate_sharpe_ratio(self, returns: List[float]) -> float:
        """
        计算夏普比率
        
        Args:
            returns: 收益率序列
            
        Returns:
            float: 夏普比率
        """
        if len(returns) < 2:
            return 0.0
        
        annualized_return = self.calculate_annualized_return(returns)
        volatility = self.calculate_volatility(returns)
        
        if volatility > 0:
            return (annualized_return - self.risk_free_rate) / volatility
        
        return 0.0
    
    def calculate_sortino_ratio(self, returns: List[float]) -> float:
        """
        计算索提诺比率
        
        Args:
            returns: 收益率序列
            
        Returns:
            float: 索提诺比率
        """
        if len(returns) < 2:
            return 0.0
        
        annualized_return = self.calculate_annualized_return(returns)
        
        # 计算下行标准差
        downside_returns = [ret for ret in returns if ret < 0]
        if len(downside_returns) < 2:
            return float('inf') if annualized_return > self.risk_free_rate else 0.0
        
        mean_downside = sum(downside_returns) / len(downside_returns)
        downside_variance = sum((ret - mean_downside) ** 2 for ret in downside_returns) / (len(downside_returns) - 1)
        downside_volatility = math.sqrt(downside_variance * self.trading_days_per_year)
        
        if downside_volatility > 0:
            return (annualized_return - self.risk_free_rate) / downside_volatility
        
        return 0.0
    
    def calculate_max_drawdown(self, returns: List[float]) -> Tuple[float, int, int]:
        """
        计算最大回撤
        
        Args:
            returns: 收益率序列
            
        Returns:
            Tuple[float, int, int]: (最大回撤, 开始位置, 结束位置)
        """
        if not returns:
            return 0.0, 0, 0
        
        # 计算累计净值
        cumulative_values = [1.0]
        for ret in returns:
            cumulative_values.append(cumulative_values[-1] * (1 + ret))
        
        # 计算最大回撤
        max_drawdown = 0.0
        peak_index = 0
        trough_index = 0
        current_peak = cumulative_values[0]
        current_peak_index = 0
        
        for i in range(1, len(cumulative_values)):
            if cumulative_values[i] > current_peak:
                current_peak = cumulative_values[i]
                current_peak_index = i
            else:
                drawdown = (current_peak - cumulative_values[i]) / current_peak
                if drawdown > max_drawdown:
                    max_drawdown = drawdown
                    peak_index = current_peak_index
                    trough_index = i
        
        return max_drawdown, peak_index, trough_index
    
    def calculate_calmar_ratio(self, returns: List[float]) -> float:
        """
        计算卡玛比率
        
        Args:
            returns: 收益率序列
            
        Returns:
            float: 卡玛比率
        """
        if not returns:
            return 0.0
        
        annualized_return = self.calculate_annualized_return(returns)
        max_drawdown, _, _ = self.calculate_max_drawdown(returns)
        
        if max_drawdown > 0:
            return annualized_return / max_drawdown
        
        return 0.0
    
    def calculate_information_ratio(self, returns: List[float], benchmark_returns: List[float]) -> float:
        """
        计算信息比率
        
        Args:
            returns: 策略收益率序列
            benchmark_returns: 基准收益率序列
            
        Returns:
            float: 信息比率
        """
        if len(returns) != len(benchmark_returns) or len(returns) < 2:
            return 0.0
        
        # 计算超额收益
        excess_returns = [ret - bench_ret for ret, bench_ret in zip(returns, benchmark_returns)]
        
        # 计算超额收益的均值和标准差
        mean_excess = sum(excess_returns) / len(excess_returns)
        
        if len(excess_returns) < 2:
            return 0.0
        
        variance_excess = sum((ret - mean_excess) ** 2 for ret in excess_returns) / (len(excess_returns) - 1)
        std_excess = math.sqrt(variance_excess)
        
        if std_excess > 0:
            return (mean_excess * math.sqrt(self.trading_days_per_year)) / (std_excess * math.sqrt(self.trading_days_per_year))
        
        return 0.0
    
    def calculate_beta(self, returns: List[float], benchmark_returns: List[float]) -> float:
        """
        计算贝塔系数
        
        Args:
            returns: 策略收益率序列
            benchmark_returns: 基准收益率序列
            
        Returns:
            float: 贝塔系数
        """
        if len(returns) != len(benchmark_returns) or len(returns) < 2:
            return 0.0
        
        # 计算协方差和方差
        mean_return = sum(returns) / len(returns)
        mean_benchmark = sum(benchmark_returns) / len(benchmark_returns)
        
        covariance = sum((ret - mean_return) * (bench_ret - mean_benchmark) 
                        for ret, bench_ret in zip(returns, benchmark_returns)) / (len(returns) - 1)
        
        benchmark_variance = sum((bench_ret - mean_benchmark) ** 2 
                               for bench_ret in benchmark_returns) / (len(benchmark_returns) - 1)
        
        if benchmark_variance > 0:
            return covariance / benchmark_variance
        
        return 0.0
    
    def calculate_alpha(self, returns: List[float], benchmark_returns: List[float]) -> float:
        """
        计算阿尔法
        
        Args:
            returns: 策略收益率序列
            benchmark_returns: 基准收益率序列
            
        Returns:
            float: 阿尔法
        """
        if len(returns) != len(benchmark_returns) or len(returns) < 2:
            return 0.0
        
        portfolio_return = self.calculate_annualized_return(returns)
        benchmark_return = self.calculate_annualized_return(benchmark_returns)
        beta = self.calculate_beta(returns, benchmark_returns)
        
        # Alpha = 组合收益率 - [无风险利率 + Beta * (基准收益率 - 无风险利率)]
        alpha = portfolio_return - (self.risk_free_rate + beta * (benchmark_return - self.risk_free_rate))
        
        return alpha
    
    def calculate_var(self, returns: List[float], confidence_level: float = 0.05) -> float:
        """
        计算风险价值（VaR）
        
        Args:
            returns: 收益率序列
            confidence_level: 置信水平
            
        Returns:
            float: VaR值
        """
        if not returns:
            return 0.0
        
        # 排序收益率
        sorted_returns = sorted(returns)
        
        # 计算分位数
        index = int(len(sorted_returns) * confidence_level)
        if index < len(sorted_returns):
            return -sorted_returns[index]  # VaR通常表示为正数
        
        return 0.0
    
    def calculate_cvar(self, returns: List[float], confidence_level: float = 0.05) -> float:
        """
        计算条件风险价值（CVaR）
        
        Args:
            returns: 收益率序列
            confidence_level: 置信水平
            
        Returns:
            float: CVaR值
        """
        if not returns:
            return 0.0
        
        # 排序收益率
        sorted_returns = sorted(returns)
        
        # 计算分位数
        index = int(len(sorted_returns) * confidence_level)
        if index > 0:
            # CVaR是VaR以下收益率的平均值
            tail_returns = sorted_returns[:index]
            if tail_returns:
                return -sum(tail_returns) / len(tail_returns)
        
        return 0.0
    
    def calculate_win_rate(self, returns: List[float]) -> float:
        """
        计算胜率
        
        Args:
            returns: 收益率序列
            
        Returns:
            float: 胜率
        """
        if not returns:
            return 0.0
        
        win_count = sum(1 for ret in returns if ret > 0)
        return win_count / len(returns)
    
    def calculate_profit_loss_ratio(self, returns: List[float]) -> float:
        """
        计算盈亏比
        
        Args:
            returns: 收益率序列
            
        Returns:
            float: 盈亏比
        """
        if not returns:
            return 0.0
        
        positive_returns = [ret for ret in returns if ret > 0]
        negative_returns = [ret for ret in returns if ret < 0]
        
        if not positive_returns or not negative_returns:
            return 0.0
        
        avg_profit = sum(positive_returns) / len(positive_returns)
        avg_loss = sum(negative_returns) / len(negative_returns)
        
        if avg_loss != 0:
            return avg_profit / abs(avg_loss)
        
        return 0.0
    
    def calculate_kelly_criterion(self, returns: List[float]) -> float:
        """
        计算凯利公式
        
        Args:
            returns: 收益率序列
            
        Returns:
            float: 凯利比例
        """
        if not returns:
            return 0.0
        
        win_rate = self.calculate_win_rate(returns)
        profit_loss_ratio = self.calculate_profit_loss_ratio(returns)
        
        if profit_loss_ratio > 0:
            # Kelly = (胜率 * 盈亏比 - 败率) / 盈亏比
            kelly = (win_rate * profit_loss_ratio - (1 - win_rate)) / profit_loss_ratio
            return max(0, kelly)  # 凯利比例不能为负
        
        return 0.0
    
    def analyze_performance(self, returns: List[float], benchmark_returns: Optional[List[float]] = None,
                          prices: Optional[List[float]] = None) -> Dict[str, Any]:
        """
        综合绩效分析
        
        Args:
            returns: 策略收益率序列
            benchmark_returns: 基准收益率序列（可选）
            prices: 价格序列（可选）
            
        Returns:
            Dict[str, Any]: 绩效分析结果
        """
        if not returns:
            return {}
        
        # 基础指标
        analysis = {
            'total_return': self.calculate_total_return(returns),
            'annualized_return': self.calculate_annualized_return(returns),
            'volatility': self.calculate_volatility(returns),
            'sharpe_ratio': self.calculate_sharpe_ratio(returns),
            'sortino_ratio': self.calculate_sortino_ratio(returns),
            'calmar_ratio': self.calculate_calmar_ratio(returns),
            'win_rate': self.calculate_win_rate(returns),
            'profit_loss_ratio': self.calculate_profit_loss_ratio(returns),
            'kelly_criterion': self.calculate_kelly_criterion(returns),
            'var_95': self.calculate_var(returns, 0.05),
            'cvar_95': self.calculate_cvar(returns, 0.05),
            'var_99': self.calculate_var(returns, 0.01),
            'cvar_99': self.calculate_cvar(returns, 0.01)
        }
        
        # 最大回撤
        max_drawdown, peak_idx, trough_idx = self.calculate_max_drawdown(returns)
        analysis['max_drawdown'] = max_drawdown
        analysis['max_drawdown_period'] = {
            'peak_index': peak_idx,
            'trough_index': trough_idx,
            'duration': trough_idx - peak_idx
        }
        
        # 与基准的比较
        if benchmark_returns and len(benchmark_returns) == len(returns):
            analysis['beta'] = self.calculate_beta(returns, benchmark_returns)
            analysis['alpha'] = self.calculate_alpha(returns, benchmark_returns)
            analysis['information_ratio'] = self.calculate_information_ratio(returns, benchmark_returns)
            analysis['benchmark_total_return'] = self.calculate_total_return(benchmark_returns)
            analysis['benchmark_annualized_return'] = self.calculate_annualized_return(benchmark_returns)
            analysis['benchmark_volatility'] = self.calculate_volatility(benchmark_returns)
            analysis['excess_return'] = analysis['total_return'] - analysis['benchmark_total_return']
        
        # 统计信息
        analysis['statistics'] = {
            'total_periods': len(returns),
            'positive_periods': sum(1 for ret in returns if ret > 0),
            'negative_periods': sum(1 for ret in returns if ret < 0),
            'zero_periods': sum(1 for ret in returns if ret == 0),
            'best_period': max(returns) if returns else 0,
            'worst_period': min(returns) if returns else 0,
            'mean_return': sum(returns) / len(returns) if returns else 0,
            'median_return': sorted(returns)[len(returns)//2] if returns else 0
        }
        
        return analysis 