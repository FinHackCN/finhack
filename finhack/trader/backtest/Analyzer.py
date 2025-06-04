import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging


class Analyzer:
    """回测分析器，计算各种统计指标"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def calculate_statistics(self, account: Any, trades: List[Any]) -> Dict[str, float]:
        """
        计算回测统计指标
        
        Args:
            account: 账户信息
            trades: 交易记录列表
            
        Returns:
            统计指标字典
        """
        stats = {}
        
        # 基础收益指标 - 确保数值类型
        initial_value = float(account.initial_cash) if account.initial_cash is not None else 0.0
        final_value = float(account.total_value) if account.total_value is not None else 0.0
        
        if initial_value > 0:
            total_return = (final_value - initial_value) / initial_value
        else:
            total_return = 0.0
        stats['total_return'] = total_return
        
        # 计算年化收益（假设252个交易日）
        if trades:
            start_date = trades[0].trade_time
            end_date = trades[-1].trade_time
            days = (end_date - start_date).days
            if days > 0:
                annual_return = (1 + total_return) ** (365 / days) - 1
                stats['annual_return'] = annual_return
            else:
                stats['annual_return'] = 0
        else:
            stats['annual_return'] = 0
            
        # 计算日收益率序列（用于夏普率和最大回撤）
        daily_returns = self._calculate_daily_returns(account, trades)
        
        # 夏普率
        if len(daily_returns) > 1:
            sharpe_ratio = self._calculate_sharpe_ratio(daily_returns)
            stats['sharpe_ratio'] = sharpe_ratio
        else:
            stats['sharpe_ratio'] = 0
            
        # 最大回撤
        max_drawdown = self._calculate_max_drawdown(daily_returns)
        stats['max_drawdown'] = max_drawdown
        
        # 交易统计
        if trades:
            trade_stats = self._calculate_trade_statistics(trades)
            stats.update(trade_stats)
        else:
            stats.update({
                'win_rate': 0,
                'avg_win': 0,
                'avg_loss': 0,
                'profit_factor': 0,
                'avg_trade_return': 0,
                'max_consecutive_wins': 0,
                'max_consecutive_losses': 0,
            })
            
        # 账户统计 - 确保数值类型
        stats['total_commission'] = float(account.total_commission) if account.total_commission is not None else 0.0
        stats['total_tax'] = float(account.total_tax) if account.total_tax is not None else 0.0
        stats['total_slippage'] = float(account.total_slippage) if account.total_slippage is not None else 0.0
        stats['final_cash'] = float(account.cash) if account.cash is not None else 0.0
        stats['final_market_value'] = float(account.market_value) if account.market_value is not None else 0.0
        
        return stats
        
    def _calculate_daily_returns(self, account: Any, trades: List[Any]) -> List[float]:
        """计算日收益率序列"""
        if not trades:
            return []
            
        # 按日期分组计算每日净值
        daily_values = {}
        current_value = float(account.initial_cash) if account.initial_cash is not None else 0.0
        
        for trade in trades:
            date = trade.trade_time.date()
            
            # 计算该笔交易的盈亏
            if trade.side.value == "sell":
                # 卖出产生盈亏
                pnl = (float(trade.price) * float(trade.quantity) - 
                       float(trade.commission) - float(trade.tax) - float(trade.slippage))
                current_value += pnl
            else:
                # 买入消耗资金
                cost = (float(trade.price) * float(trade.quantity) + 
                       float(trade.commission) + float(trade.tax) + float(trade.slippage))
                current_value -= cost
                
            daily_values[date] = current_value
            
        # 计算日收益率
        sorted_dates = sorted(daily_values.keys())
        daily_returns = []
        
        for i in range(1, len(sorted_dates)):
            prev_value = daily_values[sorted_dates[i-1]]
            curr_value = daily_values[sorted_dates[i]]
            
            if prev_value > 0:
                daily_return = (curr_value - prev_value) / prev_value
                daily_returns.append(daily_return)
                
        return daily_returns
        
    def _calculate_sharpe_ratio(self, daily_returns: List[float], 
                               risk_free_rate: float = 0.03) -> float:
        """
        计算夏普率
        
        Args:
            daily_returns: 日收益率序列
            risk_free_rate: 无风险利率（年化）
            
        Returns:
            夏普率
        """
        if not daily_returns:
            return 0
            
        daily_rf = risk_free_rate / 252  # 转换为日收益率
        excess_returns = [r - daily_rf for r in daily_returns]
        
        mean_excess = np.mean(excess_returns)
        std_excess = np.std(excess_returns)
        
        if std_excess == 0:
            return 0
            
        # 年化夏普率
        sharpe = mean_excess / std_excess * np.sqrt(252)
        return sharpe
        
    def _calculate_max_drawdown(self, daily_returns: List[float]) -> float:
        """计算最大回撤"""
        if not daily_returns:
            return 0
            
        # 计算累计收益
        cumulative = 1
        peak = 1
        max_drawdown = 0
        
        for ret in daily_returns:
            cumulative *= (1 + ret)
            if cumulative > peak:
                peak = cumulative
            drawdown = (peak - cumulative) / peak
            if drawdown > max_drawdown:
                max_drawdown = drawdown
                
        return max_drawdown
        
    def _calculate_trade_statistics(self, trades: List[Any]) -> Dict[str, float]:
        """计算交易统计指标"""
        stats = {}
        
        # 按买卖配对计算每笔交易的盈亏
        position_map = {}  # 用于跟踪持仓
        trade_returns = []
        
        for trade in trades:
            code = trade.code
            
            if trade.side.value == "buy":
                # 买入，记录成本
                if code not in position_map:
                    position_map[code] = []
                position_map[code].append({
                    'quantity': trade.quantity,
                    'cost': trade.price + (trade.commission + trade.tax) / trade.quantity
                })
            else:
                # 卖出，计算盈亏
                if code in position_map and position_map[code]:
                    sell_quantity = trade.quantity
                    sell_price = trade.price - (trade.commission + trade.tax) / trade.quantity
                    
                    while sell_quantity > 0 and position_map[code]:
                        position = position_map[code][0]
                        
                        if position['quantity'] <= sell_quantity:
                            # 全部卖出
                            trade_return = (sell_price - position['cost']) / position['cost']
                            trade_returns.append(trade_return * position['quantity'])
                            sell_quantity -= position['quantity']
                            position_map[code].pop(0)
                        else:
                            # 部分卖出
                            trade_return = (sell_price - position['cost']) / position['cost']
                            trade_returns.append(trade_return * sell_quantity)
                            position['quantity'] -= sell_quantity
                            sell_quantity = 0
                            
        # 计算胜率
        wins = [r for r in trade_returns if r > 0]
        losses = [r for r in trade_returns if r < 0]
        
        total_trades = len(trade_returns)
        if total_trades > 0:
            stats['win_rate'] = len(wins) / total_trades
            stats['avg_trade_return'] = np.mean(trade_returns)
        else:
            stats['win_rate'] = 0
            stats['avg_trade_return'] = 0
            
        # 平均盈亏
        if wins:
            stats['avg_win'] = np.mean(wins)
        else:
            stats['avg_win'] = 0
            
        if losses:
            stats['avg_loss'] = np.mean(losses)
        else:
            stats['avg_loss'] = 0
            
        # 盈亏比
        if stats['avg_loss'] != 0:
            stats['profit_factor'] = abs(stats['avg_win'] / stats['avg_loss'])
        else:
            stats['profit_factor'] = float('inf') if stats['avg_win'] > 0 else 0
            
        # 连续盈亏统计
        max_consecutive_wins = 0
        max_consecutive_losses = 0
        current_wins = 0
        current_losses = 0
        
        for ret in trade_returns:
            if ret > 0:
                current_wins += 1
                current_losses = 0
                max_consecutive_wins = max(max_consecutive_wins, current_wins)
            elif ret < 0:
                current_losses += 1
                current_wins = 0
                max_consecutive_losses = max(max_consecutive_losses, current_losses)
                
        stats['max_consecutive_wins'] = max_consecutive_wins
        stats['max_consecutive_losses'] = max_consecutive_losses
        
        return stats
        
    def generate_equity_curve(self, account: Any, trades: List[Any]) -> pd.DataFrame:
        """
        生成资金曲线
        
        Args:
            account: 账户对象
            trades: 成交记录列表
            
        Returns:
            资金曲线DataFrame
        """
        if not trades:
            return pd.DataFrame()
            
        # 初始化
        equity_data = []
        cash = account.initial_cash
        positions = {}
        
        # 按时间排序
        sorted_trades = sorted(trades, key=lambda x: x.trade_time)
        
        for trade in sorted_trades:
            code = trade.code
            
            if trade.side.value == "buy":
                # 买入
                total_cost = (trade.price * trade.quantity + 
                             trade.commission + trade.tax + trade.slippage)
                cash -= total_cost
                
                if code not in positions:
                    positions[code] = {
                        'quantity': 0,
                        'avg_cost': 0
                    }
                    
                # 更新持仓
                total_quantity = positions[code]['quantity'] + trade.quantity
                positions[code]['avg_cost'] = (
                    (positions[code]['avg_cost'] * positions[code]['quantity'] + 
                     trade.price * trade.quantity) / total_quantity
                )
                positions[code]['quantity'] = total_quantity
                
            else:
                # 卖出
                total_value = (trade.price * trade.quantity - 
                              trade.commission - trade.tax - trade.slippage)
                cash += total_value
                
                if code in positions:
                    positions[code]['quantity'] -= trade.quantity
                    if positions[code]['quantity'] <= 0:
                        del positions[code]
                        
            # 计算总市值
            market_value = sum(
                pos['quantity'] * pos['avg_cost'] 
                for pos in positions.values()
            )
            
            equity_data.append({
                'time': trade.trade_time,
                'cash': cash,
                'market_value': market_value,
                'total_value': cash + market_value,
                'trade_id': trade.trade_id
            })
            
        return pd.DataFrame(equity_data) 