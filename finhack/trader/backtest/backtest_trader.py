"""
主回测引擎实现
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from pathlib import Path

from .core.context import Context
from .core.event_center import EventCenter
from .core.trade_center import TradeCenter
from .core.data_center import DataCenter
from .strategy.strategy_manager import StrategyManager
from .events.base_event import BaseEvent, EventType
from .events.market_events import MarketEvent
from .events.trade_events import TradeEvent
from .events.user_events import UserEvent
# from finhack.library.config import Config
# from finhack.library.file_util import check_file_exists  
# from finhack.library.db_adpter.kline import Kline
# from finhack.library.db_adpter.ref_tickers import RefTickers
# from finhack.library.db_adpter.factors import factorManager

class SimpleConfig:
    """简化的配置类"""
    def __init__(self):
        self.backtest = {}


class BacktestTrader:
    """主回测引擎，整合所有组件并实现完整的回测流程"""
    
    def __init__(self, config_path: str = ""):
        """
        初始化回测引擎
        
        Args:
            config_path: 配置文件路径
        """
        self.config_path = config_path
        self.config = None
        self.context = None
        self.logger = None
        
        # 四大核心组件
        self.event_center = EventCenter()
        self.trade_center = TradeCenter()
        self.data_center = DataCenter()
        self.strategy_manager = StrategyManager()
        
        # 运行状态
        self.is_running = False
        self.start_time = None
        self.end_time = None
        
        # 性能统计
        self.performance_metrics = {}
        
        # 初始化
        self._initialize()
    
    def _initialize(self):
        """初始化回测引擎"""
        
        # 加载配置
        self._load_config()
        
        # 初始化日志
        self._setup_logging()
        
        # 创建上下文
        self._create_context()
        
        # 初始化组件
        self._initialize_components()
        
        self.logger.info("回测引擎初始化完成")
    
    def _load_config(self):
        """加载配置"""
        # 简化配置加载，直接使用默认配置
        self.config = SimpleConfig()
        
        # 设置一些默认值
        if not hasattr(self.config, 'backtest'):
            self.config.backtest = {}
        
        defaults = {
            'market': 'cn_stock',
            'start_time': '2023-01-01 00:00:00',
            'end_time': '2024-12-31 23:59:59',
            'initial_cash': 1000000.0,
            'benchmark': '000001.SH',
            'strategy': 'DemoStrategy',
            'frequency': '1d'
        }
        
        for key, value in defaults.items():
            if key not in self.config.backtest:
                self.config.backtest[key] = value
    
    def _setup_logging(self):
        """设置日志"""
        # 创建日志记录器
        self.logger = logging.getLogger('BacktestTrader')
        self.logger.setLevel(logging.INFO)
        
        # 创建处理器
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        
        # 添加处理器
        if not self.logger.handlers:
            self.logger.addHandler(handler)
    
    def _create_context(self):
        """创建上下文"""
        
        # 构建配置字典
        config_dict = {
            'account': {
                'initial_cash': self.config.backtest.get('initial_cash', 1000000.0),
                'cash': self.config.backtest.get('initial_cash', 1000000.0),
                'open_tax': self.config.backtest.get('open_tax', 0.0),
                'close_tax': self.config.backtest.get('close_tax', 0.001),
                'open_commission': self.config.backtest.get('open_commission', 0.0003),
                'close_commission': self.config.backtest.get('close_commission', 0.0003),
                'min_commission': self.config.backtest.get('min_commission', 5.0)
            },
            'trade': {
                'market': self.config.backtest.get('market', 'cn_stock'),
                'start_time': self.config.backtest.get('start_time', '2023-01-01 00:00:00'),
                'end_time': self.config.backtest.get('end_time', '2024-12-31 23:59:59'),
                'benchmark': self.config.backtest.get('benchmark', '000001.SH'),
                'strategy': self.config.backtest.get('strategy', 'DemoStrategy'),
                'frequency': self.config.backtest.get('frequency', '1d'),
                'max_position_ratio': self.config.backtest.get('max_position_ratio', 0.1),
                'max_order_ratio': self.config.backtest.get('max_order_ratio', 0.1),
                'enable_t1_rule': self.config.backtest.get('enable_t1_rule', True),
                'enable_limit_rule': self.config.backtest.get('enable_limit_rule', True),
                'slippage': self.config.backtest.get('slippage', 0.005),
                'rule_list': self.config.backtest.get('rule_list', 'delist,stop,st,limit,slip,volume_ratio,cost,volume_num,t1')
            },
            'data': {
                'cache_enabled': self.config.backtest.get('cache_enabled', True),
                'preload_days': self.config.backtest.get('preload_days', 30),
                'cache_size': self.config.backtest.get('cache_size', 1000)
            },
            'params': self.config.backtest.get('params', {}),
            'strategy_params': self.config.backtest.get('strategy_params', {})
        }
        
        # 创建上下文
        self.context = Context(config_dict)
        self.context.logger = self.logger
        
        # 解析时间
        self.start_time = datetime.strptime(self.context.trade_config.start_time, '%Y-%m-%d %H:%M:%S')
        self.end_time = datetime.strptime(self.context.trade_config.end_time, '%Y-%m-%d %H:%M:%S')
        
        self.logger.info(f"回测期间: {self.start_time} 到 {self.end_time}")
    
    def _initialize_components(self):
        """初始化组件"""
        
        # 设置组件的上下文引用
        self.context.event_center = self.event_center
        self.context.trade_center = self.trade_center
        self.context.data_center = self.data_center
        self.context.strategy_manager = self.strategy_manager
        
        # 初始化各组件
        self.event_center.initialize(self.context)
        self.trade_center.initialize(self.context)
        self.data_center.initialize(self.context)
        self.strategy_manager.initialize(self.context)
        
        # 设置事件中心的市场类型
        self.event_center.market = self.context.trade_config.market
        
        self.logger.info("组件初始化完成")
    
    def run(self, start_date: str = "", end_date: str = "") -> Dict[str, Any]:
        """
        运行回测
        
        Args:
            start_date: 开始日期（可选，覆盖配置）
            end_date: 结束日期（可选，覆盖配置）
            
        Returns:
            Dict[str, Any]: 回测结果
        """
        
        # 检查是否有激活策略
        if not self.strategy_manager.get_active_strategy():
            self.logger.error("没有激活的策略")
            return {"error": "没有激活的策略"}
        
        # 更新时间范围
        if start_date:
            self.start_time = datetime.strptime(start_date, '%Y-%m-%d')
        if end_date:
            self.end_time = datetime.strptime(end_date, '%Y-%m-%d')
        
        self.logger.info(f"开始回测: {self.start_time} 到 {self.end_time}")
        
        # 设置运行状态
        self.is_running = True
        self.context.current_dt = self.start_time
        
        try:
            # 初始化策略
            self._initialize_strategy()
            
            # 生成回测期间的所有事件
            self._generate_events()
            
            # 运行事件循环
            self._run_event_loop()
            
            # 计算绩效
            self._calculate_performance()
            
            # 生成报告
            result = self._generate_report()
            
            self.logger.info("回测完成")
            
            return result
            
        except Exception as e:
            self.logger.error(f"回测运行失败: {str(e)}")
            return {"error": str(e)}
            
        finally:
            self.is_running = False
    
    def _initialize_strategy(self):
        """初始化策略"""
        
        # 确保策略已初始化
        active_strategy = self.strategy_manager.get_active_strategy()
        if active_strategy:
            # 设置策略参数
            if self.context.strategy_params:
                active_strategy.set_params(self.context.strategy_params)
            
            # 初始化策略
            active_strategy.initialize(self.context)
            
            # 设置上下文中的策略引用
            self.context.strategy = active_strategy
            
            self.logger.info(f"策略初始化完成: {active_strategy.name}")
    
    def _generate_events(self):
        """生成回测期间的所有事件"""
        
        self.logger.info("开始生成事件...")
        
        # 获取交易日历
        trading_days = self._get_trading_days()
        
        # 为每个交易日生成事件
        for trade_date in trading_days:
            daily_events = self.event_center.generate_daily_events(
                trade_date, 
                self.context.trade_config.frequency
            )
            
            # 添加事件到队列
            self.event_center.add_events_to_queue(daily_events)
        
        self.logger.info(f"事件生成完成，共生成 {len(self.event_center.event_queue)} 个事件")
    
    def _get_trading_days(self) -> List[datetime]:
        """获取交易日历"""
        
        trading_days = []
        
        # 使用简单的日期范围（排除周末）
        current_date = self.start_time
        while current_date <= self.end_time:
            # 排除周末
            if current_date.weekday() < 5:  # 周一到周五
                trading_days.append(current_date)
            current_date += timedelta(days=1)
        
        return trading_days
    
    def _run_event_loop(self):
        """运行事件循环"""
        
        self.logger.info("开始事件循环...")
        
        event_count = 0
        
        # 处理所有事件
        while self.event_center.has_events() and self.is_running:
            
            # 获取下一个事件
            event = self.event_center.get_next_event()
            
            if event is None:
                break
            
            # 更新当前时间
            self.context.current_dt = event.event_time
            self.context.current_date = event.event_time.date()
            
            # 处理事件
            try:
                # 让事件自己处理
                event.process(self.context)
                
                # 分发事件到策略
                self.strategy_manager.dispatch_event(event)
                
                event_count += 1
                
                # 定期更新绩效
                if event_count % 1000 == 0:
                    self._update_performance()
                    self.logger.info(f"已处理 {event_count} 个事件")
                
            except Exception as e:
                self.logger.error(f"处理事件失败: {event.event_type.value}, 错误: {str(e)}")
                continue
        
        self.logger.info(f"事件循环完成，共处理 {event_count} 个事件")
    
    def _update_performance(self):
        """更新绩效统计"""
        
        # 更新组合价值
        self.context.update_portfolio_value()
        
        # 计算收益率
        self.context.calculate_returns()
        
        # 记录每日收益
        if self.context.current_date and self.context.previous_date:
            if self.context.current_date != self.context.previous_date:
                # 计算日收益率
                if self.context.account.initial_cash > 0:
                    daily_return = (self.context.portfolio.total_value - self.context.account.initial_cash) / self.context.account.initial_cash
                    self.context.add_daily_return(daily_return)
        
        # 更新前一日日期
        self.context.previous_date = self.context.current_date
    
    def _calculate_performance(self):
        """计算绩效"""
        
        self.logger.info("开始计算绩效...")
        
        # 最终更新
        self._update_performance()
        
        # 计算基本统计
        total_return = self.context.portfolio.returns
        daily_returns = self.context.portfolio.daily_returns
        
        # 计算各种绩效指标
        self.performance_metrics = {
            'total_return': total_return,
            'annualized_return': self._calculate_annualized_return(daily_returns),
            'volatility': self._calculate_volatility(daily_returns),
            'sharpe_ratio': self._calculate_sharpe_ratio(daily_returns),
            'max_drawdown': self._calculate_max_drawdown(daily_returns),
            'win_rate': self._calculate_win_rate(),
            'total_trades': len(self.context.logs['trade_list']),
            'final_value': self.context.portfolio.total_value,
            'initial_cash': self.context.account.initial_cash
        }
        
        self.logger.info(f"绩效计算完成，总收益率: {total_return:.2%}")
    
    def _calculate_annualized_return(self, daily_returns: List[float]) -> float:
        """计算年化收益率"""
        if not daily_returns:
            return 0.0
        
        # 计算累计收益率
        cumulative_return = 1.0
        for ret in daily_returns:
            cumulative_return *= (1 + ret)
        
        # 年化
        trading_days = len(daily_returns)
        if trading_days > 0:
            years = trading_days / 252  # 假设252个交易日为一年
            return (cumulative_return ** (1/years)) - 1
        
        return 0.0
    
    def _calculate_volatility(self, daily_returns: List[float]) -> float:
        """计算波动率"""
        if len(daily_returns) < 2:
            return 0.0
        
        # 计算标准差
        mean_return = sum(daily_returns) / len(daily_returns)
        variance = sum((ret - mean_return) ** 2 for ret in daily_returns) / (len(daily_returns) - 1)
        
        # 年化波动率
        return (variance ** 0.5) * (252 ** 0.5)
    
    def _calculate_sharpe_ratio(self, daily_returns: List[float]) -> float:
        """计算夏普比率"""
        if len(daily_returns) < 2:
            return 0.0
        
        # 假设无风险利率为3%
        risk_free_rate = 0.03
        
        # 计算超额收益
        annualized_return = self._calculate_annualized_return(daily_returns)
        volatility = self._calculate_volatility(daily_returns)
        
        if volatility > 0:
            return (annualized_return - risk_free_rate) / volatility
        
        return 0.0
    
    def _calculate_max_drawdown(self, daily_returns: List[float]) -> float:
        """计算最大回撤"""
        if not daily_returns:
            return 0.0
        
        # 计算累计净值
        cumulative_values = [1.0]
        for ret in daily_returns:
            cumulative_values.append(cumulative_values[-1] * (1 + ret))
        
        # 计算最大回撤
        max_drawdown = 0.0
        peak = cumulative_values[0]
        
        for value in cumulative_values[1:]:
            if value > peak:
                peak = value
            else:
                drawdown = (peak - value) / peak
                max_drawdown = max(max_drawdown, drawdown)
        
        return max_drawdown
    
    def _calculate_win_rate(self) -> float:
        """计算胜率"""
        trades = self.context.logs['trade_list']
        
        if not trades:
            return 0.0
        
        win_count = 0
        total_count = len(trades)
        
        for trade in trades:
            if trade.get('profit', 0) > 0:
                win_count += 1
        
        return win_count / total_count if total_count > 0 else 0.0
    
    def _generate_report(self) -> Dict[str, Any]:
        """生成回测报告"""
        
        report = {
            'summary': {
                'strategy': self.context.trade_config.strategy,
                'start_date': self.start_time.strftime('%Y-%m-%d'),
                'end_date': self.end_time.strftime('%Y-%m-%d'),
                'initial_cash': self.context.account.initial_cash,
                'final_value': self.context.portfolio.total_value,
                'total_return': self.performance_metrics.get('total_return', 0.0),
                'annualized_return': self.performance_metrics.get('annualized_return', 0.0),
                'volatility': self.performance_metrics.get('volatility', 0.0),
                'sharpe_ratio': self.performance_metrics.get('sharpe_ratio', 0.0),
                'max_drawdown': self.performance_metrics.get('max_drawdown', 0.0),
                'win_rate': self.performance_metrics.get('win_rate', 0.0),
                'total_trades': self.performance_metrics.get('total_trades', 0)
            },
            'positions': dict(self.context.portfolio.positions),
            'trades': self.context.logs['trade_list'],
            'orders': self.context.logs['order_list'],
            'daily_returns': self.context.portfolio.daily_returns,
            'performance_metrics': self.performance_metrics
        }
        
        return report
    
    def load_strategy(self, strategy_name: str, strategy_path: str = "") -> bool:
        """加载策略"""
        if strategy_path:
            return self.strategy_manager.load_strategy_from_file(strategy_path, strategy_name)
        else:
            return self.strategy_manager.load_strategy_from_module(f"strategies.{strategy_name}", strategy_name)
    
    def set_active_strategy(self, strategy_name: str) -> bool:
        """设置激活策略"""
        return self.strategy_manager.set_active_strategy(strategy_name)
    
    def get_status(self) -> Dict[str, Any]:
        """获取回测状态"""
        return {
            'is_running': self.is_running,
            'current_time': self.context.current_dt.isoformat() if self.context and self.context.current_dt else None,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'active_strategy': self.strategy_manager.get_active_strategy().name if self.strategy_manager.get_active_strategy() else None,
            'total_value': self.context.portfolio.total_value if self.context else 0.0,
            'cash': self.context.portfolio.cash if self.context else 0.0,
            'positions_count': len(self.context.portfolio.positions) if self.context else 0,
            'events_pending': len(self.event_center.event_queue) if self.event_center else 0
        }
    
    def stop(self):
        """停止回测"""
        self.is_running = False
        self.logger.info("回测已停止")
    
    def __str__(self) -> str:
        return f"BacktestTrader(running={self.is_running})" 