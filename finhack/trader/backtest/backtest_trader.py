import os
import sys
import importlib
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import pandas as pd
import json
from runtime.constant import *

from .EventCenter import EventCenter, EventType
from .TradeCenter import TradeCenter
from .DataCenter import DataCenter
from .Analyzer import Analyzer


class BacktestTrader:
    """回测交易器，整合所有组件运行回测"""
    
    def __init__(self,args):
        """
        初始化回测器
        
        Args:
            args: 回测参数，从配置文件中读取
        """
        self.logger = logging.getLogger(__name__)
        
        # 从args中获取配置信息
        self.start_date = getattr(args, 'start_date', '20200101')
        self.end_date = getattr(args, 'end_date', '20231231') 
        self.frequency = getattr(args, 'frequency', '1d')
        self.cache_enabled = getattr(args, 'cache_enabled', True)
        
        # 初始化组件
        self.data_center = DataCenter(cache_enabled=self.cache_enabled)
        self.trade_center = TradeCenter(self.data_center)
        self.event_center = EventCenter(
            start_date=self.start_date,
            end_date=self.end_date,
            frequency=self.frequency
        )
        self.analyzer = Analyzer()
        
        # 策略实例字典
        self.strategies: Dict[str, Any] = {}
        
        # 订单和成交记录缓存（用于检测变化）
        self.last_orders: Dict[str, List] = {}
        self.last_trades: Dict[str, List] = {}
        pass
        
    def add_strategy(self, strategy_name:str):
        """
        添加策略
        
        Args:
            strategy_config: 策略配置，包含：
                - adapter_id: 适配器ID
                - strategy_path: 策略文件路径
                - strategy_class: 策略类名
                - initial_cash: 初始资金
                - params: 策略参数
        """


        print(strategy_name)
        strategy_info=strategy_name.split(".")
        adapter_id = strategy_info[0]
        strategy_class = strategy_info[1]
        
        # 创建账户
        strategy_config={} #待补充
        initial_cash = strategy_config.get('initial_cash', getattr(self.args, 'cash', 1000000))
        self.trade_center.create_account(adapter_id, initial_cash)
        
        # 加载策略类
        strategy_path = STRATEGIES_DIR+strategy_name.replace(".","/")+".py"
        
        # 动态导入策略模块
        spec = importlib.util.spec_from_file_location("strategy", strategy_path)
        strategy_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(strategy_module)
        
        # 创建策略实例
        StrategyClass = getattr(strategy_module, strategy_class)
        strategy = StrategyClass(adapter_id)
        
        # 设置策略参数
        params = strategy_config.get('params', {})
        for key, value in params.items():
            setattr(strategy, key, value)
            
        # 初始化策略
        strategy.initialize(self.trade_center, self.data_center, self.event_center)
        
        # 注册适配器
        self.event_center.register_adapter(adapter_id)
        
        # 保存策略实例
        self.strategies[adapter_id] = strategy
        
        self.logger.info(f"Added strategy {adapter_id} from {strategy_path}")
        
    def run(self):
        """运行回测"""
        self.logger.info("Starting backtest...")
        
        # 获取交易日历
        trading_calendar = self.data_center.get_calendar()

        # 过滤到回测时间范围内的交易日
        start = datetime.strptime(self.start_date, "%Y%m%d")
        end = datetime.strptime(self.end_date, "%Y%m%d")
        trading_calendar = [
            date for date in trading_calendar
            if start <= datetime.strptime(date, "%Y%m%d") <= end
        ]
        
        # 添加策略
        strategy_list = self.args.strategies.split(",")
        for strategy in strategy_list:
            self.add_strategy(strategy)

        # 初始化事件中心
        self.event_center.initialize(trading_calendar)
        
        # 主事件循环
        event_count = 0
        while True:
            # 获取下一个事件
            event = self.event_center.get_next_event()
            
            if event is None:
                break
                
            # 更新数据中心的时间窗口
            self.data_center.update_time_window(event.time)
            
            # 分发事件
            self.event_center.dispatch_event(event, self.trade_center, self.strategies)
            
            print(self.trade_center)
            print(self.strategies)
            exit()


            # 检查订单和成交更新
            if event.event_type == EventType.BAR:
                self.event_center.check_order_trade_updates(
                    self.trade_center, self.strategies,
                    self.last_orders, self.last_trades
                )
                
            event_count += 1
            if event_count % 1000 == 0:
                self.logger.info(f"Processed {event_count} events, current time: {event.time}")
                
        self.logger.info(f"Backtest completed. Total events: {event_count}")
        
        # 生成回测报告
        self._generate_report()
        
    def _generate_report(self):
        """生成回测报告"""
        for adapter_id, strategy in self.strategies.items():
            account = self.trade_center.get_account(adapter_id)
            positions = self.trade_center.get_positions(adapter_id)
            trades = self.trade_center.get_trades(adapter_id)
            
            # 计算统计指标
            stats = self.analyzer.calculate_statistics(account, trades)
            
            # 生成报告
            report = {
                'adapter_id': adapter_id,
                'account': {
                    'initial_cash': account.initial_cash,
                    'final_value': account.total_value,
                    'cash': account.cash,
                    'market_value': account.market_value,
                    'unrealized_pnl': account.unrealized_pnl,
                    'realized_pnl': account.realized_pnl,
                    'total_commission': account.total_commission,
                    'total_tax': account.total_tax,
                    'total_slippage': account.total_slippage,
                },
                'statistics': stats,
                'positions': [
                    {
                        'code': code,
                        'quantity': pos.quantity,
                        'avg_cost': pos.avg_cost,
                        'market_value': pos.market_value,
                        'unrealized_pnl': pos.unrealized_pnl,
                        'realized_pnl': pos.realized_pnl,
                    }
                    for code, pos in positions.items()
                ],
                'trade_count': len(trades),
            }
            
            # 保存报告
            report_path = getattr(self.args, 'report_path', './backtest_report.json')
            report_dir = os.path.dirname(report_path)
            if report_dir:
                os.makedirs(report_dir, exist_ok=True)
                
            with open(report_path.replace('.json', f'_{adapter_id}.json'), 'w') as f:
                json.dump(report, f, indent=4, default=str)
                
            self.logger.info(f"Report saved for {adapter_id}")
            
            # 打印摘要
            print(f"\n{'='*60}")
            print(f"Backtest Summary for {adapter_id}")
            print(f"{'='*60}")
            print(f"Initial Cash: {account.initial_cash:,.2f}")
            print(f"Final Value: {account.total_value:,.2f}")
            print(f"Total Return: {stats.get('total_return', 0):.2%}")
            print(f"Annual Return: {stats.get('annual_return', 0):.2%}")
            print(f"Sharpe Ratio: {stats.get('sharpe_ratio', 0):.2f}")
            print(f"Max Drawdown: {stats.get('max_drawdown', 0):.2%}")
            print(f"Win Rate: {stats.get('win_rate', 0):.2%}")
            print(f"Total Trades: {len(trades)}")
            print(f"{'='*60}\n")


def main():
    """主函数，用于命令行运行"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run backtest')
    parser.add_argument('--config', type=str, required=True, help='Config file path')
    args = parser.parse_args()
    
    # 加载配置
    with open(args.config, 'r') as f:
        config = json.load(f)
        
    # 设置日志
    logging.basicConfig(
        level=getattr(logging, config.get('log_level', 'INFO')),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 创建回测器
    backtester = BacktestTrader(config)
    
    # 添加策略
    for strategy_config in config.get('strategies', []):
        backtester.add_strategy(strategy_config)
        
    # 运行回测
    backtester.run()


if __name__ == '__main__':
    main()