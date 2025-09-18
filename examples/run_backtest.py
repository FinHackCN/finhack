#!/usr/bin/env python
"""
回测运行示例
"""
import sys
import os
import json
from datetime import datetime

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from finhack.trader.backtest import BacktestTrader


def run_example_backtest():
    """运行示例回测"""
    
    # 回测配置
    config = {
        "start_date": "20240101",
        "end_date": "20240331",
        "frequency": "1d",
        "cache_enabled": True,
        "log_level": "INFO",
        "report_path": "./backtest_report.json",
        "strategies": [
            {
                "adapter_id": "strategy_01",
                "strategy_path": "../../demo_project/strategy/example_strategy.py",
                "strategy_class": "ExampleStrategy",
                "initial_cash": 1000000,
                "params": {
                    "symbols": ["000001.SZ", "000002.SZ"],
                    "ma_short": 5,
                    "ma_long": 20,
                    "position_pct": 0.3
                }
            }
        ]
    }
    
    # 创建回测器
    backtester = BacktestTrader(config)
    
    # 添加策略
    for strategy_config in config['strategies']:
        backtester.add_strategy(strategy_config)
        
    # 运行回测
    print(f"Starting backtest from {config['start_date']} to {config['end_date']}")
    backtester.run()
    
    print("Backtest completed!")


def run_custom_backtest(config_path: str):
    """运行自定义配置的回测"""
    
    # 加载配置
    with open(config_path, 'r') as f:
        config = json.load(f)
        
    # 创建回测器
    backtester = BacktestTrader(config)
    
    # 添加策略
    for strategy_config in config['strategies']:
        backtester.add_strategy(strategy_config)
        
    # 运行回测
    backtester.run()


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Run backtest')
    parser.add_argument('--config', type=str, help='Config file path')
    parser.add_argument('--example', action='store_true', help='Run example backtest')
    
    args = parser.parse_args()
    
    if args.example:
        run_example_backtest()
    elif args.config:
        run_custom_backtest(args.config)
    else:
        print("Please specify --config <path> or --example")
        parser.print_help() 