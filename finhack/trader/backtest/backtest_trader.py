import asyncio
import importlib.util
import os
import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pandas as pd

# 不使用logger变量,直接使用Log.logger

# 修复导入问题 - runtime模块应该从项目目录导入
import sys
# 添加项目目录到sys.path，这样可以导入runtime模块
if hasattr(sys, '_getframe'):  # 安全检查
    current_frame = sys._getframe()
    project_base = None
    
    # 尝试从环境变量获取项目路径
    if 'BASE_DIR' in os.environ:
        project_base = os.environ['BASE_DIR']
    else:
        # 如果没有环境变量，尝试自动检测
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # 向上查找直到找到runtime目录或到达根目录
        while current_dir != os.path.dirname(current_dir):
            runtime_path = os.path.join(current_dir, 'runtime')
            if os.path.exists(runtime_path) and os.path.isdir(runtime_path):
                project_base = current_dir
                break
            current_dir = os.path.dirname(current_dir)
    
    if project_base and project_base not in sys.path:
        sys.path.insert(0, project_base)

# 尝试导入runtime模块，如果失败则提供默认值
try:
    from runtime.constant import *
    import runtime.global_var as global_var
except ImportError:
    # 如果无法导入runtime，定义基本常量
    import os
    BASE_DIR = os.environ.get('BASE_DIR', os.getcwd())
    DATA_DIR = os.path.join(BASE_DIR, 'data')
    global_var = type('GlobalVar', (), {})()

import finhack.library.log as Log

# 导入我们的事件驱动架构组件
from .engine.backtest_engine import BacktestEngine
from .engine.context_manager import ContextManager
from .engine.scheduler import Scheduler
from .core.data_center import DataCenter
from .core.event_center import EventCenter
from .events.event_bus import EventBus
from .models.enums import *
from .models.account import Account
from .models.position import Position
from .models.order import Order
from .models.trade import Trade
from .models.instrument import Instrument


def _collect_trades(trader, context, engine):
    """收集交易记录。优先 trader._trades_list（order_buy_sync/order_sell_sync 里记录的），
    回退 all_trades / engine._captured_trades / trade_center.trades。"""
    # 优先：trader._trades_list（最可靠，在 order_buy/sell_sync 里记录）
    tl = getattr(trader, '_trades_list', None)
    if tl:
        return tl
    trades = context.get('logs', {}).get('all_trades', [])
    if trades:
        return trades
    # 回退：engine 实例属性 + trade_center
    ct = getattr(engine, '_captured_trades', None)
    if ct:
        return ct
    for src in [context.get('trade_center'), getattr(engine, 'trade_center', None), getattr(engine, 'trades', None)]:
        if src is None: continue
        if hasattr(src, 'trades') and isinstance(src.trades, dict) and src.trades:
            return [t.to_dict() if hasattr(t, 'to_dict') else dict(t) for t in src.trades.values()]
        if isinstance(src, list) and src:
            return [t.to_dict() if hasattr(t, 'to_dict') else dict(t) for t in src]
    return []
    trades = context.get('logs', {}).get('all_trades', [])
    if trades:
        return trades
    # 回退：trade_center / engine.trades
    for src in [context.get('trade_center'), getattr(engine, 'trade_center', None), getattr(engine, 'trades', None)]:
        if src is None: continue
        if hasattr(src, 'trades') and isinstance(src.trades, dict) and src.trades:
            return [t.to_dict() if hasattr(t, 'to_dict') else dict(t) for t in src.trades.values()]
        if isinstance(src, list) and src:
            return [t.to_dict() if hasattr(t, 'to_dict') else dict(t) for t in src]
    return []


class BacktestTrader:
    """回测交易器主入口类"""

    def __init__(self, args):
        self.args = args
        self.context = None
        self.strategy = None
        self.engine = None
        self.event_bus = EventBus()
        self.data_center = None
        self.event_center = None

        # 存储策略注册的定时任务
        self.scheduled_tasks = []
        
    def run(self):
        """回测主入口方法 - 改为同步版本"""
        try:
            Log.logger.info("开始初始化回测系统...")
            
            # 1. 初始化上下文
            self.init_context()
            
            # 2. 初始化组件
            self.init_components()
            
            # 3. 加载策略
            self.load_strategy()
            
            # 4. 初始化策略
            self.init_strategy()
            
            # 5. 运行回测 - 直接调用同步版本
            self.run_backtest_sync()
            
            Log.logger.info("回测完成")
            
        except Exception as e:
            Log.logger.error(f"回测运行失败: {e}")
            raise
            
    def init_context(self):
        """初始化回测上下文"""
        args_dict = self.args.__dict__

        # 生成context_id
        context_json = str(args_dict)
        context_id = hashlib.md5(context_json.encode()).hexdigest()

        # 获取命令行直接传入的参数（最高优先级）
        cmdline_args = getattr(self.args, '_cmdline_args', {})

        # 参数优先级: cmdline_args > args属性 > 配置文件
        market = getattr(self.args, 'market', 'cn_stock')
        freq = getattr(self.args, 'freq', '1d')
        start_date = getattr(self.args, 'start_time', '2024-01-01')
        end_date = getattr(self.args, 'end_time', '2024-12-31')
        initial_cash = float(getattr(self.args, 'cash', 1000000))
        benchmark = getattr(self.args, 'benchmark', '000001.SH')
        strategy_name = getattr(self.args, 'strategy', 'simple_1m_strategy')

        # 如果args中没有这些参数，尝试从配置文件中读取
        vendor = getattr(self.args, 'vendor', 'backtest')
        try:
            from finhack.library.config import Config

            # 读取 [trader-{vendor}] 配置
            vendor_section = f'trader-{vendor}'
            vendor_config = Config.get_config('args', vendor_section)

            # 读取 model.conf (trader.conf) 配置 - 这个优先级最高
            model_config = Config.get_config('trader', 'args')

            # 合并配置（model配置覆盖vendor配置）
            merged_config = {}
            merged_config.update(vendor_config)
            merged_config.update(model_config)

            # 应用配置（如果args中没有对应值）
            if 'market' in merged_config and 'market' not in cmdline_args:
                market = merged_config['market']
            if 'freq' in merged_config and 'freq' not in cmdline_args:
                freq = merged_config['freq']
            if 'start_time' in merged_config and 'start_time' not in cmdline_args:
                start_date = merged_config['start_time']
            if 'end_time' in merged_config and 'end_time' not in cmdline_args:
                end_date = merged_config['end_time']
            if 'cash' in merged_config and 'cash' not in cmdline_args:
                initial_cash = float(merged_config['cash'])
            if 'benchmark' in merged_config and 'benchmark' not in cmdline_args:
                benchmark = merged_config['benchmark']
            if 'strategy' in merged_config and 'strategy' not in cmdline_args:
                strategy_name = merged_config['strategy']

        except Exception as e:
            Log.logger.warning(f"[CONFIG] 无法读取配置文件: {e}")

        # 命令行参数最高优先级（直接覆盖）
        if 'market' in cmdline_args:
            market = cmdline_args['market']
        if 'freq' in cmdline_args:
            freq = cmdline_args['freq']
        if 'start_time' in cmdline_args:
            start_date = cmdline_args['start_time']
        if 'end_time' in cmdline_args:
            end_date = cmdline_args['end_time']
        if 'cash' in cmdline_args:
            initial_cash = float(cmdline_args['cash'])
        if 'benchmark' in cmdline_args:
            benchmark = cmdline_args['benchmark']
        if 'strategy' in cmdline_args:
            strategy_name = cmdline_args['strategy']

        Log.logger.info(f"[参数] market={market}, freq={freq}, cash={initial_cash}, strategy={strategy_name}")
        Log.logger.info(f"[参数] start={start_date}, end={end_date}, benchmark={benchmark}")
        if cmdline_args:
            Log.logger.info(f"[参数] cmdline_args={cmdline_args}")

        # 解析params参数
        params = {}
        if hasattr(self.args, 'params') and self.args.params:
            try:
                params = json.loads(self.args.params)
            except:
                pass
        
        # 创建一个简单的字典类，支持属性访问
        class DictObj(dict):
            def __getattr__(self, key):
                try:
                    return self[key]
                except KeyError:
                    raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{key}'")
            
            def __setattr__(self, key, value):
                self[key] = value
            
            def __delattr__(self, key):
                try:
                    del self[key]
                except KeyError:
                    raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{key}'")
        
        self.context = {
            'id': context_id,
            'current_dt': None,
            'previous_date': None,
            'params': params,
            'benchmark': benchmark,

            'settings': {
                'market': market,
                'freq': freq,
                'strategy_name': strategy_name,
                'start_date': start_date,
                'end_date': end_date,
                'benchmark': benchmark,
                'universe': getattr(self.args, 'universe', []),
                'initial_capital': initial_cash,
                'order_volume_ratio': getattr(self.args, 'order_volume_ratio', 1.0),
                'slip_type': getattr(self.args, 'sliptype', 'pricerelated'),
                'slip_value': float(getattr(self.args, 'slip', 0.001)),
                'open_tax': float(getattr(self.args, 'open_tax', 0.0)),
                'close_tax': float(getattr(self.args, 'close_tax', 0.001)),
                'open_commission': float(getattr(self.args, 'open_commission', 0.0003)),
                'close_commission': float(getattr(self.args, 'close_commission', 0.0003)),
                'close_today_commission': float(getattr(self.args, 'close_today_commission', 0.0)),
                'min_commission': float(getattr(self.args, 'min_commission', 5.0)),
                # 部分成交比例配置（从配置文件读取）
                'partial_fill_ratio': {
                    'large_order_threshold': float(merged_config.get('partial_fill_ratio_large_order_threshold', 10000)),
                    'large_order_min': float(merged_config.get('partial_fill_ratio_large_order_min', 0.3)),
                    'large_order_max': float(merged_config.get('partial_fill_ratio_large_order_max', 0.5)),
                    'medium_order_threshold': float(merged_config.get('partial_fill_ratio_medium_order_threshold', 5000)),
                    'medium_order_min': float(merged_config.get('partial_fill_ratio_medium_order_min', 0.5)),
                    'medium_order_max': float(merged_config.get('partial_fill_ratio_medium_order_max', 0.7)),
                    'small_order_min': float(merged_config.get('partial_fill_ratio_small_order_min', 0.7)),
                    'small_order_max': float(merged_config.get('partial_fill_ratio_small_order_max', 1.0)),
                },
                # 模拟盘配置（从配置文件读取）
                'simulation_mode': merged_config.get('simulation_mode', 'false').lower() == 'true',
                'context_persistence': merged_config.get('context_persistence', 'false').lower() == 'true',
                'base_dir': BASE_DIR,
            },
            
            'account': {
                'account_id': 'backtest_account',
                'platform': PlatformEnum.BACKTEST,
                'account_type': AccountTypeEnum.CASH,
                'currency': 'CNY',
                'total_assets': initial_cash,
                'cash_available': initial_cash,
                'cash_frozen': 0.0,
                'market_value': 0.0,
                'pnl_unrealized': 0.0,
                'pnl_realized': 0.0,
                'status': AccountStatusEnum.CONNECTED,
                'timestamp_updated': None,
            },
            
            'portfolio': {
                'positions': {},
                'orders': {},
            },
            
            'data': {
                'calendar': [],
                'schedule_event_list': [],
                'data_source': getattr(self.args, 'data_source', 'file'),
                'dividend_info': {},
                'client': None,
            },
            
            'g': DictObj(),  # 全局变量命名空间，支持字典和属性访问
            'scheduled_tasks': [],
            
            'logs': {
                'all_trades': [],
                'all_orders': [],
                'daily_history': [],
            },
            
            'performance': {
                'returns': [],
                'bench_returns': [],
                'turnover': [],
                'win_ratio': 0.0,
                'trade_num': 0,
                'indicators': {}
            }
        }
        
        Log.logger.info(f"回测上下文初始化完成，ID: {context_id}")
        Log.logger.info(f"回测设置: {market} {freq} {start_date} -> {end_date}")

        # ========== 按市场类型设置默认参数 ==========
        self._apply_market_defaults(market)

    def _apply_market_defaults(self, market: str):
        """根据市场类型设置默认参数（仅在用户未显式指定时覆盖）。
        市场配置统一从 library/market_context.MARKET_CONFIG 读取（单一来源）。"""
        from finhack.library import market_context
        settings = self.context['settings']
        account = self.context['account']

        cfg = market_context.get_market_config(market)

        # 货币和账户类型
        account['currency'] = cfg['currency']
        account['account_type'] = getattr(AccountTypeEnum, cfg['account_type'], AccountTypeEnum.CASH)

        # 费率（用户 args 优先，否则用市场默认）
        args_dict = self.args.__dict__
        fees = cfg.get('fees', {})
        for fee_key in ['open_tax', 'close_tax', 'open_commission', 'close_commission',
                        'min_commission', 'slip_value']:
            if fee_key not in args_dict or getattr(self.args, fee_key, None) is None:
                if fee_key in fees:
                    settings[fee_key] = fees[fee_key]

        # 资金费率（cryptoswap）
        funding = cfg.get('funding')
        if funding:
            for fk, ck in [('funding_rate_mode', 'mode'), ('funding_rate', 'rate'),
                           ('funding_interval_hours', 'interval_hours')]:
                if fk not in args_dict or getattr(self.args, fk, None) is None:
                    settings[fk] = funding.get(ck)

        Log.logger.info(f"[市场默认] {market}: currency={cfg['currency']}, "
                       f"close_tax={settings.get('close_tax')}, "
                       f"open_commission={settings.get('open_commission')}")
        
    def init_components(self):
        """初始化各个组件"""
        # 初始化数据中心
        self.data_center = DataCenter(
            project_path=BASE_DIR,
            market=self.context['settings']['market'],
            freq=self.context['settings']['freq']
        )
        self.data_center.set_context(self.context)
        
        # 初始化事件中心
        event_config = {
            'market': self.context['settings']['market'],
            'freq': self.context['settings']['freq'],
            'event_bus': self.event_bus
        }
        self.event_center = EventCenter(config=event_config)
        self.event_center.set_context(self.context)
        
        # 设置数据中心的引用
        self.event_center.set_data_center(self.data_center)
        
        # 初始化回测引擎
        self.engine = BacktestEngine(
            context=self.context,
            data_center=self.data_center,
            event_center=self.event_center,
            event_bus=self.event_bus
        )
        
        # 设置交易中心和事件中心的双向引用
        self.event_center.set_trade_center(self.engine.trade_center)
        self.engine.trade_center.set_event_center(self.event_center)

        # 将trade_center设置到context中，供策略使用
        self.context['trade_center'] = self.engine.trade_center

        Log.logger.info("回测组件初始化完成")
        
    def load_strategy(self):
        """加载策略文件"""
        strategy_name = self.context['settings']['strategy_name']
        market = self.context['settings']['market']

        # 构建策略文件路径
        strategy_path = f"{BASE_DIR}/strategies/{market}/{strategy_name}.py"

        if not os.path.exists(strategy_path):
            raise FileNotFoundError(f"策略文件不存在: {strategy_path}")

        # 读取策略代码
        with open(strategy_path, 'r', encoding='utf-8') as f:
            strategy_code = f.read()

        # 自动注入必要的imports到策略代码前面
        auto_imports = '''
# ==================== 自动注入的系统导入 ====================
import sys
import os
from datetime import datetime, timedelta

# 自动添加finhack路径
_project_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _project_path not in sys.path:
    sys.path.insert(0, _project_path)
_finhack_path = os.path.join(os.path.dirname(_project_path), 'finhack')
if _finhack_path not in sys.path:
    sys.path.insert(0, _finhack_path)

from finhack.trader.backtest.events.event_types import EventTypeEnum
from finhack.core.classes.dictobj import DictObj

# 创建全局变量对象g
g = DictObj()

# OrderCost类定义（简化版）
class OrderCost:
    def __init__(self, open_tax=0, close_tax=0, open_commission=0, close_commission=0, min_commission=0):
        self.open_tax = open_tax
        self.close_tax = close_tax
        self.open_commission = open_commission
        self.close_commission = close_commission
        self.close_today_commission = 0
        self.min_commission = min_commission

# FundingRateConfig类定义（资金费率配置）
class FundingRateConfig:
    def __init__(self, mode='fixed', rate=0.0001, interval_hours=8):
        self.mode = mode
        self.rate = rate
        self.interval_hours = interval_hours

# PriceRelatedSlippage类定义（简化版）
class PriceRelatedSlippage:
    def __init__(self, value):
        self.value = value

# ================================================================

'''

        # 将自动注入的代码添加到策略代码前面
        modified_code = auto_imports + strategy_code

        # 创建一个新的模块来执行修改后的代码
        module_spec = importlib.util.spec_from_file_location('strategy', strategy_path)
        strategy_module = importlib.util.module_from_spec(module_spec)

        # 执行修改后的代码而不是原始文件
        exec(modified_code, strategy_module.__dict__)
        
        # 实例化策略类 - 查找策略模块中的策略类并实例化
        strategy_instance = None
        Log.logger.debug(f"开始查找策略类，模块中的对象: {[n for n in dir(strategy_module) if not n.startswith('_')]}")
        print(f"[Trader] 模块中的类: {[n for n in dir(strategy_module) if not n.startswith('_')]}", flush=True)
        
        for name in dir(strategy_module):
            obj = getattr(strategy_module, name)
            # 查找继承自StrategyBase的类
            is_type = isinstance(obj, type)
            ends_with_strategy = name.endswith('Strategy')
            is_not_base = name != 'StrategyBase'
            
            print(f"[Trader] 检查 {name}: is_type={is_type}, ends_with_strategy={ends_with_strategy}, is_not_base={is_not_base}", flush=True)
            
            if is_type and ends_with_strategy and is_not_base:
                try:
                    # 尝试实例化策略类，传入配置
                    config = self.context.get('params', {})
                    Log.logger.debug(f"尝试实例化策略类: {name}, config={config}")
                    print(f"[Trader] 尝试实例化 {name}", flush=True)
                    strategy_instance = obj(config)
                    Log.logger.debug(f"成功实例化策略类: {name}")
                    print(f"[Trader] 成功实例化 {name}", flush=True)
                    break
                except Exception as e:
                    Log.logger.warning(f"实例化策略类 {name} 失败: {e}")
                    print(f"[Trader] 实例化 {name} 失败: {e}", flush=True)
                    import traceback
                    traceback.print_exc()
        
        if strategy_instance is None:
            # 如果没有找到策略类，回退到使用模块对象
            Log.logger.warning("未找到策略类，使用模块对象")
            strategy_instance = strategy_module
        
        self.strategy = strategy_instance
        self.context['trade'] = {'strategy_code': strategy_code}
        
        Log.logger.info(f"策略加载完成: {strategy_path}")
        
    def init_strategy(self):
        """初始化策略"""
        Log.logger.debug("开始初始化策略...")
        print(f"[Trader] 开始初始化策略, strategy类型: {type(self.strategy)}", flush=True)
        
        # 绑定API函数到策略模块
        self.bind_strategy_api()
        Log.logger.info("API绑定完成")
        
        # 调用策略的initialize函数
        if hasattr(self.strategy, 'initialize'):
            Log.logger.debug("调用strategy.initialize()...")
            print(f"[Trader] 调用strategy.initialize()", flush=True)
            try:
                self.strategy.initialize(self.context)
                Log.logger.info("策略初始化完成")
                print(f"[Trader] 策略初始化完成", flush=True)
            except Exception as e:
                Log.logger.error(f"策略初始化失败: {e}")
                print(f"[Trader] 策略初始化失败: {e}", flush=True)
                import traceback
                traceback.print_exc()
                raise
        else:
            Log.logger.warning("策略没有initialize函数")
            print(f"[Trader] 策略没有initialize函数", flush=True)
        
        # 将策略的universe同步到context中
        if hasattr(self.strategy, 'universe') and self.strategy.universe:
            self.context['settings']['universe'] = self.strategy.universe
            self.context['universe'] = self.strategy.universe  # 同时设置顶层快捷方式
            Log.logger.debug(f"从策略同步universe: {len(self.strategy.universe)}只股票/标的")
            print(f"[Trader] 同步universe: {len(self.strategy.universe)}只", flush=True)
            
    def bind_strategy_api(self):
        """绑定策略所需的API函数"""
        # ========== 核心组件绑定 ==========
        # 绑定 data_center（重要！很多策略需要直接访问）
        self.strategy.data_center = self.data_center
        
        # ========== 定时任务注册函数 ==========
        self.strategy.run_daily = self.run_daily
        self.strategy.run_weekly = self.run_weekly
        self.strategy.run_monthly = self.run_monthly
        self.strategy.run_interval = self.run_interval
        
        # ========== 数据获取方法 ==========
        # K线和行情数据
        self.strategy.get_quotes = self.data_center.get_quotes
        self.strategy.get_klines = self.data_center.get_klines
        self.strategy.get_factors = self.data_center.get_factors
        
        # 参考数据获取方法（新增）
        self.strategy.get_stock_list = self.get_stock_list_sync
        self.strategy.get_trading_calendar = self.data_center.get_trading_calendar
        
        # 为了兼容性，提供别名
        self.strategy.get_price = self.get_price_sync
        self.strategy.get_kline = self.data_center.get_klines
        
        # ========== 交易方法（同步版本） ==========
        self.strategy.place_order = self.place_order_sync
        self.strategy.cancel_order = self.cancel_order_sync
        
        # ========== 查询方法 ==========
        self.strategy.get_account = self.get_account_sync
        self.strategy.get_positions = self.get_positions_sync
        self.strategy.get_orders = self.get_orders_sync
        self.strategy.get_trades = self.get_trades_sync
        self.strategy.get_cash = self.get_cash_sync
        self.strategy.get_current_price = self.get_current_price_sync
        
        # ========== 便利方法 ==========
        self.strategy.order_buy = self.order_buy_sync
        self.strategy.order_sell = self.order_sell_sync
        self.strategy.order_short = self.order_short_sync
        self.strategy.close_short = self.close_short_sync
        self.strategy.close_long = self.close_long_sync
        self.strategy.sell_all_stocks = self.sell_all_stocks_sync

        # ========== 回测配置方法 ==========
        self.strategy.set_benchmark = self.set_benchmark
        self.strategy.set_option = self.set_option
        self.strategy.set_order_cost = self.set_order_cost
        self.strategy.set_slippage = self.set_slippage
        self.strategy.set_funding_rate = self.set_funding_rate

        Log.logger.info("策略API绑定完成")
    
    def get_price_sync(self, code, time=None):
        """获取单个股票价格的同步方法（get_quotes的简化版本）"""
        try:
            # 获取回测频率，使用实际频率而不是固定1d
            freq = self.context.get('settings', {}).get('freq', '1d')
            
            # 确保传入正确的当前回测时间
            current_time = self.context.get('current_dt', datetime.now())
            if time is None:
                time = current_time
            
                Log.logger.debug(f"获取价格数据: {code}, 频率: {freq}, 时间: {time}")
            
            # 直接使用DataCenter的get_quotes方法
            quotes = self.data_center.get_quotes([code], freq=freq, time=time, fields=['close'])
            if not quotes.empty:
                price = quotes.iloc[0]['close']
                Log.logger.debug(f"成功获取 {code} 价格: {price}")
                return price
            else:
                Log.logger.warning(f"无法获取股票 {code} 的价格数据 (时间: {time})")
                return None
                
        except Exception as e:
            Log.logger.error(f"获取股票价格失败: {code} - {str(e)}")
            return None
    
    def get_stock_list_sync(self, market=None, use_cache=True):
        """获取股票列表的同步方法
        
        Args:
            market: 市场名称，如果为None则使用当前回测市场
            use_cache: 是否使用缓存
            
        Returns:
            list: 股票代码列表
        """
        try:
            # 如果没有指定市场，使用当前回测市场
            if market is None:
                market = self.context.get('settings', {}).get('market', 'cn_stock')
            
            # 调用DataCenter的数据接口
            stock_list_df = self.data_center.data_interface.get_stock_list(market, use_cache=use_cache)
            
            if stock_list_df is not None and not stock_list_df.empty:
                # 返回股票代码列表
                stock_codes = stock_list_df['code'].tolist()
                Log.logger.debug(f"成功获取 {market} 股票列表: {len(stock_codes)} 只")
                return stock_codes
            else:
                Log.logger.warning(f"无法获取 {market} 的股票列表")
                return []
                
        except Exception as e:
            Log.logger.error(f"获取股票列表失败: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def place_order_sync(self, adapter_id, symbol, side, order_type, volume, price=None, **kwargs):
        """同步下单方法"""
        try:
            # 字符串到枚举的转换
            if isinstance(side, str):
                side = Side.BUY if side.upper() == 'BUY' else Side.SELL
            if isinstance(order_type, str):
                if order_type.upper() == 'MARKET':
                    order_type = OrderType.MARKET
                elif order_type.upper() == 'LIMIT':
                    order_type = OrderType.LIMIT
                else:
                    order_type = OrderType.LIMIT  # 默认为限价单

            # 订单数量校验
            if volume is None or volume <= 0:
                bt_time = self.context.get('current_dt')
                time_str = bt_time.strftime('%Y-%m-%d %H:%M:%S') if bt_time else '--'
                Log.logger.warning(f"[{time_str}] 订单数量无效({volume})，已拒绝: {symbol}")
                return None

            # T+1检查：仅在T+1市场中，卖出时验证可用持仓是否充足
            if side == Side.SELL and hasattr(self.engine, 'trade_center'):
                if self.engine.trade_center._is_t_plus_one():
                    position = self.engine.trade_center.positions.get(symbol)
                    available_volume = position.available_volume if position else 0
                    if available_volume < volume:
                        bt_time = self.context.get('current_dt')
                        time_str = bt_time.strftime('%Y-%m-%d %H:%M:%S') if bt_time else '--'
                        Log.logger.warning(
                            f"[{time_str}] T+1规则拦截: {symbol} 卖出{volume}股被拒绝，"
                            f"可用{available_volume}股(含T+1冻结)"
                        )
                        return None

            # 调用TradeCenter的下单方法
            if hasattr(self.engine, 'trade_center'):
                # 创建订单对象并添加到TradeCenter
                from datetime import datetime
                import uuid
                order_id = f"order_{symbol}_{int(datetime.now().timestamp() * 1000)}_{uuid.uuid4().hex[:6]}"

                # 对于市价单，获取并固定当前价格
                market_price = None
                if order_type == OrderType.MARKET:
                    current_price = self.engine.trade_center._get_price_from_datacenter(symbol)
                    if current_price and current_price > 0:
                        market_price = current_price
                        Log.logger.debug(f"市价单固定价格: {symbol} = {market_price:.2f}")

                # 创建订单
                order = Order(
                    account_id=self.context['account']['account_id'],
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    volume=volume,
                    order_id=order_id,
                    price=price,
                    status=OrderStatus.PENDING_NEW,
                    created_time=self.context.get('current_dt', datetime.now()),
                    market_price=market_price  # 市价单的固定成交价
                )

                # 订单验证（资金充足性、交割月、持仓充足性等）
                if hasattr(self.engine.trade_center, 'validate_order_sync'):
                    if not self.engine.trade_center.validate_order_sync(order):
                        bt_time = self.context.get('current_dt')
                        time_str = bt_time.strftime('%Y-%m-%d %H:%M:%S') if bt_time else '--'
                        Log.logger.warning(f"[{time_str}] 订单验证未通过，已拒绝: {symbol} {side.value} {volume}")
                        return None

                # 添加到订单列表
                self.engine.trade_center.orders[order_id] = order
                self.engine.trade_center.active_orders[order_id] = order  # 同时添加到活跃订单
                order.status = OrderStatus.NEW

                # 格式化价格显示
                price_str = "市价" if price is None else f"{price:.2f}"
                # 获取回测时间
                bt_time = self.context.get('current_dt')
                time_str = bt_time.strftime('%Y-%m-%d %H:%M:%S') if bt_time else '--'
                Log.logger.info(f"[{time_str}] 下单成功: {symbol} {side.value} {order_type.value} 数量:{volume} 价格:{price_str} | 活跃订单数: {len(self.engine.trade_center.active_orders)}")
                return order_id
            else:
                bt_time = self.context.get('current_dt')
                time_str = bt_time.strftime('%Y-%m-%d %H:%M:%S') if bt_time else '--'
                Log.logger.debug(f"[{time_str}] 模拟下单: {symbol} {side} {order_type} 数量:{volume} 价格:{price}")
                return f"order_{symbol}_{int(datetime.now().timestamp())}"
        except Exception as e:
            bt_time = self.context.get('current_dt')
            time_str = bt_time.strftime('%Y-%m-%d %H:%M:%S') if bt_time else '--'
            Log.logger.error(f"[{time_str}] 下单失败: {e}")
            return None
    
    def cancel_order_sync(self, adapter_id, order_id, **kwargs):
        """同步撤单方法"""
        try:
            if hasattr(self.engine, 'trade_center') and order_id in self.engine.trade_center.orders:
                order = self.engine.trade_center.orders[order_id]
                # 解冻订单占用的资金（期货/永续合约保证金冻结）
                frozen_amount = getattr(order, '_frozen_amount', 0)
                if frozen_amount > 0:
                    self.engine.trade_center.account.unfreeze_cash(frozen_amount)
                    order._frozen_amount = 0
                order.status = OrderStatus.CANCELLED
                order.rejected_reason = "用户撤单"
                # 从活跃订单中移除
                if order_id in self.engine.trade_center.active_orders:
                    del self.engine.trade_center.active_orders[order_id]
                bt_time = self.context.get('current_dt')
                time_str = bt_time.strftime('%Y-%m-%d %H:%M:%S') if bt_time else '--'
                Log.logger.info(f"[{time_str}] 撤单成功: {order_id}")
                return True
            else:
                bt_time = self.context.get('current_dt')
                time_str = bt_time.strftime('%Y-%m-%d %H:%M:%S') if bt_time else '--'
                Log.logger.debug(f"[{time_str}] 模拟撤单: {order_id}")
                return True
        except Exception as e:
            bt_time = self.context.get('current_dt')
            time_str = bt_time.strftime('%Y-%m-%d %H:%M:%S') if bt_time else '--'
            Log.logger.error(f"[{time_str}] 撤单失败: {e}")
            return False
    
    def get_account_sync(self, adapter_id='backtest', refresh=False):
        """同步获取账户信息"""
        try:
            if hasattr(self.engine, 'trade_center'):
                account = self.engine.trade_center.account
                Log.logger.debug(f"返回账户信息: 现金={account.cash_available}, 总资产={account.total_assets}")
                return account
            else:
                # 创建模拟账户信息
                from types import SimpleNamespace
                account = SimpleNamespace()
                account.cash_available = 1000000.0  # 初始资金
                account.total_assets = 1000000.0
                account.market_value = 0.0
                Log.logger.debug("返回模拟账户信息")
                return account
        except Exception as e:
            Log.logger.error(f"获取账户信息失败: {e}")
            return None
    
    def get_positions_sync(self, adapter_id='backtest', symbol=None, refresh=False):
        """同步获取持仓信息"""
        try:
            if hasattr(self.engine, 'trade_center'):
                positions = list(self.engine.trade_center.positions.values())
                if symbol:
                    positions = [pos for pos in positions if pos.symbol == symbol]
                # 过滤尘埃持仓（浮点精度残留的极小值）
                positions = [pos for pos in positions if pos.volume > 1e-4]
                Log.logger.debug(f"返回持仓信息: {len(positions)} 个持仓")
                return positions
            else:
                # 返回空的持仓列表（暂时）
                Log.logger.debug("返回空持仓列表")
                return []
        except Exception as e:
            Log.logger.error(f"获取持仓信息失败: {e}")
            return []
    
    def get_orders_sync(self, adapter_id='backtest', symbol=None, status=None, **kwargs):
        """同步获取订单信息"""
        try:
            if hasattr(self.engine, 'trade_center'):
                orders = list(self.engine.trade_center.orders.values())
                if symbol:
                    orders = [order for order in orders if order.symbol == symbol]
                if status:
                    orders = [order for order in orders if order.status == status]
                Log.logger.debug(f"返回订单信息: {len(orders)} 个订单")
                return orders
            else:
                Log.logger.debug("返回空订单列表")
                return []
        except Exception as e:
            Log.logger.error(f"获取订单信息失败: {e}")
            return []
    
    def get_trades_sync(self, adapter_id='backtest', symbol=None, **kwargs):
        """同步获取成交信息"""
        try:
            if hasattr(self.engine, 'trade_center'):
                trades = self.engine.trade_center.trades.copy()
                if symbol:
                    trades = [trade for trade in trades if trade.symbol == symbol]
                Log.logger.debug(f"返回成交信息: {len(trades)} 个成交")
                return trades
            else:
                Log.logger.debug("返回空成交列表")
                return []
        except Exception as e:
            Log.logger.error(f"获取成交信息失败: {e}")
            return []
    
    def _validate_order_volume(self, symbol, volume):
        """校验下单数量是否满足市场最小交易量要求"""
        from finhack.trader.backtest.constants import MIN_ORDER_QUANTITY
        market = self.context.get('settings', {}).get('market', '')
        min_qty = MIN_ORDER_QUANTITY.get(market, 0)
        if min_qty > 0 and abs(volume) < min_qty:
            Log.logger.debug(f"订单数量 {volume} 小于最小交易量 {min_qty}（{market}），忽略: {symbol}")
            return False
        return True

    def order_buy_sync(self, context, symbol, volume, price=None):
        if not self._validate_order_volume(symbol, volume):
            return None
        from finhack.trader.backtest.models.enums import Side, OrderType
        order_type = OrderType.LIMIT if price else OrderType.MARKET
        result = self.place_order_sync('backtest', symbol, Side.BUY, order_type, volume, price)
        if result:
            self._record_trade('buy', symbol, volume, price or self._get_last_price(symbol), context)
        return result
    
    def order_sell_sync(self, context, symbol, volume, price=None):
        """便利卖出方法

        Args:
            context: 回测上下文（兼容策略调用方式）
            symbol: 股票代码
            volume: 数量
            price: 价格（可选）
        """
        if not self._validate_order_volume(symbol, volume):
            return None
        from finhack.trader.backtest.models.enums import Side, OrderType
        order_type = OrderType.LIMIT if price else OrderType.MARKET

        # 期货/合约市场：根据持仓方向自动路由
        market = self.context.get('settings', {}).get('market', '')
        if market in ('cn_future', 'global_cryptoswap'):
            from finhack.trader.backtest.constants import SHORT_POSITION_SUFFIX
            trade_center = self.context.get('trade_center')
            if trade_center:
                long_position = trade_center.positions.get(symbol)
                short_key = f"{symbol}{SHORT_POSITION_SUFFIX}"
                short_position = trade_center.positions.get(short_key)
                long_available = long_position.available_volume if long_position else 0
                short_available = short_position.available_volume if short_position else 0
                if long_available <= 0:
                    if short_available > 0:
                        # 有空头持仓 → 平空仓（order_sell语义为减仓/平仓）
                        close_volume = min(volume, short_available)
                        return self.place_order_sync('backtest', symbol, Side.SHORT_CLOSE, order_type, close_volume, price)
                    else:
                        # 无任何持仓 → 开空仓
                        return self.place_order_sync('backtest', symbol, Side.SHORT_OPEN, order_type, volume, price)
                elif long_available < volume:
                    # 多头持仓不足，先平多仓再开空仓
                    sell_order = self.place_order_sync('backtest', symbol, Side.SELL, order_type, long_available, price)
                    short_order = self.place_order_sync('backtest', symbol, Side.SHORT_OPEN, order_type, volume - long_available, price)
                    return short_order if short_order else sell_order

        result = self.place_order_sync('backtest', symbol, Side.SELL, order_type, volume, price)
        if result:
            self._record_trade('sell', symbol, volume, price or self._get_last_price(symbol), context)
        return result

    def _record_trade(self, side, symbol, volume, price, context):
        """记录交易详情到 self._trades_list（dashboard 交易表 + 持久化用）"""
        if not hasattr(self, '_trades_list'):
            self._trades_list = []
        try:
            dt = context.get('current_dt', '') if isinstance(context, dict) else getattr(context, 'current_dt', '')
            ts = dt.strftime('%Y-%m-%d %H:%M:%S') if hasattr(dt, 'strftime') else str(dt)
        except Exception:
            ts = ''
        self._trades_list.append({
            'symbol': symbol, 'side': side, 'volume': volume,
            'price': float(price) if price else 0,
            'amount': float(volume * price) if price else 0,
            'trade_time': ts
        })

    def _get_last_price(self, symbol):
        """获取最新价格（用于市价单记录）"""
        try:
            tc = self.context.get('trade_center') if isinstance(self.context, dict) else None
            if tc:
                pos = tc.positions.get(symbol)
                if pos and hasattr(pos, 'last_price'):
                    return pos.last_price
        except Exception:
            pass
        return 0
        """开空仓方法（做空）

        用于期货、加密货币等支持做空的市场

        Args:
            context: 回测上下文（兼容策略调用方式）
            symbol: 标的代码
            volume: 数量
            price: 价格（可选）

        Returns:
            order_id: 订单ID
        """
        from finhack.trader.backtest.models.enums import Side, OrderType
        order_type = OrderType.LIMIT if price else OrderType.MARKET
        # 开空仓本质是卖出，标记为SHORT类型
        return self.place_order_sync('backtest', symbol, Side.SHORT_OPEN, order_type, volume, price)

    def close_short_sync(self, context, symbol, volume, price=None):
        """平空仓方法（平做空仓位）

        用于期货、加密货币等支持做空的市场

        Args:
            context: 回测上下文（兼容策略调用方式）
            symbol: 标的代码
            volume: 数量
            price: 价格（可选）

        Returns:
            order_id: 订单ID
        """
        from finhack.trader.backtest.models.enums import Side, OrderType
        order_type = OrderType.LIMIT if price else OrderType.MARKET
        # 平空仓本质是买入，标记为SHORT_CLOSE类型
        return self.place_order_sync('backtest', symbol, Side.SHORT_CLOSE, order_type, volume, price)

    def close_long_sync(self, context, symbol, volume, price=None):
        """平多仓方法（平做多仓位）

        用于期货、加密货币等支持做空的市场

        Args:
            context: 回测上下文（兼容策略调用方式）
            symbol: 标的代码
            volume: 数量
            price: 价格（可选）

        Returns:
            order_id: 订单ID
        """
        from finhack.trader.backtest.models.enums import Side, OrderType
        order_type = OrderType.LIMIT if price else OrderType.MARKET
        # 平多仓本质是卖出
        return self.place_order_sync('backtest', symbol, Side.SELL, order_type, volume, price)
    
    def sell_all_stocks_sync(self, context=None):
        """卖出所有持仓的便利方法"""
        try:
            positions = self.get_positions_sync()
            sell_orders = []

            for position in positions:
                # 跳过尘埃持仓（浮点精度残留），避免无意义卖出
                if position.volume > 1e-4:
                    order_id = self.order_sell_sync(self.context, position.symbol, position.volume)
                    if order_id:
                        sell_orders.append(order_id)
                        bt_time = self.context.get('current_dt')
                        time_str = bt_time.strftime('%Y-%m-%d %H:%M:%S') if bt_time else '--'
                        Log.logger.debug(f"[{time_str}] 卖出持仓: {position.symbol} 数量: {position.volume}")

            return sell_orders
        except Exception as e:
            bt_time = self.context.get('current_dt')
            time_str = bt_time.strftime('%Y-%m-%d %H:%M:%S') if bt_time else '--'
            Log.logger.error(f"[{time_str}] 卖出所有股票失败: {e}")
            return []
    
    def set_benchmark(self, benchmark):
        """设置基准（仅在未通过CLI/配置设置时生效）"""
        if self.context and 'settings' in self.context:
            current = self.context['settings'].get('benchmark', '')
            if not current:
                self.context['settings']['benchmark'] = benchmark
                Log.logger.debug(f"设置基准: {benchmark}")
            else:
                Log.logger.debug(f"保留已有基准设置: {current}（跳过策略中的 {benchmark}）")
    
    def set_option(self, key, value):
        """设置选项"""
        if self.context and 'settings' in self.context:
            self.context['settings'][key] = value
            Log.logger.debug(f"设置选项: {key} = {value}")
    
    def set_order_cost(self, order_cost, type='stock'):
        """设置手续费

        Args:
            order_cost: OrderCost对象，包含以下属性：
                open_tax: 买入印花税
                close_tax: 卖出印花税
                open_commission: 买入/开仓手续费率
                close_commission: 卖出/平仓手续费率
                close_today_commission: 平今手续费率（期货专用）
                min_commission: 最低手续费
            type: 市场类型 ('stock', 'fund', 'future', 'crypto')
        """
        settings = self.context.get('settings', {})

        if hasattr(order_cost, 'open_commission'):
            settings['open_commission'] = getattr(order_cost, 'open_commission', 0.0003)
        if hasattr(order_cost, 'close_commission'):
            settings['close_commission'] = getattr(order_cost, 'close_commission', 0.0003)
        if hasattr(order_cost, 'close_today_commission'):
            settings['close_today_commission'] = getattr(order_cost, 'close_today_commission', 0.0003)
        if hasattr(order_cost, 'min_commission'):
            settings['min_commission'] = getattr(order_cost, 'min_commission', 5.0)
        if hasattr(order_cost, 'open_tax'):
            settings['open_tax'] = getattr(order_cost, 'open_tax', 0.0)
        if hasattr(order_cost, 'close_tax'):
            settings['close_tax'] = getattr(order_cost, 'close_tax', 0.001)

        Log.logger.info(f"设置手续费: open_commission={settings.get('open_commission')}, "
                       f"close_commission={settings.get('close_commission')}, "
                       f"close_today_commission={settings.get('close_today_commission')}, "
                       f"min_commission={settings.get('min_commission')}, "
                       f"open_tax={settings.get('open_tax')}, close_tax={settings.get('close_tax')}")

    def set_slippage(self, slippage, type='stock'):
        """设置滑点

        Args:
            slippage: 滑点值（可以是数值或对象）
                数值: 按比例滑点（0.001 表示 0.1%）
                对象: 需有 slip_type 和 slip_value 属性
            type: 滑点类型 ('pricerelated': 按比例, 'fixed': 固定点数)
        """
        settings = self.context.get('settings', {})

        if hasattr(slippage, 'slip_type'):
            settings['slip_type'] = slippage.slip_type
            settings['slip_value'] = getattr(slippage, 'slip_value', 0.001)
        else:
            settings['slip_type'] = 'pricerelated'
            settings['slip_value'] = float(slippage) if slippage else 0.001

        Log.logger.info(f"设置滑点: slip_type={settings.get('slip_type')}, slip_value={settings.get('slip_value')}")

    def set_funding_rate(self, config):
        """设置资金费率配置（仅对 global_cryptoswap 市场生效）

        Args:
            config: FundingRateConfig对象或字典，包含:
                mode: 'fixed'（固定费率）| 'dynamic'（动态计算）| 'none'（关闭）
                rate: 固定费率值（如0.0001表示0.01%/8h），mode='fixed'时使用
                interval_hours: 结算间隔小时数（默认8小时）
        """
        settings = self.context.get('settings', {})

        if hasattr(config, 'mode'):
            settings['funding_rate_mode'] = getattr(config, 'mode', 'fixed')
            settings['funding_rate'] = getattr(config, 'rate', 0.0001)
            settings['funding_interval_hours'] = getattr(config, 'interval_hours', 8)
        elif isinstance(config, dict):
            settings['funding_rate_mode'] = config.get('mode', 'fixed')
            settings['funding_rate'] = config.get('rate', 0.0001)
            settings['funding_interval_hours'] = config.get('interval_hours', 8)

        Log.logger.info(
            f"设置资金费率: mode={settings.get('funding_rate_mode')}, "
            f"rate={settings.get('funding_rate')}, "
            f"interval={settings.get('funding_interval_hours')}h"
        )

    # ==================== 定时任务注册函数 ====================
    
    def run_daily(self, func, time="14:50:00"):
        """注册每日定时任务"""
        task = {
            'task_id': f"daily-{time}-{func.__name__}",
            'function_object': func,
            'function_name': func.__name__,
            'scheduling_rule': {
                'type': 'daily',
                'time': time
            },
            'next_run_time': None
        }
        self.context['scheduled_tasks'].append(task)
        Log.logger.info(f"注册每日任务: {func.__name__} at {time}")
        
    def run_weekly(self, func, weekday, time="14:50:00"):
        """注册每周定时任务"""
        task = {
            'task_id': f"weekly-W{weekday}-{time}-{func.__name__}",
            'function_object': func,
            'function_name': func.__name__,
            'scheduling_rule': {
                'type': 'weekly',
                'weekday': weekday,
                'time': time
            },
            'next_run_time': None
        }
        self.context['scheduled_tasks'].append(task)
        Log.logger.info(f"注册每周任务: {func.__name__} weekday={weekday} at {time}")
        
    def run_interval(self, func, frequency, reference_time="09:30:00"):
        """注册间隔定时任务"""
        task = {
            'task_id': f"interval-{frequency}-{reference_time}-{func.__name__}",
            'function_object': func,
            'function_name': func.__name__,
            'scheduling_rule': {
                'type': 'interval',
                'frequency': frequency,
                'reference_time': reference_time
            },
            'next_run_time': None
        }
        self.context['scheduled_tasks'].append(task)
        Log.logger.info(f"注册间隔任务: {func.__name__} every {frequency} from {reference_time}")

    def run_monthly(self, func, monthday=1, time="14:50:00"):
        """注册每月定时任务"""
        task = {
            'task_id': f"monthly-D{monthday}-{time}-{func.__name__}",
            'function_object': func,
            'function_name': func.__name__,
            'scheduling_rule': {
                'type': 'monthly',
                'monthday': monthday,
                'time': time
            },
            'next_run_time': None
        }
        self.context['scheduled_tasks'].append(task)
        Log.logger.info(f"注册每月任务: {func.__name__} monthday={monthday} at {time}")
    
    def run_backtest_sync(self):
        """运行回测主循环 - 同步版本"""
        Log.logger.info("开始运行回测...")
        
        # 启动回测引擎
        self.engine.run_sync(
            start_date=self.context['settings']['start_date'],
            end_date=self.context['settings']['end_date'],
            strategy=self.strategy,
            scheduled_tasks=self.context['scheduled_tasks']
        )
        # 持久化回测结果（供 dashboard 查看）
        try:
            self._save_backtest_result()
        except Exception as e:
            Log.logger.error(f"保存回测结果失败: {e}")

    def _save_backtest_result(self):
        """将回测结果 pickle 到 data/backtest/{instance_id}.pkl"""
        """将回测结果 pickle 到 data/backtest/{instance_id}.pkl"""
        import pickle
        from runtime.constant import DATA_DIR
        from datetime import datetime as _dt
        settings = self.context.get('settings', {})
        market = settings.get('market', 'unknown')
        strategy = settings.get('strategy_name') or settings.get('strategy') or 'unknown'
        instance_id = f"{_dt.now().strftime('%Y%m%d_%H%M%S')}_{market}_{strategy}"
        result = {
            'instance_id': instance_id,
            'market': market,
            'strategy': strategy,
            'freq': settings.get('freq', '1d'),
            'start_date': settings.get('start_date', ''),
            'end_date': settings.get('end_date', ''),
            'cash': settings.get('initial_capital', settings.get('cash', 1000000)),
            'performance': self.context.get('performance', {}) or {},
            'trades': _collect_trades(self, self.context, getattr(self, 'engine', None)),
            'daily_history': self.context.get('logs', {}).get('daily_history', []),
            'create_time': _dt.now().isoformat(),
        }
        backtest_dir = os.path.join(DATA_DIR, 'backtest')
        os.makedirs(backtest_dir, exist_ok=True)
        path = os.path.join(backtest_dir, f'{instance_id}.pkl')
        with open(path, 'wb') as f:
            pickle.dump(result, f)
        Log.logger.info(f"回测结果已保存: {path}")
        self.context['backtest_id'] = instance_id

    def get_cash_sync(self, context=None):
        """获取当前可用现金"""
        try:
            if hasattr(self, 'engine') and hasattr(self.engine, 'trade_center'):
                return self.engine.trade_center.account.cash_available
            elif hasattr(self, 'context') and 'account' in self.context:
                return self.context['account']['cash_available']
            return 0.0
        except Exception as e:
            Log.logger.error(f"获取现金失败: {e}")
            return 0.0
    
    def get_current_price_sync(self, context, code):
        """获取当前价格"""
        try:
            current_time = context.get('current_dt', self.context.get('current_dt'))
            freq = self.context['settings']['freq']
            
            # 获取最新价格
            quote_df = self.data_center.get_quotes([code], freq=freq, time=current_time, fields=['close'])
            
            if not quote_df.empty and code in quote_df.index:
                return quote_df.loc[code, 'close']
            
            return None
        except Exception as e:
            Log.logger.error(f"获取{code}价格失败: {e}")
            return None 