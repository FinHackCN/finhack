import asyncio
import importlib.util
import os
import json
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pandas as pd

logger = logging.getLogger(__name__)

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
        
        # 从参数中获取基础配置
        market = getattr(self.args, 'market', 'cn_stock')
        freq = getattr(self.args, 'freq', '1d') 
        start_date = getattr(self.args, 'start_time', '2024-01-01')
        end_date = getattr(self.args, 'end_time', '2024-12-31')
        initial_cash = float(getattr(self.args, 'cash', 1000000))
        benchmark = getattr(self.args, 'benchmark', '000001.SH')
        strategy_name = getattr(self.args, 'strategy', 'demoStrategy')
        
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
                'starting_cash': initial_cash,
                'order_volume_ratio': getattr(self.args, 'order_volume_ratio', 1.0),
                'slip_type': getattr(self.args, 'sliptype', 'pricerelated'),
                'slip_value': float(getattr(self.args, 'slip', 0.001)),
                'open_tax': float(getattr(self.args, 'open_tax', 0.0)),
                'close_tax': float(getattr(self.args, 'close_tax', 0.001)),
                'open_commission': float(getattr(self.args, 'open_commission', 0.0003)),
                'close_commission': float(getattr(self.args, 'close_commission', 0.0003)),
                'close_today_commission': float(getattr(self.args, 'close_today_commission', 0.0)),
                'min_commission': float(getattr(self.args, 'min_commission', 5.0)),
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
        
        # 设置交易中心的引用
        self.event_center.set_trade_center(self.engine.trade_center)
        
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
        Log.logger.info(f"开始查找策略类，模块中的对象: {[n for n in dir(strategy_module) if not n.startswith('_')]}")
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
                    Log.logger.info(f"尝试实例化策略类: {name}, config={config}")
                    print(f"[Trader] 尝试实例化 {name}", flush=True)
                    strategy_instance = obj(config)
                    Log.logger.info(f"成功实例化策略类: {name}")
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
        Log.logger.info("开始初始化策略...")
        print(f"[Trader] 开始初始化策略, strategy类型: {type(self.strategy)}", flush=True)
        
        # 绑定API函数到策略模块
        self.bind_strategy_api()
        Log.logger.info("API绑定完成")
        
        # 调用策略的initialize函数
        if hasattr(self.strategy, 'initialize'):
            Log.logger.info("调用strategy.initialize()...")
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
            Log.logger.info(f"从策略同步universe: {len(self.strategy.universe)}只股票/标的")
            print(f"[Trader] 同步universe: {len(self.strategy.universe)}只", flush=True)
            
    def bind_strategy_api(self):
        """绑定策略所需的API函数"""
        # 绑定定时任务注册函数
        self.strategy.run_daily = self.run_daily
        self.strategy.run_weekly = self.run_weekly
        self.strategy.run_interval = self.run_interval
        
        # 绑定数据获取方法
        self.strategy.get_quotes = self.data_center.get_quotes
        self.strategy.get_klines = self.data_center.get_klines
        self.strategy.get_factors = self.data_center.get_factors
        
        # 为了兼容性，提供别名
        self.strategy.get_price = self.get_price_sync
        self.strategy.get_kline = self.data_center.get_klines
        
        # 绑定交易方法（同步版本）
        self.strategy.place_order = self.place_order_sync
        self.strategy.cancel_order = self.cancel_order_sync
        self.strategy.get_account = self.get_account_sync
        self.strategy.get_positions = self.get_positions_sync
        self.strategy.get_orders = self.get_orders_sync
        self.strategy.get_trades = self.get_trades_sync
        self.strategy.get_cash = self.get_cash_sync
        self.strategy.get_current_price = self.get_current_price_sync
        
        # 绑定便利方法
        self.strategy.order_buy = self.order_buy_sync
        self.strategy.order_sell = self.order_sell_sync
        self.strategy.sell_all_stocks = self.sell_all_stocks_sync
        
        # 绑定回测配置方法（这些在回测中通常是空操作）
        self.strategy.set_benchmark = self.set_benchmark
        self.strategy.set_option = self.set_option
        self.strategy.set_order_cost = self.set_order_cost
        self.strategy.set_slippage = self.set_slippage
        
        logger.info("策略API绑定完成")
    
    def get_price_sync(self, code, time=None):
        """获取单个股票价格的同步方法（get_quotes的简化版本）"""
        try:
            # 获取回测频率，使用实际频率而不是固定1d
            freq = self.context.get('settings', {}).get('freq', '1d')
            
            # 确保传入正确的当前回测时间
            current_time = self.context.get('current_dt', datetime.now())
            if time is None:
                time = current_time
            
            logger.debug(f"获取价格数据: {code}, 频率: {freq}, 时间: {time}")
            
            # 直接使用DataCenter的get_quotes方法
            quotes = self.data_center.get_quotes([code], freq=freq, time=time, fields=['close'])
            if not quotes.empty:
                price = quotes.iloc[0]['close']
                logger.debug(f"成功获取 {code} 价格: {price}")
                return price
            else:
                logger.warning(f"无法获取股票 {code} 的价格数据 (时间: {time})")
                return None
                
        except Exception as e:
            logger.error(f"获取股票价格失败: {code} - {str(e)}")
            return None
    
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
            
            # 调用TradeCenter的下单方法
            if hasattr(self.engine, 'trade_center'):
                # 创建订单对象并添加到TradeCenter
                from datetime import datetime
                order_id = f"order_{symbol}_{int(datetime.now().timestamp())}"
                
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
                    created_time=self.context.get('current_dt', datetime.now())
                )
                
                # 添加到订单列表
                self.engine.trade_center.orders[order_id] = order
                order.status = OrderStatus.NEW
                
                logger.info(f"下单成功: {symbol} {side.value} {order_type.value} 数量:{volume} 价格:{price}")
                return order_id
            else:
                logger.info(f"模拟下单: {symbol} {side} {order_type} 数量:{volume} 价格:{price}")
                return f"order_{symbol}_{int(datetime.now().timestamp())}"
        except Exception as e:
            logger.error(f"下单失败: {e}")
            return None
    
    def cancel_order_sync(self, adapter_id, order_id, **kwargs):
        """同步撤单方法"""
        try:
            if hasattr(self.engine, 'trade_center') and order_id in self.engine.trade_center.orders:
                order = self.engine.trade_center.orders[order_id]
                order.status = OrderStatus.CANCELLED
                order.rejected_reason = "用户撤单"
                logger.info(f"撤单成功: {order_id}")
                return True
            else:
                logger.info(f"模拟撤单: {order_id}")
                return True
        except Exception as e:
            logger.error(f"撤单失败: {e}")
            return False
    
    def get_account_sync(self, adapter_id='backtest', refresh=False):
        """同步获取账户信息"""
        try:
            if hasattr(self.engine, 'trade_center'):
                account = self.engine.trade_center.account
                logger.debug(f"返回账户信息: 现金={account.cash_available}, 总资产={account.total_assets}")
                return account
            else:
                # 创建模拟账户信息
                from types import SimpleNamespace
                account = SimpleNamespace()
                account.cash_available = 1000000.0  # 初始资金
                account.total_assets = 1000000.0
                account.market_value = 0.0
                logger.debug("返回模拟账户信息")
                return account
        except Exception as e:
            logger.error(f"获取账户信息失败: {e}")
            return None
    
    def get_positions_sync(self, adapter_id='backtest', symbol=None, refresh=False):
        """同步获取持仓信息"""
        try:
            if hasattr(self.engine, 'trade_center'):
                positions = list(self.engine.trade_center.positions.values())
                if symbol:
                    positions = [pos for pos in positions if pos.symbol == symbol]
                logger.debug(f"返回持仓信息: {len(positions)} 个持仓")
                return positions
            else:
                # 返回空的持仓列表（暂时）
                logger.debug("返回空持仓列表")
                return []
        except Exception as e:
            logger.error(f"获取持仓信息失败: {e}")
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
                logger.debug(f"返回订单信息: {len(orders)} 个订单")
                return orders
            else:
                logger.debug("返回空订单列表")
                return []
        except Exception as e:
            logger.error(f"获取订单信息失败: {e}")
            return []
    
    def get_trades_sync(self, adapter_id='backtest', symbol=None, **kwargs):
        """同步获取成交信息"""
        try:
            if hasattr(self.engine, 'trade_center'):
                trades = self.engine.trade_center.trades.copy()
                if symbol:
                    trades = [trade for trade in trades if trade.symbol == symbol]
                logger.debug(f"返回成交信息: {len(trades)} 个成交")
                return trades
            else:
                logger.debug("返回空成交列表")
                return []
        except Exception as e:
            logger.error(f"获取成交信息失败: {e}")
            return []
    
    def order_buy_sync(self, context, symbol, volume, price=None):
        """便利买入方法
        
        Args:
            context: 回测上下文（兼容策略调用方式）
            symbol: 股票代码
            volume: 数量
            price: 价格（可选）
        """
        from finhack.trader.backtest.models.enums import Side, OrderType
        order_type = OrderType.LIMIT if price else OrderType.MARKET
        return self.place_order_sync('backtest', symbol, Side.BUY, order_type, volume, price)
    
    def order_sell_sync(self, context, symbol, volume, price=None):
        """便利卖出方法
        
        Args:
            context: 回测上下文（兼容策略调用方式）
            symbol: 股票代码
            volume: 数量
            price: 价格（可选）
        """
        from finhack.trader.backtest.models.enums import Side, OrderType
        order_type = OrderType.LIMIT if price else OrderType.MARKET
        return self.place_order_sync('backtest', symbol, Side.SELL, order_type, volume, price)
    
    def sell_all_stocks_sync(self, context=None):
        """卖出所有持仓的便利方法"""
        try:
            positions = self.get_positions_sync()
            sell_orders = []
            
            for position in positions:
                if position.volume > 0:  # 确保有持仓
                    order_id = self.order_sell_sync(position.symbol, position.volume)
                    if order_id:
                        sell_orders.append(order_id)
                        logger.info(f"卖出持仓: {position.symbol} 数量: {position.volume}")
                    
            return sell_orders
        except Exception as e:
            logger.error(f"卖出所有股票失败: {e}")
            return []
    
    def set_benchmark(self, benchmark):
        """设置基准"""
        if self.context and 'settings' in self.context:
            self.context['settings']['benchmark'] = benchmark
            logger.info(f"设置基准: {benchmark}")
    
    def set_option(self, key, value):
        """设置选项"""
        if self.context and 'settings' in self.context:
            self.context['settings'][key] = value
            logger.debug(f"设置选项: {key} = {value}")
    
    def set_order_cost(self, order_cost, type='stock'):
        """设置手续费"""
        logger.debug(f"设置手续费: {type}")
        # 在回测中，手续费通过TradeCenter配置
    
    def set_slippage(self, slippage, type='stock'):
        """设置滑点"""
        logger.debug(f"设置滑点: {type}")
        # 在回测中，滑点通过TradeCenter配置 
    
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
        logger.info(f"注册每日任务: {func.__name__} at {time}")
        
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
        logger.info(f"注册每周任务: {func.__name__} weekday={weekday} at {time}")
        
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
        logger.info(f"注册间隔任务: {func.__name__} every {frequency} from {reference_time}")
    
    def run_backtest_sync(self):
        """运行回测主循环 - 同步版本"""
        logger.info("开始运行回测...")
        
        # 启动回测引擎
        self.engine.run_sync(
            start_date=self.context['settings']['start_date'],
            end_date=self.context['settings']['end_date'],
            strategy=self.strategy,
            scheduled_tasks=self.context['scheduled_tasks']
        )
    
    def get_cash_sync(self, context=None):
        """获取当前可用现金"""
        try:
            if hasattr(self, 'context') and 'account' in self.context:
                return self.context['account']['cash_available']
            return 0.0
        except Exception as e:
            logger.error(f"获取现金失败: {e}")
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
            logger.error(f"获取{code}价格失败: {e}")
            return None 