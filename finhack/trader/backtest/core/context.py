"""
上下文管理器实现
"""

import hashlib
import json
import pickle
import os
from datetime import datetime, date
from typing import Any, Dict, Optional, List
from dataclasses import dataclass, field
from pathlib import Path

from finhack.core.classes.dictobj import DictObj


@dataclass
class AccountInfo:
    """账户信息"""
    username: str = ""
    password: str = ""
    account_id: str = ""
    initial_cash: float = 1000000.0
    cash: float = 1000000.0
    total_value: float = 1000000.0
    available_cash: float = 1000000.0
    locked_cash: float = 0.0
    margin: float = 0.0
    
    # 费率设置
    open_tax: float = 0.0
    close_tax: float = 0.001
    open_commission: float = 0.0003
    close_commission: float = 0.0003
    close_today_commission: float = 0.0
    min_commission: float = 5.0


@dataclass 
class PortfolioInfo:
    """组合信息"""
    total_value: float = 1000000.0
    cash: float = 1000000.0
    positions_value: float = 0.0
    locked_cash: float = 0.0
    margin: float = 0.0
    returns: float = 0.0
    daily_returns: List[float] = field(default_factory=list)
    positions: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TradeConfig:
    """交易配置"""
    market: str = "cn_stock"
    start_time: str = "2023-01-01 00:00:00"
    end_time: str = "2024-12-31 23:59:59"
    benchmark: str = "000001.SH"
    strategy: str = "DemoStrategy"
    frequency: str = "1d"  # 1d, 1h, 1m, 1s
    
    # 风控参数
    max_position_ratio: float = 0.1
    max_order_ratio: float = 0.1
    enable_t1_rule: bool = True
    enable_limit_rule: bool = True
    
    # 滑点设置
    slippage: float = 0.005
    slippage_type: str = "percentage"  # percentage, fixed
    
    # 规则列表
    rule_list: str = "delist,stop,st,limit,slip,volume_ratio,cost,volume_num,t1"


@dataclass
class DataConfig:
    """数据配置"""
    data_source: str = "file"
    cache_enabled: bool = True
    preload_data: bool = True
    calendar: List[str] = field(default_factory=list)


class Context:
    """回测上下文管理器"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化上下文
        
        Args:
            config: 配置字典
        """
        # 基础信息
        self.id: str = ""
        self.created_at: datetime = datetime.now()
        self.current_dt: Optional[datetime] = None
        self.current_date: Optional[date] = None
        self.previous_date: Optional[date] = None
        
        # 组件引用
        self.event_center = None
        self.trade_center = None
        self.data_center = None
        self.strategy = None
        self.logger = None
        
        # 配置信息
        self.account = AccountInfo()
        self.portfolio = PortfolioInfo()
        self.trade_config = TradeConfig()
        self.data_config = DataConfig()
        
        # 用户自定义变量
        self.g = DictObj()
        
        # 参数和状态
        self.params: Dict[str, Any] = {}
        self.strategy_params: Dict[str, Any] = {}
        
        # 日志和历史记录
        self.logs: Dict[str, List] = {
            'trade_list': [],
            'order_list': [],
            'position_list': [],
            'return_list': [],
            'history': {}
        }
        
        # 绩效统计
        self.performance: Dict[str, Any] = {
            'returns': [],
            'bench_returns': [],
            'turnover': [],
            'win': 0,
            'win_ratio': 0.0,
            'trade_num': 0,
            'indicators': {}
        }
        
        # 如果有配置，则更新
        if config:
            self.update_from_config(config)
        
        # 生成唯一ID
        self._generate_context_id()
    
    def update_from_config(self, config: Dict[str, Any]):
        """从配置字典更新上下文"""
        
        # 更新账户信息
        if 'account' in config:
            account_config = config['account']
            for key, value in account_config.items():
                if hasattr(self.account, key):
                    setattr(self.account, key, value)
        
        # 更新交易配置
        if 'trade' in config:
            trade_config = config['trade']
            for key, value in trade_config.items():
                if hasattr(self.trade_config, key):
                    setattr(self.trade_config, key, value)
        
        # 更新数据配置
        if 'data' in config:
            data_config = config['data']
            for key, value in data_config.items():
                if hasattr(self.data_config, key):
                    setattr(self.data_config, key, value)
        
        # 更新参数
        if 'params' in config:
            self.params.update(config['params'])
        
        if 'strategy_params' in config:
            self.strategy_params.update(config['strategy_params'])
        
        # 初始化组合
        self.portfolio.cash = self.account.initial_cash
        self.portfolio.total_value = self.account.initial_cash
        self.account.cash = self.account.initial_cash
        self.account.total_value = self.account.initial_cash
        self.account.available_cash = self.account.initial_cash
    
    def _generate_context_id(self):
        """生成唯一的上下文ID"""
        # 创建包含关键配置的字符串
        config_str = json.dumps({
            'strategy': self.trade_config.strategy,
            'market': self.trade_config.market,
            'start_time': self.trade_config.start_time,
            'end_time': self.trade_config.end_time,
            'initial_cash': self.account.initial_cash,
            'params': self.params,
            'strategy_params': self.strategy_params,
            'created_at': self.created_at.isoformat()
        }, sort_keys=True)
        
        # 生成MD5哈希
        hash_obj = hashlib.md5(config_str.encode('utf-8'))
        self.id = hash_obj.hexdigest()
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'id': self.id,
            'created_at': self.created_at.isoformat(),
            'current_dt': self.current_dt.isoformat() if self.current_dt else None,
            'current_date': self.current_date.isoformat() if self.current_date else None,
            'previous_date': self.previous_date.isoformat() if self.previous_date else None,
            'account': self.account.__dict__,
            'portfolio': {
                'total_value': self.portfolio.total_value,
                'cash': self.portfolio.cash,
                'positions_value': self.portfolio.positions_value,
                'locked_cash': self.portfolio.locked_cash,
                'margin': self.portfolio.margin,
                'returns': self.portfolio.returns,
                'daily_returns': self.portfolio.daily_returns,
                'positions': self.portfolio.positions
            },
            'trade_config': self.trade_config.__dict__,
            'data_config': self.data_config.__dict__,
            'params': self.params,
            'strategy_params': self.strategy_params,
            'g': dict(self.g),
            'logs': self.logs,
            'performance': self.performance
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Context':
        """从字典创建上下文对象"""
        context = cls()
        
        # 基础信息
        context.id = data.get('id', '')
        context.created_at = datetime.fromisoformat(data['created_at']) if data.get('created_at') else datetime.now()
        context.current_dt = datetime.fromisoformat(data['current_dt']) if data.get('current_dt') else None
        context.current_date = date.fromisoformat(data['current_date']) if data.get('current_date') else None
        context.previous_date = date.fromisoformat(data['previous_date']) if data.get('previous_date') else None
        
        # 账户信息
        if 'account' in data:
            account_data = data['account']
            for key, value in account_data.items():
                if hasattr(context.account, key):
                    setattr(context.account, key, value)
        
        # 组合信息
        if 'portfolio' in data:
            portfolio_data = data['portfolio']
            for key, value in portfolio_data.items():
                if hasattr(context.portfolio, key):
                    setattr(context.portfolio, key, value)
        
        # 交易配置
        if 'trade_config' in data:
            trade_data = data['trade_config']
            for key, value in trade_data.items():
                if hasattr(context.trade_config, key):
                    setattr(context.trade_config, key, value)
        
        # 数据配置
        if 'data_config' in data:
            data_config = data['data_config']
            for key, value in data_config.items():
                if hasattr(context.data_config, key):
                    setattr(context.data_config, key, value)
        
        # 其他信息
        context.params = data.get('params', {})
        context.strategy_params = data.get('strategy_params', {})
        context.g = DictObj(data.get('g', {}))
        context.logs = data.get('logs', {'trade_list': [], 'order_list': [], 'position_list': [], 'return_list': [], 'history': {}})
        context.performance = data.get('performance', {'returns': [], 'bench_returns': [], 'turnover': [], 'win': 0, 'win_ratio': 0.0, 'trade_num': 0, 'indicators': {}})
        
        return context
    
    def save_to_file(self, base_dir: str) -> str:
        """
        保存上下文到文件
        
        Args:
            base_dir: 基础目录
            
        Returns:
            str: 保存的文件路径
        """
        # 确保目录存在
        running_dir = Path(base_dir) / "data" / "running"
        running_dir.mkdir(parents=True, exist_ok=True)
        
        # 生成文件名
        filename = f"backtest_{self.id}.pkl"
        filepath = running_dir / filename
        
        # 保存为pickle文件
        with open(filepath, 'wb') as f:
            pickle.dump(self.to_dict(), f)
        
        return str(filepath)
    
    @classmethod
    def load_from_file(cls, filepath: str) -> 'Context':
        """
        从文件加载上下文
        
        Args:
            filepath: 文件路径
            
        Returns:
            Context: 上下文对象
        """
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
        
        return cls.from_dict(data)
    
    def check_context_file_exists(self, base_dir: str) -> bool:
        """
        检查上下文文件是否存在
        
        Args:
            base_dir: 基础目录
            
        Returns:
            bool: 文件是否存在
        """
        running_dir = Path(base_dir) / "data" / "running"
        filename = f"backtest_{self.id}.pkl"
        filepath = running_dir / filename
        
        return filepath.exists()
    
    def update_portfolio_value(self):
        """更新组合总价值"""
        # 计算持仓价值
        positions_value = 0.0
        for position in self.portfolio.positions.values():
            if hasattr(position, 'total_value'):
                positions_value += position.total_value
            elif hasattr(position, 'amount') and hasattr(position, 'last_sale_price'):
                positions_value += position.amount * position.last_sale_price
        
        self.portfolio.positions_value = positions_value
        self.portfolio.total_value = self.portfolio.cash + positions_value
        self.account.total_value = self.portfolio.total_value
    
    def calculate_returns(self):
        """计算收益率"""
        if self.account.initial_cash > 0:
            self.portfolio.returns = (self.portfolio.total_value - self.account.initial_cash) / self.account.initial_cash
        else:
            self.portfolio.returns = 0.0
    
    def add_daily_return(self, daily_return: float):
        """添加日收益率"""
        self.portfolio.daily_returns.append(daily_return)
    
    def log_trade(self, trade_info: Dict[str, Any]):
        """记录交易信息"""
        trade_info['timestamp'] = datetime.now().isoformat()
        self.logs['trade_list'].append(trade_info)
    
    def log_order(self, order_info: Dict[str, Any]):
        """记录订单信息"""
        order_info['timestamp'] = datetime.now().isoformat()
        self.logs['order_list'].append(order_info)
    
    def get_position(self, symbol: str) -> Optional[Any]:
        """获取持仓信息"""
        return self.portfolio.positions.get(symbol)
    
    def set_position(self, symbol: str, position: Any):
        """设置持仓信息"""
        self.portfolio.positions[symbol] = position
    
    def remove_position(self, symbol: str):
        """移除持仓"""
        if symbol in self.portfolio.positions:
            del self.portfolio.positions[symbol] 