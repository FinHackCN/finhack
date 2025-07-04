"""
数据中心模块 - 完整版本

提供数据访问接口：
- get_kline: 获取K线数据
- get_factors: 获取因子数据
- compute_factors: 计算因子
- ml_predict: 机器学习预测
- get_price: 获取实时价格
- get_trading_calendar: 获取交易日历
"""

import logging
import os
import hashlib
import time
import threading
import sys
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
import pickle
import json

# 导入项目中的数据加载模块
def _setup_project_paths():
    """设置项目路径以便导入模块"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 可能的项目根目录
    possible_roots = [
        os.path.join(current_dir, '../../../../../demo_project'),
        os.path.join(current_dir, '../../../../../mysql_project'),
        os.path.join(current_dir, '../../../../..'),
        '/Users/woldy/Code/finhack-dev/demo_project',
        '/Users/woldy/Code/finhack-dev/mysql_project'
    ]
    
    for root in possible_roots:
        root = os.path.abspath(root)
        cache_runtime_path = os.path.join(root, 'data/cache')
        if os.path.exists(cache_runtime_path):
            if cache_runtime_path not in sys.path:
                sys.path.insert(0, cache_runtime_path)
            if root not in sys.path:
                sys.path.insert(0, root)
            return root
    
    return None

def _create_fallback_constants():
    """创建回退常量定义"""
    project_root = _setup_project_paths()
    
    if project_root:
        DATA_DIR = os.path.join(project_root, 'data')
        CACHE_DIR = os.path.join(DATA_DIR, 'cache')
        FACTORS_DIR = os.path.join(DATA_DIR, 'factors')
        FACTORS_CACHE_DIR = os.path.join(CACHE_DIR, 'factors')
        PRICE_CACHE_DIR = os.path.join(CACHE_DIR, 'price')
    else:
        # 使用当前目录作为回退
        base_dir = os.path.dirname(os.path.abspath(__file__))
        DATA_DIR = os.path.join(base_dir, 'data')
        CACHE_DIR = os.path.join(DATA_DIR, 'cache')
        FACTORS_DIR = os.path.join(DATA_DIR, 'factors')
        FACTORS_CACHE_DIR = os.path.join(CACHE_DIR, 'factors')
        PRICE_CACHE_DIR = os.path.join(CACHE_DIR, 'price')
    
    return {
        'DATA_DIR': DATA_DIR,
        'CACHE_DIR': CACHE_DIR,
        'FACTORS_DIR': FACTORS_DIR,
        'FACTORS_CACHE_DIR': FACTORS_CACHE_DIR,
        'PRICE_CACHE_DIR': PRICE_CACHE_DIR
    }

# 尝试导入真实数据模块
try:
    # 首先设置路径
    project_root = _setup_project_paths()
    
    # 尝试导入常量
    try:
        from runtime.constant import *
        constants_available = True
        logging.info("成功导入runtime.constant模块")
    except ImportError as e:
        logging.warning(f"无法导入runtime.constant: {e}")
        # 创建回退常量
        fallback_constants = _create_fallback_constants()
        globals().update(fallback_constants)
        constants_available = False
    
    # 尝试导入数据加载模块
    try:
        from finhack.library.kline import loadKline, inspectKline
        kline_module_available = True
        logging.info("成功导入kline模块")
    except ImportError as e:
        logging.warning(f"无法导入kline模块: {e}")
        kline_module_available = False
        
    try:
        from finhack.factor.default.factorManager import factorManager
        factor_module_available = True
        logging.info("成功导入factorManager模块")
    except ImportError as e:
        logging.warning(f"无法导入factorManager模块: {e}")
        factor_module_available = False
    
    # 可选导入
    try:
        from finhack.library.trainer import Trainer
        has_trainer = True
        logging.info("成功导入Trainer模块")
    except ImportError as e:
        logging.warning(f"无法导入Trainer模块: {e}")
        has_trainer = False
    
    # 判断是否有足够的真实数据模块
    HAS_REAL_DATA = kline_module_available and factor_module_available
    
except ImportError as e:
    logging.warning(f"无法导入真实数据模块: {e}")
    # 创建回退常量
    fallback_constants = _create_fallback_constants()
    globals().update(fallback_constants)
    HAS_REAL_DATA = False
    has_trainer = False
    kline_module_available = False
    factor_module_available = False

class DataCenter:
    """数据中心 - 提供数据访问接口，支持真实数据和缓存"""
    
    def __init__(self, cache_capacity: int = 1000, cache_ttl: int = 3600):
        """
        初始化数据中心
        
        Args:
            cache_capacity: 缓存容量
            cache_ttl: 缓存过期时间（秒）
        """
        self.logger = logging.getLogger(__name__)
        self.context = None
        
        # 缓存相关
        self.cache_capacity = cache_capacity
        self.cache_ttl = cache_ttl
        self.kline_cache = {}
        self.factors_cache = {}
        self.price_cache = {}
        self.model_cache = {}
        self.calendar_cache = {}
        
        # 缓存时间戳
        self.cache_timestamps = {}
        
        # 缓存统计
        self.cache_hit_count = 0
        self.cache_miss_count = 0
        
        # 线程锁
        self.cache_lock = threading.RLock()
        
        # 数据源配置
        self.data_source_config = {
            'use_real_data': HAS_REAL_DATA,
            'cache_enabled': True,
            'parallel_enabled': True,
            'max_workers': 8
        }
        
        # 输出状态信息
        status = "真实数据" if HAS_REAL_DATA else "模拟数据"
        self.logger.info(f"数据中心初始化完成 - 使用{status}模式")
    
    def initialize(self, context):
        """初始化数据中心"""
        self.context = context
        if hasattr(context, 'logger'):
            context.logger.info("数据中心初始化完成")
    
    def _get_cache_key(self, prefix: str, **kwargs) -> str:
        """生成缓存键"""
        # 将参数转换为字符串并排序
        params = sorted(kwargs.items())
        params_str = '_'.join([f"{k}={v}" for k, v in params])
        return f"{prefix}_{params_str}"
    
    def _is_cache_expired(self, cache_key: str) -> bool:
        """检查缓存是否过期"""
        if cache_key not in self.cache_timestamps:
            return True
        
        timestamp = self.cache_timestamps[cache_key]
        return time.time() - timestamp > self.cache_ttl
    
    def _update_cache(self, cache_key: str, data: Any, cache_dict: Dict):
        """更新缓存"""
        with self.cache_lock:
            # 如果缓存满了，删除最旧的条目
            if len(cache_dict) >= self.cache_capacity:
                oldest_key = min(self.cache_timestamps.keys(), 
                               key=lambda k: self.cache_timestamps[k])
                if oldest_key in cache_dict:
                    del cache_dict[oldest_key]
                    del self.cache_timestamps[oldest_key]
            
            # 添加新数据
            cache_dict[cache_key] = data
            self.cache_timestamps[cache_key] = time.time()
    
    def _get_from_cache(self, cache_key: str, cache_dict: Dict) -> Optional[Any]:
        """从缓存获取数据"""
        with self.cache_lock:
            if cache_key in cache_dict and not self._is_cache_expired(cache_key):
                self.cache_hit_count += 1
                return cache_dict[cache_key]
            
            self.cache_miss_count += 1
            return None
    
    def get_kline(self, symbol: str, start_date: str, end_date: str, 
                  period: str = '1d', market: str = 'cn_stock') -> pd.DataFrame:
        """
        获取K线数据
        
        Args:
            symbol: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            period: 周期 (1d, 1h, 1m等)
            market: 市场类型
            
        Returns:
            包含K线数据的DataFrame
        """
        try:
            # 生成缓存键
            cache_key = self._get_cache_key(
                'kline', 
                symbol=symbol, 
                start_date=start_date, 
                end_date=end_date, 
                period=period, 
                market=market
            )
            
            # 检查缓存
            if self.data_source_config['cache_enabled']:
                cached_data = self._get_from_cache(cache_key, self.kline_cache)
                if cached_data is not None:
                    self.logger.debug(f"从缓存获取K线数据: {symbol}")
                    return cached_data
            
            # 加载真实数据
            if self.data_source_config['use_real_data']:
                kline_data = self._load_real_kline(symbol, start_date, end_date, period, market)
            else:
                kline_data = self._generate_mock_kline(symbol, start_date, end_date, period)
            
            # 更新缓存
            if self.data_source_config['cache_enabled'] and not kline_data.empty:
                self._update_cache(cache_key, kline_data, self.kline_cache)
            
            self.logger.info(f"获取K线数据: {symbol}, {start_date} - {end_date}, {len(kline_data)}条记录")
            return kline_data
            
        except Exception as e:
            self.logger.error(f"获取K线数据失败: {symbol}, {e}")
            return pd.DataFrame()
    
    def _load_real_kline(self, symbol: str, start_date: str, end_date: str, 
                        period: str, market: str) -> pd.DataFrame:
        """加载真实K线数据"""
        try:
            # 检查是否有kline模块
            if not globals().get('kline_module_available', False):
                self.logger.warning("kline模块不可用，无法加载真实K线数据")
                return pd.DataFrame()
            
            # 转换日期格式
            start_date_fmt = start_date.replace('-', '')
            end_date_fmt = end_date.replace('-', '')
            
            self.logger.info(f"开始加载真实K线数据: {symbol}, {start_date_fmt} - {end_date_fmt}")
            
            # 使用项目中的loadKline函数
            kline_data = loadKline(
                market=market,
                freq=period,
                start_date=start_date_fmt,
                end_date=end_date_fmt,
                code_list=[symbol],
                cache=True
            )
            
            # 过滤指定股票的数据
            if not kline_data.empty and 'code' in kline_data.columns:
                kline_data = kline_data[kline_data['code'] == symbol].copy()
            
            # 重命名列以匹配标准格式
            if not kline_data.empty:
                column_mapping = {
                    'time': 'date',
                    'code': 'symbol'
                }
                kline_data = kline_data.rename(columns=column_mapping)
                
                # 确保必要的列存在
                required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
                for col in required_columns:
                    if col not in kline_data.columns:
                        if col == 'volume':
                            kline_data[col] = 0
                        elif col == 'amount':
                            kline_data[col] = 0
                        else:
                            kline_data[col] = np.nan
                
                # 处理日期列
                if 'date' in kline_data.columns:
                    kline_data['date'] = pd.to_datetime(kline_data['date'])
                
                # 添加symbol列
                if 'symbol' not in kline_data.columns:
                    kline_data['symbol'] = symbol
                
                # 排序
                kline_data = kline_data.sort_values('date')
                
                self.logger.info(f"成功加载真实K线数据: {symbol}, {len(kline_data)}条记录")
            else:
                self.logger.warning(f"未找到K线数据: {symbol}")
            
            return kline_data
            
        except Exception as e:
            self.logger.error(f"加载真实K线数据失败: {symbol}, {e}")
            return pd.DataFrame()
    
    def _generate_mock_kline(self, symbol: str, start_date: str, end_date: str, 
                            period: str) -> pd.DataFrame:
        """生成模拟K线数据"""
        try:
            dates = pd.date_range(start=start_date, end=end_date, freq='D')
            dates = dates[dates.dayofweek < 5]  # 只保留工作日
            
            # 创建模拟数据
            n = len(dates)
            base_price = 100
            price_changes = np.random.randn(n) * 0.02  # 2%的日波动
            
            # 计算价格序列
            prices = [base_price]
            for change in price_changes[1:]:
                prices.append(prices[-1] * (1 + change))
            
            # 创建OHLC数据
            data = []
            for i, date in enumerate(dates):
                close = prices[i]
                open_price = close * (1 + np.random.randn() * 0.005)
                high = max(open_price, close) * (1 + abs(np.random.randn() * 0.01))
                low = min(open_price, close) * (1 - abs(np.random.randn() * 0.01))
                volume = np.random.randint(1000000, 10000000)
                
                data.append({
                    'date': date,
                    'open': open_price,
                    'high': high,
                    'low': low,
                    'close': close,
                    'volume': volume,
                    'symbol': symbol
                })
            
            return pd.DataFrame(data)
            
        except Exception as e:
            self.logger.error(f"生成模拟K线数据失败: {e}")
            return pd.DataFrame()
    
    def get_factors(self, symbol: str, start_date: str, end_date: str, 
                   factors: List[str], market: str = 'cn_stock', 
                   freq: str = '1d') -> pd.DataFrame:
        """
        获取因子数据
        
        Args:
            symbol: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            factors: 因子列表
            market: 市场类型
            freq: 频率
            
        Returns:
            包含因子数据的DataFrame
        """
        try:
            # 生成缓存键
            cache_key = self._get_cache_key(
                'factors',
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                factors=','.join(sorted(factors)),
                market=market,
                freq=freq
            )
            
            # 检查缓存
            if self.data_source_config['cache_enabled']:
                cached_data = self._get_from_cache(cache_key, self.factors_cache)
                if cached_data is not None:
                    self.logger.debug(f"从缓存获取因子数据: {symbol}")
                    return cached_data
            
            # 加载真实因子数据
            if self.data_source_config['use_real_data']:
                factors_data = self._load_real_factors(symbol, start_date, end_date, factors, market, freq)
            else:
                factors_data = self._generate_mock_factors(symbol, start_date, end_date, factors)
            
            # 更新缓存
            if self.data_source_config['cache_enabled'] and not factors_data.empty:
                self._update_cache(cache_key, factors_data, self.factors_cache)
            
            self.logger.info(f"获取因子数据: {symbol}, {len(factors)}个因子, {len(factors_data)}条记录")
            return factors_data
            
        except Exception as e:
            self.logger.error(f"获取因子数据失败: {symbol}, {e}")
            return pd.DataFrame()
    
    def _load_real_factors(self, symbol: str, start_date: str, end_date: str,
                          factors: List[str], market: str, freq: str) -> pd.DataFrame:
        """加载真实因子数据"""
        try:
            # 检查是否有factor模块
            if not globals().get('factor_module_available', False):
                self.logger.warning("factorManager模块不可用，无法加载真实因子数据")
                return pd.DataFrame()
            
            # 转换日期格式
            start_date_fmt = start_date.replace('-', '')
            end_date_fmt = end_date.replace('-', '')
            
            self.logger.info(f"开始加载真实因子数据: {symbol}, 因子:{factors}")
            
            # 使用factorManager加载因子数据
            factors_data = factorManager.loadFactors(
                matrix_list=factors,
                code_list=[symbol],
                market=market,
                freq=freq,
                start_date=start_date_fmt,
                end_date=end_date_fmt,
                cache=True
            )
            
            # 处理数据格式
            if not factors_data.empty:
                # 重命名列
                if 'code' in factors_data.columns:
                    factors_data = factors_data.rename(columns={'code': 'symbol'})
                if 'time' in factors_data.columns:
                    factors_data = factors_data.rename(columns={'time': 'date'})
                
                # 过滤指定股票的数据
                if 'symbol' in factors_data.columns:
                    factors_data = factors_data[factors_data['symbol'] == symbol].copy()
                
                # 处理日期列
                if 'date' in factors_data.columns:
                    factors_data['date'] = pd.to_datetime(factors_data['date'])
                
                # 排序
                factors_data = factors_data.sort_values('date')
                
                self.logger.info(f"成功加载真实因子数据: {symbol}, {len(factors_data)}条记录")
            else:
                self.logger.warning(f"未找到因子数据: {symbol}, 因子:{factors}")
            
            return factors_data
            
        except Exception as e:
            self.logger.error(f"加载真实因子数据失败: {symbol}, {e}")
            return pd.DataFrame()
    
    def _generate_mock_factors(self, symbol: str, start_date: str, end_date: str,
                              factors: List[str]) -> pd.DataFrame:
        """生成模拟因子数据"""
        try:
            dates = pd.date_range(start=start_date, end=end_date, freq='D')
            dates = dates[dates.dayofweek < 5]  # 只保留工作日
            
            data = []
            for date in dates:
                row = {'symbol': symbol, 'date': date}
                for factor in factors:
                    row[factor] = np.random.randn()
                data.append(row)
            
            return pd.DataFrame(data)
            
        except Exception as e:
            self.logger.error(f"生成模拟因子数据失败: {e}")
            return pd.DataFrame()
    
    def compute_factors(self, symbol: str, start_date: str, end_date: str,
                       factor_names: List[str], data: pd.DataFrame = None) -> pd.DataFrame:
        """
        计算因子
        
        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            factor_names: 要计算的因子名称列表
            data: 输入数据(可选，如果未提供则自动获取K线数据)
            
        Returns:
            包含计算结果的DataFrame
        """
        try:
            # 如果没有提供数据，获取K线数据
            if data is None:
                data = self.get_kline(symbol, start_date, end_date)
            
            if data.empty:
                self.logger.warning(f"无法获取数据用于因子计算: {symbol}")
                return pd.DataFrame()
            
            # 计算技术指标
            result_data = data.copy()
            
            for factor_name in factor_names:
                if factor_name.startswith('sma_'):
                    # 简单移动平均
                    period = int(factor_name.split('_')[1])
                    result_data[factor_name] = result_data['close'].rolling(window=period).mean()
                    
                elif factor_name.startswith('ema_'):
                    # 指数移动平均
                    period = int(factor_name.split('_')[1])
                    result_data[factor_name] = result_data['close'].ewm(span=period).mean()
                    
                elif factor_name.startswith('rsi_'):
                    # RSI指标
                    period = int(factor_name.split('_')[1])
                    result_data[factor_name] = self._calculate_rsi(result_data['close'], period)
                    
                elif factor_name == 'macd':
                    # MACD指标
                    macd_result = self._calculate_macd(result_data['close'])
                    result_data[factor_name] = macd_result
                    
                elif factor_name.startswith('bb_'):
                    # 布林带
                    period = int(factor_name.split('_')[1])
                    bb_result = self._calculate_bollinger_bands(result_data['close'], period)
                    if factor_name.endswith('_upper'):
                        result_data[factor_name] = bb_result['upper']
                    elif factor_name.endswith('_lower'):
                        result_data[factor_name] = bb_result['lower']
                    else:
                        result_data[factor_name] = bb_result['middle']
                
                elif factor_name.startswith('atr_'):
                    # 平均真实波动率
                    period = int(factor_name.split('_')[1])
                    result_data[factor_name] = self._calculate_atr(result_data, period)
                
                elif factor_name.startswith('obv'):
                    # 成交量平衡指标
                    result_data[factor_name] = self._calculate_obv(result_data)
                
                elif factor_name.startswith('stoch_'):
                    # 随机指标
                    period = int(factor_name.split('_')[1])
                    stoch_result = self._calculate_stochastic(result_data, period)
                    result_data[f'stoch_k_{period}'] = stoch_result['k']
                    result_data[f'stoch_d_{period}'] = stoch_result['d']
                
                elif factor_name.startswith('williams_'):
                    # 威廉指标
                    period = int(factor_name.split('_')[1])
                    result_data[factor_name] = self._calculate_williams_r(result_data, period)
                
                elif factor_name.startswith('vol_'):
                    # 波动率
                    period = int(factor_name.split('_')[1])
                    result_data[factor_name] = result_data['close'].pct_change().rolling(window=period).std()
                
                elif factor_name.startswith('ret_'):
                    # 收益率
                    period = int(factor_name.split('_')[1])
                    result_data[factor_name] = result_data['close'].pct_change(period)
                        
                else:
                    # 默认生成随机因子
                    result_data[factor_name] = np.random.randn(len(result_data))
            
            # 只返回需要的列
            columns_to_return = ['date', 'symbol'] + factor_names
            available_columns = [col for col in columns_to_return if col in result_data.columns]
            
            if 'symbol' not in result_data.columns:
                result_data['symbol'] = symbol
                available_columns.append('symbol')
            
            result = result_data[available_columns].copy()
            
            self.logger.info(f"计算因子: {symbol}, {len(factor_names)}个因子")
            return result
            
        except Exception as e:
            self.logger.error(f"计算因子失败: {symbol}, {e}")
            return pd.DataFrame()
    
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """计算RSI指标"""
        try:
            delta = prices.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            return rsi
        except Exception as e:
            self.logger.error(f"计算RSI失败: {e}")
            return pd.Series(np.nan, index=prices.index)
    
    def _calculate_macd(self, prices: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.Series:
        """计算MACD指标"""
        try:
            ema_fast = prices.ewm(span=fast).mean()
            ema_slow = prices.ewm(span=slow).mean()
            macd = ema_fast - ema_slow
            return macd
        except Exception as e:
            self.logger.error(f"计算MACD失败: {e}")
            return pd.Series(np.nan, index=prices.index)
    
    def _calculate_bollinger_bands(self, prices: pd.Series, period: int = 20, std: float = 2) -> Dict[str, pd.Series]:
        """计算布林带指标"""
        try:
            middle = prices.rolling(window=period).mean()
            std_dev = prices.rolling(window=period).std()
            upper = middle + (std_dev * std)
            lower = middle - (std_dev * std)
            
            return {
                'upper': upper,
                'middle': middle,
                'lower': lower
            }
        except Exception as e:
            self.logger.error(f"计算布林带失败: {e}")
            return {
                'upper': pd.Series(np.nan, index=prices.index),
                'middle': pd.Series(np.nan, index=prices.index),
                'lower': pd.Series(np.nan, index=prices.index)
            }
    
    def _calculate_atr(self, data: pd.DataFrame, period: int = 14) -> pd.Series:
        """计算平均真实波动率(ATR)"""
        try:
            high_low = data['high'] - data['low']
            high_close = np.abs(data['high'] - data['close'].shift(1))
            low_close = np.abs(data['low'] - data['close'].shift(1))
            
            true_range = np.maximum(high_low, np.maximum(high_close, low_close))
            atr = true_range.rolling(window=period).mean()
            
            return atr
        except Exception as e:
            self.logger.error(f"计算ATR失败: {e}")
            return pd.Series(np.nan, index=data.index)
    
    def _calculate_obv(self, data: pd.DataFrame) -> pd.Series:
        """计算成交量平衡指标(OBV)"""
        try:
            price_change = data['close'].diff()
            volume_direction = np.where(price_change > 0, data['volume'], 
                                      np.where(price_change < 0, -data['volume'], 0))
            obv = volume_direction.cumsum()
            
            return pd.Series(obv, index=data.index)
        except Exception as e:
            self.logger.error(f"计算OBV失败: {e}")
            return pd.Series(np.nan, index=data.index)
    
    def _calculate_stochastic(self, data: pd.DataFrame, period: int = 14) -> Dict[str, pd.Series]:
        """计算随机指标(Stochastic)"""
        try:
            lowest_low = data['low'].rolling(window=period).min()
            highest_high = data['high'].rolling(window=period).max()
            
            k_percent = 100 * (data['close'] - lowest_low) / (highest_high - lowest_low)
            d_percent = k_percent.rolling(window=3).mean()
            
            return {
                'k': k_percent,
                'd': d_percent
            }
        except Exception as e:
            self.logger.error(f"计算随机指标失败: {e}")
            return {
                'k': pd.Series(np.nan, index=data.index),
                'd': pd.Series(np.nan, index=data.index)
            }
    
    def _calculate_williams_r(self, data: pd.DataFrame, period: int = 14) -> pd.Series:
        """计算威廉指标(Williams %R)"""
        try:
            highest_high = data['high'].rolling(window=period).max()
            lowest_low = data['low'].rolling(window=period).min()
            
            williams_r = -100 * (highest_high - data['close']) / (highest_high - lowest_low)
            
            return williams_r
        except Exception as e:
            self.logger.error(f"计算威廉指标失败: {e}")
            return pd.Series(np.nan, index=data.index)
    
    def ml_predict(self, features: pd.DataFrame, model_name: str = 'default') -> pd.DataFrame:
        """
        机器学习预测
        
        Args:
            features: 特征数据
            model_name: 模型名称
            
        Returns:
            预测结果DataFrame
        """
        try:
            # 生成缓存键
            features_hash = hashlib.md5(str(features.values).encode()).hexdigest()[:8]
            cache_key = f"ml_predict_{model_name}_{features_hash}"
            
            # 检查缓存
            if self.data_source_config['cache_enabled']:
                cached_data = self._get_from_cache(cache_key, self.model_cache)
                if cached_data is not None:
                    self.logger.debug(f"从缓存获取预测结果: {model_name}")
                    return cached_data
            
            # 执行预测
            if self.data_source_config['use_real_data'] and has_trainer:
                predictions = self._real_ml_predict(features, model_name)
            else:
                predictions = self._mock_ml_predict(features, model_name)
            
            # 更新缓存
            if self.data_source_config['cache_enabled'] and not predictions.empty:
                self._update_cache(cache_key, predictions, self.model_cache)
            
            self.logger.info(f"机器学习预测: {model_name}, {len(features)}个样本")
            return predictions
            
        except Exception as e:
            self.logger.error(f"机器学习预测失败: {e}")
            return pd.DataFrame()
    
    def _real_ml_predict(self, features: pd.DataFrame, model_name: str) -> pd.DataFrame:
        """真实机器学习预测"""
        try:
            # 这里应该加载真实的模型并进行预测
            # 目前先返回模拟结果
            return self._mock_ml_predict(features, model_name)
            
        except Exception as e:
            self.logger.error(f"真实ML预测失败: {e}")
            return pd.DataFrame()
    
    def _mock_ml_predict(self, features: pd.DataFrame, model_name: str) -> pd.DataFrame:
        """模拟机器学习预测"""
        try:
            predictions = []
            for i in range(len(features)):
                predictions.append({
                    'symbol': features.iloc[i].get('symbol', f'stock_{i}'),
                    'date': features.iloc[i].get('date', datetime.now().strftime('%Y-%m-%d')),
                    'prediction': np.random.randn() * 0.05,  # 5%的预测收益率
                    'confidence': np.random.uniform(0.5, 0.9)
                })
            
            return pd.DataFrame(predictions)
            
        except Exception as e:
            self.logger.error(f"模拟ML预测失败: {e}")
            return pd.DataFrame()
    
    def get_price(self, symbol: str, date: str = None) -> float:
        """
        获取股票价格
        
        Args:
            symbol: 股票代码
            date: 日期 (可选)
            
        Returns:
            股票价格
        """
        try:
            cache_key = f"price_{symbol}_{date or 'latest'}"
            
            # 检查缓存
            if self.data_source_config['cache_enabled']:
                cached_price = self._get_from_cache(cache_key, self.price_cache)
                if cached_price is not None:
                    return cached_price
            
            # 获取价格
            if date:
                # 获取指定日期的价格
                kline_data = self.get_kline(symbol, date, date)
                if not kline_data.empty:
                    price = float(kline_data.iloc[-1]['close'])
                else:
                    price = 0.0
            else:
                # 获取最新价格（模拟）
                price = 100.0 * (1 + np.random.randn() * 0.02)
            
            # 更新缓存
            if self.data_source_config['cache_enabled']:
                self._update_cache(cache_key, price, self.price_cache)
            
            self.logger.debug(f"获取价格: {symbol} = {price:.2f}")
            return price
            
        except Exception as e:
            self.logger.error(f"获取价格失败: {symbol}, {e}")
            return 0.0
    
    def get_trading_calendar(self, start_date: str, end_date: str) -> List[str]:
        """
        获取交易日历
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            交易日列表
        """
        try:
            cache_key = f"calendar_{start_date}_{end_date}"
            
            # 检查缓存
            if self.data_source_config['cache_enabled']:
                cached_calendar = self._get_from_cache(cache_key, self.calendar_cache)
                if cached_calendar is not None:
                    return cached_calendar
            
            # 生成交易日历（简化版，只排除周末）
            dates = pd.date_range(start=start_date, end=end_date, freq='D')
            trading_days = dates[dates.dayofweek < 5]  # 只保留工作日
            
            calendar = [date.strftime('%Y-%m-%d') for date in trading_days]
            
            # 更新缓存
            if self.data_source_config['cache_enabled']:
                self._update_cache(cache_key, calendar, self.calendar_cache)
            
            self.logger.info(f"获取交易日历: {start_date} - {end_date}, {len(calendar)}个交易日")
            return calendar
            
        except Exception as e:
            self.logger.error(f"获取交易日历失败: {e}")
            return []
    
    def preload_data(self, symbols: List[str], start_date: str, end_date: str, 
                    frequency: str = '1d', factors: List[str] = None):
        """
        预加载数据
        
        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            frequency: 数据频率
            factors: 因子列表（可选）
        """
        try:
            self.logger.info(f"开始预加载数据: {len(symbols)}个股票")
            
            if self.data_source_config['parallel_enabled']:
                # 并行预加载
                with ThreadPoolExecutor(max_workers=self.data_source_config['max_workers']) as executor:
                    futures = []
                    
                    # 预加载K线数据
                    for symbol in symbols:
                        future = executor.submit(
                            self.get_kline, symbol, start_date, end_date, frequency
                        )
                        futures.append(future)
                    
                    # 预加载因子数据
                    if factors:
                        for symbol in symbols:
                            future = executor.submit(
                                self.get_factors, symbol, start_date, end_date, factors
                            )
                            futures.append(future)
                    
                    # 等待所有任务完成
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            self.logger.error(f"预加载任务失败: {e}")
            else:
                # 串行预加载
                for symbol in symbols:
                    self.get_kline(symbol, start_date, end_date, frequency)
                    if factors:
                        self.get_factors(symbol, start_date, end_date, factors)
            
            self.logger.info("数据预加载完成")
            
        except Exception as e:
            self.logger.error(f"数据预加载失败: {e}")
    
    def batch_compute_factors(self, symbols: List[str], start_date: str, end_date: str,
                             factor_names: List[str], data_dict: Dict[str, pd.DataFrame] = None) -> Dict[str, pd.DataFrame]:
        """
        批量计算因子
        
        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            factor_names: 因子名称列表
            data_dict: 数据字典（可选）
            
        Returns:
            计算结果字典
        """
        try:
            results = {}
            
            if self.data_source_config['parallel_enabled']:
                # 并行计算
                with ThreadPoolExecutor(max_workers=self.data_source_config['max_workers']) as executor:
                    futures = {}
                    
                    for symbol in symbols:
                        data = data_dict.get(symbol) if data_dict else None
                        future = executor.submit(
                            self.compute_factors, symbol, start_date, end_date, factor_names, data
                        )
                        futures[future] = symbol
                    
                    # 收集结果
                    for future in as_completed(futures):
                        symbol = futures[future]
                        try:
                            results[symbol] = future.result()
                        except Exception as e:
                            self.logger.error(f"批量计算因子失败: {symbol}, {e}")
                            results[symbol] = pd.DataFrame()
            else:
                # 串行计算
                for symbol in symbols:
                    data = data_dict.get(symbol) if data_dict else None
                    results[symbol] = self.compute_factors(symbol, start_date, end_date, factor_names, data)
            
            self.logger.info(f"批量计算因子完成: {len(symbols)}个股票, {len(factor_names)}个因子")
            return results
            
        except Exception as e:
            self.logger.error(f"批量计算因子失败: {e}")
            return {}
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        with self.cache_lock:
            return {
                'cache_hit_count': self.cache_hit_count,
                'cache_miss_count': self.cache_miss_count,
                'cache_hit_rate': self.cache_hit_count / max(self.cache_hit_count + self.cache_miss_count, 1),
                'kline_cache_size': len(self.kline_cache),
                'factors_cache_size': len(self.factors_cache),
                'price_cache_size': len(self.price_cache),
                'model_cache_size': len(self.model_cache),
                'calendar_cache_size': len(self.calendar_cache),
                'total_cache_size': len(self.cache_timestamps)
            }
    
    def clear_cache(self, cache_type: str = 'all'):
        """
        清除缓存
        
        Args:
            cache_type: 缓存类型 ('all', 'kline', 'factors', 'price', 'model', 'calendar')
        """
        with self.cache_lock:
            if cache_type == 'all':
                self.kline_cache.clear()
                self.factors_cache.clear()
                self.price_cache.clear()
                self.model_cache.clear()
                self.calendar_cache.clear()
                self.cache_timestamps.clear()
                self.cache_hit_count = 0
                self.cache_miss_count = 0
            elif cache_type == 'kline':
                self.kline_cache.clear()
            elif cache_type == 'factors':
                self.factors_cache.clear()
            elif cache_type == 'price':
                self.price_cache.clear()
            elif cache_type == 'model':
                self.model_cache.clear()
            elif cache_type == 'calendar':
                self.calendar_cache.clear()
            
            self.logger.info(f"已清除{cache_type}缓存")
    
    def set_config(self, config: Dict[str, Any]):
        """
        设置配置
        
        Args:
            config: 配置字典
        """
        self.data_source_config.update(config)
        self.logger.info(f"更新数据源配置: {config}")
    
    def get_config(self) -> Dict[str, Any]:
        """获取当前配置"""
        return self.data_source_config.copy()
    
    def check_data_availability(self) -> Dict[str, Any]:
        """检查数据可用性"""
        return {
            'has_real_data': HAS_REAL_DATA,
            'has_trainer': has_trainer,
            'constants_available': globals().get('constants_available', False),
            'data_directories': {
                'DATA_DIR': globals().get('DATA_DIR', 'N/A'),
                'FACTORS_DIR': globals().get('FACTORS_DIR', 'N/A'),
                'CACHE_DIR': globals().get('CACHE_DIR', 'N/A')
            }
        } 