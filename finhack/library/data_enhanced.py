"""
统一数据接口 - 增强版

提供整个项目的数据访问能力，包括K线数据、因子数据、参考数据等
支持高性能缓存、多线程处理、批量操作等优化功能
专门优化cn_fund、cn_future、cn_stock、global_cryptoswap、global_cryptospot市场支持
"""

import os
import pickle
import logging
import threading
import hashlib
from abc import ABC, abstractmethod
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple
from enum import Enum
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed, wait, ALL_COMPLETED
import time

import pandas as pd
import numpy as np

from runtime.constant import *

logger = logging.getLogger(__name__)


class DataType(Enum):
    """数据类型枚举"""
    KLINE = "kline"
    FACTOR_MATRIX = "factor_matrix"
    FACTOR_VECTOR = "factor_vector"
    REFERENCE = "reference"


class StorageFormat(Enum):
    """存储格式枚举"""
    CODE_BASED = "codebased"
    TIME_BASED = "timebased"
    MATRIX = "matrix"
    VECTOR = "vector"


class CacheStrategy(Enum):
    """缓存策略枚举"""
    LRU = "lru"
    TIME_EXPIRE = "time_expire"
    SIZE_LIMIT = "size_limit"


class LRUCache:
    """LRU缓存实现"""
    
    def __init__(self, max_size: int = 1000, ttl_seconds: int = 3600):
        """
        Args:
            max_size: 最大缓存条目数
            ttl_seconds: 缓存存活时间（秒）
        """
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.cache = OrderedDict()
        self.timestamps = {}
        self.lock = threading.RLock()
    
    def get(self, key: str) -> Any:
        """获取缓存值"""
        with self.lock:
            if key not in self.cache:
                return None
            
            # 检查是否过期
            if self._is_expired(key):
                self._remove(key)
                return None
            
            # 移动到末尾（最近使用）
            value = self.cache.pop(key)
            self.cache[key] = value
            return value
    
    def put(self, key: str, value: Any) -> None:
        """设置缓存值"""
        with self.lock:
            if key in self.cache:
                self.cache.pop(key)
            elif len(self.cache) >= self.max_size:
                # 删除最旧的条目
                oldest_key = next(iter(self.cache))
                self._remove(oldest_key)
            
            self.cache[key] = value
            self.timestamps[key] = time.time()
    
    def remove(self, key: str) -> bool:
        """删除缓存条目"""
        with self.lock:
            return self._remove(key)
    
    def clear(self) -> None:
        """清空缓存"""
        with self.lock:
            self.cache.clear()
            self.timestamps.clear()
    
    def size(self) -> int:
        """获取缓存大小"""
        return len(self.cache)
    
    def _remove(self, key: str) -> bool:
        """内部删除方法"""
        removed = False
        if key in self.cache:
            self.cache.pop(key)
            removed = True
        if key in self.timestamps:
            self.timestamps.pop(key)
        return removed
    
    def _is_expired(self, key: str) -> bool:
        """检查是否过期"""
        if key not in self.timestamps:
            return True
        return time.time() - self.timestamps[key] > self.ttl_seconds
    
    def cleanup_expired(self) -> int:
        """清理过期条目"""
        with self.lock:
            expired_keys = []
            for key in list(self.cache.keys()):
                if self._is_expired(key):
                    expired_keys.append(key)
            
            for key in expired_keys:
                self._remove(key)
            
            return len(expired_keys)


class MarketConfig:
    """市场配置类，定义各市场类型的特性"""
    
    # 支持的市场类型和频率组合
    MARKET_FREQ_SUPPORT = {
        'cn_stock': ['1d', '1m'],
        'cn_fund': ['1d', '1m'],
        'cn_future': ['1m'],  # 只有1m数据
        'cn_index': ['1d', '1m'],
        'cn_cb': ['1d', '1m'],
        'global_cryptospot': ['1m'],  # 只有1m数据
        'global_cryptoswap': ['1m'],  # 只有1m数据
        'global_fx': ['1d'],
        'hk_stock': ['1d'],
    }
    
    # 支持复权的市场
    ADJ_SUPPORTED_MARKETS = ['cn_stock', 'cn_fund']
    
    # 7x24交易的市场（无节假日）
    CONTINUOUS_MARKETS = ['global_cryptospot', 'global_cryptoswap']
    
    @classmethod
    def is_freq_supported(cls, market: str, freq: str) -> bool:
        """检查市场是否支持指定频率"""
        return market in cls.MARKET_FREQ_SUPPORT and freq in cls.MARKET_FREQ_SUPPORT[market]
    
    @classmethod
    def supports_adjustment(cls, market: str) -> bool:
        """检查市场是否支持复权"""
        return market in cls.ADJ_SUPPORTED_MARKETS
    
    @classmethod
    def is_continuous_market(cls, market: str) -> bool:
        """检查是否为连续交易市场"""
        return market in cls.CONTINUOUS_MARKETS


class DataInterface:
    """统一数据接口 - 增强版"""
    
    def __init__(self, project_path: str, cache_config: Dict[str, Any] = None):
        """
        初始化数据接口
        
        Args:
            project_path: 项目路径
            cache_config: 缓存配置
        """
        self.project_path = project_path
        self.data_dir = os.path.join(project_path, 'data')
        
        # 数据路径配置
        self.market_data_dir = os.path.join(self.data_dir, 'market')
        self.factors_data_dir = os.path.join(self.data_dir, 'factors')
        self.reference_data_dir = os.path.join(self.market_data_dir, 'reference')
        
        # 缓存配置
        default_cache_config = {
            'max_size': 2000,  # 增加缓存大小
            'ttl_seconds': 7200,  # 增加缓存时间
            'cleanup_interval': 300  # 5分钟清理一次过期缓存
        }
        self.cache_config = {**default_cache_config, **(cache_config or {})}
        
        # 初始化缓存
        self._init_caches()
        
        # 线程池
        self.thread_pool = ThreadPoolExecutor(max_workers=12)  # 增加线程数
        
        # 启动缓存清理任务
        self._start_cache_cleanup()
        
        logger.info(f"数据接口初始化完成: {project_path}")
    
    def _init_caches(self):
        """初始化各类缓存"""
        cache_size = self.cache_config['max_size']
        cache_ttl = self.cache_config['ttl_seconds']
        
        self.kline_cache = LRUCache(cache_size, cache_ttl)
        self.factor_cache = LRUCache(cache_size, cache_ttl)
        self.reference_cache = LRUCache(cache_size, cache_ttl)
        self.calendar_cache = LRUCache(cache_size, cache_ttl)
        
        # 元数据缓存（股票列表等）
        self.metadata_cache = LRUCache(cache_size // 10, cache_ttl * 10)
    
    def _start_cache_cleanup(self):
        """启动缓存清理任务"""
        def cleanup_task():
            while True:
                try:
                    time.sleep(self.cache_config['cleanup_interval'])
                    
                    total_cleaned = 0
                    total_cleaned += self.kline_cache.cleanup_expired()
                    total_cleaned += self.factor_cache.cleanup_expired()
                    total_cleaned += self.reference_cache.cleanup_expired()
                    total_cleaned += self.calendar_cache.cleanup_expired()
                    total_cleaned += self.metadata_cache.cleanup_expired()
                    
                    if total_cleaned > 0:
                        logger.debug(f"清理过期缓存条目: {total_cleaned}")
                        
                except Exception as e:
                    logger.error(f"缓存清理任务异常: {e}")
        
        cleanup_thread = threading.Thread(target=cleanup_task, daemon=True)
        cleanup_thread.start()
    
    def _generate_cache_key(self, *args) -> str:
        """生成缓存键"""
        key_str = "|".join(str(arg) for arg in args)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    # ==================== K线数据接口 ====================
    
    def get_klines(self, codes: Union[str, List[str]], market: str = 'cn_stock', 
                   freq: str = '1d', start_date: Union[str, datetime] = None,
                   end_date: Union[str, datetime] = None, 
                   fields: List[str] = None,
                   adj_type: str = 'none',
                   use_cache: bool = True) -> pd.DataFrame:
        """
        获取K线数据
        
        Args:
            codes: 股票代码或代码列表
            market: 市场名称 (cn_stock, cn_fund, cn_future, global_cryptospot, global_cryptoswap等)
            freq: 数据频率 (1d, 1m)
            start_date: 开始时间
            end_date: 结束时间
            fields: 需要的字段列表
            adj_type: 复权类型 ('none': 不复权, 'front': 前复权, 'back': 后复权)
            use_cache: 是否使用缓存
            
        Returns:
            pd.DataFrame: K线数据，MultiIndex(time, symbol)
        """
        # 验证市场和频率支持
        if not MarketConfig.is_freq_supported(market, freq):
            logger.warning(f"市场 {market} 不支持频率 {freq}")
            index = pd.MultiIndex.from_arrays([[], []], names=['time', 'symbol'])
            return pd.DataFrame(columns=fields or ['open', 'high', 'low', 'close', 'volume'], index=index)
        
        if isinstance(codes, str):
            codes = [codes]
            
        if fields is None:
            fields = ['open', 'high', 'low', 'close', 'volume']
        
        # 生成缓存键
        cache_key = self._generate_cache_key(
            'klines', tuple(sorted(codes)), market, freq, 
            str(start_date), str(end_date), tuple(sorted(fields)), adj_type
        )
        
        # 尝试从缓存获取
        if use_cache:
            cached_result = self.kline_cache.get(cache_key)
            if cached_result is not None:
                logger.debug(f"从缓存获取K线数据: {len(codes)} codes")
                return cached_result
        
        # 统一处理时间格式
        start_date, end_date = self._normalize_dates(start_date, end_date)
        
        # 并行获取每个股票的数据
        futures = []
        for code in codes:
            future = self.thread_pool.submit(
                self._load_single_kline, market, code, freq, start_date, end_date
            )
            futures.append((code, future))
        
        # 收集结果
        all_data = []
        for code, future in futures:
            try:
                klines = future.result()
                if not klines.empty:
                    klines['symbol'] = code
                    all_data.append(klines)
            except Exception as e:
                logger.warning(f"获取{code}的K线数据失败: {e}")
                continue
        
        # 合并数据
        if all_data:
            result_df = pd.concat(all_data, ignore_index=True)
            result_df.set_index(['time', 'symbol'], inplace=True)
            result_df = result_df[fields]
            
            # 应用复权逻辑
            if adj_type != 'none' and MarketConfig.supports_adjustment(market) and self._has_price_fields(fields):
                result_df = self._apply_adjustment(result_df, market, adj_type, start_date, end_date)
            
            # 缓存结果
            if use_cache:
                self.kline_cache.put(cache_key, result_df)
                
            logger.info(f"成功获取K线数据: {len(codes)} codes, {len(result_df)} 条记录")
            return result_df
        else:
            # 返回空DataFrame但保持正确的结构
            index = pd.MultiIndex.from_arrays([[], []], names=['time', 'symbol'])
            return pd.DataFrame(columns=fields, index=index)
    
    def _load_single_kline(self, market: str, symbol: str, freq: str,
                          start_date: str, end_date: str) -> pd.DataFrame:
        """加载单个代码的K线数据，支持多种市场类型"""
        # 确定数据文件路径（支持codebased存储格式）
        kline_dir = os.path.join(self.market_data_dir, 'kline')
        codebased_dir = os.path.join(kline_dir, 'codebased', market, freq)
        timebased_dir = os.path.join(kline_dir, 'timebased', market, freq)
        
        # 获取年份范围
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        
        all_data = []
        
        # 首先尝试codebased格式
        codebased_found = False
        for year in range(start_year, end_year + 1):
            year_dir = os.path.join(codebased_dir, str(year))
            symbol_file = os.path.join(year_dir, f"{symbol}.csv")
            
            if os.path.exists(symbol_file):
                try:
                    # 读取CSV文件（无header）
                    year_data = pd.read_csv(symbol_file, header=None, 
                                          names=['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                    
                    # 转换时间格式，统一处理时区
                    year_data['time'] = pd.to_datetime(year_data['time'], utc=False)
                    if hasattr(year_data['time'].dt, 'tz') and year_data['time'].dt.tz is not None:
                        year_data['time'] = year_data['time'].dt.tz_localize(None)
            
                    # 过滤日期范围
                    start_dt = pd.to_datetime(start_date)
                    end_dt = pd.to_datetime(end_date)
                    
                    if hasattr(start_dt, 'tz') and start_dt.tz is not None:
                        start_dt = start_dt.tz_localize(None)
                    if hasattr(end_dt, 'tz') and end_dt.tz is not None:
                        end_dt = end_dt.tz_localize(None)
                    
                    year_data = year_data[(year_data['time'] >= start_dt) & (year_data['time'] <= end_dt)]
                    
                    if not year_data.empty:
                        all_data.append(year_data)
                        codebased_found = True
                        
                except Exception as e:
                    logger.warning(f"读取{symbol}的{year}年codebased数据失败: {e}")
                    continue
        
        # 如果codebased没有找到数据，尝试timebased格式或loadKline
        if not codebased_found and os.path.exists(timebased_dir):
            logger.debug(f"未找到{symbol}的codebased数据，尝试timebased格式")
            try:
                from finhack.library.kline import loadKline
                # 使用现有的loadKline函数加载数据
                kline_data = loadKline(
                    market=market,
                    freq=freq,
                    start_date=start_date.replace('-', ''),
                    end_date=end_date.replace('-', ''),
                    code_list=[symbol],
                    cache=False
                )
                
                if not kline_data.empty and symbol in kline_data['code'].values:
                    symbol_data = kline_data[kline_data['code'] == symbol].copy()
                    if not symbol_data.empty:
                        # 重新格式化时间列
                        symbol_data['time'] = pd.to_datetime(symbol_data['time'], utc=False, errors='coerce')
                        if hasattr(symbol_data['time'].dt, 'tz') and symbol_data['time'].dt.tz is not None:
                            symbol_data['time'] = symbol_data['time'].dt.tz_localize(None)
                        
                        # 确保列名一致
                        symbol_data = symbol_data[['time', 'open', 'high', 'low', 'close', 'volume', 'amount']].copy()
                        all_data.append(symbol_data)
                        
            except Exception as e:
                logger.warning(f"使用timebased格式加载{symbol}数据失败: {e}")
        
        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            result = result.sort_values('time').reset_index(drop=True)
            # 去除重复数据
            result = result.drop_duplicates(subset=['time'], keep='last')
            # 确保列名正确
            expected_columns = ['time', 'open', 'high', 'low', 'close', 'volume', 'amount']
            result = result.reindex(columns=expected_columns)
            return result
        else:
            return pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume', 'amount'])
    
    def get_quotes(self, codes: Union[str, List[str]], market: str = 'cn_stock',
                   freq: str = '1d', time: datetime = None, 
                   fields: List[str] = None,
                   adj_type: str = 'none',
                   use_cache: bool = True) -> pd.DataFrame:
        """
        获取指定时间点的行情数据
        
        Args:
            codes: 股票代码或代码列表
            market: 市场名称
            freq: 数据频率
            time: 查询时间点
            fields: 需要的字段列表
            adj_type: 复权类型 ('none': 不复权, 'front': 前复权, 'back': 后复权)
            use_cache: 是否使用缓存
            
        Returns:
            pd.DataFrame: 行情数据，index为symbol，columns为fields
            
        Note:
            当指定时间点没有数据时，会自动使用最近的历史交易日数据
        """
        # 验证市场和频率支持
        if not MarketConfig.is_freq_supported(market, freq):
            logger.warning(f"市场 {market} 不支持频率 {freq}")
            return pd.DataFrame(columns=fields or ['open', 'high', 'low', 'close', 'volume'], 
                              index=pd.Index([], name='symbol'))
        
        if isinstance(codes, str):
            codes = [codes]
        
        if time is None:
            time = datetime.now()
        
        if fields is None:
            fields = ['open', 'high', 'low', 'close', 'volume']
        
        # 修复时间比较问题：如果查询时间是日期开始时间(00:00:00)，
        # 调整为当天结束时间以包含当天的交易数据
        if time.hour == 0 and time.minute == 0 and time.second == 0:
            time = time.replace(hour=23, minute=59, second=59)
        
        # 生成缓存键
        cache_key = self._generate_cache_key(
            'quotes', tuple(sorted(codes)), market, freq, str(time), tuple(sorted(fields)), adj_type
        )
        
        # 尝试从缓存获取
        if use_cache:
            cached_result = self.kline_cache.get(cache_key)
            if cached_result is not None:
                logger.debug(f"从缓存获取行情数据: {len(codes)} codes")
                return cached_result
        
        # 确保时间是naive datetime便于比较
        if hasattr(time, 'tz') and time.tz is not None:
            time = time.replace(tzinfo=None)
        
        result_data = []
        
        for code in codes:
            # 获取该股票的K线数据（扩展历史范围以确保找到数据）
            end_date = time.strftime('%Y-%m-%d')
            
            # 根据市场类型调整查找范围
            if MarketConfig.is_continuous_market(market):
                # 加密货币市场7x24交易，缩短查找范围
                lookback_days = 30
            else:
                # 传统市场需要考虑更长的假期
                lookback_days = 90
                
            start_date = (time - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
            
            try:
                klines = self._load_single_kline(market, code, freq, start_date, end_date)
                
                if klines.empty:
                    logger.debug(f"未找到{code}的K线数据")
                    continue
                
                # 查找指定时间点的数据（使用小于等于的最近时间）
                klines_before = klines[klines['time'] <= time]
                
                if not klines_before.empty:
                    latest_data = klines_before.iloc[-1]
                    logger.debug(f"{code}: 查询时间{time}, 使用时间{latest_data['time']}")
                    
                    row_data = {'symbol': code}
                    for field in fields:
                        if field in latest_data:
                            row_data[field] = latest_data[field]
                        else:
                            row_data[field] = np.nan
                    
                    result_data.append(row_data)
                else:
                    logger.debug(f"{code}: 在指定时间{time}之前未找到历史数据")
                    
            except Exception as e:
                logger.warning(f"获取{code}行情数据失败: {e}")
                continue
        
        if result_data:
            result_df = pd.DataFrame(result_data)
            result_df.set_index('symbol', inplace=True)
            result_df = result_df[fields]
            
            # 应用复权逻辑（将DataFrame转换为与get_klines相同的格式）
            if adj_type != 'none' and MarketConfig.supports_adjustment(market) and self._has_price_fields(fields):
                # 为行情快照数据添加时间列以便复权处理
                temp_df = result_df.copy()
                temp_df['time'] = time  # 添加查询时间
                temp_df = temp_df.reset_index().set_index(['time', 'symbol'])
                
                # 应用复权
                # 修复：使用正确的日期范围，不应该减去1天
                # 对于行情快照，应该使用当天的复权因子
                temp_df = self._apply_adjustment(temp_df, market, adj_type, 
                                               time.strftime('%Y-%m-%d'), 
                                               time.strftime('%Y-%m-%d'))
                
                # 转换回原始格式
                result_df = temp_df.reset_index(level=0, drop=True)  # 去掉time索引
            
            # 缓存结果
            if use_cache:
                self.kline_cache.put(cache_key, result_df)
            
            return result_df
        else:
            # 返回空DataFrame但保持正确的列结构
            return pd.DataFrame(columns=fields, index=pd.Index([], name='symbol'))
    
    # ==================== 因子数据接口 ====================
    
    def get_factors(self, factor_names: Union[str, List[str]], 
                   codes: Union[str, List[str]] = None,
                   market: str = 'cn_stock', freq: str = '1d',
                   start_date: Union[str, datetime] = None, 
                   end_date: Union[str, datetime] = None,
                   factor_type: str = 'matrix',
                   use_cache: bool = True) -> pd.DataFrame:
        """
        获取因子数据
        
        Args:
            factor_names: 因子名称或名称列表
            codes: 股票代码列表，如果为None则获取所有
            market: 市场名称 (目前主要支持cn_stock)
            freq: 数据频率
            start_date: 开始日期
            end_date: 结束日期
            factor_type: 因子类型 ('matrix' 或 'vector')
            use_cache: 是否使用缓存
        
        Returns:
            pd.DataFrame: 因子数据，MultiIndex(date, symbol)
        """
        if isinstance(factor_names, str):
            factor_names = [factor_names]
        
        if isinstance(codes, str):
            codes = [codes]
        
        # 生成缓存键
        cache_key = self._generate_cache_key(
            'factors', tuple(sorted(factor_names)), 
            tuple(sorted(codes)) if codes else None,
            market, freq, str(start_date), str(end_date), factor_type
        )
        
        # 尝试从缓存获取
        if use_cache:
            cached_result = self.factor_cache.get(cache_key)
            if cached_result is not None:
                logger.debug(f"从缓存获取因子数据: {len(factor_names)} factors")
                return cached_result
        
        if factor_type == 'matrix':
            result_df = self._load_matrix_factors(
                factor_names, codes, market, freq, start_date, end_date
            )
        elif factor_type == 'vector':
            result_df = self._load_vector_factors(
                factor_names, codes, market, freq, start_date, end_date
            )
        else:
            raise ValueError(f"不支持的因子类型: {factor_type}")
        
        # 缓存结果
        if use_cache and not result_df.empty:
            self.factor_cache.put(cache_key, result_df)
        
        return result_df
    
    def _load_matrix_factors(self, factor_names: List[str], codes: List[str],
                           market: str, freq: str, start_date: str, end_date: str) -> pd.DataFrame:
        """加载matrix格式因子数据"""
        # 统一处理时间格式
        start_date, end_date = self._normalize_dates(start_date, end_date)
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        
        # 获取年份范围
        start_year = start_dt.year
        end_year = end_dt.year

        all_factor_data = []

        # 确定要加载的股票代码
        target_codes = codes
        if target_codes is None:
            stock_list_df = self.get_stock_list(market=market)
            if not stock_list_df.empty:
                target_codes = stock_list_df['code'].tolist()
            else:
                logger.warning(f"无法获取市场 {market} 的股票列表，因子加载受限")
                return pd.DataFrame()

        # 按年份和股票代码加载数据
        for year in range(start_year, end_year + 1):
            factor_base_path = os.path.join(self.factors_data_dir, 'matrix', market, freq, str(year))
            if not os.path.exists(factor_base_path):
                continue

            for code in target_codes:
                stock_path = os.path.join(factor_base_path, code)
                if not os.path.exists(stock_path):
                    continue
                
                for factor_name in factor_names:
                    # 尝试不同的因子文件命名格式
                    factor_files = [
                        os.path.join(stock_path, f"{factor_name}.pkl"),
                        os.path.join(stock_path, f"{factor_name}_0.pkl")
                    ]
                    
                    factor_file = None
                    for potential_file in factor_files:
                        if os.path.exists(potential_file):
                            factor_file = potential_file
                            break
                    
                    if factor_file is None:
                        logger.debug(f"Factor file not found for {factor_name} in {stock_path}")
                        continue

                    try:
                        with open(factor_file, 'rb') as f:
                            factor_series = pickle.load(f)
                        
                        if isinstance(factor_series, pd.Series) and not factor_series.empty:
                            logger.debug(f"Loaded factor {factor_name} for {code} in {year}, size: {len(factor_series)}")
                            
                            # 处理时区信息 - 保持本地时间不变，只移除时区信息
                            if hasattr(factor_series.index, 'tz') and factor_series.index.tz is not None:
                                # 直接移除时区信息，保持本地时间不变
                                factor_series.index = factor_series.index.tz_localize(None)
                            else:
                                factor_series.index = pd.to_datetime(factor_series.index)
                            
                            # 按日期过滤
                            filtered_series = factor_series[(factor_series.index >= start_dt) & (factor_series.index <= end_dt)]
                            
                            if not filtered_series.empty:
                                logger.debug(f"Filtered series for {factor_name} has {len(filtered_series)} records.")
                                df = filtered_series.to_frame(name=factor_name)
                                df['symbol'] = code
                                df = df.reset_index().set_index(['time', 'symbol'])
                                all_factor_data.append(df)

                    except Exception as e:
                        logger.warning(f"读取或处理因子文件失败 {factor_file}: {e}")

        if not all_factor_data:
            logger.debug("No factor data loaded.")
            index = pd.MultiIndex.from_arrays([[], []], names=['time', 'symbol'])
            return pd.DataFrame(columns=factor_names, index=index)
        
        # 合并所有DataFrame
        logger.debug(f"Concatenating {len(all_factor_data)} factor dataframes.")
        
        # 使用concat合并所有数据
        result_df = pd.concat(all_factor_data, axis=0, sort=False)
        
        # 如果有重复的索引，进行分组聚合（取平均值或最后一个值）
        if result_df.index.duplicated().any():
            logger.debug("Found duplicated index, aggregating...")
            result_df = result_df.groupby(level=[0, 1]).last()
        
        # 确保所有请求的列都存在
        existing_cols = [col for col in factor_names if col in result_df.columns]
        missing_cols = set(factor_names) - set(existing_cols)
        
        for col in missing_cols:
            result_df[col] = np.nan
        
        # 按时间和代码排序
        result_df = result_df.sort_index()
        
        logger.info(f"Successfully loaded factors: {existing_cols}, missing: {list(missing_cols)}")
        return result_df[factor_names]
    
    def _load_vector_factors(self, factor_names: List[str], codes: List[str],
                           market: str, freq: str, start_date: str, end_date: str) -> pd.DataFrame:
        """加载vector格式因子数据"""
        base_vector_path = os.path.join(self.factors_data_dir, 'vector', market, freq)
        
        all_factors = []
        
        for factor_name in factor_names:
            vector_path = os.path.join(base_vector_path, f"{factor_name}.pkl")
            
            if os.path.exists(vector_path):
                try:
                    vector_df = pd.read_pickle(vector_path)
                    
                    # 确保time列为字符串格式
                    if 'time' in vector_df.columns:
                        vector_df['time'] = vector_df['time'].astype(str)
                        
                    # 根据日期范围过滤
                    if start_date and end_date and 'time' in vector_df.columns:
                        start_date_str = start_date.replace('-', '')
                        end_date_str = end_date.replace('-', '')
                        vector_df = vector_df[(vector_df['time'] >= start_date_str) & 
                                            (vector_df['time'] <= end_date_str)]
                    
                    # 如果codes不为空，则筛选
                    if codes and 'code' in vector_df.columns:
                        vector_df = vector_df[vector_df['code'].isin(codes)]
                    
                    # 重命名因子列
                    if factor_name in vector_df.columns:
                        vector_df = vector_df[['code', 'time', factor_name]]
                        all_factors.append(vector_df)
                        
                except Exception as e:
                    logger.error(f"加载向量因子{factor_name}失败: {e}")
                    continue
        
        if not all_factors:
            index = pd.MultiIndex.from_arrays([[], []], names=['date', 'symbol'])
            return pd.DataFrame(columns=factor_names, index=index)
        
        # 合并所有因子
        result_df = all_factors[0]
        for factor_df in all_factors[1:]:
            result_df = result_df.merge(factor_df, on=['code', 'time'], how='outer')
        
        # 设置索引
        if 'code' in result_df.columns and 'time' in result_df.columns:
            result_df['time'] = pd.to_datetime(result_df['time'])
            result_df = result_df.set_index(['time', 'code'])
            result_df.index.names = ['date', 'symbol']
        
        return result_df
    
    # ==================== 参考数据接口 ====================
    
    def get_stock_list(self, market: str = 'cn_stock', use_cache: bool = True) -> pd.DataFrame:
        """获取股票列表"""
        cache_key = f"stock_list_{market}"
        
        if use_cache:
            cached_result = self.metadata_cache.get(cache_key)
            if cached_result is not None:
                return cached_result
        
        list_file = os.path.join(self.reference_data_dir, market, f"{market}_list.csv")
        
        if os.path.exists(list_file):
            stock_list = pd.read_csv(list_file)
            
            if use_cache:
                self.metadata_cache.put(cache_key, stock_list)
            
            return stock_list
        else:
            logger.warning(f"股票列表文件不存在: {list_file}")
            return pd.DataFrame()
    
    def get_adj_factors(self, market: str = 'cn_stock', codes: List[str] = None,
                       start_date: str = None, end_date: str = None,
                       use_cache: bool = True) -> pd.DataFrame:
        """获取复权因子"""
        if not MarketConfig.supports_adjustment(market):
            logger.debug(f"市场 {market} 不支持复权")
            return pd.DataFrame()
        
        cache_key = self._generate_cache_key('adj_factors', market, tuple(sorted(codes)) if codes else None, start_date, end_date)
        
        if use_cache:
            cached_result = self.reference_cache.get(cache_key)
            if cached_result is not None:
                return cached_result
        
        adj_file = os.path.join(self.reference_data_dir, market, f"{market}_adj.csv")
        
        if os.path.exists(adj_file):
            adj_factors = pd.read_csv(adj_file)
            
            # 过滤条件
            if codes:
                adj_factors = adj_factors[adj_factors['code'].isin(codes)]
            
            if start_date:
                start_date_int = int(start_date.replace('-', ''))
                adj_factors = adj_factors[adj_factors['date'] >= start_date_int]
            
            if end_date:
                end_date_int = int(end_date.replace('-', ''))
                adj_factors = adj_factors[adj_factors['date'] <= end_date_int]
            
            if use_cache:
                self.reference_cache.put(cache_key, adj_factors)
            
            return adj_factors
        else:
            logger.warning(f"复权因子文件不存在: {adj_file}")
            return pd.DataFrame()
    
    def get_trading_calendar(self, market: str = 'cn_stock', 
                           start_date: Union[str, date] = None,
                           end_date: Union[str, date] = None,
                           use_cache: bool = True) -> List[date]:
        """获取交易日历"""
        cache_key = self._generate_cache_key('calendar', market, str(start_date), str(end_date))
        
        if use_cache:
            cached_result = self.calendar_cache.get(cache_key)
            if cached_result is not None:
                return cached_result
        
        calendar_file = os.path.join(self.reference_data_dir, market, f"{market}_calendar.csv")
        
        if not os.path.exists(calendar_file):
            logger.warning(f"交易日历文件不存在: {calendar_file}")
            # 生成默认交易日历
            if MarketConfig.is_continuous_market(market):
                # 连续市场按天生成
                if isinstance(start_date, str):
                    start_dt = pd.to_datetime(start_date)
                else:
                    start_dt = pd.Timestamp(start_date)
                    
                if isinstance(end_date, str):
                    end_dt = pd.to_datetime(end_date)
                else:
                    end_dt = pd.Timestamp(end_date)
                
                date_range = pd.date_range(start_dt, end_dt, freq='D')
                result_dates = [d.date() for d in date_range]
            else:
                # 传统市场排除周末
                if isinstance(start_date, str):
                    start_dt = pd.to_datetime(start_date)
                else:
                    start_dt = pd.Timestamp(start_date)
                    
                if isinstance(end_date, str):
                    end_dt = pd.to_datetime(end_date)
                else:
                    end_dt = pd.Timestamp(end_date)
                
                date_range = pd.date_range(start_dt, end_dt, freq='D')
                trading_days = date_range[date_range.weekday < 5]
                result_dates = [d.date() for d in trading_days]
        else:
            calendar_df = pd.read_csv(calendar_file)
            
            # 过滤条件
            if start_date or end_date:
                if isinstance(start_date, (str, date)):
                    start_date_int = int(str(start_date).replace('-', ''))
                if isinstance(end_date, (str, date)):
                    end_date_int = int(str(end_date).replace('-', ''))
                
                calendar_df['cal_date'] = calendar_df['cal_date'].astype(str)
                
                if start_date:
                    calendar_df = calendar_df[calendar_df['cal_date'].astype(int) >= start_date_int]
                if end_date:
                    calendar_df = calendar_df[calendar_df['cal_date'].astype(int) <= end_date_int]
                
                calendar_df = calendar_df[calendar_df['is_open'] == 1]
            
            # 转换为date对象
            result_dates = []
            for date_str in calendar_df['cal_date'].tolist():
                if len(str(date_str)) == 8:  # YYYYMMDD
                    trade_date = datetime.strptime(str(date_str), '%Y%m%d').date()
                else:  # YYYY-MM-DD
                    trade_date = datetime.strptime(str(date_str), '%Y-%m-%d').date()
                result_dates.append(trade_date)
        
        result_dates = sorted(result_dates)
        
        if use_cache:
            self.calendar_cache.put(cache_key, result_dates)
        
        return result_dates
    
    # ==================== 工具方法 ====================
    
    def _normalize_dates(self, start_date: Union[str, datetime], 
                        end_date: Union[str, datetime]) -> Tuple[str, str]:
        """统一日期格式为YYYY-MM-DD"""
        if start_date is None:
            start_date = datetime.now().strftime('%Y-%m-%d')
        elif isinstance(start_date, datetime):
            start_date = start_date.strftime('%Y-%m-%d')
        elif len(start_date) == 8:  # YYYYMMDD
            start_date = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        elif isinstance(end_date, datetime):
            end_date = end_date.strftime('%Y-%m-%d')
        elif len(end_date) == 8:  # YYYYMMDD
            end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
        
        return start_date, end_date
    
    def _has_price_fields(self, fields: List[str]) -> bool:
        """检查字段列表中是否包含价格字段"""
        price_fields = {'open', 'high', 'low', 'close'}
        return bool(set(fields) & price_fields)
    
    def _apply_adjustment(self, df: pd.DataFrame, market: str, adj_type: str, 
                         start_date: str, end_date: str) -> pd.DataFrame:
        """
        应用复权计算
        
        Args:
            df: K线数据，index为MultiIndex(time, symbol)
            market: 市场名称
            adj_type: 复权类型 ('front': 前复权, 'back': 后复权)
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            pd.DataFrame: 复权后的数据
        """
        if df.empty or adj_type == 'none' or not MarketConfig.supports_adjustment(market):
            return df
        
        try:
            # 获取复权因子
            symbols = df.index.get_level_values('symbol').unique().tolist()
            adj_factors = self.get_adj_factors(
                market=market,
                codes=symbols,
                start_date=start_date,
                end_date=end_date,
                use_cache=True
            )
            
            if adj_factors.empty:
                logger.debug(f"未找到{market}的复权因子数据，返回原始数据")
                return df
            
            # 转换复权因子日期格式
            adj_factors = adj_factors.copy()
            adj_factors['date'] = pd.to_datetime(adj_factors['date'].astype(str), format='%Y%m%d')
            
            result_df = df.copy()
            
            # 价格字段
            price_fields = ['open', 'high', 'low', 'close']
            available_price_fields = [field for field in price_fields if field in result_df.columns]
            
            if not available_price_fields:
                return result_df
            
            # 按股票分组处理复权
            for symbol in symbols:
                if symbol not in df.index.get_level_values('symbol'):
                    continue
                
                # 获取该股票的复权因子
                symbol_adj = adj_factors[adj_factors['code'] == symbol].copy()
                if symbol_adj.empty:
                    continue
                
                # 获取该股票的K线数据
                symbol_data = result_df.xs(symbol, level='symbol').reset_index()

                if symbol_data.empty:
                    continue
                
                # 合并复权因子（使用向前填充）
                symbol_data['time'] = pd.to_datetime(symbol_data['time'])
                
                # 将复权因子数据与K线数据对齐
                merged_data = pd.merge_asof(
                    symbol_data.sort_values('time'),
                    symbol_adj[['date', 'adj_factor']].sort_values('date').rename(columns={'date': 'time'}),
                    on='time',
                    direction='backward'  # 使用向前查找
                )
                
                # 应用复权计算
                if adj_type == 'front':
                    # 前复权：当前价格 = 原始价格 * 当前复权因子 / 最新复权因子
                    latest_adj_factor = symbol_adj['adj_factor'].iloc[-1] if not symbol_adj.empty else 1.0
                    
                    for field in available_price_fields:
                        if field in merged_data.columns and not merged_data['adj_factor'].isna().all():
                            merged_data[field] = merged_data[field] * merged_data['adj_factor'] / latest_adj_factor
                
                elif adj_type == 'back':
                    # 后复权：当前价格 = 原始价格 * 复权因子
                    for field in available_price_fields:
                        if field in merged_data.columns and not merged_data['adj_factor'].isna().all():
                            merged_data[field] = merged_data[field] * merged_data['adj_factor']
                
                # 更新结果数据
                merged_data = merged_data.set_index(['time'])
                merged_data['symbol'] = symbol
                merged_data = merged_data.set_index(['symbol'], append=True).reorder_levels(['time', 'symbol'])

                # 更新result_df中对应股票的数据
                result_df.update(merged_data[available_price_fields])
            
            logger.debug(f"应用{adj_type}复权完成，处理{len(symbols)}只股票")
            return result_df
            
        except Exception as e:
            logger.warning(f"应用复权失败: {e}，返回原始数据")
            return df
    
    def clear_cache(self, cache_type: str = None):
        """清空缓存"""
        if cache_type is None or cache_type == 'all':
            self.kline_cache.clear()
            self.factor_cache.clear()
            self.reference_cache.clear()
            self.calendar_cache.clear()
            self.metadata_cache.clear()
            logger.info("已清空所有缓存")
        elif cache_type == 'kline':
            self.kline_cache.clear()
            logger.info("已清空K线缓存")
        elif cache_type == 'factor':
            self.factor_cache.clear()
            logger.info("已清空因子缓存")
        elif cache_type == 'reference':
            self.reference_cache.clear()
            self.calendar_cache.clear()
            self.metadata_cache.clear()
            logger.info("已清空参考数据缓存")
    
    def get_cache_stats(self) -> Dict[str, Dict[str, int]]:
        """获取缓存统计信息"""
        return {
            'kline_cache': {
                'size': self.kline_cache.size(),
                'max_size': self.kline_cache.max_size
            },
            'factor_cache': {
                'size': self.factor_cache.size(),
                'max_size': self.factor_cache.max_size
            },
            'reference_cache': {
                'size': self.reference_cache.size(),
                'max_size': self.reference_cache.max_size
            },
            'calendar_cache': {
                'size': self.calendar_cache.size(),
                'max_size': self.calendar_cache.max_size
            },
            'metadata_cache': {
                'size': self.metadata_cache.size(),
                'max_size': self.metadata_cache.max_size
            }
        }
    
    def shutdown(self):
        """关闭数据接口"""
        self.thread_pool.shutdown(wait=True)
        self.clear_cache()
        logger.info("数据接口已关闭")


# 全局数据接口实例
_data_interface = None

def get_data_interface(project_path: str = None, cache_config: Dict[str, Any] = None) -> DataInterface:
    """获取数据接口单例"""
    global _data_interface
    
    if _data_interface is None:
        if project_path is None:
            try:
                from runtime.constant import BASE_DIR
                project_path = BASE_DIR
            except ImportError:
                raise ValueError("未指定project_path且无法从runtime获取BASE_DIR")
        
        _data_interface = DataInterface(project_path, cache_config)
    
    return _data_interface

def shutdown_data_interface():
    """关闭数据接口"""
    global _data_interface
    if _data_interface is not None:
        _data_interface.shutdown()
        _data_interface = None
