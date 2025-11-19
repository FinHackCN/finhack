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
            'max_size': 500,  # 减少缓存大小，避免内存占用过高
            'ttl_seconds': 1800,  # 减少缓存时间到30分钟
            'cleanup_interval': 60  # 1分钟清理一次过期缓存，更频繁
        }
        self.cache_config = {**default_cache_config, **(cache_config or {})}
        
        # 初始化缓存
        self._init_caches()
        
        # 线程池
        self.thread_pool = ThreadPoolExecutor(max_workers=4)  # 减少线程数，避免资源竞争
        
        # 预加载复权因子数据
        self.adj_factors_cache = {}  # 市场 -> DataFrame 的映射
        self._preload_adj_factors()
        
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
    
    def _preload_adj_factors(self):
        """预加载复权因子数据"""
        try:
            # 预加载支持复权的市场数据
            for market in ['cn_stock', 'cn_fund']:  # 目前只有这两个市场支持复权
                adj_file = os.path.join(self.reference_data_dir, market, f"{market}_adj.csv")
                if os.path.exists(adj_file):
                    logger.debug(f"预加载 {market} 复权因子数据...")
                    adj_factors = pd.read_csv(adj_file)
                    self.adj_factors_cache[market] = adj_factors
                    logger.debug(f"预加载 {market} 复权因子完成: {len(adj_factors)} 条记录")
                else:
                    logger.debug(f"复权因子文件不存在: {adj_file}")
        except Exception as e:
            logger.warning(f"预加载复权因子数据失败: {e}")
    
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
        获取K线数据 - 优化版
        
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
            DataFrame: K线数据，索引为MultiIndex(datetime, symbol)
        """
        # 参数标准化
        if isinstance(codes, str):
            codes = [codes]
        
        if isinstance(start_date, datetime):
            start_date = start_date.strftime('%Y-%m-%d')
        elif start_date is None:
            start_date = '20240101'
            
        if isinstance(end_date, datetime):
            end_date = end_date.strftime('%Y-%m-%d')
        elif end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        if fields is None:
            fields = ['open', 'high', 'low', 'close', 'volume', 'amount']
        
        # 生成缓存键
        cache_key = self._generate_cache_key(
            '|'.join(sorted(codes)), market, freq, start_date, end_date, 
            '|'.join(sorted(fields)), adj_type
        )
        
        # 尝试从缓存获取
        if use_cache:
            cached_data = self.kline_cache.get(cache_key)
            if cached_data is not None:
                logger.debug(f"从缓存获取K线数据: {len(codes)} 只股票, {len(cached_data)} 条记录")
                return cached_data
        
        # 从数据源获取
        try:
            # 对于1分钟数据，优先使用timebased方式加载，减少文件数量
            if freq == '1m' and len(codes) > 10:
                # 对于大量股票的1分钟数据，使用timebased方式更高效
                data = self._load_timebased_klines(codes, market, freq, start_date, end_date, fields)
            else:
                # 对于少量股票或日线数据，使用codebased方式
                data = self._load_codebased_klines(codes, market, freq, start_date, end_date, fields)
            
            # 应用复权
            if adj_type != 'none' and market in ['cn_stock', 'cn_fund']:
                data = self._apply_adjustment(data, market, adj_type)
            
            # 缓存结果
            if use_cache and not data.empty:
                self.kline_cache.put(cache_key, data)
            
            logger.debug(f"加载K线数据: {len(codes)} 只股票, {len(data)} 条记录")
            return data
            
        except Exception as e:
            logger.error(f"获取K线数据失败: {e}")
            return pd.DataFrame()
    
    def _load_timebased_klines(self, codes: List[str], market: str, freq: str, 
                              start_date: str, end_date: str, fields: List[str]) -> pd.DataFrame:
        """使用timebased方式加载K线数据 - 优化版"""
        try:
            # 解析日期
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            # 生成日期列表
            date_list = []
            current_dt = start_dt
            while current_dt <= end_dt:
                date_list.append(current_dt)
                current_dt += timedelta(days=1)
            
            # 按月分组，减少文件读取次数
            monthly_groups = {}
            for dt in date_list:
                month_key = (dt.year, dt.month)
                if month_key not in monthly_groups:
                    monthly_groups[month_key] = []
                monthly_groups[month_key].append(dt)
            
            # 并行加载各月数据
            futures = []
            for (year, month), dates in monthly_groups.items():
                future = self.thread_pool.submit(
                    self._load_month_timebased, year, month, dates, market, freq, fields
                )
                futures.append(future)
            
            # 合并结果
            all_data = []
            for future in as_completed(futures):
                try:
                    month_data = future.result()
                    if not month_data.empty:
                        # 过滤出需要的股票
                        filtered_data = month_data[month_data.index.get_level_values(1).isin(codes)]
                        all_data.append(filtered_data)
                except Exception as e:
                    logger.error(f"加载月份数据失败: {e}")
            
            if all_data:
                result = pd.concat(all_data, ignore_index=False)
                # 按日期和股票代码排序
                result.sort_index(inplace=True)
                return result
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"加载timebased K线数据失败: {e}")
            return pd.DataFrame()
    
    def _load_month_timebased(self, year: int, month: int, dates: List[datetime], 
                             market: str, freq: str, fields: List[str]) -> pd.DataFrame:
        """加载指定月份的timebased数据"""
        try:
            month_data = []
            
            for dt in dates:
                # 构建文件路径
                file_path = os.path.join(
                    self.market_data_dir, 'kline', 'timebased', market, freq,
                    f"{year:04d}", f"{month:02d}", f"{dt.day:02d}",
                    f"{market}_kline_merged.csv"
                )
                
                if os.path.exists(file_path):
                    # 读取文件
                    df = pd.read_csv(file_path, header=None, 
                                   names=['time', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                    
                    # 转换时间格式
                    df['time'] = pd.to_datetime(df['time'])
                    
                    # 确保时间是无时区的，便于比较
                    if hasattr(df['time'].dt, 'tz') and df['time'].dt.tz is not None:
                        df['time'] = df['time'].dt.tz_localize(None)
                    
                    # 设置多级索引
                    df.set_index(['time', 'symbol'], inplace=True)
                    
                    # 选择需要的字段
                    available_fields = [f for f in fields if f in df.columns]
                    if available_fields:
                        df = df[available_fields]
                    
                    month_data.append(df)
            
            if month_data:
                return pd.concat(month_data, ignore_index=False)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"加载月份 {year}-{month:02d} 数据失败: {e}")
            return pd.DataFrame()
    
    def _load_codebased_klines(self, codes: List[str], market: str, freq: str, 
                              start_date: str, end_date: str, fields: List[str]) -> pd.DataFrame:
        """使用codebased方式加载K线数据"""
        try:
            # 并行加载各股票数据
            futures = []
            for code in codes:
                future = self.thread_pool.submit(
                    self._load_single_codebased_kline, market, code, freq, start_date, end_date, fields
                )
                futures.append((code, future))
            
            # 收集结果
            all_data = []
            for code, future in futures:
                try:
                    klines = future.result()
                    if not klines.empty:
                        # 设置股票代码
                        klines['symbol'] = code
                        all_data.append(klines)
                except Exception as e:
                    logger.warning(f"获取{code}的K线数据失败: {e}")
                    continue
            
            if all_data:
                result = pd.concat(all_data, ignore_index=True)
                result.set_index(['time', 'symbol'], inplace=True)
                return result
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"加载codebased K线数据失败: {e}")
            return pd.DataFrame()
    
    def _load_single_codebased_kline(self, market: str, symbol: str, freq: str,
                                    start_date: str, end_date: str, fields: List[str]) -> pd.DataFrame:
        """加载单个股票的codebased数据"""
        try:
            logger.debug(f"加载{symbol}的codebased数据: 市场={market}, 频率={freq}, 开始日期={start_date}, 结束日期={end_date}")
            
            # 确定数据文件路径
            kline_dir = os.path.join(self.market_data_dir, 'kline', 'codebased', market, freq)
            
            # 获取年份范围
            start_year = int(start_date[:4])
            end_year = int(end_date[:4])
            
            logger.debug(f"数据目录: {kline_dir}, 年份范围: {start_year}-{end_year}")
            
            all_data = []
            
            # 按年加载数据
            for year in range(start_year, end_year + 1):
                year_dir = os.path.join(kline_dir, str(year))
                symbol_file = os.path.join(year_dir, f"{symbol}.csv")
                
                logger.debug(f"检查文件: {symbol_file}")
                
                if os.path.exists(symbol_file):
                    try:
                        # 读取CSV文件（无header）
                        year_data = pd.read_csv(symbol_file, header=None, 
                                              names=['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                        
                        logger.debug(f"读取{year}年{symbol}数据: 原始行数={len(year_data)}")
                        
                        # 转换时间格式
                        year_data['time'] = pd.to_datetime(year_data['time'])

                        # 过滤日期范围 - 对于日线数据，扩展到整天范围以确保包含性
                        start_dt = pd.to_datetime(start_date).normalize()  # 设置为当天的开始
                        end_dt = pd.to_datetime(end_date).normalize() + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)  # 设置为当天的结束

                        # 确保时间比较的兼容性 - 移除时区信息
                        if hasattr(year_data['time'].dt, 'tz') and year_data['time'].dt.tz is not None:
                            year_data['time'] = year_data['time'].dt.tz_localize(None)
                        
                        logger.debug(f"时间范围检查: 开始={start_dt}, 结束={end_dt}, 数据时间范围={year_data['time'].min()}-{year_data['time'].max()}")
                        
                        original_count = len(year_data)
                        year_data = year_data[(year_data['time'] >= start_dt) & (year_data['time'] <= end_dt)]
                        filtered_count = len(year_data)
                        
                        logger.debug(f"过滤后数据: 原始={original_count}, 过滤后={filtered_count}")
                        
                        if not year_data.empty:
                            # 选择需要的字段
                            available_fields = ['time'] + [f for f in fields if f in year_data.columns]
                            year_data = year_data[available_fields]
                            all_data.append(year_data)
                            
                    except Exception as e:
                        logger.warning(f"读取{symbol}的{year}年数据失败: {e}")
                        continue
            
            if all_data:
                result = pd.concat(all_data, ignore_index=True)
                result = result.sort_values('time').reset_index(drop=True)
                return result
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"加载{symbol}的codebased数据失败: {e}")
            return pd.DataFrame()
    
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
            time: 时间点
            fields: 需要的字段列表
            adj_type: 复权类型
            use_cache: 是否使用缓存
            
        Returns:
            DataFrame: 行情数据，索引为symbol
        """
        # 参数标准化
        if isinstance(codes, str):
            codes = [codes]
        
        if fields is None:
            fields = ['open', 'high', 'low', 'close', 'volume']
        
        # 如果没有指定时间，使用当前时间
        if time is None:
            time = datetime.now()
        
        # 生成缓存键
        time_str = time.strftime('%Y-%m-%d %H:%M:%S')
        cache_key = self._generate_cache_key(
            '|'.join(sorted(codes)), market, freq, time_str, 
            '|'.join(sorted(fields)), adj_type
        )
        
        # 尝试从缓存获取
        if use_cache:
            cached_data = self.kline_cache.get(cache_key)
            if cached_data is not None:
                logger.debug(f"从缓存获取行情数据: {len(codes)} 只股票")
                return cached_data
        
        # 从数据源获取
        try:
            # 获取指定时间点的K线数据 - 精确时间匹配
            start_time = time.strftime('%Y-%m-%d %H:%M:%S')
            end_time = time.strftime('%Y-%m-%d %H:%M:%S')

            # 使用get_klines方法获取精确时间的数据
            klines_df = self.get_klines(
                codes=codes,
                market=market,
                freq=freq,
                start_date=start_time,
                end_date=end_time,
                fields=fields,
                adj_type=adj_type,
                use_cache=use_cache
            )

            # 提取指定时间点的数据
            if not klines_df.empty:
                # 查找指定时间点的数据
                target_time = time

                # 根据频率调整匹配策略
                if freq == '1d':
                    # 对于日线数据，只匹配日期部分，忽略时间
                    target_date = target_time.date()
                    matching_data = klines_df[klines_df.index.get_level_values('time').date == target_date]
                else:
                    # 对于分钟数据，精确匹配时间
                    matching_data = klines_df[klines_df.index.get_level_values('time') == target_time]

                if not matching_data.empty:
                    # 提取指定时间点的数据
                    quotes_df = matching_data.copy()
                    # 重置索引，将symbol作为列
                    quotes_df = quotes_df.reset_index(level=0, drop=True).reset_index()
                    quotes_df = quotes_df.rename(columns={'symbol': 'code'})
                    quotes_df.set_index('code', inplace=True)

                    # 缓存结果
                    if use_cache:
                        self.kline_cache.put(cache_key, quotes_df)

                    logger.debug(f"获取行情数据: {len(codes)} 只股票, 时间点: {time}")
                    return quotes_df
                else:
                    # 如果精确时间没有数据，尝试查找最近的有效数据
                    logger.debug(f"精确时间点 {time} 无数据，尝试查找最近数据")

                    # 扩大时间范围到前后5分钟
                    start_time = (time - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
                    end_time = (time + timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')

                    klines_df = self.get_klines(
                        codes=codes,
                        market=market,
                        freq=freq,
                        start_date=start_time,
                        end_date=end_time,
                        fields=fields,
                        adj_type=adj_type,
                        use_cache=use_cache
                    )

                    if not klines_df.empty:
                        # 查找最接近目标时间的数据
                        target_timestamp = time.timestamp()
                        closest_data = []

                        for code in codes:
                            code_data = klines_df[klines_df.index.get_level_values('symbol') == code]
                            if not code_data.empty:
                                # 计算时间差，找到最近的数据
                                time_values = code_data.index.get_level_values('time')
                                time_diffs = abs(time_values.astype('int64') // 10**9 - target_timestamp)
                                # 找到最小时间差的索引
                                closest_idx = time_diffs.argmin()
                                closest_timestamp = time_values[closest_idx]
                                closest_row = code_data.loc[(closest_timestamp, code)]
                                closest_data.append(closest_row)

                        if closest_data:
                            # 从Series列表创建DataFrame
                            quotes_df = pd.DataFrame(closest_data)

                            # 从Series的索引中提取symbol和时间信息
                            symbols = [row.name[1] if isinstance(row.name, tuple) else row.name for row in closest_data]

                            # 添加code列
                            quotes_df['code'] = symbols

                            # 设置code为索引
                            quotes_df.set_index('code', inplace=True)

                            if use_cache:
                                self.kline_cache.put(cache_key, quotes_df)

                            logger.debug(f"使用最近数据获取行情: {len(codes)} 只股票")
                            return quotes_df
                    else:
                        return pd.DataFrame()
            else:
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"获取行情数据失败: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
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
        
        # 使用预加载的复权因子数据
        if market in self.adj_factors_cache:
            adj_factors = self.adj_factors_cache[market].copy()
        else:
            # 如果预加载失败，则回退到文件读取
            adj_file = os.path.join(self.reference_data_dir, market, f"{market}_adj.csv")
            if os.path.exists(adj_file):
                adj_factors = pd.read_csv(adj_file)
            else:
                logger.debug(f"复权因子文件不存在: {adj_file}")
                return pd.DataFrame()
        
        # 过滤条件
        if codes:
            adj_factors = adj_factors[adj_factors['code'].isin(codes)]
        
        if start_date:
            # 处理可能包含时间的日期字符串
            try:
                # 尝试解析为datetime，然后格式化为YYYYMMDD
                start_dt = pd.to_datetime(start_date)
                start_date_int = int(start_dt.strftime('%Y%m%d'))
                adj_factors = adj_factors[adj_factors['date'] >= start_date_int]
            except Exception as e:
                logger.warning(f"解析开始日期失败: {start_date}, 错误: {e}")
                # 尝试直接转换为整数（处理YYYYMMDD格式）
                try:
                    start_date_int = int(start_date.replace('-', '').replace(' ', '').replace(':', ''))
                    adj_factors = adj_factors[adj_factors['date'] >= start_date_int]
                except:
                    logger.warning(f"无法解析开始日期: {start_date}")
            
            if end_date:
                # 处理可能包含时间的日期字符串
                try:
                    # 尝试解析为datetime，然后格式化为YYYYMMDD
                    end_dt = pd.to_datetime(end_date)
                    end_date_int = int(end_dt.strftime('%Y%m%d'))
                    adj_factors = adj_factors[adj_factors['date'] <= end_date_int]
                except Exception as e:
                    logger.warning(f"解析结束日期失败: {end_date}, 错误: {e}")
                    # 尝试直接转换为整数（处理YYYYMMDD格式）
                    try:
                        end_date_int = int(end_date.replace('-', '').replace(' ', '').replace(':', ''))
                        adj_factors = adj_factors[adj_factors['date'] <= end_date_int]
                    except:
                        logger.warning(f"无法解析结束日期: {end_date}")
            
        if use_cache:
            self.reference_cache.put(cache_key, adj_factors)
        
        return adj_factors
    
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
    
    def _normalize_adj_type(self, adj_type: str) -> str:
        """标准化复权类型参数"""
        adj_type_mapping = {
            'qfq': 'front',  # 前复权
            'hfq': 'back',   # 后复权  
            'front': 'front',
            'back': 'back',
            'none': 'none'
        }
        return adj_type_mapping.get(adj_type, adj_type)
    
    def _apply_adjustment(self, df: pd.DataFrame, market: str, adj_type: str) -> pd.DataFrame:
        """
        应用复权计算
        
        Args:
            df: K线数据，index为MultiIndex(time, symbol)
            market: 市场名称
            adj_type: 复权类型 ('qfq'/'front': 前复权, 'hfq'/'back': 后复权)
            
        Returns:
            pd.DataFrame: 复权后的数据
        """
        # 标准化复权类型
        adj_type = self._normalize_adj_type(adj_type)
        
        if df.empty or adj_type == 'none' or not MarketConfig.supports_adjustment(market):
            return df
        
        try:
            # 获取复权因子
            symbols = df.index.get_level_values('symbol').unique().tolist()
            adj_factors = self.get_adj_factors(
                market=market,
                codes=symbols,
                start_date=None,
                end_date=None,  # 获取全部历史数据
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
                
                # 准备复权因子数据，确保时间戳类型一致
                adj_for_merge = symbol_adj[['date', 'adj_factor']].copy()
                adj_for_merge['time'] = pd.to_datetime(adj_for_merge['date'])
                
                # 确保两个时间序列都是相同的datetime精度
                symbol_data['time'] = symbol_data['time'].astype('datetime64[ns]')
                adj_for_merge['time'] = adj_for_merge['time'].astype('datetime64[ns]')
                
                # 将复权因子数据与K线数据对齐
                merged_data = pd.merge_asof(
                    symbol_data.sort_values('time'),
                    adj_for_merge[['time', 'adj_factor']].sort_values('time'),
                    on='time',
                    direction='backward'  # 使用向前查找
                )
                
                # 应用复权计算
                if adj_type == 'front':
                    # 前复权：当前价格 = 原始价格 * 当前复权因子 / 最新复权因子
                    # 获取该股票历史上的最新复权因子（不限于查询日期范围）
                    latest_adj_factors = self.get_adj_factors(
                        market=market,
                        codes=[symbol],
                        start_date=None,
                        end_date=None,  # 获取全部历史数据
                        use_cache=True
                    )
                    latest_adj_factor = latest_adj_factors['adj_factor'].iloc[-1] if not latest_adj_factors.empty else 1.0
                    
                    for field in available_price_fields:
                        if field in merged_data.columns and not merged_data['adj_factor'].isna().all():
                            merged_data[field] = merged_data[field] * merged_data['adj_factor'] / latest_adj_factor
                
                elif adj_type == 'back':
                    # 后复权：当前价格 = 原始价格 * 当前复权因子 / 首个复权因子
                    # 获取该股票历史上的首个复权因子
                    first_adj_factors = self.get_adj_factors(
                        market=market,
                        codes=[symbol],
                        start_date=None,
                        end_date=None,  # 获取全部历史数据
                        use_cache=True
                    )
                    first_adj_factor = first_adj_factors['adj_factor'].iloc[0] if not first_adj_factors.empty else 1.0
                    
                    for field in available_price_fields:
                        if field in merged_data.columns and not merged_data['adj_factor'].isna().all():
                            merged_data[field] = merged_data[field] * merged_data['adj_factor'] / first_adj_factor
                
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
