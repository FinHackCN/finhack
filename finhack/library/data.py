"""
统一数据接口 - 增强版

提供整个项目的数据访问能力，包括K线数据、因子数据、参考数据等
支持高性能缓存、多线程处理、批量操作等优化功能
专门优化cn_fund、cn_future、cn_stock、global_cryptoswap、global_cryptospot市场支持
"""

import os
import sys
import io
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

# 添加项目路径到sys.path以支持runtime模块
if 'BASE_DIR' not in os.environ:
    # 尝试从当前目录或父目录查找runtime
    possible_paths = [
        os.path.join(os.getcwd(), 'data', 'cache', 'runtime'),
        os.path.join(os.path.dirname(os.getcwd()), 'data', 'cache', 'runtime'),
        os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', 'demo_project', 'data', 'cache', 'runtime'),
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            runtime_path = os.path.dirname(os.path.dirname(path))
            if runtime_path not in sys.path:
                sys.path.insert(0, runtime_path)
            break

from runtime.constant import *

logger = logging.getLogger(__name__)

# 提高递归上限：pandas 的 dtype 推断(is_float_dtype 等)内部带递归，在回测引擎较深的事件
# 调用栈下，对个别空/畸形数据查询会触发 RecursionError(maximum recursion depth exceeded)，
# 导致 get_klines 卡死。提高到 6000 给 pandas 留足栈空间（Python 默认 1000，6000 仍安全）。
try:
    if sys.getrecursionlimit() < 6000:
        sys.setrecursionlimit(6000)
except Exception:
    pass

# Parquet支持检查
PARQUET_AVAILABLE = True
try:
    import pyarrow.parquet as pq
except ImportError:
    PARQUET_AVAILABLE = False
    logger.warning("pyarrow未安装，Parquet缓存功能不可用")


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
        'cn_future': ['1d', '1m'],  # 支持1d和1m数据
        'cn_index': ['1d', '1m'],
        'cn_cb': ['1d', '1m'],
        'global_cryptospot': ['1d', '1m'],  # 支持1d和1m数据
        'global_cryptoswap': ['1d', '1m'],  # 支持1d和1m数据
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

        # codebased 代码存在性索引：{(market, freq, year): (codes_set, dir_mtime)}
        # 仅内存缓存（每次运行动态初始化），按目录 mtime 失效——不落盘、无 TTL，
        # 因此 codebased 被 time2code 重建后会自动刷新，不存在"永久漏判"过期问题。
        self._codebased_codes_index = {}

        # timebased 单文件 code→行块索引：{file_path: (mtime, size, {code: [(start_row, count)]})}
        # 内存级。配合磁盘 .idx（见 _get_tb_file_index）实现跨进程持久化。
        # 按 (mtime, size) 校验——数据被 collector 追加/重写时自动失效重建，自维护。
        self._tb_file_index_cache = {}

        # timebased 单文件 已解析df 的 LRU 缓存（整表读路径用）。
        # 解决"滑动窗口"查询：策略每分钟查 24h 回溯（窗口逐分钟滑动），同一日文件会被
        # 反复读取。缓存整文件解析结果(按mtime校验)，1440次滑窗查询复用一次解析。
        from collections import OrderedDict as _OD
        self._tb_file_df_cache = _OD()
        # 容量需覆盖策略最宽回溯窗口(crypto_swap 1m 有37天回溯→约37个日文件)，
        # 否则宽查询会淘汰缓存、紧随的滑窗查询被迫重解析92MB文件而卡死。留足余量。
        self._tb_file_df_cache_max = 60

        # codebased 单文件 时间范围缓存：{file_path: (mtime, size, min_dt, max_dt)}
        # 只读首尾行得到[min,max]，用于跳过"查询区间在文件数据范围之外"的情形
        # （如小币2023-11才上线，却每分钟查2023-06的24h滑窗 → 整文件逐行扫描后返回空，卡死）。
        self._cb_daterange_cache = {}
        try:
            self._tb_index_dir = os.path.join(os.path.dirname(self.market_data_dir), 'cache', 'tb_idx')
            os.makedirs(self._tb_index_dir, exist_ok=True)
        except Exception:
            self._tb_index_dir = None

    def _get_codebased_codes(self, market: str, freq: str, year: int):
        """返回某年 codebased 中存在的代码集合，作为 timebased 代码存在性的判据。

        依据：codebased 是从 timebased 抽取的"每个代码一个文件"，故其目录列表 = timebased
        中有数据的代码集合。os.listdir 仅微秒级，远快于扫描 timebased 内容。
        - 该年 codebased 未构建/为空时返回 None（不可作为判据，通常是当年尚未生成）；
        - 目录 mtime 变化（time2code 重建）时自动重建索引。
        """
        key = (market, freq, year)
        year_dir = os.path.join(self.market_data_dir, 'kline', 'codebased', market, freq, str(year))
        if not os.path.isdir(year_dir):
            return None
        try:
            mtime = os.stat(year_dir).st_mtime
        except OSError:
            return None
        cached = self._codebased_codes_index.get(key)
        if cached and cached[1] == mtime:
            return cached[0]
        try:
            codes = {f[:-4] for f in os.listdir(year_dir) if f.endswith('.csv')}
        except OSError:
            return None
        if not codes:
            return None  # 空目录（当年可能尚未生成），不可靠
        self._codebased_codes_index[key] = (codes, mtime)
        return codes

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
            DataFrame: K线数据，索引为MultiIndex(datetime, code)
        """
        # 参数标准化
        if isinstance(codes, str):
            codes = [codes]
        
        if isinstance(start_date, datetime):
            # 如果是datetime对象，保留完整时间信息
            start_date = start_date.strftime('%Y-%m-%d %H:%M:%S')
        elif start_date is None:
            start_date = '2024-01-01 00:00:00'
        elif ' ' not in start_date:
            # 如果只有日期没有时间，添加时间
            start_date = start_date + ' 00:00:00'
            
        if isinstance(end_date, datetime):
            # 如果是datetime对象，保留完整时间信息
            end_date = end_date.strftime('%Y-%m-%d %H:%M:%S')
        elif end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d 23:59:59')
        elif ' ' not in end_date:
            # 如果只有日期没有时间，添加时间
            end_date = end_date + ' 23:59:59'
        
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
                # 【性能优化】改为debug级别
                logger.debug(f"[Cache] 从缓存获取K线数据: {len(codes)} 只股票, {len(cached_data)} 条记录")
                return cached_data
            else:
                logger.debug(f"[Cache] 缓存未命中: {len(codes)} 只股票")

        # 从数据源获取
        try:
            # 智能选择加载方式
            # 解析日期范围，判断是否跨年份
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            is_cross_year = (start_dt.year != end_dt.year)

            # 【性能优化】所有调试日志改为debug级别
            logger.debug(f"[DataInterface] get_klines参数: codes数量={len(codes)}, market={market}, freq={freq}")

            # Parquet适合：单年份、代码数量多（>100）
            use_parquet = (
                not is_cross_year and  # 单年份
                len(codes) > 100 and  # 代码数量较多
                PARQUET_AVAILABLE and  # Parquet支持可用
                freq in ['1d', '1m']  # 支持的频率
            )

            logger.debug(f"[DataInterface] use_parquet={use_parquet}")

            # 根据条件选择加载方式
            if use_parquet:
                # 单年份+大量代码：优先使用parquet
                data = self._load_codebased_klines(codes, market, freq, start_date, end_date, fields)
                logger.debug(f"[DataInterface] Parquet/Codebased返回: {len(data)}条记录")
                # parquet/codebased返回空时，尝试回退到timebased
                if data.empty:
                    logger.info(f"[DataInterface] Parquet/Codebased数据为空，尝试Timebased回退")
                    data = self._load_timebased_klines(codes, market, freq, start_date, end_date, fields)
                    if not data.empty:
                        logger.info(f"[DataInterface] Timebased回退成功: {len(data)}条记录")
            elif freq == '1m' and len(codes) > 50:
                # 1分钟+中大量代码：使用timebased（CSV，减少文件数量）
                data = self._load_timebased_klines(codes, market, freq, start_date, end_date, fields)
                logger.debug(f"[DataInterface] Timebased返回: {len(data)}条记录")
            else:
                # 其他情况：使用codebased（少量代码或日线数据）
                data = self._load_codebased_klines(codes, market, freq, start_date, end_date, fields)
                logger.debug(f"[DataInterface] Codebased返回: {len(data)}条记录")

                # codebased返回空时，尝试回退到timebased加载
                if data.empty:
                    logger.info(f"[DataInterface] Codebased数据为空，尝试Timebased回退")
                    data = self._load_timebased_klines(codes, market, freq, start_date, end_date, fields)
                    if not data.empty:
                        logger.info(f"[DataInterface] Timebased回退成功: {len(data)}条记录")

            # 应用复权
            if adj_type != 'none' and market in ['cn_stock', 'cn_fund']:
                data = self._apply_adjustment(data, market, adj_type)

            # 缓存结果（含负缓存：空结果也缓存）
            # 否则对"无数据 symbol+日期组合"（crypto 小币/退市币）每次查询都触发慢速
            # codebased→timebased 全扫描，回测会卡死在反复扫描上。LRUCache 带 TTL，
            # 同一回测内数据静态、空结果稳定；后续补数据后 TTL 过期自动重查。
            if use_cache:
                self.kline_cache.put(cache_key, data)
                if data.empty:
                    logger.debug(f"[Cache] 缓存空结果(负缓存): codes={len(codes)}")
                else:
                    logger.debug(f"[Cache] 已缓存数据: {len(data)}条记录")

            # 【内存优化】优化数据类型
            data = self._optimize_dtypes(data)

            # 检查返回数据的索引结构
            if not data.empty:
                if not hasattr(data.index, 'names') or 'code' not in data.index.names:
                    logger.warning(f"[DataInterface] 索引结构不正确，尝试修复")
                    if 'code' in data.columns and 'time' in data.columns:
                        data = data.set_index(['time', 'code'])
                    elif 'code' in data.columns:
                        data = data.set_index('code')

            if data.empty:
                logger.warning(f"[DataInterface] 返回空DataFrame！codes={len(codes)}, market={market}, freq={freq}")

            return data

        except RecursionError:
            # 栈溢出（多见于 pandas dtype 推断在深栈/畸形数据下触发）。
            # 处理须极简（此时栈已近耗尽）：仅记一行警告并优雅返回空，避免级联卡死。
            # 已在模块级把递归上限提到 6000，正常情况不应再触发；此处为兜底。
            try:
                logger.warning(f"get_klines 触发 RecursionError(codes={codes[:3]}, market={market}, freq={freq})，返回空")
            except Exception:
                pass
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"获取K线数据失败: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def _load_timebased_klines(self, codes: List[str], market: str, freq: str, 
                              start_date: str, end_date: str, fields: List[str]) -> pd.DataFrame:
        """使用timebased方式加载K线数据 - 优化版"""
        try:
            logger.info(f"[Timebased] 开始加载: market={market}, freq={freq}, "
                       f"date_range={start_date}~{end_date}, codes={len(codes)}")
            
            # 解析日期（支持两种格式：'YYYY-MM-DD' 和 'YYYY-MM-DD HH:MM:SS'）
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            
            try:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')

            # 【性能】早退优化：剔除 codebased 判定为"区间内不存在"的代码，避免对无数据代码
            # （如已退市/无数据的 AIOUSDT、ETHDOWN 等）扫描全部 timebased 月文件后返回空。
            # 正确性守卫：仅当区间内【每个】年份的 codebased 都"新鲜"（codebased 不早于本查询涉及
            # 月份的 timebased 目录）才剔除——这样用户回填 timebased 数据后，只要还没重建 codebased，
            # 该年会被判为不新鲜而回退到完整扫描，绝不漏数据。索引按 codebased 目录 mtime 失效。
            # 可用环境变量 DISABLE_TIMEBASED_EARLY_EXIT=1 全局关闭。
            if os.environ.get('DISABLE_TIMEBASED_EARLY_EXIT') != '1':
                years_in_range = list(range(start_dt.year, end_dt.year + 1))
                cb_root = os.path.join(self.market_data_dir, 'kline', 'codebased', market, freq)
                tb_root = os.path.join(self.market_data_dir, 'kline', 'timebased', market, freq)
                # 收集每年涉及到的月份
                qmonths = {}
                _cd = start_dt
                while _cd <= end_dt:
                    qmonths.setdefault(_cd.year, set()).add(_cd.month)
                    _cd += timedelta(days=1)
                year_indices = []
                for _y in years_in_range:
                    _idx = self._get_codebased_codes(market, freq, _y)
                    if _idx is None:
                        year_indices.append((_y, None)); continue
                    # 新鲜度：codebased/{year} mtime 必须 >= 涉及月份的 timebased 目录 mtime
                    try:
                        cb_mtime = os.stat(os.path.join(cb_root, str(_y))).st_mtime
                    except OSError:
                        year_indices.append((_y, None)); continue
                    fresh = True
                    for _m in qmonths.get(_y, ()):
                        tb_month = os.path.join(tb_root, str(_y), f"{_m:02d}")
                        try:
                            if os.path.isdir(tb_month) and os.stat(tb_month).st_mtime > cb_mtime:
                                fresh = False; break
                        except OSError:
                            pass
                    year_indices.append((_y, _idx if fresh else None))
                if all(idx is not None for _, idx in year_indices):
                    if market == 'cn_future':
                        existing_base = set()
                        for _, idx in year_indices:
                            existing_base |= {c.split('.')[0] for c in idx}
                        filtered = [c for c in codes if (c.split('.')[0] if '.' in c else c) in existing_base]
                    else:
                        existing = set()
                        for _, idx in year_indices:
                            existing |= idx
                        filtered = [c for c in codes if c in existing]
                    skipped = len(codes) - len(filtered)
                    if skipped > 0:
                        logger.info(f"[Timebased] 早退：{skipped}/{len(codes)} 个代码在 codebased 中不存在，跳过其 timebased 扫描")
                        codes = filtered
                        if not codes:
                            logger.info("[Timebased] 全部代码均无数据，跳过 timebased 扫描")
                            return pd.DataFrame()

            # 生成日期列表
            date_list = []
            current_dt = start_dt
            while current_dt <= end_dt:
                date_list.append(current_dt)
                current_dt += timedelta(days=1)

            logger.debug(f"[Timebased] 需要加载的日期数: {len(date_list)}")

            # 按月分组，减少文件读取次数
            monthly_groups = {}
            for dt in date_list:
                month_key = (dt.year, dt.month)
                if month_key not in monthly_groups:
                    monthly_groups[month_key] = []
                monthly_groups[month_key].append(dt)

            logger.debug(f"[Timebased] 需要加载的月份数: {len(monthly_groups)}")
            
            # 并行加载各月数据。
            # 定点读适用于: cn_stock / cn_fund / global_cryptospot（文件按 code 连续排序、
            #   码数适中，定点读显著快于整表读）。
            # 整表读保留: cn_future（按 base code 匹配需上游过滤）、global_cryptoswap
            #   （单文件 ~92MB/92万行/多块布局，定点读索引开销反而不划算，整表读更稳）。
            codes_for_read = None if market in ('cn_future', 'global_cryptoswap') else codes
            futures = []
            for (year, month), dates in monthly_groups.items():
                future = self.thread_pool.submit(
                    self._load_month_timebased, year, month, dates, market, freq, fields, codes_for_read
                )
                futures.append(future)
            
            # 合并结果
            all_data = []
            for future in as_completed(futures):
                try:
                    month_data = future.result()
                    if not month_data.empty:
                        # 过滤出需要的股票（期货支持多后缀匹配）
                        if market == 'cn_future':
                            base_codes = set(c.split('.')[0] if '.' in c else c for c in codes)
                            cache_base = month_data.index.get_level_values(1).str.split('.').str[0]
                            filtered_data = month_data[cache_base.isin(base_codes)]
                            # 统一code格式：reset_index → map → set_index
                            code_map = {}
                            for c in codes:
                                base = c.split('.')[0] if '.' in c else c
                                code_map[base] = c
                            filtered_data = filtered_data.reset_index()
                            filtered_data['code'] = filtered_data['code'].apply(
                                lambda c: code_map.get(c.split('.')[0] if '.' in c else c, c)
                            )
                            filtered_data = filtered_data.set_index(['time', 'code'])
                        else:
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
    
    def _tb_disk_index_path(self, file_path):
        if not self._tb_index_dir:
            return None
        h = hashlib.md5(os.path.abspath(file_path).encode()).hexdigest()[:16]
        return os.path.join(self._tb_index_dir, h + '.idx')

    def _load_tb_disk_index(self, file_path, mtime, size):
        p = self._tb_disk_index_path(file_path)
        if not p or not os.path.exists(p):
            return None
        try:
            with open(p, 'rb') as f:
                obj = pickle.load(f)
            if obj.get('mtime') == mtime and obj.get('size') == size and obj.get('path') == file_path:
                return obj
        except Exception:
            pass
        return None

    def _save_tb_disk_index(self, file_path, mtime, size, byte_idx, sorted_layout, total_rows):
        p = self._tb_disk_index_path(file_path)
        if not p:
            return
        try:
            with open(p, 'wb') as f:
                pickle.dump({'mtime': mtime, 'size': size, 'path': file_path,
                             'byte_idx': byte_idx, 'sorted': sorted_layout,
                             'total_rows': total_rows}, f, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception:
            pass

    def _get_tb_byte_index(self, file_path):
        """返回 timebased 单文件的字节索引。

        扫描文件一次，记录每个 code 块的字节区间 {code: (byte_start, byte_end)} 及是否按
        code 连续排序。按 (mtime, size) 做 内存→磁盘 两级缓存，collector 追加/重写数据后
        (mtime 变) 自动重建——自维护、跨进程持久。用于定点 seek 读取目标 code 的字节，
        避免整表读 47MB/64万行。
        返回 {'byte_idx': {...}, 'sorted': bool, 'total_rows': int} 或 None。
        """
        try:
            st = os.stat(file_path)
            mtime, size = st.st_mtime, st.st_size
        except OSError:
            return None
        cached = self._tb_file_index_cache.get(file_path)
        if cached and cached[0] == mtime and cached[1] == size:
            return cached[2]
        disk = self._load_tb_disk_index(file_path, mtime, size)
        if disk is not None:
            self._tb_file_index_cache[file_path] = (mtime, size, disk)
            return disk
        # 构建：逐行扫描，记录每个 code 块起始字节
        block_starts = []   # [(byte_start, code)]
        seen_codes = set()
        unsorted = False
        total_rows = 0
        try:
            with open(file_path, 'rb') as f:
                prev_code = None
                while True:
                    pos = f.tell()
                    line = f.readline()
                    if not line:
                        break
                    total_rows += 1
                    seg = line.split(b',', 2)
                    if len(seg) < 2:
                        continue
                    try:
                        code = seg[1].decode('utf-8', 'replace').strip()
                    except Exception:
                        continue
                    if code != prev_code:
                        if code in seen_codes:
                            unsorted = True   # 同 code 多块 → 非连续，回退整表读
                        seen_codes.add(code)
                        block_starts.append((pos, code))
                        prev_code = code
        except Exception as e:
            logger.debug(f"[TBIndex] 扫描失败 {file_path}: {e}")
            return None
        byte_idx = {}
        for i, (pos, code) in enumerate(block_starts):
            end = block_starts[i + 1][0] if i + 1 < len(block_starts) else size
            byte_idx[code] = (pos, end)
        result = {'byte_idx': byte_idx, 'sorted': (not unsorted), 'total_rows': total_rows}
        self._tb_file_index_cache[file_path] = (mtime, size, result)
        self._save_tb_disk_index(file_path, mtime, size, byte_idx, (not unsorted), total_rows)
        return result

    def _read_tb_full(self, file_path):
        names = ['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount']
        return pd.read_csv(file_path, header=None, names=names)

    def _get_tb_file_df(self, file_path):
        """整表读路径的按文件已解析df缓存(mtime校验, LRU)。

        策略常做"滑动窗口"查询(每分钟查24h回溯，窗口逐分钟滑动)→ 同一日文件被反复读取。
        缓存整文件解析结果，1440 次滑窗查询复用一次解析(解析才是耗时大头)。
        返回的 df 已建好(time,code)索引；调用方只读/过滤/列选取，不就地修改。
        """
        try:
            st = os.stat(file_path)
            mtime, size = st.st_mtime, st.st_size
        except OSError:
            return self._clean_tb_df(self._read_tb_full(file_path))
        cache = self._tb_file_df_cache
        cached = cache.get(file_path)
        if cached and cached[0] == mtime and cached[1] == size:
            cache.move_to_end(file_path)  # LRU
            return cached[2]
        df = self._clean_tb_df(self._read_tb_full(file_path))
        cache[file_path] = (mtime, size, df)
        cache.move_to_end(file_path)
        while len(cache) > self._tb_file_df_cache_max:
            cache.popitem(last=False)  # 淘汰最旧
        return df

    def _clean_tb_df(self, df):
        """对原始 K线 df 做类型/时间清洗（保持与原 _load_month_timebased 一致）。"""
        if df is None or df.empty:
            return df
        df = df[df['open'].apply(lambda x: not isinstance(x, str) or x.replace('.', '').replace('-', '').isdigit())]
        for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=['close'])
        df['time'] = df['time'].str.strip()
        try:
            df['time'] = pd.to_datetime(df['time'], format='mixed')
            if hasattr(df['time'].dt, 'tz') and df['time'].dt.tz is not None:
                df['time'] = df['time'].dt.tz_localize(None)
        except Exception:
            try:
                df['time'] = df['time'].str.replace(r'[+-]\d{2}:\d{2}$', '', regex=True)
                df['time'] = pd.to_datetime(df['time'], errors='coerce')
            except Exception:
                df['time'] = pd.to_datetime(df['time'], errors='coerce')
        df.set_index(['time', 'code'], inplace=True)
        return df

    def _read_tb_file_codes(self, file_path, codes):
        """用字节索引定点读取目标 codes 的字节块；索引不可用/非连续/目标过半时回退整表读。"""
        names = ['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount']
        code_set = set(codes)
        idx_info = self._get_tb_byte_index(file_path)
        if idx_info and idx_info.get('sorted', False):
            byte_idx = idx_info['byte_idx']
            present = [c for c in code_set if c in byte_idx]
            if not present:
                return pd.DataFrame()  # 文件中无目标 code，整文件跳过
            n_total = len(byte_idx)
            if n_total > 0 and len(present) / n_total > 0.5:
                # 目标过半，定点读不划算，整表读
                return self._clean_tb_df(self._read_tb_full(file_path))
            # 收集字节区间并合并相邻
            ranges = sorted(byte_idx[c] for c in present)
            merged = []
            for s, e in ranges:
                if merged and s <= merged[-1][1]:
                    merged[-1] = (merged[-1][0], max(merged[-1][1], e))
                else:
                    merged.append([s, e])
            chunks = []
            try:
                with open(file_path, 'rb') as f:
                    for s, e in merged:
                        f.seek(s)
                        chunks.append(f.read(e - s))
            except Exception as e:
                logger.debug(f"[TBIndex] 定点读失败 {file_path}: {e}，回退整表")
                return self._clean_tb_df(self._read_tb_full(file_path))
            raw = b''.join(chunks)   # 每个 chunk 已以 \n 结尾（区间 [s, e)，e 为下一块行首），直接拼接
            try:
                df = pd.read_csv(io.BytesIO(raw), header=None, names=names)
            except Exception:
                return self._clean_tb_df(self._read_tb_full(file_path))
            return self._clean_tb_df(df)
        # 非连续布局或无索引 → 整表读
        return self._clean_tb_df(self._read_tb_full(file_path))

    def _load_month_timebased(self, year: int, month: int, dates: List[datetime],
                             market: str, freq: str, fields: List[str],
                             codes: Optional[List[str]] = None) -> pd.DataFrame:
        """加载指定月份的timebased数据。

        codes 非 None 时，用 _read_tb_file_codes 按字节索引定点读取目标 code 的字节块，
        避免整表读 47MB/64万行/446码（仅读目标码的行）。codes 为 None 时回退原整表读。
        """
        try:
            logger.debug(f"[LoadMonth] 加载 {year}-{month:02d}，日期数: {len(dates)}")
            month_data = []
            files_found = 0
            files_missing = 0

            for dt in dates:
                # 构建文件路径
                # 尝试两种文件名格式：{market}_kline_{freq}.csv 和 {market}_kline_merged.csv
                file_path_freq = os.path.join(
                    self.market_data_dir, 'kline', 'timebased', market, freq,
                    f"{year:04d}", f"{month:02d}", f"{dt.day:02d}",
                    f"{market}_kline_{freq}.csv"
                )
                file_path_merged = os.path.join(
                    self.market_data_dir, 'kline', 'timebased', market, freq,
                    f"{year:04d}", f"{month:02d}", f"{dt.day:02d}",
                    f"{market}_kline_merged.csv"
                )
                file_path = file_path_freq if os.path.exists(file_path_freq) else file_path_merged

                if os.path.exists(file_path):
                    files_found += 1
                    if codes is not None:
                        df = self._read_tb_file_codes(file_path, codes)
                    else:
                        df = self._get_tb_file_df(file_path)  # 整表读+按文件缓存(滑窗查询复用)

                    if df is not None and not df.empty:
                        # 选择需要的字段
                        available_fields = [f for f in fields if f in df.columns]
                        if available_fields:
                            df = df[available_fields]
                        month_data.append(df)
                    # df 为空（如该文件无目标 code）则跳过，等价于"缺失"
                else:
                    files_missing += 1
                    logger.debug(f"[LoadMonth] 文件不存在: {file_path}")

            logger.debug(f"[LoadMonth] {year}-{month:02d} 加载完成: 找到{files_found}个文件, 缺失{files_missing}个")

            if month_data:
                result = pd.concat(month_data, ignore_index=False)
                logger.debug(f"[LoadMonth] 合并后: {len(result)}条记录")
                return result
            else:
                logger.warning(f"[LoadMonth] {year}-{month:02d} 没有加载到任何数据")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"加载月份 {year}-{month:02d} 数据失败: {e}")
            return pd.DataFrame()

    def _load_codebased_klines(self, codes: List[str], market: str, freq: str,
                              start_date: str, end_date: str, fields: List[str]) -> pd.DataFrame:
        """使用codebased方式加载K线数据，优先使用Parquet缓存"""
        try:
            # 解析日期范围，获取年份
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            years = set(range(start_dt.year, end_dt.year + 1))

            logger.debug(f"[CodeBased] 开始加载: {len(codes)}只股票, 时间范围={start_date}~{end_date}")

            # 智能判断：决定使用Parquet还是CSV
            # Parquet适合：代码数量多（>100）、单年份（大文件一次性读取高效）
            # CSV适合：代码数量少（<=100）、任意年份（日期前缀预过滤已大幅优化）
            use_parquet = (
                PARQUET_AVAILABLE and
                len(years) == 1 and  # 单年份
                len(codes) > 100  # 代码数量较多时Parquet更高效
            )

            logger.debug(f"[CodeBased] use_parquet={use_parquet}, 年份={years}, 代码数={len(codes)}")

            # 尝试优先使用Parquet缓存（如果满足条件）
            if use_parquet:
                logger.debug(f"[CodeBased] 尝试Parquet加载")
                parquet_result = self._try_load_parquet_cache(
                    market, freq, start_dt, end_dt, codes, fields
                )
                if parquet_result is not None:
                    logger.debug(f"[CodeBased] 成功使用Parquet缓存: {len(parquet_result)}条")
                    return parquet_result
                else:
                    logger.debug(f"[CodeBased] Parquet缓存不可用，回退到CSV加载")

            # 回退到CSV加载（多年份或Parquet不可用或代码数量少时）
            
            # 回退到CSV加载（多年份或Parquet不可用或代码数量少时）
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
                        klines['code'] = code
                        all_data.append(klines)
                except Exception as e:
                    logger.warning(f"获取{code}的K线数据失败: {e}")
                    continue

            if all_data:
                result = pd.concat(all_data, ignore_index=True)
                # 确保有 code 列再设置索引
                if 'code' not in result.columns:
                    logger.error(f"[CodeBased] 合并后的数据缺少 'code' 列")
                    # 尝试从索引中获取 code
                    if result.index.name == 'code' or (hasattr(result.index, 'names') and 'code' in result.index.names):
                        result = result.reset_index()
                    else:
                        logger.error(f"[CodeBased] 无法恢复 'code' 列")
                        return pd.DataFrame()
                if 'time' not in result.columns:
                    logger.error(f"[CodeBased] 合并后的数据缺少 'time' 列")
                    return pd.DataFrame()
                # 去重：防止同一(time, code)重复（期货跨年后缀统一可能导致）
                result = result.drop_duplicates(subset=['time', 'code'], keep='last')
                result.set_index(['time', 'code'], inplace=True)
                return result
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"加载codebased K线数据失败: {e}")
            return pd.DataFrame()
    
    def _try_load_parquet_cache(self, market: str, freq: str, start_dt: pd.Timestamp,
                               end_dt: pd.Timestamp, codes: List[str], fields: List[str],
                               max_workers: int = 4) -> Optional[pd.DataFrame]:
        """
        尝试从Parquet缓存加载K线数据（优化版，支持跨年查询）

        Args:
            market: 市场名称
            freq: 频率
            start_dt: 开始时间
            end_dt: 结束时间
            codes: 股票代码列表
            fields: 需要的字段列表
            max_workers: 最大工作线程数

        Returns:
            DataFrame: 加载的数据，如果失败则返回None
        """
        try:
            # 获取所有涉及的年份
            start_year = start_dt.year
            end_year = end_dt.year
            years = list(range(start_year, end_year + 1))

            logger.debug(f"[Parquet] 查询跨越{len(years)}个年份: {years}")

            # 定义需要的列（列裁剪）
            required_columns = ['time', 'code'] + [f for f in fields if f in ['open', 'high', 'low', 'close', 'volume', 'amount']]

            all_data = []
            for year in years:
                parquet_file = os.path.join(
                    self.market_data_dir, 'kline', 'codebased', market, freq, f'{year}.parquet'
                )

                logger.debug(f"[Parquet] [{year}] 检查文件: {parquet_file}")

                # 检查Parquet文件是否存在
                if not os.path.exists(parquet_file):
                    logger.warning(f"[Parquet] [{year}] 文件不存在")
                    continue

                file_size = os.path.getsize(parquet_file) / 1024 / 1024 / 1024
                logger.debug(f"[Parquet] [{year}] 文件大小: {file_size:.2f}GB")

                try:
                    # 计算该年的过滤时间范围
                    year_start = pd.Timestamp(f'{year}-01-01 00:00:00')
                    year_end = pd.Timestamp(f'{year}-12-31 23:59:59')
                    filter_start = max(start_dt, year_start)
                    filter_end = min(end_dt, year_end)

                    # 使用多线程读取
                    try:
                        table = pq.read_table(
                            parquet_file,
                            columns=required_columns,
                            use_threads=True
                        )
                        df = table.to_pandas()

                        # 移除时区信息
                        if hasattr(df['time'].dt, 'tz') and df['time'].dt.tz is not None:
                            df['time'] = df['time'].dt.tz_localize(None)

                        # 过滤时间范围
                        before_time_filter = len(df)
                        df = df[(df['time'] >= filter_start) & (df['time'] <= filter_end)]
                        if len(df) < before_time_filter:
                            logger.debug(f"[Parquet] [{year}] 时间过滤: {before_time_filter:,} -> {len(df):,}")

                        if df.empty:
                            logger.debug(f"[Parquet] [{year}] 时间过滤后结果为空")
                            continue

                        # 过滤代码（期货支持多后缀匹配）
                        before_filter = len(df)
                        if market == 'cn_future':
                            # 期货：按base code（去掉后缀）匹配
                            base_codes = set(c.split('.')[0] if '.' in c else c for c in codes)
                            df['_base'] = df['code'].str.split('.').str[0]
                            df = df[df['_base'].isin(base_codes)]
                            df = df.drop(columns=['_base'])
                            # 将code统一为查询时的格式
                            code_map = {}
                            for c in codes:
                                base = c.split('.')[0] if '.' in c else c
                                code_map[base] = c
                            df['code'] = df['code'].apply(
                                lambda x: code_map.get(x.split('.')[0] if '.' in x else x, x)
                            )
                        else:
                            df = df[df['code'].isin(codes)]
                        if len(df) < before_filter:
                            logger.debug(f"[Parquet] [{year}] 代码过滤: {before_filter:,} -> {len(df):,}")

                        if len(df) == 0:
                            continue

                        # 数据已过滤，直接添加
                        all_data.append(df)

                    except Exception as e:
                        logger.warning(f"[Parquet] [{year}] 读取失败: {e}")
                        continue

                except Exception as e:
                    logger.warning(f"加载 {year}.parquet 失败: {e}")
                    continue

            if not all_data:
                logger.debug(f"[Parquet] 所有年份的数据加载后结果为空")
                return None

            # 合并所有年份的数据
            df = pd.concat(all_data, ignore_index=True)
            logger.debug(f"[Parquet] 合并后共 {len(df)} 条数据")

            # 期货去重：跨年后缀统一可能产生重复(time, code)
            if market == 'cn_future' and len(df) > 0:
                before = len(df)
                df = df.drop_duplicates(subset=['time', 'code'], keep='last')
                if len(df) < before:
                    logger.debug(f"[Parquet] 期货去重: {before} -> {len(df)}条")

            # 设置MultiIndex
            df = df.set_index(['time', 'code'])

            # 确保只包含请求的字段
            available_fields = [f for f in fields if f in df.columns]
            if available_fields:
                df = df[available_fields]

            # 排序索引
            df = df.sort_index()

            logger.debug(f"[Parquet] 加载成功: {len(df)} 条数据, {len(codes)} 个代码")
            return df

        except Exception as e:
            logger.warning(f"Parquet缓存加载失败: {e}, 将回退到CSV加载")
            import traceback
            traceback.print_exc()
            return None
    
    def _get_future_code_variants(self, symbol: str) -> List[str]:
        """获取期货代码的所有可能变体（不同年份的数据文件可能使用不同的交易所后缀）

        Args:
            symbol: 期货代码，可能带后缀也可能不带

        Returns:
            所有可能的代码变体列表
        """
        # 已知的交易所后缀变体映射（同一交易所在不同年份使用不同的后缀）
        EXCHANGE_SUFFIX_VARIANTS = {
            'CCFX': ['CCFX', 'CFFEX', 'CFX'],       # 中金所
            'SHFE': ['SHFE', 'XSGE', 'XSHF'],       # 上期所
            'DCE':  ['DCE', 'XDCE'],                  # 大商所
            'XZCE': ['XZCE', 'CZCE', 'XZCE'],        # 郑商所
            'GFEX': ['GFEX'],                         # 广期所
            'INE':  ['INE', 'XINE'],                  # 能源中心
        }

        # 品种代码到交易所的映射
        VARIETY_EXCHANGE = {
            'IF': 'CCFX', 'IH': 'CCFX', 'IC': 'CCFX', 'IM': 'CCFX',
            'TS': 'CCFX', 'TF': 'CCFX', 'T': 'CCFX', 'TL': 'CCFX',
            'CU': 'SHFE', 'AL': 'SHFE', 'ZN': 'SHFE', 'PB': 'SHFE',
            'NI': 'SHFE', 'SN': 'SHFE', 'AU': 'SHFE', 'AG': 'SHFE',
            'RB': 'SHFE', 'WR': 'SHFE', 'HC': 'SHFE', 'SS': 'SHFE',
            'FU': 'SHFE', 'BU': 'SHFE', 'RU': 'SHFE', 'SP': 'SHFE',
            'AO': 'SHFE',
            'A': 'DCE', 'B': 'DCE', 'M': 'DCE', 'Y': 'DCE',
            'P': 'DCE', 'C': 'DCE', 'CS': 'DCE', 'JD': 'DCE',
            'L': 'DCE', 'V': 'DCE', 'PP': 'DCE', 'FB': 'DCE',
            'BB': 'DCE', 'J': 'DCE', 'JM': 'DCE', 'I': 'DCE',
            'PG': 'DCE', 'EB': 'DCE', 'EG': 'DCE', 'LH': 'DCE',
            'SR': 'XZCE', 'CF': 'XZCE', 'TA': 'XZCE', 'OI': 'XZCE',
            'MA': 'XZCE', 'FG': 'XZCE', 'RM': 'XZCE', 'ZC': 'XZCE',
            'SF': 'XZCE', 'SM': 'XZCE', 'UR': 'XZCE', 'SA': 'XZCE',
            'PK': 'XZCE', 'AP': 'XZCE', 'CJ': 'XZCE', 'RS': 'XZCE',
            'RI': 'XZCE', 'JR': 'XZCE', 'LR': 'XZCE', 'WH': 'XZCE',
            'WT': 'XZCE', 'PM': 'XZCE',
            'SI': 'GFEX', 'LC': 'GFEX',
            'BC': 'INE', 'SC': 'INE', 'NR': 'INE', 'LU': 'INE',
        }

        variants = set()
        base = symbol

        # 去掉现有后缀
        if '.' in symbol:
            base = symbol.split('.')[0]

        # 查找品种对应的交易所
        exchange = None
        for prefix in sorted(VARIETY_EXCHANGE.keys(), key=len, reverse=True):
            if base.startswith(prefix):
                exchange = VARIETY_EXCHANGE[prefix]
                break

        if exchange and exchange in EXCHANGE_SUFFIX_VARIANTS:
            for suffix in EXCHANGE_SUFFIX_VARIANTS[exchange]:
                variants.add(f"{base}.{suffix}")
        else:
            # 无法识别，保留原始代码和去后缀的版本
            variants.add(symbol)
            if '.' in symbol:
                variants.add(base)

        # 也加入无后缀的原始代码（某些场景可能用到）
        variants.add(base)
        # 保留原始输入
        variants.add(symbol)

        return list(variants)

    def _get_cb_daterange(self, file_path):
        """读 codebased 文件首尾行，得到数据时间范围 (min_dt, max_dt)。按(mtime,size)缓存。

        用于跳过"查询区间在文件数据范围外"的查询——避免对部分数据币(如小币晚于查询期才上线)
        反复整文件逐行扫描后返回空。返回 None 表示无法判定(回退原行为)。
        """
        try:
            st = os.stat(file_path)
            mtime, size = st.st_mtime, st.st_size
        except OSError:
            return None
        cached = self._cb_daterange_cache.get(file_path)
        if cached and cached[0] == mtime and cached[1] == size:
            return (cached[2], cached[3])
        try:
            with open(file_path, 'r') as f:
                first = f.readline()
                f.seek(max(0, size - 8192))
                tail_lines = f.read().splitlines()
            last = tail_lines[-1] if tail_lines else ''
            if not first or not last:
                return None
            # CSV: time,code,... 取首列时间(前19字符 'YYYY-MM-DD HH:MM:SS')
            def _parse(line):
                t = line.split(',', 1)[0].strip()[:19]
                try:
                    return datetime.strptime(t, '%Y-%m-%d %H:%M:%S')
                except Exception:
                    try:
                        return datetime.strptime(t[:10], '%Y-%m-%d')
                    except Exception:
                        return None
            mn = _parse(first); mx = _parse(last)
            if mn and mx:
                self._cb_daterange_cache[file_path] = (mtime, size, mn, mx)
                return (mn, mx)
        except Exception:
            pass
        return None

    def _load_single_codebased_kline(self, market: str, symbol: str, freq: str,
                                    start_date: str, end_date: str, fields: List[str]) -> pd.DataFrame:
        """加载单个股票的codebased数据"""
        try:
            logger.debug(f"加载{symbol}的codebased数据: 市场={market}, 频率={freq}, 开始日期={start_date}, 结束日期={end_date}")
            logger.debug(f"[{symbol}] 参数类型: start_date类型={type(start_date)}, end_date类型={type(end_date)}")

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

                # 【期货多后缀支持】尝试多种后缀变体查找文件
                symbol_file = None
                actual_symbol = symbol  # 记录实际找到文件时的代码（用于保留原始code列）

                if market == 'cn_future':
                    code_variants = self._get_future_code_variants(symbol)
                    for variant in code_variants:
                        candidate = os.path.join(year_dir, f"{variant}.csv")
                        if os.path.exists(candidate):
                            symbol_file = candidate
                            actual_symbol = variant
                            logger.debug(f"[期货多后缀] 找到文件: {variant}.csv (查询: {symbol})")
                            break
                    if symbol_file is None:
                        # 最后尝试目录扫描：按base code前缀匹配
                        base = symbol.split('.')[0] if '.' in symbol else symbol
                        if os.path.exists(year_dir):
                            for fname in os.listdir(year_dir):
                                if fname.startswith(base + '.') and fname.endswith('.csv'):
                                    symbol_file = os.path.join(year_dir, fname)
                                    actual_symbol = fname[:-4]  # 去掉.csv
                                    logger.debug(f"[期货目录扫描] 找到文件: {fname} (查询: {symbol})")
                                    break
                else:
                    symbol_file = os.path.join(year_dir, f"{symbol}.csv")

                if symbol_file is None:
                    continue

                # 【性能】区间外快速跳过：若查询区间完全在该文件数据范围之外，直接跳过
                # （部分数据币如小币晚于查询期才上线，对早期区间逐行扫描必为空却反复执行，卡死回测）。
                try:
                    _qs = pd.to_datetime(start_date)
                    _qe = pd.to_datetime(end_date)
                    _dr = self._get_cb_daterange(symbol_file)
                    if _dr and (_qe < _dr[0] or _qs > _dr[1]):
                        continue
                except Exception:
                    pass

                try:
                    # 【性能优化】日期前缀预过滤：只读取匹配日期范围的行
                    # 原方式：read_csv读全量58000行 → strip+regex替换 → to_datetime → 过滤
                    # 优化后：按日期前缀过滤只读目标行(~480行) → slice去时区 → to_datetime
                    from io import StringIO as _StringIO
                    from datetime import date as _date, timedelta as _timedelta

                    # 计算日期前缀集合（支持跨年）
                    date_prefixes = set()
                    _sd = _date(int(start_date[:4]), int(start_date[5:7]), int(start_date[8:10]))
                    _ed = _date(int(end_date[:4]), int(end_date[5:7]), int(end_date[8:10]))
                    _d = _sd
                    while _d <= _ed:
                        date_prefixes.add(_d.isoformat())
                        _d += _timedelta(days=1)

                    # 按行预过滤：只保留时间列前10字符匹配目标日期的行
                    filtered_lines = []
                    with open(symbol_file, 'r') as _f:
                        for _line in _f:
                            if len(_line) > 10 and _line[:10] in date_prefixes:
                                filtered_lines.append(_line)

                    if not filtered_lines:
                        logger.debug(f"{year}年{symbol}数据: 日期范围内无数据")
                        continue

                    year_data = pd.read_csv(_StringIO('\n'.join(filtered_lines)), header=None,
                                          names=['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount'])

                    logger.debug(f"读取{year}年{symbol}数据: 预过滤后行数={len(year_data)}")

                    # 转换时间格式：用slice替代regex（"2023-06-01 09:30:00+08:00" → "2023-06-01 09:30:00"）
                    year_data['time'] = year_data['time'].str.slice(0, 19)
                    year_data['time'] = pd.to_datetime(year_data['time'], errors='coerce',
                                                        format='%Y-%m-%d %H:%M:%S')

                    # 【期货多后缀】将code列统一为查询时的symbol，确保后续过滤能匹配
                    if market == 'cn_future' and actual_symbol != symbol:
                        year_data['code'] = symbol

                    # 精确过滤（前缀过滤是粗筛，这里做精确时间范围过滤）
                    start_dt = pd.to_datetime(start_date).normalize()
                    end_dt = pd.to_datetime(end_date).normalize() + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

                    # 已通过slice去掉了时区，无需再tz_localize(None)
                    filtered_count = len(year_data)
                    year_data = year_data[(year_data['time'] >= start_dt) & (year_data['time'] <= end_dt)]

                    logger.debug(f"过滤后数据: 预过滤={filtered_count}, 精确过滤={len(year_data)}")

                    if not year_data.empty:
                        # 选择需要的字段（保留code列，期货去重时需要）
                        available_fields = ['time', 'code'] + [f for f in fields if f in year_data.columns]
                        year_data = year_data[available_fields]
                        all_data.append(year_data)

                except Exception as e:
                    logger.warning(f"读取{symbol}的{year}年数据失败: {e}")
                    continue
            
            if all_data:
                result = pd.concat(all_data, ignore_index=True)
                result = result.sort_values('time').reset_index(drop=True)
                # 期货多后缀去重：同合约跨年数据可能有重叠时间戳
                if market == 'cn_future' and len(result) > 0:
                    before = len(result)
                    result = result.drop_duplicates(subset=['time', 'code'], keep='last')
                    if len(result) < before:
                        logger.debug(f"期货{symbol}去重: {before} -> {len(result)}条")
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
            # 获取指定时间点的K线数据 - 扩大范围以确保包含目标时间
            # 对于日线数据，向前回溯30天以确保停牌股票也能获取最近价格
            # 对于分钟数据，查询当日00:00到23:59
            if freq == '1d':
                # 日线：向前回溯30天 + 未来7天，确保停牌股票也能获取最近价格
                start_time_dt = time - timedelta(days=30)
                start_time = start_time_dt.strftime('%Y-%m-%d 00:00:00')
                end_time_dt = time + timedelta(days=7)
                end_time = end_time_dt.strftime('%Y-%m-%d 23:59:59')
            else:
                # 分钟数据：查询当天整个交易时段
                start_time = time.strftime('%Y-%m-%d 00:00:00')
                end_time = time.strftime('%Y-%m-%d 23:59:59')

            logger.debug(f"[get_quotes] 查询时间范围: {start_time} - {end_time}")

            # 使用get_klines方法获取数据
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
                logger.debug(f"[get_quotes] klines_df shape={klines_df.shape}")

                # 查找指定时间点的数据
                target_time = time

                # 根据频率调整匹配策略
                if freq == '1d':
                    # 对于日线数据，优先匹配目标日期，无数据则取最近的前一个交易日
                    target_date = target_time.date()
                    time_index = klines_df.index.get_level_values('time')

                    # 先尝试精确匹配目标日期
                    if hasattr(time_index, 'date'):
                        exact_match = klines_df[time_index.date == target_date]
                    else:
                        exact_match = klines_df[time_index == target_time]

                    if not exact_match.empty:
                        matching_data = exact_match
                        # 检查是否有缺失的股票，为缺失的股票向前回溯
                        matched_codes = set(matching_data.index.get_level_values('code'))
                        missing_codes = [c for c in codes if c not in matched_codes]
                        if missing_codes:
                            logger.debug(f"[get_quotes] 以下股票在目标日期无数据，向前回溯: {missing_codes}")
                            # 筛选目标日期之前的数据
                            if hasattr(time_index, 'date'):
                                before_target = klines_df[time_index.date < target_date]
                            else:
                                before_target = klines_df[time_index < target_time]

                            if not before_target.empty:
                                for code in missing_codes:
                                    code_before = before_target[before_target.index.get_level_values('code') == code]
                                    if not code_before.empty:
                                        # 取最近的一条数据
                                        latest = code_before.groupby(level='code').tail(1)
                                        matching_data = pd.concat([matching_data, latest])
                                        logger.debug(f"[get_quotes] {code} 使用回溯价格: 日期={latest.index.get_level_values('time')[0]}")
                    else:
                        # 目标日期完全没有数据，取所有数据中每个code最新的
                        matching_data = klines_df.groupby(level='code').tail(1)
                        logger.debug(f"[get_quotes] 目标日期无数据，使用最近历史数据: {matching_data.shape}")

                    logger.debug(f"[get_quotes] 日线匹配后: matching_data.shape={matching_data.shape}")
                else:
                    # 对于分钟数据，找到小于等于目标时间的最近数据
                    time_index = klines_df.index.get_level_values('time')
                    # 只保留小于等于目标时间的数据
                    before_or_at_target = klines_df[time_index <= target_time]
                    if not before_or_at_target.empty:
                        # 按code分组，取每个code最新的数据
                        matching_data = before_or_at_target.groupby(level='code').tail(1)
                    else:
                        matching_data = before_or_at_target

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
                    # 如果精确时间没有数据，尝试查找最近的有效数据（向前回溯）
                    logger.debug(f"精确时间点 {time} 无数据，尝试查找最近数据")

                    if freq == '1d':
                        # 日线数据：向前回溯最多30天，找最近的交易日价格
                        lookback_start = (time - timedelta(days=30)).strftime('%Y-%m-%d 00:00:00')
                        lookback_end = time.strftime('%Y-%m-%d 23:59:59')

                        lookback_df = self.get_klines(
                            codes=codes,
                            market=market,
                            freq=freq,
                            start_date=lookback_start,
                            end_date=lookback_end,
                            fields=fields,
                            adj_type=adj_type,
                            use_cache=use_cache
                        )

                        if not lookback_df.empty:
                            closest_data = []
                            for code in codes:
                                code_data = lookback_df[lookback_df.index.get_level_values('code') == code]
                                if not code_data.empty:
                                    # 取该code在目标日期之前（含当日）的最新一条数据
                                    time_index = code_data.index.get_level_values('time')
                                    before_target = code_data[time_index <= time]
                                    if not before_target.empty:
                                        latest_row = before_target.groupby(level='code').tail(1).iloc[0]
                                        closest_data.append((code, latest_row))
                                    elif not code_data.empty:
                                        # 如果没有<=目标时间的，取所有数据中最新的
                                        latest_row = code_data.groupby(level='code').tail(1).iloc[0]
                                        closest_data.append((code, latest_row))

                            if closest_data:
                                quotes_df = pd.DataFrame([row for _, row in closest_data])
                                quotes_df['code'] = [code for code, _ in closest_data]
                                quotes_df.set_index('code', inplace=True)

                                if use_cache:
                                    self.kline_cache.put(cache_key, quotes_df)

                                found_codes = [code for code, _ in closest_data]
                                logger.debug(f"日线向前回溯找到最近数据: {found_codes}")
                                return quotes_df
                    else:
                        # 分钟数据：扩大时间范围到前后5分钟
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
                            target_timestamp = time.timestamp()
                            closest_data = []

                            for code in codes:
                                code_data = klines_df[klines_df.index.get_level_values('code') == code]
                                if not code_data.empty:
                                    time_values = code_data.index.get_level_values('time')
                                    time_diffs = abs(time_values.astype('int64') // 10**9 - target_timestamp)
                                    closest_idx = time_diffs.argmin()
                                    closest_timestamp = time_values[closest_idx]
                                    closest_row = code_data.loc[(closest_timestamp, code)]
                                    closest_data.append(closest_row)

                            if closest_data:
                                quotes_df = pd.DataFrame(closest_data)
                                symbols = [row.name[1] if isinstance(row.name, tuple) else row.name for row in closest_data]
                                quotes_df['code'] = symbols
                                quotes_df.set_index('code', inplace=True)

                                if use_cache:
                                    self.kline_cache.put(cache_key, quotes_df)

                                logger.debug(f"使用最近数据获取行情: {len(codes)} 只股票")
                                return quotes_df

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
            pd.DataFrame: 因子数据，MultiIndex(date, code)
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
                                df['code'] = code
                                df = df.reset_index().set_index(['time', 'code'])
                                all_factor_data.append(df)

                    except Exception as e:
                        logger.warning(f"读取或处理因子文件失败 {factor_file}: {e}")

        if not all_factor_data:
            logger.debug("No factor data loaded.")
            index = pd.MultiIndex.from_arrays([[], []], names=['time', 'code'])
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
            index = pd.MultiIndex.from_arrays([[], []], names=['time', 'code'])
            return pd.DataFrame(columns=factor_names, index=index)
        
        # 合并所有因子
        result_df = all_factors[0]
        for factor_df in all_factors[1:]:
            result_df = result_df.merge(factor_df, on=['code', 'time'], how='outer')
        
        # 设置索引
        if 'code' in result_df.columns and 'time' in result_df.columns:
            result_df['time'] = pd.to_datetime(result_df['time'])
            result_df = result_df.set_index(['time', 'code'])
            result_df.index.names = ['time', 'code']

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

        # 如果标准文件名不存在，尝试查找其他可能的文件名
        if not os.path.exists(list_file):
            ref_dir = os.path.join(self.reference_data_dir, market)
            if os.path.isdir(ref_dir):
                for f in os.listdir(ref_dir):
                    if f.endswith('_list.csv') or f.endswith('_lst.csv'):
                        list_file = os.path.join(ref_dir, f)
                        logger.debug(f"[StockList] 使用替代列表文件: {f}")
                        break

            # 如果reference目录也没找到，尝试在list目录查找
            if not os.path.exists(list_file):
                list_dir = os.path.join(self.market_data_dir, 'list')
                if os.path.isdir(list_dir):
                    for f in os.listdir(list_dir):
                        if f.startswith(market) and '_list' in f and f.endswith('.csv'):
                            list_file = os.path.join(list_dir, f)
                            logger.debug(f"[StockList] 使用list目录文件: {f}")
                            break

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

    def get_corporate_actions(self, market: str = 'cn_stock', query_date: Union[str, datetime.date] = None,
                              use_cache: bool = True) -> List[Dict[str, Any]]:
        """获取指定日期的公司行为数据（分红送股等）

        Args:
            market: 市场名称
            query_date: 查询日期
            use_cache: 是否使用缓存

        Returns:
            List[Dict]: 公司行为事件列表，每个事件包含:
                - symbol: 股票代码
                - action_date: 除权除息日
                - split_ratio: 送股比例（每10股送X股）
                - dividend_ratio: 分红比例（每10股派X元）
                - transfer_ratio: 转增比例（每10股转增X股）
        """
        try:
            # 转换日期格式
            if isinstance(query_date, date):
                date_str = query_date.strftime('%Y-%m-%d')
            elif isinstance(query_date, str):
                date_str = query_date
            else:
                date_str = datetime.now().strftime('%Y-%m-%d')

            # 尝试从reference目录读取分红送股数据
            # 优先使用 astock_finance_dividend.csv（Tushare格式）
            # 备选使用 market_corporate_actions.csv（通用格式）
            dividend_file = os.path.join(self.reference_data_dir, market, "astock_finance_dividend.csv")
            action_file = os.path.join(self.reference_data_dir, market, f"{market}_corporate_actions.csv")

            # 确定要使用的文件
            data_file = None
            use_tushare_format = False

            if os.path.exists(dividend_file):
                data_file = dividend_file
                use_tushare_format = True
            elif os.path.exists(action_file):
                data_file = action_file
                use_tushare_format = False
            else:
                logger.debug(f"公司行为文件不存在: {dividend_file} 或 {action_file}")
                return []

            # 读取CSV文件
            df = pd.read_csv(data_file)

            if use_tushare_format:
                # Tushare格式: astock_finance_dividend.csv
                # 列: code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,cash_div,cash_div_tax,record_date,ex_date,pay_date,...
                # 只处理 "实施" 状态的记录
                if 'div_proc' in df.columns:
                    df = df[df['div_proc'] == '实施']

                # 按 ex_date（除权除息日）过滤
                # ex_date 格式可能是浮点数 20240607.0 或整数 20240607 或字符串 2024-06-07
                if 'ex_date' in df.columns:
                    df = df[df['ex_date'].notna()]

                    # 将查询日期转换为整数进行比较 (2024-06-07 -> 20240607)
                    query_date_int = int(date_str.replace('-', ''))

                    # 处理不同格式的 ex_date
                    def normalize_ex_date(x):
                        if pd.isna(x):
                            return None
                        # 如果是浮点数或整数，直接转换为整数
                        if isinstance(x, (int, float)):
                            try:
                                return int(x)
                            except:
                                return None
                        # 如果是字符串，尝试解析
                        x = str(x).strip()
                        # 尝试 YYYYMMDD 格式
                        if len(x) == 8 and x.isdigit():
                            return int(x)
                        # 尝试 YYYY-MM-DD 格式
                        if '-' in x:
                            try:
                                return int(x.replace('-', ''))
                            except:
                                return None
                        return None

                    df['ex_date_normalized'] = df['ex_date'].apply(normalize_ex_date)
                    df = df[df['ex_date_normalized'] == query_date_int]
                else:
                    return []

                # 转换为字典列表
                result = []
                for _, row in df.iterrows():
                    # stk_bo_rate: 送股比例（每10股送X股）
                    # stk_co_rate: 转增比例（每10股转增X股）
                    # cash_div: 现金分红（每10股派X元）
                    # cash_div_tax: 扣税后现金分红

                    stk_bo_rate = row.get('stk_bo_rate', 0)
                    stk_co_rate = row.get('stk_co_rate', 0)
                    cash_div = row.get('cash_div', 0)

                    # 跳过没有任何公司行为的记录
                    if pd.isna(stk_bo_rate) and pd.isna(stk_co_rate) and pd.isna(cash_div):
                        continue

                    action = {
                        'symbol': row.get('code', ''),
                        'action_date': date_str,
                        'split_ratio': float(stk_bo_rate) if pd.notna(stk_bo_rate) else 0,
                        'dividend_ratio': float(cash_div) if pd.notna(cash_div) else 0,
                        'transfer_ratio': float(stk_co_rate) if pd.notna(stk_co_rate) else 0,
                        'record_date': row.get('record_date', ''),
                    }
                    result.append(action)

            else:
                # 通用格式: market_corporate_actions.csv
                # 过滤指定日期的数据
                if 'action_date' in df.columns:
                    df['action_date'] = pd.to_datetime(df['action_date']).dt.strftime('%Y-%m-%d')
                    df = df[df['action_date'] == date_str]
                elif 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                    df = df[df['date'] == date_str]

                if df.empty:
                    return []

                # 转换为字典列表
                result = []
                for _, row in df.iterrows():
                    action = {
                        'symbol': row.get('symbol', row.get('code', '')),
                        'action_date': row.get('action_date', row.get('date', date_str)),
                        'split_ratio': row.get('split_ratio', row.get('bonus_ratio', 0)),
                        'dividend_ratio': row.get('dividend_ratio', row.get('cash_dividend', 0)),
                        'transfer_ratio': row.get('transfer_ratio', 0),
                        'record_date': row.get('record_date', ''),
                    }
                    result.append(action)

            if result:
                logger.info(f"[公司行为] {date_str} 找到{len(result)}条事件")
                for action in result:
                    logger.debug(f"  {action['symbol']}: 送股={action['split_ratio']}, "
                                f"分红={action['dividend_ratio']}, 转增={action['transfer_ratio']}")

            return result

        except Exception as e:
            logger.warning(f"获取公司行为数据失败: {e}")
            return []
    
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
            symbols = df.index.get_level_values('code').unique().tolist()
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
                if symbol not in df.index.get_level_values('code'):
                    continue
                
                # 获取该股票的复权因子
                symbol_adj = adj_factors[adj_factors['code'] == symbol].copy()
                if symbol_adj.empty:
                    continue
                
                # 获取该股票的K线数据
                symbol_data = result_df.xs(symbol, level='code').reset_index()

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
                merged_data['code'] = symbol
                merged_data = merged_data.set_index(['code'], append=True).reorder_levels(['time', 'code'])

                # 更新result_df中对应股票的数据
                result_df.update(merged_data[available_price_fields])
            
            logger.debug(f"应用{adj_type}复权完成，处理{len(symbols)}只股票")
            return result_df
            
        except Exception as e:
            logger.warning(f"应用复权失败: {e}，返回原始数据")
            return df

    def _optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """优化DataFrame的数据类型以减少内存占用

        Args:
            df: 原始DataFrame

        Returns:
            优化后的DataFrame
        """
        if df.empty:
            return df

        try:
            original_memory = df.memory_usage(deep=True).sum()

            # 优化价格列 (float64 -> float32)
            price_columns = ['open', 'high', 'low', 'close']
            for col in price_columns:
                if col in df.columns and df[col].dtype == 'float64':
                    df[col] = df[col].astype('float32')

            # 优化成交量 (int64 -> int32)
            if 'volume' in df.columns and df['volume'].dtype == 'int64':
                df['volume'] = df['volume'].astype('int32')

            # 优化成交额 (float64 -> float32)
            if 'amount' in df.columns and df['amount'].dtype == 'float64':
                df['amount'] = df['amount'].astype('float32')

            new_memory = df.memory_usage(deep=True).sum()
            if original_memory > 0:
                saved_pct = (original_memory - new_memory) / original_memory * 100
                if saved_pct > 1:  # 只记录显著的节省
                    logger.debug(f"[内存优化] 数据类型优化节省 {saved_pct:.1f}% 内存 ({original_memory/1024**2:.1f}MB -> {new_memory/1024**2:.1f}MB)")

        except Exception as e:
            logger.debug(f"[内存优化] 数据类型优化失败: {e}")

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
