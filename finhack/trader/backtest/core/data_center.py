"""
数据中心

负责管理回测中的所有数据，包括行情数据、因子数据、参考数据等
支持多市场多频次，从真实数据源加载
现已重构为使用统一的数据接口，并实现按月预加载策略
"""

import logging
import os
import pickle
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional, Union, Callable
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import psutil
import talib

from finhack.library.data import get_data_interface

logger = logging.getLogger(__name__)

# 期货代码标准化映射表（小写交易所代码到大写）
_FUTURE_EXCHANGE_SUFFIX_MAP = {
    'cffex': 'CFFEX',
    'shfe': 'SHFE',
    'dce': 'DCE',
    'czce': 'XZCE',
    'gfex': 'GFEX',
}

# 期货品种代码到交易所映射
_FUTURE_CODE_EXCHANGE_MAP = {
    # 中金所
    'IF': 'CFFEX', 'IH': 'CFFEX', 'IC': 'CFFEX', 'IM': 'CFFEX',
    'TS': 'CFFEX', 'TF': 'CFFEX', 'T': 'CFFEX', 'TL': 'CFFEX',
    # 上期所
    'CU': 'SHFE', 'AL': 'SHFE', 'ZN': 'SHFE', 'PB': 'SHFE',
    'NI': 'SHFE', 'SN': 'SHFE', 'AU': 'SHFE', 'AG': 'SHFE',
    'RB': 'SHFE', 'WR': 'SHFE', 'HC': 'SHFE', 'SS': 'SHFE',
    'FU': 'SHFE', 'BU': 'SHFE', 'RU': 'SHFE', 'SP': 'SHFE',
    'AO': 'SHFE',
    # 大商所
    'A': 'DCE', 'B': 'DCE', 'M': 'DCE', 'Y': 'DCE',
    'P': 'DCE', 'C': 'DCE', 'CS': 'DCE', 'JD': 'DCE',
    'L': 'DCE', 'V': 'DCE', 'PP': 'DCE', 'FB': 'DCE',
    'BB': 'DCE', 'J': 'DCE', 'JM': 'DCE', 'I': 'DCE',
    'PG': 'DCE', 'EB': 'DCE', 'EG': 'DCE', 'LH': 'DCE',
    # 郑商所
    'SR': 'XZCE', 'CF': 'XZCE', 'TA': 'XZCE', 'OI': 'XZCE',
    'MA': 'XZCE', 'FG': 'XZCE', 'RM': 'XZCE', 'ZC': 'XZCE',
    'SF': 'XZCE', 'SM': 'XZCE', 'UR': 'XZCE', 'SA': 'XZCE',
    'PK': 'XZCE', 'AP': 'XZCE', 'CJ': 'XZCE', 'RS': 'XZCE',
    'RI': 'XZCE', 'JR': 'XZCE', 'LR': 'XZCE', 'WH': 'XZCE',
    'WT': 'XZCE', 'PM': 'XZCE',
    # 广期所
    'SI': 'GFEX', 'LC': 'GFEX',
}


def _normalize_future_code(code: str) -> str:
    """标准化期货代码，添加交易所后缀

    Args:
        code: 期货代码，可能带有或不带有交易所后缀

    Returns:
        带有交易所后缀的标准化代码，如 'IF2401.CFFEX'
    """
    # 如果已经有后缀，需要统一格式（reference文件中可能用.CFX，数据文件用.CFFEX）
    if '.' in code:
        # 从带后缀的代码中提取品种前缀，重新匹配标准后缀
        base = code.split('.')[0]
        for prefix in sorted(_FUTURE_CODE_EXCHANGE_MAP.keys(), key=len, reverse=True):
            if base.startswith(prefix):
                exchange_suffix = _FUTURE_CODE_EXCHANGE_MAP[prefix]
                return f"{base}.{exchange_suffix}"
        # 如果无法识别前缀，保留原样
        return code

    # 提取品种代码前缀
    for prefix in sorted(_FUTURE_CODE_EXCHANGE_MAP.keys(), key=len, reverse=True):
        if code.startswith(prefix):
            exchange_suffix = _FUTURE_CODE_EXCHANGE_MAP[prefix]
            return f"{code}.{exchange_suffix}"

    # 如果无法识别，默认添加中金所后缀
    return f"{code}.CFFEX"


class DataCenter:
    """数据中心
    
    管理回测中的所有数据，支持多市场多频次，实现按月预加载策略
    """
    
    def __init__(self, project_path: str, market: str = 'cn_stock', freq: str = '1d'):
        """初始化数据中心
        
        Args:
            project_path: 项目路径
            market: 市场名称
            freq: 数据频率
        """
        self.project_path = project_path
        self.market = market
        self.freq = freq
        self.context = None
        
        # 获取统一数据接口实例
        self.data_interface = get_data_interface(project_path)
        
        # 保持向后兼容性的属性
        self.base_data_dir = os.path.join(project_path, 'data')
        self.market_data_dir = os.path.join(self.base_data_dir, 'market')
        self.factors_data_dir = os.path.join(self.base_data_dir, 'factors')
        self.reference_data_dir = os.path.join(self.base_data_dir, 'market', 'reference')

        # 多频率数据缓存
        self.kline_cache = {}  # 格式: {market: {freq: {symbol: DataFrame}}}
        self.factor_cache = {}  # 格式: {market: {freq: {factor_name: DataFrame}}}

        # 【新增】缓存线程锁（确保线程安全）
        self._kline_cache_lock = threading.RLock()  # 使用RLock支持同线程重入
        self._factor_cache_lock = threading.RLock()

        # 支持的频率列表
        self.supported_frequencies = ['1m', '30m', '120m', '1d']

        # 按月预加载策略相关属性
        self.preloaded_months = {}  # 格式: {market: {year_month: datetime}}
        self.preload_lock = threading.Lock()  # 线程锁，确保预加载过程线程安全
        # 增加线程池大小以提高并行加载性能
        import os as _os
        cpu_count = _os.cpu_count() or 4
        max_workers = min(cpu_count, 8)  # 最多8个worker，避免过多线程
        self.preload_thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        logger.info(f"数据预加载线程池初始化: {max_workers} workers")

        # 【优化】Parquet元数据缓存，带过期机制
        self._parquet_meta_cache = {}  # 格式: {file_path: (metadata, cache_time)}
        self._parquet_meta_cache_ttl = 300  # 缓存过期时间：5分钟
        self._parquet_meta_cache_lock = threading.Lock()

        # 【优化】读取配置选项
        self._use_memory_map = False  # 默认不启用内存映射（对大文件可能更快）
        self._min_rows_for_dtype_opt = 500000  # 数据类型优化的最小行数

        # 【新增】股票交易状态管理（停牌/退市）
        self._trading_status = {}  # 格式: {symbol: {'status': 'active'/'suspended'/'delisted', 'reason': str, 'since': datetime}}
        self._trading_status_lock = threading.Lock()

        # 【性能优化】分钟级价格快速查找缓存
        # 格式: {cache_key_minute: {symbol: {'close': float, 'volume': float}}}
        # 避免每次TRY_MATCH都做DataFrame copy + filter + time slice
        self._minute_price_cache = {}
        self._minute_price_cache_key = None  # 当前缓存的月份key
        self._minute_price_cache_lock = threading.Lock()

        # 【性能优化】日级别K线缓存 - 用于1m频率的快速价格查询
        # 避免月度缓存未命中时（如crypto>100标的跳过预加载）每次查询都触发磁盘加载
        # 格式: {(code, freq, date_str): DataFrame}
        self._daily_klines_cache = {}
        self._daily_cache_date = None  # 当前缓存对应的日期
        self._daily_cache_lock = threading.RLock()

        # 【性能优化】每代码首根分钟K线索引 - 解决crypto 00:00(UTC+8)查询时首根K线在08:01的时差问题
        self._daily_price_first_minute = {}  # {code: minute_key_str}

        # 【性能诊断】数据API计时统计
        self._perf_api_timings = {}  # {api_name: [total_sec, count]}

        # 【性能优化】分钟级价格字典 - O(1)查找替代DataFrame切片
        # 格式: {(code, '2023-06-01 10:30'): {'open': float, 'close': float, ...}}
        self._daily_price_dict = {}
        self._daily_price_dict_date = None

        logger.info(f"数据中心初始化完成: {market} {freq} (使用统一数据接口，支持按月预加载)")

    def _get_parquet_metadata(self, parquet_file: str):
        """获取Parquet文件元数据，带缓存和过期机制

        Args:
            parquet_file: Parquet文件路径

        Returns:
            Parquet文件元数据
        """
        with self._parquet_meta_cache_lock:
            now = datetime.now()
            # 检查缓存是否存在且未过期
            if parquet_file in self._parquet_meta_cache:
                metadata, cache_time = self._parquet_meta_cache[parquet_file]
                if (now - cache_time).total_seconds() < self._parquet_meta_cache_ttl:
                    logger.debug(f"[Parquet缓存] 命中: {parquet_file}")
                    return metadata
                else:
                    logger.debug(f"[Parquet缓存] 过期: {parquet_file}")
                    del self._parquet_meta_cache[parquet_file]

            # 缓存未命中或已过期，读取元数据
            try:
                import pyarrow.parquet as pq
                metadata = pq.ParquetFile(parquet_file).metadata
                self._parquet_meta_cache[parquet_file] = (metadata, now)
                logger.debug(f"[Parquet缓存] 加载: {parquet_file}")
                return metadata
            except Exception as e:
                logger.warning(f"[Parquet缓存] 读取元数据失败: {parquet_file}, {e}")
                return None

    def clear_parquet_metadata_cache(self):
        """主动清除Parquet元数据缓存"""
        with self._parquet_meta_cache_lock:
            count = len(self._parquet_meta_cache)
            self._parquet_meta_cache.clear()
            logger.info(f"[Parquet缓存] 已清除 {count} 个元数据缓存")

    def set_performance_options(self, use_memory_map: bool = None,
                                min_rows_for_dtype_opt: int = None,
                                parquet_meta_cache_ttl: int = None):
        """设置性能优化选项

        Args:
            use_memory_map: 是否使用内存映射读取Parquet文件
            min_rows_for_dtype_opt: 数据类型优化的最小行数阈值
            parquet_meta_cache_ttl: Parquet元数据缓存过期时间（秒）
        """
        if use_memory_map is not None:
            self._use_memory_map = use_memory_map
            logger.debug(f"[性能配置] 内存映射: {use_memory_map}")

        if min_rows_for_dtype_opt is not None:
            self._min_rows_for_dtype_opt = min_rows_for_dtype_opt
            logger.debug(f"[性能配置] 数据类型优化阈值: {min_rows_for_dtype_opt:,}行")

        if parquet_meta_cache_ttl is not None:
            self._parquet_meta_cache_ttl = parquet_meta_cache_ttl
            logger.debug(f"[性能配置] 元数据缓存TTL: {parquet_meta_cache_ttl}秒")

    def update_trading_status(self, symbol: str, status: str, reason: str = ""):
        """更新股票交易状态（停牌/复牌/退市）

        Args:
            symbol: 股票代码
            status: 交易状态 ('active':正常交易, 'suspended':停牌, 'delisted':退市)
            reason: 状态变更原因
        """
        with self._trading_status_lock:
            from datetime import datetime
            self._trading_status[symbol] = {
                'status': status,
                'reason': reason,
                'since': datetime.now()
            }
            logger.info(f"[交易状态] {symbol} 状态更新为: {status}, 原因: {reason}")

    def get_trading_status(self, symbol: str) -> dict:
        """获取股票交易状态

        Args:
            symbol: 股票代码

        Returns:
            交易状态字典，如果未记录则返回正常交易状态
        """
        with self._trading_status_lock:
            return self._trading_status.get(symbol, {
                'status': 'active',
                'reason': '',
                'since': None
            })

    def is_tradable(self, symbol: str) -> bool:
        """检查股票是否可交易

        Args:
            symbol: 股票代码

        Returns:
            True: 可交易, False: 停牌或退市
        """
        status_info = self.get_trading_status(symbol)
        return status_info['status'] == 'active'

    def get_suspended_stocks(self) -> list:
        """获取当前停牌的股票列表

        Returns:
            停牌股票代码列表
        """
        with self._trading_status_lock:
            return [symbol for symbol, info in self._trading_status.items()
                    if info['status'] == 'suspended']

    def get_delisted_stocks(self) -> list:
        """获取已退市的股票列表

        Returns:
            退市股票代码列表
        """
        with self._trading_status_lock:
            return [symbol for symbol, info in self._trading_status.items()
                    if info['status'] == 'delisted']

    def clear_trading_status(self):
        """清除所有交易状态记录"""
        with self._trading_status_lock:
            count = len(self._trading_status)
            self._trading_status.clear()
            logger.info(f"[交易状态] 已清除 {count} 条状态记录")

    def set_context(self, context: Dict[str, Any]):
        """设置上下文
        
        Args:
            context: 回测上下文
        """
        self.context = context
        logger.debug("数据中心已设置上下文")
    
    def preload_monthly_data(self, market: str, year: int, month: int,
                            universe: List[str] = None, frequency: str = '1m'):
        """预加载指定月份的1分钟数据 - 优化版（直接使用Parquet批量加载）

        Args:
            market: 市场名称
            year: 年份
            month: 月份
            universe: 股票池，如果为空则只加载当前策略需要的股票
            frequency: 数据频率
        """
        month_key = f"{year}-{month:02d}"

        # 检查是否已经预加载
        with self.preload_lock:
            if market in self.preloaded_months and month_key in self.preloaded_months[market]:
                logger.debug(f"{market} {month_key} 数据已预加载，跳过")
                return

        # 如果没有提供universe，尝试从context中获取
        if universe is None and self.context:
            universe = []
            context_universe = self.context.get('universe', None)
            if context_universe:
                if isinstance(context_universe, dict):
                    if market in context_universe:
                        universe = context_universe[market]
                else:
                    universe = context_universe

        # 如果仍然没有universe，则加载全市场数据
        if not universe:
            logger.debug(f"没有提供股票池，将加载 {market} 全市场数据进行预加载")
            try:
                stock_list_df = self.data_interface.get_stock_list(market, use_cache=True)
                if 'code' in stock_list_df.columns:
                    universe = stock_list_df['code'].tolist()
                else:
                    universe = stock_list_df.index.tolist()
                logger.debug(f"成功获取 {market} 全市场股票列表，共 {len(universe)} 只")
            except Exception as e:
                logger.error(f"获取 {market} 股票列表失败: {e}")
                return

            # 对于1m数据，如果标的数量过多（如crypto全市场），跳过预加载以避免内存爆炸
            if frequency == '1m' and len(universe) > 100:
                logger.debug(f"[预加载] {market} 1m数据标的数 {len(universe)} 超过100，"
                           f"跳过预加载（改用按需加载）")
                with self.preload_lock:
                    self.preloaded_months.setdefault(market, {})[month_key] = datetime.now()
                return

        # 对期货市场代码进行标准化（reference文件中可能用.CFX，数据文件用交易所全称后缀）
        if market == 'cn_future':
            universe = [_normalize_future_code(code) for code in universe]
            # 去重
            universe = list(dict.fromkeys(universe))
            logger.debug(f"[预加载] 期货代码标准化后: {len(universe)} 只")

        import time
        start_time = time.time()
        logger.debug(f"[预加载] 开始预加载 {market} {month_key} 的{frequency}数据，股票数量: {len(universe)}")
        print(f"[预加载] 开始加载 {market} {month_key}，共{len(universe)}只股票", flush=True)

        try:
            # 计算月份的开始和结束日期
            # 对于1m频率，start_date前移1天以捕获前一天文件中的早盘数据
            if frequency == '1m':
                start_date = datetime(year, month, 1, 0, 0, 0) - timedelta(days=1)
            else:
                start_date = datetime(year, month, 1, 0, 0, 0)
            if month == 12:
                end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
                end_date = end_date.replace(hour=23, minute=59, second=59)
            else:
                end_date = datetime(year, month + 1, 1) - timedelta(days=1)
                end_date = end_date.replace(hour=23, minute=59, second=59)

            # 【优化】优先使用Parquet批量加载，而不是分批加载CSV
            # Parquet支持列裁剪和行过滤，内存效率高，速度快
            parquet_file = os.path.join(
                self.data_interface.market_data_dir, 'kline', 'codebased',
                market, frequency, f'{year}.parquet'
            )

            total_records = 0

            if os.path.exists(parquet_file):
                # 使用Parquet批量加载（推荐方式）
                logger.debug(f"[预加载] 使用Parquet批量加载: {parquet_file}")
                print(f"[预加载] 使用Parquet批量加载...", flush=True)

                # 定义需要的字段
                required_columns = ['time', 'code'] + ['open', 'high', 'low', 'close', 'volume', 'amount']

                try:
                    import pyarrow.parquet as pq

                    # 读取parquet文件
                    table = pq.read_table(
                        parquet_file,
                        columns=required_columns,
                        use_threads=True
                    )
                    df = table.to_pandas()

                    logger.debug(f"[预加载] Parquet文件大小: {len(df):,}行")

                    # 移除时区信息
                    if hasattr(df['time'].dt, 'tz') and df['time'].dt.tz is not None:
                        df['time'] = df['time'].dt.tz_localize(None)

                    # 先按code过滤（快速过滤）
                    if market == 'cn_future':
                        # 期货：按base code（去掉交易所后缀）匹配，因为不同年份后缀不同
                        base_codes = set(c.split('.')[0] if '.' in c else c for c in universe)
                        df['_base'] = df['code'].str.split('.').str[0]
                        df = df[df['_base'].isin(base_codes)]
                        # 将code统一为查询时的格式
                        code_map = {}
                        for c in universe:
                            base = c.split('.')[0] if '.' in c else c
                            code_map[base] = c
                        df['code'] = df['code'].apply(
                            lambda x: code_map.get(x.split('.')[0] if '.' in x else x, x)
                        )
                        df = df.drop(columns=['_base'])
                    else:
                        df = df[df['code'].isin(universe)]
                    logger.debug(f"[预加载] 代码过滤后: {len(df):,}行")

                    # 再按时间过滤
                    df = df[(df['time'] >= start_date) & (df['time'] <= end_date)]
                    logger.debug(f"[预加载] 时间过滤后: {len(df):,}行")

                    if not df.empty:
                        # 设置MultiIndex
                        df = df.set_index(['time', 'code'])
                        df = df.sort_index()

                        # 按月份分组缓存
                        df['month'] = df.index.get_level_values('time').strftime('%Y-%m')
                        for month_group, month_df in df.groupby('month'):
                            cache_key = f"{market}_{frequency}_{month_group}"
                            month_df = month_df.drop(columns=['month'])
                            # 【线程安全】使用锁保护缓存写入
                            with self._kline_cache_lock:
                                self.kline_cache[cache_key] = month_df
                            logger.debug(f"[预加载] 缓存月份 {month_group}: {len(month_df):,}行")

                        total_records = len(df)

                except Exception as e:
                    logger.warning(f"[预加载] Parquet加载失败，回退到分批加载: {e}")
                    # 回退到原有的分批加载方式
                    total_records = self._fallback_batch_load(market, universe, frequency, start_date, end_date)

            else:
                # Parquet文件不存在，使用分批加载CSV的方式
                logger.debug(f"[预加载] Parquet文件不存在: {parquet_file}")
                logger.debug(f"[预加载] 使用CSV分批加载方式")
                total_records = self._fallback_batch_load(market, universe, frequency, start_date, end_date)

            # 标记该月数据已预加载
            with self.preload_lock:
                if market not in self.preloaded_months:
                    self.preloaded_months[market] = {}
                self.preloaded_months[market][month_key] = datetime.now()

            elapsed = time.time() - start_time
            logger.info(f"[预加载] 完成预加载 {market} {month_key}，共 {len(universe)} 只股票，{total_records} 条记录，耗时 {elapsed:.2f}秒")
            print(f"[预加载] ✓ 完成！共{total_records}条记录，耗时{elapsed:.2f}秒", flush=True)

        except Exception as e:
            logger.error(f"预加载 {market} {month_key} 数据失败: {e}")
            raise

    def _fallback_batch_load(self, market: str, universe: List[str], frequency: str,
                             start_date: datetime, end_date: datetime) -> int:
        """回退方式：分批加载CSV数据（兼容性保留）

        Args:
            market: 市场名称
            universe: 股票池
            frequency: 数据频率
            start_date: 开始时间
            end_date: 结束时间

        Returns:
            int: 加载的记录总数
        """
        total_records = 0

        # 计算批次大小
        calculated_batch = self._calculate_adaptive_batch_size(len(universe), frequency)
        if len(universe) > 1000:
            batch_size = max(1000, calculated_batch)
        else:
            batch_size = calculated_batch

        batches = [universe[i:i + batch_size] for i in range(0, len(universe), batch_size)]
        logger.debug(f"[预加载] CSV分批加载: {len(batches)}个批次，每批{batch_size}只")
        print(f"[预加载] 分为{len(batches)}个批次进行并行加载", flush=True)

        # 使用线程池并行加载
        futures = {}
        for idx, batch in enumerate(batches):
            future = self.preload_thread_pool.submit(
                self._preload_batch, market, batch, frequency, start_date, end_date
            )
            futures[future] = (idx, batch)

        # 等待所有批次加载完成
        completed = 0
        for future in as_completed(futures):
            try:
                batch_idx, batch = futures[future]
                result = future.result(timeout=120)
                total_records += sum(result.values())
                completed += 1
                logger.debug(f"[预加载] 批次 {completed}/{len(batches)} 完成")
                print(f"[预加载] 进度: {completed}/{len(batches)} ({completed*100//len(batches)}%)", flush=True)
            except Exception as e:
                logger.error(f"[预加载] 批次预加载失败: {e}")

        return total_records

    def _calculate_adaptive_batch_size(self, total_stocks: int, frequency: str = '1m') -> int:
        """
        根据系统资源动态计算最优批次大小

        Args:
            total_stocks: 总股票数量
            frequency: 数据频率 ('1m' 或 '1d')

        Returns:
            int: 最优批次大小
        """
        # 配置参数
        memory_per_stock_1m = 200  # MB，每只股票分钟数据预估内存使用
        memory_per_stock_1d = 50   # MB，每只股票日线数据预估内存使用
        cpu_utilization_target = 0.75  # 目标CPU使用率
        min_batch_size = 2       # 最小批次大小
        max_batch_size = 500     # 最大批次大小（支持大批量Parquet加载）

        try:
            # 获取系统资源
            available_memory_gb = psutil.virtual_memory().available / (1024**3)
            cpu_cores = psutil.cpu_count(logical=True)
            current_cpu_usage = psutil.cpu_percent(interval=0.1) / 100.0

            # 根据数据频率调整内存预估
            if frequency == '1m':
                memory_per_stock = memory_per_stock_1m
            else:
                memory_per_stock = memory_per_stock_1d

            # 基于内存限制计算批次大小
            # 保守估计：使用60%的可用内存，留出安全边际
            memory_based_batch = int(
                (available_memory_gb * 0.6 * 1024) / memory_per_stock
            )

            # 基于CPU核心数和当前负载计算批次大小
            if current_cpu_usage < cpu_utilization_target:
                cpu_multiplier = 2.0  # CPU空闲，可以增加批次
            elif current_cpu_usage < 0.9:
                cpu_multiplier = 1.5  # CPU适中
            else:
                cpu_multiplier = 1.0  # CPU繁忙，保持保守

            cpu_based_batch = int(cpu_cores * cpu_multiplier)

            # 基于总股票数调整，避免过多小批次
            if total_stocks <= 10:
                total_stock_adjusted = max(2, total_stocks // 2)
            elif total_stocks <= 50:
                total_stock_adjusted = 8
            elif total_stocks <= 200:
                total_stock_adjusted = 15
            elif total_stocks <= 1000:
                total_stock_adjusted = 25
            else:
                total_stock_adjusted = 40

            # 取三个因素的加权平均，并确保在合理范围内
            candidates = [
                max(memory_based_batch, min_batch_size),
                max(cpu_based_batch, min_batch_size),
                max(total_stock_adjusted, min_batch_size)
            ]

            # 使用加权平均，但加强总股票数的权重，避免过度优化
            optimal_batch = int(
                candidates[0] * 0.3 +  # 内存权重30%
                candidates[1] * 0.3 +  # CPU权重30%
                candidates[2] * 0.4    # 总股票数权重40%（提高权重，避免无限制增大批次）
            )

            # 确保在合理范围内
            optimal_batch = max(min(optimal_batch, max_batch_size), min_batch_size)

            # 记录详细信息
            logger.debug(f"[批次计算] 可用内存: {available_memory_gb:.2f}GB, "
                       f"CPU核心: {cpu_cores}, 当前CPU使用率: {current_cpu_usage:.1%}")
            logger.debug(f"[批次计算] 内存建议: {candidates[0]}, CPU建议: {candidates[1]}, "
                       f"规模建议: {candidates[2]}, 最终采用: {optimal_batch}")

            return optimal_batch

        except Exception as e:
            logger.warning(f"[批次计算] 动态计算失败，使用默认值: {e}")
            # 降级到基于经验的默认值
            if total_stocks <= 20:
                return 3
            elif total_stocks <= 100:
                return 5
            elif total_stocks <= 500:
                return 10
            else:
                return 20

    def _preload_batch(self, market: str, batch: List[str], frequency: str,
                      start_date: datetime, end_date: datetime) -> Dict[str, int]:
        """预加载一批股票的数据并缓存到kline_cache

        Args:
            market: 市场名称
            batch: 股票代码批次
            frequency: 数据频率
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            Dict[str, int]: 每只股票加载的记录数
        """
        import time
        batch_start = time.time()
        results = {}

        try:
            logger.debug(f"[预加载批次] 开始加载 {batch}")

            # 使用数据接口批量获取K线数据（保留完整时间信息）
            klines_df = self.data_interface.get_klines(
                codes=batch,
                market=market,
                freq=frequency,
                start_date=start_date.strftime('%Y-%m-%d %H:%M:%S'),
                end_date=end_date.strftime('%Y-%m-%d %H:%M:%S'),
                use_cache=True
            )

            # 统计每只股票的记录数
            if not klines_df.empty:
                for symbol in batch:
                    symbol_data = klines_df.xs(symbol, level=1) if symbol in klines_df.index.get_level_values(1) else pd.DataFrame()
                    results[symbol] = len(symbol_data)

                # 缓存到kline_cache（按月份分组）
                if hasattr(klines_df.index, 'get_level_values') and 'time' in klines_df.index.names:
                    time_level = klines_df.index.get_level_values('time')
                    months = time_level.strftime('%Y-%m').unique()
                    for month_str in months:
                        cache_key = f"{market}_{frequency}_{month_str}"
                        month_mask = time_level.strftime('%Y-%m') == month_str
                        month_df = klines_df[month_mask]
                        with self._kline_cache_lock:
                            if cache_key in self.kline_cache:
                                existing = self.kline_cache[cache_key]
                                month_df = pd.concat([existing, month_df])
                                month_df = month_df[~month_df.index.duplicated(keep='last')]
                                month_df = month_df.sort_index()
                            self.kline_cache[cache_key] = month_df
                        logger.debug(f"[预加载] 缓存月份 {month_str}: {len(month_df):,}行")
            else:
                results = {symbol: 0 for symbol in batch}

            elapsed = time.time() - batch_start
            total_records = sum(results.values())
            logger.debug(f"[预加载批次] 完成 {batch}，{total_records}条记录，耗时{elapsed:.2f}秒")

            return results

        except Exception as e:
            elapsed = time.time() - batch_start
            logger.error(f"[预加载批次] 失败 {batch}，耗时{elapsed:.2f}秒: {e}")
            import traceback
            traceback.print_exc()
            return {symbol: 0 for symbol in batch}
    
    def ensure_monthly_data_loaded(self, market: str, current_date: datetime,
                                 universe: List[str] = None, frequency: str = '1m'):
        """确保当前月份的数据已预加载 - 优化版（支持配置额外预加载历史月数）

        优化策略：按年份组织加载，避免重复加载同一Parquet文件

        Args:
            market: 市场名称
            current_date: 当前日期
            universe: 股票池
            frequency: 数据频率
        """
        # 当前月份
        current_year = current_date.year
        current_month = current_date.month

        # 获取额外预加载月数的配置（默认1个月，减少内存占用）
        extra_months = 1
        if self.context and hasattr(self.context, 'data_config'):
            extra_months = getattr(self.context.data_config, 'preload_extra_months', 1)

        # 【内存保护】检查当前内存使用情况，如果内存不足则减少预加载月数
        import psutil
        mem = psutil.virtual_memory()
        available_gb = mem.available / (1024**3)
        if available_gb < 20:  # 如果可用内存小于20GB
            logger.warning(f"[内存警告] 可用内存仅 {available_gb:.1f}GB，减少预加载月数")
            extra_months = max(0, extra_months - 1)
        if available_gb < 10:  # 如果可用内存小于10GB
            logger.warning(f"[内存警告] 可用内存仅 {available_gb:.1f}GB，只加载当前月")
            extra_months = 0

        # 构建要预加载的月份列表（当前月 + 前N个月）
        months_to_load = [
            (current_year, current_month),
        ]

        for i in range(1, extra_months + 1):
            prev_month = current_month - i
            prev_year = current_year
            if prev_month <= 0:
                prev_month += 12
                prev_year -= 1
            months_to_load.append((prev_year, prev_month))

        logger.debug(f"[预加载] 当前: {current_year}-{current_month:02d}, "
                    f"额外预加载前{extra_months}个月: {[(f'{y}-{m:02d}') for y, m in months_to_load]}")

        # 如果没有提供universe，尝试从context中获取
        if universe is None and self.context:
            universe = []
            context_universe = self.context.get('universe', None)
            if context_universe:
                if isinstance(context_universe, dict):
                    if market in context_universe:
                        universe = context_universe[market]
                else:
                    universe = context_universe

        # 【优化】按年份组织月份，避免重复加载同一Parquet
        from collections import defaultdict
        years_months = defaultdict(list)
        for year, month in months_to_load:
            month_key = f"{year}-{month:02d}"
            # 检查该月是否已加载
            with self.preload_lock:
                if market in self.preloaded_months and month_key in self.preloaded_months[market]:
                    logger.debug(f"{market} {month_key} 数据已预加载，跳过")
                    continue
            years_months[year].append((year, month))

        if not years_months:
            logger.debug("[预加载] 所有月份都已加载，跳过")
            return

        logger.debug(f"[预加载] 按年份组织: {dict(years_months)}")
        print(f"[预加载] 按年份批量加载（避免重复加载Parquet）...", flush=True)

        # 串行加载各年份（避免内存爆炸）
        for year in sorted(years_months.keys()):
            months_in_year = years_months[year]
            logger.debug(f"[预加载] 加载 {year} 年，包含 {len(months_in_year)} 个月份")
            try:
                self._load_year_months(market, year, months_in_year, universe, frequency)

                # 【新增】同时预加载日线数据，用于技术指标计算
                # 对于预加载的年份，都加载日线数据（确保有足够的历史数据）
                daily_preloaded_key = f"{market}_1d_{year}"
                with self.preload_lock:
                    if daily_preloaded_key not in self.preloaded_months.get(market, {}):
                        logger.debug(f"[预加载] 同时预加载 {year} 年的日线数据（用于技术指标）")
                        try:
                            self._load_year_daily_data(market, year, universe)
                            if market not in self.preloaded_months:
                                self.preloaded_months[market] = {}
                            self.preloaded_months[market][daily_preloaded_key] = datetime.now()
                        except Exception as e:
                            logger.warning(f"预加载 {year} 年日线数据失败: {e}")

            except Exception as e:
                logger.error(f"加载 {year} 年数据失败: {e}")
                import traceback
                traceback.print_exc()

        # 【内存优化】清理不再需要的旧月份数据
        self._cleanup_old_months(market, months_to_load, frequency)

        # 打印加载完成后的内存状态
        self._log_memory_status()

    def _cleanup_old_months(self, market: str, keep_months: list, frequency: str = '1m'):
        """清理不再需要的旧月份数据，释放内存

        注意：不清除preloaded_months记录，只清kline_cache，
        避免重复触发Parquet加载。
        """
        import gc

        # 构建需要保留的月份key集合
        keep_keys = set()
        for year, month in keep_months:
            month_key = f"{market}_{frequency}_{year}-{month:02d}"
            keep_keys.add(month_key)
            # 同时保留日线数据
            daily_key = f"{market}_1d_{year}-{month:02d}"
            keep_keys.add(daily_key)
            # 保留preloaded_months标记（防止重复加载）
            preloaded_1m_key = f"{year}-{month:02d}"
            preloaded_1d_key = f"{market}_1d_{year}"
            keep_keys.add(preloaded_1m_key)
            keep_keys.add(preloaded_1d_key)

        # 找出需要删除的缓存key
        keys_to_delete = []
        for cache_key in list(self.kline_cache.keys()):
            # 检查是否属于当前市场且不在保留列表中
            if cache_key.startswith(f"{market}_{frequency}_") or cache_key.startswith(f"{market}_1d_"):
                if cache_key not in keep_keys:
                    keys_to_delete.append(cache_key)

        # 删除旧数据
        freed_count = 0
        freed_memory_mb = 0
        for key in keys_to_delete:
            try:
                df = self.kline_cache.pop(key, None)
                if df is not None:
                    freed_count += 1
                    # 估算释放的内存 (MB)
                    freed_memory_mb += df.memory_usage(deep=True).sum() / (1024 * 1024)
                    del df
            except Exception as e:
                logger.debug(f"清理缓存 {key} 失败: {e}")

        # 强制垃圾回收
        if freed_count > 0:
            gc.collect()
            logger.debug(f"[内存清理] 释放 {freed_count} 个月份缓存，估算内存 {freed_memory_mb:.2f}MB")
            print(f"[内存清理] ✓ 释放 {freed_count} 个月份缓存，估算内存 {freed_memory_mb:.2f}MB", flush=True)

        # 【性能优化】清理过期的日级K线缓存（非当天的）
        with self._daily_cache_lock:
            if self._daily_cache_date:
                keep_date_str = str(self._daily_cache_date)
                expired_keys = [k for k in self._daily_klines_cache
                                if k[2] != keep_date_str]
                for k in expired_keys:
                    del self._daily_klines_cache[k]
                if expired_keys:
                    logger.debug(f"[内存清理] 释放 {len(expired_keys)} 个过期日级缓存条目")

        # 打印当前内存状态
        self._log_memory_status()

    def _log_memory_status(self):
        """打印当前内存使用状态"""
        try:
            import psutil
            mem = psutil.virtual_memory()
            # 计算缓存占用
            cache_mb = 0
            for key, df in self.kline_cache.items():
                cache_mb += df.memory_usage(deep=True).sum() / (1024 * 1024)
            logger.debug(f"[内存状态] 已用: {mem.used/1024**3:.1f}GB / {mem.total/1024**3:.0f}GB "
                       f"({mem.percent}%), 缓存: {cache_mb:.0f}MB, 可用: {mem.available/1024**3:.1f}GB")
        except Exception as e:
            logger.debug(f"获取内存状态失败: {e}")

    def _optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """优化DataFrame的数据类型以减少内存占用

        Args:
            df: 原始DataFrame

        Returns:
            优化后的DataFrame
        """
        try:
            original_memory = df.memory_usage(deep=True).sum()

            # 优化价格列 (float64 -> float32)
            price_columns = ['open', 'high', 'low', 'close']
            for col in price_columns:
                if col in df.columns:
                    df[col] = df[col].astype('float32')

            # 优化成交量 (int64 -> int32, 或使用uint32如果数据都是正数)
            if 'volume' in df.columns:
                df['volume'] = df['volume'].astype('int32')

            # 优化成交额 (float64 -> float32)
            if 'amount' in df.columns:
                df['amount'] = df['amount'].astype('float32')

            new_memory = df.memory_usage(deep=True).sum()
            saved_pct = (original_memory - new_memory) / original_memory * 100
            logger.debug(f"[内存优化] 数据类型优化节省 {saved_pct:.1f}% 内存 ({original_memory/1024**2:.1f}MB -> {new_memory/1024**2:.1f}MB)")

        except Exception as e:
            logger.warning(f"[内存优化] 数据类型优化失败: {e}")

        return df

    def _load_year_daily_data(self, market: str, year: int, universe: List[str]):
        """加载指定年份的日线数据（用于技术指标计算）

        Args:
            market: 市场名称
            year: 年份
            universe: 股票池
        """
        import time
        start_time = time.time()

        # 如果未提供universe，尝试获取全市场股票列表
        if not universe:
            logger.debug(f"[预加载] 未提供股票池，尝试加载 {market} 全市场日线数据")
            try:
                stock_list_df = self.data_interface.get_stock_list(market, use_cache=True)
                if stock_list_df is not None and not stock_list_df.empty:
                    if 'code' in stock_list_df.columns:
                        universe = stock_list_df['code'].tolist()
                    else:
                        universe = stock_list_df.index.tolist()
                    logger.debug(f"[预加载] 成功获取 {market} 全市场股票列表: {len(universe)} 只")
                else:
                    logger.warning(f"[预加载] 无法获取 {market} 股票列表，将加载所有可用数据")
                    universe = None
            except Exception as e:
                logger.error(f"[预加载] 获取 {market} 股票列表失败: {e}")
                universe = None

        logger.debug(f"[预加载] 开始加载 {year} 年日线数据，共{len(universe) if universe else '全市场'}只股票")
        print(f"[预加载] 加载 {year} 年日线数据（技术指标用）...", flush=True)

        # Parquet文件路径
        parquet_file = os.path.join(
            self.data_interface.market_data_dir, 'kline', 'codebased',
            market, '1d', f'{year}.parquet'
        )

        if not os.path.exists(parquet_file):
            logger.warning(f"[预加载] 日线Parquet不存在: {parquet_file}，跳过")
            return

        import pyarrow.parquet as pq

        try:
            # 读取Parquet文件（单线程）
            table = pq.read_table(
                parquet_file,
                columns=['time', 'code'] + ['open', 'high', 'low', 'close', 'volume', 'amount'],
                use_threads=False
            )
            df = table.to_pandas()

            logger.debug(f"[预加载] {year}年日线原始数据: {len(df):,}行")

            # 移除时区信息
            if hasattr(df['time'].dt, 'tz') and df['time'].dt.tz is not None:
                df['time'] = df['time'].dt.tz_localize(None)

            # 如果指定了universe，检查parquet包含的股票数量
            if universe:
                parquet_codes = set(df['code'].unique())

                if market == 'cn_future':
                    # 期货：按base code比较
                    base_universe = set(c.split('.')[0] if '.' in c else c for c in universe)
                    base_parquet = set(c.split('.')[0] if '.' in c else c for c in parquet_codes)
                    missing_ratio = len(base_universe - base_parquet) / max(len(base_universe), 1)

                    if missing_ratio > 0.5:
                        logger.warning(f"[预加载] {year}年日线Parquet数据不完整（{len(base_parquet & base_universe)}/{len(base_universe)}），回退到CSV加载")
                        self._load_year_daily_from_csv(market, year, universe, start_time)
                        return

                    # 按base code过滤并统一code格式
                    df['_base'] = df['code'].str.split('.').str[0]
                    df = df[df['_base'].isin(base_universe)]
                    code_map = {}
                    for c in universe:
                        base = c.split('.')[0] if '.' in c else c
                        code_map[base] = c
                    df['code'] = df['code'].apply(
                        lambda x: code_map.get(x.split('.')[0] if '.' in x else x, x)
                    )
                    df = df.drop(columns=['_base'])
                else:
                    universe_set = set(universe)
                    missing_codes = universe_set - parquet_codes
                    if len(missing_codes) > len(universe) * 0.5:
                        logger.warning(f"[预加载] {year}年日线Parquet数据不完整（{len(parquet_codes)}/{len(universe)}），回退到CSV加载")
                        self._load_year_daily_from_csv(market, year, universe, start_time)
                        return
                    df = df[df['code'].isin(universe)]
                logger.debug(f"[预加载] 日线代码过滤后: {len(df):,}行")
            else:
                logger.debug(f"[预加载] 未指定universe，加载全市场日线数据: {len(df):,}行")

            # 设置索引
            df = df.set_index(['time', 'code'])
            df = df.sort_index()

            # 按月分组缓存（全部缓存，不限制月份）
            df['month'] = df.index.get_level_values('time').strftime('%Y-%m')

            total_cached = 0
            for month_key, month_df in df.groupby('month'):
                month_df = month_df.drop(columns=['month'])
                cache_key = f"{market}_1d_{month_key}"
                # 【线程安全】使用锁保护缓存写入
                with self._kline_cache_lock:
                    self.kline_cache[cache_key] = month_df
                total_cached += len(month_df)

            # 主动释放内存
            del df, table
            import gc
            gc.collect()

            elapsed = time.time() - start_time
            logger.debug(f"[预加载] {year}年日线完成，{total_cached}条记录，耗时{elapsed:.2f}秒")
            print(f"[预加载] ✓ {year}年日线完成！{total_cached}条记录", flush=True)

        except Exception as e:
            logger.error(f"加载 {year} 年日线数据失败: {e}")
            import traceback
            traceback.print_exc()

    def _load_year_daily_from_csv(self, market: str, year: int, universe: List[str], start_time: float):
        """从CSV加载指定年份的日线数据（回退方式）

        Args:
            market: 市场名称
            year: 年份
            universe: 股票池
            start_time: 开始时间戳
        """
        import time

        # 如果未提供universe，尝试获取全市场股票列表
        if not universe:
            logger.debug(f"[预加载] CSV日线方式未提供股票池，尝试获取 {market} 全市场数据")
            try:
                stock_list_df = self.data_interface.get_stock_list(market, use_cache=True)
                if stock_list_df is not None and not stock_list_df.empty:
                    if 'code' in stock_list_df.columns:
                        universe = stock_list_df['code'].tolist()
                    else:
                        universe = stock_list_df.index.tolist()
                    logger.debug(f"[预加载] CSV日线方式成功获取 {market} 全市场股票列表: {len(universe)} 只")
                else:
                    logger.warning(f"[预加载] CSV日线方式无法获取 {market} 股票列表")
                    return
            except Exception as e:
                logger.error(f"[预加载] CSV日线方式获取 {market} 股票列表失败: {e}")
                return

        logger.debug(f"[预加载] 从CSV加载 {year} 年日线数据，共{len(universe)}只股票")

        # CSV目录路径（按年组织）
        year_dir = os.path.join(
            self.data_interface.market_data_dir, 'kline', 'codebased',
            market, '1d', f'{year}'
        )

        if not os.path.exists(year_dir):
            logger.warning(f"[预加载] 日线CSV目录不存在: {year_dir}")
            return

        # CSV列名（无表头文件）
        csv_columns = ['time', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount']

        all_data = []
        loaded_count = 0

        # 遍历股票的CSV文件
        for code in universe:
            csv_file = os.path.join(year_dir, f'{code}.csv')
            if not os.path.exists(csv_file):
                continue

            try:
                # 读取CSV（无表头）
                stock_df = pd.read_csv(csv_file, header=None, names=csv_columns)

                # 转换时间列
                stock_df['time'] = pd.to_datetime(stock_df['time'])
                # 移除时区
                if hasattr(stock_df['time'].dt, 'tz') and stock_df['time'].dt.tz is not None:
                    stock_df['time'] = stock_df['time'].dt.tz_localize(None)

                all_data.append(stock_df)
                loaded_count += 1

                # 每100只股票输出一次进度
                if loaded_count % 100 == 0:
                    logger.debug(f"[预加载] 已加载 {loaded_count}/{len(universe)} 只股票的日线数据")

            except Exception as e:
                logger.debug(f"读取 {csv_file} 失败: {e}")
                continue

        if not all_data:
            logger.warning(f"[预加载] {year}年日线CSV数据为空")
            return

        # 合并所有数据
        df = pd.concat(all_data, ignore_index=True)
        logger.debug(f"[预加载] {year}年日线CSV合并后: {len(df):,}行，来自{loaded_count}只股票")

        # 移除时区信息
        if hasattr(df['time'].dt, 'tz') and df['time'].dt.tz is not None:
            df['time'] = df['time'].dt.tz_localize(None)

        # 设置索引
        df = df.set_index(['time', 'code'])
        df = df.sort_index()

        # 按月分组缓存
        df['month'] = df.index.get_level_values('time').strftime('%Y-%m')

        total_cached = 0
        for month_key, month_df in df.groupby('month'):
            month_df = month_df.drop(columns=['month'])
            cache_key = f"{market}_1d_{month_key}"
            # 【线程安全】使用锁保护缓存写入
            with self._kline_cache_lock:
                self.kline_cache[cache_key] = month_df
            total_cached += len(month_df)

        # 释放内存
        del df, all_data
        import gc
        gc.collect()

        elapsed = time.time() - start_time
        logger.debug(f"[预加载] {year}年日线CSV完成，{total_cached}条记录，耗时{elapsed:.2f}秒")
        print(f"[预加载] ✓ {year}年日线CSV完成！{total_cached}条记录", flush=True)

    def _load_year_months(self, market: str, year: int, months_to_load: List[tuple],
                          universe: List[str], frequency: str):
        """加载指定年份的多个月份数据（只读取一次Parquet）- 优化版

        优化点：
        1. 启用多线程读取Parquet
        2. 使用PyArrow过滤条件下推，减少内存占用
        3. 延迟数据类型优化

        Args:
            market: 市场名称
            year: 年份
            months_to_load: 该年需要加载的月份列表 [(year, month), ...]
            universe: 股票池
            frequency: 数据频率
        """
        import time
        start_time = time.time()

        # 如果未提供universe，先从context获取策略universe，再回退到全市场
        if not universe:
            # 优先从context获取策略设置的universe（通常只有少量标的）
            if self.context:
                context_universe = self.context.get('universe', None)
                if context_universe:
                    if isinstance(context_universe, dict):
                        if market in context_universe:
                            universe = context_universe[market]
                    else:
                        universe = context_universe
                    if universe:
                        logger.debug(f"[预加载] 使用策略universe: {len(universe)} 只标的")

        if not universe:
            logger.debug(f"[预加载] 未提供股票池，尝试加载 {market} 全市场数据")
            try:
                stock_list_df = self.data_interface.get_stock_list(market, use_cache=True)
                if stock_list_df is not None and not stock_list_df.empty:
                    if 'code' in stock_list_df.columns:
                        universe = stock_list_df['code'].tolist()
                    else:
                        universe = stock_list_df.index.tolist()
                    logger.debug(f"[预加载] 成功获取 {market} 全市场股票列表: {len(universe)} 只")
                    # 对于1m数据，如果标的数量过多则跳过预加载
                    if frequency == '1m' and len(universe) > 100:
                        logger.debug(f"[预加载] {market} 1m数据标的数 {len(universe)} 超过100，"
                                   f"跳过预加载（改用按需加载）")
                        return
                else:
                    logger.warning(f"[预加载] 无法获取 {market} 股票列表，将加载所有可用数据")
                    universe = None
            except Exception as e:
                logger.error(f"[预加载] 获取 {market} 股票列表失败: {e}")
                universe = None

        month_strs = [f"{m:02d}" for _, m in months_to_load]
        logger.debug(f"[预加载] 开始加载 {year} 年份: {month_strs}，共{len(universe) if universe else '全市场'}只股票")
        print(f"[预加载] 开始加载 {year} ({','.join(month_strs)})，共{len(universe) if universe else '全市场'}只股票", flush=True)

        # Parquet文件路径
        parquet_file = os.path.join(
            self.data_interface.market_data_dir, 'kline', 'codebased',
            market, frequency, f'{year}.parquet'
        )

        if not os.path.exists(parquet_file):
            logger.warning(f"[预加载] Parquet文件不存在: {parquet_file}，使用CSV方式")
            for year, month in months_to_load:
                self._load_single_month_from_csv(market, year, month, universe, frequency)
            return

        # 【优化1】使用Parquet元数据缓存
        metadata = self._get_parquet_metadata(parquet_file)
        if metadata:
            logger.debug(f"[预加载] Parquet文件: {metadata.num_rows:,}行, {metadata.num_columns}列, "
                       f"{metadata.num_row_groups}个row groups")

        # 【优化2】使用多线程和过滤条件下推加载
        import pyarrow.parquet as pq
        import pyarrow as pa
        import pyarrow.compute as pc

        required_columns = ['time', 'code'] + ['open', 'high', 'low', 'close', 'volume', 'amount']

        # 计算时间范围过滤条件
        month_nums = [m for _, m in months_to_load]
        if month_nums:
            min_month = min(month_nums)
            max_month = max(month_nums)
            start_date = datetime(year, min_month, 1)
            if max_month == 12:
                end_date = datetime(year + 1, 1, 1)
            else:
                end_date = datetime(year, max_month + 1, 1)
        else:
            start_date = datetime(year, 1, 1)
            end_date = datetime(year + 1, 1, 1)

        try:
            # 【优化3】使用多线程读取（暂不使用过滤条件下推，因为时间类型匹配复杂）
            # 注意：PyArrow的filters对带时区的时间戳支持有限，暂时回退到加载后过滤
            logger.debug(f"[预加载] 使用多线程读取Parquet")
            table = pq.read_table(
                parquet_file,
                columns=required_columns,
                use_threads=True  # 【优化】启用多线程
            )
            df = table.to_pandas()
            logger.debug(f"[预加载] {year}年Parquet原始数据: {len(df):,}行")

            # 在DataFrame层面进行时间和代码过滤（更可靠）
            # 移除时区信息
            if hasattr(df['time'].dt, 'tz') and df['time'].dt.tz is not None:
                df['time'] = df['time'].dt.tz_localize(None)

            # 时间范围过滤
            before_filter = len(df)
            df = df[(df['time'] >= start_date) & (df['time'] < end_date)]
            if len(df) < before_filter:
                logger.debug(f"[预加载] 时间过滤: {before_filter:,} -> {len(df):,}行")

            # 代码过滤
            if universe:
                before_filter = len(df)
                if market == 'cn_future':
                    # 期货：按base code匹配
                    base_codes = set(c.split('.')[0] if '.' in c else c for c in universe)
                    df['_base'] = df['code'].str.split('.').str[0]
                    df = df[df['_base'].isin(base_codes)]
                    # 统一code格式
                    code_map = {}
                    for c in universe:
                        base = c.split('.')[0] if '.' in c else c
                        code_map[base] = c
                    df['code'] = df['code'].apply(
                        lambda x: code_map.get(x.split('.')[0] if '.' in x else x, x)
                    )
                    df = df.drop(columns=['_base'])
                else:
                    df = df[df['code'].isin(universe)]
                if len(df) < before_filter:
                    logger.debug(f"[预加载] 代码过滤: {before_filter:,} -> {len(df):,}行")

        except Exception as e:
            # 读取失败
            logger.error(f"[预加载] Parquet读取失败: {e}")
            raise

        # 【优化4】延迟数据类型优化 - 只在数据量大时优化
        # 数据量小于配置阈值时不优化，节省时间
        if len(df) > self._min_rows_for_dtype_opt:
            df = self._optimize_dtypes(df)
        else:
            logger.debug(f"[预加载] 数据量较小({len(df):,}行)，跳过类型优化")

        # 移除时区信息
        if hasattr(df['time'].dt, 'tz') and df['time'].dt.tz is not None:
            df['time'] = df['time'].dt.tz_localize(None)

        # 如果使用了过滤，这里可能不需要再次过滤
        if not universe and universe is not None:
            df = df[df['code'].isin(universe)]
            logger.debug(f"[预加载] 代码过滤后: {len(df):,}行")

        # 设置索引方便后续操作
        df = df.set_index(['time', 'code'])
        df = df.sort_index()

        # 提取需要的月份并分别缓存
        total_cached = 0
        for target_year, target_month in months_to_load:
            month_key = f"{target_year}-{target_month:02d}"

            # 计算该月的开始和结束时间
            if target_month == 12:
                month_end = datetime(target_year + 1, 1, 1) - timedelta(days=1)
                month_end = month_end.replace(hour=23, minute=59, second=59)
            else:
                month_end = datetime(target_year, target_month + 1, 1) - timedelta(days=1)
                month_end = month_end.replace(hour=23, minute=59, second=59)

            month_start = datetime(target_year, target_month, 1, 0, 0, 0)

            # 过滤该月的数据
            month_df = df.loc[month_start:month_end]

            if not month_df.empty:
                cache_key = f"{market}_{frequency}_{month_key}"
                # 【线程安全】使用锁保护缓存写入
                with self._kline_cache_lock:
                    self.kline_cache[cache_key] = month_df
                total_cached += len(month_df)

                # 标记已加载
                with self.preload_lock:
                    if market not in self.preloaded_months:
                        self.preloaded_months[market] = {}
                    self.preloaded_months[market][month_key] = datetime.now()

                logger.debug(f"[预加载] 缓存 {month_key}: {len(month_df):,}行")
            else:
                # 数据为空也要标记为已加载，避免反复扫描Parquet
                with self.preload_lock:
                    if market not in self.preloaded_months:
                        self.preloaded_months[market] = {}
                    self.preloaded_months[market][month_key] = datetime.now()
                logger.debug(f"[预加载] {month_key} 数据为空，已标记为已加载")

        # 主动释放内存
        del df, table
        import gc
        gc.collect()

        # 计算耗时
        elapsed = time.time() - start_time

        # 【优化】打印内存使用状态
        try:
            import psutil
            mem = psutil.virtual_memory()
            logger.debug(f"[预加载] {year}年完成，共{total_cached}条记录，"
                       f"耗时{elapsed:.2f}秒，内存使用: {mem.percent}%")
        except:
            logger.debug(f"[预加载] {year}年完成，共{total_cached}条记录，耗时{elapsed:.2f}秒")
        print(f"[预加载] ✓ {year}年完成！{total_cached}条记录，耗时{elapsed:.2f}秒", flush=True)

    def _load_single_month_from_csv(self, market: str, year: int, month: int,
                                   universe: List[str], frequency: str):
        """从CSV加载单个月份数据（回退方式）

        Args:
            market: 市场名称
            year: 年份
            month: 月份
            universe: 股票池
            frequency: 数据频率
        """
        month_key = f"{year}-{month:02d}"
        import time
        start_time = time.time()

        logger.debug(f"[预加载] CSV加载 {market} {month_key}")
        print(f"[预加载] CSV加载 {market} {month_key}...", flush=True)

        # 计算月份的开始和结束日期
        # 对于1m频率，start_date前移1天以捕获前一天文件中的早盘数据
        # （加密货币等24h市场：每日文件覆盖08:00~次日07:59，需要加载前一天文件获取00:00-07:59数据）
        if frequency == '1m':
            start_date = datetime(year, month, 1, 0, 0, 0) - timedelta(days=1)
        else:
            start_date = datetime(year, month, 1, 0, 0, 0)
        if month == 12:
            end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
            end_date = end_date.replace(hour=23, minute=59, second=59)
        else:
            end_date = datetime(year, month + 1, 1) - timedelta(days=1)
            end_date = end_date.replace(hour=23, minute=59, second=59)

        # 如果未提供universe，尝试获取全市场股票列表
        if not universe:
            logger.debug(f"[预加载] CSV方式未提供股票池，尝试获取 {market} 全市场数据")
            try:
                stock_list_df = self.data_interface.get_stock_list(market, use_cache=True)
                if stock_list_df is not None and not stock_list_df.empty:
                    if 'code' in stock_list_df.columns:
                        universe = stock_list_df['code'].tolist()
                    else:
                        universe = stock_list_df.index.tolist()
                    logger.debug(f"[预加载] CSV方式成功获取 {market} 全市场股票列表: {len(universe)} 只")
                else:
                    logger.warning(f"[预加载] CSV方式无法获取 {market} 股票列表")
                    return
            except Exception as e:
                logger.error(f"[预加载] CSV方式获取 {market} 股票列表失败: {e}")
                return

        # 使用分批加载CSV
        batch_size = min(500, len(universe)) if universe else 500
        batches = [universe[i:i + batch_size] for i in range(0, len(universe), batch_size)] if universe else []

        total_records = 0
        for idx, batch in enumerate(batches):
            result = self._preload_batch(market, batch, frequency, start_date, end_date)
            total_records += sum(result.values())

        # 标记已加载
        with self.preload_lock:
            if market not in self.preloaded_months:
                self.preloaded_months[market] = {}
            self.preloaded_months[market][month_key] = datetime.now()

        elapsed = time.time() - start_time
        logger.debug(f"[预加载] {month_key} CSV加载完成，{total_records}条记录，耗时{elapsed:.2f}秒")
        print(f"[预加载] ✓ {month_key} 完成！{total_records}条记录", flush=True)
    
    def preload_data(self, market: str, start_date: str, end_date: str, 
                    universe: List[str] = None, frequency: str = '1d'):
        """预加载数据（兼容旧接口，内部调用按月预加载）
        
        Args:
            market: 市场名称
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            universe: 股票池，如果为空则加载全部
            frequency: 数据频率
        """
        logger.debug(f"开始预加载数据: market={market}, freq={frequency}, "
                   f"date_range={start_date}~{end_date}, universe_size={len(universe) if universe else 'all'}")
        
        try:
            # 预加载交易日历（使用统一接口）
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
            trading_calendar = self.data_interface.get_trading_calendar(
                market, start_date_obj, end_date_obj, use_cache=True
            )
            logger.debug(f"加载交易日历: {market}, 交易日数量: {len(trading_calendar)}")
            
            # 预加载股票列表（使用统一接口）
            stock_list = self.data_interface.get_stock_list(market, use_cache=True)
            logger.debug(f"加载股票列表: {len(stock_list)} 只")
            
            # 预加载复权因子（使用统一接口）
            adj_factors = self.data_interface.get_adj_factors(
                market, codes=universe, start_date=start_date, end_date=end_date, use_cache=True
            )
            logger.debug(f"加载复权因子: {len(adj_factors)} 条记录")
            
            # 如果是1分钟频率，使用按月预加载策略
            if frequency == '1m':
                # 计算需要预加载的月份范围
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                
                current_year = start_dt.year
                current_month = start_dt.month
                
                while (current_year < end_dt.year) or (current_year == end_dt.year and current_month <= end_dt.month):
                    self.preload_monthly_data(market, current_year, current_month, universe, frequency)
                    
                    # 移动到下个月
                    if current_month == 12:
                        current_year += 1
                        current_month = 1
                    else:
                        current_month += 1
            else:
                # 其他频率，使用原有预加载逻辑
                if universe:
                    logger.debug(f"预加载universe中的K线数据: {len(universe)} 只股票")
                    # 预加载universe中股票的K线数据到缓存
                    self.data_interface.get_klines(
                        codes=universe,
                        market=market,
                        freq=frequency,
                        start_date=start_date,
                        end_date=end_date,
                        use_cache=True
                    )
                else:
                    logger.debug("智能预加载模式：universe为空，将按需加载数据")
            
            logger.debug("数据预加载完成")
                
        except Exception as e:
            logger.error(f"数据预加载失败: {e}")
            raise
    


    def get_quotes(self, codes: Union[str, List[str]], freq: str = '1d',
                  time: datetime = None, fields: List[str] = None,
                  adj_type: str = 'none') -> pd.DataFrame:
        """获取指定时间点的行情数据
        【性能诊断】包裹计时

        Args:
            codes: 股票代码或代码列表
            freq: 数据频率
            time: 查询时间点，如果为None则使用context.current_dt
            fields: 需要的字段列表
            adj_type: 复权类型 ('none': 不复权, 'front': 前复权, 'back': 后复权)

        Returns:
            pd.DataFrame: 行情数据，index为symbol，columns为fields

        Note:
            当指定时间点没有数据时，会自动使用最近的历史交易日数据
            会自动过滤超过回测当前时间的数据，防止未来函数
        """
        # 【性能诊断】计时
        import time as _time
        _t0 = _time.perf_counter()

        if time is None and self.context:
            time = self.context.get('current_dt')

        if time is None:
            raise ValueError("必须指定查询时间或设置context")

        if fields is None:
            fields = ['open', 'high', 'low', 'close', 'volume']

        market = self._get_market_from_context()

        # 限制查询时间不超过回测当前时间
        backtest_time = self._get_backtest_time()
        if backtest_time and time > backtest_time:
            logger.debug(f"[时间约束] 限制查询时间 {time} -> {backtest_time}")
            time = backtest_time

        # 【性能优化】对1m频率，优先使用O(1)价格字典查找
        if freq == '1m' and self._daily_price_dict:
            minute_key = time.strftime('%Y-%m-%d %H:%M')
            results = {}
            if isinstance(codes, str):
                codes_list = [codes]
            else:
                codes_list = codes
            for code in codes_list:
                price = self._daily_price_dict.get((code, minute_key))
                if price:
                    results[code] = {f: price[f] for f in fields if f in price}
                else:
                    # 精确分钟未命中，回退查找最近的历史分钟（向后10分钟）
                    found = False
                    for offset in range(1, 11):
                        prev_key = (time - pd.Timedelta(minutes=offset)).strftime('%Y-%m-%d %H:%M')
                        price = self._daily_price_dict.get((code, prev_key))
                        if price:
                            results[code] = {f: price[f] for f in fields if f in price}
                            found = True
                            break
                    # 向后搜索失败时（如00:00无前日数据），使用首根K线
                    # 适用于7x24市场（crypto UTC+8数据首根在08:01）
                    if not found:
                        first_min = self._daily_price_first_minute.get(code)
                        if first_min:
                            price = self._daily_price_dict.get((code, first_min))
                            if price:
                                results[code] = {f: price[f] for f in fields if f in price}
            if results:
                import pandas as _pd
                result_df = _pd.DataFrame.from_dict(results, orient='index')
                result_df.index.name = 'code'
                # 【性能诊断】
                _dt = _time.perf_counter() - _t0
                _key = 'get_quotes(price_dict)'
                if _key not in self._perf_api_timings:
                    self._perf_api_timings[_key] = [0.0, 0]
                self._perf_api_timings[_key][0] += _dt
                self._perf_api_timings[_key][1] += 1
                return result_df

        # 【性能优化】对1m频率，字典未命中时使用日级缓存（避免月度缓存未命中时的重复磁盘加载）
        if freq == '1m':
            daily_result = self._get_or_load_daily_klines(
                codes, market, freq, time, fields
            )
            if daily_result is not None and not daily_result.empty:
                # 从日级缓存数据中提取目标时间点的价格
                try:
                    time_level = daily_result.index.get_level_values('time')
                    before_or_at = daily_result[time_level <= time]
                    if not before_or_at.empty:
                        matching_data = before_or_at.groupby(level='code').tail(1)
                    else:
                        # 00:00等场景：请求时间早于首根K线（如crypto UTC+8首根在08:01）
                        # 使用每只代码的首根K线作为当前价格
                        matching_data = daily_result.groupby(level='code').head(1)
                    result = matching_data.reset_index(level='time', drop=True).reset_index()
                    if 'code' in result.columns:
                        result = result.set_index('code')
                    if fields:
                        available = [f for f in fields if f in result.columns]
                        result = result[available]
                        # 【性能诊断】记录耗时
                        _dt = _time.perf_counter() - _t0
                        _key = 'get_quotes(daily_cache)'
                        if _key not in self._perf_api_timings:
                            self._perf_api_timings[_key] = [0.0, 0]
                        self._perf_api_timings[_key][0] += _dt
                        self._perf_api_timings[_key][1] += 1
                        return result
                except Exception as e:
                    logger.debug(f"[get_quotes] 日级缓存提取失败，回退到原有逻辑: {e}")

        # 【性能诊断】记录fallback调用的freq和codes（仅前5次）
        _fallback_key = f'get_quotes(fallback,freq={freq})'

        # 【性能优化】优先从预加载缓存获取数据，避免绕过缓存直接调用DataInterface
        result = self._get_quotes_from_cache(codes, market, freq, time, fields, adj_type)

        if result is None:
            # 缓存未命中，使用DataInterface获取
            result = self.data_interface.get_quotes(
                codes=codes,
                market=market,
                freq=freq,
                time=time,
                fields=fields,
                adj_type=adj_type,
                use_cache=True
            )

            if result.empty:
                logger.debug(f"[get_quotes] 数据接口返回空数据: codes={codes}, market={market}, freq={freq}, time={time}")
            else:
                logger.debug(f"[get_quotes] 数据接口返回数据: shape={result.shape}")
        else:
            logger.debug(f"[get_quotes] 从缓存获取数据: shape={result.shape}")

        # 双重保险：过滤可能超过回测时间的数据
        if hasattr(result, 'index') and hasattr(result.index, 'names') and 'time' in getattr(result.index, 'names', []):
            filtered = self._filter_future_data(result, backtest_time)
            if filtered.empty and not result.empty:
                logger.warning(f"[get_quotes] 过滤后数据为空！原始shape={result.shape}, backtest_time={backtest_time}")
            # 【性能诊断】记录耗时
            _dt = _time.perf_counter() - _t0
            _key = f'get_quotes(fallback,freq={freq})'
            if _key not in self._perf_api_timings:
                self._perf_api_timings[_key] = [0.0, 0]
            self._perf_api_timings[_key][0] += _dt
            self._perf_api_timings[_key][1] += 1
            return filtered
        # 【性能诊断】记录耗时
        _dt = _time.perf_counter() - _t0
        _key = f'get_quotes(fallback,freq={freq})'
        if _key not in self._perf_api_timings:
            self._perf_api_timings[_key] = [0.0, 0]
        self._perf_api_timings[_key][0] += _dt
        self._perf_api_timings[_key][1] += 1
        return result

    def _get_or_load_daily_klines(self, codes, market, freq, time_point, fields):
        """日级别K线缓存 - 对同一代码同一天的1m查询复用已加载数据

        解决crypto等标的数>100时预加载被跳过、月度缓存为空、
        每次get_quotes都触发Timebased磁盘加载的性能问题。

        缓存粒度: (code, freq, date) — 同一代码同一天仅首次查询触发磁盘加载。

        Args:
            codes: 股票代码列表
            market: 市场名称
            freq: 数据频率
            time_point: 查询时间点
            fields: 需要的字段列表

        Returns:
            pd.DataFrame: 合并后的K线数据(MultiIndex(time,code))，失败返回None
        """
        if freq != '1m':
            return None

        current_date = time_point.date()
        day_start = time_point.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = time_point.replace(hour=23, minute=59, second=59, microsecond=0)

        if isinstance(codes, str):
            codes = [codes]

        all_data = []
        missing_codes = []

        with self._daily_cache_lock:
            # 日期切换时清空缓存
            if self._daily_cache_date != current_date:
                self._daily_klines_cache.clear()
                self._daily_cache_date = current_date
                self._daily_price_dict.clear()
                self._daily_price_dict_date = current_date
                self._daily_price_first_minute.clear()

            # 检查哪些代码已缓存
            for code in codes:
                cache_key = (code, freq, str(current_date))
                if cache_key in self._daily_klines_cache:
                    cached = self._daily_klines_cache[cache_key]
                    if not cached.empty:
                        all_data.append(cached)
                else:
                    missing_codes.append(code)

        # 加载缺失的代码（锁外执行，避免阻塞其他查询）
        if missing_codes:
            try:
                # 强制全字段加载：日级缓存和 _daily_price_dict(撮合的O(1)价格源)由此构建，
                # 若按调用方的窄 fields（如取价的 ['close']）加载，价格字典无 volume
                # → 撮合 quote.get('volume',0)=0 → "市场成交量为0"永不成交（cn_stock 1m 全灭的根因）
                loaded = self.data_interface.get_klines(
                    codes=missing_codes, market=market, freq=freq,
                    start_date=day_start.strftime('%Y-%m-%d %H:%M:%S'),
                    end_date=day_end.strftime('%Y-%m-%d %H:%M:%S'),
                    fields=['open', 'high', 'low', 'close', 'volume'], use_cache=True
                )
                if not loaded.empty:
                    with self._daily_cache_lock:
                        # 按代码拆分并缓存
                        if hasattr(loaded.index, 'get_level_values') and 'code' in loaded.index.names:
                            code_level = loaded.index.get_level_values('code')
                            for code in missing_codes:
                                code_data = loaded[code_level == code]
                                cache_key = (code, freq, str(current_date))
                                self._daily_klines_cache[cache_key] = code_data
                                if not code_data.empty:
                                    all_data.append(code_data)
                                    # 【性能优化】构建分钟级价格字典
                                    self._build_price_dict(code, code_data)
                        else:
                            # 索引结构异常，整体缓存
                            all_data.append(loaded)

                    # 【性能优化】回填月度kline_cache，使get_minute_prices后续可直接命中
                    # 解决：preload的月度缓存可能不包含策略所需的codes，
                    # 导致日切后daily_cache清空、monthly_cache未命中、get_minute_prices回退慢路径
                    try:
                        month_key = current_date.strftime('%Y-%m')
                        kc_key = f"{market}_{freq}_{month_key}"
                        if hasattr(loaded.index, 'get_level_values') and 'code' in loaded.index.names:
                            with self._kline_cache_lock:
                                if kc_key in self.kline_cache:
                                    existing = self.kline_cache[kc_key]
                                    loaded_codes = set(loaded.index.get_level_values('code'))
                                    existing_codes = set(existing.index.get_level_values('code'))
                                    new_codes = loaded_codes - existing_codes
                                    if new_codes:
                                        new_data = loaded[loaded.index.get_level_values('code').isin(new_codes)]
                                        combined = pd.concat([existing, new_data])
                                        combined = combined.sort_index()
                                        self.kline_cache[kc_key] = combined
                                else:
                                    self.kline_cache[kc_key] = loaded
                    except Exception as e:
                        logger.debug(f"[日级缓存] 回填kline_cache失败: {e}")
            except Exception as e:
                logger.warning(f"[日级缓存] 加载失败: {e}")

        if not all_data:
            return None

        try:
            return pd.concat(all_data)
        except Exception as e:
            logger.warning(f"[日级缓存] 合并数据失败: {e}")
            return None

    def _build_price_dict(self, code, code_data):
        """从单个代码的日K线DataFrame构建分钟级价格字典

        格式: {(code, '2023-06-01 10:30'): {'open': float, 'close': float, ...}}
        用于get_quotes的O(1)快速查找路径。
        """
        try:
            if code_data.empty:
                return
            cols = set(code_data.columns)
            needed = {'close'}
            if not needed.issubset(cols):
                return

            time_idx = code_data.index.get_level_values('time')
            has_open = 'open' in cols
            has_high = 'high' in cols
            has_low = 'low' in cols
            has_volume = 'volume' in cols

            first_minute = None
            for t, row in zip(time_idx, code_data.itertuples()):
                minute_key = pd.Timestamp(t).strftime('%Y-%m-%d %H:%M')
                entry = {'close': float(row.close) if hasattr(row, 'close') else 0.0}
                if has_open:
                    entry['open'] = float(row.open) if hasattr(row, 'open') else 0.0
                if has_high:
                    entry['high'] = float(row.high) if hasattr(row, 'high') else 0.0
                if has_low:
                    entry['low'] = float(row.low) if hasattr(row, 'low') else 0.0
                if has_volume:
                    entry['volume'] = float(row.volume) if hasattr(row, 'volume') else 0.0
                self._daily_price_dict[(code, minute_key)] = entry
                # 记录首根K线（time_idx已排序，第一个就是最早的）
                if first_minute is None:
                    first_minute = minute_key

            # 记录该code的首根分钟K线时间
            if first_minute:
                existing = self._daily_price_first_minute.get(code)
                if existing is None or first_minute < existing:
                    self._daily_price_first_minute[code] = first_minute
        except Exception as e:
            logger.debug(f"[价格字典] 构建{code}价格字典失败: {e}")

    def _get_quotes_from_cache(self, codes: Union[str, List[str]], market: str,
                                freq: str, time: datetime, fields: List[str],
                                adj_type: str) -> Optional[pd.DataFrame]:
        """从预加载缓存获取行情数据

        通过DataCenter.get_klines()利用预加载的月度缓存，
        避免每次调用都绕过缓存直接访问DataInterface。

        Returns:
            pd.DataFrame or None: 如果缓存命中返回行情数据(index=code)，否则返回None
        """
        if isinstance(codes, str):
            codes = [codes]

        try:
            # 根据频率确定查询时间范围
            if freq == '1d':
                start_time = (time - timedelta(days=30)).strftime('%Y-%m-%d 00:00:00')
                end_time = (time + timedelta(days=7)).strftime('%Y-%m-%d 23:59:59')
            else:
                start_time = time.strftime('%Y-%m-%d 00:00:00')
                end_time = time.strftime('%Y-%m-%d 23:59:59')

            # 通过get_klines获取数据（会使用预加载缓存）
            klines_df = self.get_klines(
                codes=codes, freq=freq,
                start_time=start_time, end_time=end_time,
                fields=fields, adj_type=adj_type
            )

            if klines_df.empty:
                return None

            # 提取目标时间点的数据
            time_index = klines_df.index.get_level_values('time')

            if freq == '1d':
                target_date = time.date()
                if hasattr(time_index, 'date'):
                    exact_match = klines_df[time_index.date == target_date]
                else:
                    exact_match = klines_df[time_index == pd.Timestamp(time)]

                if not exact_match.empty:
                    matching_data = exact_match
                    matched_codes = set(matching_data.index.get_level_values('code'))
                    missing_codes = [c for c in codes if c not in matched_codes]
                    if missing_codes and hasattr(time_index, 'date'):
                        before_target = klines_df[time_index.date < target_date]
                        for code in missing_codes:
                            code_before = before_target[before_target.index.get_level_values('code') == code]
                            if not code_before.empty:
                                latest = code_before.groupby(level='code').tail(1)
                                matching_data = pd.concat([matching_data, latest])
                else:
                    matching_data = klines_df.groupby(level='code').tail(1)
            else:
                before_or_at = klines_df[time_index <= time]
                if not before_or_at.empty:
                    matching_data = before_or_at.groupby(level='code').tail(1)
                else:
                    matching_data = pd.DataFrame()

            if matching_data.empty:
                return None

            # 转换为quotes格式 (index=code)
            quotes_df = matching_data.reset_index(level='time', drop=True).reset_index()
            if 'code' in quotes_df.columns:
                quotes_df = quotes_df.set_index('code')

            return quotes_df

        except Exception as e:
            logger.debug(f"[get_quotes] 缓存查询失败，将回退到DataInterface: {e}")
            return None

    def get_minute_prices(self, codes: List[str], market: str, freq: str,
                          current_time: datetime) -> Dict[str, Dict[str, float]]:
        """快速获取当前分钟的价格数据（用于撮合引擎）

        相比get_klines，避免了DataFrame copy + filter + time slice的开销。
        直接从预加载缓存中按索引查找，时间复杂度O(1)。

        Returns:
            Dict[symbol, {'close': float, 'volume': float}]
        """
        if freq != '1m':
            return {}

        result = {}
        minute_key = current_time.strftime('%Y-%m-%d %H:%M')
        month_key = current_time.strftime('%Y-%m')
        cache_key = f"{market}_{freq}_{month_key}"

        with self._kline_cache_lock:
            if cache_key not in self.kline_cache:
                return {}

            month_df = self.kline_cache[cache_key]
            if month_df.empty:
                return {}

            try:
                # 直接用loc精确查找当前分钟的数据，避免copy整个月份数据
                if hasattr(month_df.index, 'get_level_values') and 'time' in month_df.index.names:
                    time_level = month_df.index.get_level_values('time')
                    # 使用searchsorted快速定位时间范围
                    minute_start = pd.Timestamp(current_time.strftime('%Y-%m-%d %H:%M:00'))
                    minute_end = minute_start + pd.Timedelta(minutes=1) - pd.Timedelta(seconds=1)

                    left = time_level.searchsorted(minute_start, side='left')
                    right = time_level.searchsorted(minute_end, side='right')

                    if left < right and left < len(month_df):
                        minute_slice = month_df.iloc[left:right]
                        code_level = minute_slice.index.get_level_values('code')

                        for code in codes:
                            base_code = code.split('.')[0] if '.' in code else code
                            mask = code_level.str.split('.').str[0] == base_code
                            matched = minute_slice[mask]
                            if not matched.empty:
                                last_row = matched.iloc[-1]
                                result[code] = {
                                    'close': float(last_row.get('close', 0)),
                                    'volume': float(last_row.get('volume', 0)),
                                }
            except Exception:
                pass

        # 【性能优化】优先使用 price_dict O(1)查找，避免DataFrame操作
        if not result and self._daily_price_dict:
            minute_key_str = current_time.strftime('%Y-%m-%d %H:%M')
            for code in codes:
                price = self._daily_price_dict.get((code, minute_key_str))
                if price:
                    result[code] = {
                        'close': price.get('close', 0.0),
                        'volume': price.get('volume', 0.0),
                    }
                else:
                    # 向前回溯10分钟
                    found = False
                    for offset in range(1, 11):
                        prev_key = (current_time - pd.Timedelta(minutes=offset)).strftime('%Y-%m-%d %H:%M')
                        price = self._daily_price_dict.get((code, prev_key))
                        if price:
                            result[code] = {
                                'close': price.get('close', 0.0),
                                'volume': price.get('volume', 0.0),
                            }
                            found = True
                            break
                    # 仍未找到：使用该code的首根K线价格（解决crypto 00:00无数据问题）
                    if not found:
                        first_min = self._daily_price_first_minute.get(code)
                        if first_min:
                            price = self._daily_price_dict.get((code, first_min))
                            if price:
                                result[code] = {
                                    'close': price.get('close', 0.0),
                                    'volume': price.get('volume', 0.0),
                                }

        # price_dict也未命中（当天首次调用），触发日级缓存加载
        # 加载后会级联构建price_dict和回填kline_cache，后续调用直接走price_dict路径
        if not result:
            try:
                self._get_or_load_daily_klines(
                    codes, market, freq, current_time, ['close', 'volume']
                )
                # 加载后price_dict已构建，递归调用自身获取数据
                # 递归深度最多1层（price_dict已填充）
                return self.get_minute_prices(codes, market, freq, current_time)
            except Exception:
                pass

        return result

    def _get_from_cache(self, codes: Union[str, List[str]], market: str, freq: str,
                       start_time: Union[str, datetime], end_time: Union[str, datetime],
                       fields: List[str]) -> pd.DataFrame:
        """从预加载缓存中获取K线数据

        Args:
            codes: 股票代码或代码列表
            market: 市场名称
            freq: 数据频率
            start_time: 开始时间
            end_time: 结束时间
            fields: 需要的字段列表

        Returns:
            pd.DataFrame: 如果缓存命中返回数据，否则返回None
        """
        if not start_time or not end_time:
            return None

        # 标准化时间
        if isinstance(start_time, str):
            start_time = pd.to_datetime(start_time)
        if isinstance(end_time, str):
            end_time = pd.to_datetime(end_time)

        # 确保codes是列表
        if isinstance(codes, str):
            codes = [codes]

        # 计算需要查询的月份范围
        all_data = []
        current = start_time
        while current <= end_time:
            month_key = current.strftime('%Y-%m')
            cache_key = f"{market}_{freq}_{month_key}"

            # 【线程安全】使用锁保护缓存读取和检查
            with self._kline_cache_lock:
                if cache_key not in self.kline_cache:
                    logger.debug(f"[缓存] 缓存未命中: {cache_key}")
                    return None

                cached_df = self.kline_cache[cache_key]
                logger.debug(f"[缓存] 命中: {cache_key}, {len(cached_df)}行, codes查询: {codes[:3]}")

                cached_df = self.kline_cache[cache_key]

            # 检查索引结构是否正确
            if not hasattr(cached_df.index, 'names') or 'code' not in cached_df.index.names:
                logger.warning(f"[缓存] {cache_key} 索引结构不正确，尝试修复")
                with self._kline_cache_lock:
                    if 'code' in cached_df.columns and 'time' in cached_df.columns:
                        cached_df = cached_df.set_index(['time', 'code'])
                    elif 'code' in cached_df.columns:
                        cached_df = cached_df.set_index('code')
                    self.kline_cache[cache_key] = cached_df

            # 【性能优化】先用searchsorted定位时间范围，再slice，避免copy整个月份数据
            month_start_dt = current.replace(day=1, hour=0, minute=0, second=0)
            if current.month == 12:
                month_end_dt = current.replace(year=current.year + 1, month=1, day=1) - pd.Timedelta(seconds=1)
            else:
                month_end_dt = current.replace(month=current.month + 1, day=1) - pd.Timedelta(seconds=1)

            query_start = max(month_start_dt, start_time)
            query_end = min(month_end_dt, end_time)

            # 【可见性自愈】月度缓存可能定格在建立时刻的快照（首查回写只含当时
            # 可见的数据），若请求窗口尾部日期超出缓存快照的最后日期，说明该月
            # 后续数据从未补入——视为 stale 整体 miss，走 data_interface 重载并
            # 合并回写。按"日期粒度"比较（不含时刻），每天首个查询触发一次重载
            # （数据未变时仅性能损耗），换取数据可见性正确。
            # （曾致策略长期读到滞后数个交易日的数据 → 委托价基于陈旧参考价）
            try:
                _tmax = cached_df.index.get_level_values('time').max()
                if pd.Timestamp(query_end).date() > pd.Timestamp(_tmax).date():
                    logger.info(f"[缓存自愈] {cache_key} 快照滞后(tmax={_tmax} < "
                                f"query_end~{query_end})，触发重载合并")
                    return None
            except Exception:
                pass

            try:
                if hasattr(cached_df.index, 'get_level_values') and 'time' in cached_df.index.names:
                    time_level = cached_df.index.get_level_values('time')
                    left = time_level.searchsorted(pd.Timestamp(query_start), side='left')
                    right = time_level.searchsorted(pd.Timestamp(query_end), side='right')
                    month_df = cached_df.iloc[left:right].copy()
                else:
                    month_df = cached_df.loc[query_start:query_end].copy()
            except Exception:
                month_df = cached_df.copy()

            if month_df.empty:
                current = month_end_dt + timedelta(seconds=1)
                continue

            # 过滤股票代码
            if market == 'cn_future':
                base_codes = set(c.split('.')[0] if '.' in c else c for c in codes)
                cache_base = month_df.index.get_level_values('code').str.split('.').str[0]
                month_df = month_df[cache_base.isin(base_codes)]
                code_map = {}
                for c in codes:
                    base = c.split('.')[0] if '.' in c else c
                    code_map[base] = c
                month_df = month_df.reset_index()
                month_df['code'] = month_df['code'].apply(
                    lambda c: code_map.get(c.split('.')[0] if '.' in c else c, c)
                )
                month_df = month_df.set_index(['time', 'code'])
            else:
                month_df = month_df[month_df.index.get_level_values('code').isin(codes)]

            if not month_df.empty:
                all_data.append(month_df)

            # 移动到下个月
            current = month_end_dt + timedelta(seconds=1)

        if not all_data:
            return None

        # 合并所有月份数据
        result = pd.concat(all_data)

        # 选择需要的字段
        if fields:
            requested = [f for f in fields if f != 'time']
            available_fields = [f for f in requested if f in result.columns]
            # 缓存列不全（此前 fields=['close'] 之类窄查询回写建立的缓存）：
            # 直接返回会让调用方拿到缺 volume 的数据（撮合 volume=NaN 永不成交）。
            # 视为 miss，走 data_interface 全字段加载并回写加宽缓存。
            if len(available_fields) < len(requested):
                logger.debug(f"[缓存] 列不全({available_fields} < {requested})，回退 data_interface")
                return None
            result = result[available_fields]

        return result

    def _write_back_to_cache(self, data: pd.DataFrame, market: str, freq: str):
        """【O1优化】将按需加载的数据写回月度缓存，避免后续重复从磁盘加载

        场景：crypto 1m 预加载跳过（>100标的），导致每次 get_klines 缓存未命中，
        重复读磁盘。写回后，同一月份数据的后续查询直接命中缓存。

        Args:
            data: 从 data_interface 加载的完整数据（未过滤未来数据），MultiIndex(time, code)
            market: 市场名称
            freq: 数据频率
        """
        if data.empty:
            return
        if not hasattr(data.index, 'names') or 'time' not in data.index.names:
            return

        try:
            time_level = data.index.get_level_values('time')
            # 按月份分组
            periods = time_level.to_period('M')
            unique_months = periods.unique()

            for period in unique_months:
                month_key = f"{period.year}-{period.month:02d}"
                cache_key = f"{market}_{freq}_{month_key}"

                # 筛选该月数据
                month_mask = (periods == period)
                month_data = data[month_mask].copy()

                if month_data.empty:
                    continue

                with self._kline_cache_lock:
                    if cache_key in self.kline_cache:
                        # 窄字段保护（同下方新建分支）：fields=['close'] 之类窄查询的回写
                        # 只有部分列，concat 后新 (time,code) 行的 open/high/low/volume 全为
                        # NaN，combine_first 只能回填旧缓存已有的行、救不了新行 → volume=NaN
                        # 入缓存 → 撮合"数据异常"跳单。窄数据不参与合并（每次走 data_interface）。
                        base_cols = {'open', 'high', 'low', 'close', 'volume'}
                        if not base_cols.issubset(set(month_data.columns)):
                            logger.debug(f"[缓存回写] 跳过窄字段合并: {cache_key}, "
                                       f"cols={list(month_data.columns)}")
                            continue
                        # 合并到已有缓存（去重+排序，保持searchsorted正确性）
                        existing = self.kline_cache[cache_key]
                        combined = pd.concat([existing, month_data])
                        combined = combined[~combined.index.duplicated(keep='last')]
                        # 窄字段回写保护：fields=['close'] 的按需加载回写只有部分列，
                        # 去重 keep='last' 会用它覆盖全字段旧行 → volume 等整列 NaN
                        # → 撮合 market_volume=NaN 永不成交。用旧缓存回填新行的 NaN 列。
                        combined = combined.combine_first(existing)
                        combined = combined.sort_index()
                        self.kline_cache[cache_key] = combined
                        logger.debug(f"[缓存回写] 合并: {cache_key}, "
                                   f"新增{len(month_data)}行, 总计{len(combined)}行")
                    else:
                        # 新建分支的窄字段保护：fields=['close'] 之类窄查询的数据不建缓存——
                        # 否则该月后续宽查询命中"列名并集齐全但部分行缺列"的缓存
                        # （concat 宽月+窄月后 volume=NaN），撮合 volume=NaN 永不成交。
                        # 窄查询每次走 data_interface（其内部有缓存，代价可接受）。
                        base_cols = {'open', 'high', 'low', 'close', 'volume'}
                        if not base_cols.issubset(set(month_data.columns)):
                            logger.debug(f"[缓存回写] 跳过窄字段新建: {cache_key}, "
                                       f"cols={list(month_data.columns)}")
                            continue
                        self.kline_cache[cache_key] = month_data
                        logger.debug(f"[缓存回写] 新建: {cache_key}, {len(month_data)}行")
        except Exception as e:
            logger.debug(f"[缓存回写] 写回缓存失败（不影响正常流程）: {e}")

    def get_klines(self, codes: Union[str, List[str]], freq: str = '1d',
                  start_time: Union[str, datetime] = None, end_time: Union[str, datetime] = None,
                  fields: List[str] = None, adj_type: str = 'none') -> pd.DataFrame:
        """获取K线数据

        Args:
            codes: 股票代码或代码列表
            freq: 数据频率
            start_time: 开始时间
            end_time: 结束时间（会被限制为不超过回测当前时间）
            fields: 需要的字段列表
            adj_type: 复权类型 ('none': 不复权, 'front': 前复权, 'back': 后复权)

        Returns:
            pd.DataFrame: K线数据，MultiIndex(time, code)

        Note:
            会自动限制end_time不超过回测当前时间，防止未来函数
        """
        # 【性能诊断】计时
        import time as _time
        _t0 = _time.perf_counter()

        if fields is None:
            fields = ['open', 'high', 'low', 'close', 'volume']

        market = self._get_market_from_context()

        # 获取回测当前时间作为硬性上限
        backtest_time = self._get_backtest_time()

        # 【end语义修复】纯日期的 end_time（如 '2020-05-14'）解析为当日 00:00，
        # 早于日线 bar 时间戳（09:30/15:00），会把"当日"bar 永久排除——策略
        # 查"截至昨日"时实际拿到的是截至前日（T-1 信号变 T-2）。补到当日末尾，
        # 防未来由下方 backtest_time 截断保证。
        try:
            if end_time is not None:
                _end_pd = pd.Timestamp(end_time)
                if _end_pd == _end_pd.normalize():
                    end_time = _end_pd + pd.Timedelta(hours=23, minutes=59, seconds=59)
        except Exception:
            pass

        # 限制 end_time 不超过回测当前时间（加缓冲避免双重截断丢K线）
        if backtest_time and end_time:
            # 添加1个bar的缓冲，避免 end_time 截断 + _filter_future_data 双重过滤
            # 导致 backtest_time 处的K线因数据加载的exclusive边界而丢失
            if freq and freq.endswith('m'):
                buffer = pd.Timedelta(minutes=int(freq[:-1]))
            else:
                buffer = pd.Timedelta(hours=1)
            end_time = self._min_time(end_time, backtest_time + buffer)
            logger.debug(f"[时间约束] 限制end_time <= {backtest_time}")

        # 扩展 start_time 以提供盘前"最近一根完整K线"功能
        # 策略在盘前（如09:00）查询分钟线时，当天数据尚未生成（09:30开始）
        # 对于分钟线，回溯1天确保包含前一交易日的收盘K线
        # 对于日线，回溯2天确保包含前一交易日的K线
        if backtest_time and freq:
            if freq.endswith('m'):
                lookback_start = backtest_time - pd.Timedelta(days=1)
            elif freq.endswith('d'):
                lookback_start = backtest_time - pd.Timedelta(days=2)
            else:
                lookback_start = backtest_time - pd.Timedelta(days=1)
            # start_time为None或晚于lookback时，扩展到lookback_start
            if start_time is None or pd.to_datetime(start_time) > lookback_start:
                logger.debug(f"[时间约束] 扩展start_time至{lookback_start}以提供盘前数据")
                start_time = lookback_start

        logger.debug(f"[DataCenter] get_klines调用: market={market}, freq={freq}, "
                   f"codes_count={len(codes) if isinstance(codes, list) else 1}, "
                   f"start={start_time}, end={end_time}")

        # 检查频率是否支持
        if freq not in self.supported_frequencies:
            logger.warning(f"不支持的频率: {freq}, 将使用1m数据进行聚合")
            # 如果请求的频率不是1m，则从1m数据聚合
            if freq != '1m':
                return self._aggregate_klines(codes, '1m', freq, start_time, end_time, fields, adj_type)

        # 【新增】先检查预加载缓存
        cached_result = self._get_from_cache(codes, market, freq, start_time, end_time, fields)
        if cached_result is not None:
            # 【性能优化】改为debug级别，避免高频IO
            logger.debug(f"[DataCenter] 从缓存获取数据: shape={cached_result.shape}")
            # 仍然需要过滤未来数据
            filtered = self._filter_future_data(cached_result, backtest_time)
            logger.debug(f"[缓存] get_klines缓存命中: {len(filtered)}行")
            # 【性能诊断】
            _dt = _time.perf_counter() - _t0
            _key = 'get_klines(monthly_cache)'
            if _key not in self._perf_api_timings:
                self._perf_api_timings[_key] = [0.0, 0]
            self._perf_api_timings[_key][0] += _dt
            self._perf_api_timings[_key][1] += 1
            return filtered

        logger.debug(f"[缓存] get_klines缓存未命中: {market} {freq} codes={codes[:3] if isinstance(codes, list) else codes} start={start_time} end={end_time}")

        # 【性能优化】月度缓存miss时，对1m频率尝试日级缓存
        if freq == '1m':
            daily_result = self._get_or_load_daily_klines(
                codes, market, freq, backtest_time or pd.Timestamp.now(), fields
            )
            if daily_result is not None and not daily_result.empty:
                # 按请求的时间范围截取
                try:
                    if isinstance(start_time, str):
                        start_time_pd = pd.to_datetime(start_time)
                    else:
                        start_time_pd = pd.Timestamp(start_time) if start_time else None
                    if isinstance(end_time, str):
                        end_time_pd = pd.to_datetime(end_time)
                    else:
                        end_time_pd = pd.Timestamp(end_time) if end_time else None

                    time_level = daily_result.index.get_level_values('time')
                    if start_time_pd and end_time_pd:
                        mask = (time_level >= start_time_pd) & (time_level <= end_time_pd)
                        sliced = daily_result[mask]
                    elif end_time_pd:
                        sliced = daily_result[time_level <= end_time_pd]
                    else:
                        sliced = daily_result

                    if not sliced.empty:
                        filtered = self._filter_future_data(sliced, backtest_time)
                        # 【性能诊断】
                        _dt = _time.perf_counter() - _t0
                        _key = 'get_klines(daily_cache)'
                        if _key not in self._perf_api_timings:
                            self._perf_api_timings[_key] = [0.0, 0]
                        self._perf_api_timings[_key][0] += _dt
                        self._perf_api_timings[_key][1] += 1
                        return filtered
                except Exception as e:
                    logger.debug(f"[get_klines] 日级缓存截取失败，回退到data_interface: {e}")

        # 缓存未命中，使用统一数据接口获取K线数据
        # 【性能诊断】辅助函数：记录 data_interface 路径耗时
        def _record_klines_perf():
            _dt = _time.perf_counter() - _t0
            _key = 'get_klines(data_interface)'
            if _key not in self._perf_api_timings:
                self._perf_api_timings[_key] = [0.0, 0]
            self._perf_api_timings[_key][0] += _dt
            self._perf_api_timings[_key][1] += 1

        # 对期货代码进行标准化（添加交易所后缀），确保与数据文件名匹配
        query_codes = codes
        if market == 'cn_future':
            query_codes = [_normalize_future_code(code) if isinstance(code, str) else code for code in codes]
            logger.debug(f"[DataCenter] 期货代码标准化: {codes[:3]} -> {query_codes[:3]}")

        result = self.data_interface.get_klines(
            codes=query_codes,
            market=market,
            freq=freq,
            start_date=start_time,
            end_date=end_time,
            fields=fields,
            adj_type=adj_type,
            use_cache=True
        )

        # 【性能优化】改为debug级别
        logger.debug(f"[DataCenter] get_klines返回: shape={result.shape if not result.empty else 'empty'}, "
                   f"empty={result.empty}")

        if result.empty:
            display_codes = query_codes[:3] if isinstance(query_codes, list) else query_codes
            logger.debug(f"[DataCenter] ⚠️ 返回空DataFrame！参数: codes={display_codes}, "
                         f"start={start_time}, end={end_time}")

        # 【O1优化】将加载的数据写回月度缓存，避免后续重复从磁盘加载
        # 注意：使用未过滤的result，因为缓存应包含完整月份数据
        if not result.empty:
            self._write_back_to_cache(result, market, freq)

        # 双重保险：过滤可能超过回测时间的数据
        filtered = self._filter_future_data(result, backtest_time)
        if filtered.empty and not result.empty:
            # 盘前场景：所有加载数据都在backtest_time之后（如09:20查询，数据从09:30开始）
            # 尝试回溯加载前一个交易日的数据
            if backtest_time and freq and freq.endswith('m'):
                retry_start = (backtest_time - pd.Timedelta(days=5)).strftime('%Y-%m-%d 00:00:00')
                retry_end = backtest_time.strftime('%Y-%m-%d 23:59:59')
                try:
                    retry_result = self.data_interface.get_klines(
                        codes=query_codes, market=market, freq=freq,
                        start_date=retry_start, end_date=retry_end,
                        fields=fields, adj_type=adj_type, use_cache=True
                    )
                    # 【O1优化】盘前回退数据也写回缓存
                    if not retry_result.empty:
                        self._write_back_to_cache(retry_result, market, freq)
                    retry_filtered = self._filter_future_data(retry_result, backtest_time)
                    if not retry_filtered.empty:
                        logger.info(f"[get_klines] 盘前回退成功: 获取到{len(retry_filtered)}行历史K线")
                        _record_klines_perf()
                        return retry_filtered
                except Exception as e:
                    logger.debug(f"[get_klines] 盘前回退查询失败: {e}")

            # 最终回退：返回当天第一根K线作为参考（盘前场景下的近似方案）
            # 注：这包含约10-30分钟的前瞻，但比返回空数据导致策略异常更好
            if backtest_time and not result.empty:
                if isinstance(result.index, pd.MultiIndex):
                    first_bar = result.head(1)
                else:
                    first_bar = result.head(1)
                logger.info(f"[get_klines] 盘前参考: 返回当日首根K线作为参考 "
                           f"(backtest_time={backtest_time}, 首根时间={result.index[0] if len(result) > 0 else 'N/A'})")
                _record_klines_perf()
                return first_bar

            logger.warning(f"[get_klines] 过滤后数据为空！原始shape={result.shape}, backtest_time={backtest_time}")
        _record_klines_perf()
        return filtered
    
    def _aggregate_klines(self, codes: Union[str, List[str]], source_freq: str, target_freq: str,
                         start_time: Union[str, datetime] = None, end_time: Union[str, datetime] = None,
                         fields: List[str] = None, adj_type: str = 'none') -> pd.DataFrame:
        """从低频数据聚合到高频数据
        
        Args:
            codes: 股票代码或代码列表
            source_freq: 源频率
            target_freq: 目标频率
            start_time: 开始时间
            end_time: 结束时间
            fields: 需要的字段列表
            adj_type: 复权类型
            
        Returns:
            pd.DataFrame: 聚合后的K线数据
        """
        # 获取1m数据 - 通过get_klines利用预加载缓存
        market = self._get_market_from_context()

        df_1m = self.get_klines(
            codes=codes,
            freq=source_freq,
            start_time=start_time,
            end_time=end_time,
            fields=fields,
            adj_type=adj_type
        )
        
        if df_1m.empty:
            return df_1m
        
        # 计算聚合规则
        if target_freq == '30m':
            rule = '30T'
        elif target_freq == '120m':
            rule = '120T'
        elif target_freq == '1d':
            rule = '1D'
        else:
            logger.warning(f"不支持的目标频率: {target_freq}")
            return df_1m
        
        # 聚合数据
        agg_dict = {}
        for field in fields:
            if field in ['open']:
                agg_dict[field] = 'first'
            elif field in ['high']:
                agg_dict[field] = 'max'
            elif field in ['low']:
                agg_dict[field] = 'min'
            elif field in ['close']:
                agg_dict[field] = 'last'
            elif field in ['volume', 'amount']:
                agg_dict[field] = 'sum'
            else:
                agg_dict[field] = 'mean'
        
        # 按时间和代码分组聚合
        df_agg = df_1m.groupby([pd.Grouper(level=0, freq=rule), pd.Grouper(level=1)]).agg(agg_dict)
        
        return df_agg
    
    def get_factors(self, factor_names: Union[str, List[str]] = None, codes: Union[str, List[str]] = None,
                   freq: str = '1d', start_date: Union[str, datetime] = None,
                   end_date: Union[str, datetime] = None,
                   factor_list: Union[str, List[str]] = None,
                   **kwargs) -> pd.DataFrame:
        """获取因子数据 (matrix格式)

        Args:
            factor_names: 因子名称或名称列表
            codes: 股票代码列表，如果为None则获取所有
            freq: 数据频率
            start_date: 开始日期
            end_date: 结束日期
            factor_list: 因子名称或名称列表（兼容旧参数名，与factor_names等效）
        """
        # 兼容旧参数名 factor_list
        if factor_names is None and factor_list is not None:
            factor_names = factor_list
        if factor_names is None:
            return pd.DataFrame()

        market = self._get_market_from_context()

        result = self.data_interface.get_factors(
            factor_names=factor_names,
            codes=codes,
            market=market,
            freq=freq,
            start_date=start_date,
            end_date=end_date,
            factor_type='matrix',
            use_cache=True
        )

        # 防未来函数: 按 backtest_time 过滤(因子同样可能含当期/未来值, 如当日收盘才计算的因子)
        backtest_time = self._get_backtest_time()
        if backtest_time is not None and result is not None and not result.empty:
            result = self._filter_future_data(result, backtest_time)
        return result
    
    def get_trading_calendar(self, market: str = None, start_date=None, end_date=None, **kwargs) -> List[date]:
        """获取交易日历

        优先使用引擎已生成的完整日历（context['calendar']），
        避免策略API走DataInterface fallback导致结果不一致。
        当请求范围超出缓存日历时，自动补充DataInterface结果。

        Args:
            market: 市场名称（可选，默认使用当前回测市场）
            start_date: 开始日期（可选，不传则返回完整日历）
            end_date: 结束日期（可选，不传则返回完整日历）

        Returns:
            List[date]: 交易日期列表
        """
        if market is None:
            market = self._get_market_from_context()

        # 优先使用引擎已生成的完整日历
        cached_calendar = self.context.get('calendar') if self.context else None
        if cached_calendar:
            # engine calendar 是 datetime 列表，转为 date
            all_dates = [dt.date() if hasattr(dt, 'date') and callable(dt.date) else dt
                         for dt in cached_calendar]

            if not start_date and not end_date:
                # 不传日期，返回完整日历
                return list(all_dates)

            # 解析过滤边界
            sd = pd.to_datetime(start_date).date() if start_date else all_dates[0]
            ed = pd.to_datetime(end_date).date() if end_date else all_dates[-1]

            # 从缓存日历中过滤
            cached_result = [d for d in all_dates if sd <= d <= ed]

            # 检查请求范围是否超出缓存日历，需要补充
            cal_start = all_dates[0] if all_dates else None
            cal_end = all_dates[-1] if all_dates else None
            need_before = cal_start and sd < cal_start
            need_after = cal_end and ed > cal_end

            if not need_before and not need_after:
                # 请求范围完全在缓存内
                return cached_result

            # 部分超出，补充缺失段
            supplement = []
            try:
                supplement = self.data_interface.get_trading_calendar(
                    market=market, start_date=start_date, end_date=end_date,
                    use_cache=True
                )
            except Exception:
                pass

            if not supplement:
                return cached_result if cached_result else []

            # 合并去重
            merged = sorted(set(cached_result) | set(supplement))
            return merged

        # fallback: 无缓存日历，走DataInterface
        return self.data_interface.get_trading_calendar(
            market=market,
            start_date=start_date,
            end_date=end_date,
            use_cache=True
        )
    
    def get_corporate_actions(self, date, market):
        """获取指定日期的公司行为数据

        Args:
            date: 日期
            market: 市场名称

        Returns:
            List[Dict]: 公司行为事件列表
        """
        try:
            logger.info(f"[DataCenter] get_corporate_actions调用: date={date}, market={market}")
            # 使用统一数据接口获取公司行为数据
            corporate_actions = self.data_interface.get_corporate_actions(
                market=market,
                query_date=date,
                use_cache=True
            )

            logger.info(f"[DataCenter] get_corporate_actions返回: {len(corporate_actions)}条记录")
            return corporate_actions

        except Exception as e:
            logger.error(f"获取公司行为数据失败: {e}")
            return []
    
    def _get_backtest_time(self) -> Optional[datetime]:
        """获取回测当前时间

        Returns:
            回测当前时间，如果context未设置则返回None
        """
        if self.context:
            return self.context.get('current_dt')
        return None

    def _min_time(self, t1: Union[str, datetime], t2: datetime) -> datetime:
        """比较两个时间，返回较小的（较早的）

        Args:
            t1: 时间1（可能是str或datetime）
            t2: 时间2（datetime）

        Returns:
            较早的时间（datetime类型）
        """
        # 统一转换为datetime进行比较
        if isinstance(t1, str):
            # 尝试常见格式，避免警告
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                t1 = pd.to_datetime(t1, errors='coerce')
        if isinstance(t2, str):
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                t2 = pd.to_datetime(t2, errors='coerce')

        return min(t1, t2)

    def _filter_future_data(self, df: pd.DataFrame, backtest_time: Optional[datetime]) -> pd.DataFrame:
        """过滤超过回测时间的数据（防止未来函数）

        注意：
        - 对于 get_klines 返回的 MultiIndex(time, symbol) 数据，进行时间过滤
        - 对于 get_quotes 返回的 index=symbol 数据，跳过过滤（无时间维度）

        Args:
            df: 数据DataFrame
            backtest_time: 回测当前时间

        Returns:
            过滤后的DataFrame
        """
        if backtest_time is None or df.empty:
            return df

        # 处理MultiIndex (time, symbol) 的情况
        if isinstance(df.index, pd.MultiIndex):
            time_index = df.index.get_level_values(0)
        else:
            time_index = df.index

        # 检查是否为时间索引：如果不是 datetime 类型，尝试转换
        # 如果已经是 datetime 类型，直接使用
        if not isinstance(time_index, pd.DatetimeIndex):
            # 尝试转换为 datetime
            try:
                # 智能处理多种时间格式
                if time_index.dtype in ['int64', 'int32', 'int16', 'float64', 'float32']:
                    # 可能是Unix时间戳（秒或毫秒）或YYYYMMDD格式
                    # 尝试Unix时间戳（先尝试秒，再尝试毫秒）
                    import warnings
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")

                        # 首先尝试Unix时间戳（秒）
                        converted = pd.to_datetime(time_index, unit='s', errors='coerce')
                        # 检查转换是否成功（不是NaT）
                        if converted.isna().all():
                            # 如果秒级失败，尝试毫秒级
                            converted = pd.to_datetime(time_index, unit='ms', errors='coerce')
                        # 如果时间戳都失败，尝试YYYYMMDD格式
                        if converted.isna().all():
                            converted = pd.to_datetime(time_index.astype(str), format='%Y%m%d', errors='coerce')

                        # 如果全部成功，使用转换后的时间
                        if not converted.isna().all():
                            time_index = converted
                        else:
                            # 转换失败，可能是股票代码索引，直接返回
                            return df
                else:
                    # 字符串或其他格式，让pandas自动推断
                    import warnings
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        time_index = pd.to_datetime(time_index, errors='coerce', infer_datetime_format=True)
                    # 如果转换后全部是NaT，说明不是时间格式，直接返回
                    if time_index.isna().all():
                        return df
            except Exception:
                # 转换失败：可能是股票代码索引（get_quotes 的返回结果）
                # 这种情况不需要时间过滤，直接返回
                return df

        # 转换backtest_time以匹配index的时区（如果有）
        if hasattr(time_index, 'tz') and time_index.tz is not None:
            if backtest_time.tzinfo is None:
                backtest_time = backtest_time.tz_localize(time_index.tz)
            else:
                backtest_time = backtest_time.tz_convert(time_index.tz)

        # 过滤未来数据
        # 使用pd.to_datetime确保backtest_time也是datetime类型
        backtest_time_dt = pd.to_datetime(backtest_time)

        # 过滤时也要处理NaT（无效时间）
        valid_time_mask = time_index.notna()
        # 严格小于: 不返回 backtest_time 当根 bar(防未来函数)。
        # 1m: 策略在分钟M只能看到≤M-1的数据, 当根M的收盘价要到M+1才可见(配合撮合1分钟延迟)。
        # 注: 1d bar时间戳为当日00:00, 严格<对1d日内(如14:50)仍会含当日bar(00:00<14:50),
        #     1d未来函数需另行通过"日线bar按收盘时刻打时间戳"解决(设计变更, 暂未实施)。
        price_filter_mask = time_index < backtest_time_dt
        mask = valid_time_mask & price_filter_mask
        filtered = df[mask]

        # 记录被过滤的数据量
        filtered_count = len(df) - len(filtered)
        if filtered_count > 0:
            logger.debug(f"[时间约束] 过滤了 {filtered_count} 条未来数据 "
                        f"(backtest_time={backtest_time})")

        # 如果所有数据都被过滤，且原本有数据，记录警告
        if len(df) > 0 and len(filtered) == 0:
            # 输出调试信息
            if hasattr(time_index, 'min'):
                min_time = time_index.min()
                max_time = time_index.max()
                logger.warning(f"[时间约束] 所有数据被过滤！原始数据量: {len(df)}, "
                            f"时间范围: {min_time} - {max_time}, "
                            f"backtest_time={backtest_time_dt}, "
                            f"索引类型: {type(time_index)}, "
                            f"索引dtype: {time_index.dtype}")
                # 输出前几行数据用于调试
                logger.warning(f"[时间约束] DataFrame前几行:\n{df.head(3)}")

        return filtered

    def _get_market_from_context(self) -> str:
        """从context获取当前市场"""
        if self.context:
            return self.context.get('settings', {}).get('market', 'cn_stock')
        return self.market
    
    def stop(self):
        """停止数据中心"""
        # 清理数据接口缓存
        self.data_interface.clear_cache()

        logger.info("数据中心已停止")

    def calculate_indicator(
        self,
        func: Callable,
        codes: Union[str, List[str]],
        freq: str = '1d',
        start_time: Union[str, datetime] = None,
        end_time: Union[str, datetime] = None,
        price_field: str = 'close',
        **kwargs
    ) -> pd.DataFrame:
        """通用talib技术指标计算函数

        Args:
            func: talib函数，如 talib.MACD, talib.RSI
            codes: 股票代码或代码列表
            freq: 数据频率
            start_time: 开始时间
            end_time: 结束时间
            price_field: 价格字段，默认 'close'，某些指标需要 'high', 'low', 'open'
            **kwargs: 传递给talib函数的参数

        Returns:
            pd.DataFrame: 指标数据，MultiIndex(time, symbol)

        Examples:
            # 计算MACD
            df = data_center.calculate_indicator(talib.MACD, ['000001.SZ'],
                                                 '1d', '2024-01-01', '2024-12-31',
                                                 fastperiod=12, slowperiod=26, signalperiod=9)

            # 计算RSI
            df = data_center.calculate_indicator(talib.RSI, ['000001.SZ'],
                                                 '1d', '2024-01-01', '2024-12-31',
                                                 timeperiod=14)

            # 计算ATR（需要high/low/close）
            df = data_center.calculate_indicator(talib.ATR, ['000001.SZ'],
                                                 '1d', '2024-01-01', '2024-12-31',
                                                 price_field='ohlc', timeperiod=14)
        """
        func_name = getattr(func, '__name__', str(func))

        # 获取数据
        fields = ['open', 'high', 'low', 'close'] if price_field == 'ohlc' else ['close']
        data_df = self.get_klines(codes, freq, start_time, end_time, fields)

        if data_df.empty:
            return pd.DataFrame()

        # 计算指标
        results = {}
        for symbol in data_df.index.get_level_values(1).unique():
            symbol_data = data_df.xs(symbol, level=1)

            try:
                if price_field == 'ohlc':
                    result = func(
                        symbol_data['high'].values.astype('float64'),
                        symbol_data['low'].values.astype('float64'),
                        symbol_data['open'].values.astype('float64'),
                        symbol_data['close'].values.astype('float64'),
                        **kwargs
                    )
                else:
                    prices = symbol_data['close'].values.astype('float64')
                    result = func(prices, **kwargs)

                # 处理结果
                if isinstance(result, tuple):
                    output_dict = {f'output_{i}': r for i, r in enumerate(result)}
                else:
                    output_dict = {'value': result}

                results[symbol] = pd.DataFrame(output_dict, index=symbol_data.index)

            except Exception as e:
                logger.warning(f"计算{func_name}失败 {symbol}: {str(e)}")
                continue

        if results:
            return pd.concat(results, names=['code'])
        return pd.DataFrame()

    # 保留旧方法以兼容
    def calculate_macd(self, codes: Union[str, List[str]], freq: str = '1d',
                      start_time: Union[str, datetime] = None, end_time: Union[str, datetime] = None,
                      fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> pd.DataFrame:
        return self.calculate_indicator(talib.MACD, codes, freq, start_time, end_time,
                                       fastperiod=fast_period, slowperiod=slow_period, signalperiod=signal_period)

    def calculate_rsi(self, codes: Union[str, List[str]], freq: str = '1d',
                     start_time: Union[str, datetime] = None, end_time: Union[str, datetime] = None,
                     period: int = 14) -> pd.DataFrame:
        return self.calculate_indicator(talib.RSI, codes, freq, start_time, end_time, timeperiod=period)

    def calculate_bollinger_bands(self, codes: Union[str, List[str]], freq: str = '1d',
                                start_time: Union[str, datetime] = None, end_time: Union[str, datetime] = None,
                                period: int = 20, std_dev: float = 2.0) -> pd.DataFrame:
        return self.calculate_indicator(talib.BBANDS, codes, freq, start_time, end_time,
                                       timeperiod=period, nbdevup=std_dev, nbdevdn=std_dev)

    # ========================================================================
    # 资源清理方法
    # ========================================================================

    def close(self):
        """关闭数据中心，释放所有资源

        释放的资源包括：
        - 线程池
        - 缓存
        - 其他系统资源
        """
        try:
            # 关闭线程池
            if hasattr(self, 'preload_thread_pool') and self.preload_thread_pool is not None:
                self.preload_thread_pool.shutdown(wait=True)
                logger.info("数据预加载线程池已关闭")

            # 清空缓存
            if hasattr(self, 'kline_cache'):
                self.kline_cache.clear()
                logger.debug("K线缓存已清空")

            if hasattr(self, 'factor_cache'):
                self.factor_cache.clear()
                logger.debug("因子缓存已清空")

            if hasattr(self, '_parquet_meta_cache'):
                self._parquet_meta_cache.clear()
                logger.debug("Parquet元数据缓存已清空")

            if hasattr(self, '_trading_status'):
                self._trading_status.clear()
                logger.debug("交易状态已清空")

            if hasattr(self, 'preloaded_months'):
                self.preloaded_months.clear()
                logger.debug("预加载记录已清空")

            logger.info("数据中心已关闭，所有资源已释放")

        except Exception as e:
            logger.error(f"关闭数据中心时发生错误: {e}")

    def __del__(self):
        """析构函数，确保资源被释放"""
        try:
            self.close()
        except Exception:
            # 析构函数中忽略所有异常，避免Python警告
            pass

    def __enter__(self):
        """支持上下文管理器协议"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """支持上下文管理器协议"""
        self.close()
        return False