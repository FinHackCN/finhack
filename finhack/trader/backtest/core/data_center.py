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
        
        logger.info(f"数据中心初始化完成: {market} {freq} (使用统一数据接口，支持按月预加载)")
    
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
            logger.info(f"没有提供股票池，将加载 {market} 全市场数据进行预加载")
            try:
                stock_list_df = self.data_interface.get_stock_list(market, use_cache=True)
                if 'code' in stock_list_df.columns:
                    universe = stock_list_df['code'].tolist()
                else:
                    universe = stock_list_df.index.tolist()
                logger.info(f"成功获取 {market} 全市场股票列表，共 {len(universe)} 只")
            except Exception as e:
                logger.error(f"获取 {market} 股票列表失败: {e}")
                return

        import time
        start_time = time.time()
        logger.info(f"[预加载] 开始预加载 {market} {month_key} 的{frequency}数据，股票数量: {len(universe)}")
        print(f"[预加载] 开始加载 {market} {month_key}，共{len(universe)}只股票", flush=True)

        try:
            # 计算月份的开始和结束日期
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
                logger.info(f"[预加载] 使用Parquet批量加载: {parquet_file}")
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

                    logger.info(f"[预加载] Parquet文件大小: {len(df):,}行")

                    # 移除时区信息
                    if hasattr(df['time'].dt, 'tz') and df['time'].dt.tz is not None:
                        df['time'] = df['time'].dt.tz_localize(None)

                    # 先按code过滤（快速过滤）
                    df = df[df['code'].isin(universe)]
                    logger.info(f"[预加载] 代码过滤后: {len(df):,}行")

                    # 再按时间过滤
                    df = df[(df['time'] >= start_date) & (df['time'] <= end_date)]
                    logger.info(f"[预加载] 时间过滤后: {len(df):,}行")

                    if not df.empty:
                        # 设置MultiIndex
                        df = df.set_index(['time', 'code'])
                        df = df.sort_index()

                        # 按月份分组缓存
                        df['month'] = df.index.get_level_values('time').strftime('%Y-%m')
                        for month_group, month_df in df.groupby('month'):
                            cache_key = f"{market}_{frequency}_{month_group}"
                            month_df = month_df.drop(columns=['month'])
                            self.kline_cache[cache_key] = month_df
                            logger.info(f"[预加载] 缓存月份 {month_group}: {len(month_df):,}行")

                        total_records = len(df)

                except Exception as e:
                    logger.warning(f"[预加载] Parquet加载失败，回退到分批加载: {e}")
                    # 回退到原有的分批加载方式
                    total_records = self._fallback_batch_load(market, universe, frequency, start_date, end_date)

            else:
                # Parquet文件不存在，使用分批加载CSV的方式
                logger.info(f"[预加载] Parquet文件不存在: {parquet_file}")
                logger.info(f"[预加载] 使用CSV分批加载方式")
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
        logger.info(f"[预加载] CSV分批加载: {len(batches)}个批次，每批{batch_size}只")
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
                logger.info(f"[预加载] 批次 {completed}/{len(batches)} 完成")
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
            logger.info(f"[批次计算] 可用内存: {available_memory_gb:.2f}GB, "
                       f"CPU核心: {cpu_cores}, 当前CPU使用率: {current_cpu_usage:.1%}")
            logger.info(f"[批次计算] 内存建议: {candidates[0]}, CPU建议: {candidates[1]}, "
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
        """预加载一批股票的数据
        
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

        # 获取额外预加载月数的配置（默认3个月）
        extra_months = 3
        if self.context and hasattr(self.context, 'data_config'):
            extra_months = getattr(self.context.data_config, 'preload_extra_months', 3)

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

        logger.info(f"[预加载] 按年份组织: {dict(years_months)}")
        print(f"[预加载] 按年份批量加载（避免重复加载Parquet）...", flush=True)

        # 串行加载各年份（避免内存爆炸）
        for year in sorted(years_months.keys()):
            months_in_year = years_months[year]
            logger.info(f"[预加载] 加载 {year} 年，包含 {len(months_in_year)} 个月份")
            try:
                self._load_year_months(market, year, months_in_year, universe, frequency)

                # 【新增】同时预加载日线数据，用于技术指标计算
                # 对于预加载的年份，都加载日线数据（确保有足够的历史数据）
                daily_preloaded_key = f"{market}_1d_{year}"
                with self.preload_lock:
                    if daily_preloaded_key not in self.preloaded_months.get(market, {}):
                        logger.info(f"[预加载] 同时预加载 {year} 年的日线数据（用于技术指标）")
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

    def _load_year_daily_data(self, market: str, year: int, universe: List[str]):
        """加载指定年份的日线数据（用于技术指标计算）

        Args:
            market: 市场名称
            year: 年份
            universe: 股票池
        """
        import time
        start_time = time.time()

        logger.info(f"[预加载] 开始加载 {year} 年日线数据，共{len(universe)}只股票")
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

            logger.info(f"[预加载] {year}年日线原始数据: {len(df):,}行")

            # 移除时区信息
            if hasattr(df['time'].dt, 'tz') and df['time'].dt.tz is not None:
                df['time'] = df['time'].dt.tz_localize(None)

            # 检查parquet包含的股票数量
            parquet_codes = set(df['code'].unique())
            universe_set = set(universe)
            missing_codes = universe_set - parquet_codes

            # 如果parquet缺少超过50%的股票，回退到CSV加载
            if len(missing_codes) > len(universe) * 0.5:
                logger.warning(f"[预加载] {year}年日线Parquet数据不完整（{len(parquet_codes)}/{len(universe)}只股票），回退到CSV加载")
                print(f"[预加载] Parquet数据不完整，使用CSV加载...", flush=True)
                self._load_year_daily_from_csv(market, year, universe, start_time)
                return

            # 先按代码过滤
            df = df[df['code'].isin(universe)]
            logger.info(f"[预加载] 日线代码过滤后: {len(df):,}行")

            # 设置索引
            df = df.set_index(['time', 'code'])
            df = df.sort_index()

            # 按月分组缓存（全部缓存，不限制月份）
            df['month'] = df.index.get_level_values('time').strftime('%Y-%m')

            total_cached = 0
            for month_key, month_df in df.groupby('month'):
                month_df = month_df.drop(columns=['month'])
                cache_key = f"{market}_1d_{month_key}"
                self.kline_cache[cache_key] = month_df
                total_cached += len(month_df)

            # 主动释放内存
            del df, table
            import gc
            gc.collect()

            elapsed = time.time() - start_time
            logger.info(f"[预加载] {year}年日线完成，{total_cached}条记录，耗时{elapsed:.2f}秒")
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
        logger.info(f"[预加载] 从CSV加载 {year} 年日线数据，共{len(universe)}只股票")

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
        logger.info(f"[预加载] {year}年日线CSV合并后: {len(df):,}行，来自{loaded_count}只股票")

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
            self.kline_cache[cache_key] = month_df
            total_cached += len(month_df)

        # 释放内存
        del df, all_data
        import gc
        gc.collect()

        elapsed = time.time() - start_time
        logger.info(f"[预加载] {year}年日线CSV完成，{total_cached}条记录，耗时{elapsed:.2f}秒")
        print(f"[预加载] ✓ {year}年日线CSV完成！{total_cached}条记录", flush=True)

    def _load_year_months(self, market: str, year: int, months_to_load: List[tuple],
                          universe: List[str], frequency: str):
        """加载指定年份的多个月份数据（只读取一次Parquet）

        Args:
            market: 市场名称
            year: 年份
            months_to_load: 该年需要加载的月份列表 [(year, month), ...]
            universe: 股票池
            frequency: 数据频率
        """
        import time
        start_time = time.time()

        month_strs = [f"{m:02d}" for _, m in months_to_load]
        logger.info(f"[预加载] 开始加载 {year} 年份: {month_strs}，共{len(universe)}只股票")
        print(f"[预加载] 开始加载 {year} ({','.join(month_str)})，共{len(universe)}只股票", flush=True)

        # Parquet文件路径
        parquet_file = os.path.join(
            self.data_interface.market_data_dir, 'kline', 'codebased',
            market, frequency, f'{year}.parquet'
        )

        if not os.path.exists(parquet_file):
            logger.warning(f"[预加载] Parquet文件不存在: {parquet_file}，使用CSV方式")
            # 回退到逐月加载CSV
            for year, month in months_to_load:
                self._load_single_month_from_csv(market, year, month, universe, frequency)
            return

        # 使用Parquet批量加载
        import pyarrow.parquet as pq

        required_columns = ['time', 'code'] + ['open', 'high', 'low', 'close', 'volume', 'amount']

        # 【关键】单线程加载，避免内存爆炸
        table = pq.read_table(
            parquet_file,
            columns=required_columns,
            use_threads=False
        )
        df = table.to_pandas()

        logger.info(f"[预加载] {year}年Parquet原始数据: {len(df):,}行")

        # 移除时区信息
        if hasattr(df['time'].dt, 'tz') and df['time'].dt.tz is not None:
            df['time'] = df['time'].dt.tz_localize(None)

        # 先按代码过滤
        df = df[df['code'].isin(universe)]
        logger.info(f"[预加载] 代码过滤后: {len(df):,}行")

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
                self.kline_cache[cache_key] = month_df
                total_cached += len(month_df)

                # 标记已加载
                with self.preload_lock:
                    if market not in self.preloaded_months:
                        self.preloaded_months[market] = {}
                    self.preloaded_months[market][month_key] = datetime.now()

                logger.info(f"[预加载] 缓存 {month_key}: {len(month_df):,}行")

        # 主动释放内存
        del df, table
        import gc
        gc.collect()

        elapsed = time.time() - start_time
        logger.info(f"[预加载] {year}年完成，共{total_cached}条记录，耗时{elapsed:.2f}秒")
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

        logger.info(f"[预加载] CSV加载 {market} {month_key}")
        print(f"[预加载] CSV加载 {market} {month_key}...", flush=True)

        # 计算月份的开始和结束日期
        start_date = datetime(year, month, 1, 0, 0, 0)
        if month == 12:
            end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
            end_date = end_date.replace(hour=23, minute=59, second=59)
        else:
            end_date = datetime(year, month + 1, 1) - timedelta(days=1)
            end_date = end_date.replace(hour=23, minute=59, second=59)

        # 使用分批加载CSV
        batch_size = min(500, len(universe))
        batches = [universe[i:i + batch_size] for i in range(0, len(universe), batch_size)]

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
        logger.info(f"[预加载] {month_key} CSV加载完成，{total_records}条记录，耗时{elapsed:.2f}秒")
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
        logger.info(f"开始预加载数据: market={market}, freq={frequency}, "
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
                    logger.info(f"预加载universe中的K线数据: {len(universe)} 只股票")
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
                    logger.info("智能预加载模式：universe为空，将按需加载数据")
            
            logger.info("数据预加载完成")
                
        except Exception as e:
            logger.error(f"数据预加载失败: {e}")
            raise
    


    def get_quotes(self, codes: Union[str, List[str]], freq: str = '1d',
                  time: datetime = None, fields: List[str] = None,
                  adj_type: str = 'none') -> pd.DataFrame:
        """获取指定时间点的行情数据

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

        # 使用统一数据接口获取行情数据
        result = self.data_interface.get_quotes(
            codes=codes,
            market=market,
            freq=freq,
            time=time,
            fields=fields,
            adj_type=adj_type,
            use_cache=True
        )

        # 调试：记录原始返回数据
        if result.empty:
            logger.debug(f"[get_quotes] 数据接口返回空数据: codes={codes}, market={market}, freq={freq}, time={time}")
        else:
            logger.debug(f"[get_quotes] 数据接口返回数据: shape={result.shape}, index类型={type(result.index)}, "
                        f"index前3个={result.index[:3].tolist() if len(result.index) >= 3 else result.index.tolist()}")

        # 双重保险：过滤可能超过回测时间的数据
        filtered = self._filter_future_data(result, backtest_time)
        if filtered.empty and not result.empty:
            logger.warning(f"[get_quotes] 过滤后数据为空！原始shape={result.shape}, backtest_time={backtest_time}")
        return filtered

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

        # 【调试】输出缓存查询信息
        logger.info(f"[缓存查询] market={market}, freq={freq}, start={start_time}, end={end_time}")
        logger.info(f"[缓存查询] 当前缓存中的keys: {list(self.kline_cache.keys())[:10]}...")  # 只显示前10个

        # 计算需要查询的月份范围
        all_data = []
        current = start_time
        while current <= end_time:
            month_key = current.strftime('%Y-%m')
            cache_key = f"{market}_{freq}_{month_key}"

            logger.info(f"[缓存查询] 查找key: {cache_key}, 找到: {cache_key in self.kline_cache}")

            if cache_key not in self.kline_cache:
                logger.debug(f"[缓存] 缓存未命中: {cache_key}")
                return None  # 缓存不完整，回退到磁盘加载

            month_df = self.kline_cache[cache_key]

            # 过滤股票代码和时间范围
            month_df = month_df[month_df.index.get_level_values('code').isin(codes)]

            # 获取该月的起始和结束时间
            month_start = current.replace(day=1, hour=0, minute=0, second=0)
            if current.month == 12:
                month_end = current.replace(year=current.year + 1, month=1, day=1) - pd.Timedelta(seconds=1)
            else:
                month_end = current.replace(month=current.month + 1, day=1) - pd.Timedelta(seconds=1)

            # 限制在查询时间范围内
            month_start = max(month_start, start_time)
            month_end = min(month_end, end_time)

            # 从缓存中提取时间范围数据
            month_df = month_df.loc[month_start:month_end]

            if not month_df.empty:
                all_data.append(month_df)

            # 移动到下个月
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1, day=1)
            else:
                current = current.replace(month=current.month + 1, day=1)

        if not all_data:
            return None

        # 合并所有月份数据
        result = pd.concat(all_data)

        # 选择需要的字段
        if fields:
            available_fields = [f for f in fields if f in result.columns]
            result = result[available_fields]

        return result

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
            pd.DataFrame: K线数据，MultiIndex(time, symbol)

        Note:
            会自动限制end_time不超过回测当前时间，防止未来函数
        """
        if fields is None:
            fields = ['open', 'high', 'low', 'close', 'volume']

        market = self._get_market_from_context()

        # 获取回测当前时间作为硬性上限
        backtest_time = self._get_backtest_time()

        # 限制 end_time 不超过回测当前时间
        if backtest_time and end_time:
            end_time = self._min_time(end_time, backtest_time)
            if end_time == backtest_time:
                logger.debug(f"[时间约束] 限制end_time <= {backtest_time}")

        logger.info(f"[DataCenter] get_klines调用: market={market}, freq={freq}, "
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
            logger.info(f"[DataCenter] 从缓存获取数据: shape={cached_result.shape}")
            # 仍然需要过滤未来数据
            filtered = self._filter_future_data(cached_result, backtest_time)
            return filtered

        # 缓存未命中，使用统一数据接口获取K线数据
        result = self.data_interface.get_klines(
            codes=codes,
            market=market,
            freq=freq,
            start_date=start_time,
            end_date=end_time,
            fields=fields,
            adj_type=adj_type,
            use_cache=True
        )

        logger.info(f"[DataCenter] get_klines返回: shape={result.shape if not result.empty else 'empty'}, "
                   f"empty={result.empty}, "
                   f"index_names={result.index.names if not result.empty else 'N/A'}")

        if result.empty:
            logger.warning(f"[DataCenter] ⚠️ 返回空DataFrame！参数: codes={codes[:3] if isinstance(codes, list) else codes}, "
                         f"start={start_time}, end={end_time}")

        # 双重保险：过滤可能超过回测时间的数据
        filtered = self._filter_future_data(result, backtest_time)
        if filtered.empty and not result.empty:
            logger.warning(f"[get_klines] 过滤后数据为空！原始shape={result.shape}, backtest_time={backtest_time}")
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
        # 获取1m数据
        df_1m = self.data_interface.get_klines(
            codes=codes,
            market=self._get_market_from_context(),
            freq=source_freq,
            start_date=start_time,
            end_date=end_time,
            fields=fields,
            adj_type=adj_type,
            use_cache=True
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
    
    def get_factors(self, factor_names: Union[str, List[str]], codes: Union[str, List[str]] = None,
                   freq: str = '1d', start_date: Union[str, datetime] = None, 
                   end_date: Union[str, datetime] = None) -> pd.DataFrame:
        """获取因子数据 (matrix格式)
        
        Args:
            factor_names: 因子名称或名称列表
            codes: 股票代码列表，如果为None则获取所有
            freq: 数据频率
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            pd.DataFrame: 因子数据，MultiIndex(date, symbol)
        """
        market = self._get_market_from_context()
        
        # 使用统一数据接口获取因子数据
        return self.data_interface.get_factors(
            factor_names=factor_names,
            codes=codes,
            market=market,
            freq=freq,
            start_date=start_date,
            end_date=end_date,
            factor_type='matrix',
            use_cache=True
        )
    
    def get_trading_calendar(self, market: str, start_date: date, end_date: date) -> List[date]:
        """获取交易日历
        
        Args:
            market: 市场名称
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            List[date]: 交易日期列表
        """
        # 使用统一数据接口获取交易日历
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
        price_filter_mask = time_index <= backtest_time_dt
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
                        symbol_data['high'].values,
                        symbol_data['low'].values,
                        symbol_data['open'].values,
                        symbol_data['close'].values,
                        **kwargs
                    )
                else:
                    prices = symbol_data['close'].values
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
            return pd.concat(results, names=['symbol'])
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