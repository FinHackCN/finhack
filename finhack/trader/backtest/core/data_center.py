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
        """预加载指定月份的1分钟数据 - 优化版
        
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
            # 从context中获取当前策略需要的股票（context是字典）
            context_universe = self.context.get('universe', None)
            if context_universe:
                if isinstance(context_universe, dict):
                    # 多市场情况
                    if market in context_universe:
                        universe = context_universe[market]
                else:
                    # 单市场情况
                    universe = context_universe
        
        # 如果仍然没有universe，则不进行预加载，避免加载全市场数据
        if not universe:
            logger.warning(f"没有提供股票池，跳过 {market} {month_key} 的预加载")
            return
        
        import time
        start_time = time.time()
        logger.info(f"[预加载] 开始预加载 {market} {month_key} 的1分钟数据，股票数量: {len(universe)}")
        print(f"[预加载] 开始加载 {market} {month_key}，共{len(universe)}只股票", flush=True)
        
        try:
            # 计算月份的开始和结束日期
            start_date = datetime(year, month, 1)
            if month == 12:
                end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = datetime(year, month + 1, 1) - timedelta(days=1)
            
            # 动态计算最优批次大小，基于系统资源自适应调整
            batch_size = self._calculate_adaptive_batch_size(len(universe), frequency)
            batches = [universe[i:i + batch_size] for i in range(0, len(universe), batch_size)]
            logger.info(f"[预加载] 分为 {len(batches)} 个批次，每批 {batch_size} 只股票")
            print(f"[预加载] 分为{len(batches)}个批次进行并行加载", flush=True)
            
            # 使用线程池并行加载
            futures = {}
            for idx, batch in enumerate(batches):
                future = self.preload_thread_pool.submit(
                    self._preload_batch, market, batch, frequency, start_date, end_date
                )
                futures[future] = (idx, batch)
            
            # 等待所有批次加载完成，并显示进度
            total_records = 0
            completed = 0
            for future in as_completed(futures):
                try:
                    batch_idx, batch = futures[future]
                    result = future.result(timeout=60)  # 60秒超时
                    total_records += sum(result.values())
                    completed += 1
                    logger.info(f"[预加载] 批次 {completed}/{len(batches)} 完成: {batch}")
                    print(f"[预加载] 进度: {completed}/{len(batches)} ({completed*100//len(batches)}%)", flush=True)
                except Exception as e:
                    logger.error(f"[预加载] 批次预加载失败: {e}")
                    import traceback
                    traceback.print_exc()
            
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
        max_batch_size = 50      # 最大批次大小

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
            
            # 使用数据接口批量获取K线数据
            klines_df = self.data_interface.get_klines(
                codes=batch,
                market=market,
                freq=frequency,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
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
        """确保当前月份和上个月的数据已预加载 - 优化版
        
        Args:
            market: 市场名称
            current_date: 当前日期
            universe: 股票池
            frequency: 数据频率
        """
        # 当前月份
        current_year = current_date.year
        current_month = current_date.month
        
        # 上个月
        if current_month == 1:
            prev_year = current_year - 1
            prev_month = 12
        else:
            prev_year = current_year
            prev_month = current_month - 1
        
        # 检查并预加载当前月份和上个月的数据
        months_to_load = [
            (current_year, current_month),
            (prev_year, prev_month)
        ]
        
        # 如果没有提供universe，尝试从context中获取
        if universe is None and self.context:
            universe = []
            # 从context中获取当前策略需要的股票（context是字典）
            context_universe = self.context.get('universe', None)
            if context_universe:
                if isinstance(context_universe, dict):
                    # 多市场情况
                    if market in context_universe:
                        universe = context_universe[market]
                else:
                    # 单市场情况
                    universe = context_universe
        
        # 并行预加载多个月份的数据
        futures = []
        for year, month in months_to_load:
            month_key = f"{year}-{month:02d}"
            
            # 检查是否已经预加载
            with self.preload_lock:
                if market in self.preloaded_months and month_key in self.preloaded_months[market]:
                    logger.debug(f"{market} {month_key} 数据已预加载，跳过")
                    continue
            
            # 提交预加载任务
            future = self.preload_thread_pool.submit(
                self.preload_monthly_data, market, year, month, universe, frequency
            )
            futures.append(future)
        
        # 等待所有预加载任务完成
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"预加载任务失败: {e}")
    
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
        """
        if time is None and self.context:
            time = self.context.get('current_dt')
        
        if time is None:
            raise ValueError("必须指定查询时间或设置context")
        
        if fields is None:
            fields = ['open', 'high', 'low', 'close', 'volume']
        
        market = self._get_market_from_context()
        
        # 使用统一数据接口获取行情数据
        return self.data_interface.get_quotes(
            codes=codes,
            market=market,
            freq=freq,
            time=time,
            fields=fields,
            adj_type=adj_type,
            use_cache=True
        )
    
    def get_klines(self, codes: Union[str, List[str]], freq: str = '1d',
                  start_time: Union[str, datetime] = None, end_time: Union[str, datetime] = None,
                  fields: List[str] = None, adj_type: str = 'none') -> pd.DataFrame:
        """获取K线数据
        
        Args:
            codes: 股票代码或代码列表
            freq: 数据频率
            start_time: 开始时间
            end_time: 结束时间
            fields: 需要的字段列表
            adj_type: 复权类型 ('none': 不复权, 'front': 前复权, 'back': 后复权)
            
        Returns:
            pd.DataFrame: K线数据，MultiIndex(time, symbol)
        """
        if fields is None:
            fields = ['open', 'high', 'low', 'close', 'volume']
        
        market = self._get_market_from_context()
        
        # 检查频率是否支持
        if freq not in self.supported_frequencies:
            logger.warning(f"不支持的频率: {freq}, 将使用1m数据进行聚合")
            # 如果请求的频率不是1m，则从1m数据聚合
            if freq != '1m':
                return self._aggregate_klines(codes, '1m', freq, start_time, end_time, fields, adj_type)
        
        # 使用统一数据接口获取K线数据
        return self.data_interface.get_klines(
            codes=codes,
            market=market,
            freq=freq,
            start_date=start_time,
            end_date=end_time,
            fields=fields,
            adj_type=adj_type,
            use_cache=True
        )
    
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
            # 使用统一数据接口获取公司行为数据
            corporate_actions = self.data_interface.get_corporate_actions(
                market=market,
                date=date,
                use_cache=True
            )
            
            return corporate_actions
            
        except Exception as e:
            logger.error(f"获取公司行为数据失败: {e}")
            return []
    
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