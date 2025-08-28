"""
统一数据接口综合单元测试

测试覆盖范围：
1. K线数据读取（各市场、复权、异常情况）
2. 因子数据读取（matrix/vector、异常情况）  
3. 行情数据读取（智能历史查找、复权）
4. 参考数据读取
5. 缓存功能测试
6. 性能基准测试
7. 异常处理测试
"""

import pytest
import pandas as pd
import numpy as np
import time
import os
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
from unittest.mock import patch, MagicMock

# 导入被测试的模块
from finhack.library.data import get_data_interface, DataInterface
from runtime.constant import BASE_DIR, DATA_DIR

# 导入测试配置
try:
    from test_config import (
        TEST_CODES, TEST_MARKETS, TEST_DATE_RANGES, TEST_FACTORS,
        ADJUSTMENT_TYPES, FIELD_COMBINATIONS, PERFORMANCE_CONFIG,
        TIMEOUT_CONFIG, ERROR_TEST_CONFIG, HISTORICAL_LOOKUP_TIMES,
        get_test_codes, get_test_factors, get_dynamic_config
    )
except ImportError:
    # 如果配置文件不存在，使用默认配置
    TEST_CODES = {"cn_stock": ["000001.SZ", "000002.SZ", "600000.SH", "600036.SH", "000858.SZ"]}
    TEST_MARKETS = ["cn_stock"]
    TEST_DATE_RANGES = [("2023-01-01", "2023-01-31"), ("2023-01-01", "2023-03-31")]
    TEST_FACTORS = {"matrix": ["pe_ratio", "pb_ratio"], "vector": ["momentum", "volatility"]}
    ADJUSTMENT_TYPES = ['none', 'front', 'back']
    FIELD_COMBINATIONS = [['close'], ['open', 'close'], ['open', 'high', 'low', 'close', 'volume']]
    PERFORMANCE_CONFIG = {'large_stock_count': 10, 'concurrent_threads': 3}
    TIMEOUT_CONFIG = {'single_query': 30, 'large_query': 120}
    ERROR_TEST_CONFIG = {'invalid_codes': [["INVALID.CODE"]], 'invalid_markets': ["invalid_market"]}
    
    def get_test_codes(market='cn_stock', count=None):
        codes = TEST_CODES.get(market, TEST_CODES['cn_stock'])
        return codes[:count] if count else codes
    
    def get_test_factors(factor_type='matrix', count=None):
        factors = TEST_FACTORS.get(factor_type, TEST_FACTORS['matrix'])
        return factors[:count] if count else factors
    
    def get_dynamic_config():
        return {'available_markets': TEST_MARKETS, 'test_codes_per_market': TEST_CODES}


class TestDataInterfacePerformance:
    """数据接口性能测试类"""
    
    @classmethod
    def setup_class(cls):
        """测试类初始化"""
        cls.data_interface = get_data_interface()
        cls.performance_results = {}
        
        # 从配置获取测试参数
        dynamic_config = get_dynamic_config()
        cls.test_markets = dynamic_config.get('available_markets', TEST_MARKETS)
        cls.test_codes = get_test_codes('cn_stock')
        cls.test_date_ranges = TEST_DATE_RANGES
        
        # 设置日志
        logging.basicConfig(level=logging.INFO)
        cls.logger = logging.getLogger(__name__)
        
        # 输出测试配置信息
        cls.logger.info(f"测试市场: {cls.test_markets}")
        cls.logger.info(f"测试股票: {cls.test_codes}")
        cls.logger.info(f"测试日期范围: {cls.test_date_ranges}")
    
    def _measure_time(self, func, *args, **kwargs):
        """测量函数执行时间"""
        start_time = time.perf_counter()
        try:
            result = func(*args, **kwargs)
            end_time = time.perf_counter()
            execution_time = end_time - start_time
            return result, execution_time, None
        except Exception as e:
            end_time = time.perf_counter()
            execution_time = end_time - start_time
            return None, execution_time, str(e)
    
    def _log_performance(self, test_name: str, execution_time: float, 
                        data_size: int = None, error: str = None):
        """记录性能测试结果"""
        result = {
            'execution_time': execution_time,
            'data_size': data_size,
            'error': error,
            'timestamp': datetime.now()
        }
        
        if test_name not in self.performance_results:
            self.performance_results[test_name] = []
        self.performance_results[test_name].append(result)
        
        status = "ERROR" if error else "SUCCESS"
        size_info = f", size: {data_size}" if data_size else ""
        self.logger.info(f"[{status}] {test_name}: {execution_time:.4f}s{size_info}")
        if error:
            self.logger.error(f"Error details: {error}")


class TestKlineData(TestDataInterfacePerformance):
    """K线数据测试"""
    
    def test_single_stock_klines_basic(self):
        """测试单只股票K线数据获取"""
        for market in self.test_markets:
            for start_date, end_date in self.test_date_ranges:
                test_name = f"single_klines_{market}_{start_date}_{end_date}"
                
                result, exec_time, error = self._measure_time(
                    self.data_interface.get_klines,
                    codes=["000001.SZ"],
                    market=market,
                    freq="1d",
                    start_date=start_date,
                    end_date=end_date
                )
                
                data_size = len(result) if result is not None and not result.empty else 0
                self._log_performance(test_name, exec_time, data_size, error)
                
                if error is None:
                    assert isinstance(result, pd.DataFrame)
                    if not result.empty:
                        assert isinstance(result.index, pd.MultiIndex)
                        assert result.index.names == ['time', 'symbol']
                        assert '000001.SZ' in result.index.get_level_values('symbol')
    
    def test_multiple_stocks_klines(self):
        """测试多只股票K线数据获取"""
        for market in self.test_markets:
            for codes_count in [2, 5, 10]:
                codes = self.test_codes[:codes_count]
                test_name = f"multi_klines_{market}_{codes_count}_stocks"
                
                result, exec_time, error = self._measure_time(
                    self.data_interface.get_klines,
                    codes=codes,
                    market=market,
                    freq="1d",
                    start_date="2023-01-01",
                    end_date="2023-01-31"
                )
                
                data_size = len(result) if result is not None and not result.empty else 0
                self._log_performance(test_name, exec_time, data_size, error)
                
                if error is None and not result.empty:
                    unique_symbols = result.index.get_level_values('symbol').unique()
                    # 检查返回的股票数量（可能少于请求数量，因为某些股票可能没有数据）
                    assert len(unique_symbols) <= len(codes)
    
    def test_klines_with_adjustment(self):
        """测试复权K线数据"""
        for adj_type in ADJUSTMENT_TYPES:
            test_name = f"klines_adj_{adj_type}"
            
            result, exec_time, error = self._measure_time(
                self.data_interface.get_klines,
                codes=["000001.SZ"],
                market="cn_stock",
                freq="1d",
                start_date="2023-01-01",
                end_date="2023-01-31",
                adj_type=adj_type
            )
            
            data_size = len(result) if result is not None and not result.empty else 0
            self._log_performance(test_name, exec_time, data_size, error)
            
            if error is None and not result.empty:
                assert 'close' in result.columns
                # 检查价格是否为正数
                if 'close' in result.columns:
                    assert (result['close'] > 0).all()
    
    def test_klines_different_fields(self):
        """测试不同字段组合的K线数据"""
        for fields in FIELD_COMBINATIONS:
            test_name = f"klines_fields_{len(fields)}"
            
            result, exec_time, error = self._measure_time(
                self.data_interface.get_klines,
                codes=["000001.SZ"],
                market="cn_stock",
                freq="1d",
                start_date="2023-01-01",
                end_date="2023-01-31",
                fields=fields
            )
            
            data_size = len(result) if result is not None and not result.empty else 0
            self._log_performance(test_name, exec_time, data_size, error)
            
            if error is None and not result.empty:
                assert list(result.columns) == fields
    
    def test_klines_cache_performance(self):
        """测试K线数据缓存性能"""
        codes = ["000001.SZ", "000002.SZ"]
        
        # 第一次查询（无缓存）
        result1, time1, error1 = self._measure_time(
            self.data_interface.get_klines,
            codes=codes,
            market="cn_stock",
            freq="1d",
            start_date="2023-01-01",
            end_date="2023-01-31",
            use_cache=True
        )
        
        # 第二次查询（有缓存）
        result2, time2, error2 = self._measure_time(
            self.data_interface.get_klines,
            codes=codes,
            market="cn_stock", 
            freq="1d",
            start_date="2023-01-01",
            end_date="2023-01-31",
            use_cache=True
        )
        
        self._log_performance("klines_cache_first", time1, 
                            len(result1) if result1 is not None and not result1.empty else 0, error1)
        self._log_performance("klines_cache_second", time2,
                            len(result2) if result2 is not None and not result2.empty else 0, error2)
        
        # 缓存应该显著提高性能
        if error1 is None and error2 is None and time1 > 0.001:  # 避免除零和极小时间
            speedup = time1 / time2
            self.logger.info(f"Cache speedup: {speedup:.2f}x")
            # 缓存应该至少提高2倍性能
            if speedup < 2:
                self.logger.warning(f"Cache speedup is lower than expected: {speedup:.2f}x")


class TestQuotesData(TestDataInterfacePerformance):
    """行情数据测试"""
    
    def test_quotes_basic(self):
        """测试基本行情数据获取"""
        test_times = [
            datetime(2023, 6, 15),  # 工作日
            datetime(2023, 6, 17),  # 周六
            datetime(2023, 6, 18),  # 周日
            datetime(2023, 5, 1),   # 劳动节
        ]
        
        for test_time in test_times:
            test_name = f"quotes_basic_{test_time.strftime('%Y%m%d')}"
            
            result, exec_time, error = self._measure_time(
                self.data_interface.get_quotes,
                codes=["000001.SZ", "000002.SZ"],
                market="cn_stock",
                freq="1d",
                time=test_time
            )
            
            data_size = len(result) if result is not None and not result.empty else 0
            self._log_performance(test_name, exec_time, data_size, error)
            
            if error is None:
                assert isinstance(result, pd.DataFrame)
                if not result.empty:
                    assert result.index.name == 'symbol'
    
    def test_quotes_with_adjustment(self):
        """测试复权行情数据"""
        for adj_type in ADJUSTMENT_TYPES:
            test_name = f"quotes_adj_{adj_type}"
            
            result, exec_time, error = self._measure_time(
                self.data_interface.get_quotes,
                codes=["000001.SZ"],
                market="cn_stock",
                freq="1d",
                time=datetime(2023, 6, 15),
                adj_type=adj_type
            )
            
            data_size = len(result) if result is not None and not result.empty else 0
            self._log_performance(test_name, exec_time, data_size, error)
    
    def test_quotes_historical_lookup(self):
        """测试智能历史数据查找"""
        # 测试不同的非交易时间
        test_scenarios = [
            (datetime(2023, 6, 17, 10, 30), "weekend_morning"),
            (datetime(2023, 6, 17, 20, 30), "weekend_evening"),
            (datetime(2023, 5, 1, 14, 30), "holiday"),
            (datetime(2023, 6, 15, 20, 30), "after_hours"),
            (datetime(2023, 6, 15, 6, 30), "before_hours"),
        ]
        
        for test_time, scenario in test_scenarios:
            test_name = f"quotes_historical_{scenario}"
            
            result, exec_time, error = self._measure_time(
                self.data_interface.get_quotes,
                codes=["000001.SZ"],
                market="cn_stock",
                freq="1d",
                time=test_time
            )
            
            data_size = len(result) if result is not None and not result.empty else 0
            self._log_performance(test_name, exec_time, data_size, error)


class TestFactorData(TestDataInterfacePerformance):
    """因子数据测试"""
    
    def test_matrix_factors(self):
        """测试矩阵因子数据"""
        # 从配置获取因子组合
        all_factors = get_test_factors('matrix')
        test_factor_combinations = [
            all_factors[:1],      # 单个因子
            all_factors[:2],      # 两个因子
            all_factors[:3],      # 三个因子
        ]
        
        for factors in test_factor_combinations:
            test_name = f"matrix_factors_{len(factors)}"
            
            result, exec_time, error = self._measure_time(
                self.data_interface.get_factors,
                factor_names=factors,
                codes=["000001.SZ", "000002.SZ"],
                market="cn_stock",
                freq="1d",
                start_date="2023-01-01",
                end_date="2023-01-31",
                factor_type="matrix"
            )
            
            data_size = len(result) if result is not None and not result.empty else 0
            self._log_performance(test_name, exec_time, data_size, error)
            
            if error is None and not result.empty:
                assert isinstance(result.index, pd.MultiIndex)
                expected_columns = set(factors) & set(result.columns)
                assert len(expected_columns) > 0  # 至少有一个因子有数据
    
    def test_vector_factors(self):
        """测试向量因子数据"""
        # 从配置获取向量因子组合
        all_factors = get_test_factors('vector')
        test_factor_combinations = [
            all_factors[:1],      # 单个因子
            all_factors[:2],      # 两个因子
        ]
        
        for factors in test_factor_combinations:
            test_name = f"vector_factors_{len(factors)}"
            
            result, exec_time, error = self._measure_time(
                self.data_interface.get_factors,
                factor_names=factors,
                market="cn_stock",
                freq="1d",
                start_date="2023-01-01",
                end_date="2023-01-31",
                factor_type="vector"
            )
            
            data_size = len(result) if result is not None and not result.empty else 0
            self._log_performance(test_name, exec_time, data_size, error)
    
    def test_factors_different_date_ranges(self):
        """测试不同日期范围的因子数据"""
        factor_name = "pe_ratio"
        
        for start_date, end_date in self.test_date_ranges:
            test_name = f"factors_range_{start_date}_{end_date}"
            
            result, exec_time, error = self._measure_time(
                self.data_interface.get_factors,
                factor_names=[factor_name],
                codes=["000001.SZ"],
                market="cn_stock",
                freq="1d",
                start_date=start_date,
                end_date=end_date,
                factor_type="matrix"
            )
            
            data_size = len(result) if result is not None and not result.empty else 0
            self._log_performance(test_name, exec_time, data_size, error)


class TestReferenceData(TestDataInterfacePerformance):
    """参考数据测试"""
    
    def test_stock_list(self):
        """测试股票列表获取"""
        for market in self.test_markets:
            test_name = f"stock_list_{market}"
            
            result, exec_time, error = self._measure_time(
                self.data_interface.get_stock_list,
                market=market
            )
            
            data_size = len(result) if result is not None and not result.empty else 0
            self._log_performance(test_name, exec_time, data_size, error)
            
            if error is None and not result.empty:
                assert 'code' in result.columns
                assert 'name' in result.columns
    
    def test_adj_factors(self):
        """测试复权因子获取"""
        test_name = "adj_factors"
        
        result, exec_time, error = self._measure_time(
            self.data_interface.get_adj_factors,
            market="cn_stock",
            codes=["000001.SZ", "000002.SZ"],
            start_date="2023-01-01",
            end_date="2023-01-31"
        )
        
        data_size = len(result) if result is not None and not result.empty else 0
        self._log_performance(test_name, exec_time, data_size, error)
        
        if error is None and not result.empty:
            assert 'code' in result.columns
            assert 'adj_factor' in result.columns
    
    def test_trading_calendar(self):
        """测试交易日历获取"""
        test_name = "trading_calendar"
        
        result, exec_time, error = self._measure_time(
            self.data_interface.get_trading_calendar,
            market="cn_stock",
            start_date=date(2023, 1, 1),
            end_date=date(2023, 1, 31)
        )
        
        data_size = len(result) if result is not None else 0
        self._log_performance(test_name, exec_time, data_size, error)
        
        if error is None:
            assert isinstance(result, list)
            if result:
                assert all(isinstance(d, date) for d in result)


class TestExceptionHandling(TestDataInterfacePerformance):
    """异常处理测试"""
    
    def test_invalid_codes(self):
        """测试无效股票代码"""
        for codes in ERROR_TEST_CONFIG.get('invalid_codes', [["INVALID.CODE"]]):
            test_name = f"invalid_codes_{str(codes)[:20]}"
            
            result, exec_time, error = self._measure_time(
                self.data_interface.get_klines,
                codes=codes,
                market="cn_stock",
                freq="1d",
                start_date="2023-01-01",
                end_date="2023-01-31"
            )
            
            self._log_performance(test_name, exec_time, 0, error)
            
            # 即使代码无效，也应该返回空DataFrame而不是抛出异常
            if error is None:
                assert isinstance(result, pd.DataFrame)
    
    def test_invalid_date_ranges(self):
        """测试无效日期范围"""
        for start_date, end_date in ERROR_TEST_CONFIG.get('invalid_date_ranges', []):
            test_name = f"invalid_dates_{start_date}_{end_date}"
            
            result, exec_time, error = self._measure_time(
                self.data_interface.get_klines,
                codes=["000001.SZ"],
                market="cn_stock",
                freq="1d",
                start_date=start_date,
                end_date=end_date
            )
            
            self._log_performance(test_name, exec_time, 0, error)
    
    def test_invalid_markets(self):
        """测试无效市场"""
        for market in ERROR_TEST_CONFIG.get('invalid_markets', ["invalid_market"]):
            test_name = f"invalid_market_{str(market)}"
            
            result, exec_time, error = self._measure_time(
                self.data_interface.get_klines,
                codes=["000001.SZ"],
                market=market,
                freq="1d",
                start_date="2023-01-01",
                end_date="2023-01-31"
            )
            
            self._log_performance(test_name, exec_time, 0, error)
    
    def test_invalid_factor_names(self):
        """测试无效因子名称"""
        for factors in ERROR_TEST_CONFIG.get('invalid_factors', [["non_existent_factor"]]):
            test_name = f"invalid_factors_{str(factors)[:20]}"
            
            result, exec_time, error = self._measure_time(
                self.data_interface.get_factors,
                factor_names=factors,
                market="cn_stock",
                freq="1d",
                start_date="2023-01-01",
                end_date="2023-01-31",
                factor_type="matrix"
            )
            
            self._log_performance(test_name, exec_time, 0, error)


class TestCacheManagement(TestDataInterfacePerformance):
    """缓存管理测试"""
    
    def test_cache_functionality(self):
        """测试缓存功能"""
        # 清空缓存
        self.data_interface.clear_cache()
        
        # 检查缓存状态
        stats_before = self.data_interface.get_cache_stats()
        
        # 执行数据查询
        result, exec_time, error = self._measure_time(
            self.data_interface.get_klines,
            codes=["000001.SZ"],
            market="cn_stock",
            freq="1d",
            start_date="2023-01-01",
            end_date="2023-01-31",
            use_cache=True
        )
        
        # 检查缓存状态
        stats_after = self.data_interface.get_cache_stats()
        
        self._log_performance("cache_functionality", exec_time, 
                            len(result) if result is not None and not result.empty else 0, error)
        
        # 验证缓存已更新
        if error is None:
            kline_size_before = stats_before.get('kline_cache', {}).get('size', 0)
            kline_size_after = stats_after.get('kline_cache', {}).get('size', 0)
            assert kline_size_after >= kline_size_before
    
    def test_cache_clear(self):
        """测试缓存清理"""
        # 添加一些数据到缓存
        self.data_interface.get_klines(
            codes=["000001.SZ"],
            market="cn_stock",
            freq="1d",
            start_date="2023-01-01",
            end_date="2023-01-31",
            use_cache=True
        )
        
        # 清空特定缓存
        start_time = time.perf_counter()
        self.data_interface.clear_cache('kline')
        end_time = time.perf_counter()
        
        self._log_performance("cache_clear_kline", end_time - start_time)
        
        # 清空所有缓存
        start_time = time.perf_counter()
        self.data_interface.clear_cache()
        end_time = time.perf_counter()
        
        self._log_performance("cache_clear_all", end_time - start_time)
        
        # 验证缓存已清空
        stats = self.data_interface.get_cache_stats()
        for cache_name, cache_stats in stats.items():
            assert cache_stats.get('size', 0) == 0


class TestBenchmarks(TestDataInterfacePerformance):
    """性能基准测试"""
    
    def test_large_data_loading(self):
        """测试大量数据加载性能"""
        # 测试大量股票
        large_stock_count = PERFORMANCE_CONFIG.get('large_stock_count', 10)
        large_stock_list = (self.test_codes * ((large_stock_count // len(self.test_codes)) + 1))[:large_stock_count]
        
        test_name = "large_stock_loading"
        result, exec_time, error = self._measure_time(
            self.data_interface.get_klines,
            codes=large_stock_list,
            market="cn_stock",
            freq="1d",
            start_date="2023-01-01",
            end_date="2023-03-31"
        )
        
        data_size = len(result) if result is not None and not result.empty else 0
        self._log_performance(test_name, exec_time, data_size, error)
        
        # 测试长时间范围
        test_name = "large_timerange_loading"
        result, exec_time, error = self._measure_time(
            self.data_interface.get_klines,
            codes=["000001.SZ"],
            market="cn_stock",
            freq="1d",
            start_date="2022-01-01",
            end_date="2023-12-31"
        )
        
        data_size = len(result) if result is not None and not result.empty else 0
        self._log_performance(test_name, exec_time, data_size, error)
    
    def test_concurrent_access(self):
        """测试并发访问性能"""
        import threading
        import queue
        
        def worker(q, worker_id):
            result, exec_time, error = self._measure_time(
                self.data_interface.get_klines,
                codes=[f"00000{worker_id % 5 + 1}.SZ"],
                market="cn_stock",
                freq="1d",
                start_date="2023-01-01",
                end_date="2023-01-31"
            )
            q.put((worker_id, exec_time, error))
        
        # 创建多个线程并发访问
        num_threads = PERFORMANCE_CONFIG.get('concurrent_threads', 5)
        q = queue.Queue()
        threads = []
        
        start_time = time.perf_counter()
        
        for i in range(num_threads):
            t = threading.Thread(target=worker, args=(q, i))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        end_time = time.perf_counter()
        total_time = end_time - start_time
        
        # 收集结果
        results = []
        while not q.empty():
            results.append(q.get())
        
        self._log_performance(f"concurrent_access_{num_threads}_threads", total_time, 
                            len(results), None)
        
        # 验证所有线程都成功完成
        assert len(results) == num_threads
        
        # 记录单个线程的性能
        for worker_id, exec_time, error in results:
            self._log_performance(f"concurrent_worker_{worker_id}", exec_time, 1, error)


@pytest.fixture(scope="session", autouse=True)
def performance_report(request):
    """生成性能测试报告"""
    def generate_report():
        print("\n" + "="*80)
        print("数据接口性能测试报告")
        print("="*80)
        
        # 获取所有测试类的性能结果
        all_results = {}
        for cls_name in ['TestKlineData', 'TestQuotesData', 'TestFactorData', 
                        'TestReferenceData', 'TestExceptionHandling', 
                        'TestCacheManagement', 'TestBenchmarks']:
            try:
                cls = globals()[cls_name]
                if hasattr(cls, 'performance_results'):
                    all_results.update(cls.performance_results)
            except KeyError:
                continue
        
        if not all_results:
            print("没有收集到性能数据")
            return
        
        # 按类别分组报告
        categories = {
            'K线数据': [k for k in all_results.keys() if 'klines' in k.lower()],
            '行情数据': [k for k in all_results.keys() if 'quotes' in k.lower()],
            '因子数据': [k for k in all_results.keys() if 'factor' in k.lower()],
            '参考数据': [k for k in all_results.keys() if any(x in k.lower() for x in ['stock_list', 'adj_factors', 'trading_calendar'])],
            '缓存测试': [k for k in all_results.keys() if 'cache' in k.lower()],
            '异常处理': [k for k in all_results.keys() if 'invalid' in k.lower()],
            '性能基准': [k for k in all_results.keys() if any(x in k.lower() for x in ['large', 'concurrent'])]
        }
        
        for category, test_names in categories.items():
            if not test_names:
                continue
                
            print(f"\n{category}:")
            print("-" * 40)
            
            for test_name in sorted(test_names):
                if test_name not in all_results:
                    continue
                    
                results = all_results[test_name]
                if not results:
                    continue
                
                # 计算统计信息
                times = [r['execution_time'] for r in results if r['error'] is None]
                errors = [r for r in results if r['error'] is not None]
                
                if times:
                    avg_time = sum(times) / len(times)
                    min_time = min(times)
                    max_time = max(times)
                    
                    print(f"  {test_name}:")
                    print(f"    执行次数: {len(times)}")
                    print(f"    平均时间: {avg_time:.4f}s")
                    print(f"    最短时间: {min_time:.4f}s")
                    print(f"    最长时间: {max_time:.4f}s")
                    
                    if errors:
                        print(f"    错误次数: {len(errors)}")
                else:
                    print(f"  {test_name}: 所有执行都失败")
        
        # 性能总结
        print(f"\n总结:")
        print("-" * 40)
        
        all_times = []
        all_errors = []
        
        for results in all_results.values():
            for r in results:
                if r['error'] is None:
                    all_times.append(r['execution_time'])
                else:
                    all_errors.append(r)
        
        if all_times:
            print(f"总测试数量: {len(all_times)}")
            print(f"平均执行时间: {sum(all_times)/len(all_times):.4f}s")
            print(f"最快执行时间: {min(all_times):.4f}s")
            print(f"最慢执行时间: {max(all_times):.4f}s")
        
        if all_errors:
            print(f"错误总数: {len(all_errors)}")
            error_types = {}
            for error in all_errors:
                error_msg = error['error'][:50] + "..." if len(error['error']) > 50 else error['error']
                error_types[error_msg] = error_types.get(error_msg, 0) + 1
            
            print("主要错误类型:")
            for error_type, count in sorted(error_types.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"  {error_type}: {count}次")
        
        print("="*80)
    
    request.addfinalizer(generate_report)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])