import pytest
import pandas as pd
import numpy as np
import time
import os
import sys
import logging
from datetime import datetime, date
import functools

# Add project cache path to allow importing 'runtime'
PROJECT_PATH = os.path.abspath("demo_project")
sys.path.insert(0, os.path.join(PROJECT_PATH, 'data', 'cache'))


from finhack.library.data import DataInterface

# Use an absolute path to the demo project
# Adjust this path if your project structure is different
# Assuming the script is run from the root of the finhack-dev directory

@pytest.fixture(scope="module")
def data_interface():
    """Fixture to initialize DataInterface for the entire test module."""
    if not os.path.exists(PROJECT_PATH):
        raise FileNotFoundError(f"Test project not found at {PROJECT_PATH}. Please ensure the path is correct.")
    
    di = DataInterface(project_path=PROJECT_PATH)
    yield di
    di.shutdown()

class TestDataCenter:
    """Comprehensive tests for the DataInterface class."""

    def _log_and_run(self, func, description: str, **kwargs):
        """Helper to run a test, log details, and return the result."""
        print(f"\n--- Running Test: {description} ---")
        print(f"  Query: {func.__name__}")
        # Format kwargs for readability
        params_str = ", ".join(f"{k}={v!r}" for k, v in kwargs.items())
        print(f"  Parameters: {{ {params_str} }}")

        start_time = time.time()
        result = func(**kwargs)
        duration = time.time() - start_time
        
        print(f"  Execution Time: {duration:.4f} seconds")
        
        if isinstance(result, pd.DataFrame):
            print(f"  Result Shape: {result.shape}")
            if not result.empty:
                print("  Result Preview:")
                # Indent the dataframe printout for better readability
                print(result.head().to_string().replace('\n', '\n  '))
        elif isinstance(result, list):
            print(f"  Result Length: {len(result)}")
            if result:
                print("  Result Preview:")
                print(f"  {result[:5]}")
        
        print(f"--- Test Finished: {description} ---")
        return result

    def test_initialization(self, data_interface):
        """Test if the DataInterface initializes correctly."""
        assert data_interface is not None
        assert data_interface.project_path == PROJECT_PATH
        assert os.path.isdir(data_interface.data_dir)

    # ==================== K-line Data Tests ====================

    def test_get_klines_single_stock(self, data_interface):
        """Test fetching k-lines for a single stock."""
        params = {
            "codes": "000001.SZ",
            "start_date": "2023-01-01",
            "end_date": "2023-01-31"
        }
        df = self._log_and_run(data_interface.get_klines, self.test_get_klines_single_stock.__doc__, **params)
        
        assert not df.empty
        assert isinstance(df.index, pd.MultiIndex)
        assert 'time' in df.index.names
        assert 'symbol' in df.index.names
        assert params["codes"] in df.index.get_level_values('symbol')
        # Check if dates are within the requested range
        dates = df.index.get_level_values('time')
        assert dates.min() >= pd.to_datetime(params["start_date"])
        assert dates.max() <= pd.to_datetime(params["end_date"])

    def test_get_klines_multiple_stocks(self, data_interface):
        """Test fetching k-lines for multiple stocks."""
        params = {
            "codes": ["000001.SZ", "600000.SH"],
            "start_date": "2023-02-01",
            "end_date": "2023-02-15"
        }
        df = self._log_and_run(data_interface.get_klines, self.test_get_klines_multiple_stocks.__doc__, **params)

        assert not df.empty
        unique_symbols = df.index.get_level_values('symbol').unique()
        assert set(params["codes"]) == set(unique_symbols)

    def test_get_klines_adj_types(self, data_interface):
        """Test different adjustment types for k-lines."""
        params = {
            "codes": "000001.SZ",
            "start_date": "2023-01-01",
            "end_date": "2023-01-31"
        }
        df_no_adj = self._log_and_run(data_interface.get_klines, "Get klines with no adjustment", **params, adj_type='none')
        df_front_adj = self._log_and_run(data_interface.get_klines, "Get klines with front adjustment", **params, adj_type='front')
        
        assert not df_no_adj.equals(df_front_adj)
        assert 'close' in df_front_adj.columns

    def test_get_klines_custom_fields(self, data_interface):
        """Test fetching specific fields for k-lines."""
        params = {
            "codes": "000001.SZ",
            "start_date": "2023-01-01",
            "end_date": "2023-01-31",
            "fields": ["open", "close", "volume"]
        }
        df = self._log_and_run(data_interface.get_klines, self.test_get_klines_custom_fields.__doc__, **params)
        
        assert list(df.columns) == params["fields"]

    def test_get_klines_no_data_range(self, data_interface):
        """Test fetching k-lines for a date range with no data."""
        params = {
            "codes": "000001.SZ",
            "start_date": "2099-01-01",
            "end_date": "2099-12-31"
        }
        df = self._log_and_run(data_interface.get_klines, self.test_get_klines_no_data_range.__doc__, **params)
        assert df.empty

    # ==================== Quotes Data Tests ====================
    
    def test_get_quotes_trading_day(self, data_interface):
        """Test get_quotes on a known trading day."""
        params = {
            "codes": ["000001.SZ", "600000.SH"],
            "time": datetime(2023, 1, 3) # A Tuesday
        }
        df = self._log_and_run(data_interface.get_quotes, self.test_get_quotes_trading_day.__doc__, **params)
        
        assert not df.empty
        assert len(df) == len(params["codes"])
        assert set(df.index) == set(params["codes"])

    def test_get_quotes_non_trading_day(self, data_interface):
        """Test get_quotes on a non-trading day (weekend)."""
        params = {
            "codes": ["000001.SZ"],
            "time": datetime(2023, 1, 1) # A Sunday
        }
        df = self._log_and_run(data_interface.get_quotes, self.test_get_quotes_non_trading_day.__doc__, **params)
        
        # Should return the last available data, so not empty
        assert not df.empty
        assert len(df) == 1

    # ==================== Factor Data Tests ====================

    def test_get_factors_matrix_single(self, data_interface):
        """Test fetching a single matrix factor."""
        params = {
            "factor_names": "pe_0",
            "codes": "000001.SZ",
            "start_date": "2023-01-01",
            "end_date": "2023-01-10",
            "factor_type": 'matrix'
        }
        df = self._log_and_run(data_interface.get_factors, self.test_get_factors_matrix_single.__doc__, **params)
        
        assert not df.empty
        assert params["factor_names"] in df.columns
        
    def test_get_factors_matrix_multiple(self, data_interface):
        """Test fetching multiple matrix factors."""
        params = {
            "factor_names": ["pe_0", "pb_0"],
            "codes": ["000001.SZ", "600000.SH"],
            "start_date": "2023-01-01",
            "end_date": "2023-01-10",
            "factor_type": 'matrix'
        }
        df = self._log_and_run(data_interface.get_factors, self.test_get_factors_matrix_multiple.__doc__, **params)
        
        assert not df.empty
        assert all(f in df.columns for f in params["factor_names"])
        assert len(df.index.get_level_values('symbol').unique()) == 2

    # ==================== Reference Data Tests ====================

    def test_get_stock_list(self, data_interface):
        """Test fetching the stock list."""
        params = {"market": 'cn_stock'}
        df = self._log_and_run(data_interface.get_stock_list, self.test_get_stock_list.__doc__, **params)
        
        assert not df.empty
        assert 'code' in df.columns
        assert 'name' in df.columns

    def test_get_adj_factors(self, data_interface):
        """Test fetching adjustment factors."""
        params = {
            "codes": ["000001.SZ"],
            "start_date": "2023-01-01",
            "end_date": "2023-12-31"
        }
        df = self._log_and_run(data_interface.get_adj_factors, self.test_get_adj_factors.__doc__, **params)
        
        assert not df.empty
        assert 'adj_factor' in df.columns
    
    def test_get_trading_calendar(self, data_interface):
        """Test fetching the trading calendar."""
        params = {
            "start_date": date(2023, 1, 1),
            "end_date": date(2023, 1, 31)
        }
        calendar = self._log_and_run(data_interface.get_trading_calendar, self.test_get_trading_calendar.__doc__, **params)
        
        assert isinstance(calendar, list)
        # January 2023 had 16 trading days in China.
        assert len(calendar) == 16
        assert all(isinstance(d, date) for d in calendar)

    # ==================== Cache Tests ====================

    def test_cache_speedup(self, data_interface):
        """Test if caching improves performance."""
        data_interface.clear_cache('kline')
        params = {
            "codes": ["000001.SZ", "600000.SH", "000002.SZ", "600036.SH"],
            "start_date": "2022-01-01",
            "end_date": "2022-12-31",
            "use_cache": True
        }

        print("\n--- Running Test: Caching Speedup ---")
        
        # First call (uncached)
        start_time1 = time.time()
        self._log_and_run(data_interface.get_klines, "Uncached kline fetch", **params)
        duration1 = time.time() - start_time1

        # Second call (cached)
        start_time2 = time.time()
        self._log_and_run(data_interface.get_klines, "Cached kline fetch", **params)
        duration2 = time.time() - start_time2

        logging.info(f"Uncached call took: {duration1:.4f}s")
        logging.info(f"Cached call took: {duration2:.4f}s")

        assert duration2 < duration1
        # A simple heuristic for speedup, might need adjustment on very fast systems
        if duration1 > 0.001:
            assert duration2 < duration1 * 0.5 

    def test_clear_cache(self, data_interface):
        """Test clearing the cache."""
        print("\n--- Running Test: Cache Clearing ---")
        data_interface.clear_cache()
        stats = data_interface.get_cache_stats()
        for cache_name, cache_stats in stats.items():
            assert cache_stats['size'] == 0
        print("  All caches cleared successfully.")

        # Add something to cache
        self._log_and_run(data_interface.get_stock_list, "Populate metadata cache", use_cache=True)
        stats_after = data_interface.get_cache_stats()
        assert stats_after['metadata_cache']['size'] > 0
        print(f"  Metadata cache populated, size: {stats_after['metadata_cache']['size']}")

        # Clear again
        data_interface.clear_cache('reference')
        stats_final = data_interface.get_cache_stats()
        assert stats_final['metadata_cache']['size'] == 0
        print("  Reference caches cleared successfully.")
