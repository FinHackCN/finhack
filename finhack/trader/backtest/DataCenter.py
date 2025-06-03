import os
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
import logging
import pickle
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import sys
import importlib

# 导入项目的kline模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
from finhack.library.kline import loadKline, inspectKline
from runtime.constant import *


class DataCenter:
    """数据中心，负责数据的加载、缓存和计算"""
    
    def __init__(self, cache_enabled: bool = True):
        """
        初始化数据中心
        
        Args:
            cache_enabled: 是否启用缓存
        """
        self.cache_enabled = cache_enabled
        self.logger = logging.getLogger(__name__)
        
        # 数据缓存
        self._kline_cache: Dict[str, pd.DataFrame] = {}
        self._factor_cache: Dict[str, pd.DataFrame] = {}
        self._model_cache: Dict[str, Any] = {}
        
        # 当前时间窗口
        self._current_window: Optional[Tuple[datetime, datetime]] = None
        self._window_size_days = 30  # 默认数据窗口大小
        
        # 线程池
        self._thread_pool = ThreadPoolExecutor(max_workers=4)
        
    def set_window_size(self, days: int):
        """设置数据窗口大小"""
        self._window_size_days = days
        
    def update_time_window(self, current_time: datetime):
        """
        更新当前时间窗口，决定是否需要加载新数据
        
        Args:
            current_time: 当前时间
        """
        window_start = current_time - timedelta(days=self._window_size_days)
        window_end = current_time + timedelta(days=1)
        
        # 检查是否需要更新窗口
        if self._current_window is None:
            self._current_window = (window_start, window_end)
            self._clear_cache()
        else:
            old_start, old_end = self._current_window
            # 如果当前时间超出窗口范围，更新窗口
            if current_time > old_end or current_time < old_start:
                self._current_window = (window_start, window_end)
                self._clear_cache()
                
    def get_kline(self, adapter_id: str, codes: Union[str, List[str]], 
                  frequency: str = "1d", end_time: Optional[datetime] = None,
                  count: int = 100) -> pd.DataFrame:
        """
        获取K线数据
        
        Args:
            adapter_id: 适配器ID
            codes: 股票代码或代码列表
            frequency: K线频率
            end_time: 结束时间，默认为当前时间
            count: 获取的K线数量
            
        Returns:
            K线数据DataFrame
        """
        if isinstance(codes, str):
            codes = [codes]
            
        if end_time is None:
            end_time = datetime.now()
            
        # 计算开始时间
        if frequency.endswith('m'):
            minutes = int(frequency[:-1])
            start_time = end_time - timedelta(minutes=minutes * count)
        elif frequency.endswith('d'):
            start_time = end_time - timedelta(days=count * 2)  # 考虑非交易日
        else:
            raise ValueError(f"Unsupported frequency: {frequency}")
            
        # 生成缓存键
        cache_key = self._generate_cache_key(
            "kline", adapter_id, codes, frequency, 
            start_time.strftime("%Y%m%d"), end_time.strftime("%Y%m%d")
        )
        
        # 检查缓存
        if self.cache_enabled and cache_key in self._kline_cache:
            return self._kline_cache[cache_key]
            
        # 加载数据
        try:
            df = loadKline(
                market='cn_stock',  # TODO: 从adapter_id推断市场
                freq=frequency,
                start_date=start_time.strftime("%Y%m%d"),
                end_date=end_time.strftime("%Y%m%d"),
                code_list=codes,
                cache=self.cache_enabled
            )
            
            # 转换时间格式
            if not df.empty:
                df['time'] = pd.to_datetime(df['time'])
                # 过滤到指定时间范围
                df = df[df['time'] <= end_time].tail(count * len(codes))
                
            # 缓存数据
            if self.cache_enabled:
                self._kline_cache[cache_key] = df
                
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading kline data: {str(e)}")
            return pd.DataFrame()
            
    def get_factors(self, adapter_id: str, codes: Union[str, List[str]], 
                   factor_names: List[str], end_time: Optional[datetime] = None,
                   count: int = 100) -> pd.DataFrame:
        """
        获取因子数据
        
        Args:
            adapter_id: 适配器ID
            codes: 股票代码或代码列表
            factor_names: 因子名称列表
            end_time: 结束时间
            count: 获取的数据数量
            
        Returns:
            因子数据DataFrame
        """
        if isinstance(codes, str):
            codes = [codes]
            
        if end_time is None:
            end_time = datetime.now()
            
        # 生成缓存键
        cache_key = self._generate_cache_key(
            "factors", adapter_id, codes, factor_names,
            end_time.strftime("%Y%m%d"), str(count)
        )
        
        # 检查缓存
        if self.cache_enabled and cache_key in self._factor_cache:
            return self._factor_cache[cache_key]
            
        # 加载因子数据
        try:
            # TODO: 实现因子数据加载逻辑
            # 这里需要根据实际的因子数据存储格式来实现
            factors_dir = f"{DATA_DIR}/factors/matrix/cn_stock/1d"
            
            all_data = []
            for code in codes:
                code_dir = os.path.join(factors_dir, code.split('.')[0])
                if os.path.exists(code_dir):
                    # 加载因子文件
                    for factor_name in factor_names:
                        factor_file = os.path.join(code_dir, f"{factor_name}.pkl")
                        if os.path.exists(factor_file):
                            with open(factor_file, 'rb') as f:
                                factor_data = pickle.load(f)
                                # TODO: 处理因子数据格式
                                pass
                                
            # 合并数据
            if all_data:
                df = pd.concat(all_data, ignore_index=True)
                # 缓存数据
                if self.cache_enabled:
                    self._factor_cache[cache_key] = df
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error loading factor data: {str(e)}")
            return pd.DataFrame()
            
    def compute_factors(self, adapter_id: str, factor_list: List[str], 
                       data: pd.DataFrame) -> pd.DataFrame:
        """
        计算因子
        
        Args:
            adapter_id: 适配器ID
            factor_list: 因子公式列表
            data: 输入数据
            
        Returns:
            计算后的因子数据
        """
        try:
            result = data.copy()
            
            # 动态导入因子计算模块
            indicators_path = f"{BASE_DIR}/indicators/cn_stock/x1d"
            if os.path.exists(indicators_path):
                sys.path.insert(0, indicators_path)
                
            for factor_expr in factor_list:
                # 解析因子表达式
                # 支持格式：factor_name = expression 或 module.function(params)
                if '=' in factor_expr:
                    factor_name, expression = factor_expr.split('=', 1)
                    factor_name = factor_name.strip()
                    expression = expression.strip()
                    
                    # 使用eval计算表达式（注意安全性）
                    # 创建安全的计算环境
                    safe_dict = {
                        'data': data,
                        'np': np,
                        'pd': pd,
                        'result': result,
                        # 添加常用的技术指标函数
                        'SMA': self._sma,
                        'EMA': self._ema,
                        'RSI': self._rsi,
                        'MACD': self._macd,
                    }
                    
                    try:
                        result[factor_name] = eval(expression, {"__builtins__": {}}, safe_dict)
                    except Exception as e:
                        self.logger.error(f"Error computing factor {factor_name}: {str(e)}")
                        
                elif '.' in factor_expr and '(' in factor_expr:
                    # 调用模块函数
                    module_name, func_call = factor_expr.split('.', 1)
                    func_name = func_call.split('(')[0]
                    
                    try:
                        module = importlib.import_module(module_name)
                        if hasattr(module, func_name):
                            func = getattr(module, func_name)
                            # TODO: 解析函数参数
                            result = func(result)
                    except Exception as e:
                        self.logger.error(f"Error calling {factor_expr}: {str(e)}")
                        
            return result
            
        except Exception as e:
            self.logger.error(f"Error computing factors: {str(e)}")
            return data
            
    def ml_predict(self, model_id: str, features: pd.DataFrame) -> np.ndarray:
        """
        使用机器学习模型进行预测
        
        Args:
            model_id: 模型ID
            features: 特征数据
            
        Returns:
            预测结果
        """
        try:
            # 检查模型缓存
            if model_id not in self._model_cache:
                # 加载模型
                model_path = f"{MODELS_DIR}/model_{model_id}.pkl"
                if os.path.exists(model_path):
                    with open(model_path, 'rb') as f:
                        self._model_cache[model_id] = pickle.load(f)
                else:
                    raise FileNotFoundError(f"Model {model_id} not found")
                    
            model = self._model_cache[model_id]
            
            # 进行预测
            predictions = model.predict(features)
            return predictions
            
        except Exception as e:
            self.logger.error(f"Error in ML prediction: {str(e)}")
            return np.array([])
            
    def get_calendar(self, market: str = "cn_stock") -> List[str]:
        """获取交易日历"""
        try:
            # TODO: 实现交易日历获取逻辑
            # 暂时返回简单的示例
            from finhack.library.db import DB
            cal = DB.select_to_df(
                "select cal_date from astock_trade_cal where is_open=1 "
                "and exchange='SSE' order by cal_date asc", 'tushare'
            )
            return cal['cal_date'].tolist()
        except Exception as e:
            self.logger.error(f"Error getting calendar: {str(e)}")
            return []
            
    def _generate_cache_key(self, *args) -> str:
        """生成缓存键"""
        key_str = "_".join(str(arg) for arg in args)
        return hashlib.md5(key_str.encode()).hexdigest()
        
    def _clear_cache(self):
        """清理缓存"""
        self._kline_cache.clear()
        self._factor_cache.clear()
        
    # 技术指标计算函数
    def _sma(self, data: pd.Series, period: int) -> pd.Series:
        """简单移动平均"""
        return data.rolling(window=period).mean()
        
    def _ema(self, data: pd.Series, period: int) -> pd.Series:
        """指数移动平均"""
        return data.ewm(span=period, adjust=False).mean()
        
    def _rsi(self, data: pd.Series, period: int = 14) -> pd.Series:
        """相对强弱指标"""
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
        
    def _macd(self, data: pd.Series, fast: int = 12, slow: int = 26, 
              signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """MACD指标"""
        ema_fast = self._ema(data, fast)
        ema_slow = self._ema(data, slow)
        macd_line = ema_fast - ema_slow
        signal_line = self._ema(macd_line, signal)
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram
        
    def __del__(self):
        """清理资源"""
        self._thread_pool.shutdown(wait=False)
