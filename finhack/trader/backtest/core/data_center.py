"""
数据中心

负责管理回测中的所有数据，包括行情数据、因子数据、参考数据等
支持多市场多频次，从真实数据源加载
现已重构为使用统一的数据接口
"""

import logging
import os
import pickle
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional, Union
import numpy as np

from finhack.library.data import get_data_interface

logger = logging.getLogger(__name__)


class DataCenter:
    """数据中心
    
    管理回测中的所有数据，支持多市场多频次
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
        
        logger.info(f"数据中心初始化完成: {market} {freq} (使用统一数据接口)")
    
    def set_context(self, context: Dict[str, Any]):
        """设置上下文
        
        Args:
            context: 回测上下文
        """
        self.context = context
        logger.debug("数据中心已设置上下文")
    
    def preload_data(self, market: str, start_date: str, end_date: str, 
                    universe: List[str] = None, frequency: str = '1d'):
        """预加载数据
        
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
            
            # 智能预加载K线数据（使用统一接口）
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