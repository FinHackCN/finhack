"""
全球外汇市场适配器

实现全球外汇市场的交易规则和事件生成
"""

from typing import List, Dict, Any
from datetime import datetime, date, time, timedelta
import logging

from .global_cryptospot_adapter import GlobalCryptoSpotMarketAdapter
from ...events.event_types import BaseEvent, MarketEvent, EventTypeEnum

logger = logging.getLogger(__name__)


class GlobalForexMarketAdapter(GlobalCryptoSpotMarketAdapter):
    """全球外汇市场适配器
    
    基于加密货币现货市场规则，但有一些特殊差异
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """初始化全球外汇市场适配器"""
        # 如果没有提供配置，使用默认配置
        if not config:
            config = self._get_default_config()
        
        # 修改市场名称
        config['market_name'] = 'global_forex'
        
        # 外汇特有的配置
        config['trading_rules']['commission']['forex'] = {
            'open_commission': 0.0002,  # 外汇佣金更低
            'close_commission': 0.0002,
            'open_tax': 0.0,
            'close_tax': 0.0,
            'min_commission': 0.0
        }
        
        super().__init__(config)
        
        # 外汇特有属性
        self.market_name = 'global_forex'
        
        logger.info(f"全球外汇市场适配器初始化完成，支持频率: {self.supported_frequencies}")
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取外汇市场默认配置"""
        config = super()._get_default_config()
        config['market_name'] = 'global_forex'
        return config


# 向后兼容的别名
ForexAdapter = GlobalForexMarketAdapter

