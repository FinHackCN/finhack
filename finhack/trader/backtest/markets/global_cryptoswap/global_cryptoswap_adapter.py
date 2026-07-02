"""
全球加密货币永续合约(Swap)市场适配器

基于现货适配器，增加保证金/杠杆/做空支持
"""

from typing import List, Dict, Any
from datetime import datetime, date, time
import logging

from ..global_cryptospot.global_cryptospot_adapter import GlobalCryptoSpotMarketAdapter
from ...events.event_types import BaseEvent, MarketEvent, EventTypeEnum

logger = logging.getLogger(__name__)


class GlobalCryptoSwapMarketAdapter(GlobalCryptoSpotMarketAdapter):
    """全球加密货币永续合约市场适配器

    与现货的区别：
    - 支持保证金交易（杠杆）
    - 支持做空（SHORT_OPEN / SHORT_CLOSE）
    - 无合约乘数（1:1名义价值）
    - 资金费率（每8小时结算）
    """

    def __init__(self, config: Dict[str, Any] = None):
        if not config:
            config = self._get_default_config()

        # 传入 'global_cryptospot' 让父类初始化通过，然后覆写
        super().__init__(config)

        # 覆写市场名称
        self.market_name = 'global_cryptoswap'

        # 从配置或交易规则版本获取保证金/杠杆参数
        self._init_margin_config(config)

        # 合约特有配置
        self.margin_enabled = True
        self.short_sell_enabled = True

        logger.info(f"加密货币永续合约适配器初始化完成，保证金比例: {self.margin_ratio}, 杠杆: {self.leverage}x")

    def _init_margin_config(self, config: Dict[str, Any]):
        """从 crypto_trading_rules_versions 初始化保证金配置"""
        try:
            from ..global_cryptospot.crypto_trading_rules_versions import LEVERAGE_VERSIONS
            from ..global_cryptospot.crypto_trading_rules_versions import RuleVersion

            # 查询适用于当前日期的规则（使用回测日期而非系统日期）
            today = self._current_date
            rule = RuleVersion.get_applicable_rule(LEVERAGE_VERSIONS, 'all', today, 'binance')
            if rule:
                self.margin_ratio = config.get('margin_ratio', rule.get('initial_margin_ratio', 0.10))
                self.leverage = config.get('leverage', rule.get('default_leverage', 10))
            else:
                self.margin_ratio = config.get('margin_ratio', 0.10)
                self.leverage = config.get('leverage', 10)
        except Exception:
            self.margin_ratio = config.get('margin_ratio', 0.10)
            self.leverage = config.get('leverage', 10)

    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        config = super()._get_default_config()

        # 合约手续费（通常比现货低）
        config['trading_rules']['commission']['crypto'] = {
            'open_commission': 0.0004,   # 0.04% maker
            'close_commission': 0.0004,  # 0.04% taker
            'open_tax': 0.0,
            'close_tax': 0.0,
            'min_commission': 0.0
        }

        # 保证金和杠杆配置
        config['margin_enabled'] = True
        config['short_sell_enabled'] = True

        return config

    def get_margin_ratio(self, symbol: str = None) -> float:
        """获取保证金比例"""
        return self.margin_ratio

    def get_leverage_info(self, symbol: str = None) -> Dict[str, Any]:
        """获取杠杆信息"""
        return {
            'leverage': self.leverage,
            'max_leverage': 125,
            'margin_ratio': self.margin_ratio,
            'maint_margin_ratio': self.margin_ratio * 0.5,
        }

    def get_contract_size(self, symbol: str) -> float:
        """获取合约乘数（永续合约为1）"""
        return 1.0

    def validate_order(self, symbol: str, volume: float, price: float,
                       side: str) -> tuple:
        """验证订单（支持做空方向）"""
        is_valid, error_msg = super().validate_order(symbol, volume, price, side)
        if not is_valid:
            return is_valid, error_msg

        # 允许 SHORT_OPEN / SHORT_CLOSE 方向
        if side in ('SHORT_OPEN', 'SHORT_CLOSE', 'short_open', 'short_close'):
            return True, ""

        return True, ""

    def _generate_daily_events_1d(self, trade_date) -> List[BaseEvent]:
        """生成1d频率事件序列（增加资金费率结算和保证金检查事件）

        在父类事件序列基础上，每8小时插入资金费率结算 + 保证金检查事件对。
        资金费率结算先于保证金检查执行，确保保证金检查反映扣除后的真实权益。
        """
        events = super()._generate_daily_events_1d(trade_date)

        # 资金费率结算时间（每8小时，与主流交易所对齐）
        settlement_times = [
            time(8, 0),    # 08:00 结算
            time(16, 0),   # 16:00 结算
            time(0, 0),    # 00:00 结算
        ]

        funding_events = []
        for t in settlement_times:
            # 先资金费率结算
            funding_events.append(MarketEvent(
                event_type=EventTypeEnum.FUNDING_RATE_SETTLE,
                event_time=datetime.combine(trade_date, t),
                market='global_cryptoswap',
                frequency='1d',
                event_description="资金费率结算"
            ))
            # 再保证金检查（在funding扣除后检查真实权益）
            funding_events.append(MarketEvent(
                event_type=EventTypeEnum.MARGIN_CALL_CHECK,
                event_time=datetime.combine(trade_date, t),
                market='global_cryptoswap',
                frequency='1d',
                event_description="保证金检查"
            ))

        # 合并并按时间排序
        events.extend(funding_events)
        events.sort(key=lambda x: x.event_time)

        return events

    def _generate_daily_events_min(self, trade_date: date, schedule: Dict[str, time], frequency: str) -> List[BaseEvent]:
        """生成分钟线频率事件序列（增加资金费率结算和保证金检查事件）

        与 _generate_daily_events_1d 对齐：每8小时插入 资金费率结算 + 保证金检查 事件对。
        资金费率结算先于保证金检查执行，确保保证金检查反映扣除后的真实权益。

        修复：此前 1m/分钟频率回落到基类 GlobalCryptoSpotMarketAdapter._generate_daily_events_min
        （仅委托 BaseMinutelyEventGenerator，不含资金费事件），导致永续合约分钟级回测不结算
        资金费率（实测 crypto_swap 1m 结算次数=0，而 1d 有 2373 次、累计 -30927 USDT）。
        此处显式补齐，使分钟级永续回测与日线一样计入资金费成本。
        """
        events = super()._generate_daily_events_min(trade_date, schedule, frequency)

        # 资金费率结算时间（每8小时，与主流交易所对齐）
        settlement_times = [
            time(8, 0),    # 08:00 结算
            time(16, 0),   # 16:00 结算
            time(0, 0),    # 00:00 结算
        ]

        funding_events = []
        for t in settlement_times:
            # 先资金费率结算
            funding_events.append(MarketEvent(
                event_type=EventTypeEnum.FUNDING_RATE_SETTLE,
                event_time=datetime.combine(trade_date, t),
                market='global_cryptoswap',
                frequency=frequency,
                event_description="资金费率结算"
            ))
            # 再保证金检查（在funding扣除后检查真实权益）
            funding_events.append(MarketEvent(
                event_type=EventTypeEnum.MARGIN_CALL_CHECK,
                event_time=datetime.combine(trade_date, t),
                market='global_cryptoswap',
                frequency=frequency,
                event_description="保证金检查"
            ))

        # 合并并按时间排序
        events.extend(funding_events)
        events.sort(key=lambda x: x.event_time)

        return events
