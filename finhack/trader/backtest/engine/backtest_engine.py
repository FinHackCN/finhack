"""
主回测引擎

协调EventCenter、TradeCenter、DataCenter、StrategyExecutor四大组件
实现完整的回测流程
"""

import asyncio
import os
import pickle
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np

import finhack.library.log as Log

from ..constants import (
    SHORT_POSITION_SUFFIX,
    ANNUAL_TRADING_DAYS,
    DEFAULT_INITIAL_CASH,
    DEFAULT_RISK_FREE_RATE,
    MIN_ORDER_QUANTITY,
    ABNORMAL_DAILY_RETURN_THRESHOLD,
    DEFAULT_CRYPTO_MARGIN_RATIO,
    POSITION_DUST_THRESHOLD,
    ZOMBIE_ORDER_THRESHOLD,
)


# ========== 性能优化工具类 ==========

class TimeFormatter:
    """
    时间字符串格式化缓存器 - 性能优化

    避免重复调用 strftime，缓存格式化结果
    同一分钟的多个事件共享同一个格式化字符串
    """
    _cache: Dict[tuple, str] = {}
    _cache_date: Optional[date] = None

    @classmethod
    def format(cls, dt: datetime) -> str:
        """格式化时间为字符串，使用缓存加速"""
        if dt is None:
            return '--:--:--'

        current_date = dt.date()

        # 新的一天，清空缓存
        if cls._cache_date != current_date:
            cls._cache.clear()
            cls._cache_date = current_date

        # 以 (hour, minute) 为键缓存
        minute_key = (dt.hour, dt.minute)
        if minute_key not in cls._cache:
            cls._cache[minute_key] = dt.strftime('%Y-%m-%d %H:%M:%S')

        return cls._cache[minute_key]

    @classmethod
    def format_range(cls, start: datetime, end: datetime) -> tuple:
        """批量格式化开始和结束时间"""
        return cls.format(start), cls.format(end)

    @classmethod
    def clear(cls):
        """清空缓存（用于测试）"""
        cls._cache.clear()
        cls._cache_date = None


def _match_quote_symbols(quote_df: pd.DataFrame, symbols_list: List[str]) -> Dict[str, Dict]:
    """从quote_df中提取市场数据，支持期货多后缀匹配

    Args:
        quote_df: get_quotes返回的DataFrame，index为code
        symbols_list: 需要查找的代码列表

    Returns:
        Dict[symbol, {field: value}]
    """
    market_data = {}
    # 构建base code映射
    index_base_map = {}
    for idx in quote_df.index:
        base = idx.split('.')[0] if '.' in str(idx) else str(idx)
        index_base_map[base] = idx
    for symbol in symbols_list:
        row = None
        if symbol in quote_df.index:
            row = quote_df.loc[symbol]
        else:
            base = symbol.split('.')[0] if '.' in symbol else symbol
            if base in index_base_map:
                row = quote_df.loc[index_base_map[base]]
        if row is not None:
            # 处理MultiIndex情况：loc可能返回DataFrame
            if isinstance(row, pd.DataFrame):
                row = row.iloc[-1]
            result = row.to_dict()
            # 确保所有值都是标量，而非Series
            market_data[symbol] = {
                k: float(v.iloc[-1]) if isinstance(v, pd.Series) else v
                for k, v in result.items()
            }
    return market_data


def _safe_float(val, default=0.0):
    """安全转换为float，处理pd.NA/NaN等异常值"""
    try:
        if val is pd.NA or pd.isna(val):
            return default
        return float(val)
    except (TypeError, ValueError):
        return default

def extract_latest_prices_batch(klines_df: pd.DataFrame, symbols: List[str]) -> Dict[str, Dict]:
    """
    批量提取最新价格 - 性能优化

    使用 groupby 代替循环查询 MultiIndex，性能提升 5-10 倍

    Args:
        klines_df: K线数据，MultiIndex(time, code)
        symbols: 需要提取的股票代码列表

    Returns:
        Dict[symbol, {'close': price, 'volume': vol}]
    """
    if klines_df.empty:
        return {}

    result = {}

    try:
        # 方法1: 使用 groupby 批量获取每个股票的最后一行（最快）
        # groupby().last() 一次性获取所有股票的最新数据
        latest = klines_df.groupby(level='code').last()

        has_volume = 'volume' in latest.columns

        # 【期货多后缀支持】构建base code到查询symbol的映射
        # 处理如 IF2401 vs IF2401.CCFX vs IF2401.CFFEX 等不同格式
        symbol_base_map = {}  # base_code -> query_symbol
        for s in symbols:
            base = s.split('.')[0] if '.' in s else s
            symbol_base_map[base] = s

        # 构建 DataFrame 索引中 base code 到 index key 的映射
        index_base_map = {}  # base_code -> actual_index_key
        for idx_key in latest.index:
            base = idx_key.split('.')[0] if '.' in str(idx_key) else str(idx_key)
            index_base_map[base] = idx_key

        for symbol in symbols:
            try:
                # 先精确匹配
                if symbol in latest.index:
                    row = latest.loc[symbol]
                    result[symbol] = {
                        'close': _safe_float(row['close']),
                        'volume': _safe_float(row['volume']) if has_volume else 0
                    }
                else:
                    # 模糊匹配：通过base code
                    base = symbol.split('.')[0] if '.' in symbol else symbol
                    if base in index_base_map:
                        idx_key = index_base_map[base]
                        row = latest.loc[idx_key]
                        result[symbol] = {
                            'close': _safe_float(row['close']),
                            'volume': _safe_float(row['volume']) if has_volume else 0
                        }
            except (KeyError, IndexError):
                continue

    except Exception as e:
        # 回退方案: 使用 xs 逐个查询（仍然比 get_level_values 快）
        Log.logger.debug(f"[extract_latest_prices_batch] groupby失败，回退到xs方式: {e}")
        for symbol in symbols:
            try:
                symbol_data = klines_df.xs(symbol, level='code')
                if not symbol_data.empty:
                    result[symbol] = {
                        'close': _safe_float(symbol_data['close'].iloc[-1]),
                        'volume': _safe_float(symbol_data['volume'].iloc[-1]) if 'volume' in symbol_data.columns else 0
                    }
            except (KeyError, IndexError):
                continue

    return result
from ..events.event_types import EventTypeEnum, BaseEvent
from ..models.enums import *
from ..models.account import Account
from ..models.position import Position
from ..models.order import Order
from ..models.trade import Trade
from ..core.context import Context


class TradeCenter:
    """交易中心 - 模拟交易所功能"""

    def __init__(self, context: Dict, market_adapter=None, data_center=None):
        self.context = context
        self.market_adapter = market_adapter
        self.data_center = data_center  # 数据中心引用

        # 事件中心（由BacktestEngine注入）
        self.event_center = None

        # 初始化账户
        self.account = Account.from_dict(context['account'])
        self.positions = {}  # symbol -> Position
        self.orders = {}     # order_id -> Order (所有订单历史)
        self.active_orders = {}  # order_id -> Order (仅活跃订单，用于撮合)
        self.trades = []     # List[Trade]

        # 订单ID计数器
        self.order_id_counter = 1
        self.trade_id_counter = 1

        # ========== 部分成交比例配置 ==========
        # 从配置文件获取，如果没有则使用默认值
        settings = context.get('settings', {})
        partial_fill_config = settings.get('partial_fill_ratio', {
            'large_order_threshold': 10000,    # 大订单阈值（股）
            'large_order_min': 0.3,             # 大订单最小成交比例
            'large_order_max': 0.5,             # 大订单最大成交比例
            'medium_order_threshold': 5000,    # 中订单阈值（股）
            'medium_order_min': 0.5,            # 中订单最小成交比例
            'medium_order_max': 0.7,            # 中订单最大成交比例
            'small_order_min': 0.7,             # 小订单最小成交比例
            'small_order_max': 1.0,             # 小订单最大成交比例
        })
        # 兼容旧格式：如果配置是单个数字，则作为统一比例
        if isinstance(partial_fill_config, (int, float)):
            ratio = partial_fill_config
            partial_fill_config = {
                'large_order_threshold': 10000,
                'large_order_min': ratio,
                'large_order_max': ratio,
                'medium_order_threshold': 5000,
                'medium_order_min': ratio,
                'medium_order_max': ratio,
                'small_order_min': ratio,
                'small_order_max': ratio,
            }
        self.partial_fill_config = partial_fill_config

        # 防重复处理：已处理的公司行为记录
        # 格式: {(symbol, ex_date, action_type): True}
        self._processed_corporate_actions: set = set()

    # ========== 市场规则委托方法 ==========

    def _get_lot_size(self, symbol: str) -> float:
        """获取最小交易单位（委托给market_adapter）"""
        if self.market_adapter and hasattr(self.market_adapter, 'get_lot_size'):
            try:
                return float(self.market_adapter.get_lot_size(symbol))
            except Exception:
                pass
        # 默认值：根据市场类型返回
        market = self.context.get('settings', {}).get('market', 'cn_stock')
        return MIN_ORDER_QUANTITY.get(market, 100.0)

    def _get_min_fill_volume(self, symbol: str) -> float:
        """获取最小成交量"""
        return self._get_lot_size(symbol)

    def _normalize_volume(self, volume: float, symbol: str) -> float:
        """标准化交易数量（按市场规则取整）"""
        lot_size = self._get_lot_size(symbol)
        if lot_size <= 0:
            return volume  # 支持小数（如加密货币）
        return (volume // lot_size) * lot_size

    def _is_t_plus_one(self, symbol: str = None) -> bool:
        """判断当前市场是否T+1

        Args:
            symbol: 证券代码（可选），传入时按具体产品类型判断T+0/T+1
        """
        if self.market_adapter:
            # 优先使用按代码动态判断的方法
            if symbol and hasattr(self.market_adapter, 'is_t_plus_one'):
                return self.market_adapter.is_t_plus_one(symbol)
            if hasattr(self.market_adapter, 't_plus_one'):
                return self.market_adapter.t_plus_one
            if hasattr(self.market_adapter, 'is_t_plus_zero_allowed'):
                return not self.market_adapter.is_t_plus_zero_allowed('')
        market = self.context.get('settings', {}).get('market', 'cn_stock')
        return market == 'cn_stock'

    def _is_cn_future(self):
        """检查当前市场是否为期货（有交割）。优先问 adapter。"""
        if self.market_adapter:
            try:
                return self.market_adapter.has_delivery()
            except Exception:
                pass
        market = self.context.get('settings', {}).get('market', '')
        return market == 'cn_future'

    def _is_crypto_swap(self):
        """检查当前市场是否为加密永续（有资金费率）。优先问 adapter。"""
        if self.market_adapter:
            try:
                return self.market_adapter.has_funding_rate()
            except Exception:
                pass
        market = self.context.get('settings', {}).get('market', '')
        return market == 'global_cryptoswap'

    def _uses_margin(self):
        """检查当前市场是否使用保证金模式（衍生品）。优先问 adapter。"""
        if self.market_adapter:
            try:
                return self.market_adapter.is_derivatives()
            except Exception:
                pass
        return self._is_cn_future() or self._is_crypto_swap()

    def _get_contract_size(self, symbol):
        """获取合约乘数（统一走 adapter；非衍生品 adapter 返回 1.0）。"""
        if self.market_adapter:
            try:
                return self.market_adapter.get_contract_size(symbol)
            except Exception as e:
                Log.logger.warning(f"[乘数] {symbol} 获取合约乘数失败: {e}")
        return 1

    def _get_margin_ratio(self, symbol):
        """获取保证金比例（统一走 adapter；非衍生品 adapter 返回 1.0）。"""
        if self.market_adapter:
            try:
                return self.market_adapter.get_margin_ratio(symbol)
            except Exception:
                pass
        return 1.0

    def _get_futures_contract_value(self, symbol, volume, price):
        """计算合约价值（含乘数）"""
        contract_size = self._get_contract_size(symbol)
        return volume * price * contract_size

    def _get_futures_margin(self, symbol, volume, price):
        """计算保证金"""
        contract_value = self._get_futures_contract_value(symbol, volume, price)
        margin_ratio = self._get_margin_ratio(symbol)
        return contract_value * margin_ratio

    def set_event_center(self, event_center):
        """设置事件中心

        Args:
            event_center: 事件中心实例
        """
        self.event_center = event_center

    def validate_order_sync(self, order: Order) -> bool:
        """同步版本的订单验证（供place_order_sync调用）

        包含：资金充足性、交割月检查、持仓充足性、手数/市场规则验证
        """
        if order.volume <= 0:
            return False

        if order.order_type == OrderType.LIMIT and (not order.price or order.price <= 0):
            return False

        # 退市/停牌状态检查
        if hasattr(self, 'data_center') and self.data_center:
            if hasattr(self.data_center, 'is_tradable') and not self.data_center.is_tradable(order.symbol):
                status_info = self.data_center.get_trading_status(order.symbol) if hasattr(self.data_center, 'get_trading_status') else {}
                Log.logger.warning(
                    f"订单被拒绝: {order.symbol} 不可交易, 状态: {status_info.get('status', 'unknown')}, "
                    f"原因: {status_info.get('reason', 'unknown')}"
                )
                return False

        # 期货合约到期检查：拒绝交割月合约的新订单
        if self._is_cn_future() and self.market_adapter and hasattr(self.market_adapter, 'get_delivery_info'):
            try:
                delivery_info = self.market_adapter.get_delivery_info(order.symbol)
                natural_person_ban = delivery_info.get('natural_person_ban', {})
                if natural_person_ban.get('is_banned', False):
                    reason = natural_person_ban.get('reason', '合约已到期')
                    Log.logger.warning(f"订单被拒绝: {order.symbol} {reason}")
                    return False
            except Exception:
                pass

        # 手数验证（仅整数手数的市场，如股票/期货）
        lot_size = self._get_lot_size(order.symbol)
        if lot_size >= 1 and order.side in (Side.BUY, Side.SHORT_OPEN):
            from ..utils.float_validation import is_valid_volume
            if not is_valid_volume(order.volume, lot_size):
                Log.logger.warning(f"订单数量必须是{lot_size}的整数倍: {order.volume}")
                return False

        # 市场规则验证（限价单用委托价, 市价单用最新价, 都做价格规则校验: tick/涨跌停/价格笼子）
        if self.market_adapter and hasattr(self.market_adapter, 'validate_order'):
            try:
                if order.order_type == OrderType.LIMIT and order.price:
                    validation_price = order.price
                else:
                    # 市价单: 用最新价做规则校验(原实现传-1并被validation_price>0跳过, 导致市价单绕过所有价格规则)
                    validation_price = self._get_last_price(order.symbol)
                is_valid, msg = self.market_adapter.validate_order(
                    order.symbol, order.volume,
                    validation_price, order.side.value
                )
                if not is_valid and validation_price > 0:
                    Log.logger.warning(f"市场规则验证失败: {msg}")
                    return False
            except Exception:
                pass

        # 期货/永续合约买入/开空仓：检查保证金充足性并冻结资金
        if self._uses_margin() and order.side in (Side.BUY, Side.SHORT_OPEN):
            if order.order_type == OrderType.LIMIT and order.price:
                estimated_price = order.price
            else:
                estimated_price = self._get_last_price(order.symbol)
                if estimated_price <= 0:
                    return True

            estimated_amount = order.volume * estimated_price
            contract_value = self._get_futures_contract_value(order.symbol, order.volume, estimated_price)
            estimated_commission = self._calculate_commission(contract_value, order.side)
            estimated_tax = self._calculate_tax(estimated_amount, order.side)
            margin = self._get_futures_margin(order.symbol, order.volume, estimated_price)
            total_required = margin + estimated_commission + estimated_tax

            if not self.account.freeze_cash(total_required):
                action = "开空仓" if order.side == Side.SHORT_OPEN else ""
                Log.logger.warning(
                    f"资金不足，{action}订单被拒绝: {order.symbol} {order.side} {order.volume} "
                    f"需要{total_required:.2f}元，可用{self.account.cash_available:.2f}元"
                )
                return False
            order._frozen_amount = total_required

        # 非保证金市场（A股/基金等）：限价买单冻结资金
        # 防止挂多个限价单导致资金超额占用
        elif not self._uses_margin() and order.side == Side.BUY and order.order_type == OrderType.LIMIT:
            estimated_price = order.price if order.price else self._get_last_price(order.symbol)
            if estimated_price and estimated_price > 0:
                estimated_amount = order.volume * estimated_price
                estimated_commission = self._calculate_commission(estimated_amount, order.side, order.symbol)
                estimated_tax = self._calculate_tax(estimated_amount, order.side)
                total_required = estimated_amount + estimated_commission + estimated_tax

                if not self.account.freeze_cash(total_required):
                    Log.logger.warning(
                        f"资金不足，限价买单被拒绝: {order.symbol} BUY {order.volume} "
                        f"需要{total_required:.2f}元，可用{self.account.cash_available:.2f}元"
                    )
                    return False
                order._frozen_amount = total_required

        # 卖出：持仓充足性由place_order_sync中的T+1检查处理，此处不再重复验证
        # 但期货市场无多头持仓时，SELL会转为SHORT_OPEN，需要冻结保证金
        elif order.side == Side.SELL and self._uses_margin():
            position = self.positions.get(order.symbol)
            available_volume = position.available_volume if position else 0
            if available_volume < order.volume:
                # 可能转为开空仓，需要冻结保证金
                if order.order_type == OrderType.LIMIT and order.price:
                    estimated_price = order.price
                else:
                    estimated_price = self._get_last_price(order.symbol)
                    if estimated_price <= 0:
                        return True
                contract_value = self._get_futures_contract_value(order.symbol, order.volume, estimated_price)
                estimated_commission = self._calculate_commission(contract_value, Side.SHORT_OPEN)
                estimated_tax = self._calculate_tax(order.volume * estimated_price, Side.SHORT_OPEN)
                margin = self._get_futures_margin(order.symbol, order.volume, estimated_price)
                total_required = margin + estimated_commission + estimated_tax
                if not self.account.freeze_cash(total_required):
                    Log.logger.warning(
                        f"资金不足，卖出转开空仓订单被拒绝: {order.symbol} SELL {order.volume} "
                        f"需要保证金{total_required:.2f}元，可用{self.account.cash_available:.2f}元"
                    )
                    return False
                order._frozen_amount = total_required

        # 平空仓：检查空头持仓
        elif order.side == Side.SHORT_CLOSE:
            short_key = f"{order.symbol}{SHORT_POSITION_SUFFIX}"
            position = self.positions.get(short_key)
            available_volume = position.available_volume if position else 0
            if available_volume < order.volume:
                Log.logger.warning(
                    f"空头持仓不足，平空订单被拒绝: {order.symbol} 需要{order.volume}，可用{available_volume}"
                )
                return False

        return True

    def handle_order_submission(self, event):
        """处理订单提交事件 - 空操作（订单已在place_order_sync中直接处理）"""
        pass

    def handle_order_cancellation(self, event):
        """处理订单撤销事件 - 空操作（订单已在cancel_order_sync中直接处理）"""
        pass

    def get_account(self) -> Account:
        """获取账户信息"""
        return self.account

    async def get_positions(self, adapter_id: str, symbol: Optional[str] = None, refresh: bool = False) -> List[Position]:
        """获取持仓信息"""
        if symbol:
            return [self.positions[symbol]] if symbol in self.positions else []
        return list(self.positions.values())

    def get_all_positions(self) -> Dict[str, Position]:
        """获取所有持仓 - 同步版本

        Returns:
            Dict[str, Position]: 持仓字典 {symbol: Position}
        """
        return self.positions
        
    async def get_orders(self, adapter_id: str, symbol: Optional[str] = None, 
                        status: Optional[OrderStatus] = None, **kwargs) -> List[Order]:
        """获取订单信息"""
        orders = list(self.orders.values())
        
        if symbol:
            orders = [o for o in orders if o.symbol == symbol]
        if status:
            orders = [o for o in orders if o.status == status]
            
        return orders
        
    async def get_trades(self, adapter_id: str, symbol: Optional[str] = None, **kwargs) -> List[Trade]:
        """获取成交信息"""
        trades = self.trades.copy()
        
        if symbol:
            trades = [t for t in trades if t.symbol == symbol]
            
        return trades
        
    async def place_order(self, adapter_id: str, symbol: str, side: Side, 
                         order_type: OrderType, volume: float, price: Optional[float] = None,
                         **kwargs) -> str:
        """下单"""
        # 生成订单ID
        order_id = f"order_{self.order_id_counter:06d}"
        self.order_id_counter += 1
        
        # 创建订单对象
        order = Order(
            account_id=self.account.account_id,
            symbol=symbol,
            side=side,
            order_type=order_type,
            volume=volume,
            order_id=order_id,
            price=price,
            status=OrderStatus.PENDING_NEW,
            created_time=self.context.get('current_dt', datetime.now())
        )
        
        # 验证订单
        if not await self._validate_order(order):
            order.status = OrderStatus.REJECTED
            order.rejected_reason = "订单验证失败"
            Log.logger.warning(f"订单被拒绝: {order_id} - {order.rejected_reason}")
            return None
            
        # 添加到订单列表
        self.orders[order_id] = order
        self.active_orders[order_id] = order  # 同时添加到活跃订单
        order.status = OrderStatus.NEW

        # 发布订单提交事件
        if self.event_center:
            self.event_center.publish_order_event(
                event_type=EventTypeEnum.ORDER_SUBMISSION,
                order_data={
                    "order_id": order_id,
                    "symbol": symbol,
                    "side": side.value if hasattr(side, 'value') else str(side),
                    "volume": volume,
                    "price": price
                }
            )

        Log.logger.info(f"订单提交成功: {order_id} {symbol} {side} {volume}@{price}")
        return order_id
        
    async def cancel_order(self, adapter_id: str, order_id: str, **kwargs) -> bool:
        """撤单"""
        if order_id not in self.orders:
            Log.logger.warning(f"订单不存在: {order_id}")
            return False
            
        order = self.orders[order_id]
        if order.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]:
            Log.logger.warning(f"订单状态不允许撤销: {order_id} {order.status}")
            return False
            
        order.status = OrderStatus.CANCELLED
        order.updated_time = self.context.get('current_dt', datetime.now())

        # 发布订单撤销事件
        if self.event_center:
            self.event_center.publish_order_event(
                event_type=EventTypeEnum.ORDER_CANCELLATION,
                order_data={
                    "order_id": order_id,
                    "symbol": order.symbol
                }
            )

        # 从活跃订单中移除
        if order_id in self.active_orders:
            del self.active_orders[order_id]

        Log.logger.info(f"订单撤销成功: {order_id}")
        return True
        
    def try_match_orders(self, event):
        """尝试撮合订单（事件驱动接口）

        Args:
            event: TRY_MATCH 事件
        """
        # 如果事件包含 market_data，直接使用
        if hasattr(event, 'market_data') and event.market_data:
            self.try_match_orders_sync(event.market_data)
            return

        # 否则从 DataCenter 动态获取市场数据
        current_time = self.context.get('current_dt', datetime.now())
        freq = self.context.get('settings', {}).get('freq', '1d')

        # 获取所有需要行情的标的
        symbols = set(order.symbol for order in self.active_orders.values()
                     if order.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED])

        if not symbols:
            return

        # 获取市场数据
        market_data = {}
        # 优先使用 self.data_center，如果没有则尝试使用 event_center.data_center
        data_center = getattr(self, 'data_center', None) or (self.event_center.data_center if self.event_center and hasattr(self.event_center, 'data_center') else None)
        if freq == '1m' and data_center:
            from datetime import timedelta
            start_time = current_time - timedelta(minutes=5)
            end_time = current_time + timedelta(minutes=1)

            try:
                klines_df = data_center.get_klines(
                    codes=list(symbols),
                    freq=freq,
                    start_time=start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    end_time=end_time.strftime('%Y-%m-%d %H:%M:%S'),
                    fields=['close', 'volume']  # 【修复】获取成交量数据
                )

                if not klines_df.empty:
                    for symbol in symbols:
                        try:
                            symbol_klines = klines_df[klines_df.index.get_level_values('code') == symbol]
                            if not symbol_klines.empty:
                                market_data[symbol] = {
                                    'close': symbol_klines['close'].iloc[-1],
                                    'volume': symbol_klines['volume'].iloc[-1] if 'volume' in symbol_klines.columns else 0
                                }
                        except Exception:
                            pass
            except Exception:
                pass

        # 调用同步撮合逻辑
        if market_data:
            self.try_match_orders_sync(market_data)
            
    async def _execute_trade(self, order: Order, price: float, trade_time: datetime):
        """执行交易"""
        # 应用滑点
        slip_price = self._apply_slippage(price, order.side, order.symbol)
        
        # 计算手续费和税费
        trade_amount = order.volume * slip_price
        commission = self._calculate_commission(trade_amount, order.side)
        tax = self._calculate_tax(trade_amount, order.side)
        
        # 检查资金充足性
        if order.side == Side.BUY:
            required_cash = trade_amount + commission + tax
            if self.account.cash_available < required_cash:
                order.status = OrderStatus.REJECTED
                order.rejected_reason = "资金不足"
                Log.logger.warning(f"资金不足，订单被拒绝: {order.order_id}")
                return
                
        # 检查持仓充足性
        if order.side == Side.SELL:
            # 期货/合约市场无多头持仓时，SELL自动转为SHORT_OPEN（开空仓）
            market = self.context.get('settings', {}).get('market', '')
            if order.symbol not in self.positions:
                if self._uses_margin():
                    order.side = Side.SHORT_OPEN
                else:
                    order.status = OrderStatus.REJECTED
                    order.rejected_reason = "无持仓"
                    Log.logger.warning(f"无持仓，订单被拒绝: {order.order_id}")
                    return
            elif order.symbol in self.positions:
                position = self.positions[order.symbol]
                if position.available_volume < order.volume:
                    # 期货/合约市场：无多头持仓(total≈0)时转为开空仓
                    if self._uses_margin() and position.volume <= POSITION_DUST_THRESHOLD:
                        order.side = Side.SHORT_OPEN
                    # 有持仓但冻结中(available≈0)：拒绝
                    elif self._uses_margin() and position.volume > POSITION_DUST_THRESHOLD and position.available_volume <= POSITION_DUST_THRESHOLD:
                        order.status = OrderStatus.REJECTED
                        order.rejected_reason = "持仓冻结中，无法卖出"
                        Log.logger.warning(f"持仓冻结中，订单被拒绝: {order.order_id}")
                        return
                    # 期货/合约市场：有可用持仓但不足时，缩减至可用量（部分平仓）
                    elif self._uses_margin() and position.available_volume > POSITION_DUST_THRESHOLD:
                        original_volume = order.volume
                        order.volume = position.available_volume
                        Log.logger.info(
                            f"持仓不足，缩减卖出量: {order.symbol} "
                            f"申请{original_volume}手 → 实际{order.volume}手"
                        )
                    else:
                        order.status = OrderStatus.REJECTED
                        order.rejected_reason = "持仓不足"
                        Log.logger.warning(f"持仓不足，订单被拒绝: {order.order_id}")
                        return
                
        # 生成成交记录
        trade_id = f"trade_{self.trade_id_counter:06d}"
        self.trade_id_counter += 1
        
        trade = Trade(
            account_id=self.account.account_id,
            symbol=order.symbol,
            order_id=order.order_id,
            trade_id=trade_id,
            side=order.side,
            volume=order.volume,
            price=slip_price,
            trade_time=trade_time,
            amount=trade_amount,
            commission=commission,
            tax=tax
        )
        
        # 更新订单状态
        order.status = OrderStatus.FILLED
        order.filled_volume = order.volume
        order.filled_amount = trade_amount
        order.avg_fill_price = slip_price
        order.updated_time = trade_time
        
        # 更新账户和持仓
        await self._update_account_and_positions(trade)
        
        # 记录成交
        self.trades.append(trade)
        self.context['logs']['all_trades'].append(trade.to_dict())
        
        Log.logger.info(f"交易执行成功: {trade_id} {order.symbol} {order.side.value} {order.volume}@{slip_price:.4f}")
        
    async def _update_account_and_positions(self, trade: Trade):
        """更新账户和持仓"""
        if trade.side == Side.BUY:
            # 买入：减少现金，增加持仓
            self.account.cash_available -= (trade.amount + trade.commission + trade.tax)

            if trade.symbol not in self.positions:
                # 新建持仓
                self.positions[trade.symbol] = Position(
                    account_id=self.account.account_id,
                    symbol=trade.symbol,
                    position_side=PositionSide.LONG,
                    volume=trade.volume,
                    available_volume=trade.volume,
                    cost_price=trade.price,
                    market_value=trade.volume * trade.price,
                    open_time=trade.trade_time
                )
            else:
                # 增加持仓
                position = self.positions[trade.symbol]
                old_cost = position.volume * position.cost_price
                new_cost = trade.volume * trade.price + trade.commission + trade.tax
                position.volume += trade.volume
                position.available_volume += trade.volume
                position.cost_price = (old_cost + new_cost) / position.volume
                position.market_value = position.volume * trade.price

        elif trade.side == Side.SELL:
            # 卖出：增加现金，减少持仓
            self.account.cash_available += (trade.amount - trade.commission - trade.tax)

            if trade.symbol in self.positions:
                position = self.positions[trade.symbol]
                position.volume -= trade.volume
                position.available_volume -= trade.volume
                position.market_value = position.volume * trade.price

                # 计算已实现盈亏
                realized_pnl = (trade.price - position.cost_price) * trade.volume - trade.commission - trade.tax
                self.account.pnl_realized += realized_pnl

                # 如果持仓清零，删除持仓记录（浮点精度容差）
                if position.volume <= POSITION_DUST_THRESHOLD:
                    position.volume = 0
                    del self.positions[trade.symbol]

        elif trade.side == Side.SHORT_OPEN:
            # 开空仓：减少现金（衍生品扣保证金，现货扣全款）—— 统一走 adapter
            if self.market_adapter and self.market_adapter.get_short_open_cash_model() == 'margin_only':
                margin_ratio = self._get_margin_ratio(trade.symbol)
                margin = trade.amount * margin_ratio
                self.account.cash_available -= (margin + trade.commission + trade.tax)
            else:
                self.account.cash_available -= (trade.amount + trade.commission + trade.tax)

            # 生成空头持仓的唯一key
            short_key = f"{trade.symbol}{SHORT_POSITION_SUFFIX}"

            if short_key not in self.positions:
                # 新建空头持仓
                self.positions[short_key] = Position(
                    account_id=self.account.account_id,
                    symbol=trade.symbol,
                    position_side=PositionSide.SHORT,
                    volume=trade.volume,
                    available_volume=trade.volume,
                    cost_price=trade.price,
                    market_value=trade.volume * trade.price,
                    open_time=trade.trade_time
                )
            else:
                # 增加空头持仓
                position = self.positions[short_key]
                old_cost = position.volume * position.cost_price
                new_cost = trade.volume * trade.price
                position.volume += trade.volume
                position.available_volume += trade.volume
                position.cost_price = (old_cost + new_cost) / position.volume
                position.market_value = position.volume * trade.price

        elif trade.side == Side.SHORT_CLOSE:
            # 平空仓：增加现金，减少空头持仓
            self.account.cash_available += (trade.amount - trade.commission - trade.tax)

            short_key = f"{trade.symbol}{SHORT_POSITION_SUFFIX}"

            if short_key in self.positions:
                position = self.positions[short_key]
                position.volume -= trade.volume
                position.available_volume -= trade.volume
                position.market_value = position.volume * trade.price

                # 计算已实现盈亏（空头：价格下跌盈利）
                realized_pnl = (position.cost_price - trade.price) * trade.volume - trade.commission - trade.tax
                self.account.pnl_realized += realized_pnl

                # 如果持仓清零，删除持仓记录（浮点精度容差）
                if position.volume <= POSITION_DUST_THRESHOLD:
                    position.volume = 0
                    del self.positions[short_key]

        # 更新账户总资产
        self._update_account_value()
        
    def _update_account_value(self):
        """更新账户总资产"""
        # 记录更新前的总资产（用于异常检测）
        prev_total_assets = getattr(self.account, 'total_assets', 0)

        # 保证金市场：基于当前持仓和最新价格实时重算保证金占用
        # 确保margin_used始终反映最新的保证金水平，避免日内/日终差异
        if self._uses_margin():
            total_margin = 0.0
            for pos in self.positions.values():
                if pos.volume > 0:
                    # 优先使用最新市价，若不可用则回退到成本价（如合约停牌/交割日）
                    price = pos.last_price if pos.last_price > 0 else pos.cost_price
                    if price > 0:
                        margin_ratio = self._get_margin_ratio(pos.symbol)
                        contract_size = getattr(pos, 'contract_multiplier', 1)
                        total_margin += pos.volume * price * contract_size * margin_ratio
            self.account.margin_used = total_margin

        # 计算持仓市值（区分市场和方向）
        positions_value = 0.0
        if self._uses_margin():
            # 期货/永续合约：所有持仓只计入浮动盈亏
            # 原因：开仓时只扣保证金，未扣全额合约价值
            # 总资产 = 可用现金 + 冻结资金 + 保证金 + 浮动盈亏
            for pos in self.positions.values():
                positions_value += pos.unrealized_pnl
            total_assets = (self.account.cash_available
                            + self.account.cash_frozen
                            + self.account.margin_used
                            + positions_value)
        else:
            # 非期货：多头计入市值（现金已扣全额），空头计入浮动盈亏
            for pos in self.positions.values():
                if hasattr(pos, 'position_side') and pos.position_side == PositionSide.SHORT:
                    positions_value += pos.unrealized_pnl
                else:
                    positions_value += pos.market_value
            total_assets = self.account.cash_available + self.account.cash_frozen + positions_value

        # 打印持仓市值
        Log.logger.debug(f"持仓市值计算: {positions_value:.2f}")

        # 总资产为负数：穿仓场景下合法，保留真实负值
        initial_capital = self.context.get('settings', {}).get('initial_capital', DEFAULT_INITIAL_CASH)
        if total_assets < 0:
            Log.logger.warning(f"总资产为负数({total_assets:.2f})，穿仓亏损"
                             f"现金={self.account.cash_available:.2f}, "
                             f"冻结={self.account.cash_frozen:.2f}, "
                             f"保证金={getattr(self.account, 'margin_used', 0):.2f}, "
                             f"持仓盈亏={positions_value:.2f}")
        elif total_assets < initial_capital * 0.01:  # 小于初始资金的1%
            Log.logger.warning(f"总资产异常低({total_assets:.2f})，请检查！")

        # 异常变化检测：单日总资产变化超过30%时输出详细告警
        if prev_total_assets > 0:
            daily_change_ratio = (total_assets - prev_total_assets) / prev_total_assets
            if abs(daily_change_ratio) > 0.3:
                Log.logger.warning(
                    f"[异常波动] 总资产单日变化 {daily_change_ratio:.2%} "
                    f"({prev_total_assets:.2f} → {total_assets:.2f})，"
                    f"现金={self.account.cash_available:.2f}, "
                    f"冻结={self.account.cash_frozen:.2f}, "
                    f"持仓市值={positions_value:.2f}"
                )
                # 打印每只持仓的详细信息以便排查
                for pos_key, pos in self.positions.items():
                    Log.logger.warning(
                        f"  持仓 {pos_key}: volume={pos.volume}, "
                        f"cost_price={pos.cost_price:.4f}, "
                        f"last_price={getattr(pos, 'last_price', 'N/A')}, "
                        f"market_value={pos.market_value:.2f}"
                    )

        # 更新账户
        self.account.market_value = positions_value
        self.account.total_assets = total_assets
        self.account.timestamp_updated = self.context.get('current_dt', datetime.now())

        # 计算未实现盈亏
        self.account.pnl_unrealized = sum(pos.unrealized_pnl for pos in self.positions.values())

        # 打印更新后的账户信息
        Log.logger.debug(f"账户更新: 总资产={self.account.total_assets:.2f}, 持仓市值={self.account.market_value:.2f}")
        
    async def _validate_order(self, order: Order) -> bool:
        """验证订单"""
        # 基本验证
        if order.volume <= 0:
            return False

        if order.order_type == OrderType.LIMIT and (not order.price or order.price <= 0):
            return False

        # 期货合约到期检查：拒绝已过期合约的新订单
        if self._is_cn_future() and self.market_adapter and hasattr(self.market_adapter, 'get_delivery_info'):
            try:
                delivery_info = self.market_adapter.get_delivery_info(order.symbol)
                natural_person_ban = delivery_info.get('natural_person_ban', {})
                if natural_person_ban.get('is_banned', False):
                    reason = natural_person_ban.get('reason', '合约已到期')
                    Log.logger.warning(f"订单被拒绝: {order.symbol} {reason}")
                    return False
            except Exception:
                pass  # 无法获取交割信息时放行，由其他验证逻辑处理

        # 市场规则验证 - 委托给market_adapter
        lot_size = self._get_lot_size(order.symbol)
        if lot_size > 0 and order.side in (Side.BUY, Side.SHORT_OPEN):
            # 买入/开空时检查手数（卖出/平空允许零股）
            from ..utils.float_validation import is_valid_volume
            if not is_valid_volume(order.volume, lot_size):
                Log.logger.warning(f"订单数量必须是{lot_size}的整数倍: {order.volume}")
                return False
        # 如果adapter有validate_order方法，也调用它
        if self.market_adapter and hasattr(self.market_adapter, 'validate_order'):
            try:
                is_valid, msg = self.market_adapter.validate_order(
                    order.symbol, order.volume,
                    order.price or 0, order.side.value
                )
                if not is_valid:
                    Log.logger.warning(f"市场规则验证失败: {msg}")
                    return False
            except Exception:
                pass

        # 买入订单：检查资金是否充足
        if order.side == Side.BUY:
            # 获取成交价格（限价单使用订单价格，市价单使用最新价格）
            if order.order_type == OrderType.LIMIT and order.price:
                estimated_price = order.price
            else:
                # 市价单：尝试获取最新价格
                estimated_price = self._get_last_price(order.symbol)
                if estimated_price <= 0:
                    Log.logger.warning(f"无法获取{order.symbol}的最新价格，无法验证资金")
                    return False

            # 计算预估成交金额（含手续费）
            estimated_amount = order.volume * estimated_price
            if self._is_cn_future() or self._is_crypto_swap():
                # 期货/永续合约：按合约价值计算手续费，按保证金检查资金
                contract_value = self._get_futures_contract_value(order.symbol, order.volume, estimated_price)
                estimated_commission = self._calculate_commission(contract_value, Side.BUY)
                estimated_tax = self._calculate_tax(estimated_amount, Side.BUY)
                margin = self._get_futures_margin(order.symbol, order.volume, estimated_price)
                total_required = margin + estimated_commission + estimated_tax
            else:
                estimated_commission = self._calculate_commission(estimated_amount, Side.BUY)
                estimated_tax = self._calculate_tax(estimated_amount, Side.BUY)
                total_required = estimated_amount + estimated_commission + estimated_tax

            # 检查可用资金
            if self.account.cash_available < total_required:
                Log.logger.warning(
                    f"资金不足，订单被拒绝: {order.symbol} {order.side} {order.volume}股 "
                    f"需要{total_required:.2f}元，可用{self.account.cash_available:.2f}元"
                )
                return False

        # 卖出订单：检查持仓是否充足
        elif order.side == Side.SELL:
            position = self.positions.get(order.symbol)
            available_volume = position.available_volume if position else 0
            if available_volume < order.volume:
                # 期货市场：无多头持仓时允许SELL，后续撮合时转为SHORT_OPEN
                market = self.context.get('settings', {}).get('market', '')
                if self._uses_margin() and available_volume == 0:
                    pass  # 允许通过，撮合时会转为SHORT_OPEN
                else:
                    Log.logger.warning(
                        f"持仓不足，订单被拒绝: {order.symbol} 需要{order.volume}股，可用{available_volume}股"
                    )
                    return False

        # 开空仓订单：检查资金是否充足（作为保证金）
        elif order.side == Side.SHORT_OPEN:
            # 获取成交价格
            if order.order_type == OrderType.LIMIT and order.price:
                estimated_price = order.price
            else:
                estimated_price = self._get_last_price(order.symbol)
                if estimated_price <= 0:
                    Log.logger.warning(f"无法获取{order.symbol}的最新价格，无法验证资金")
                    return False

            # 计算预估成交金额（含手续费）
            # 开空仓需要冻结资金作为保证金
            estimated_amount = order.volume * estimated_price
            if self._is_cn_future() or self._is_crypto_swap():
                # 期货/永续合约：按合约价值计算手续费，按保证金检查资金
                contract_value = self._get_futures_contract_value(order.symbol, order.volume, estimated_price)
                estimated_commission = self._calculate_commission(contract_value, Side.SHORT_OPEN)
                estimated_tax = self._calculate_tax(estimated_amount, Side.SHORT_OPEN)
                margin = self._get_futures_margin(order.symbol, order.volume, estimated_price)
                total_required = margin + estimated_commission + estimated_tax
            else:
                estimated_commission = self._calculate_commission(estimated_amount, Side.SHORT_OPEN)
                estimated_tax = self._calculate_tax(estimated_amount, Side.SHORT_OPEN)
                total_required = estimated_amount + estimated_commission + estimated_tax

            # 检查可用资金
            if self.account.cash_available < total_required:
                Log.logger.warning(
                    f"资金不足，开空仓订单被拒绝: {order.symbol} 需要{total_required:.2f}元，可用{self.account.cash_available:.2f}元"
                )
                return False

        # 平空仓订单：检查空头持仓是否充足
        elif order.side == Side.SHORT_CLOSE:
            short_key = f"{order.symbol}{SHORT_POSITION_SUFFIX}"
            position = self.positions.get(short_key)
            available_volume = position.available_volume if position else 0
            if available_volume < order.volume:
                Log.logger.warning(
                    f"空头持仓不足，平空订单被拒绝: {order.symbol} 需要{order.volume}，可用{available_volume}"
                )
                return False

        return True
        
    def _apply_slippage(self, price: float, side: Side, symbol: str = None) -> float:
        """应用滑点，并对期货价格做tick_size取整"""
        import math as _math
        slip_type = self.context['settings'].get('slip_type', 'pricerelated')
        slip_value = self.context['settings'].get('slip_value', 0.001)

        if slip_type == 'pricerelated':
            if side in (Side.BUY, Side.SHORT_CLOSE):  # 买入和平空仓价格偏高
                result = price * (1 + slip_value)
            else:  # SELL, SHORT_OPEN 价格偏低
                result = price * (1 - slip_value)
        elif slip_type == 'fixed':
            if side in (Side.BUY, Side.SHORT_CLOSE):
                result = price + slip_value
            else:
                result = price - slip_value
        else:
            return price

        # 期货价格按tick_size取整（不利方向取整）
        if symbol:
            tick_size = self._get_tick_size(symbol)
            if tick_size and tick_size > 0:
                if side in (Side.BUY, Side.SHORT_CLOSE):
                    # 买入方向：向上取整（多付）
                    result = _math.ceil(result / tick_size) * tick_size
                else:
                    # 卖出方向：向下取整（少收）
                    result = _math.floor(result / tick_size) * tick_size

        return result

    def _get_tick_size(self, symbol: str) -> float:
        """获取合约的最小变动价位

        仅期货市场有tick_size概念，其他市场返回0（不做取整）。
        """
        if self._is_cn_future():
            try:
                from finhack.trader.backtest.markets.cn_future.future_trading_rules_versions import get_tick_size
                return get_tick_size(symbol)
            except Exception:
                return 0
        return 0

    def _calculate_commission(self, amount: float, side: Side, symbol: str = None) -> float:
        """计算手续费

        Args:
            amount: 成交金额
            side: 交易方向
            symbol: 交易标的（用于判断是否为平今）

        Returns:
            float: 手续费金额
        """
        if side in (Side.BUY, Side.SHORT_OPEN):
            commission_rate = self.context['settings'].get('open_commission', 0.0003)
        else:
            # 判断是否为平今（期货T+0：当日开仓当日平仓）
            is_close_today = self._is_close_today(symbol, side)
            if is_close_today:
                close_today_rate = self.context['settings'].get('close_today_commission', None)
                if close_today_rate is not None:
                    commission_rate = close_today_rate
                else:
                    commission_rate = self.context['settings'].get('close_commission', 0.0003)
            else:
                commission_rate = self.context['settings'].get('close_commission', 0.0003)

        commission = amount * commission_rate
        min_commission = self.context['settings'].get('min_commission', 5.0)

        return max(commission, min_commission)

    def _is_close_today(self, symbol: str, side: Side) -> bool:
        """判断是否为平今操作（当日开仓当日平仓）

        Args:
            symbol: 交易标的
            side: 交易方向

        Returns:
            bool: 是否为平今
        """
        if side not in (Side.SELL, Side.SHORT_CLOSE):
            return False

        current_time = self.context.get('current_dt')
        if not current_time:
            return False

        # 检查持仓的买入日期
        pos_key = symbol
        if side == Side.SHORT_CLOSE:
            pos_key = f"{symbol}{SHORT_POSITION_SUFFIX}"

        position = self.positions.get(pos_key)
        if position and hasattr(position, 'buy_dates') and position.buy_dates:
            for buy_time, buy_volume in position.buy_dates:
                if buy_time.date() == current_time.date():
                    return True

        return False

    def _calculate_tax(self, amount: float, side: Side) -> float:
        """计算税费"""
        if side in (Side.BUY, Side.SHORT_OPEN):
            tax_rate = self.context['settings'].get('open_tax', 0.0)
        else:
            tax_rate = self.context['settings'].get('close_tax', 0.001)

        return amount * tax_rate

    def _get_last_price(self, symbol: str) -> float:
        """获取最新价格（同步版本）

        Args:
            symbol: 股票代码

        Returns:
            float: 最新价格，获取失败返回0
        """
        try:
            current_time = self.context.get('current_dt')
            if not current_time:
                return 0.0

            # 尝试从data_center获取最新行情
            if hasattr(self, 'data_center') and self.data_center:
                quote_df = self.data_center.get_quotes([symbol], '1d', current_time)
                if not quote_df.empty:
                    matched = _match_quote_symbols(quote_df, [symbol])
                    if symbol in matched:
                        val = matched[symbol]['close']
                        if isinstance(val, pd.Series):
                            val = float(val.iloc[-1])
                        return float(val)

            # 尝试从持仓获取最后价格
            if symbol in self.positions:
                position = self.positions[symbol]
                if hasattr(position, 'last_price') and position.last_price > 0:
                    return float(position.last_price)
                if hasattr(position, 'cost_price') and position.cost_price > 0:
                    return float(position.cost_price)

            return 0.0
        except Exception as e:
            Log.logger.debug(f"获取{symbol}最新价格失败: {e}")
            return 0.0

    def _get_price_from_datacenter(self, symbol: str) -> float:
        """从数据中心获取股票价格（供backtest_trader使用）

        Args:
            symbol: 股票代码

        Returns:
            float: 最新价格，获取失败返回0
        """
        return self._get_last_price(symbol)

    def try_match_orders_sync(self, market_data: Dict[str, Dict]):
        """撮合订单 - 同步版本，支持部分成交"""
        matched_orders = []
        partial_orders = []
        current_time = self.context.get('current_dt')
        time_str = current_time.strftime('%Y-%m-%d %H:%M:%S') if current_time else '--:--:--'
        freq = self.context.get('settings', {}).get('freq', '1d')

        # 添加INFO级别日志方便调试
        Log.logger.debug(f"[{time_str}] 开始撮合，共{len(self.active_orders)}个活跃订单，市场数据标的: {list(market_data.keys())[:5]}...")

        if not market_data:
            # 对于24小时市场(crypto等)，无数据时段是正常的，降低日志级别
            market = self.context.get('settings', {}).get('market', '')
            if market in ('global_cryptospot', 'global_cryptoswap'):
                # crypto市场无数据时用debug级别，避免大量重复warning
                Log.logger.debug(f"[{time_str}] 市场数据为空（crypto正常现象），跳过撮合")
            else:
                Log.logger.warning(f"[{time_str}] 市场数据为空，无法撮合")
            return

        Log.logger.debug(f"[{time_str}] 开始撮合，共{len(self.active_orders)}个活跃订单（总订单{len(self.orders)}），市场数据: {list(market_data.keys())}")

        # 使用 active_orders 替代 orders，避免遍历所有历史订单
        for order_id, order in list(self.active_orders.items()):
            # 检查订单状态
            if order.status not in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
                continue

            symbol = order.symbol
            Log.logger.debug(f"[{time_str}] 检查订单 {order_id}: {symbol}, 类型: {order.order_type}, 状态: {order.status}, 剩余: {order.remaining_volume}")

            # 延迟撮合(防未来函数): 仅1m生效——当分钟下的单最早下一分钟撮合,
            # 避免用当根1m bar的收盘价撮合当分钟订单。1d不延迟(下单即撮合是1d设计,
            # 1d的未来函数由数据过滤严格<保证, 见 _filter_future_data)。
            if freq == '1m' and getattr(order, 'created_time', None) and current_time:
                time_diff = (current_time - order.created_time).total_seconds() / 60
                if time_diff < 1:
                    Log.logger.debug(f"[{time_str}] 跳过订单 {order_id}: 1m延迟撮合, 已等待 {time_diff:.2f} 分钟")
                    continue

            if symbol not in market_data:
                Log.logger.debug(f"[{time_str}] 跳过订单 {order_id}: {symbol} 不在市场数据中")
                continue

            quote = market_data[symbol]
            current_price = quote.get('close', 0)
            market_volume = quote.get('volume', 0)  # 【修复】获取市场成交量
            Log.logger.debug(f"[{time_str}] 订单 {order_id} 当前价格: {current_price}, 市场成交量: {market_volume}")

            if current_price <= 0:
                Log.logger.debug(f"[{time_str}] 跳过订单 {order_id}: 价格无效 {current_price}")
                continue

            # 【修复】基于市场成交量的成交限制配置 - 通过adapter获取市场规则
            max_fill_ratio = 0.15  # 单笔成交不超过市场成交量的15%
            min_fill_volume = self._get_min_fill_volume(symbol)
            lot_size = self._get_lot_size(symbol)

            # 计算基于市场成交量的最大可成交数量
            if market_volume > 0:
                max_fill_by_market = max(min_fill_volume, market_volume * max_fill_ratio)
            else:
                # 市场成交量为0时跳过撮合（无成交的K线不应执行订单）
                # volume=NaN 属数据异常（如缓存被窄字段回写污染），warning 便于诊断
                import math as _math
                if _math.isnan(market_volume) if isinstance(market_volume, float) else False:
                    Log.logger.warning(f"[{time_str}] 跳过订单 {order_id}: {symbol} volume=NaN (数据异常)")
                else:
                    Log.logger.debug(f"[{time_str}] 跳过订单 {order_id}: 市场成交量为0")
                continue

            # 判断是否可以成交
            can_fill = False
            fill_price = current_price
            fill_volume = order.remaining_volume  # 默认全部成交

            # 判断是否为日线频率（日线不做随机部分成交）
            is_daily_freq = freq == '1d'

            if order.order_type == OrderType.MARKET:
                if is_daily_freq:
                    # 日线频率：市价单直接全部成交（基于日线收盘价）
                    fill_volume = order.remaining_volume
                    fill_price = self._apply_slippage(current_price, order.side, order.symbol)
                    can_fill = True
                    Log.logger.debug(f"[{time_str}] [撮合] {order_id} {symbol} 日线市价单成交: {fill_volume}股 @{fill_price:.4f}")
                else:
                    # 分钟频率：部分成交逻辑（每次撮合只成交剩余量的部分比例）
                    import random, zlib
                    # 确定性成交比例: 按"订单ID+当前bar时间"用crc32定种子, 保证可复现
                    # (同份输入两次回测结果完全一致), 不用全局random以免结果随机漂移
                    _rng = random.Random(zlib.crc32(f"{order_id}|{current_time.isoformat()}".encode()))
                    # 从配置获取成交比例
                    if order.remaining_volume > self.partial_fill_config['large_order_threshold']:
                        fill_ratio = _rng.uniform(
                            self.partial_fill_config['large_order_min'],
                            self.partial_fill_config['large_order_max']
                        )
                    elif order.remaining_volume > self.partial_fill_config['medium_order_threshold']:
                        fill_ratio = _rng.uniform(
                            self.partial_fill_config['medium_order_min'],
                            self.partial_fill_config['medium_order_max']
                        )
                    else:
                        fill_ratio = _rng.uniform(
                            self.partial_fill_config['small_order_min'],
                            self.partial_fill_config['small_order_max']
                        )  # 小订单更容易全部成交

                    fill_volume = max(min_fill_volume, order.remaining_volume * fill_ratio)
                    # 按市场规则取整
                    fill_volume = self._normalize_volume(fill_volume, symbol)
                    # 不超过剩余量，且不超过市场成交量限制
                    fill_volume = min(fill_volume, order.remaining_volume, max_fill_by_market)

                    can_fill = True
                    fill_price = self._apply_slippage(current_price, order.side, order.symbol)
                    if fill_volume < order.remaining_volume:
                        Log.logger.debug(f"[{time_str}] [撮合] {order_id} {symbol} 市价单部分成交: {order.remaining_volume}->{fill_volume}股 @{fill_price:.4f} (市场成交量限制: {max_fill_by_market})")
                    else:
                        Log.logger.debug(f"[{time_str}] [撮合] {order_id} {symbol} 市价单成交: {fill_volume}股 @{fill_price:.4f}")
            elif order.order_type == OrderType.LIMIT:
                # 限价单需要判断价格
                if order.side == Side.BUY and order.price >= current_price:
                    can_fill = True
                    fill_price = min(order.price, current_price)
                    if is_daily_freq:
                        fill_volume = order.remaining_volume
                    else:
                        fill_volume = min(order.remaining_volume, max_fill_by_market)
                        fill_volume = self._normalize_volume(fill_volume, symbol)
                        fill_volume = max(min_fill_volume, fill_volume)
                    Log.logger.debug(f"[{time_str}] [撮合] {order_id} {symbol} 限价买单可成交: {order.price}>={current_price}, 成交{fill_volume}股")
                elif order.side == Side.SELL and order.price <= current_price:
                    can_fill = True
                    fill_price = max(order.price, current_price)
                    if is_daily_freq:
                        fill_volume = order.remaining_volume
                    else:
                        fill_volume = min(order.remaining_volume, max_fill_by_market)
                        fill_volume = self._normalize_volume(fill_volume, symbol)
                        fill_volume = max(min_fill_volume, fill_volume)
                    Log.logger.debug(f"[{time_str}] [撮合] {order_id} {symbol} 限价卖单可成交: {order.price}<={current_price}, 成交{fill_volume}股")
                # 开空仓限价单：卖方逻辑（价格低于等于限价时成交）
                elif order.side == Side.SHORT_OPEN and order.price >= current_price:
                    can_fill = True
                    fill_price = min(order.price, current_price)
                    if is_daily_freq:
                        fill_volume = order.remaining_volume
                    else:
                        fill_volume = min(order.remaining_volume, max_fill_by_market)
                        fill_volume = self._normalize_volume(fill_volume, symbol)
                        fill_volume = max(min_fill_volume, fill_volume)
                    Log.logger.debug(f"[{time_str}] [撮合] {order_id} {symbol} 限价开空单可成交: {order.price}>={current_price}, 成交{fill_volume}股")
                # 平空仓限价单：买方逻辑（价格高于等于限价时成交）
                elif order.side == Side.SHORT_CLOSE and order.price <= current_price:
                    can_fill = True
                    fill_price = max(order.price, current_price)
                    if is_daily_freq:
                        fill_volume = order.remaining_volume
                    else:
                        fill_volume = min(order.remaining_volume, max_fill_by_market)
                        fill_volume = self._normalize_volume(fill_volume, symbol)
                        fill_volume = max(min_fill_volume, fill_volume)
                    Log.logger.debug(f"[{time_str}] [撮合] {order_id} {symbol} 限价平空单可成交: {order.price}<={current_price}, 成交{fill_volume}股")
                else:
                    Log.logger.debug(f"[{time_str}] 订单 {order_id} 限价单价格不匹配: {order.price} vs {current_price}")

            if can_fill and fill_volume > 0:
                # 统一标准化成交量（确保符合市场最小交易单位）
                fill_volume = self._normalize_volume(fill_volume, symbol)
                if fill_volume <= 0:
                    # 标准化后不足一手，跳过
                    continue

                # 执行成交
                Log.logger.debug(f"[{time_str}] [撮合] {order_id} {symbol} {order.side} 成交{fill_volume}/{order.volume}股 @{fill_price:.4f}")
                self._execute_trade_sync(order, fill_price, fill_volume)

                # 检查订单状态
                if order.status == OrderStatus.FILLED:
                    matched_orders.append(order_id)
                    Log.logger.debug(f"[{time_str}] [撮合] {order_id} 全部成交 ✓")
                elif order.status == OrderStatus.PARTIALLY_FILLED:
                    partial_orders.append(order_id)
                    Log.logger.debug(f"[{time_str}] [撮合] {order_id} 部分成交 {order.filled_volume}/{order.volume}")
            else:
                Log.logger.debug(f"[{time_str}] 订单 {order_id} 不可成交: can_fill={can_fill}, fill_volume={fill_volume}, current_price={current_price}, order_type={order.order_type}")

        # 从活跃订单中移除已完全成交的订单
        for order_id in matched_orders:
            if order_id in self.active_orders:
                del self.active_orders[order_id]

        # 清理僵尸订单：剩余量极小时强制完成（避免浮点精度导致永远无法完全成交）
        ZOMBIE_THRESHOLD = ZOMBIE_ORDER_THRESHOLD
        zombie_cleaned = []
        for order_id in list(self.active_orders.keys()):
            order = self.active_orders.get(order_id)
            if order and order.remaining_volume < ZOMBIE_THRESHOLD:
                # 将微小剩余量视为全部成交
                if order.remaining_volume > 0:
                    tiny_volume = order.remaining_volume
                    order.filled_volume = order.volume
                    order.status = OrderStatus.FILLED
                    del self.active_orders[order_id]
                    zombie_cleaned.append(order_id)
                    Log.logger.debug(f"[{time_str}] [撮合] 清理僵尸订单 {order_id}: 剩余{tiny_volume:.2e}视为全部成交")

        Log.logger.debug(f"[{time_str}] [撮合] 完成: 全部成交{len(matched_orders)}单, 部分成交{len(partial_orders)}单, 清理僵尸{len(zombie_cleaned)}单, 剩余活跃{len(self.active_orders)}单")

        if matched_orders:
            Log.logger.debug(f"[{time_str}] [撮合] 全部成交: {matched_orders}")
        if partial_orders:
            Log.logger.debug(f"[{time_str}] [撮合] 部分成交: {partial_orders}")
        
    def _execute_trade_sync(self, order: Order, fill_price: float, fill_volume: float = None):
        """执行成交 - 同步版本，支持部分成交

        Args:
            order: 订单对象
            fill_price: 成交价格
            fill_volume: 成交数量，如果为None则使用order.remaining_volume
        """
        try:
            # 确定成交数量
            if fill_volume is None:
                fill_volume = order.remaining_volume

            # 确保不超过剩余量
            fill_volume = min(fill_volume, order.remaining_volume)

            # 获取事件时间
            current_time = self.context.get('current_dt', datetime.now())
            time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')

            # 计算成交金额
            fill_amount = fill_volume * fill_price

            # 计算费用（保证金市场需按合约价值计算手续费）
            if self._uses_margin():
                contract_value = self._get_futures_contract_value(order.symbol, fill_volume, fill_price)
                commission = self._calculate_commission(contract_value, order.side, order.symbol)
            else:
                commission = self._calculate_commission(fill_amount, order.side, order.symbol)
            tax = self._calculate_tax(fill_amount, order.side)
            total_cost = commission + tax

            # 买入订单：检查资金是否充足（防止超额交易）
            if order.side == Side.BUY:
                frozen_amount = getattr(order, '_frozen_amount', 0)
                if self._is_cn_future() or self._is_crypto_swap():
                    margin = self._get_futures_margin(order.symbol, fill_volume, fill_price)
                    total_required = margin + total_cost
                else:
                    total_required = fill_amount + total_cost
                if frozen_amount > 0:
                    # 部分成交：按比例解冻已成交部分，保留剩余冻结
                    fill_ratio = fill_volume / order.volume if order.volume > 0 else 1.0
                    release_amount = frozen_amount * fill_ratio
                    self.account.unfreeze_cash(release_amount)
                    order._frozen_amount = frozen_amount - release_amount
                elif self.account.cash_available < total_required:
                    Log.logger.error(f"[{time_str}] 资金不足，成交被拒绝: {order.symbol} "
                                   f"需要{total_required:.2f}元，可用{self.account.cash_available:.2f}元")
                    order.status = OrderStatus.REJECTED
                    order.rejected_reason = f"资金不足: 需要{total_required:.2f}, 可用{self.account.cash_available:.2f}"
                    if order.order_id in self.active_orders:
                        del self.active_orders[order.order_id]
                    return

            # 卖出订单：检查持仓是否充足
            elif order.side == Side.SELL:
                position = self.positions.get(order.symbol)
                available_volume = position.available_volume if position else 0
                # 解冻SELL订单可能冻结的保证金（非转开空的情况）
                frozen_amount_sell = getattr(order, '_frozen_amount', 0)
                if frozen_amount_sell > 0 and available_volume >= fill_volume:
                    self.account.unfreeze_cash(frozen_amount_sell)
                    order._frozen_amount = 0
                if available_volume < fill_volume:
                    # BUG 3 fix: 期货市场：无多头持仓(total=0)时自动转为开空仓
                    market = self.context.get('settings', {}).get('market', '')
                    total_volume = position.volume if position else 0
                    if self._uses_margin() and total_volume <= POSITION_DUST_THRESHOLD:
                        order.side = Side.SHORT_OPEN
                        margin = self._get_futures_margin(order.symbol, fill_volume, fill_price)
                        total_required = margin + total_cost
                        frozen_amount = getattr(order, '_frozen_amount', 0)
                        if frozen_amount > 0:
                            self.account.unfreeze_cash(frozen_amount)
                            order._frozen_amount = 0
                        elif self.account.cash_available < total_required:
                            Log.logger.error(f"[{time_str}] 资金不足，开空仓被拒绝: {order.symbol} "
                                           f"需要保证金{total_required:.2f}元，可用{self.account.cash_available:.2f}元")
                            order.status = OrderStatus.REJECTED
                            order.rejected_reason = f"资金不足: 开空仓需要{total_required:.2f}, 可用{self.account.cash_available:.2f}"
                            if order.order_id in self.active_orders:
                                del self.active_orders[order.order_id]
                            return
                    elif self._uses_margin() and total_volume > POSITION_DUST_THRESHOLD and available_volume <= POSITION_DUST_THRESHOLD:
                        # 有持仓但被冻结，拒绝订单
                        Log.logger.error(f"[{time_str}] 持仓冻结中，卖出被拒绝: {order.symbol} "
                                       f"总持仓{total_volume}，可用{available_volume}")
                        order.status = OrderStatus.REJECTED
                        order.rejected_reason = f"持仓冻结中: 总持仓{total_volume}, 可用{available_volume}"
                        if order.order_id in self.active_orders:
                            del self.active_orders[order.order_id]
                        return
                    else:
                        # 期货/合约市场：有可用持仓但不足时，缩减至可用量（部分平仓）
                        if self._uses_margin() and available_volume > POSITION_DUST_THRESHOLD:
                            original_volume = fill_volume
                            fill_volume = available_volume
                            # 重新计算缩减后的成交金额和手续费
                            contract_value = self._get_futures_contract_value(order.symbol, fill_volume, fill_price)
                            commission = self._calculate_commission(contract_value, order.side, order.symbol)
                            total_cost = commission + self._calculate_tax(fill_volume * fill_price, order.side)
                            Log.logger.info(f"[{time_str}] 持仓不足，缩减卖出量: {order.symbol} "
                                           f"申请{original_volume}手 → 实际{fill_volume}手")
                        else:
                            Log.logger.error(f"[{time_str}] 持仓不足，成交被拒绝: {order.symbol} "
                                           f"需要{fill_volume}股，可用{available_volume}股")
                            order.status = OrderStatus.REJECTED
                            order.rejected_reason = f"持仓不足: 需要{fill_volume}, 可用{available_volume}"
                            if order.order_id in self.active_orders:
                                del self.active_orders[order.order_id]
                            return

            # 生成成交ID
            trade_id = f"trade_{self.trade_id_counter:06d}"
            self.trade_id_counter += 1

            # 创建成交记录
            trade = Trade(
                account_id=order.account_id,
                symbol=order.symbol,
                order_id=order.order_id,
                trade_id=trade_id,
                side=order.side,
                volume=fill_volume,
                price=fill_price,
                amount=fill_amount,
                commission=commission,
                tax=tax,
                trade_time=current_time
            )

            # 更新订单状态（支持部分成交）
            # 更新累计成交数据
            total_amount = order.filled_amount + fill_amount
            total_volume = order.filled_volume + fill_volume

            # 计算新的平均成交价
            order.avg_fill_price = total_amount / total_volume if total_volume > 0 else 0.0
            order.filled_volume = total_volume
            order.filled_amount = total_amount
            order.updated_time = trade.trade_time

            # 更新订单状态
            if order.filled_volume >= order.volume:
                order.status = OrderStatus.FILLED
                Log.logger.debug(f"[{time_str}] [订单] {order.order_id} 全部成交 ✓ ({order.filled_volume}/{order.volume}股)")
            else:
                order.status = OrderStatus.PARTIALLY_FILLED
                Log.logger.debug(f"[{time_str}] [订单] {order.order_id} 部分成交 ({order.filled_volume}/{order.volume}股)")

            # 从活跃订单中移除已完全成交的订单
            if order.status == OrderStatus.FILLED and order.order_id in self.active_orders:
                del self.active_orders[order.order_id]

            # 添加成交记录
            self.trades.append(trade)

            # 更新持仓和账户
            # 先更新持仓，获取卖出时的成本价（用于准确计算盈亏）
            try:
                sell_cost_price = self._update_position_sync(trade, total_cost)
                Log.logger.debug(f"[调试] _update_position_sync 返回: {sell_cost_price}")
            except Exception as e:
                Log.logger.error(f"[调试] _update_position_sync 失败: {e}")
                import traceback
                Log.logger.error(traceback.format_exc())
                sell_cost_price = 0.0
            # 再更新账户，传入成本价避免持仓删除后获取不到
            try:
                self._update_account_sync(trade, total_cost, sell_cost_price)
            except Exception as e:
                Log.logger.error(f"[调试] _update_account_sync 失败: {e}")
                import traceback
                Log.logger.error(traceback.format_exc())

            # 调试：检查持仓成本价
            if trade.symbol in self.positions:
                pos = self.positions[trade.symbol]
                Log.logger.debug(f"[调试] 持仓更新后 {trade.symbol}: volume={pos.volume}, cost_price={pos.cost_price:.4f}")

            Log.logger.debug(f"[{time_str}] [成交] {trade_id} {order.symbol} {order.side.value} "
                          f"{fill_volume}股@{fill_price:.4f} 费用:{total_cost:.2f} "
                          f"(累计:{order.filled_volume}/{order.volume}股)")

        except Exception as e:
            current_time = self.context.get('current_dt', datetime.now())
            time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
            Log.logger.error(f"[{time_str}] 执行成交失败: {e}")
            order.status = OrderStatus.REJECTED
            order.rejected_reason = f"成交执行失败: {str(e)}"
            # 从活跃订单中移除失败的订单
            if order.order_id in self.active_orders:
                del self.active_orders[order.order_id]
            
    def _update_position_sync(self, trade: Trade, total_cost: float = 0.0) -> float:
        """更新持仓 - 同步版本

        Args:
            trade: 成交记录
            total_cost: 交易费用（手续费+税费），用于准确计算成本价

        Returns:
            float: 卖出/平仓时的成本价（用于盈亏计算），买入时返回0
        """
        symbol = trade.symbol
        sell_cost_price = 0.0  # 用于记录卖出时的成本价

        # ========== 多头方向：BUY / SELL ==========
        if trade.side in (Side.BUY, Side.SELL):
            if symbol not in self.positions:
                # 创建新持仓
                cm = self._get_contract_size(symbol)
                self.positions[symbol] = Position(
                    account_id=trade.account_id,
                    symbol=symbol,
                    position_side=PositionSide.LONG,
                    volume=0,
                    available_volume=0,
                    cost_price=0,
                    market_value=0,
                    unrealized_pnl=0,
                    contract_multiplier=cm,
                )

            position = self.positions[symbol]

            if trade.side == Side.BUY:
                # 买入：增加持仓
                old_volume = position.volume
                old_cost = position.volume * position.cost_price
                if self._uses_margin():
                    # 保证金市场：成本价不含手续费（手续费已从现金扣除）
                    new_cost = trade.volume * trade.price
                else:
                    # 新成本 = 成交金额 + 交易费用（费用分摊到每股）
                    new_cost = trade.volume * trade.price + total_cost

                Log.logger.debug(f"[调试] BUY {symbol}: old_volume={old_volume}, old_cost={old_cost}, "
                               f"trade.volume={trade.volume}, trade.price={trade.price}, total_cost={total_cost}, "
                               f"new_cost={new_cost}")

                position.volume += trade.volume

                # T+1规则：根据市场adapter和具体代码判断
                if self._is_t_plus_one(symbol):
                    if not hasattr(position, 'buy_dates'):
                        position.buy_dates = []
                    current_time = self.context.get('current_dt')
                    position.buy_dates.append((current_time, trade.volume))
                    if old_volume == 0:
                        position.available_volume = 0
                else:
                    # 非T+1市场（期货、加密货币），买入立即可用
                    position.available_volume += trade.volume

                # 计算新的加权平均成本价
                if position.volume > 0:
                    position.cost_price = (old_cost + new_cost) / position.volume
                else:
                    position.cost_price = 0

            elif trade.side == Side.SELL:
                # 卖出：减少持仓
                sell_cost_price = position.cost_price

                position.volume -= trade.volume

                # T+1: 卖出时同步消费buy_dates（FIFO），防止available_volume损坏
                if hasattr(position, 'buy_dates') and position.buy_dates:
                    sell_remaining = trade.volume
                    # 先从已解冻(available)的份额中消费
                    if sell_remaining <= position.available_volume:
                        position.available_volume -= sell_remaining
                        sell_remaining = 0
                    else:
                        sell_remaining -= position.available_volume
                        position.available_volume = 0

                    # 剩余从冻结的buy_dates中消费（FIFO）
                    if sell_remaining > 0:
                        remaining_buy_dates = []
                        for buy_time, buy_volume in position.buy_dates:
                            if sell_remaining <= 0:
                                remaining_buy_dates.append((buy_time, buy_volume))
                            elif sell_remaining >= buy_volume:
                                sell_remaining -= buy_volume
                            else:
                                remaining_buy_dates.append((buy_time, buy_volume - sell_remaining))
                                sell_remaining = 0
                        position.buy_dates = remaining_buy_dates
                else:
                    position.available_volume -= trade.volume

                # 浮点精度修复：持仓量极小时视为清零
                if position.volume < POSITION_DUST_THRESHOLD:
                    Log.logger.debug(f"[浮点清零] {symbol} 残留持仓 {position.volume:.2e} 视为清零")
                    del self.positions[symbol]
                    return sell_cost_price

                # 如果持仓为0，移除持仓记录
                if position.volume <= 0:
                    del self.positions[symbol]
                    return sell_cost_price

            # 更新持仓市值（使用 Position 模型方法，正确处理 contract_multiplier 和多空方向）
            if symbol in self.positions and position.volume > 0:
                position.update_market_price(trade.price)

        # ========== 空头方向：SHORT_OPEN / SHORT_CLOSE ==========
        elif trade.side in (Side.SHORT_OPEN, Side.SHORT_CLOSE):
            short_key = f"{symbol}{SHORT_POSITION_SUFFIX}"

            if short_key not in self.positions:
                self.positions[short_key] = Position(
                    account_id=trade.account_id,
                    symbol=symbol,
                    position_side=PositionSide.SHORT,
                    volume=0,
                    available_volume=0,
                    cost_price=0,
                    market_value=0,
                    unrealized_pnl=0,
                    contract_multiplier=self._get_contract_size(symbol),
                )

            position = self.positions[short_key]

            if trade.side == Side.SHORT_OPEN:
                # 开空：增加空头持仓
                old_volume = position.volume
                old_cost = position.volume * position.cost_price
                new_cost = trade.volume * trade.price

                position.volume += trade.volume
                # 期货/加密货币T+0，立即可用
                position.available_volume += trade.volume

                if position.volume > 0:
                    position.cost_price = (old_cost + new_cost) / position.volume

                Log.logger.debug(f"[开空] {symbol}: volume={old_volume}->{position.volume}, cost_price={position.cost_price:.4f}")

            elif trade.side == Side.SHORT_CLOSE:
                # 平空：减少空头持仓
                sell_cost_price = position.cost_price  # 这里是开空均价

                position.volume -= trade.volume
                position.available_volume -= trade.volume

                Log.logger.debug(f"[平空] {symbol}: volume -> {position.volume}, cost_price={position.cost_price:.4f}")

                # 浮点精度修复：持仓量极小时视为清零
                if position.volume < POSITION_DUST_THRESHOLD:
                    Log.logger.debug(f"[浮点清零] {short_key} 残留持仓 {position.volume:.2e} 视为清零")
                    del self.positions[short_key]
                    return sell_cost_price

                if position.volume <= 0:
                    del self.positions[short_key]
                    return sell_cost_price

            # 更新空头持仓市值和盈亏（使用 Position 模型方法）
            if short_key in self.positions and position.volume > 0:
                position.update_market_price(trade.price)

        return sell_cost_price
        
    def _update_account_sync(self, trade: Trade, total_cost: float, sell_cost_price: float = 0.0):
        """更新账户 - 同步版本

        Args:
            trade: 成交记录
            total_cost: 交易费用（手续费+税费）
            sell_cost_price: 卖出时的成本价（从_update_position_sync返回），用于准确计算已实现盈亏
        """
        if trade.side == Side.BUY:
            if self._uses_margin():
                # 保证金市场（期货/永续）：只扣除保证金+手续费
                margin = self._get_futures_margin(trade.symbol, trade.volume, trade.price)
                self.account.cash_available -= (margin + total_cost)
                self.account.margin_used += margin
            else:
                # 买入：减少现金，增加持仓市值
                self.account.cash_available -= (trade.amount + total_cost)
                self.account.market_value += trade.amount
        elif trade.side == Side.SELL:
            cost_price = sell_cost_price if sell_cost_price > 0 else self._get_cost_price(trade.symbol)
            if self._uses_margin():
                # 保证金市场：释放保证金 + 结算盈亏
                margin_released = self._get_futures_margin(trade.symbol, trade.volume, cost_price)
                contract_size = self._get_contract_size(trade.symbol)
                realized_pnl = (trade.price - cost_price) * trade.volume * contract_size - total_cost
                self.account.cash_available += (margin_released + realized_pnl)
                self.account.margin_used -= margin_released
            else:
                # 卖出：增加现金，减少持仓市值
                self.account.cash_available += (trade.amount - total_cost)
                self.account.market_value -= trade.amount
                realized_pnl = (trade.price - cost_price) * trade.volume - total_cost
            self.account.pnl_realized += realized_pnl
        elif trade.side == Side.SHORT_OPEN:
            if self._uses_margin():
                # 保证金市场：按合约乘数和保证金比例扣除保证金
                margin = self._get_futures_margin(trade.symbol, trade.volume, trade.price)
                self.account.cash_available -= (margin + total_cost)
                self.account.margin_used += margin
            else:
                # 其他市场：全额
                self.account.cash_available -= (trade.amount + total_cost)
                self.account.market_value += trade.amount
        elif trade.side == Side.SHORT_CLOSE:
            cost_price = sell_cost_price if sell_cost_price > 0 else self._get_cost_price(trade.symbol + SHORT_POSITION_SUFFIX)
            if self._uses_margin():
                # 保证金市场：释放保证金 + 结算盈亏
                margin_released = self._get_futures_margin(trade.symbol, trade.volume, cost_price)
                contract_size = self._get_contract_size(trade.symbol)
                realized_pnl = (cost_price - trade.price) * trade.volume * contract_size - total_cost
                self.account.cash_available += (margin_released + realized_pnl)
                self.account.margin_used -= margin_released
            else:
                # 平空：释放资金，计算盈亏
                self.account.cash_available += (trade.amount - total_cost)
                self.account.market_value -= trade.amount
                realized_pnl = (cost_price - trade.price) * trade.volume - total_cost
            self.account.pnl_realized += realized_pnl

        # 更新总资产
        self._update_account_value()
        
    def _get_cost_price(self, symbol: str) -> float:
        """获取持仓成本价"""
        if symbol in self.positions:
            return self.positions[symbol].cost_price
        return 0

    def check_futures_expiry(self):
        """检查期货合约是否到期/进入交割月，自动平仓

        通过市场适配器获取交割信息，在交割月或合约过期时自动平仓。
        仅对 cn_future 市场生效。
        """
        if not self._is_cn_future():
            return
        if not self.market_adapter:
            return
        if not hasattr(self.market_adapter, 'get_delivery_info'):
            return

        current_time = self.context.get('current_dt')
        if not current_time:
            return

        positions_to_close = []
        for pos_key, position in list(self.positions.items()):
            symbol = position.symbol
            try:
                delivery_info = self.market_adapter.get_delivery_info(symbol)
                natural_person_ban = delivery_info.get('natural_person_ban', {})
                is_banned = natural_person_ban.get('is_banned', False)
                reason = natural_person_ban.get('reason', '')

                if is_banned and ('过期' in reason or '交割月' in reason):
                    positions_to_close.append((pos_key, position, reason))
            except Exception as e:
                Log.logger.debug(f"检查合约到期 {symbol}: {e}")

        for pos_key, position, reason in positions_to_close:
            symbol = position.symbol
            volume = position.volume
            last_price = getattr(position, 'last_price', None) or position.cost_price

            if volume <= 0 or last_price <= 0:
                continue

            side = Side.SELL if position.position_side == PositionSide.LONG else Side.SHORT_CLOSE
            side_desc = "卖出" if side == Side.SELL else "平空"

            Log.logger.warning(
                f"[合约到期] {symbol} {reason}，自动{side_desc} {volume}手 @结算价{last_price:.2f}"
            )

            # 创建平仓交易记录并直接执行（绕过撮合，因为合约可能已无市场数据）
            trade = Trade(
                trade_id=f"expiry_{pos_key}_{int(current_time.timestamp())}",
                order_id=f"expiry_order_{pos_key}_{int(current_time.timestamp())}",
                account_id=self.context.get('account', {}).get('account_id', 'default'),
                symbol=symbol,
                side=side,
                volume=volume,
                price=last_price,
                amount=volume * last_price * position.contract_multiplier,
                commission=0,  # 到期平仓不收手续费
                tax=0,
                trade_time=current_time,
            )
            total_cost = 0.0

            # 先更新持仓，记录清仓前的unrealized_pnl
            position.update_market_price(last_price)
            sell_cost_price = self._update_position_sync(trade, total_cost)
            self._update_account_sync(trade, total_cost, sell_cost_price)

            # 确保已删除的持仓不再被后续计算引用
            # 注意：必须用 pos_key 而非 symbol，因为空头持仓的 key 是 "symbol_SHORT"
            if pos_key in self.positions:
                del self.positions[pos_key]

            # 强制重算账户价值，避免残留的unrealized_pnl影响
            self._update_account_value()

            Log.logger.info(
                f"[合约到期] {symbol} 已自动平仓: {volume}手 @ {last_price:.2f}"
            )

    def handle_corporate_action(self, event):
        """处理公司行为事件（分红、送股等）

        具有防重复处理机制，确保同一公司行为不会被重复处理。
        """
        try:
            # 获取公司行为信息（支持属性访问和字典访问）
            if hasattr(event, 'data') and event.data:
                action_type = event.data.get('action_type', '')
                symbol = event.data.get('symbol', '')
            else:
                action_type = getattr(event, 'action_type', '')
                symbol = getattr(event, 'symbol', '')

            if not symbol or not action_type:
                return

            # 获取除权除息日期（用于防重复检查）
            ex_date = None
            if hasattr(event, 'data') and event.data:
                ex_date = event.data.get('ex_date')
            if not ex_date and hasattr(event, 'event_time'):
                ex_date = event.event_time.date() if event.event_time else None

            # 创建唯一键进行防重复检查
            if ex_date:
                action_key = (symbol, str(ex_date), action_type)
                if action_key in self._processed_corporate_actions:
                    Log.logger.debug(f"公司行为已处理，跳过: {symbol} {action_type} {ex_date}")
                    return

            # 处理除权除息
            if action_type in ['dividend', 'bonus', 'split', 'rights']:
                if symbol not in self.positions:
                    return

                position = self.positions[symbol]

                # 处理分红
                if action_type == 'dividend':
                    self._handle_dividend(event, position, symbol)

                # 处理送股
                elif action_type == 'bonus':
                    self._handle_bonus(event, position, symbol)

                # 处理拆股
                elif action_type == 'split':
                    self._handle_split(event, position, symbol)

                # 更新持仓市值（公司行为后需要重新计算）
                # 获取当前价格重新计算市值
                current_price = self._get_last_price(symbol)
                if current_price and current_price > 0:
                    position.market_value = position.volume * current_price
                    position.last_price = current_price
                    position.unrealized_pnl = (current_price - position.cost_price) * position.volume

                # 更新账户总资产
                self._update_account_value()

                # 记录已处理的公司行为
                if ex_date:
                    action_key = (symbol, str(ex_date), action_type)
                    self._processed_corporate_actions.add(action_key)

        except Exception as e:
            Log.logger.error(f"处理公司行为事件失败: {e}")
            import traceback
            traceback.print_exc()

    def _handle_dividend(self, event, position, symbol):
        """处理分红事件（含除息处理）"""
        # 获取数据
        if hasattr(event, 'data') and event.data:
            dividend_per_share = event.data.get('dividend_per_share', 0)
            tax_rate = event.data.get('tax_rate', 0.10)
        else:
            dividend_per_share = getattr(event, 'dividend_per_share', 0)
            tax_rate = getattr(event, 'tax_rate', 0.10)

        if dividend_per_share <= 0:
            return

        dividend_amount = position.volume * dividend_per_share
        after_tax = dividend_amount * (1 - tax_rate)

        # 记录除息前成本
        old_cost_price = position.cost_price

        # 增加现金
        self.account.cash_available += after_tax

        # 除息处理：降低持仓成本价
        # 每股分红导致每股成本降低
        new_cost_price = max(0, old_cost_price - dividend_per_share)
        position.cost_price = new_cost_price

        Log.logger.info(f"[公司行为] {symbol} 分红: 每股{dividend_per_share:.4f}元, "
                       f"税率{tax_rate:.0%}, 实得{after_tax:.2f}元, "
                       f"除息: 成本{old_cost_price:.4f}→{new_cost_price:.4f}")

    def _handle_bonus(self, event, position, symbol):
        """处理送股事件"""
        if hasattr(event, 'data') and event.data:
            bonus_ratio = event.data.get('bonus_ratio', 0)
        else:
            bonus_ratio = getattr(event, 'bonus_ratio', 0)

        if bonus_ratio <= 0:
            return

        bonus_shares = int(position.volume * bonus_ratio)
        if bonus_shares > 0:
            # 100股整数倍
            bonus_shares = (bonus_shares // 100) * 100
            old_volume = position.volume
            position.volume += bonus_shares
            position.available_volume += bonus_shares

            # 调整成本价（送股后成本降低）
            if position.volume > 0:
                position.cost_price = position.cost_price * old_volume / position.volume

            Log.logger.info(f"[公司行为] {symbol} 送股: 比例{bonus_ratio:.2f}, "
                           f"持仓{old_volume}→{position.volume}股, "
                           f"新成本{position.cost_price:.4f}")

    def _handle_split(self, event, position, symbol):
        """处理拆股事件"""
        if hasattr(event, 'data') and event.data:
            split_ratio = event.data.get('split_ratio', 1)
        else:
            split_ratio = getattr(event, 'split_ratio', 1)

        if split_ratio <= 0 or split_ratio == 1:
            return

        old_volume = position.volume
        new_volume = int(old_volume * split_ratio)
        actual_ratio = new_volume / old_volume if old_volume > 0 else 1

        position.volume = new_volume
        position.available_volume = new_volume
        position.cost_price = position.cost_price / actual_ratio

        Log.logger.info(f"[公司行为] {symbol} 拆股: 比例{split_ratio:.2f}, "
                       f"持仓{old_volume}→{new_volume}股, "
                       f"新成本{position.cost_price:.4f}")

class BacktestEngine:
    """回测引擎主类"""

    def __init__(self, context: Dict, data_center, event_center, event_bus):
        self.context = context
        self.data_center = data_center
        self.event_center = event_center
        self.event_bus = event_bus

        # 初始化交易中心 - 注入market_adapter实现市场规则委托
        market_name = context.get('settings', {}).get('market', 'cn_stock')
        market_adapter = None
        if self.event_center and hasattr(self.event_center, 'market_adapters'):
            market_adapter = self.event_center.market_adapters.get(market_name)
        self.trade_center = TradeCenter(context, market_adapter=market_adapter, data_center=data_center)
        self._last_synced_date = None  # 日期同步缓存

        # 智能加速跳过追踪（用于合并日志）
        self._skip_total_minutes = 0.0
        self._skip_start_time = None

        # 撮合跳过追踪（用于合并无活跃订单的日志）
        self._match_skip_count = 0
        self._match_skip_start_time = None
        
        # 注册所有市场事件的默认处理器
        market_events = [
            EventTypeEnum.DAY_START,
            EventTypeEnum.BEFORE_MARKET,
            EventTypeEnum.PRE_OPENING_START,
            EventTypeEnum.PRE_OPENING_END,
            EventTypeEnum.MATCHING_START,
            EventTypeEnum.OPENING_PRICE_DETERMINED,
            EventTypeEnum.MARKET_START,
            EventTypeEnum.MORNING_END,
            EventTypeEnum.AFTERNOON_START,
            EventTypeEnum.CLOSING_START,
            EventTypeEnum.CLOSING_END,
            EventTypeEnum.CLOSING_PRICE_DETERMINED,
            EventTypeEnum.MARKET_END,
            EventTypeEnum.DAILY_BAR_CLOSED,
            EventTypeEnum.AFTER_MARKET,
            EventTypeEnum.DAY_END
        ]
        
        for event_type in market_events:
            self.event_center.event_bus.register_handler(event_type, self._process_event_sync)
        
        # 注册动态事件的默认处理器
        dynamic_events = [
            EventTypeEnum.ON_TIME,
            EventTypeEnum.TRY_MATCH,
            EventTypeEnum.ORDER_SUBMISSION,
            EventTypeEnum.ORDER_CANCELLATION,
            EventTypeEnum.ORDER_REJECT,
            EventTypeEnum.ORDER_FILL
        ]
        
        for event_type in dynamic_events:
            self.event_center.event_bus.register_handler(event_type, self._process_event_sync)

    def _is_cn_future(self):
        """检查当前市场是否为期货（有交割）。优先问 adapter，否则 fallback market==。"""
        adapter = getattr(self, 'market_adapter', None)
        if adapter:
            try: return adapter.has_delivery()
            except Exception: pass
        market = self.context.get('settings', {}).get('market', '')
        return market == 'cn_future'

    def _is_crypto_swap(self):
        """检查当前市场是否为加密永续（有资金费率）。优先问 adapter。"""
        adapter = getattr(self, 'market_adapter', None)
        if adapter:
            try: return adapter.has_funding_rate()
            except Exception: pass
        market = self.context.get('settings', {}).get('market', '')
        return market == 'global_cryptoswap'

    def _uses_margin(self):
        """检查当前市场是否使用保证金（衍生品）。优先问 adapter。"""
        adapter = getattr(self, 'market_adapter', None)
        if adapter:
            try: return adapter.is_derivatives()
            except Exception: pass
        return self._is_cn_future() or self._is_crypto_swap()

    def _sync_backtest_date(self, current_date: date):
        """同步回测日期到所有市场适配器

        确保所有适配器使用回测日期而非系统日期，
        避免历史回测使用当前的交易规则、佣金率、保证金比例等。

        Args:
            current_date: 回测当前日期
        """
        if self._last_synced_date == current_date:
            return
        self._last_synced_date = current_date

        # 通过 TradeCenter 获取 market_adapter 并同步
        adapter = getattr(self.trade_center, 'market_adapter', None)
        if adapter and hasattr(adapter, 'set_current_date'):
            adapter.set_current_date(current_date)
        # 同步数据中心引用（供 validate_order 取前收盘/标的元数据，如 cn_stock 涨跌停校验）
        if adapter and hasattr(adapter, 'set_data_center') and self.data_center is not None:
            adapter.set_data_center(self.data_center)

    def _has_pending_orders(self):
        """检查是否有挂单（未成交的订单）

        Returns:
            bool: True表示有挂单，False表示没有挂单
        """
        # 使用 active_orders，性能更好
        return len(self.trade_center.active_orders) > 0
        
    def _handle_order_submission(self, event: Dict):
        """处理订单提交事件"""
        # 订单提交逻辑已在place_order中处理
        pass
        
    def _handle_order_cancellation(self, event: Dict):
        """处理订单撤销事件"""
        # 订单撤销逻辑已在cancel_order中处理
        pass
        
    def _get_context_filepath(self) -> Optional[str]:
        """获取上下文文件路径"""
        settings = self.context.get('settings', {})
        base_dir = settings.get('base_dir', os.getcwd())
        running_dir = Path(base_dir) / "data" / "running"
        running_dir.mkdir(parents=True, exist_ok=True)

        # 使用策略名和market生成文件名
        strategy_name = settings.get('strategy_name', 'unknown')
        market = settings.get('market', 'unknown')
        filename = f"backtest_{market}_{strategy_name}.pkl"
        return str(running_dir / filename)

    def _save_context(self) -> bool:
        """保存上下文到文件（用于模拟盘续跑）

        Returns:
            bool: 是否保存成功
        """
        try:
            filepath = self._get_context_filepath()
            if not filepath:
                return False

            # 准备保存的数据
            save_data = {
                'context': self.context,
                'trade_center_state': {
                    'account': self.trade_center.account.to_dict() if hasattr(self.trade_center.account, 'to_dict') else self.trade_center.account.__dict__,
                    'positions': {k: v.to_dict() if hasattr(v, 'to_dict') else v.__dict__
                                 for k, v in self.trade_center.positions.items()},
                    'orders': {k: v.to_dict() if hasattr(v, 'to_dict') else v.__dict__
                              for k, v in self.trade_center.orders.items()},
                    'order_id_counter': self.trade_center.order_id_counter,
                    'trade_id_counter': self.trade_center.trade_id_counter,
                },
                'saved_at': datetime.now().isoformat(),
                'saved_end_date': self.context.get('settings', {}).get('end_date'),
            }

            with open(filepath, 'wb') as f:
                pickle.dump(save_data, f)

            Log.logger.info(f"上下文已保存到: {filepath}")
            return True
        except Exception as e:
            Log.logger.error(f"保存上下文失败: {e}")
            return False

    def _load_context(self) -> Optional[Dict]:
        """加载保存的上下文

        Returns:
            Optional[Dict]: 保存的上下文数据，如果不存在则返回None
        """
        try:
            filepath = self._get_context_filepath()
            if not filepath or not os.path.exists(filepath):
                return None

            with open(filepath, 'rb') as f:
                save_data = pickle.load(f)

            Log.logger.info(f"从文件加载上下文: {filepath}")
            Log.logger.info(f"保存时间: {save_data.get('saved_at')}")
            Log.logger.info(f"保存时的结束日期: {save_data.get('saved_end_date')}")
            return save_data
        except Exception as e:
            Log.logger.error(f"加载上下文失败: {e}")
            return None

    def _restore_from_saved_context(self, saved_data: Dict) -> datetime:
        """从保存的上下文恢复状态

        Args:
            saved_data: 保存的上下文数据

        Returns:
            datetime: 恢复的开始时间（从保存的结束时间的下一天开始）
        """
        # 恢复context状态
        saved_context = saved_data['context']
        self.context.update(saved_context)

        # 恢复TradeCenter状态
        tc_state = saved_data['trade_center_state']
        if 'account' in tc_state:
            from ..models.account import Account
            self.trade_center.account = Account.from_dict(tc_state['account'])

        # 恢复positions
        from ..models.position import Position
        self.trade_center.positions = {}
        for symbol, pos_data in tc_state.get('positions', {}).items():
            self.trade_center.positions[symbol] = Position.from_dict(pos_data)

        # 恢复orders
        from ..models.order import Order
        self.trade_center.orders = {}
        for order_id, order_data in tc_state.get('orders', {}).items():
            self.trade_center.orders[order_id] = Order.from_dict(order_data)

        # 恢复活跃订单
        self.trade_center.active_orders = {
            k: v for k, v in self.trade_center.orders.items()
            if v.status not in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]
        }

        # 恢复计数器
        self.trade_center.order_id_counter = tc_state.get('order_id_counter', 1)
        self.trade_center.trade_id_counter = tc_state.get('trade_id_counter', 1)

        # 计算新的开始时间：从保存结束日期的下一天开始
        saved_end_date_str = saved_data.get('saved_end_date')
        if saved_end_date_str:
            saved_end_date = datetime.strptime(saved_end_date_str, '%Y-%m-%d %H:%M:%S')
            new_start_date = saved_end_date + timedelta(days=1)
            Log.logger.info(f"从保存的上下文恢复，新的开始日期: {new_start_date.strftime('%Y-%m-%d')}")
            return new_start_date

        return None

    def _precheck_data(self, market, frequency, universe, start_date, end_date, calendar):
        """回测前数据预校验（warning 模式：只告警，绝不阻断回测）。

        防止坏数据（残缺/缺失/异常值）静默流入回测——这是"坏数据→坏回测"的最后一道闸门。
        检查项：
          1. universe 覆盖率：codebased 有数据的标的数 / universe
          2. OHLC 合理性：抽样 high>=low>0、close>0、无空值
          3. adj_factor 对齐(cn_stock/cn_fund)：复权因子表存在且非空
          4. corporate_actions 非空(cn_stock)：跨度>60交易日时应有分红送股记录
        任何异常只 Log.logger.warning；预校验自身出错也被吞掉，确保不影响回测。
        """
        import os as _os
        tag = "[数据预校验]"
        try:
            mdir = getattr(self.data_center, 'market_data_dir', None)
            if not mdir or not _os.path.isdir(mdir):
                Log.logger.warning(f"{tag} 取不到数据目录({mdir})，跳过预校验")
                return
            # 解析年份范围
            try:
                sy = int(str(start_date)[:4]); ey = int(str(end_date)[:4])
                years = list(range(sy, ey + 1))
            except Exception:
                years = []
            risks = []

            # ---- 1. universe 覆盖率 ----
            if universe and years:
                cb_base = _os.path.join(mdir, 'kline', 'codebased', str(market), str(frequency))
                avail = set()
                for y in years:
                    yd = _os.path.join(cb_base, str(y))
                    if _os.path.isdir(yd):
                        for fn in _os.listdir(yd):
                            if fn.endswith('.csv'):
                                avail.add(fn[:-4])
                uni = set(universe)
                covered = uni & avail
                cov = len(covered) / len(uni) if uni else 1.0
                if cov < 0.5:
                    risks.append(f"universe 覆盖率仅 {cov*100:.1f}%({len(covered)}/{len(uni)})，大量标的缺 codebased {frequency} 数据 → 可能静默用空/缺数据")
                elif cov < 0.9:
                    risks.append(f"universe 覆盖率 {cov*100:.1f}%({len(covered)}/{len(uni)})，部分标的缺数据")

            # ---- 2. OHLC 合理性（抽样最近年份的若干标的）----
            if years:
                sample_dir = _os.path.join(mdir, 'kline', 'codebased', str(market), str(frequency), str(years[-1]))
                if _os.path.isdir(sample_dir):
                    import random as _r
                    files = [f for f in _os.listdir(sample_dir) if f.endswith('.csv')]
                    _r.shuffle(files)
                    bad = 0; checked = 0
                    for fn in files[:10]:
                        try:
                            with _os.open(_os.path.join(sample_dir, fn)) as fh:
                                rows = fh.read().strip().split('\n')[-200:]
                            for line in rows:
                                p = line.split(',')
                                if len(p) < 8: continue
                                try:
                                    o, h, l, c = float(p[2]), float(p[3]), float(p[4]), float(p[5])
                                except ValueError:
                                    continue
                                checked += 1
                                if not (h >= l > 0 and o > 0 and c > 0):
                                    bad += 1
                        except Exception:
                            continue
                    if checked and bad / checked > 0.001:
                        risks.append(f"OHLC 异常：抽样 {checked} 行有 {bad} 行 high<low/≤0/NaN({bad*100//checked}%)")

            # ---- 3. adj_factor 对齐(cn_stock/cn_fund) ----
            if market in ('cn_stock', 'cn_fund') and years:
                adj_name = 'cn_stock_adj.csv' if market == 'cn_stock' else 'cn_fund_adj.csv'
                adj_file = _os.path.join(mdir, 'reference', str(market), adj_name)
                if not _os.path.exists(adj_file) or _os.path.getsize(adj_file) < 100:
                    risks.append(f"复权因子表缺失或为空：{adj_file} → 复权价不正确(除权日虚假跳变)")

            # ---- 4. corporate_actions 非空(cn_stock, 跨度>60交易日) ----
            if market == 'cn_stock' and calendar and len(calendar) > 60:
                try:
                    mid = calendar[len(calendar) // 2]
                    ca = self.data_center.get_corporate_actions(mid, market)
                    if ca is None or (hasattr(ca, '__len__') and len(ca) == 0):
                        risks.append(f"corporate_actions 在 {mid.date()} 返回空 → 分红送股可能整体未加载，复权/成本基准将失真")
                except Exception as e:
                    risks.append(f"corporate_actions 查询异常：{e} → 分红数据可能不可用")

            # ---- 输出 ----
            if risks:
                Log.logger.warning(f"{tag} 发现 {len(risks)} 项数据风险(warning模式未阻断，但回测结果可信度存疑)：")
                for r in risks:
                    Log.logger.warning(f"{tag}   - {r}")
            else:
                Log.logger.info(f"{tag} 通过：universe 覆盖/adj/corp_actions/OHLC 抽样均正常")
        except Exception as e:
            Log.logger.warning(f"{tag} 预校验自身异常(已忽略，不影响回测)：{e}")

    def run_sync(self, start_date: str, end_date: str, strategy, scheduled_tasks: List):
        """运行回测 - 同步版本"""
        import time as _run_perf_time
        _run_start = _run_perf_time.perf_counter()
        Log.logger.info(f"开始回测: {start_date} -> {end_date}")

        # 保存策略引用
        self.strategy = strategy

        # 初始化全局函数
        self.initialize_global_functions()

        # 如果策略有set_order_functions函数，则调用它
        if hasattr(strategy, 'set_order_functions'):
            strategy.set_order_functions(
                globals()['order_value'],
                globals()['order_volume']
            )

        # ========== 模拟盘：检查是否有保存的上下文 ==========
        settings = self.context.get('settings', {})
        simulation_mode = settings.get('simulation_mode', False)
        context_persistence = settings.get('context_persistence', False)

        if simulation_mode and context_persistence:
            saved_data = self._load_context()
            if saved_data:
                saved_end_date_str = saved_data.get('saved_end_date')
                current_end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')

                # 如果当前结束日期 > 保存的结束日期，说明用户延长了回测时间
                if saved_end_date_str:
                    saved_end_date = datetime.strptime(saved_end_date_str, '%Y-%m-%d %H:%M:%S')
                    if current_end_date > saved_end_date:
                        Log.logger.info(f"检测到延长结束日期：{saved_end_date} -> {current_end_date}")
                        Log.logger.info("从保存的上下文恢复状态...")
                        restored_start = self._restore_from_saved_context(saved_data)
                        if restored_start:
                            start_date = restored_start.strftime('%Y-%m-%d %H:%M:%S')
                            Log.logger.info(f"新的开始日期: {start_date}")
                        else:
                            Log.logger.warning("恢复上下文失败，使用原始配置")
                    else:
                        Log.logger.info("检测到保存的上下文，但结束日期未延长，正常回测")

        # 生成交易日历
        calendar = self._generate_calendar_sync(start_date, end_date)
        Log.logger.info(f"交易日历已生成，共{len(calendar)}天")

        # 将日历存入context，供策略API（get_trading_calendar）直接使用
        self.context['calendar'] = calendar

        # 设置调度任务
        self.event_center.set_scheduled_tasks(scheduled_tasks)
        
        # 获取市场设置
        market = self.context.get('settings', {}).get('market', 'cn_stock')
        frequency = self.context.get('settings', {}).get('freq', '1d')

        # ===== 数据预校验（warning 模式）：回测前体检，坏数据只告警不阻断 =====
        try:
            self._precheck_data(
                market=market,
                frequency=frequency,
                universe=self.context.get('universe', None),
                start_date=start_date,
                end_date=end_date,
                calendar=calendar,
            )
        except Exception as _e:
            Log.logger.warning(f"[数据预校验] 预校验调用异常(已忽略，不影响回测)：{_e}")

        # 如果是1分钟频率，预加载前两个月的数据
        if frequency == '1m':
            # 检查是否启用预加载（可通过配置禁用）
            enable_preload = True
            if hasattr(self.context, 'data_config'):
                enable_preload = getattr(self.context.data_config, 'preload_data', True)

            if enable_preload:
                # 获取额外预加载月数配置
                extra_months = 3
                if hasattr(self.context, 'data_config'):
                    extra_months = getattr(self.context.data_config, 'preload_extra_months', 3)

                Log.logger.info(f"检测到1分钟频率回测，启用预加载策略（额外预加载前{extra_months}个月）")

                # 预加载第一个交易日及其前N个月的数据
                if calendar:
                    first_trade_date = calendar[0]
                    Log.logger.info(f"开始预加载初始数据: {first_trade_date}（含前{extra_months}个月）")
                    Log.logger.info(f"开始预加载 {market} 数据（含前{extra_months}个月历史数据）...")

                    # 获取universe（股票池）
                    universe = self.context.get('universe', None)

                    self.data_center.ensure_monthly_data_loaded(
                        market=market,
                        current_date=first_trade_date,
                        universe=universe,
                        frequency=frequency
                    )
                    Log.logger.info(f"初始数据预加载完成（已加载前{extra_months}个月历史数据）")
            else:
                Log.logger.info("检测到1分钟频率回测，使用按需加载策略（禁用预加载）")
                Log.logger.info("使用按需加载策略，数据将在策略需要时加载")
        
        # 记录上一次处理的月份，用于检测月份变化
        last_processed_month = None

        # 【性能诊断】启动耗时
        _startup_time = _run_perf_time.perf_counter() - _run_start
        Log.logger.info(f"[性能] 启动耗时: {_startup_time:.2f}s")
        
        # 主循环：遍历每个交易日
        # BUG 4 fix: 账户清零检测计数器
        self._zero_asset_days = 0
        self._bankruptcy_triggered = False
        _initial_capital = self.context.get('settings', {}).get('initial_capital', 1000000)

        for trade_date in calendar:
            import time as _perf_time
            _day_t0 = _perf_time.perf_counter()

            self.context['current_dt'] = trade_date
            self._sync_backtest_date(trade_date.date())
            Log.logger.debug(f"交易日: {trade_date.strftime('%Y-%m-%d')}")
            
            # 检查是否进入新的月份
            if frequency == '1m':
                current_month = f"{trade_date.year}-{trade_date.month:02d}"
                if current_month != last_processed_month:
                    Log.logger.info(f"检测到进入新月份: {current_month}")

                    # 检查是否启用预加载
                    enable_preload = True
                    if hasattr(self.context, 'data_config'):
                        enable_preload = getattr(self.context.data_config, 'preload_data', True)

                    if enable_preload:
                        extra_months = 3
                        if hasattr(self.context, 'data_config'):
                            extra_months = getattr(self.context.data_config, 'preload_extra_months', 3)
                        Log.logger.info(f"进入新月份 {current_month}，预加载本月及前{extra_months}个月数据")

                        # 预加载当前月及前N个月的数据
                        self.data_center.ensure_monthly_data_loaded(
                            market=market,
                            current_date=trade_date,
                            universe=self.context.get('universe', None),
                            frequency=frequency
                        )
                    else:
                        Log.logger.info(f"进入新月份 {current_month}，使用按需加载")

                    last_processed_month = current_month
            
            # 生成当日事件列表
            daily_events = self.event_center.generate_daily_events(trade_date.date())
            
            # 调试：打印前3个事件
            Log.logger.debug(f"生成了 {len(daily_events)} 个事件")
            for idx in range(min(3, len(daily_events))):
                e = daily_events[idx]
                Log.logger.info(f"  [{idx}] {e.event_time} ({e.event_type.value})")
                Log.logger.debug(f"[事件列表] [{idx}] {e.event_time} ({e.event_type.value})")
            
            # 按时间顺序处理事件
            i = 0
            while i < len(daily_events):
                # 即时空转检测：账户完全归零时跳过当天剩余事件
                if i == 0:
                    try:
                        acct = self.account
                        pos = self.trade_center.positions if hasattr(self.trade_center, 'positions') else {}
                        if (acct.cash_available <= 0
                            and len(pos) == 0
                            and getattr(acct, 'margin_used', 0) <= 0):
                            Log.logger.info(f"账户已完全归零（无现金、无持仓、无保证金），"
                                          f"跳过 {trade_date.date()} 剩余事件")
                            self._zero_asset_days += 1
                            if self._zero_asset_days >= 3:
                                Log.logger.warning(f"账户归零连续{self._zero_asset_days}天，回测提前终止")
                                break
                            break  # 跳过当天剩余事件
                    except Exception:
                        pass

                event = daily_events[i]

                # 【性能诊断】事件级计时
                _evt_t0 = __import__('time').perf_counter()
                self._process_event_sync(event, strategy)
                _evt_dt = __import__('time').perf_counter() - _evt_t0
                _evt_key = event.event_type.value
                if not hasattr(self, '_perf_event_timings'):
                    self._perf_event_timings = {}
                if _evt_key not in self._perf_event_timings:
                    self._perf_event_timings[_evt_key] = [0.0, 0]
                self._perf_event_timings[_evt_key][0] += _evt_dt
                self._perf_event_timings[_evt_key][1] += 1
                # 慢事件告警（>1秒）
                if _evt_dt > 1.0:
                    Log.logger.warning(f"[性能] 慢事件 {_evt_key} @ {event.event_time}: {_evt_dt:.3f}s")

                # 穿仓破产检测：强平导致负余额后立即终止
                if self._bankruptcy_triggered:
                    Log.logger.warning(f"账户穿仓破产，回测提前终止于 {trade_date.date()}")
                    break

                # 智能加速逻辑：仅在1分钟频率下生效
                frequency = self.context.get('settings', {}).get('freq', '1d')

                # 检查策略是否注册了MARKET_BAR_1M事件处理器
                strategy_has_bar_handler = (
                    hasattr(strategy, 'event_handlers') and
                    EventTypeEnum.MARKET_BAR_1M in strategy.event_handlers
                )

                # 如果没有注册1分钟K线处理器，在MARKET_BAR_1M事件时跳过
                if frequency == '1m' and not strategy_has_bar_handler:
                    if event.event_type == EventTypeEnum.MARKET_BAR_1M:
                        Log.logger.debug(f"智能加速：跳过MARKET_BAR_1M事件 {event.event_time}")
                        i += 1
                        continue
                    elif event.event_type == EventTypeEnum.TRY_MATCH:
                        # 检查是否有需要即时撮合的订单
                        has_pending_orders = self._has_pending_orders()
                        active_count = len(self.trade_center.active_orders) if hasattr(self.trade_center, 'active_orders') else 0
                        # 检查是否有需要即时撮合的市价单（排除僵尸单：remaining_volume < ZOMBIE_ORDER_THRESHOLD）
                        has_market_orders = False
                        if hasattr(self.trade_center, 'active_orders') and active_count > 0:
                            from ..models.order import OrderType
                            has_market_orders = any(
                                o.order_type == OrderType.MARKET and o.remaining_volume > ZOMBIE_ORDER_THRESHOLD
                                for o in self.trade_center.active_orders.values()
                            )
                        has_only_stale_orders = has_pending_orders and not has_market_orders

                        can_skip = not has_pending_orders or has_only_stale_orders
                        if can_skip:
                            # 无挂单或仅有限价单时，跳过所有TRY_MATCH，直达下一个ON_TIME或重要事件
                            next_event_idx = i + 1
                            important_events = [
                                EventTypeEnum.ON_TIME,
                                EventTypeEnum.MARKET_START,
                                EventTypeEnum.MORNING_END,
                                EventTypeEnum.AFTERNOON_START,
                                EventTypeEnum.CLOSING_START,
                                EventTypeEnum.MARKET_END,
                            ]
                            while next_event_idx < len(daily_events):
                                next_event = daily_events[next_event_idx]
                                if next_event.event_type in important_events:
                                    break
                                next_event_idx += 1

                            if next_event_idx > i + 1 and next_event_idx < len(daily_events):
                                target_event = daily_events[next_event_idx]
                                time_diff = (target_event.event_time - event.event_time).total_seconds() / 60

                                if self._skip_start_time is None:
                                    self._skip_start_time = event.event_time
                                self._skip_total_minutes += time_diff

                                i = next_event_idx
                                continue

                i += 1

            # 每日结束时，输出剩余的累积跳过日志
            if self._skip_total_minutes > 0:
                Log.logger.info(f"智能加速：累计跳过 {self._skip_total_minutes:.0f} 分钟 (从 {self._skip_start_time} 到交易日结束)")
                self._skip_total_minutes = 0
                self._skip_start_time = None

            # 每日结束时，输出剩余的撮合跳过日志
            if self._match_skip_count > 0:
                Log.logger.debug(f"[TRY_MATCH] 累计跳过撮合 {self._match_skip_count} 次 (从 {self._match_skip_start_time} 到交易日结束)")
                self._match_skip_count = 0
                self._match_skip_start_time = None

            # 【性能诊断】日终性能报告
            if hasattr(self, '_perf_event_timings') and self._perf_event_timings:
                _day_total = sum(v[0] for v in self._perf_event_timings.values())
                Log.logger.info(f"[性能] ===== {trade_date.date()} 性能报告 =====")
                Log.logger.info(f"[性能] 事件总耗时: {_day_total:.2f}s")
                for _k, (_total, _count) in sorted(self._perf_event_timings.items(), key=lambda x: -x[1][0]):
                    _avg = _total / _count * 1000 if _count > 0 else 0
                    Log.logger.info(f"[性能]   {_k}: total={_total:.2f}s, count={_count}, avg={_avg:.1f}ms")
                self._perf_event_timings.clear()

            # 【性能诊断】数据中心API计时报告
            if hasattr(self.data_center, '_perf_api_timings') and self.data_center._perf_api_timings:
                Log.logger.info(f"[性能] 数据API耗时:")
                for _k, (_total, _count) in sorted(self.data_center._perf_api_timings.items(), key=lambda x: -x[1][0]):
                    _avg = _total / _count * 1000 if _count > 0 else 0
                    Log.logger.info(f"[性能]   {_k}: total={_total:.2f}s, count={_count}, avg={_avg:.1f}ms")
                self.data_center._perf_api_timings.clear()

            # 穿仓破产后跳出外层循环
            if self._bankruptcy_triggered:
                break

            # 检测账户清零：总资产极低或归零状态持续，提前终止
            try:
                total_assets = self.account.total_assets
                pos = self.trade_center.positions if hasattr(self.trade_center, 'positions') else {}
                # 无持仓且总资产极低（< 0.1%初始资金）视为空转
                if total_assets < _initial_capital * 0.001 and len(pos) == 0:
                    self._zero_asset_days += 1
                    if self._zero_asset_days >= 3:
                        Log.logger.warning(f"账户资产连续{self._zero_asset_days}天极低（{total_assets:.2f}），回测提前终止")
                        break
                else:
                    self._zero_asset_days = 0
            except Exception:
                pass

            # 更新前一交易日
            self.context['previous_date'] = trade_date.date()

            # 【性能诊断】日级总耗时（含非事件开销）
            _day_total = _perf_time.perf_counter() - _day_t0
            _evt_total = sum(v[0] for v in self._perf_event_timings.values()) if hasattr(self, '_perf_event_timings') and self._perf_event_timings else 0
            Log.logger.info(f"[性能] 日总耗时: {_day_total:.2f}s (事件: {_evt_total:.2f}s, 非事件: {_day_total - _evt_total:.2f}s)")

        # 计算绩效
        self._calculate_performance_sync()

        Log.logger.info("回测完成")

        # ========== 模拟盘：保存上下文 ==========
        if simulation_mode and context_persistence:
            Log.logger.info("模拟盘模式：保存上下文状态...")
            self._save_context()

        # 返回回测结果
        return self.context['performance']
    
    def initialize_global_functions(self):
        """初始化全局函数"""
        global order_value, order_volume
        
        def order_value(security, amount, side, order_type=OrderType.MARKET, 
                         order_cost=None, slippage=None):
            """下单函数 - 按金额下单"""
            try:
                # 获取当前价格
                current_time = self.context['current_dt']
                freq = self.context['settings']['freq']
                
                # 获取价格数据
                quote_df = self.data_center.get_quotes([security], freq, current_time)
                if quote_df.empty or security not in quote_df.index:
                    Log.logger.warning(f"无法获取 {security} 的价格数据")
                    return None
                
                price = quote_df.loc[security, 'close']
                if isinstance(price, pd.Series):
                    price = float(price.iloc[-1])

                # 计算数量 - 期货需考虑合约乘数和保证金比率
                if price <= 0:
                    Log.logger.warning(f"[order_value] {security} 价格无效({price})，无法下单")
                    return None
                if self._is_cn_future() or self._is_crypto_swap():
                    contract_size = self.trade_center._get_contract_size(security)
                    margin_ratio = self.trade_center._get_margin_ratio(security)
                    if contract_size > 0 and margin_ratio > 0:
                        volume = amount / (price * contract_size * margin_ratio)
                    else:
                        volume = amount / price
                else:
                    volume = amount / price

                # 按市场规则取整（委托给trade_center的_normalize_volume）
                lot_size = self.trade_center._get_lot_size(security)
                if lot_size > 0 and side.lower() == 'buy':
                    if volume < 0:
                        Log.logger.warning(f"[order_value] {security} 计算数量为负({volume})，已拒绝下单")
                        return None
                    volume = int(volume / lot_size) * lot_size
                    if volume <= 0:
                        volume = lot_size  # 至少买入1手

                # 转换订单类型
                from finhack.trader.backtest.models.enums import Side
                order_side = Side.BUY if side.lower() == "buy" else Side.SELL
                # 支持做空方向
                if side.lower() == "short_open":
                    order_side = Side.SHORT_OPEN
                elif side.lower() == "short_close":
                    order_side = Side.SHORT_CLOSE
                
                # 调用TradeCenter下单（同步版本）
                import asyncio
                try:
                    # 尝试获取当前事件循环
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    # 如果没有事件循环，创建一个新的
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

                # 调用异步方法
                order_id = loop.run_until_complete(
                    self.trade_center.place_order(
                        adapter_id="default",
                        symbol=security,
                        side=order_side,
                        order_type=order_type,
                        volume=volume,
                        price=price
                    )
                )

                if order_id is not None:
                    current_time = self.context.get('current_dt')
                    time_str = TimeFormatter.format(current_time)
                    Log.logger.info(f"[{time_str}] 下单成功: {security} {side} {volume:.2f} @ {price:.2f}, 订单ID: {order_id}")
                return order_id

            except Exception as e:
                current_time = self.context.get('current_dt')
                time_str = TimeFormatter.format(current_time)
                Log.logger.error(f"[{time_str}] 下单失败: {e}")
                return None

        def order_volume(security, volume, side, order_type=OrderType.MARKET,
                          order_cost=None, slippage=None):
            """下单函数 - 按数量下单"""
            try:
                # 获取当前价格
                current_time = self.context['current_dt']
                freq = self.context['settings']['freq']
                
                # 获取价格数据
                quote_df = self.data_center.get_quotes([security], freq, current_time)
                if quote_df.empty or security not in quote_df.index:
                    Log.logger.warning(f"无法获取 {security} 的价格数据")
                    return None
                
                price = quote_df.loc[security, 'close']
                if isinstance(price, pd.Series):
                    price = float(price.iloc[-1])

                # 按市场规则取整（委托给trade_center的_normalize_volume）
                lot_size = self.trade_center._get_lot_size(security)
                if lot_size > 0 and side.lower() == 'buy':
                    if volume < 0:
                        Log.logger.warning(f"[order_volume] {security} 下单数量为负({volume})，已拒绝下单")
                        return None
                    volume = int(volume / lot_size) * lot_size
                    if volume <= 0:
                        volume = lot_size

                # 转换订单类型
                from finhack.trader.backtest.models.enums import Side
                order_side = Side.BUY if side.lower() == "buy" else Side.SELL
                # 支持做空方向
                if side.lower() == "short_open":
                    order_side = Side.SHORT_OPEN
                elif side.lower() == "short_close":
                    order_side = Side.SHORT_CLOSE
                
                # 调用TradeCenter下单（同步版本）
                import asyncio
                try:
                    # 尝试获取当前事件循环
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    # 如果没有事件循环，创建一个新的
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                # 调用异步方法
                order_id = loop.run_until_complete(
                    self.trade_center.place_order(
                        adapter_id="default",
                        symbol=security,
                        side=order_side,
                        order_type=order_type,
                        volume=volume,
                        price=price
                    )
                )

                if order_id is not None:
                    current_time = self.context.get('current_dt')
                    time_str = TimeFormatter.format(current_time)
                    Log.logger.info(f"[{time_str}] 下单成功: {security} {side} {volume:.2f} @ {price:.2f}, 订单ID: {order_id}")
                return order_id

            except Exception as e:
                current_time = self.context.get('current_dt')
                time_str = TimeFormatter.format(current_time)
                Log.logger.error(f"[{time_str}] 下单失败: {e}")
                return None

        # 设置全局函数
        globals()['order_value'] = order_value
        globals()['order_volume'] = order_volume
        
        # 设置全局变量'engine'
        globals()['engine'] = self
        
        # 如果策略有set_order_functions函数，则调用它
        if hasattr(self, 'strategy') and hasattr(self.strategy, 'set_order_functions'):
            self.strategy.set_order_functions(order_value, order_volume)
    
    def _generate_calendar_sync(self, start_date: str, end_date: str) -> List[datetime]:
        """生成交易日历 - 同步版本

        使用真实交易日历过滤非交易日（节假日等）
        """
        # 尝试多种日期格式
        date_formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']

        start_dt = None
        end_dt = None

        # 尝试解析开始日期
        for fmt in date_formats:
            try:
                start_dt = datetime.strptime(start_date, fmt)
                break
            except ValueError:
                continue

        # 尝试解析结束日期
        for fmt in date_formats:
            try:
                end_dt = datetime.strptime(end_date, fmt)
                break
            except ValueError:
                continue

        if start_dt is None or end_dt is None:
            raise ValueError(f"无法解析日期: start_date={start_date}, end_date={end_date}")

        # 获取市场类型
        market_name = self.context.get('settings', {}).get('market', 'cn_stock')

        # 加密货币市场是7x24交易，不跳过周末和节假日
        if market_name.startswith('global_crypto'):
            calendar = []
            current_dt = start_dt
            while current_dt <= end_dt:
                calendar.append(current_dt)
                current_dt += timedelta(days=1)
            return calendar

        # 尝试从交易日历文件加载真实交易日
        trade_dates = self._load_trade_calendar(market_name, start_dt, end_dt)

        if trade_dates:
            # 使用真实交易日历
            calendar = []
            for trade_date in trade_dates:
                dt = datetime.combine(trade_date, datetime.min.time())
                if start_dt <= dt <= end_dt:
                    calendar.append(dt)
            Log.logger.info(f"使用交易日历，共{len(calendar)}个交易日")
            return calendar
        else:
            # 回退：只过滤周末
            Log.logger.warning("未找到交易日历文件，使用简单工作日过滤")
            calendar = []
            current_dt = start_dt
            while current_dt <= end_dt:
                if current_dt.weekday() < 5:  # 跳过周末
                    calendar.append(current_dt)
                current_dt += timedelta(days=1)
            return calendar

    def _load_trade_calendar(self, market: str, start_dt: datetime, end_dt: datetime) -> List[date]:
        """从交易日历文件加载交易日列表

        Args:
            market: 市场名称
            start_dt: 开始日期
            end_dt: 结束日期

        Returns:
            交易日列表（date对象），如果加载失败返回空列表
        """
        import os

        try:
            # 尝试多个可能的路径
            ref_paths = []
            try:
                from finhack.library.data import get_data_interface
                ref_paths.append(os.path.join(get_data_interface().reference_data_dir, market, f'{market}_calendar.csv'))
            except Exception:
                pass
            possible_paths = ref_paths + [
                # demo_project 路径
                os.path.join(self.context.get('project_path', ''), 'data', 'market', 'reference', market, f'{market}_calendar.csv'),
                # cwd 路径
                os.path.join(os.getcwd(), 'demo_project', 'data', 'market', 'reference', market, f'{market}_calendar.csv'),
                # mysql_project 路径
                os.path.join(os.path.dirname(self.context.get('project_path', '')), 'mysql_project', 'data', 'market', 'reference', market, f'{market}_calendar.csv'),
            ]

            calendar_file = None
            for path in possible_paths:
                if os.path.exists(path):
                    calendar_file = path
                    break

            if not calendar_file:
                Log.logger.debug(f"未找到交易日历文件: {possible_paths[0]}")
                return []

            # 读取交易日历
            import pandas as pd
            df = pd.read_csv(calendar_file)

            # 将 cal_date 转换为整数进行比较（CSV中是int64类型）
            start_date_int = int(start_dt.strftime('%Y%m%d'))
            end_date_int = int(end_dt.strftime('%Y%m%d'))

            trade_df = df[
                (df['is_open'] == 1) &
                (df['cal_date'] >= start_date_int) &
                (df['cal_date'] <= end_date_int)
            ]

            # 期货日历包含多个交易所（CFFEX/DCE/CZCE/SHFE），交易日相同，需去重
            trade_df = trade_df.drop_duplicates(subset=['cal_date'])

            # 转换为date列表
            trade_dates = []
            for date_val in trade_df['cal_date'].values:
                trade_dates.append(datetime.strptime(str(int(date_val)), '%Y%m%d').date())

            Log.logger.info(f"从交易日历加载 {len(trade_dates)} 个交易日")
            return sorted(trade_dates)

        except Exception as e:
            Log.logger.warning(f"加载交易日历失败: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _process_event_sync(self, event: BaseEvent, strategy=None):
        """处理单个事件 - 同步版本"""
        if strategy is None:
            strategy = getattr(self, 'strategy', None)
        event_name = event.event_type.value
        event_time = event.event_time
        
        # 更新当前时间
        self.context['current_dt'] = event_time
        self._sync_backtest_date(event_time.date())

        # 添加调试日志
        if event.event_type == EventTypeEnum.TRY_MATCH:
            Log.logger.debug(f"处理撮合事件: {event_time}")
        
        # ====== 调用策略注册的事件处理器 ======
        if hasattr(strategy, 'event_handlers'):
            # 尝试通过枚举值和枚举对象本身查找处理器
            handlers = None

            # 方法1：直接用枚举对象查找
            if event.event_type in strategy.event_handlers:
                handlers = strategy.event_handlers[event.event_type]
            else:
                # 方法2：通过枚举值查找（防止不同模块的枚举对象不相等）
                for registered_event_type, registered_handlers in strategy.event_handlers.items():
                    if registered_event_type.value == event.event_type.value:
                        handlers = registered_handlers
                        break

            if handlers:
                Log.logger.debug(f"[事件分发] 调用策略事件处理器: {event.event_type.value}，共{len(handlers)}个处理器")
                for handler in handlers:
                    try:
                        handler(self.context, event)
                    except Exception as e:
                        Log.logger.error(f"调用策略事件处理器失败 ({event.event_type.value}): {e}")
                        import traceback
                        traceback.print_exc()
            else:
                Log.logger.debug(f"事件 {event.event_type.value} 没有注册处理器")
        else:
            Log.logger.warning("策略没有event_handlers属性！")
        
        # 调用策略的handle_bar函数（如果有）
        if hasattr(strategy, 'handle_bar') and event.event_type in [
            EventTypeEnum.MARKET_BAR_1D, EventTypeEnum.MARKET_BAR_1M, 
            EventTypeEnum.MARKET_BAR_30M, EventTypeEnum.MARKET_BAR_120M,
            EventTypeEnum.DAILY_BAR_CLOSED
        ]:
            try:
                # 创建bar_dict
                bar_dict = {
                    'datetime': event_time,
                    'open': 0,  # 这里可以获取实际价格
                    'high': 0,
                    'low': 0,
                    'close': 0,
                    'volume': 0
                }
                strategy.handle_bar(self.context, bar_dict)
            except Exception as e:
                Log.logger.error(f"调用策略handle_bar失败: {e}")
        
        # 根据事件类型直接处理
        if event.event_type == EventTypeEnum.ON_TIME:
            self._handle_on_time_sync(event)
        elif event.event_type == EventTypeEnum.TRY_MATCH:
            self._handle_try_match_sync(event)
        elif event.event_type == EventTypeEnum.MARKET_END:
            self._handle_market_end_sync(event)
        elif event.event_type == EventTypeEnum.DAILY_BAR_CLOSED:
            self._handle_day_end_sync(event)
        elif event.event_type == EventTypeEnum.DAY_END:
            self._handle_day_end_sync(event)
        elif event.event_type == EventTypeEnum.BEFORE_MARKET:
            self._handle_before_market_sync(event)
        elif event.event_type == EventTypeEnum.CORPORATE_ACTION:
            self._handle_corporate_action_sync(event)
        elif event.event_type.value == 'DELISTING':
            self._handle_delisting_sync(event)
        elif event.event_type == EventTypeEnum.MARGIN_CALL_CHECK:
            self._handle_margin_call_check_sync(event)
        elif event.event_type == EventTypeEnum.FUNDING_RATE_SETTLE:
            self._handle_funding_rate_settle_sync(event)
        else:
            # 其他市场事件的默认处理
            Log.logger.debug(f"市场事件 {event.event_type.value} 已处理")
    
    def _handle_on_time_sync(self, event):
        """处理定时任务事件 - 同步版本"""
        try:
            # 从上下文中查找定时任务
            if not self.context or 'scheduled_tasks' not in self.context:
                return

            function_name = getattr(event, 'function_name', None)
            if not function_name:
                Log.logger.warning("ON_TIME事件缺少function_name属性")
                return

            # 查找对应的函数对象
            func = None
            for task in self.context['scheduled_tasks']:
                if task.get('function_name') == function_name:
                    func = task.get('function_object')
                    break

            if func:
                # 调用策略函数 - 强制同步调用
                func(self.context)
                Log.logger.debug(f"执行定时任务成功: {function_name}")

                # 注意：不再立即撮合，让订单在后续的分钟级撮合事件中自然成交
                # 分钟级回测中，后续每分钟都会触发撮合事件

            else:
                Log.logger.warning(f"未找到定时任务函数: {function_name}")

        except Exception as e:
            Log.logger.error(f"执行定时任务失败: {e}")

    def _try_match_after_scheduled_task_sync(self):
        """定时任务执行后尝试撮合订单 - 同步版本

        性能优化版本:
        - 使用 TimeFormatter 缓存时间格式化
        - 使用 extract_latest_prices_batch 批量提取价格
        """
        try:
            current_time = self.context.get('current_dt')
            if not current_time:
                return

            market = self.context.get('settings', {}).get('market', 'cn_stock')
            freq = self.context.get('settings', {}).get('freq', '1d')

            # 获取所有需要行情的标的
            symbols = set()
            for order in self.trade_center.active_orders.values():
                symbols.add(order.symbol)

            if not symbols:
                return

            # 获取行情数据
            try:
                market_data = {}
                symbols_list = list(symbols)

                if freq == '1d':
                    # 日线数据批量获取
                    quote_df = self.data_center.get_quotes(symbols_list, freq=freq, time=current_time, fields=['close', 'volume'])
                    if not quote_df.empty:
                        market_data = _match_quote_symbols(quote_df, symbols_list)
                else:
                    # 分钟线数据
                    start_time = current_time - timedelta(minutes=5)
                    end_time = current_time + timedelta(minutes=1)

                    # 【优化】使用缓存的时间格式化器
                    start_time_str, end_time_str = TimeFormatter.format_range(start_time, end_time)

                    klines_df = self.data_center.get_klines(
                        codes=symbols_list,
                        freq=freq,
                        start_time=start_time_str,
                        end_time=end_time_str,
                        fields=['close', 'volume']
                    )

                    if not klines_df.empty:
                        # 【性能优化】使用批量提取函数
                        market_data = extract_latest_prices_batch(klines_df, symbols_list)

                # 执行撮合
                if market_data:
                    # 【优化】使用缓存的时间格式化器
                    time_str = TimeFormatter.format(current_time)
                    Log.logger.debug(f"[{time_str}] [定时任务后撮合] 活跃订单={len(self.trade_center.active_orders)}, 标的={list(symbols)}")
                    self.trade_center.try_match_orders_sync(market_data)

            except Exception as e:
                Log.logger.error(f"定时任务后撮合失败: {e}")

        except Exception as e:
            Log.logger.error(f"_try_match_after_scheduled_task_sync 失败: {e}")
            
    def _handle_try_match_sync(self, event):
        """处理撮合事件 - 同步版本

        性能优化版本:
        - 使用 TimeFormatter 缓存时间格式化
        - 使用 extract_latest_prices_batch 批量提取价格
        """
        # 获取当前市场数据
        current_time = self.context['current_dt']
        # 【优化】使用缓存的时间格式化器
        time_str = TimeFormatter.format(current_time)
        market = self.context['settings']['market']
        freq = self.context['settings']['freq']

        # 获取所有需要行情的标的（使用 active_orders）
        symbols = set()
        for order_id, order in self.trade_center.active_orders.items():
            symbols.add(order.symbol)

        # 如果没有活跃订单，合并日志输出
        if not symbols:
            self._match_skip_count += 1
            if self._match_skip_start_time is None:
                self._match_skip_start_time = current_time
            # 不立即输出日志，等待有订单或结束时再输出
            return

        # 如果之前有累积的跳过记录，先输出合并后的日志
        if self._match_skip_count > 0:
            # 【优化】使用缓存的时间格式化器
            start_str = TimeFormatter.format(self._match_skip_start_time)
            Log.logger.debug(f"[{time_str}] [TRY_MATCH] 累计跳过撮合 {self._match_skip_count} 次 ({start_str} -> {time_str})")
            self._match_skip_count = 0
            self._match_skip_start_time = None

        Log.logger.debug(f"[{time_str}] [TRY_MATCH] 活跃订单={len(self.trade_center.active_orders)}, 标的={list(symbols)}")

        # 获取行情数据 - 批量获取以提高性能
        try:
            market_data = {}
            symbols_list = list(symbols)

            # 批量获取行情数据而不是逐个获取
            if freq == '1m':
                # 【性能优化】优先使用快速价格查找，避免DataFrame copy
                market_data = self.data_center.get_minute_prices(
                    codes=symbols_list, market=market, freq=freq,
                    current_time=current_time
                )

                if market_data:
                    Log.logger.debug(f"[{time_str}] [TRY_MATCH] 快速查找命中: {len(market_data)}个标的")
                else:
                    # 回退到get_klines
                    start_time = current_time - timedelta(minutes=5)
                    end_time = current_time + timedelta(minutes=1)

                    start_time_str, end_time_str = TimeFormatter.format_range(start_time, end_time)
                    Log.logger.debug(f"[{time_str}] [TRY_MATCH] 查询K线: codes={len(symbols_list)}个, start={start_time_str}, end={end_time_str}")

                    klines_df = self.data_center.get_klines(
                        codes=symbols_list,
                        freq=freq,
                        start_time=start_time_str,
                        end_time=end_time_str,
                        fields=['close', 'volume']
                    )

                    Log.logger.debug(f"[{time_str}] [TRY_MATCH] get_klines返回: empty={klines_df.empty}, shape={klines_df.shape if not klines_df.empty else 'N/A'}")

                    if not klines_df.empty:
                        market_data = extract_latest_prices_batch(klines_df, symbols_list)
                        found_count = len(market_data)
                        Log.logger.debug(f"[{time_str}] [TRY_MATCH] 共找到{found_count}个标的的价格")
                        if found_count <= 3 and found_count > 0:
                            for symbol, data in list(market_data.items())[:3]:
                                Log.logger.debug(f"[{time_str}] [TRY_MATCH] 找到{symbol}价格: {data['close']}, 成交量: {data['volume']}")
            else:
                # 对于日线数据，批量获取
                quote_df = self.data_center.get_quotes(symbols_list, freq=freq, time=current_time, fields=['close', 'volume'])
                if not quote_df.empty:
                    market_data = _match_quote_symbols(quote_df, symbols_list)

            # 执行撮合
            Log.logger.debug(f"[{time_str}] [TRY_MATCH] 获取到市场数据: {len(market_data)}个标的, 数据内容: {list(market_data.items())[:3]}...")
            self.trade_center.try_match_orders_sync(market_data)

            # 期货/永续合约：撮合后立即检查保证金
            if self._is_cn_future() or self._is_crypto_swap():
                self._handle_margin_call_check_sync(event)

        except Exception as e:
            Log.logger.error(f"[{time_str}] 撮合过程中发生错误: {e}")
            import traceback
            traceback.print_exc()

    def _handle_market_end_sync(self, event):
        """处理收盘事件 - 同步版本"""
        # 取消所有未成交订单（市价单和限价单）
        # 限价单不应跨日存活：A股限价单有效期仅为当日，期货亦然
        for order_id, order in list(self.trade_center.active_orders.items()):
            # 解冻订单占用的资金
            frozen_amount = getattr(order, '_frozen_amount', 0)
            if frozen_amount > 0:
                self.trade_center.account.unfreeze_cash(frozen_amount)
                order._frozen_amount = 0
            order.status = OrderStatus.CANCELLED
            order.rejected_reason = "收盘时未成交自动撤销"
            # 从活跃订单中移除
            del self.trade_center.active_orders[order_id]
                
    def _handle_day_end_sync(self, event):
        """处理日终事件 - 同步版本"""
        try:
            current_time = self.context['current_dt']
            Log.logger.debug(f"处理日终事件: {current_time}")

            freq = self.context['settings']['freq']

            # 撤销所有未成交的市场订单（1d模式下MARKET_END事件不存在，需在此处理）
            cancelled_count = 0
            for order_id, order in list(self.trade_center.active_orders.items()):
                if order.order_type == OrderType.MARKET and order.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
                    # 解冻订单占用的资金
                    frozen_amount = getattr(order, '_frozen_amount', 0)
                    if frozen_amount > 0:
                        self.trade_center.account.unfreeze_cash(frozen_amount)
                        order._frozen_amount = 0
                    order.status = OrderStatus.CANCELLED
                    order.rejected_reason = "收盘时未成交自动撤销"
                    del self.trade_center.active_orders[order_id]
                    cancelled_count += 1
            if cancelled_count > 0:
                Log.logger.info(f"[日终] 撤销{cancelled_count}个未成交的市场订单")

            # 更新所有持仓的市值
            market = self.context['settings']['market']
            
            # 获取所有持仓的标的（需要分离多头和空头）
            # 空头仓的symbol如 IF2401.CFFEX_SHORT，需要提取base code来查行情
            position_keys = list(self.trade_center.positions.keys())
            quote_symbols = []
            for pos_key in position_keys:
                if pos_key.endswith(SHORT_POSITION_SUFFIX):
                    quote_symbols.append(pos_key[:-len(SHORT_POSITION_SUFFIX)])  # 去掉 _SHORT 后缀
                else:
                    quote_symbols.append(pos_key)

            if quote_symbols:
                try:
                    # 获取最新价格（get_quotes 已内置向前回溯逻辑）
                    quote_df = self.data_center.get_quotes(quote_symbols, freq, current_time)
                    if not quote_df.empty:
                        matched = _match_quote_symbols(quote_df, quote_symbols)
                    else:
                        matched = {}

                    for pos_key in position_keys:
                        # 确定用于查行情的symbol
                        if pos_key.endswith(SHORT_POSITION_SUFFIX):
                            quote_key = pos_key[:-len(SHORT_POSITION_SUFFIX)]
                        else:
                            quote_key = pos_key

                        position = self.trade_center.positions[pos_key]

                        # 获取最新价格，三级fallback：报价数据 → last_price → cost_price
                        if quote_key in matched:
                            latest_price = matched[quote_key]['close']
                            # 确保latest_price是标量，而非Series
                            if isinstance(latest_price, pd.Series):
                                latest_price = float(latest_price.iloc[-1])
                            # 防御性检查：价格为0或异常
                            if latest_price <= 0:
                                Log.logger.warning(f"[日终] {quote_key} 价格异常({latest_price})，使用fallback")
                                latest_price = getattr(position, 'last_price', None) or position.cost_price
                        else:
                            # 报价数据缺失，使用上一次价格或成本价
                            latest_price = getattr(position, 'last_price', None) or position.cost_price
                            Log.logger.debug(f"[日终] {quote_key} 无报价数据，使用{'last_price' if hasattr(position, 'last_price') and position.last_price else 'cost_price'}: {latest_price:.4f}")

                        if latest_price and latest_price > 0:
                            # 使用 Position 模型的 update_market_price() 方法
                            # 该方法已正确处理 contract_multiplier 和多空方向
                            position.update_market_price(latest_price)
                            Log.logger.debug(f"更新持仓市值: {pos_key} 数量:{position.volume} 价格:{latest_price} 乘数:{position.contract_multiplier} 市值:{position.market_value}")
                except Exception as e:
                    Log.logger.warning(f"更新持仓市值失败: {e}")

            # 保证金市场：基于当前价格重算保证金占用
            # 注意：margin_used 已在 _update_account_value 中实时重算，
            # 此处只需在持仓重定价后执行强平检查
            if self._is_cn_future() or self._is_crypto_swap():
                # 日终保证金检查：持仓重定价后检查是否需要强平
                self._handle_margin_call_check_sync(event)

            # 更新账户价值
            self.trade_center._update_account_value()
            
            # 调试信息
            Log.logger.debug(f"调试 - 更新后账户总资产: {self.trade_center.account.total_assets:.2f}")
            Log.logger.debug(f"调试 - 更新后持仓市值: {self.trade_center.account.market_value:.2f}")
            Log.logger.debug(f"调试 - 更新后现金: {self.trade_center.account.cash_available:.2f}")
            
            # 记录每日净值
            daily_record = {
                'date': current_time.strftime('%Y-%m-%d'),
                'total_assets': self.trade_center.account.total_assets,
                'cash': self.trade_center.account.cash_available,
                'cash_frozen': self.trade_center.account.cash_frozen,
                'margin_used': getattr(self.trade_center.account, 'margin_used', 0.0),
                'positions_value': self.trade_center.account.market_value,
                'pnl_realized': self.trade_center.account.pnl_realized,
                'pnl_unrealized': sum(pos.unrealized_pnl for pos in self.trade_center.positions.values()),
                'pnl_funding': self.trade_center.account.pnl_funding
            }
            self.context['logs']['daily_history'].append(daily_record)

            Log.logger.debug(f"记录每日净值: {daily_record['date']}, 总资产: {daily_record['total_assets']:.2f}")

            # 打印每日资产情况
            # 对账关系: 总资产 = 可用现金 + 冻结资金 + 保证金占用 + 持仓市值
            #   非保证金市场: 保证金占用=0, 持仓市值=持仓市价
            #   保证金市场:   持仓市值=浮动盈亏(开仓只扣保证金未扣全额)
            # 盈亏归因: 总资产 - 起始资金 ≈ 已实现盈亏 + 未实现盈亏 + 资金费 - 累计手续费(开/平仓)
            Log.logger.info(f"日期: {daily_record['date']}, 总资产: {daily_record['total_assets']:.2f}, "
                           f"可用现金: {daily_record['cash']:.2f}, 冻结资金: {daily_record['cash_frozen']:.2f}, 保证金占用: {daily_record['margin_used']:.2f}, 持仓市值: {daily_record['positions_value']:.2f}, "
                           f"已实现盈亏: {daily_record['pnl_realized']:.2f}, 未实现盈亏: {daily_record['pnl_unrealized']:.2f}, 资金费: {daily_record['pnl_funding']:.2f}")
            
        except Exception as e:
            Log.logger.error(f"处理日终事件失败: {e}")
            
    def _handle_before_market_sync(self, event):
        """处理盘前事件 - 同步版本

        注意：分红送股等公司行为由 CORPORATE_ACTION 事件统一处理，
        不在此处处理，以避免重复计算。
        """
        try:
            current_time = self.context['current_dt']
            current_date = current_time.date()

            Log.logger.debug(f"[盘前事件] {current_date} 盘前准备 - 当前账户: 现金={self.trade_center.account.cash_available:.2f}, "
                           f"持仓市值={sum(p.market_value for p in self.trade_center.positions.values()):.2f}, "
                           f"持仓数={len(self.trade_center.positions)}")

            # 期货合约到期检查（在T+1解冻和市值更新之前执行）
            self.trade_center.check_futures_expiry()

            # T+1规则：日始时解冻昨日买入的持仓
            for symbol, position in list(self.trade_center.positions.items()):
                if not self.trade_center._is_t_plus_one(symbol):
                    continue  # T+0产品跳过解冻逻辑
                if hasattr(position, 'buy_dates') and position.buy_dates:
                    # 解冻昨日及之前买入的持仓
                    newly_available = 0.0
                    remaining_buy_dates = []
                    for buy_time, buy_volume in position.buy_dates:
                        if buy_time.date() < current_date:
                            # 昨日及之前买入的，解冻
                            newly_available += buy_volume
                        else:
                            # 今日买入的，保持冻结
                            remaining_buy_dates.append((buy_time, buy_volume))

                    if newly_available > 0:
                        position.available_volume += newly_available
                        position.buy_dates = remaining_buy_dates
                        Log.logger.info(f"[T+1解冻] {symbol}: 解冻{newly_available}股, 可用{position.available_volume}股, 冻结{position.frozen_volume}股")

            # 更新持仓市值（使用上一交易日收盘价，因为当前日收盘价在盘前还不可用）
            freq = self.context.get('settings', {}).get('freq', '1d')
            previous_date = self.context.get('previous_date')

            # 【性能优化】批量获取所有持仓标的的报价，替代逐个调用get_quotes
            positions_items = list(self.trade_center.positions.items())
            if positions_items:
                # 收集所有需要查询的quote_symbol，同时记录映射关系
                quote_symbols = []
                quote_to_pos = []  # [(pos_key, position, quote_symbol)]
                for pos_key, position in positions_items:
                    quote_symbol = pos_key[:-len(SHORT_POSITION_SUFFIX)] if pos_key.endswith(SHORT_POSITION_SUFFIX) else pos_key
                    quote_symbols.append(quote_symbol)
                    quote_to_pos.append((pos_key, position, quote_symbol))

                # 去重查询（同一标的可能有多空头）
                unique_symbols = list(dict.fromkeys(quote_symbols))
                try:
                    all_quotes = self.data_center.get_quotes(unique_symbols, freq, current_time)
                except Exception as e:
                    Log.logger.warning(f"批量获取盘前报价失败: {e}")
                    all_quotes = pd.DataFrame()

                # 逐持仓更新市值
                for pos_key, position, quote_symbol in quote_to_pos:
                    try:
                        if not all_quotes.empty and quote_symbol in all_quotes.index:
                            current_price = all_quotes.loc[quote_symbol, 'close']
                            if isinstance(current_price, pd.Series):
                                current_price = float(current_price.iloc[-1])
                            if current_price <= 0:
                                Log.logger.warning(f"[盘前] {quote_symbol} 价格异常({current_price})，使用fallback")
                                current_price = getattr(position, 'last_price', None) or position.cost_price
                        else:
                            current_price = getattr(position, 'last_price', None) or position.cost_price
                            Log.logger.debug(f"[盘前] {quote_symbol} 无报价数据，使用fallback价格: {current_price:.4f}")

                        if current_price and current_price > 0:
                            position.update_market_price(current_price)
                    except Exception as e:
                        Log.logger.warning(f"更新{pos_key}持仓市值失败: {e}")

            # 更新账户总资产
            self.trade_center._update_account_value()

        except Exception as e:
            Log.logger.error(f"处理盘前事件失败: {e}")
            import traceback
            traceback.print_exc()

    def _handle_funding_rate_settle_sync(self, event):
        """处理资金费率结算事件 - 每8小时从持仓中收取/支付funding rate

        仅对 global_cryptoswap 市场生效。
        正费率时多头付空头，负费率时空头付多头。
        结算后紧接的 MARGIN_CALL_CHECK 会反映扣除后的真实权益。
        """
        if not self._is_crypto_swap():
            return

        settings = self.context.get('settings', {})
        funding_mode = settings.get('funding_rate_mode', 'fixed')
        if funding_mode == 'none':
            return

        current_time = self.context.get('current_dt')
        if not current_time:
            return

        total_paid = 0.0
        total_received = 0.0
        funding_rate = 0.0

        for pos_key in list(self.trade_center.positions.keys()):
            position = self.trade_center.positions.get(pos_key)
            if not position or position.volume <= 0:
                continue

            current_price = getattr(position, 'last_price', None) or position.cost_price
            if not current_price or current_price <= 0:
                continue

            # 持仓价值 = 数量 × 当前价 × 合约乘数
            contract_multiplier = getattr(position, 'contract_multiplier', 1)
            position_value = position.volume * current_price * contract_multiplier

            # 确定费率
            if funding_mode == 'fixed':
                funding_rate = settings.get('funding_rate', 0.0001)
            elif funding_mode == 'dynamic':
                funding_rate = self._calculate_dynamic_funding_rate(
                    position.symbol, current_price, current_time)
            else:
                continue

            if funding_rate == 0.0:
                continue

            # 计算资金费: |费率| × 持仓价值
            funding_fee = position_value * abs(funding_rate)

            # 正费率: 多头支付，空头收取
            # 负费率: 空头支付，多头收取
            if position.is_long:
                if funding_rate > 0:
                    self.trade_center.account.cash_available -= funding_fee
                    total_paid += funding_fee
                else:
                    self.trade_center.account.cash_available += funding_fee
                    total_received += funding_fee
            else:
                if funding_rate > 0:
                    self.trade_center.account.cash_available += funding_fee
                    total_received += funding_fee
                else:
                    self.trade_center.account.cash_available -= funding_fee
                    total_paid += funding_fee

        # 记录结算日志
        if total_paid > 0 or total_received > 0:
            self.trade_center._update_account_value()
            net = total_received - total_paid
            # 资金费已计入 cash_available(并经_update_account_value反映到总资产),
            # 此处单列累计到 pnl_funding, 使盈亏归因可对账(已实现+未实现+资金费 ≈ 总资产-起始),
            # 不并入 pnl_realized 以保留"交易盈亏"与"资金费收入"的区分。
            self.trade_center.account.pnl_funding += net
            Log.logger.info(
                f"[资金费率结算] 支付={total_paid:.4f}, 收取={total_received:.4f}, "
                f"净额={net:.4f}, 费率={funding_rate:.6f}, 累计资金费={self.trade_center.account.pnl_funding:.4f}"
            )

    def _calculate_dynamic_funding_rate(self, symbol, current_price, current_time):
        """动态计算资金费率（使用已有的 CryptoFundingRateCalculator）

        注意: 回测中 mark_price 与 index_price 通常相同（单一数据源），
        因此动态模式下费率可能接近0。如需精确动态费率，需要独立的
        mark/index 价格数据。推荐使用 fixed 模式。
        """
        try:
            from ..markets.global_cryptospot.crypto_calculator import CryptoFundingRateCalculator
            result = CryptoFundingRateCalculator.calculate_funding_rate(
                symbol=symbol,
                mark_price=current_price,
                index_price=current_price,
                query_date=current_time.date()
            )
            return result.get('funding_rate', 0.0)
        except Exception as e:
            Log.logger.debug(f"动态资金费率计算失败 {symbol}: {e}")
            return 0.0

    def _handle_margin_call_check_sync(self, event):
        """处理保证金检查事件 - 检查是否需要强制平仓

        对 cn_future 和 global_cryptoswap 市场生效。
        遍历所有持仓，计算维持保证金/强平价，
        如果触发条件则强制平仓。
        """
        if not self._is_cn_future() and not self._is_crypto_swap():
            return

        current_time = self.context.get('current_dt')
        if not current_time:
            return

        if self._is_cn_future():
            self._check_futures_margin(current_time)
        elif self._is_crypto_swap():
            self._check_crypto_swap_margin(current_time)

    def _check_futures_margin(self, current_time):
        """期货保证金检查（cn_future）"""
        from ..markets.cn_future.future_calculator import FutureMarginCalculator

        for pos_key in list(self.trade_center.positions.keys()):
            position = self.trade_center.positions.get(pos_key)
            if not position or position.volume <= 0:
                continue

            symbol = position.symbol
            current_price = getattr(position, 'last_price', None) or position.cost_price
            if not current_price or current_price <= 0:
                continue

            position_signed = position.volume if position.is_long else -position.volume

            try:
                # 使用 cash_available 作为账户余额
                # check_margin_call 内部计算: current_equity = account_balance + unrealized_pnl
                # 即: current_equity = cash_available + unrealized_pnl
                # 这等价于交易所的"可用余额 + 浮动盈亏"概念，是正确且稳健的做法
                # 注：不能用 cash_available + margin_used(重算后) 或 total_assets，
                # 因为 Issue 1 的 margin_used 实时重算会导致 cash_available + margin_used ≠ total_cash，
                # 引入 error = unrealized_pnl × margin_ratio，对亏损仓位造成级联强平
                account_balance = self.trade_center.account.cash_available

                margin_result = FutureMarginCalculator.check_margin_call(
                    symbol=symbol,
                    position=position_signed,
                    entry_price=position.cost_price,
                    current_price=current_price,
                    account_balance=account_balance,
                    query_date=current_time.date()
                )
            except Exception as e:
                Log.logger.warning(f"[保证金检查] {symbol} 计算失败: {e}")
                continue

            if margin_result['need_margin_call']:
                self._force_liquidate_position(pos_key, position, current_price, current_time, margin_result)

    def _check_crypto_swap_margin(self, current_time):
        """加密货币永续合约保证金检查（global_cryptoswap）"""
        from ..markets.global_cryptospot.crypto_calculator import CryptoLiquidationCalculator

        for pos_key in list(self.trade_center.positions.keys()):
            position = self.trade_center.positions.get(pos_key)
            if not position or position.volume <= 0:
                continue

            symbol = position.symbol
            current_price = getattr(position, 'last_price', None) or position.cost_price
            if not current_price or current_price <= 0:
                continue

            # 获取杠杆信息
            leverage = 10
            if hasattr(self, 'trade_center') and self.trade_center.market_adapter and hasattr(self.trade_center.market_adapter, 'get_leverage_info'):
                try:
                    leverage_info = self.trade_center.market_adapter.get_leverage_info(symbol)
                    leverage = leverage_info.get('leverage', leverage)
                except Exception:
                    pass

            # 计算强平价（crypto swap 市场中所有交易对都视为期货）
            position_side = 'long' if position.is_long else 'short'
            try:
                liq_result = CryptoLiquidationCalculator.calculate_liquidation_price(
                    symbol=symbol,
                    entry_price=position.cost_price,
                    leverage=leverage,
                    position_side=position_side,
                    query_date=current_time.date()
                )
                liq_price = liq_result.get('liquidation_price')
            except Exception as e:
                Log.logger.warning(f"[保证金检查] {symbol} 强平价计算失败: {e}")
                liq_price = None

            # 如果计算器返回None（因pair_type被识别为spot），手动计算
            if liq_price is None:
                mmr = 0.005  # 默认维持保证金率
                if position_side == 'long':
                    liq_price = position.cost_price * (1 - 1.0 / leverage + mmr)
                else:
                    liq_price = position.cost_price * (1 + 1.0 / leverage - mmr)

            # 判断是否触发强平
            need_liquidation = False
            if position.is_long and current_price <= liq_price:
                need_liquidation = True
            elif not position.is_long and current_price >= liq_price:
                need_liquidation = True

            if need_liquidation:
                # 直接使用账户总资产（已在 _update_account_value 中正确计算）
                total_equity = self.trade_center.account.total_assets
                contract_value = abs(position.volume) * current_price * getattr(position, 'contract_multiplier', 1)
                maintenance_margin = contract_value * 0.005  # 默认维持保证金率

                margin_result = {
                    'need_margin_call': True,
                    'current_equity': total_equity,
                    'maintenance_margin': maintenance_margin,
                    'shortfall': max(0, maintenance_margin - total_equity),
                }
                Log.logger.warning(
                    f"[强制平仓] {symbol} {'多头' if position.is_long else '空头'}触发强平: "
                    f"当前价={current_price:.6g}, 强平价={liq_price:.6g}, "
                    f"权益={total_equity:.2f}, 维持保证金={maintenance_margin:.2f}, "
                    f"杠杆={leverage}x"
                )
                self._force_liquidate_position(pos_key, position, current_price, current_time, margin_result)

        # 账户级资金检查：遍历完所有持仓后，如果现金仍为负且总权益为正，
        # 说明资金费率等扣除导致现金缺口，需要主动平仓弥补
        cash_avail = self.trade_center.account.cash_available
        total_assets = getattr(self.trade_center.account, 'total_assets', 0)
        if cash_avail < -1.0 and total_assets > 0:
            Log.logger.warning(
                f"[账户资金检查] 遍历持仓后现金={cash_avail:.2f}，总权益={total_assets:.2f}，"
                f"主动平仓弥补缺口"
            )
            self._liquidate_to_cover_deficit(current_time)

    def _force_liquidate_position(self, pos_key, position, current_price, current_time, margin_result):
        """强制平仓（保证金不足时）

        复用 check_futures_expiry 的模式：创建Trade，更新持仓和账户，删除持仓。
        """
        symbol = position.symbol
        volume = position.volume

        if volume <= 0 or current_price <= 0:
            return

        if position.is_long:
            side = Side.SELL
            side_desc = "卖出平仓"
        else:
            side = Side.SHORT_CLOSE
            side_desc = "平空仓"

        Log.logger.warning(
            f"[强制平仓] {symbol} {side_desc}: "
            f"权益={margin_result.get('current_equity', 0):.2f}, "
            f"维持保证金={margin_result.get('maintenance_margin', 0):.2f}, "
            f"平仓 {volume} @{current_price:.6g}"
        )

        # 创建平仓Trade（绕过撮合直接执行）
        trade = Trade(
            trade_id=f"margin_{pos_key}_{int(current_time.timestamp())}",
            order_id=f"margin_order_{pos_key}_{int(current_time.timestamp())}",
            account_id=self.context.get('account', {}).get('account_id', 'default'),
            symbol=symbol,
            side=side,
            volume=volume,
            price=current_price,
            amount=volume * current_price * getattr(position, 'contract_multiplier', 1),
            commission=0,
            tax=0,
            trade_time=current_time,
        )

        position.update_market_price(current_price)
        total_cost = 0.0
        sell_cost_price = self.trade_center._update_position_sync(trade, total_cost)
        self.trade_center._update_account_sync(trade, total_cost, sell_cost_price)

        if pos_key in self.trade_center.positions:
            del self.trade_center.positions[pos_key]

        # 穿仓处理：现金为负数时，清算所有剩余持仓并终止回测
        # 修复：增加浮点容差和总权益检查，避免浮点精度导致盈利账户被误判为破产
        cash_avail = self.trade_center.account.cash_available
        total_assets = getattr(self.trade_center.account, 'total_assets', 0)
        FLOAT_TOLERANCE = 1.0  # 浮点容差，1元以内视为0
        # 只有当现金显著为负（超过容差）且总权益也为负时，才判定为真正穿仓
        is_truly_bankrupt = (cash_avail < -FLOAT_TOLERANCE) and (total_assets < -FLOAT_TOLERANCE)
        if cash_avail < 0 and is_truly_bankrupt:
            if not getattr(self, '_bankruptcy_triggered', False):
                # 先设置标志，防止递归调用重复进入清算循环
                self._bankruptcy_triggered = True

                # 首次穿仓：平掉所有剩余持仓
                Log.logger.warning(
                    f"[穿仓] {symbol} 强平后现金={self.trade_center.account.cash_available:.2f}，"
                    f"开始清算所有剩余持仓"
                )
                for remaining_key in list(self.trade_center.positions.keys()):
                    remaining_pos = self.trade_center.positions.get(remaining_key)
                    if not remaining_pos or remaining_pos.volume <= 0:
                        continue
                    remaining_price = getattr(remaining_pos, 'last_price', None) or remaining_pos.cost_price
                    if not remaining_price or remaining_price <= 0:
                        continue
                    self._force_liquidate_position(
                        remaining_key, remaining_pos, remaining_price, current_time,
                        {'current_equity': 0, 'maintenance_margin': 0, 'shortfall': 0}
                    )

                # 修正 margin_used（所有持仓已平）
                self.trade_center.account.margin_used = max(0.0, self.trade_center.account.margin_used)

                Log.logger.warning(
                    f"[穿仓] 清算完成，最终现金={self.trade_center.account.cash_available:.2f}"
                )
                self.trade_center._update_account_value()
        elif cash_avail < 0:
            # 现金为负但总权益为正
            if abs(cash_avail) <= FLOAT_TOLERANCE:
                # 微小浮点误差，修正为0
                Log.logger.debug(
                    f"[浮点修正] {symbol} 强平后现金微负({cash_avail:.2f})，"
                    f"总权益={total_assets:.2f}，修正现金为0"
                )
                self.trade_center.account.cash_available = 0.0
                if hasattr(self.trade_center.account, 'cash'):
                    self.trade_center.account.cash = max(0.0, getattr(self.trade_center.account, 'cash', 0))
            else:
                # 现金显著为负但总权益为正（如资金费率持续扣除导致），
                # 需要平仓来弥补资金缺口，而非简单归零
                Log.logger.warning(
                    f"[账户资金不足] {symbol} 强平后现金={cash_avail:.2f}，"
                    f"总权益={total_assets:.2f}，开始平仓弥补资金缺口"
                )
                self._liquidate_to_cover_deficit(current_time)

        self.trade_center._update_account_value()

        Log.logger.info(f"[强制平仓] {symbol} 已{side_desc}: {volume}手 @{current_price:.6g}")

    def _liquidate_to_cover_deficit(self, current_time):
        """账户现金为负但总权益为正时，按未实现盈亏从差到好依次平仓，直到现金回正

        适用于 global_cryptoswap 等保证金市场，资金费率持续扣除可能导致
        单个持仓未触发强平但账户现金已经为负的场景。
        """
        cash_avail = self.trade_center.account.cash_available
        if cash_avail >= 0:
            return

        # 按未实现盈亏排序（最差的先平）
        positions_info = []
        for pos_key in list(self.trade_center.positions.keys()):
            pos = self.trade_center.positions.get(pos_key)
            if not pos or pos.volume <= 0:
                continue
            price = getattr(pos, 'last_price', None) or pos.cost_price
            if not price or price <= 0:
                continue
            unrealized = (price - pos.cost_price) * pos.volume * getattr(pos, 'contract_multiplier', 1)
            if not pos.is_long:
                unrealized = -unrealized
            positions_info.append((pos_key, pos, price, unrealized))

        # 按未实现盈亏升序排列（亏损最多的先平）
        positions_info.sort(key=lambda x: x[3])

        for pos_key, pos, price, unrealized in positions_info:
            cash_avail = self.trade_center.account.cash_available
            if cash_avail >= -1.0:
                break

            Log.logger.warning(
                f"[账户资金弥补] 平仓 {pos.symbol} 未实现盈亏={unrealized:.2f}，"
                f"当前现金={cash_avail:.2f}"
            )
            margin_result = {
                'current_equity': self.trade_center.account.total_assets,
                'maintenance_margin': 0,
                'shortfall': abs(cash_avail),
            }
            self._force_liquidate_position(pos_key, pos, price, current_time, margin_result)

        # 最终检查：如果平仓后现金仍为负，修正为0（剩余微小误差）
        final_cash = self.trade_center.account.cash_available
        if final_cash < 0 and final_cash > -1.0:
            self.trade_center.account.cash_available = 0.0
            if hasattr(self.trade_center.account, 'cash'):
                self.trade_center.account.cash = max(0.0, getattr(self.trade_center.account, 'cash', 0))

    def _handle_corporate_action_sync(self, event):
        """处理公司行为事件 - 同步版本

        只处理与持仓相关的公司行为，非持仓股票的公司行为不会输出日志。
        """
        try:
            # 获取基本信息（需要先获取symbol来检查持仓）
            if hasattr(event, 'data') and event.data:
                symbol = event.data.get('symbol', '')
                action_type = event.data.get('action_type', '')
            else:
                symbol = getattr(event, 'symbol', '')
                action_type = getattr(event, 'action_type', '')

            # 先检查是否在持仓中，不在持仓中则直接返回（不处理也不输出日志）
            if not symbol or symbol not in self.trade_center.positions:
                return

            # 调用TradeCenter处理公司行为事件
            self.trade_center.handle_corporate_action(event)

            # 记录事件 - 构建详细的日志信息
            log_parts = []

            log_parts.append(f"{symbol}")

            # 根据事件类型添加详细信息
            action_type_lower = action_type.lower() if action_type else ''

            if action_type_lower == 'dividend':
                # 分红事件
                dividend = getattr(event, 'dividend_per_share', None) or (event.data.get('dividend_per_share') if hasattr(event, 'data') else 0)
                if dividend:
                    log_parts.append(f"现金分红 每股{float(dividend):.4f}元")

            elif action_type_lower == 'bonus':
                # 送股事件
                ratio = getattr(event, 'bonus_ratio', None) or (event.data.get('bonus_ratio') if hasattr(event, 'data') else 0)
                if ratio:
                    # 转换显示格式：0.1 -> 10送1
                    log_parts.append(f"送股 每10股送{float(ratio)*10:.0f}股")

            elif action_type_lower == 'dividend_bonus':
                # 分红送股事件
                dividend = getattr(event, 'dividend_per_share', None) or (event.data.get('dividend_per_share') if hasattr(event, 'data') else 0)
                ratio = getattr(event, 'bonus_ratio', None) or (event.data.get('bonus_ratio') if hasattr(event, 'data') else 0)
                parts = []
                if dividend:
                    parts.append(f"分红{float(dividend):.4f}元")
                if ratio:
                    parts.append(f"送{float(ratio)*10:.0f}股")
                if parts:
                    log_parts.append(f"10股: {' '.join(parts)}")

            elif action_type_lower == 'transfer':
                # 转增事件
                ratio = getattr(event, 'transfer_ratio', None) or (event.data.get('transfer_ratio') if hasattr(event, 'data') else 0)
                if ratio:
                    log_parts.append(f"转增 每10股转增{float(ratio)*10:.0f}股")

            elif action_type_lower == 'split':
                # 拆股事件
                ratio = getattr(event, 'split_ratio', None) or (event.data.get('split_ratio') if hasattr(event, 'data') else 0)
                if ratio:
                    # 转换显示格式：2 -> 1拆2
                    log_parts.append(f"拆股 1拆{float(ratio):.0f}")

            elif action_type_lower == 'rights':
                # 配股事件
                ratio = getattr(event, 'rights_ratio', None) or (event.data.get('rights_ratio') if hasattr(event, 'data') else 0)
                price = getattr(event, 'rights_price', None) or (event.data.get('rights_price') if hasattr(event, 'data') else 0)
                if ratio:
                    log_parts.append(f"配股 每10股配{float(ratio)*10:.0f}股 配股价{float(price):.2f}元")

            else:
                # 其他类型或未知类型
                log_parts.append(f"{action_type}")

            # 获取除权除息日期
            ex_date = getattr(event, 'ex_date', None) or (event.data.get('ex_date') if hasattr(event, 'data') else None)
            if ex_date:
                log_parts.append(f"除权除息日{ex_date}")

            Log.logger.info(f"公司行为: {' | '.join(log_parts)}")

        except Exception as e:
            Log.logger.error(f"处理公司行为事件失败: {e}")

    def _handle_delisting_sync(self, event):
        """处理退市事件 - 强制清算持仓"""
        try:
            # 获取symbol
            symbol = getattr(event, 'symbol', None) or ''
            if not symbol and hasattr(event, 'data') and event.data:
                symbol = event.data.get('symbol', '')

            if not symbol:
                return

            delisting_date = getattr(event, 'delisting_date', None)
            delisting_reason = getattr(event, 'delisting_reason', '退市')
            if hasattr(event, 'data') and event.data:
                if not delisting_date:
                    delisting_date = event.data.get('delisting_date')
                if delisting_reason == '退市':
                    delisting_reason = event.data.get('delisting_reason', '退市')

            Log.logger.info(f"[退市处理] {symbol} 退市日期: {delisting_date}, 原因: {delisting_reason}")

            # 更新交易状态为退市
            data_center = getattr(self, 'data_center', None)
            if data_center and hasattr(data_center, 'update_trading_status'):
                data_center.update_trading_status(symbol, 'delisted', delisting_reason)

            # 强制清算持仓
            positions = self.trade_center.positions if hasattr(self, 'trade_center') else {}
            if symbol in positions:
                position = positions[symbol]
                shares = position.volume

                if shares > 0:
                    # 使用最后价格或清算价格
                    last_price = position.last_price
                    liquidation_price = getattr(event, 'liquidation_price', 0)
                    if liquidation_price and liquidation_price > 0:
                        last_price = liquidation_price

                    # 退市价可能为0（停牌/无数据），尝试获取退市前最后交易日的收盘价
                    if last_price <= 0:
                        try:
                            market = self.context.get('settings', {}).get('market', 'cn_stock')
                            last_quotes = self.data_center.get_klines(
                                codes=symbol, market=market, freq='1d',
                                start_date=None, end_date=None,
                                fields=['close'], backtest_time=None
                            )
                            if last_quotes is not None and not last_quotes.empty:
                                # 取最后一根有收盘价的K线
                                close_col = 'close' if 'close' in last_quotes.columns else last_quotes.columns[-1]
                                valid_closes = last_quotes[last_quotes[close_col] > 0][close_col]
                                if not valid_closes.empty:
                                    last_price = float(valid_closes.iloc[-1])
                                    Log.logger.info(f"[退市处理] {symbol} 从历史K线获取最后有效价格: {last_price:.4f}")
                        except Exception as e:
                            Log.logger.debug(f"[退市处理] {symbol} 获取历史价格失败: {e}")

                    # 如果仍然为0，使用持仓成本价作为兜底
                    if last_price <= 0 and hasattr(position, 'cost_price') and position.cost_price > 0:
                        last_price = position.cost_price
                        Log.logger.warning(f"[退市处理] {symbol} 无有效市场价格，使用成本价 {last_price:.4f} 作为清算价")

                    if last_price <= 0:
                        Log.logger.warning(f"[退市处理] {symbol} 无法获取有效清算价格，清算价值为0")

                    liquidation_value = shares * last_price

                    # 更新现金
                    if hasattr(self, 'trade_center'):
                        self.trade_center.account.cash_available += liquidation_value
                    else:
                        self.account.cash_available += liquidation_value

                    del positions[symbol]

                    Log.logger.info(f"[退市处理] 强制清仓: {symbol}, 数量: {shares}, "
                                   f"清算价格: {last_price:.4f}, 清算价值: {liquidation_value:.2f}")
            else:
                Log.logger.debug(f"[退市处理] {symbol} 不在持仓中，无需清仓")

        except Exception as e:
            Log.logger.error(f"处理退市事件失败: {e}")
            
    def _calculate_performance_sync(self):
        """计算绩效指标 - 同步版本"""
        Log.logger.info("开始计算绩效指标")

        daily_history = self.context['logs']['daily_history']
        Log.logger.info(f"每日历史记录数量: {len(daily_history)}")

        if len(daily_history) < 2:
            Log.logger.warning("每日历史记录不足，无法计算绩效指标")
            return

        # BUG 5 fix: 使用 initial_capital 作为净值基准
        initial_capital = self.context.get('settings', {}).get('initial_capital', 1000000)

        # 构建完整净值序列：[initial_capital, day1_assets, day2_assets, ...]
        nav_values = [initial_capital] + [dh['total_assets'] for dh in daily_history]

        # 计算日收益率（基于完整净值序列，含首日相对初始资金的变化）
        returns = []
        for i in range(1, len(nav_values)):
            prev_value = nav_values[i-1]
            curr_value = nav_values[i]
            # 防御性检查：避免除以0或负数
            if prev_value > 0:
                daily_return = (curr_value - prev_value) / prev_value
            else:
                Log.logger.warning(f"第{i-1}天总资产异常({prev_value:.2f})，无法计算收益率")
                daily_return = 0.0
            returns.append(daily_return)

        self.context['performance']['returns'] = returns

        # 计算基本统计指标
        if returns:
            import numpy as np
            returns_array = np.array(returns)

            # BUG 5 fix: 总收益率使用 initial_capital 作为基准
            total_return = (daily_history[-1]['total_assets'] / initial_capital) - 1
            # 确保是实数
            if isinstance(total_return, complex):
                total_return = total_return.real
            trading_days = len(returns)
            market = self.context.get('settings', {}).get('market', 'cn_stock')
            annual_trading_days = ANNUAL_TRADING_DAYS.get(market, 252)
            annual_return = (1 + total_return) ** (annual_trading_days / trading_days) - 1
            # 确保是实数
            if isinstance(annual_return, complex):
                annual_return = annual_return.real
            
            # 年化波动率
            annual_volatility = np.std(returns_array) * np.sqrt(annual_trading_days)
            
            # 夏普比率
            risk_free_rate = DEFAULT_RISK_FREE_RATE  # 假设无风险利率3%
            sharpe_ratio = (annual_return - risk_free_rate) / annual_volatility if annual_volatility > 0 else 0
            
            # 最大回撤
            # nav_values已在上文构建: [initial_capital, day1_assets, day2_assets, ...]
            cumulative_returns = np.array(nav_values) / initial_capital
            peak = np.maximum.accumulate(cumulative_returns)
            drawdown = (cumulative_returns - peak) / peak
            max_drawdown = np.min(drawdown)

            # 打印历史净值曲线（用于排查回撤问题）
            self._print_equity_curve(daily_history, cumulative_returns, drawdown, max_drawdown, nav_values)

            # 胜率
            win_trades = len([r for r in returns if r > 0])
            win_ratio = win_trades / len(returns) if returns else 0
            
            # 更新绩效指标
            self.context['performance']['indicators'] = {
                'total_return': total_return,
                'annual_return': annual_return,
                'annual_volatility': annual_volatility,
                'sharpe_ratio': sharpe_ratio,
                'max_drawdown': max_drawdown
            }
            self.context['performance']['win_ratio'] = win_ratio
            self.context['performance']['trade_num'] = len(self.trade_center.trades)
            # 捕获交易到引擎实例属性（context['logs'] 可能被清理清空；to_dict 可能抛异常→手动兜底）
            self._captured_trades = []
            try:
                self._captured_trades = [t.to_dict() for t in self.trade_center.trades.values()]
            except Exception:
                try:
                    self._captured_trades = [{'symbol': str(getattr(t,'symbol','')), 'side': str(getattr(t,'side','')),
                        'volume': float(getattr(t,'volume',0)), 'price': float(getattr(t,'price',0)),
                        'amount': float(getattr(t,'amount',0)), 'trade_time': str(getattr(t,'trade_time',''))}
                        for t in self.trade_center.trades.values()]
                except Exception:
                    pass
            
            Log.logger.info(f"回测绩效 - 总收益: {total_return:.2%}, 年化收益: {annual_return:.2%}, "
                          f"夏普比率: {sharpe_ratio:.2f}, 最大回撤: {max_drawdown:.2%}, "
                          f"胜率: {win_ratio:.2%}, 交易次数: {len(self.trade_center.trades)}")

            # ========== 计算基准收益率和超额收益 ==========
            self._calculate_benchmark_performance(daily_history, returns_array, trading_days)

    def _print_equity_curve(self, daily_history: List[Dict], cumulative_returns: np.ndarray,
                            drawdown: np.ndarray, max_drawdown: float, nav_values: list = None):
        """打印历史净值曲线和回撤曲线，用于排查回撤问题

        Args:
            daily_history: 每日历史记录列表
            cumulative_returns: 累计收益率数组
            drawdown: 回撤数组
            max_drawdown: 最大回撤值
        """
        if not daily_history or len(daily_history) < 2:
            return

        # BUG 5 fix: 使用 initial_capital 显示真实初始资金
        _initial_capital = self.context.get('settings', {}).get('initial_capital', 1000000)

        Log.logger.info("=" * 80)
        Log.logger.info("【净值曲线分析】")

        # 基本信息
        start_date = daily_history[0].get('date', 'N/A')
        end_date = daily_history[-1].get('date', 'N/A')
        start_value = daily_history[0].get('total_assets', 0)
        end_value = daily_history[-1].get('total_assets', 0)

        Log.logger.info(f"回测区间: {start_date} ~ {end_date}")
        Log.logger.info(f"初始资金: {_initial_capital:,.2f}, 首日收盘资金: {start_value:,.2f}, 最终资金: {end_value:,.2f}")

        # 找到关键点位
        max_dd_idx = np.argmin(drawdown)
        peak_idx = np.argmax(cumulative_returns[:max_dd_idx + 1]) if max_dd_idx > 0 else 0

        # 累计净值曲线关键点
        Log.logger.info("-" * 80)
        Log.logger.info("【累计净值关键点】")
        Log.logger.info(f"起点    : 净值=1.0000, 初始资金={_initial_capital:,.2f}")

        # 最高点 (cumulative_returns[0]是初始资金, cumulative_returns[i+1]对应daily_history[i])
        max_nav_idx = np.argmax(cumulative_returns)
        max_nav_dh_idx = max(0, min(max_nav_idx - 1, len(daily_history) - 1))
        max_nav_date = daily_history[max_nav_dh_idx].get('date', start_date) if max_nav_idx > 0 else '初始'
        max_nav_value = nav_values[max_nav_idx] if nav_values is not None else cumulative_returns[max_nav_idx] * _initial_capital
        Log.logger.info(f"最高点  : 日期={max_nav_date}, 净值={cumulative_returns[max_nav_idx]:.4f}, 资金={max_nav_value:,.2f}")

        # 最低点
        min_nav_idx = np.argmin(cumulative_returns)
        min_nav_dh_idx = max(0, min(min_nav_idx - 1, len(daily_history) - 1))
        min_nav_date = daily_history[min_nav_dh_idx].get('date', start_date) if min_nav_idx > 0 else '初始'
        min_nav_value = nav_values[min_nav_idx] if nav_values is not None else cumulative_returns[min_nav_idx] * _initial_capital
        Log.logger.info(f"最低点  : 日期={min_nav_date}, 净值={cumulative_returns[min_nav_idx]:.4f}, 资金={min_nav_value:,.2f}")

        # 终点
        Log.logger.info(f"终点    : 日期={end_date}, 净值={cumulative_returns[-1]:.4f}, 资金={end_value:,.2f}")

        # 最大回撤详情
        Log.logger.info("-" * 80)
        Log.logger.info("【最大回撤详情】")
        if max_drawdown < 0:
            peak_dh_idx = max(0, min(peak_idx - 1, len(daily_history) - 1))
            trough_dh_idx = max(0, min(max_dd_idx - 1, len(daily_history) - 1))
            peak_date = daily_history[peak_dh_idx].get('date', 'N/A') if peak_idx > 0 else '初始'
            trough_date = daily_history[trough_dh_idx].get('date', 'N/A') if max_dd_idx > 0 else '初始'
            peak_nav = cumulative_returns[peak_idx]
            trough_nav = cumulative_returns[max_dd_idx]
            Log.logger.info(f"回撤区间: {peak_date} ~ {trough_date}")
            Log.logger.info(f"峰值净值: {peak_nav:.4f} (日期: {peak_date})")
            Log.logger.info(f"谷值净值: {trough_nav:.4f} (日期: {trough_date})")
            Log.logger.info(f"最大回撤: {max_drawdown:.2%}")
            Log.logger.info(f"回撤天数: {max_dd_idx - peak_idx} 天")
        else:
            Log.logger.info("无回撤（最大回撤=0）")

        # 如果回撤异常（>50%），打印详细的历史数据
        if max_drawdown < -0.5:
            Log.logger.info("-" * 80)
            Log.logger.info("【关键】回撤区间详细记录（峰值前5天 ~ 谷值后5天）：")
            Log.logger.info(f"{'序号':>6} | {'日期':>12} | {'总资产':>15} | {'累计净值':>10} | {'回撤':>10} | {'备注':>10}")
            Log.logger.info("-" * 80)

            # 计算回撤区间的起始和结束索引（cumulative_returns[0]=1.0 对应 daily_history[0]）
            start_idx = max(0, peak_idx - 5)  # 峰值前5天
            end_idx = min(len(daily_history) - 1, max_dd_idx + 5)  # 谷值后5天

            for i in range(start_idx, end_idx + 1):
                date = daily_history[i].get('date', 'N/A')
                total = daily_history[i].get('total_assets', 0)
                nav = cumulative_returns[i] if i < len(cumulative_returns) else 0
                dd = drawdown[i] if i < len(drawdown) else 0

                # 添加备注标记关键点位
                remark = ""
                if i == peak_idx:
                    remark = "【峰值】"
                elif i == max_dd_idx:
                    remark = "【谷值】"

                Log.logger.info(f"{i + 1:>6} | {date:>12} | {total:>15,.2f} | {nav:>10.4f} | {dd:>9.2%} | {remark:>10}")

            Log.logger.info("-" * 80)
            Log.logger.info("【警告】最大回撤超过50%，同时打印完整净值历史（前20条和后20条）：")
            Log.logger.info(f"{'序号':>6} | {'日期':>12} | {'总资产':>15} | {'累计净值':>10} | {'回撤':>10}")
            Log.logger.info("-" * 80)

            # 打印前20条
            for i in range(min(20, len(daily_history) - 1)):
                date = daily_history[i].get('date', 'N/A')
                total = daily_history[i].get('total_assets', 0)
                nav = cumulative_returns[i] if i < len(cumulative_returns) else 0
                dd = drawdown[i] if i < len(drawdown) else 0
                Log.logger.info(f"{i + 1:>6} | {date:>12} | {total:>15,.2f} | {nav:>10.4f} | {dd:>9.2%}")

            if len(daily_history) > 40:
                Log.logger.info(f"{'...':>6} | {'...':>12} | {'...':>15} | {'...':>10} | {'...':>10}")

            # 打印后20条
            for i in range(max(20, len(daily_history) - 20), len(daily_history)):
                date = daily_history[i].get('date', 'N/A')
                total = daily_history[i].get('total_assets', 0)
                nav = cumulative_returns[i] if i < len(cumulative_returns) else 0
                dd = drawdown[i] if i < len(drawdown) else 0
                Log.logger.info(f"{i + 1:>6} | {date:>12} | {total:>15,.2f} | {nav:>10.4f} | {dd:>9.2%}")

        # 月度/季度统计摘要
        Log.logger.info("-" * 80)
        Log.logger.info("【收益分布统计】")
        # 使用已计算好的returns（与_calculate_performance_sync一致），而非独立重算
        returns_array = cumulative_returns_pct = None
        if len(daily_history) > 1:
            # 复用已有的收益率计算逻辑（与_calculate_performance_sync一致）
            returns_list = []
            for i in range(1, len(daily_history)):
                prev_value = daily_history[i-1].get('total_assets', 0)
                curr_value = daily_history[i].get('total_assets', 0)
                if prev_value > 0:
                    returns_list.append((curr_value - prev_value) / prev_value)
                else:
                    returns_list.append(0.0)
            returns_array = np.array(returns_list)

        if returns_array is not None and len(returns_array) > 0:
            positive_days = len([r for r in returns_array if r > 0])
            negative_days = len([r for r in returns_array if r < 0])
            zero_days = len(returns_array) - positive_days - negative_days
            Log.logger.info(f"盈利天数: {positive_days} ({positive_days/len(returns_array):.1%})")
            Log.logger.info(f"亏损天数: {negative_days} ({negative_days/len(returns_array):.1%})")
            Log.logger.info(f"持平天数: {zero_days} ({zero_days/len(returns_array):.1%})")

            max_daily_return = float(np.max(returns_array))
            min_daily_return = float(np.min(returns_array))
            Log.logger.info(f"最大单日盈利: {max_daily_return:.2%}")
            Log.logger.info(f"最大单日亏损: {min_daily_return:.2%}")

            # 异常值告警：A股涨跌停10%，超过20%基本不可能
            abnormal_threshold = ABNORMAL_DAILY_RETURN_THRESHOLD
            abnormal_days = [(i, returns_array[i]) for i in range(len(returns_array))
                           if abs(returns_array[i]) > abnormal_threshold]
            if abnormal_days:
                Log.logger.warning(f"【异常收益率告警】共{len(abnormal_days)}天收益率超过±{abnormal_threshold:.0%}：")
                for idx, ret in abnormal_days[:10]:  # 最多显示10条
                    date = daily_history[idx + 1].get('date', 'N/A')
                    prev_assets = daily_history[idx].get('total_assets', 0)
                    curr_assets = daily_history[idx + 1].get('total_assets', 0)
                    Log.logger.warning(f"  {date}: {ret:.2%} "
                                     f"(前日资产={prev_assets:,.2f} → 当日={curr_assets:,.2f})")
                if len(abnormal_days) > 10:
                    Log.logger.warning(f"  ... 还有{len(abnormal_days) - 10}天异常")
        else:
            Log.logger.warning("数据不足，无法计算收益分布")

        Log.logger.info("=" * 80)

    def _calculate_benchmark_performance(self, daily_history: List[Dict], strategy_returns_array, trading_days: int):
        """计算基准收益率和超额收益指标

        Args:
            daily_history: 每日历史记录
            strategy_returns_array: 策略日收益率数组
            trading_days: 交易日数量
        """
        benchmark = self.context.get('benchmark')
        if not benchmark:
            Log.logger.info("未设置基准，跳过基准收益率计算")
            return

        try:
            # 获取年化交易日数
            market = self.context.get('settings', {}).get('market', 'cn_stock')
            annual_trading_days = ANNUAL_TRADING_DAYS.get(market, 252)

            # 获取回测起止日期
            start_date = daily_history[0].get('date') if isinstance(daily_history[0], dict) else daily_history[0]
            end_date = daily_history[-1].get('date') if isinstance(daily_history[-1], dict) else daily_history[-1]

            # 如果是 datetime 对象，转为字符串
            if hasattr(start_date, 'strftime'):
                start_date = start_date.strftime('%Y-%m-%d')
            if hasattr(end_date, 'strftime'):
                end_date = end_date.strftime('%Y-%m-%d')

            # 获取基准市场：优先使用配置，否则根据代码后缀智能推断
            benchmark_market = self.context.get('settings', {}).get('benchmark_market')
            if not benchmark_market:
                # 根据基准代码后缀推断市场
                benchmark_market = self._infer_benchmark_market(benchmark)

            Log.logger.info(f"获取基准数据: {benchmark}, 市场: {benchmark_market}, {start_date} -> {end_date}")

            # 获取基准K线数据 - 基准通常是指数，直接调用 data_interface
            # 不能使用 data_center.get_klines()，因为后者会从 context 获取市场（默认 cn_stock）
            benchmark_df = self.data_center.data_interface.get_klines(
                codes=benchmark,
                market=benchmark_market,
                freq='1d',
                start_date=start_date,
                end_date=end_date,
                fields=['close'],
                adj_type='none',
                use_cache=True
            )

            if benchmark_df is None or benchmark_df.empty:
                # BUG 6 fix: 加密货币市场fallback逻辑
                if benchmark_market in ('global_cryptospot', 'global_cryptoswap'):
                    fallback_market = 'global_cryptoswap' if benchmark_market == 'global_cryptospot' else 'global_cryptospot'
                    Log.logger.info(f"基准数据在 {benchmark_market} 中未找到，尝试 fallback 到 {fallback_market}")
                    benchmark_df = self.data_center.data_interface.get_klines(
                        codes=benchmark,
                        market=fallback_market,
                        freq='1d',
                        start_date=start_date,
                        end_date=end_date,
                        fields=['close'],
                        adj_type='none',
                        use_cache=True
                    )
                if benchmark_df is None or benchmark_df.empty:
                    Log.logger.warning(f"无法获取基准数据: {benchmark}")
                    return

            # 提取基准收盘价（处理 MultiIndex）
            if hasattr(benchmark_df.index, 'levels') and len(benchmark_df.index.levels) > 0:
                # MultiIndex (time, symbol)，按 symbol 过滤
                if benchmark in benchmark_df.index.get_level_values(1):
                    benchmark_prices = benchmark_df.loc[(slice(None), benchmark), 'close']
                else:
                    Log.logger.warning(f"基准 {benchmark} 不在返回数据中")
                    return
            else:
                # 单一 index，直接使用
                benchmark_prices = benchmark_df['close']

            # 计算基准日收益率
            benchmark_returns = benchmark_prices.pct_change().dropna().values

            # 对齐收益率序列长度（取较小值）
            min_len = min(len(strategy_returns_array), len(benchmark_returns))
            if min_len < 2:
                Log.logger.warning("数据点不足，无法计算基准指标")
                return

            aligned_strategy_returns = strategy_returns_array[:min_len]
            aligned_benchmark_returns = benchmark_returns[:min_len]

            # 计算基准指标
            benchmark_total_return = (benchmark_prices.iloc[-1] / benchmark_prices.iloc[0]) - 1
            benchmark_annual_return = (1 + benchmark_total_return) ** (annual_trading_days / trading_days) - 1
            benchmark_volatility = np.std(aligned_benchmark_returns) * np.sqrt(annual_trading_days)

            # 超额收益
            strategy_total_return = self.context['performance']['indicators']['total_return']
            excess_return = strategy_total_return - benchmark_total_return

            # 跟踪误差（策略收益与基准收益的差值的标准差）
            excess_returns_daily = aligned_strategy_returns - aligned_benchmark_returns
            tracking_error = np.std(excess_returns_daily) * np.sqrt(annual_trading_days)

            # 信息比率（年化超额收益 / 跟踪误差）
            information_ratio = excess_return / tracking_error if tracking_error > 0 else 0

            # 更新绩效指标
            self.context['performance']['benchmark'] = {
                'total_return': float(benchmark_total_return),
                'annual_return': float(benchmark_annual_return),
                'volatility': float(benchmark_volatility),
            }

            self.context['performance']['indicators'].update({
                'excess_return': float(excess_return),
                'tracking_error': float(tracking_error),
                'information_ratio': float(information_ratio),
            })

            # 持久化基准日收益序列（供 dashboard 画基准/超额净值曲线；此前算了即弃）
            try:
                self.context['performance']['bench_returns'] = [float(x) for x in aligned_benchmark_returns]
            except Exception:
                pass

            Log.logger.info(f"基准收益率 - 总收益: {benchmark_total_return:.2%}, 年化收益: {benchmark_annual_return:.2%}")
            Log.logger.info(f"超额收益 - 超额收益: {excess_return:.2%}, 跟踪误差: {tracking_error:.2%}, 信息比率: {information_ratio:.2f}")

        except Exception as e:
            Log.logger.error(f"计算基准收益率失败: {e}")

    def _infer_benchmark_market(self, benchmark: str) -> str:
        """根据策略市场和基准代码推断市场类型

        Args:
            benchmark: 基准代码，如 000001.SH, 000300.SZ, HSI.HK

        Returns:
            str: 市场类型 (cn_index, cn_fund, hk_index, global_index等)
        """
        strategy_market = self.context.get('settings', {}).get('market')

        # 1. 如果策略市场是cn_fund，基准也应该优先在cn_fund查找
        #    ETF代码如510300.SH在cn_index中不存在，需要从cn_fund查找
        if strategy_market == 'cn_fund':
            Log.logger.debug(f"基准 {benchmark} ETF市场，使用策略市场: cn_fund")
            return 'cn_fund'

        # 2. 根据基准代码后缀推断市场
        suffix_to_market = {
            '.SH': 'cn_index',   # 上证
            '.SZ': 'cn_index',   # 深证
            '.HK': 'hk_index',   # 香港
            '.US': 'us_index',   # 美国
            '.UK': 'global_index',
            '.JP': 'global_index',
        }

        for suffix, market in suffix_to_market.items():
            if benchmark.endswith(suffix):
                Log.logger.debug(f"基准 {benchmark} 后缀 {suffix} 推断为市场: {market}")
                return market

        # BUG 6 fix: 2.5 检测加密货币交易对（如BTCUSDT, ETHUSDT）
        # 加密货币基准的K线数据在 global_cryptospot 中
        crypto_suffixes = ('USDT', 'BUSD', 'BTC', 'ETH', 'BNB')
        if any(benchmark.endswith(s) for s in crypto_suffixes):
            Log.logger.debug(f"基准 {benchmark} 识别为加密货币交易对，使用市场: global_cryptospot")
            return 'global_cryptospot'

        # 3. 回退到策略所在市场
        if strategy_market:
            Log.logger.debug(f"基准 {benchmark} 使用策略市场: {strategy_market}")
            return strategy_market

        # 4. 默认返回 cn_index
        Log.logger.debug(f"基准 {benchmark} 无法推断市场，使用默认: cn_index")
        return 'cn_index'