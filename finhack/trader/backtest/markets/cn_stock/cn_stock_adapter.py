"""
中国股票市场适配器

实现中国A股市场的交易规则和事件生成，支持多频次
"""

from typing import List, Dict, Any, Tuple
from datetime import datetime, date, time, timedelta
import logging

from ..base_market import BaseMarket
from ..base_minutely_events import BaseMinutelyEventGenerator
from ..base_daily_events import BaseDailyEventGenerator
from ...events.event_types import BaseEvent, MarketEvent, EventTypeEnum
from .cn_stock_calculator import StockPriceCalculator
from .cn_stock_trading_rules_versions import is_st_stock, st_status_from_namechange

logger = logging.getLogger(__name__)


class CnStockMarketAdapter(BaseMarket):
    """中国股票市场适配器
    
    支持A股、基金、可转债等的交易规则
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """初始化中国股票市场适配器
        
        Args:
            config: 市场配置，如果为空则加载默认配置
        """
        # 如果没有提供配置，使用默认配置
        if not config:
            config = self._get_default_config()
            
        super().__init__('cn_stock', config)
        
        # 中国股票市场特有配置
        self.t_plus_one = config.get('t_plus_one', True)  # T+1制度
        self.price_limit_enabled = config.get('price_limit_enabled', True)  # 涨跌停限制
        self.daily_price_limit = config.get('daily_price_limit', 0.10)  # 10%涨跌停

        # 标的元数据缓存（name/list_date/category，用于 ST/新股/板块判定）
        self._stock_meta_cache = None  # type: ignore
        # 名称变更历史缓存（按日精确 ST 的数据源；为空则回退静态快照）
        self._namechange_cache = None  # type: ignore

        logger.info(f"中国股票市场适配器初始化完成，支持频率: {self.supported_frequencies}")

    # ==================== 涨跌停校验 ====================

    def validate_order(self, symbol: str, volume: float, price: float,
                       side: str) -> Tuple[bool, str]:
        """下单关卡：涨跌停 + 价格区间校验（覆盖 BaseMarket 的空实现）

        Args:
            symbol: 标的代码
            volume: 委托数量
            price: 委托价（限价单）或最新价（市价单，由引擎传入）
            side: 'buy' / 'sell'

        Returns:
            (是否通过, 错误信息)
        """
        # 1) 基础校验（量、价>0）
        try:
            is_valid, msg = super().validate_order(symbol, volume, price, side)
            if not is_valid:
                return is_valid, msg
        except Exception:
            return True, ""

        # 无数据中心（如单元测试）→ 不做涨跌停校验
        if not self._data_center or not price or price <= 0:
            return True, ""

        try:
            prev_close = self._get_prev_close(symbol)
            # 无前收盘（如上市首日）→ 无法校验，放行
            if not prev_close or prev_close <= 0:
                return True, ""

            is_st, list_date, category = self._get_stock_meta(symbol)
            result = StockPriceCalculator.validate_order_price(
                symbol, price, side, prev_close, self._current_date,
                is_st=is_st, list_date=list_date, category=category,
            )
            if not result['valid']:
                return False, result['message']

            # 封板语义（价格近似）：买入价==涨停价 → 视为封涨停，拒买；
            # 卖出价==跌停价 → 视为封跌停，拒卖。
            upper = result.get('limit_upper')
            lower = result.get('limit_lower')
            eps = 1e-9
            if upper and abs(price - upper) < eps and str(side).lower() == 'buy':
                return False, f'{symbol} 已涨停，买入被拒（涨停价 {upper:.2f}）'
            if lower and abs(price - lower) < eps and str(side).lower() == 'sell':
                return False, f'{symbol} 已跌停，卖出被拒（跌停价 {lower:.2f}）'

            return True, ""
        except Exception as e:
            logger.debug(f"涨跌停校验异常 {symbol}: {e}，放行")
            return True, ""

    def _get_prev_close(self, symbol: str) -> float:
        """取严格早于当前交易日的最后一根日 K 收盘价（前收盘）"""
        if not self._data_center:
            return 0.0
        try:
            df = self._data_center.get_klines(
                [symbol], freq='1d', end_time=self._current_date
            )
            if df is None or getattr(df, 'empty', True):
                return 0.0
            # 规整成带 time/code 列的 DataFrame
            work = df.reset_index() if df.index.names != [None] else df.copy()
            if 'time' not in work.columns or 'code' not in work.columns:
                # 兜底：取列名
                return 0.0
            work = work[work['code'] == symbol]
            # 仅保留 date < 当前交易日
            cur = self._current_date
            times = work['time']
            if hasattr(times.iloc[0], 'date'):
                mask = times.map(lambda t: t.date() < cur)
            else:
                mask = times.map(lambda t: str(t)[:10] < str(cur))
            work = work[mask]
            if work.empty:
                return 0.0
            return float(work.sort_values('time').iloc[-1]['close'])
        except Exception as e:
            logger.debug(f"获取 {symbol} 前收盘失败: {e}")
            return 0.0

    def _get_stock_meta(self, symbol: str) -> Tuple[bool, Any, Any]:
        """从 cn_stock_list 取 (is_st, list_date, category)；缓存 DataFrame"""
        code = symbol.split('.')[0]
        if self._stock_meta_cache is None:
            self._stock_meta_cache = False  # sentinel: 已尝试
            try:
                dc = self._data_center
                di = getattr(dc, 'data_interface', None) or dc
                df = di.get_stock_list('cn_stock', use_cache=True)
                if df is not None and not df.empty and 'code' in df.columns:
                    self._stock_meta_cache = df.set_index('code')
            except Exception as e:
                logger.debug(f"加载 cn_stock 标的列表失败: {e}")

        is_st = False
        list_date = None
        category = None
        cache = self._stock_meta_cache
        if hasattr(cache, 'loc') and code in cache.index:
            row = cache.loc[code]
            if isinstance(row, type(cache)):  # 多行重复 code
                row = row.iloc[0]
            name = row.get('name') if hasattr(row, 'get') else None
            is_st = self._is_st_as_of(code, name)
            ld = row.get('list_date') if hasattr(row, 'get') else None
            if ld is not None and str(ld) not in ('None', 'nan', ''):
                try:
                    list_date = datetime.strptime(str(ld)[:8], '%Y%m%d').date()
                except Exception:
                    list_date = None
            cat = row.get('category') if hasattr(row, 'get') else None
            category = None if (cat is None or str(cat) in ('nan',)) else cat
        return is_st, list_date, category

    def _load_namechange(self):
        """加载 cn_stock_namechange.csv（名称变更历史）；不存在返回 None"""
        if self._namechange_cache is not None:
            return self._namechange_cache if self._namechange_cache is not False else None
        try:
            import os
            import pandas as _pd
            dc = self._data_center
            di = getattr(dc, 'data_interface', None) or dc
            ref_dir = getattr(di, 'reference_data_dir', None)
            if not ref_dir:
                return None
            path = os.path.join(ref_dir, 'cn_stock', 'cn_stock_namechange.csv')
            if not os.path.exists(path):
                self._namechange_cache = False  # 标记：已尝试但无文件
                return None
            df = _pd.read_csv(path)
            if 'code' not in df.columns:
                # 兜底：tushare 原始列名 ts_code
                if 'ts_code' in df.columns:
                    df = df.rename(columns={'ts_code': 'code'})
            df['code'] = df['code'].astype(str)
            self._namechange_cache = df
            return df
        except Exception as e:
            logger.debug(f"加载 cn_stock_namechange 失败: {e}")
            self._namechange_cache = False
            return None

    def _is_st_as_of(self, code: str, snapshot_name=None) -> bool:
        """按日精确判 ST：优先用 namechange 历史；无记录回退静态快照名"""
        try:
            nc = self._load_namechange()
            if nc is not None:
                verdict = st_status_from_namechange(code, self._current_date, nc)
                if verdict is not None:
                    return verdict
        except Exception as e:
            logger.debug(f"namechange ST 判定异常 {code}: {e}")
        # 回退：静态快照名
        return is_st_stock(snapshot_name)
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取中国股票市场默认配置"""
        return {
            'market_name': 'cn_stock',
            'supported_frequencies': ['1d', '1m', '30m', '120m'],
            'timezone': 'Asia/Shanghai',
            'currency': 'CNY',
            'trading_schedule': {
                '1d': {
                    'morning_start': '09:30',
                    'morning_end': '11:30',
                    'afternoon_start': '13:00',
                    'afternoon_end': '15:00'
                },
                '1m': {
                    'morning_start': '09:30',
                    'morning_end': '11:30',
                    'afternoon_start': '13:00',
                    'afternoon_end': '15:00'
                },
                '30m': {
                    'morning_start': '09:30',
                    'morning_end': '11:30',
                    'afternoon_start': '13:00',
                    'afternoon_end': '15:00'
                },
                '120m': {
                    'morning_start': '09:30',
                    'morning_end': '11:30',
                    'afternoon_start': '13:00',
                    'afternoon_end': '15:00'
                }
            },
            'trading_rules': {
                'commission': {
                    'stock': {
                        'open_commission': 0.0003,
                        'close_commission': 0.0003,
                        'open_tax': 0.0,
                        'close_tax': 0.001,  # 印花税
                        'min_commission': 5.0
                    }
                },
                'slippage': {
                    'slip_type': 'pricerelated',
                    'slip_value': 0.001
                },
                'limits': {
                    'lot_size': 100,  # 最小交易单位
                    'min_order_volume': 100,  # 最小下单数量
                    'max_order_volume': 1000000  # 最大下单数量
                }
            }
        }
    
    def generate_daily_events(self, trade_date: date, frequency: str = '1d') -> List[BaseEvent]:
        """生成指定日期的市场事件列表

        Args:
            trade_date: 交易日期
            frequency: 数据频率

        Returns:
            List[BaseEvent]: 事件列表
        """
        events = []

        if frequency == '1d':
            # ========== 日频事件 - 使用1d精简事件序列 ==========
            # 1d频率仅加载日频数据，只有开盘和收盘进行撮合
            events = self._generate_1d_events(trade_date)

        elif frequency in ['1m', '30m', '120m']:
            # ========== 分钟频事件 - 使用统一框架 ==========
            # 基础事件由统一框架生成
            events = BaseMinutelyEventGenerator.generate_minutely_events(
                adapter=self,
                trade_date=trade_date,
                frequency=frequency
            )

            # 添加中国A股特有的集合竞价事件
            additional_events = self._get_auction_events(trade_date, frequency)

            # 添加细分时段事件
            additional_events.extend(self._get_session_events(trade_date, frequency))

            # 添加收盘相关事件
            additional_events.extend(self._get_closing_events(trade_date, frequency))

            # 添加K线事件（如果需要）
            kline_events = self._get_kline_events(trade_date, frequency)
            additional_events.extend(kline_events)

            events.extend(additional_events)
            events.sort(key=lambda x: x.event_time)

        return events

    def _generate_1d_events(self, trade_date: date) -> List[BaseEvent]:
        """生成1d频率的精简事件序列

        1d频率回测特点：
        - 仅加载日频数据
        - 只有开盘和收盘进行撮合
        - 不生成盘中分钟事件
        - 不生成午休分段事件

        事件序列：
        09:00 DAY_START              每日开始（初始化、分红送股处理等）
        09:00 BEFORE_MARKET          盘前准备（可自定义事件）
        09:25 OPENING_PRICE_DETERMINED 开盘价确定
        09:25 TRY_MATCH              开盘集合竞价撮合 ← 可交易（匹配盘前挂单）
        09:30 MARKET_START           开盘（连续竞价开始）
        09:30 TRY_MATCH              开盘撮合 ← 可交易（匹配策略09:30下单）
        -- 盘中无事件 --
        14:57 CLOSING_START          收盘集合竞价开始
        15:00 CLOSING_PRICE_DETERMINED 收盘价确定
        15:00 TRY_MATCH              收盘集合竞价撮合 ← 可交易
        15:00 CLOSING_END            收盘集合竞价结束
        15:00 DAILY_BAR_CLOSED       日线K线收盘
        15:05 AFTER_MARKET           盘后处理（可自定义事件）
        15:05 DAY_END                每日结束（净值记录等）

        Args:
            trade_date: 交易日期

        Returns:
            List[BaseEvent]: 精简的事件列表
        """
        events = []

        # 1. 每日开始（初始化、分红送股处理等）
        events.append(MarketEvent(
            event_type=EventTypeEnum.DAY_START,
            event_time=datetime.combine(trade_date, time(9, 0)),
            market=self.market_name,
            frequency='1d',
            event_description="每日开始"
        ))

        # 2. 盘前准备（策略初始化、调仓准备）
        events.append(MarketEvent(
            event_type=EventTypeEnum.BEFORE_MARKET,
            event_time=datetime.combine(trade_date, time(9, 0)),
            market=self.market_name,
            frequency='1d',
            event_description="盘前准备"
        ))

        # 3. 开盘集合竞价开始
        events.append(MarketEvent(
            event_type=EventTypeEnum.PRE_OPENING_START,
            event_time=datetime.combine(trade_date, time(9, 15)),
            market=self.market_name,
            frequency='1d',
            event_description="集合竞价开始"
        ))

        # 4. 开盘集合竞价不可撤单
        events.append(MarketEvent(
            event_type=EventTypeEnum.PRE_OPENING_END,
            event_time=datetime.combine(trade_date, time(9, 20)),
            market=self.market_name,
            frequency='1d',
            event_description="集合竞价不可撤单"
        ))

        # 5. 开盘价确定
        events.append(MarketEvent(
            event_type=EventTypeEnum.OPENING_PRICE_DETERMINED,
            event_time=datetime.combine(trade_date, time(9, 25)),
            market=self.market_name,
            frequency='1d',
            event_description="开盘价确定"
        ))

        # 6. 开盘集合竞价撮合 ← 可交易
        events.append(MarketEvent(
            event_type=EventTypeEnum.TRY_MATCH,
            event_time=datetime.combine(trade_date, time(9, 25)),
            market=self.market_name,
            frequency='1d',
            event_description="开盘集合竞价撮合"
        ))

        # 7. 开盘（连续竞价开始，策略run_daily(09:30)的下单在此撮合）
        events.append(MarketEvent(
            event_type=EventTypeEnum.MARKET_START,
            event_time=datetime.combine(trade_date, time(9, 30)),
            market=self.market_name,
            frequency='1d',
            event_description="开盘"
        ))

        # 8. 开盘撮合 ← 策略09:30下的单在此成交
        events.append(MarketEvent(
            event_type=EventTypeEnum.TRY_MATCH,
            event_time=datetime.combine(trade_date, time(9, 30)),
            market=self.market_name,
            frequency='1d',
            event_description="开盘撮合"
        ))

        # -- 盘中无事件 --

        # 7. 收盘集合竞价开始
        events.append(MarketEvent(
            event_type=EventTypeEnum.CLOSING_START,
            event_time=datetime.combine(trade_date, time(14, 57)),
            market=self.market_name,
            frequency='1d',
            event_description="收盘集合竞价开始"
        ))

        # 8. 收盘价确定
        events.append(MarketEvent(
            event_type=EventTypeEnum.CLOSING_PRICE_DETERMINED,
            event_time=datetime.combine(trade_date, time(15, 0)),
            market=self.market_name,
            frequency='1d',
            event_description="收盘价确定"
        ))

        # 9. 收盘集合竞价撮合 ← 可交易
        events.append(MarketEvent(
            event_type=EventTypeEnum.TRY_MATCH,
            event_time=datetime.combine(trade_date, time(15, 0)),
            market=self.market_name,
            frequency='1d',
            event_description="收盘集合竞价撮合"
        ))

        # 10. 收盘结束
        events.append(MarketEvent(
            event_type=EventTypeEnum.CLOSING_END,
            event_time=datetime.combine(trade_date, time(15, 0)),
            market=self.market_name,
            frequency='1d',
            event_description="收盘集合竞价结束"
        ))

        # 11. 盘后处理（可自定义事件）
        events.append(MarketEvent(
            event_type=EventTypeEnum.AFTER_MARKET,
            event_time=datetime.combine(trade_date, time(15, 5)),
            market=self.market_name,
            frequency='1d',
            event_description="盘后处理"
        ))

        # 12. 每日结束（净值记录等）
        events.append(MarketEvent(
            event_type=EventTypeEnum.DAY_END,
            event_time=datetime.combine(trade_date, time(15, 5)),
            market=self.market_name,
            frequency='1d',
            event_description="每日结束"
        ))

        return events

    def _get_auction_events(self, trade_date: date, frequency: str) -> List[BaseEvent]:
        """生成A股集合竞价相关事件"""
        events = []

        # 集合竞价开始
        events.append(MarketEvent(
            event_type=EventTypeEnum.PRE_OPENING_START,
            event_time=datetime.combine(trade_date, time(9, 15)),
            market=self.market_name,
            frequency=frequency,
            event_description="集合竞价开始"
        ))

        # 集合竞价可撤单结束
        events.append(MarketEvent(
            event_type=EventTypeEnum.PRE_OPENING_END,
            event_time=datetime.combine(trade_date, time(9, 20)),
            market=self.market_name,
            frequency=frequency,
            event_description="集合竞价不可撤单"
        ))

        # 集合竞价撮合
        events.append(MarketEvent(
            event_type=EventTypeEnum.MATCHING_START,
            event_time=datetime.combine(trade_date, time(9, 25)),
            market=self.market_name,
            frequency=frequency,
            event_description="集合竞价撮合"
        ))

        # 开盘价确定
        events.append(MarketEvent(
            event_type=EventTypeEnum.OPENING_PRICE_DETERMINED,
            event_time=datetime.combine(trade_date, time(9, 25)),
            market=self.market_name,
            frequency=frequency,
            event_description="开盘价确定"
        ))

        return events

    def _get_session_events(self, trade_date: date, frequency: str) -> List[BaseEvent]:
        """生成分段时段事件（午休分段）"""
        events = []

        # 上午收盘
        events.append(MarketEvent(
            event_type=EventTypeEnum.MORNING_END,
            event_time=datetime.combine(trade_date, time(11, 30)),
            market=self.market_name,
            frequency=frequency,
            event_description="上午收盘"
        ))

        # 下午开盘
        events.append(MarketEvent(
            event_type=EventTypeEnum.AFTERNOON_START,
            event_time=datetime.combine(trade_date, time(13, 0)),
            market=self.market_name,
            frequency=frequency,
            event_description="下午开盘"
        ))

        return events

    def _get_closing_events(self, trade_date: date, frequency: str) -> List[BaseEvent]:
        """生成收盘相关事件"""
        events = []

        # 收盘集合竞价开始
        events.append(MarketEvent(
            event_type=EventTypeEnum.CLOSING_START,
            event_time=datetime.combine(trade_date, time(14, 57)),
            market=self.market_name,
            frequency=frequency,
            event_description="收盘集合竞价开始"
        ))

        # 收盘结束
        events.append(MarketEvent(
            event_type=EventTypeEnum.CLOSING_END,
            event_time=datetime.combine(trade_date, time(15, 0)),
            market=self.market_name,
            frequency=frequency,
            event_description="收盘集合竞价结束"
        ))

        # 收盘价确定
        events.append(MarketEvent(
            event_type=EventTypeEnum.CLOSING_PRICE_DETERMINED,
            event_time=datetime.combine(trade_date, time(15, 0)),
            market=self.market_name,
            frequency=frequency,
            event_description="收盘价确定"
        ))

        return events

    def _get_kline_events(self, trade_date: date, frequency: str) -> List[BaseEvent]:
        """生成K线事件（可选，某些策略可能依赖这些事件）"""
        events = []

        # 确定间隔
        interval_minutes = 1 if frequency == '1m' else (30 if frequency == '30m' else 120)

        # 获取交易时段
        trading_sessions = self.get_trading_sessions(trade_date, frequency)

        # 为每个交易时段生成K线事件
        for session_start, session_end in trading_sessions:
            start_dt = datetime.combine(trade_date, session_start)
            end_dt = datetime.combine(trade_date, session_end)
            current_time = start_dt

            while current_time <= end_dt:
                if frequency == '1m':
                    events.append(MarketEvent(
                        event_type=EventTypeEnum.MARKET_BAR_1M,
                        event_time=current_time,
                        market=self.market_name,
                        frequency=frequency,
                        event_description="1分钟K线"
                    ))
                elif frequency == '30m':
                    events.append(MarketEvent(
                        event_type=EventTypeEnum.MARKET_BAR_30M,
                        event_time=current_time,
                        market=self.market_name,
                        frequency=frequency,
                        event_description="30分钟K线"
                    ))
                elif frequency == '120m':
                    events.append(MarketEvent(
                        event_type=EventTypeEnum.MARKET_BAR_120M,
                        event_time=current_time,
                        market=self.market_name,
                        frequency=frequency,
                        event_description="120分钟K线"
                    ))
                current_time += timedelta(minutes=interval_minutes)

        return events

    def is_trading_time(self, dt: datetime, frequency: str = '1d') -> bool:
        """判断指定时间是否为交易时间
        
        Args:
            dt: 时间
            frequency: 数据频率
            
        Returns:
            bool: 是否为交易时间
        """
        # 检查是否为工作日
        if dt.weekday() >= 5:  # 周六、周日
            return False
        
        # 获取交易时段
        trading_sessions = self.get_trading_sessions(dt.date(), frequency)
        
        # 检查是否在任一交易时段内
        for session_start, session_end in trading_sessions:
            if session_start <= dt.time() <= session_end:
                return True
        
        return False
    
    def get_trading_sessions(self, trade_date: date, frequency: str = '1d') -> List[Tuple[time, time]]:
        """获取交易时段
        
        Args:
            trade_date: 交易日期
            frequency: 数据频率
            
        Returns:
            List[Tuple[time, time]]: 交易时段列表，每个元素为(开始时间, 结束时间)
        """
        # 检查是否为工作日
        if trade_date.weekday() >= 5:  # 周六、周日
            return []
        
        # 中国A股交易时段
        morning_session = (time(9, 30), time(11, 30))
        afternoon_session = (time(13, 0), time(15, 0))
        
        return [morning_session, afternoon_session]


# 向后兼容的别名
CnStockAdapter = CnStockMarketAdapter


class CnFundMarketAdapter(CnStockMarketAdapter):
    """中国基金市场适配器
    
    基于中国股票市场规则，但有一些特殊差异
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """初始化中国基金市场适配器"""
        # 如果没有提供配置，使用默认配置
        if not config:
            config = self._get_default_config()
            
        # 修改市场名称
        config['market_name'] = 'cn_fund'
        
        # 基金特有的配置
        config['trading_rules']['commission']['fund'] = {
            'open_commission': 0.0003,
            'close_commission': 0.0003,
            'open_tax': 0.0,
            'close_tax': 0.0,             # 基金免印花税
            'min_commission': 5.0
        }
        
        super().__init__(config)
        
        # 基金特有属性
        self.market_name = 'cn_fund'
        
        logger.info(f"中国基金市场适配器初始化完成，支持频率: {self.supported_frequencies}")
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取基金市场默认配置"""
        config = super()._get_default_config()
        config['market_name'] = 'cn_fund'
        return config
    
    def generate_daily_events(self, trade_date: datetime.date, frequency: str) -> List[BaseEvent]:
        """生成指定交易日和频率的事件列表"""
        if frequency == '1d':
            return self._generate_daily_events_1d(trade_date)
        elif frequency in ['1m', '30m', '120m']:
            return self._generate_daily_events_min(trade_date, frequency)
        else:
            raise ValueError(f"Unsupported frequency for cn_fund: {frequency}")
    
    def _generate_daily_events_1d(self, trade_date: datetime.date) -> List[BaseEvent]:
        """生成1d频率的日内事件"""
        events = []
        
        # 交易前事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.BEFORE_MARKET,
            event_time=datetime.combine(trade_date, time(9, 0)),
            market=self.market_name,
            frequency='1d',
            event_description="基金交易前准备"
        ))
        
        # 开盘事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.MARKET_START,
            event_time=datetime.combine(trade_date, time(9, 30)),
            market=self.market_name,
            frequency='1d',
            event_description="基金开盘"
        ))
        
        # 撮合事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.TRY_MATCH,
            event_time=datetime.combine(trade_date, time(9, 30)),
            market=self.market_name,
            frequency='1d',
            event_description="基金日级撮合"
        ))
        
        # 收盘事件
        events.append(MarketEvent(
            event_type=EventTypeEnum.MARKET_END,
            event_time=datetime.combine(trade_date, time(15, 0)),
            market=self.market_name,
            frequency='1d',
            event_description="基金收盘"
        ))
        
        return events 