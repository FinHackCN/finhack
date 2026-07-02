# -*- coding: utf-8 -*-
"""
A股涨跌停价计算器

镜像 cn_future/future_calculator 与 cn_fund/etf_calculator 的结构，
按 **板块 + 日期 + ST + 新股** 计算涨跌停价，并校验委托价是否落在区间内。

与 etf_calculator 的差异：
  - 个股需要 `is_st`（ST/*ST → 5%，科创/创业/北交的 st_limit 已等于 daily_limit，自动无加严）。
  - 涨跌停价取整采用交易所口径：涨停向下取整(floor)、跌停向上取整(ceil)，保证限价"可达"。
"""

import math
from datetime import date
from typing import Dict, Optional
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING

try:
    from .cn_stock_trading_rules_versions import (
        get_stock_board_type,
        PRICE_LIMIT_VERSIONS,
        LIMIT_PRICE_CALCULATION_VERSIONS,
        RuleVersion,
        get_exchange_from_symbol,
    )
except ImportError:  # 单元测试 / 直接运行
    from cn_stock_trading_rules_versions import (  # type: ignore
        get_stock_board_type,
        PRICE_LIMIT_VERSIONS,
        LIMIT_PRICE_CALCULATION_VERSIONS,
        RuleVersion,
        get_exchange_from_symbol,
    )


class StockPriceCalculator:
    """A股涨跌停价计算器"""

    @staticmethod
    def calculate_limit_prices(symbol: str, prev_close: float,
                               query_date: date, is_st: bool = False,
                               list_date: Optional[date] = None,
                               category: Optional[str] = None) -> Dict:
        """计算涨跌停价

        Args:
            symbol: 股票代码，如 '600519.SH'
            prev_close: 前收盘价（基准）
            query_date: 查询（回测）日期
            is_st: 是否 ST/*ST
            list_date: 上市日期（判断新股无限制窗口）
            category: 标的分类（'主板'/'创业板'/...，辅助判板块）

        Returns:
            {'upper_limit','lower_limit','limit_ratio','is_new_stock','is_st','board'}
        """
        board = get_stock_board_type(symbol, category)
        exchange = get_exchange_from_symbol(symbol)

        limit_rule = RuleVersion.get_applicable_rule(
            PRICE_LIMIT_VERSIONS, board, query_date, exchange
        )

        limit_ratio = 0.10
        is_new_stock = False
        new_stock_days_left = 0

        if limit_rule:
            limit_ratio = limit_rule.get('daily_limit', 0.10)

            # ST：使用 st_limit（主板/老创业板=5%；科创/创业/北交 st_limit==daily_limit）
            if is_st and 'st_limit' in limit_rule:
                limit_ratio = limit_rule['st_limit']

            # 新股无涨跌幅限制窗口
            if list_date and 'new_stock_no_limit_days' in limit_rule:
                no_limit_days = limit_rule['new_stock_no_limit_days']
                days_since = (query_date - list_date).days
                if 0 <= days_since < no_limit_days:
                    is_new_stock = True
                    new_stock_days_left = no_limit_days - days_since
                    limit_ratio = float('inf')

        # 价格取整规则（tick）
        calc_rule = RuleVersion.get_applicable_rule(
            LIMIT_PRICE_CALCULATION_VERSIONS, board, query_date, exchange
        )

        if is_new_stock or prev_close is None or prev_close <= 0:
            upper_limit = float('inf')
            lower_limit = 0.0
        else:
            raw_upper = prev_close * (1 + limit_ratio)
            raw_lower = prev_close * (1 - limit_ratio)
            tick_size = StockPriceCalculator._get_tick_size(calc_rule, prev_close)
            # 交易所口径：涨停向下取整（floor）、跌停向上取整（ceil）
            upper_limit = StockPriceCalculator._round_limit(raw_upper, tick_size, 'down')
            lower_limit = StockPriceCalculator._round_limit(raw_lower, tick_size, 'up')

        return {
            'upper_limit': upper_limit,
            'lower_limit': lower_limit,
            'limit_ratio': None if is_new_stock else limit_ratio,
            'is_new_stock': is_new_stock,
            'new_stock_days_left': new_stock_days_left,
            'is_st': is_st,
            'board': board,
        }

    @staticmethod
    def _get_tick_size(calc_rule: Optional[Dict], ref_price: float) -> float:
        """从计算规则取 tick_size（支持科创/创业 ≥阈值 → 较大 tick 的分段）"""
        if calc_rule is None:
            return 0.01
        tick_size = calc_rule.get('tick_size', 0.01)
        threshold = calc_rule.get('tick_size_threshold')
        if threshold and ref_price and ref_price >= threshold:
            tick_size = calc_rule.get('tick_size_high', tick_size)
        return tick_size

    @staticmethod
    def _round_limit(price: float, tick_size: float, direction: str) -> float:
        """按 tick 取整：'down'→floor（涨停）、'up'→ceil（跌停）"""
        if price == float('inf') or price <= 0 or tick_size <= 0:
            return price
        dec_price = Decimal(str(price))
        dec_tick = Decimal(str(tick_size))
        quotient = dec_price / dec_tick
        if direction == 'down':
            ticks = quotient.to_integral_value(rounding=ROUND_FLOOR)
        else:
            ticks = quotient.to_integral_value(rounding=ROUND_CEILING)
        return float(ticks * dec_tick)

    @staticmethod
    def validate_order_price(symbol: str, order_price: float, side: str,
                             prev_close: float, query_date: date,
                             is_st: bool = False,
                             list_date: Optional[date] = None,
                             category: Optional[str] = None) -> Dict:
        """校验委托价是否在涨跌停区间内

        Args:
            order_price: 委托价（限价单）或最新价（市价单）
            side: 'buy' / 'sell'

        Returns:
            {'valid','message','limit_upper','limit_lower','is_new_stock'}
        """
        if not prev_close or prev_close <= 0 or not order_price or order_price <= 0:
            return {'valid': True, 'message': '无前收盘/委托价，跳过涨跌停校验',
                    'limit_upper': None, 'limit_lower': None, 'is_new_stock': False}

        limits = StockPriceCalculator.calculate_limit_prices(
            symbol, prev_close, query_date, is_st, list_date, category
        )

        if limits['is_new_stock']:
            return {'valid': True, 'message': '新股无涨跌停限制',
                    'limit_upper': limits['upper_limit'],
                    'limit_lower': limits['lower_limit'],
                    'is_new_stock': True}

        upper = limits['upper_limit']
        lower = limits['lower_limit']
        eps = 1e-9
        if order_price > upper + eps:
            return {'valid': False,
                    'message': f'委托价 {order_price:.2f} 超过涨停价 {upper:.2f}',
                    'limit_upper': upper, 'limit_lower': lower, 'is_new_stock': False}
        if order_price < lower - eps:
            return {'valid': False,
                    'message': f'委托价 {order_price:.2f} 低于跌停价 {lower:.2f}',
                    'limit_upper': upper, 'limit_lower': lower, 'is_new_stock': False}
        return {'valid': True, 'message': '',
                'limit_upper': upper, 'limit_lower': lower, 'is_new_stock': False}
