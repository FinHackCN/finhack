"""
ETF交易规则计算器

实现涨跌停价计算、价格笼子验证、最小单位计算等
"""

import math
from datetime import date
from typing import Dict, Tuple, Optional, Union
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN, ROUND_UP, ROUND_CEILING, ROUND_FLOOR

# 尝试相对导入，如果失败则使用绝对导入
try:
    from .etf_trading_rules_versions import (
        PRICE_LIMIT_VERSIONS,
        LOT_SIZE_VERSIONS,
        PRICE_CAGE_VERSIONS,
        LIMIT_PRICE_CALCULATION_VERSIONS,
        COMMISSION_VERSIONS,
        RuleVersion,
        get_etf_board_type,
        get_etf_type,
        get_exchange_from_symbol,
    )
except ImportError:
    from etf_trading_rules_versions import (
        PRICE_LIMIT_VERSIONS,
        LOT_SIZE_VERSIONS,
        PRICE_CAGE_VERSIONS,
        LIMIT_PRICE_CALCULATION_VERSIONS,
        COMMISSION_VERSIONS,
        RuleVersion,
        get_etf_board_type,
        get_etf_type,
        get_exchange_from_symbol,
    )


class ETFPriceCalculator:
    """ETF价格计算器"""

    @staticmethod
    def calculate_limit_prices(symbol: str, prev_close: float, query_date: date,
                              listing_date: date = None) -> Dict[str, float]:
        """计算涨跌停价格

        Args:
            symbol: ETF代码
            prev_close: 前收盘价
            query_date: 查询日期
            listing_date: 上市日期（用于判断新股）

        Returns:
            {
                'upper_limit': 涨停价,
                'lower_limit': 跌停价,
                'limit_ratio': 涨跌幅比例,
                'is_new_stock': 是否新股,
                'new_stock_days_left': 新股剩余无限制天数
            }
        """
        board = get_etf_board_type(symbol)
        exchange = get_exchange_from_symbol(symbol)

        # 获取涨跌幅限制规则
        limit_rule = RuleVersion.get_applicable_rule(
            PRICE_LIMIT_VERSIONS, board, query_date, exchange
        )

        if limit_rule is None:
            # 默认10%涨跌停
            limit_ratio = 0.10
            is_new_stock = False
            new_stock_days_left = 0
        else:
            limit_ratio = limit_rule.get('daily_limit', 0.10)

            # 判断是否新股
            is_new_stock = False
            new_stock_days_left = 0
            if listing_date and 'new_stock_no_limit_days' in limit_rule:
                days_since_listing = (query_date - listing_date).days
                no_limit_days = limit_rule['new_stock_no_limit_days']
                if days_since_listing < no_limit_days:
                    is_new_stock = True
                    new_stock_days_left = no_limit_days - days_since_listing
                    limit_ratio = float('inf')  # 新股无涨跌幅限制

        # 获取价格计算规则
        calc_rule = RuleVersion.get_applicable_rule(
            LIMIT_PRICE_CALCULATION_VERSIONS, board, query_date, exchange
        )

        # 计算原始涨跌停价
        if is_new_stock:
            raw_upper = float('inf')
            raw_lower = 0.0
        else:
            raw_upper = prev_close * (1 + limit_ratio)
            raw_lower = prev_close * (1 - limit_ratio)

        # 应用四舍五入规则
        upper_limit = ETFPriceCalculator._round_price(
            raw_upper, calc_rule, prev_close
        )
        lower_limit = ETFPriceCalculator._round_price(
            raw_lower, calc_rule, prev_close
        )

        return {
            'upper_limit': upper_limit,
            'lower_limit': lower_limit,
            'limit_ratio': limit_ratio if not is_new_stock else None,
            'is_new_stock': is_new_stock,
            'new_stock_days_left': new_stock_days_left,
        }

    @staticmethod
    def _round_price(price: float, calc_rule: Dict, ref_price: float = None) -> float:
        """按规则对价格进行四舍五入

        Args:
            price: 原始价格
            calc_rule: 计算规则
            ref_price: 参考价格（用于判断使用哪个tick_size）

        Returns:
            四舍五入后的价格
        """
        if price == float('inf') or price == 0:
            return price

        if calc_rule is None:
            # 默认：保留2位小数，四舍五入
            return round(price, 2)

        rounding_method = calc_rule.get('rounding_method', 'round')

        # 确定最小价格单位
        tick_size = calc_rule.get('tick_size', 0.01)
        threshold = calc_rule.get('tick_size_threshold')

        if threshold and ref_price and ref_price >= threshold:
            tick_size = calc_rule.get('tick_size_high', tick_size)

        # 使用Decimal进行精确计算
        dec_price = Decimal(str(price))
        dec_tick = Decimal(str(tick_size))

        if rounding_method == 'round':
            # 四舍五入
            result = (dec_price / dec_tick).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * dec_tick
        elif rounding_method == 'ceil':
            # 向上进位（科创板）
            result = math.ceil(float(price) / tick_size) * tick_size
            result = Decimal(str(result))
        elif rounding_method == 'truncate':
            # 直接截断（北交所）
            decimals = calc_rule.get('rounding_decimals', 2)
            result = dec_price.quantize(Decimal(f'1e-{decimals}'), rounding=ROUND_DOWN)
        else:
            result = dec_price.quantize(Decimal(f'1e-{int(-math.log10(tick_size))}'))

        return float(result)


class ETFPriceCageValidator:
    """价格笼子验证器"""

    @staticmethod
    def validate_order_price(symbol: str, order_price: float, side: str,
                           reference_price: float, bid_price: float = None,
                           ask_price: float = None, query_date: date = None) -> Dict:
        """验证订单价格是否符合价格笼子限制

        Args:
            symbol: ETF代码
            order_price: 订单价格
            side: 订单方向 ('buy' or 'sell')
            reference_price: 参考价（最近成交价或最优报价）
            bid_price: 买一价
            ask_price: 卖一价
            query_date: 查询日期

        Returns:
            {
                'valid': 是否有效,
                'message': 错误信息,
                'limit_price': 限制价格,
            }
        """
        if query_date is None:
            query_date = date.today()

        board = get_etf_board_type(symbol)
        exchange = get_exchange_from_symbol(symbol)

        # 获取价格笼子规则
        cage_rule = RuleVersion.get_applicable_rule(
            PRICE_CAGE_VERSIONS, board, query_date, exchange
        )

        # 如果未启用价格笼子
        if cage_rule is None or not cage_rule.get('enabled', False):
            return {
                'valid': True,
                'message': '价格笼子未启用',
                'limit_price': None,
            }

        # 确定基准价
        if side == 'buy':
            base_price = ask_price if ask_price else reference_price
        else:
            base_price = bid_price if bid_price else reference_price

        if base_price is None or base_price <= 0:
            return {
                'valid': True,
                'message': '无法获取基准价',
                'limit_price': None,
            }

        # 计算限制价格
        if side == 'buy':
            limit_price = ETFPriceCageValidator._calculate_buy_limit(
                base_price, cage_rule, board
            )
            is_valid = order_price <= limit_price
            message = f'买入价不能高于 {limit_price:.2f}' if not is_valid else ''
        else:
            limit_price = ETFPriceCageValidator._calculate_sell_limit(
                base_price, cage_rule, board
            )
            is_valid = order_price >= limit_price
            message = f'卖出价不能低于 {limit_price:.2f}' if not is_valid else ''

        return {
            'valid': is_valid,
            'message': message,
            'limit_price': limit_price,
        }

    @staticmethod
    def _calculate_buy_limit(base_price: float, rule: Dict, board: str) -> float:
        """计算买入价格上限"""
        limit_type = rule.get('buy_type', 'min')

        pct_limit = base_price * rule.get('buy_limit_pct', 1.02)

        if 'buy_limit_abs' in rule:
            abs_limit = base_price + rule['buy_limit_abs']
        elif 'buy_limit_units' in rule:
            # 需要获取该板块的最小价格单位
            tick_size = 0.01  # 简化处理
            abs_limit = base_price + rule['buy_limit_units'] * tick_size
        else:
            abs_limit = pct_limit

        if limit_type == 'min':
            return min(pct_limit, abs_limit)
        elif limit_type == 'max':
            return max(pct_limit, abs_limit)
        else:
            return max(pct_limit, abs_limit)

    @staticmethod
    def _calculate_sell_limit(base_price: float, rule: Dict, board: str) -> float:
        """计算卖出价格下限"""
        limit_type = rule.get('sell_type', 'max')

        pct_limit = base_price * rule.get('sell_limit_pct', 0.98)

        if 'sell_limit_abs' in rule:
            abs_limit = base_price - rule['sell_limit_abs']
        elif 'sell_limit_units' in rule:
            tick_size = 0.01
            abs_limit = base_price - rule['sell_limit_units'] * tick_size
        else:
            abs_limit = pct_limit

        if limit_type == 'min':
            return min(pct_limit, abs_limit)
        elif limit_type == 'max':
            return max(pct_limit, abs_limit)
        else:
            return min(pct_limit, abs_limit)


class ETFLotSizeCalculator:
    """最小交易单位计算器"""

    @staticmethod
    def get_lot_size_info(symbol: str, query_date: date) -> Dict:
        """获取最小交易单位信息

        Args:
            symbol: ETF代码
            query_date: 查询日期

        Returns:
            {
                'min_buy': 最小买入数量,
                'buy_increment': 买入增量,
                'sell_allow_fractional': 是否允许零股卖出,
                'sell_min': 最小卖出数量,
                'sell_below_min_buy': 卖出可否低于最小买入量,
            }
        """
        board = get_etf_board_type(symbol)
        exchange = get_exchange_from_symbol(symbol)

        # 获取规则
        rule = RuleVersion.get_applicable_rule(
            LOT_SIZE_VERSIONS, board, query_date, exchange
        )

        if rule is None:
            # 默认规则：100股整数倍
            return {
                'min_buy': 100,
                'buy_increment': 100,
                'sell_allow_fractional': True,
                'sell_min': 1,
                'sell_below_min_buy': True,
            }

        return rule

    @staticmethod
    def normalize_order_volume(symbol: str, volume: float, side: str,
                              query_date: date) -> Tuple[float, str]:
        """标准化订单数量

        Args:
            symbol: ETF代码
            volume: 原始数量
            side: 订单方向 ('buy' or 'sell')
            query_date: 查询日期

        Returns:
            (标准化后的数量, 消息)
        """
        lot_info = ETFLotSizeCalculator.get_lot_size_info(symbol, query_date)

        if side == 'buy':
            min_volume = lot_info['min_buy']
            increment = lot_info['buy_increment']

            # 向上取整到最小单位
            normalized = math.ceil(volume / increment) * increment

            if normalized < min_volume:
                return 0, f'买入数量不得低于 {min_volume} 股'

            return normalized, 'OK'
        else:
            # 卖出可以零股
            sell_min = lot_info.get('sell_min', 1)

            if volume < sell_min:
                return 0, f'卖出数量不得低于 {sell_min} 股'

            return volume, 'OK'


class ETFCommissionCalculator:
    """手续费计算器"""

    @staticmethod
    def get_commission_info(symbol: str, query_date: date = None) -> Dict:
        """获取手续费率信息

        Args:
            symbol: ETF代码
            query_date: 查询日期

        Returns:
            {
                'commission_rate': 佣金率,
                'min_commission': 最低佣金,
                'stamp_tax_buy': 买入印花税率,
                'stamp_tax_sell': 卖出印花税率,
            }
        """
        if query_date is None:
            query_date = date.today()

        etf_type = get_etf_type(symbol)
        exchange = get_exchange_from_symbol(symbol)

        # 获取规则
        rule = RuleVersion.get_applicable_rule(
            COMMISSION_VERSIONS, 'all', query_date, exchange
        )

        if rule is None:
            # 根据ETF类型返回默认规则
            if etf_type == 'money':
                return {
                    'commission_rate': 0.0,
                    'min_commission': 0.0,
                    'stamp_tax_buy': 0.0,
                    'stamp_tax_sell': 0.0,
                }
            elif etf_type in ['bond', 'cross_border']:
                return {
                    'commission_rate': 0.0003,
                    'min_commission': 5.0,
                    'stamp_tax_buy': 0.0,
                    'stamp_tax_sell': 0.0,
                }
            else:
                return {
                    'commission_rate': 0.0003,
                    'min_commission': 5.0,
                    'stamp_tax_buy': 0.0,
                    'stamp_tax_sell': 0.0,  # ETF免征印花税
                }

        # 根据ETF类型返回规则
        type_key = f'{etf_type}_etf'
        if type_key in rule:
            return rule[type_key]

        # 默认返回股票ETF规则
        default_rule = rule.get('stock_etf', {
            'commission_rate': 0.0003,
            'min_commission': 5.0,
            'stamp_tax_buy': 0.0,
            'stamp_tax_sell': 0.0,  # ETF免征印花税
        })

        # 根据ETF类型调整印花税
        if etf_type == 'money':
            default_rule['stamp_tax_sell'] = 0.0
        elif etf_type in ['bond', 'cross_border']:
            default_rule['stamp_tax_sell'] = 0.0

        return default_rule

    @staticmethod
    def calculate_commission(symbol: str, volume: float, price: float,
                            side: str, query_date: date = None) -> Dict[str, float]:
        """计算手续费

        Args:
            symbol: ETF代码
            volume: 数量
            price: 价格
            side: 方向 ('buy' or 'sell')
            query_date: 查询日期

        Returns:
            {
                'commission': 佣金,
                'stamp_tax': 印花税,
                'total_fee': 总费用,
            }
        """
        commission_info = ETFCommissionCalculator.get_commission_info(symbol, query_date)

        amount = volume * price

        # 计算佣金
        commission = amount * commission_info['commission_rate']
        min_commission = commission_info['min_commission']
        commission = max(commission, min_commission)

        # 计算印花税
        if side == 'buy':
            stamp_tax = amount * commission_info['stamp_tax_buy']
        else:
            stamp_tax = amount * commission_info['stamp_tax_sell']

        return {
            'commission': commission,
            'stamp_tax': stamp_tax,
            'total_fee': commission + stamp_tax,
        }


# ============================================================================
# 除权除息计算器
# ============================================================================
class ETFDividendAdjuster:
    """分红除权调整计算器"""

    @staticmethod
    def calculate_ex_dividend_price(close_price: float, dividend_per_share: float) -> float:
        """计算除息价

        Args:
            close_price: 收盘价
            dividend_per_share: 每股现金红利

        Returns:
            除息价（保留2位小数，第三位直接截断）
        """
        ex_price = close_price - dividend_per_share

        # 保留2位小数，第三位直接截断
        dec_price = Decimal(str(ex_price))
        truncated = dec_price.quantize(Decimal('0.01'), rounding=ROUND_DOWN)

        return float(truncated)

    @staticmethod
    def calculate_ex_split_price(close_price: float, split_ratio: float) -> float:
        """计算除权价（送股/转增）

        Args:
            close_price: 收盘价
            split_ratio: 送转比例（如10送10则为1.0）

        Returns:
            除权价（保留2位小数，第三位四舍五入）
        """
        ex_price = close_price / (1 + split_ratio)

        # 保留2位小数，第三位四舍五入
        return round(ex_price, 2)

    @staticmethod
    def calculate_ex_combined_price(close_price: float, dividend_per_share: float,
                                  split_ratio: float) -> float:
        """计算复合除权除息价

        Args:
            close_price: 收盘价
            dividend_per_share: 每股现金红利
            split_ratio: 送转比例

        Returns:
            除权除息价（保留2位小数，第五位直接截断）
        """
        ex_price = (close_price - dividend_per_share) / (1 + split_ratio)

        # 保留2位小数，第五位直接截断
        dec_price = Decimal(str(ex_price))
        truncated = dec_price.quantize(Decimal('0.00001'), rounding=ROUND_DOWN)
        truncated = truncated.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        return float(truncated)


if __name__ == '__main__':
    # 测试代码
    test_date = date(2024, 1, 15)

    # 测试涨跌停价计算
    print("=== 涨跌停价计算测试 ===")
    test_cases = [
        ('510300.SH', 4.500),  # 主板ETF
        ('588000.SH', 1.200),  # 科创板ETF
        ('159915.SZ', 2.300),  # 创业板ETF
    ]

    for symbol, prev_close in test_cases:
        result = ETFPriceCalculator.calculate_limit_prices(symbol, prev_close, test_date)
        print(f"{symbol} (前收盘: {prev_close}):")
        print(f"  涨停: {result['upper_limit']}, 跌停: {result['lower_limit']}, 涨跌幅: {result['limit_ratio']}")

    # 测试最小交易单位
    print("\n=== 最小交易单位测试 ===")
    for symbol, _ in test_cases:
        info = ETFLotSizeCalculator.get_lot_size_info(symbol, test_date)
        print(f"{symbol}: 最小买入={info['min_buy']}, 增量={info['buy_increment']}")

    # 测试手续费
    print("\n=== 手续费计算测试 ===")
    for symbol, _ in test_cases:
        fee = ETFCommissionCalculator.calculate_commission(symbol, 1000, 4.5, 'buy', test_date)
        print(f"{symbol}: 佣金={fee['commission']}, 印花税={fee['stamp_tax']}, 总费用={fee['total_fee']}")
