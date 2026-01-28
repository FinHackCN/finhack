"""
美股交易规则计算器

实现熔断机制计算、零碎股计算、手续费计算等
"""

import math
from datetime import date, time
from typing import Dict, Tuple, Optional, Union
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN, ROUND_UP

# 尝试相对导入
try:
    from .us_trading_rules_versions import (
        PRICE_LIMIT_VERSIONS,
        LOT_SIZE_VERSIONS,
        SHORT_SALE_RULES_VERSIONS,
        PDT_RULE_VERSIONS,
        SETTLEMENT_VERSIONS,
        PRICE_PRECISION_VERSIONS,
        COMMISSION_VERSIONS,
        RuleVersion,
        get_exchange_from_symbol,
        get_stock_type,
    )
except ImportError:
    from us_trading_rules_versions import (
        PRICE_LIMIT_VERSIONS,
        LOT_SIZE_VERSIONS,
        SHORT_SALE_RULES_VERSIONS,
        PDT_RULE_VERSIONS,
        SETTLEMENT_VERSIONS,
        PRICE_PRECISION_VERSIONS,
        COMMISSION_VERSIONS,
        RuleVersion,
        get_exchange_from_symbol,
        get_stock_type,
    )


class USCircuitBreakerCalculator:
    """熔断机制计算器"""

    @staticmethod
    def check_circuit_breaker(index_level: float, previous_close: float,
                             trigger_time: time, query_date: date) -> Dict:
        """检查是否触发市场级熔断

        Args:
            index_level: S&P 500当前点位
            previous_close: S&P 500前收盘价
            trigger_time: 触发时间
            query_date: 查询日期

        Returns:
            {
                'triggered': 是否触发,
                'level': 触发级别 (1/2/3),
                'decline_pct': 下跌百分比,
                'pause_duration': 暂停时长（分钟）,
                'action': 需要采取的行动
            }
        """
        # 获取熔断规则
        rule = RuleVersion.get_applicable_rule(
            PRICE_LIMIT_VERSIONS, query_date, None
        )

        if rule is None:
            return {
                'triggered': False,
                'level': None,
                'decline_pct': 0,
                'pause_duration': 0,
                'action': 'no_breaker'
            }

        # 计算下跌百分比
        decline_pct = (previous_close - index_level) / previous_close

        # 检查是否触发各级别熔断
        level3_threshold = rule.get('circuit_breaker_level3', 0.20)
        if decline_pct >= level3_threshold:
            return {
                'triggered': True,
                'level': 3,
                'decline_pct': decline_pct,
                'pause_duration': rule.get('level3_pause', 'rest_of_day'),
                'action': 'market_closed'
            }

        level2_threshold = rule.get('circuit_breaker_level2', 0.13)
        if decline_pct >= level2_threshold:
            return {
                'triggered': True,
                'level': 2,
                'decline_pct': decline_pct,
                'pause_duration': rule.get('level2_pause', 15),
                'action': 'pause_15min'
            }

        level1_threshold = rule.get('circuit_breaker_level1', 0.07)
        if decline_pct >= level1_threshold:
            # 如果在下午3:25后触发Level 1，不暂停
            if trigger_time >= time(15, 25):
                return {
                    'triggered': True,
                    'level': 1,
                    'decline_pct': decline_pct,
                    'pause_duration': 0,
                    'action': 'no_pause_after_325'
                }
            return {
                'triggered': True,
                'level': 1,
                'decline_pct': decline_pct,
                'pause_duration': rule.get('level1_pause', 15),
                'action': 'pause_15min'
            }

        return {
            'triggered': False,
            'level': None,
            'decline_pct': decline_pct,
            'pause_duration': 0,
            'action': 'no_breaker'
        }

    @staticmethod
    def check_individual_pause(symbol: str, current_price: float,
                              reference_price: float, query_date: date) -> Dict:
        """检查个股是否触发限制暂停

        Args:
            symbol: 股票代码
            current_price: 当前价格
            reference_price: 参考价（前收盘价）
            query_date: 查询日期

        Returns:
            {
                'triggered': 是否触发,
                'move_pct': 涨跌幅百分比,
                'pause_duration': 暂停时长
            }
        """
        rule = RuleVersion.get_applicable_rule(
            PRICE_LIMIT_VERSIONS, query_date, None
        )

        if rule is None:
            return {
                'triggered': False,
                'move_pct': 0,
                'pause_duration': 0
            }

        threshold = rule.get('individual_security_pause', 0.05)
        pause_duration = rule.get('individual_pause_duration', 5)

        move_pct = abs(current_price - reference_price) / reference_price

        if move_pct >= threshold:
            return {
                'triggered': True,
                'move_pct': move_pct,
                'pause_duration': pause_duration
            }

        return {
            'triggered': False,
            'move_pct': move_pct,
            'pause_duration': 0
        }


class USLotSizeCalculator:
    """最小交易单位计算器（支持零碎股）"""

    @staticmethod
    def get_lot_size_info(symbol: str, query_date: date) -> Dict:
        """获取最小交易单位信息

        Args:
            symbol: 股票代码
            query_date: 查询日期

        Returns:
            {
                'min_buy': 最小买入数量,
                'buy_increment': 买入增量,
                'sell_allow_fractional': 是否允许零碎股卖出,
                'sell_min': 最小卖出数量,
                'fractional_shares': 是否支持零碎股,
            }
        """
        rule = RuleVersion.get_applicable_rule(
            LOT_SIZE_VERSIONS, query_date, None
        )

        if rule is None:
            # 默认规则：支持1股和零碎股
            return {
                'min_buy': 1,
                'buy_increment': 1,
                'sell_allow_fractional': True,
                'sell_min': 0.0001,
                'fractional_shares': True,
                'fractional_increment': 0.0001,
            }

        return rule

    @staticmethod
    def normalize_order_volume(symbol: str, volume: float, side: str,
                              query_date: date) -> Tuple[float, str]:
        """标准化订单数量

        Args:
            symbol: 股票代码
            volume: 原始数量
            side: 订单方向 ('buy' or 'sell')
            query_date: 查询日期

        Returns:
            (标准化后的数量, 消息)
        """
        lot_info = USLotSizeCalculator.get_lot_size_info(symbol, query_date)

        if side == 'buy':
            min_volume = lot_info['min_buy']

            if volume < min_volume:
                if lot_info.get('fractional_shares', False):
                    # 支持零碎股，允许小于1股的买入
                    fractional_min = lot_info.get('fractional_increment', 0.0001)
                    if volume < fractional_min:
                        return 0, f'买入数量不得低于 {fractional_min} 股'
                    return volume, 'OK'
                else:
                    return 0, f'买入数量不得低于 {min_volume} 股'

            return volume, 'OK'
        else:
            # 卖出支持零碎股
            sell_min = lot_info.get('sell_min', 0.0001)

            if volume < sell_min:
                return 0, f'卖出数量不得低于 {sell_min} 股'

            return volume, 'OK'


class USShortSaleCalculator:
    """卖空规则计算器"""

    @staticmethod
    def check_short_sale_restriction(symbol: str, daily_change_pct: float,
                                     bid_price: float, query_date: date) -> Dict:
        """检查卖空限制（Rule 201 提价规则）

        Args:
            symbol: 股票代码
            daily_change_pct: 当日涨跌幅（负数表示下跌）
            bid_price: 当前最优买价
            query_date: 查询日期

        Returns:
            {
                'restricted': 是否受限,
                'rule': 适用规则,
                'min_price': 最低卖空价
            }
        """
        rule = RuleVersion.get_applicable_rule(
            SHORT_SALE_RULES_VERSIONS, query_date, None
        )

        if rule is None:
            return {
                'restricted': False,
                'rule': None,
                'min_price': 0
            }

        # 检查是否触发限制
        threshold = rule.get('circuit_breaker_threshold', -0.10)

        if daily_change_pct <= threshold:
            # 触发限制，卖空价需高于当前最优买价
            return {
                'restricted': True,
                'rule': 'rule_201',
                'min_price': bid_price if rule.get('require_higher_price', True) else 0
            }

        return {
            'restricted': False,
            'rule': rule.get('short_sale_rule', None),
            'min_price': 0
        }


class USPDTChecker:
    """PDT规则检查器（日内回转交易限制）"""

    @staticmethod
    def check_pdt_restriction(account_equity: float, day_trades_count: int,
                             query_date: date) -> Dict:
        """检查PDT规则限制

        Args:
            account_equity: 账户净值
            day_trades_count: 滚动窗口内的日内交易次数
            query_date: 查询日期

        Returns:
            {
                'restricted': 是否受限,
                'threshold': 门槛金额,
                'max_day_trades': 最大日内交易次数,
                'current_trades': 当前交易次数,
                'remaining_trades': 剩余交易次数
            }
        """
        rule = RuleVersion.get_applicable_rule(
            PDT_RULE_VERSIONS, query_date, None
        )

        if rule is None:
            return {
                'restricted': False,
                'threshold': 25000,
                'max_day_trades': 3,
                'current_trades': day_trades_count,
                'remaining_trades': float('inf')
            }

        threshold = rule.get('account_threshold', 25000)
        max_trades = rule.get('max_day_trades', 3)

        if account_equity < threshold:
            remaining = max(0, max_trades - day_trades_count)
            return {
                'restricted': True,
                'threshold': threshold,
                'max_day_trades': max_trades,
                'current_trades': day_trades_count,
                'remaining_trades': remaining
            }

        return {
            'restricted': False,
            'threshold': threshold,
            'max_day_trades': max_trades,
            'current_trades': day_trades_count,
            'remaining_trades': float('inf')
        }


class USPriceCalculator:
    """价格计算器"""

    @staticmethod
    def round_price(price: float, symbol: str, query_date: date) -> float:
        """按规则对价格进行四舍五入

        Args:
            price: 原始价格
            symbol: 股票代码
            query_date: 查询日期

        Returns:
            四舍五入后的价格
        """
        if price == float('inf') or price == 0:
            return price

        rule = RuleVersion.get_applicable_rule(
            PRICE_PRECISION_VERSIONS, query_date, None
        )

        if rule is None:
            # 默认：保留2位小数，四舍五入
            return round(price, 2)

        decimals = rule.get('rounding_decimals', 2)
        rounding_method = rule.get('rounding_method', 'round')

        # 使用Decimal进行精确计算
        dec_price = Decimal(str(price))

        if rounding_method == 'round':
            result = dec_price.quantize(Decimal(f'1e-{decimals}'), rounding=ROUND_HALF_UP)
        elif rounding_method == 'ceil':
            result = dec_price.quantize(Decimal(f'1e-{decimals}'), rounding=ROUND_UP)
        elif rounding_method == 'truncate':
            result = dec_price.quantize(Decimal(f'1e-{decimals}'), rounding=ROUND_DOWN)
        else:
            result = dec_price.quantize(Decimal(f'1e-{decimals}'))

        return float(result)


class USCommissionCalculator:
    """手续费计算器"""

    @staticmethod
    def get_commission_info(symbol: str, query_date: date = None) -> Dict:
        """获取手续费率信息

        Args:
            symbol: 股票代码
            query_date: 查询日期

        Returns:
            {
                'commission_rate': 佣金率,
                'min_commission': 最低佣金,
                'per_share_fee': 每股费用,
                'sec_fee': SEC费用率,
                'trading_activity_fee': 交易活动费率,
            }
        """
        if query_date is None:
            query_date = date.today()

        stock_type = get_stock_type(symbol)

        # 获取规则
        rule = RuleVersion.get_applicable_rule(
            COMMISSION_VERSIONS, query_date, None
        )

        if rule is None:
            # 默认零佣金
            return {
                'commission_rate': 0.0,
                'min_commission': 0.0,
                'per_share_fee': 0.0,
                'sec_fee': 0.0000131,
                'trading_activity_fee': 0.000119,
                'stamp_tax_buy': 0.0,
                'stamp_tax_sell': 0.0,
            }

        # 根据股票类型返回规则
        type_key = stock_type if stock_type in rule else 'stock'
        return rule.get(type_key, rule['stock'])

    @staticmethod
    def calculate_commission(symbol: str, volume: float, price: float,
                            side: str, query_date: date = None) -> Dict[str, float]:
        """计算手续费

        Args:
            symbol: 股票代码
            volume: 数量
            price: 价格
            side: 方向 ('buy' or 'sell')
            query_date: 查询日期

        Returns:
            {
                'commission': 佣金,
                'sec_fee': SEC费用,
                'trading_activity_fee': 交易活动费,
                'total_fee': 总费用,
            }
        """
        commission_info = USCommissionCalculator.get_commission_info(symbol, query_date)

        amount = volume * price

        # 计算佣金
        commission = amount * commission_info['commission_rate']
        per_share_fee = volume * commission_info.get('per_share_fee', 0.0)
        commission = max(commission + per_share_fee, commission_info['min_commission'])

        # SEC费用和交易活动费（仅卖出收取）
        sec_fee = 0.0
        trading_activity_fee = 0.0

        if side == 'sell':
            sec_fee = amount * commission_info.get('sec_fee', 0.0000131)
            # SEC费用有最低和最高限制
            sec_fee = max(min(sec_fee, 5.9565), 0.01)

            trading_activity_fee = amount * commission_info.get('trading_activity_fee', 0.000119)
            # 交易活动费有最低和最高限制
            trading_activity_fee = max(min(trading_activity_fee, 5.95), 0.0001)

        return {
            'commission': commission,
            'sec_fee': sec_fee,
            'trading_activity_fee': trading_activity_fee,
            'total_fee': commission + sec_fee + trading_activity_fee,
        }


class USSettlementCalculator:
    """结算周期计算器"""

    @staticmethod
    def get_settlement_info(query_date: date) -> Dict:
        """获取结算周期信息

        Args:
            query_date: 查询日期

        Returns:
            {
                'settlement_cycle': 结算周期 ('T+1', 'T+2'),
                'settlement_date': 结算日期
            }
        """
        rule = RuleVersion.get_applicable_rule(
            SETTLEMENT_VERSIONS, query_date, None
        )

        if rule is None:
            cycle = 'T+2'
        else:
            cycle = rule.get('settlement_cycle', 'T+2')

        # 计算结算日期（简单处理，实际需要考虑节假日）
        if cycle == 'T+1':
            from datetime import timedelta
            settlement_date = query_date + timedelta(days=1)
        else:  # T+2
            from datetime import timedelta
            settlement_date = query_date + timedelta(days=2)

        return {
            'settlement_cycle': cycle,
            'settlement_date': settlement_date
        }


if __name__ == '__main__':
    # 测试代码
    test_date = date(2024, 6, 1)

    # 测试零碎股
    print("=== 零碎股计算测试 ===")
    info = USLotSizeCalculator.get_lot_size_info('AAPL', test_date)
    print(f"AAPL: {info}")

    # 测试手续费
    print("\n=== 手续费计算测试 ===")
    fee = USCommissionCalculator.calculate_commission('AAPL', 100, 175.0, 'buy', test_date)
    print(f"AAPL买入100股@175: {fee}")

    fee = USCommissionCalculator.calculate_commission('AAPL', 100, 175.0, 'sell', test_date)
    print(f"AAPL卖出100股@175: {fee}")

    # 测试结算周期
    print("\n=== 结算周期测试 ===")
    settlement_2023 = USSettlementCalculator.get_settlement_info(date(2023, 6, 1))
    print(f"2023年: {settlement_2023}")

    settlement_2024 = USSettlementCalculator.get_settlement_info(date(2024, 6, 1))
    print(f"2024年: {settlement_2024}")

    # 测试熔断
    print("\n=== 熔断测试 ===")
    breaker = USCircuitBreakerCalculator.check_circuit_breaker(
        index_level=4500, previous_close=5000,
        trigger_time=time(10, 0), query_date=test_date
    )
    print(f"S&P 500下跌10%: {breaker}")
