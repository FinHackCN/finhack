"""
期货交易规则计算器

实现期货涨跌停价计算、保证金计算、手续费计算、持仓限制验证等
"""

import math
from datetime import date, datetime
from typing import Dict, Tuple, Optional, List
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN, ROUND_UP, ROUND_CEILING, ROUND_FLOOR

# 尝试相对导入，如果失败则使用绝对导入
try:
    from .future_trading_rules_versions import (
        get_future_exchange,
        get_future_type,
        get_future_product,
        get_tick_size,
        get_tick_value,
        get_contract_size,
        get_price_limit,
        get_margin_ratio,
        get_commission_info,
        SPECIFIC_PRICE_LIMITS,
        TICK_SIZE_RULES,
        POSITION_LIMITS,
        DELIVERY_MONTH_RESTRICTIONS,
        RuleVersion,
    )
except ImportError:
    from future_trading_rules_versions import (
        get_future_exchange,
        get_future_type,
        get_future_product,
        get_tick_size,
        get_tick_value,
        get_contract_size,
        get_price_limit,
        get_margin_ratio,
        get_commission_info,
        SPECIFIC_PRICE_LIMITS,
        TICK_SIZE_RULES,
        POSITION_LIMITS,
        DELIVERY_MONTH_RESTRICTIONS,
        RuleVersion,
    )


class FuturePriceCalculator:
    """期货价格计算器"""

    @staticmethod
    def calculate_limit_prices(symbol: str, prev_close: float, query_date: date,
                              is_new_contract: bool = False) -> Dict[str, any]:
        """计算期货涨跌停价格

        Args:
            symbol: 期货代码
            prev_close: 前结算价
            query_date: 查询日期
            is_new_contract: 是否为新合约（首日交易）

        Returns:
            {
                'upper_limit': 涨停价,
                'lower_limit': 跌停价,
                'limit_ratio': 涨跌幅比例,
                'has_limit': 是否有涨跌停限制,
                'tick_size': 最小变动价位,
            }
        """
        product = get_future_product(symbol)
        exchange = get_future_exchange(symbol)

        # 获取涨跌幅限制
        limit_ratio = get_price_limit(symbol)

        # 检查是否为国债期货（无涨跌停）
        has_limit = limit_ratio is not None

        # 新合约首日可能有不同的涨跌幅
        if is_new_contract and product in SPECIFIC_PRICE_LIMITS:
            first_day_limit = SPECIFIC_PRICE_LIMITS[product].get('first_day_limit')
            if first_day_limit:
                limit_ratio = first_day_limit

        # 获取最小变动价位
        tick_size = get_tick_size(symbol)

        if not has_limit:
            # 无涨跌停限制（如国债期货）
            return {
                'upper_limit': float('inf'),
                'lower_limit': 0.0,
                'limit_ratio': None,
                'has_limit': False,
                'tick_size': tick_size,
            }

        # 计算原始涨跌停价
        raw_upper = prev_close * (1 + limit_ratio)
        raw_lower = prev_close * (1 - limit_ratio)

        # 按Tick Size取整
        upper_limit = FuturePriceCalculator._round_to_tick(
            raw_upper, tick_size, direction='down'  # 涨停价向下取整
        )
        lower_limit = FuturePriceCalculator._round_to_tick(
            raw_lower, tick_size, direction='up'  # 跌停价向上取整
        )

        return {
            'upper_limit': upper_limit,
            'lower_limit': lower_limit,
            'limit_ratio': limit_ratio,
            'has_limit': has_limit,
            'tick_size': tick_size,
        }

    @staticmethod
    def _round_to_tick(price: float, tick_size: float,
                      direction: str = 'nearest') -> float:
        """按Tick Size对价格进行取整

        Args:
            price: 原始价格
            tick_size: 最小变动价位
            direction: 取整方向 ('up', 'down', 'nearest')

        Returns:
            取整后的价格
        """
        if price == float('inf') or price == 0:
            return price

        # 计算Tick数量
        tick_count = price / tick_size

        if direction == 'up':
            # 向上取整
            rounded_ticks = math.ceil(tick_count)
        elif direction == 'down':
            # 向下取整
            rounded_ticks = math.floor(tick_count)
        else:
            # 四舍五入
            rounded_ticks = round(tick_count)

        return rounded_ticks * tick_size

    @staticmethod
    def validate_order_price(symbol: str, order_price: float,
                           reference_price: float = None,
                           prev_settlement: float = None) -> Dict:
        """验证订单价格是否有效

        Args:
            symbol: 期货代码
            order_price: 订单价格
            reference_price: 参考价（最新价或买一/卖一价）
            prev_settlement: 前结算价

        Returns:
            {
                'valid': 是否有效,
                'message': 错误信息,
                'limit_upper': 涨停价,
                'limit_lower': 跌停价,
            }
        """
        # 使用前结算价计算涨跌停
        ref_price = prev_settlement or reference_price

        if ref_price is None or ref_price <= 0:
            return {
                'valid': True,
                'message': '无法获取参考价',
                'limit_upper': None,
                'limit_lower': None,
            }

        # 计算涨跌停价
        limits = FuturePriceCalculator.calculate_limit_prices(
            symbol, ref_price, date.today()
        )

        # 检查价格是否在涨跌停范围内
        if limits['has_limit']:
            if order_price > limits['upper_limit']:
                return {
                    'valid': False,
                    'message': f'买入价超过涨停价 {limits["upper_limit"]:.2f}',
                    'limit_upper': limits['upper_limit'],
                    'limit_lower': limits['lower_limit'],
                }
            elif order_price < limits['lower_limit']:
                return {
                    'valid': False,
                    'message': f'卖出价低于跌停价 {limits["lower_limit"]:.2f}',
                    'limit_upper': limits['upper_limit'],
                    'limit_lower': limits['lower_limit'],
                }

        # 检查是否符合Tick Size
        tick_size = limits['tick_size']
        tick_count = order_price / tick_size
        if not abs(tick_count - round(tick_count)) < 1e-6:
            return {
                'valid': False,
                'message': f'价格必须是{tick_size}的整数倍',
                'limit_upper': limits['upper_limit'],
                'limit_lower': limits['lower_limit'],
            }

        return {
            'valid': True,
            'message': '',
            'limit_upper': limits['upper_limit'],
            'limit_lower': limits['lower_limit'],
        }


class FutureMarginCalculator:
    """保证金计算器"""

    @staticmethod
    def calculate_margin(symbol: str, volume: int, price: float,
                        margin_type: str = 'initial',
                        query_date: date = None) -> Dict[str, float]:
        """计算保证金

        Args:
            symbol: 期货代码
            volume: 手数
            price: 价格
            margin_type: 保证金类型 ('initial' or 'maintenance')
            query_date: 查询日期

        Returns:
            {
                'margin_ratio': 保证金比例,
                'contract_value': 合约价值,
                'margin': 保证金金额,
                'contract_size': 合约单位,
            }
        """
        if query_date is None:
            query_date = date.today()

        # 获取保证金比例
        margin_ratio = get_margin_ratio(symbol, margin_type)

        # 获取合约单位
        contract_size = get_contract_size(symbol)

        # 计算合约价值
        contract_value = volume * price * contract_size

        # 计算保证金
        margin = contract_value * margin_ratio

        return {
            'margin_ratio': margin_ratio,
            'contract_value': contract_value,
            'margin': margin,
            'contract_size': contract_size,
        }

    @staticmethod
    def calculate_position_margin(symbol: str, position: int,
                                 entry_price: float, current_price: float,
                                 query_date: date = None) -> Dict:
        """计算持仓保证金和盈亏

        Args:
            symbol: 期货代码
            position: 持仓手数（正数为多头，负数为空头）
            entry_price: 开仓价
            current_price: 当前价
            query_date: 查询日期

        Returns:
            {
                'initial_margin': 初始保证金,
                'maintenance_margin': 维持保证金,
                'unrealized_pnl': 浮动盈亏,
                'pnl_ratio': 盈亏比例,
            }
        """
        if query_date is None:
            query_date = date.today()

        volume = abs(position)

        # 计算初始保证金和维持保证金
        initial_info = FutureMarginCalculator.calculate_margin(
            symbol, volume, entry_price, 'initial', query_date
        )
        maintenance_info = FutureMarginCalculator.calculate_margin(
            symbol, volume, current_price, 'maintenance', query_date
        )

        # 计算浮动盈亏
        contract_size = get_contract_size(symbol)
        if position > 0:
            # 多头盈亏
            unrealized_pnl = (current_price - entry_price) * volume * contract_size
        else:
            # 空头盈亏
            unrealized_pnl = (entry_price - current_price) * volume * contract_size

        # 盈亏比例
        pnl_ratio = unrealized_pnl / initial_info['margin'] if initial_info['margin'] > 0 else 0

        return {
            'initial_margin': initial_info['margin'],
            'maintenance_margin': maintenance_info['margin'],
            'unrealized_pnl': unrealized_pnl,
            'pnl_ratio': pnl_ratio,
        }

    @staticmethod
    def check_margin_call(symbol: str, position: int,
                         entry_price: float, current_price: float,
                         account_balance: float, query_date: date = None) -> Dict:
        """检查是否需要追加保证金

        Args:
            symbol: 期货代码
            position: 持仓手数
            entry_price: 开仓价
            current_price: 当前价
            account_balance: 账户余额
            query_date: 查询日期

        Returns:
            {
                'need_margin_call': 是否需要追加保证金,
                'shortfall': 保证金缺口,
                'maintenance_margin': 维持保证金,
                'current_equity': 当前权益,
            }
        """
        if query_date is None:
            query_date = date.today()

        # 计算持仓保证金和盈亏
        margin_info = FutureMarginCalculator.calculate_position_margin(
            symbol, position, entry_price, current_price, query_date
        )

        # 当前权益 = 账户余额 + 浮动盈亏 - 初始保证金
        current_equity = account_balance + margin_info['unrealized_pnl']

        # 维持保证金
        maintenance_margin = margin_info['maintenance_margin']

        # 检查是否需要追加保证金
        need_margin_call = current_equity < maintenance_margin

        # 保证金缺口
        shortfall = max(0, maintenance_margin - current_equity)

        return {
            'need_margin_call': need_margin_call,
            'shortfall': shortfall,
            'maintenance_margin': maintenance_margin,
            'current_equity': current_equity,
        }


class FutureLotSizeCalculator:
    """合约单位计算器"""

    @staticmethod
    def get_lot_size_info(symbol: str) -> Dict:
        """获取合约单位信息

        Args:
            symbol: 期货代码

        Returns:
            {
                'lot_size': 合约单位（手/张）,
                'contract_size': 合约乘数/单位,
                'tick_size': 最小变动价位,
                'tick_value': 最小变动价值,
                'min_volume': 最小下单量（手）,
            }
        """
        product = get_future_product(symbol)

        # 获取合约单位
        contract_size = get_contract_size(symbol)

        # 获取最小变动价位
        tick_size = get_tick_size(symbol)

        # 计算最小变动价值
        tick_value = tick_size * contract_size

        # 期货最小下单量为1手
        min_volume = 1

        return {
            'lot_size': 1,  # 期货以1手为单位
            'contract_size': contract_size,
            'tick_size': tick_size,
            'tick_value': tick_value,
            'min_volume': min_volume,
        }

    @staticmethod
    def normalize_order_volume(symbol: str, volume: float) -> Tuple[int, str]:
        """标准化订单数量

        Args:
            symbol: 期货代码
            volume: 原始数量

        Returns:
            (标准化后的数量, 消息)
        """
        # 期货必须为整数手
        if not isinstance(volume, int) or volume != int(volume):
            volume = int(volume)

        if volume < 1:
            return 0, '最小下单量为1手'

        return volume, 'OK'


class FutureCommissionCalculator:
    """手续费计算器"""

    @staticmethod
    def calculate_commission(symbol: str, volume: int, price: float,
                           offset_flag: str = 'open',
                           query_date: date = None) -> Dict[str, float]:
        """计算手续费

        Args:
            symbol: 期货代码
            volume: 手数
            price: 价格
            offset_flag: 开平标志 ('open'=开仓, 'close'=平仓, 'close_today'=平今)
            query_date: 查询日期

        Returns:
            {
                'commission': 手续费,
                'commission_rate': 手续费率,
                'contract_value': 合约价值,
                'min_commission': 最低手续费,
            }
        """
        if query_date is None:
            query_date = date.today()

        # 获取手续费配置
        commission_info = get_commission_info(symbol)

        # 确定手续费率
        if offset_flag == 'open':
            commission_rate = commission_info['open']
        elif offset_flag == 'close':
            commission_rate = commission_info['close']
        elif offset_flag == 'close_today':
            commission_rate = commission_info['close_today']
        else:
            commission_rate = commission_info['open']

        # 获取合约单位
        contract_size = get_contract_size(symbol)

        # 计算合约价值
        contract_value = volume * price * contract_size

        # 计算手续费
        commission = contract_value * commission_rate

        # 应用最低手续费
        min_commission = commission_info.get('min', 0)
        commission = max(commission, min_commission)

        return {
            'commission': commission,
            'commission_rate': commission_rate,
            'contract_value': contract_value,
            'min_commission': min_commission,
        }

    @staticmethod
    def calculate_total_commission(symbol: str, open_volume: int,
                                  open_price: float,
                                  close_volume: int,
                                  close_price: float,
                                  close_today: bool = False,
                                  query_date: date = None) -> Dict[str, float]:
        """计算开平仓总手续费

        Args:
            symbol: 期货代码
            open_volume: 开仓手数
            open_price: 开仓价
            close_volume: 平仓手数
            close_price: 平仓价
            close_today: 是否平今
            query_date: 查询日期

        Returns:
            {
                'open_commission': 开仓手续费,
                'close_commission': 平仓手续费,
                'total_commission': 总手续费,
            }
        """
        if query_date is None:
            query_date = date.today()

        # 开仓手续费
        open_result = FutureCommissionCalculator.calculate_commission(
            symbol, open_volume, open_price, 'open', query_date
        )

        # 平仓手续费
        offset_flag = 'close_today' if close_today else 'close'
        close_result = FutureCommissionCalculator.calculate_commission(
            symbol, close_volume, close_price, offset_flag, query_date
        )

        return {
            'open_commission': open_result['commission'],
            'close_commission': close_result['commission'],
            'total_commission': open_result['commission'] + close_result['commission'],
        }


class FutureDeliveryValidator:
    """交割月限制验证器"""

    @staticmethod
    def parse_delivery_month(symbol: str) -> Tuple[int, int]:
        """解析合约代码中的交割年月

        Args:
            symbol: 期货代码，如 'IF2406.CFFEX'

        Returns:
            (year, month) 交割年月
        """
        code = symbol.split('.')[0]

        # 提取品种代码
        product = get_future_product(symbol)

        # 剩余部分为交割月代码
        month_code = code[len(product):]

        # 解析年份和月份
        # 格式：YYMM，如2406表示2024年6月
        if len(month_code) >= 4:
            year = 2000 + int(month_code[:2])
            month = int(month_code[2:4])
            return (year, month)

        # 默认返回当前年月
        today = date.today()
        return (today.year, today.month)

    @staticmethod
    def is_delivery_month(symbol: str, current_date: date) -> bool:
        """判断当前是否为交割月

        Args:
            symbol: 期货代码
            current_date: 当前日期

        Returns:
            是否为交割月
        """
        delivery_year, delivery_month = FutureDeliveryValidator.parse_delivery_month(symbol)

        return current_date.year == delivery_year and current_date.month == delivery_month

    @staticmethod
    def is_near_delivery_month(symbol: str, current_date: date,
                              ahead_months: int = 2) -> bool:
        """判断是否临近交割月

        Args:
            symbol: 期货代码
            current_date: 当前日期
            ahead_months: 提前月数（默认2个月）

        Returns:
            是否临近交割月
        """
        delivery_year, delivery_month = FutureDeliveryValidator.parse_delivery_month(symbol)

        # 计算距离交割月的月数
        months_diff = (delivery_year - current_date.year) * 12 + (delivery_month - current_date.month)

        return 0 <= months_diff <= ahead_months

    @staticmethod
    def check_natural_person_ban(symbol: str, current_date: date,
                                trading_days_in_month: int = None) -> Dict:
        """检查自然人是否被禁止交易（临近交割月）

        Args:
            symbol: 期货代码
            current_date: 当前日期
            trading_days_in_month: 当月已交易天数（可选）

        Returns:
            {
                'is_banned': 是否被禁止,
                'reason': 禁止原因,
                'days_until_ban': 距离禁止的交易日数,
            }
        """
        exchange = get_future_exchange(symbol)
        delivery_year, delivery_month = FutureDeliveryValidator.parse_delivery_month(symbol)

        # 获取禁止规则
        ban_days_ahead = DELIVERY_MONTH_RESTRICTIONS['natural_person_ban_days'].get(exchange, 15)

        # 计算当前日期与交割月的距离
        if current_date.year > delivery_year:
            return {
                'is_banned': True,
                'reason': '合约已过期',
                'days_until_ban': 0,
            }

        if current_date.year == delivery_year:
            months_diff = delivery_month - current_date.month

            if months_diff < 0:
                # 已经过交割月
                return {
                    'is_banned': True,
                    'reason': '合约已过期',
                    'days_until_ban': 0,
                }
            elif months_diff == 0:
                # 在交割月
                return {
                    'is_banned': True,
                    'reason': '交割月自然人不能交易',
                    'days_until_ban': 0,
                }
            elif months_diff == 1:
                # 交割月前一个月
                # 这里简化处理，实际需要根据交易日历计算
                return {
                    'is_banned': False,
                    'reason': '',
                    'days_until_ban': ban_days_ahead,
                }

        # 未临近交割月
        return {
            'is_banned': False,
            'reason': '',
            'days_until_ban': None,
        }


class FuturePositionValidator:
    """持仓限制验证器"""

    @staticmethod
    def get_position_limit(symbol: str, position_type: str = 'speculator') -> int:
        """获取持仓限制

        Args:
            symbol: 期货代码
            position_type: 持仓类型 ('speculator'=投机, 'hedger'=套保, 'arbitrageur'=套利)

        Returns:
            持仓限制（手数）
        """
        product = get_future_product(symbol)

        if product in POSITION_LIMITS:
            return POSITION_LIMITS[product].get(position_type, 1000)

        # 默认限制
        return 1000

    @staticmethod
    def validate_position_limit(symbol: str, volume: int,
                               current_position: int = 0,
                               position_type: str = 'speculator') -> Dict:
        """验证持仓限制

        Args:
            symbol: 期货代码
            volume: 新增持仓手数（正数为多头，负数为空头）
            current_position: 当前持仓手数
            position_type: 持仓类型

        Returns:
            {
                'valid': 是否有效,
                'message': 错误信息,
                'current_position': 当前持仓,
                'new_position': 新持仓,
                'limit': 持仓限制,
            }
        """
        # 计算新持仓
        new_position = current_position + volume

        # 获取持仓限制
        limit = FuturePositionValidator.get_position_limit(symbol, position_type)

        # 检查是否超过限制
        if abs(new_position) > limit:
            return {
                'valid': False,
                'message': f'超过持仓限制 {limit} 手',
                'current_position': current_position,
                'new_position': new_position,
                'limit': limit,
            }

        return {
            'valid': True,
            'message': '',
            'current_position': current_position,
            'new_position': new_position,
            'limit': limit,
        }

    @staticmethod
    def check_delivery_month_position(symbol: str, current_date: date,
                                     position: int) -> Dict:
        """检查交割月持仓限制

        Args:
            symbol: 期货代码
            current_date: 当前日期
            position: 持仓手数

        Returns:
            {
                'need_reduce': 是否需要减仓,
                'max_position': 最大允许持仓,
                'message': 提示信息,
            }
        """
        # 检查是否临近交割月
        if not FutureDeliveryValidator.is_near_delivery_month(symbol, current_date, ahead_months=2):
            return {
                'need_reduce': False,
                'max_position': abs(position),
                'message': '',
            }

        # 临近交割月需要限制持仓
        # 这里简化处理，实际规则各交易所不同
        max_position = max(0, abs(position) - 1)  # 需要至少减少1手

        return {
            'need_reduce': True,
            'max_position': max_position,
            'message': f'临近交割月，需将持仓减至{max_position}手以下',
        }


if __name__ == '__main__':
    # 测试代码
    test_date = date(2024, 1, 15)

    # 测试涨跌停价计算
    print("=== 涨跌停价计算测试 ===")
    test_cases = [
        ('IF2406.CFFEX', 3500.0),  # 股指期货
        ('CU2406.SHFE', 68000.0),  # 铜期货
        ('RB2406.SHFE', 3800.0),   # 螺纹钢期货
    ]

    for symbol, prev_close in test_cases:
        result = FuturePriceCalculator.calculate_limit_prices(symbol, prev_close, test_date)
        print(f"{symbol} (前结算: {prev_close}):")
        print(f"  涨停: {result['upper_limit']}, 跌停: {result['lower_limit']}, 涨跌幅: {result['limit_ratio']}")
        print()

    # 测试保证金计算
    print("=== 保证金计算测试 ===")
    for symbol, prev_close in test_cases:
        margin = FutureMarginCalculator.calculate_margin(symbol, 10, prev_close, 'initial', test_date)
        print(f"{symbol}: 10手保证金 = {margin['margin']:.2f} 元")
        print()

    # 测试手续费计算
    print("=== 手续费计算测试 ===")
    for symbol, prev_close in test_cases:
        commission = FutureCommissionCalculator.calculate_commission(
            symbol, 10, prev_close, 'open', test_date
        )
        print(f"{symbol}: 开仓10手手续费 = {commission['commission']:.2f} 元")
        print()

    # 测试交割月验证
    print("=== 交割月验证测试 ===")
    symbol = 'IF2406.CFFEX'
    test_dates = [
        date(2024, 5, 1),
        date(2024, 6, 1),
        date(2024, 6, 15),
    ]

    for test_d in test_dates:
        is_delivery = FutureDeliveryValidator.is_delivery_month(symbol, test_d)
        ban_info = FutureDeliveryValidator.check_natural_person_ban(symbol, test_d)
        print(f"{test_d}: 交割月={is_delivery}, 禁止={ban_info['is_banned']}, 原因={ban_info['reason']}")
