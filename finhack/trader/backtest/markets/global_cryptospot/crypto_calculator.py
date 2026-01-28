"""
加密货币交易规则计算器

实现价格计算、手续费计算、最小单位计算、资金费率计算等
"""

import math
from datetime import date, timedelta
from typing import Dict, Tuple, Optional, Union
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN, ROUND_UP, ROUND_CEILING, ROUND_FLOOR

try:
    from .crypto_trading_rules_versions import (
        PRICE_LIMIT_VERSIONS,
        LOT_SIZE_VERSIONS,
        COMMISSION_VERSIONS,
        FUNDING_RATE_VERSIONS,
        LEVERAGE_VERSIONS,
        LIQUIDATION_VERSIONS,
        PRICE_PRECISION_VERSIONS,
        RuleVersion,
        get_crypto_exchange,
        get_crypto_pair_type,
        get_base_currency,
        get_quote_currency,
        get_crypto_precision,
    )
except ImportError:
    from crypto_trading_rules_versions import (
        PRICE_LIMIT_VERSIONS,
        LOT_SIZE_VERSIONS,
        COMMISSION_VERSIONS,
        FUNDING_RATE_VERSIONS,
        LEVERAGE_VERSIONS,
        LIQUIDATION_VERSIONS,
        PRICE_PRECISION_VERSIONS,
        RuleVersion,
        get_crypto_exchange,
        get_crypto_pair_type,
        get_base_currency,
        get_quote_currency,
        get_crypto_precision,
    )


class CryptoPriceCalculator:
    """加密货币价格计算器"""

    @staticmethod
    def calculate_limit_prices(symbol: str, prev_close: float, query_date: date,
                              listing_date: date = None) -> Dict[str, Union[float, None]]:
        """计算涨跌停价格

        加密货币市场通常无涨跌幅限制，但部分交易所有极端行情保护

        Args:
            symbol: 交易对代码
            prev_close: 前收盘价
            query_date: 查询日期
            listing_date: 上市日期（用于判断新币）

        Returns:
            {
                'upper_limit': 涨停价（通常为inf）,
                'lower_limit': 跌停价（通常为0）,
                'limit_ratio': 涨跌幅比例（通常为None）,
                'has_circuit_breaker': 是否有熔断机制,
                'circuit_breaker_threshold': 熔断阈值,
            }
        """
        exchange = get_crypto_exchange(symbol)
        pair_type = get_crypto_pair_type(symbol)

        # 获取涨跌幅限制规则
        limit_rule = RuleVersion.get_applicable_rule(
            PRICE_LIMIT_VERSIONS, 'all', query_date, exchange
        )

        if limit_rule is None:
            # 默认：无涨跌幅限制
            return {
                'upper_limit': float('inf'),
                'lower_limit': 0.0,
                'limit_ratio': None,
                'has_circuit_breaker': False,
                'circuit_breaker_threshold': None,
            }

        # 检查熔断机制
        has_circuit_breaker = limit_rule.get('has_circuit_breaker', False)
        circuit_breaker_threshold = limit_rule.get('circuit_breaker_threshold')

        # 加密货币现货通常无涨跌幅限制
        return {
            'upper_limit': float('inf'),
            'lower_limit': 0.0,
            'limit_ratio': None,
            'has_circuit_breaker': has_circuit_breaker,
            'circuit_breaker_threshold': circuit_breaker_threshold,
        }

    @staticmethod
    def round_price(price: float, symbol: str, query_date: date = None) -> float:
        """按规则对价格进行四舍五入

        Args:
            price: 原始价格
            symbol: 交易对代码
            query_date: 查询日期

        Returns:
            四舍五入后的价格
        """
        if price == float('inf') or price == 0 or price is None:
            return price

        if query_date is None:
            query_date = date.today()

        exchange = get_crypto_exchange(symbol)

        # 获取价格精度规则
        precision_rule = RuleVersion.get_applicable_rule(
            PRICE_PRECISION_VERSIONS, 'all', query_date, exchange
        )

        if precision_rule is None:
            # 默认：保留2位小数
            return round(price, 2)

        # 检查是否使用动态精度
        if precision_rule.get('use_dynamic_tick', False):
            tick_size = CryptoPriceCalculator._get_dynamic_tick_size(
                price, precision_rule.get('tick_size_rules', [])
            )
        else:
            tick_size = precision_rule.get('tick_size', 0.01)

        rounding_method = precision_rule.get('rounding_method', 'round_half_up')

        # 使用Decimal进行精确计算
        dec_price = Decimal(str(price))
        dec_tick = Decimal(str(tick_size))

        if rounding_method == 'round_half_up':
            result = (dec_price / dec_tick).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * dec_tick
        elif rounding_method == 'round_down':
            result = (dec_price / dec_tick).quantize(Decimal('1'), rounding=ROUND_DOWN) * dec_tick
        elif rounding_method == 'round_up':
            result = (dec_price / dec_tick).quantize(Decimal('1'), rounding=ROUND_UP) * dec_tick
        else:
            result = (dec_price / dec_tick).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * dec_tick

        return float(result)

    @staticmethod
    def _get_dynamic_tick_size(price: float, tick_size_rules: list) -> float:
        """根据价格获取动态最小价格单位"""
        for rule in tick_size_rules:
            if price <= rule.get('price_max', float('inf')):
                return rule.get('tick_size', 0.01)
        return 0.01


class CryptoLotSizeCalculator:
    """最小交易单位计算器"""

    @staticmethod
    def get_lot_size_info(symbol: str, query_date: date = None) -> Dict:
        """获取最小交易单位信息

        Args:
            symbol: 交易对代码
            query_date: 查询日期

        Returns:
            {
                'min_notional': 最小名义价值,
                'min_quantity': 最小数量,
                'quantity_increment': 数量增量,
                'price_increment': 价格增量,
                'allow_fractional': 是否允许小数交易,
            }
        """
        if query_date is None:
            query_date = date.today()

        exchange = get_crypto_exchange(symbol)

        # 获取规则
        rule = RuleVersion.get_applicable_rule(
            LOT_SIZE_VERSIONS, 'all', query_date, exchange
        )

        if rule is None:
            # 默认规则
            return {
                'min_notional': 10.0,
                'min_quantity': 0.00001,
                'quantity_increment': 0.00001,
                'price_increment': 0.01,
                'allow_fractional': True,
            }

        return rule

    @staticmethod
    def normalize_order_volume(symbol: str, volume: float, price: float, side: str,
                              query_date: date = None) -> Tuple[float, str]:
        """标准化订单数量

        Args:
            symbol: 交易对代码
            volume: 原始数量
            price: 价格
            side: 订单方向 ('buy' or 'sell')
            query_date: 查询日期

        Returns:
            (标准化后的数量, 消息)
        """
        lot_info = CryptoLotSizeCalculator.get_lot_size_info(symbol, query_date)

        # 计算名义价值
        notional = volume * price

        # 检查最小名义价值
        min_notional = lot_info['min_notional']
        if notional < min_notional:
            # 计算满足最小名义价值的最小数量
            min_volume = min_notional / price
            volume_increment = lot_info['quantity_increment']
            adjusted_volume = math.ceil(min_volume / volume_increment) * volume_increment
            return adjusted_volume, f'订单价值低于最小要求 {min_notional} USDT，已调整数量'

        # 检查数量增量
        quantity_increment = lot_info['quantity_increment']
        normalized = math.ceil(volume / quantity_increment) * quantity_increment

        if normalized < lot_info['min_quantity']:
            return 0, f'交易数量低于最小值 {lot_info["min_quantity"]}'

        return normalized, 'OK'

    @staticmethod
    def normalize_order_price(symbol: str, price: float, query_date: date = None) -> float:
        """标准化订单价格

        Args:
            symbol: 交易对代码
            price: 原始价格
            query_date: 查询日期

        Returns:
            标准化后的价格
        """
        return CryptoPriceCalculator.round_price(price, symbol, query_date)


class CryptoCommissionCalculator:
    """手续费计算器"""

    @staticmethod
    def get_commission_info(symbol: str, query_date: date = None,
                           vip_level: str = 'vip0') -> Dict:
        """获取手续费率信息

        Args:
            symbol: 交易对代码
            query_date: 查询日期
            vip_level: VIP等级

        Returns:
            {
                'maker_fee': 挂单手续费率,
                'taker_fee': 吃单手续费率,
                'min_commission': 最低手续费,
            }
        """
        if query_date is None:
            query_date = date.today()

        exchange = get_crypto_exchange(symbol)

        # 获取规则
        rule = RuleVersion.get_applicable_rule(
            COMMISSION_VERSIONS, 'all', query_date, exchange
        )

        if rule is None:
            # 默认规则
            return {
                'maker_fee': 0.001,
                'taker_fee': 0.001,
                'min_commission': 0.0,
            }

        # 检查VIP折扣
        if 'vip_discounts' in rule and vip_level in rule['vip_discounts']:
            vip_discount = rule['vip_discounts'][vip_level]
            return {
                'maker_fee': vip_discount.get('maker', rule['maker_fee']),
                'taker_fee': vip_discount.get('taker', rule['taker_fee']),
                'min_commission': rule.get('min_commission', 0.0),
            }

        return {
            'maker_fee': rule.get('maker_fee', 0.001),
            'taker_fee': rule.get('taker_fee', 0.001),
            'min_commission': rule.get('min_commission', 0.0),
        }

    @staticmethod
    def calculate_commission(symbol: str, volume: float, price: float,
                            side: str, order_type: str = 'limit',
                            query_date: date = None, vip_level: str = 'vip0') -> Dict[str, float]:
        """计算手续费

        Args:
            symbol: 交易对代码
            volume: 数量
            price: 价格
            side: 方向 ('buy' or 'sell')
            order_type: 订单类型 ('limit' or 'market')
            query_date: 查询日期
            vip_level: VIP等级

        Returns:
            {
                'commission': 手续费,
                'fee_rate': 费率,
            }
        """
        commission_info = CryptoCommissionCalculator.get_commission_info(
            symbol, query_date, vip_level
        )

        amount = volume * price

        # 根据订单类型确定费率
        if order_type == 'limit':
            fee_rate = commission_info['maker_fee']
        else:
            fee_rate = commission_info['taker_fee']

        # 计算手续费
        commission = amount * fee_rate
        min_commission = commission_info.get('min_commission', 0.0)
        commission = max(commission, min_commission)

        return {
            'commission': commission,
            'fee_rate': fee_rate,
            'commission_currency': get_quote_currency(symbol),
        }


class CryptoFundingRateCalculator:
    """资金费率计算器"""

    @staticmethod
    def get_funding_rate_info(symbol: str, query_date: date = None) -> Dict:
        """获取资金费率信息

        Args:
            symbol: 交易对代码
            query_date: 查询日期

        Returns:
            {
                'funding_interval_hours': 资金费率间隔（小时）,
                'max_funding_rate': 最大资金费率,
                'min_funding_rate': 最小资金费率,
                'calculation_method': 计算方法,
            }
        """
        if query_date is None:
            query_date = date.today()

        exchange = get_crypto_exchange(symbol)
        pair_type = get_crypto_pair_type(symbol)

        # 现货交易无资金费率
        if pair_type != 'futures':
            return {
                'has_funding_rate': False,
                'funding_interval_hours': None,
                'max_funding_rate': None,
                'min_funding_rate': None,
            }

        # 获取规则
        rule = RuleVersion.get_applicable_rule(
            FUNDING_RATE_VERSIONS, 'all', query_date, exchange
        )

        if rule is None:
            # 默认规则
            return {
                'has_funding_rate': True,
                'funding_interval_hours': 8,
                'max_funding_rate': 0.0005,
                'min_funding_rate': -0.0005,
                'calculation_method': 'clamp',
            }

        return {
            'has_funding_rate': True,
            'funding_interval_hours': rule.get('funding_interval_hours', 8),
            'max_funding_rate': rule.get('max_funding_rate', 0.0005),
            'min_funding_rate': rule.get('min_funding_rate', -0.0005),
            'calculation_method': rule.get('funding_rate_calculation', 'clamp'),
        }

    @staticmethod
    def calculate_funding_rate(symbol: str, mark_price: float, index_price: float,
                              query_date: date = None) -> Dict[str, float]:
        """计算资金费率

        Args:
            symbol: 交易对代码
            mark_price: 标记价格
            index_price: 指数价格
            query_date: 查询日期

        Returns:
            {
                'funding_rate': 资金费率,
                'next_funding_time': 下次资金费率时间,
            }
        """
        funding_info = CryptoFundingRateCalculator.get_funding_rate_info(symbol, query_date)

        if not funding_info['has_funding_rate']:
            return {
                'funding_rate': 0.0,
                'next_funding_time': None,
            }

        # 计算原始费率（溢价率）
        premium = (mark_price - index_price) / index_price
        raw_funding_rate = premium

        # 应用限制
        max_rate = funding_info['max_funding_rate']
        min_rate = funding_info['min_funding_rate']
        calculation_method = funding_info['calculation_method']

        if calculation_method == 'clamp':
            funding_rate = max(min_rate, min(max_rate, raw_funding_rate))
        elif calculation_method == 'smooth':
            # 平滑处理
            funding_rate = raw_funding_rate * 0.8  # 简化
            funding_rate = max(min_rate, min(max_rate, funding_rate))
        else:
            funding_rate = raw_funding_rate

        return {
            'funding_rate': funding_rate,
            'next_funding_time': None,  # 需要根据当前时间计算
        }

    @staticmethod
    def calculate_funding_fee(position_value: float, funding_rate: float,
                             hours: float = 8) -> float:
        """计算资金费率费用

        Args:
            position_value: 持仓价值
            funding_rate: 资金费率
            hours: 持仓小时数

        Returns:
            资金费用（正数表示支付，负数表示收取）
        """
        return position_value * funding_rate * (hours / 24)


class CryptoLeverageCalculator:
    """杠杆计算器"""

    @staticmethod
    def get_leverage_info(symbol: str, query_date: date = None) -> Dict:
        """获取杠杆信息

        Args:
            symbol: 交易对代码
            query_date: 查询日期

        Returns:
            {
                'max_leverage': 最大杠杆,
                'default_leverage': 默认杠杆,
                'leverage_increment': 杠杆增量,
            }
        """
        if query_date is None:
            query_date = date.today()

        exchange = get_crypto_exchange(symbol)
        pair_type = get_crypto_pair_type(symbol)

        # 现货交易不支持杠杆
        if pair_type != 'futures':
            return {
                'supports_leverage': False,
                'max_leverage': 1,
                'default_leverage': 1,
                'leverage_increment': 1,
            }

        # 获取规则
        rule = RuleVersion.get_applicable_rule(
            LEVERAGE_VERSIONS, 'all', query_date, exchange
        )

        if rule is None:
            # 默认规则
            return {
                'supports_leverage': True,
                'max_leverage': 125,
                'default_leverage': 10,
                'leverage_increment': 1,
            }

        return {
            'supports_leverage': True,
            'max_leverage': rule.get('max_leverage', 125),
            'default_leverage': rule.get('default_leverage', 10),
            'leverage_increment': rule.get('leverage_increment', 1),
        }

    @staticmethod
    def calculate_position_value(symbol: str, margin: float, leverage: int,
                                query_date: date = None) -> Dict:
        """计算杠杆持仓价值

        Args:
            symbol: 交易对代码
            margin: 保证金
            leverage: 杠杆倍数
            query_date: 查询日期

        Returns:
            {
                'position_value': 持仓价值,
                'required_margin': 所需保证金,
            }
        """
        leverage_info = CryptoLeverageCalculator.get_leverage_info(symbol, query_date)

        if not leverage_info['supports_leverage']:
            leverage = 1

        # 限制杠杆倍数
        max_leverage = leverage_info['max_leverage']
        leverage = min(leverage, max_leverage)

        position_value = margin * leverage

        return {
            'position_value': position_value,
            'required_margin': margin,
            'actual_leverage': leverage,
        }


class CryptoLiquidationCalculator:
    """强平计算器"""

    @staticmethod
    def get_liquidation_info(symbol: str, query_date: date = None) -> Dict:
        """获取强平信息

        Args:
            symbol: 交易对代码
            query_date: 查询日期

        Returns:
            强平配置信息
        """
        if query_date is None:
            query_date = date.today()

        exchange = get_crypto_exchange(symbol)
        pair_type = get_crypto_pair_type(symbol)

        # 现货交易无强平
        if pair_type != 'futures':
            return {
                'has_liquidation': False,
            }

        # 获取规则
        rule = RuleVersion.get_applicable_rule(
            LIQUIDATION_VERSIONS, 'all', query_date, exchange
        )

        if rule is None:
            # 默认规则
            return {
                'has_liquidation': True,
                'liquidation_method': 'mark_price',
                'maintenance_margin_rate': 0.005,
                'margin_call_threshold': 0.01,
                'use_tiered_margin': False,
            }

        # 添加has_liquidation标记
        rule['has_liquidation'] = True
        return rule

    @staticmethod
    def calculate_liquidation_price(symbol: str, entry_price: float,
                                   leverage: int, position_side: str = 'long',
                                   query_date: date = None) -> Dict:
        """计算强平价格

        Args:
            symbol: 交易对代码
            entry_price: 开仓价格
            leverage: 杠杆倍数
            position_side: 持仓方向 ('long' or 'short')
            query_date: 查询日期

        Returns:
            {
                'liquidation_price': 强平价格,
                'bankruptcy_price': 破产价格,
                'maintenance_margin': 维持保证金,
            }
        """
        liquidation_info = CryptoLiquidationCalculator.get_liquidation_info(symbol, query_date)

        if not liquidation_info.get('has_liquidation', False):
            return {
                'liquidation_price': None,
                'bankruptcy_price': None,
                'maintenance_margin': None,
            }

        mmr = liquidation_info.get('maintenance_margin_rate', 0.005)

        if position_side == 'long':
            # 多头强平价 = 开仓价 * (1 - 1/杠杆 + 维持保证金率)
            liquidation_price = entry_price * (1 - 1/leverage + mmr)
            bankruptcy_price = entry_price * (1 - 1/leverage)
        else:
            # 空头强平价 = 开仓价 * (1 + 1/杠杆 - 维持保证金率)
            liquidation_price = entry_price * (1 + 1/leverage - mmr)
            bankruptcy_price = entry_price * (1 + 1/leverage)

        return {
            'liquidation_price': liquidation_price,
            'bankruptcy_price': bankruptcy_price,
            'maintenance_margin': entry_price * mmr,
        }


if __name__ == '__main__':
    # 测试代码
    test_date = date(2024, 1, 15)

    # 测试涨跌停价计算
    print("=== 涨跌停价计算测试 ===")
    test_cases = [
        ('BTCUSDT', 45000.0),    # 现货
        ('ETHUSDT', 2500.0),     # 现货
        ('BTCPERP', 45000.0),    # 合约
    ]

    for symbol, prev_close in test_cases:
        result = CryptoPriceCalculator.calculate_limit_prices(symbol, prev_close, test_date)
        print(f"{symbol} (前收盘: {prev_close}):")
        print(f"  涨停: {result['upper_limit']}, 跌停: {result['lower_limit']}")
        print(f"  熔断机制: {result['has_circuit_breaker']}")

    # 测试手续费
    print("\n=== 手续费计算测试 ===")
    for symbol, _ in test_cases:
        fee = CryptoCommissionCalculator.calculate_commission(symbol, 1.0, 45000, 'buy', 'limit', test_date)
        print(f"{symbol}: 手续费={fee['commission']}, 费率={fee['fee_rate']}")

    # 测试资金费率
    print("\n=== 资金费率计算测试 ===")
    for symbol, _ in test_cases:
        funding = CryptoFundingRateCalculator.calculate_funding_rate(symbol, 45100, 45000, test_date)
        print(f"{symbol}: 资金费率={funding['funding_rate']}")

    # 测试强平价格（合约）
    print("\n=== 强平价格计算测试 ===")
    liq_price = CryptoLiquidationCalculator.calculate_liquidation_price('BTCPERP', 45000, 10, 'long', test_date)
    print(f"BTCPERP 10倍做多开仓价45000: 强平价={liq_price['liquidation_price']}, 破产价={liq_price['bankruptcy_price']}")
