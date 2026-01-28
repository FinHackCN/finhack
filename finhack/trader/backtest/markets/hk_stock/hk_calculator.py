"""
香港股票市场交易规则计算器

实现手续费计算、最小单位计算、VCM机制验证、价格精度计算等
"""

import math
from datetime import date, time, timedelta
from typing import Dict, Tuple, Optional
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN, ROUND_UP

try:
    from .hk_trading_rules_versions import (
        COMMISSION_VERSIONS,
        VCM_VERSIONS,
        TICK_SIZE_VERSIONS,
        LOT_SIZE_VERSIONS,
        RuleVersion,
        get_hk_stock_board_type,
        get_exchange_from_symbol,
        is_vcm_stock,
        get_lot_size_by_price,
        get_tick_size_by_price,
    )
except ImportError:
    from hk_trading_rules_versions import (
        COMMISSION_VERSIONS,
        VCM_VERSIONS,
        TICK_SIZE_VERSIONS,
        LOT_SIZE_VERSIONS,
        RuleVersion,
        get_hk_stock_board_type,
        get_exchange_from_symbol,
        is_vcm_stock,
        get_lot_size_by_price,
        get_tick_size_by_price,
    )


class HKStockPriceCalculator:
    """港股价格计算器"""

    @staticmethod
    def round_to_tick_size(price: float) -> float:
        """将价格四舍五入到最小价格变动单位

        Args:
            price: 原始价格

        Returns:
            四舍五入后的价格
        """
        tick_size = get_tick_size_by_price(price)
        rounded = round(price / tick_size) * tick_size
        return rounded

    @staticmethod
    def calculate_limit_prices(symbol: str, prev_close: float, query_date: date,
                              listing_date: date = None) -> Dict[str, float]:
        """计算涨跌停价格（港股无涨跌停限制）

        Args:
            symbol: 港股代码
            prev_close: 前收盘价
            query_date: 查询日期
            listing_date: 上市日期（用于判断新股）

        Returns:
            {
                'upper_limit': 无限制（inf）,
                'lower_limit': 无限制（0.01）,
                'limit_ratio': None,
                'is_new_stock': False,
            }
        """
        # 港股无涨跌停限制
        return {
            'upper_limit': float('inf'),
            'lower_limit': 0.01,  # 理论上最低价0.01港元
            'limit_ratio': None,
            'is_new_stock': False,
            'new_stock_days_left': 0,
        }

    @staticmethod
    def validate_order_price(symbol: str, order_price: float, side: str,
                           reference_price: float = None,
                           query_date: date = None) -> Dict:
        """验证订单价格是否合法

        Args:
            symbol: 港股代码
            order_price: 订单价格
            side: 订单方向 ('buy' or 'sell')
            reference_price: 参考价
            query_date: 查询日期

        Returns:
            {
                'valid': 是否有效,
                'message': 错误信息,
                'rounded_price': 四舍五入后的价格,
            }
        """
        # 检查价格是否合法
        if order_price <= 0:
            return {
                'valid': False,
                'message': '价格必须大于0',
                'rounded_price': None,
            }

        # 四舍五入到tick size
        rounded_price = HKStockPriceCalculator.round_to_tick_size(order_price)

        # 检查是否为VCM股票且在冷静期
        if query_date and query_date >= date(2016, 8, 22):
            if is_vcm_stock(symbol):
                # VCM机制会在回测引擎中动态检查
                pass

        return {
            'valid': True,
            'message': '',
            'rounded_price': rounded_price,
        }


class HKVCMValidator:
    """VCM（波动调节机制）验证器"""

    @staticmethod
    def is_vcm_stock(symbol: str, query_date: date = None) -> bool:
        """判断是否为VCM股票

        Args:
            symbol: 港股代码
            query_date: 查询日期

        Returns:
            是否为VCM股票
        """
        if query_date is None:
            query_date = date.today()

        # VCM从2016年8月22日开始
        if query_date < date(2016, 8, 22):
            return False

        return is_vcm_stock(symbol)

    @staticmethod
    def check_vcm_trigger(symbol: str, current_price: float,
                         reference_price: float, query_date: date = None) -> Dict:
        """检查VCM是否触发

        Args:
            symbol: 港股代码
            current_price: 当前价格
            reference_price: 参考价（通常为前收盘价）
            query_date: 查询日期

        Returns:
            {
                'is_vcm_stock': 是否为VCM股票,
                'triggered': 是否触发VCM,
                'change_pct': 变动百分比,
                'cooling_period_minutes': 冷静期分钟数,
                'price_limit': 冷静期内价格限制,
            }
        """
        if query_date is None:
            query_date = date.today()

        # 获取VCM规则
        vcm_rule = RuleVersion.get_applicable_rule(
            VCM_VERSIONS, 'all', query_date, 'hkex'
        )

        if not vcm_rule or not vcm_rule.get('vcm_enabled', False):
            return {
                'is_vcm_stock': False,
                'triggered': False,
                'change_pct': 0.0,
                'cooling_period_minutes': 0,
                'price_limit': None,
            }

        # 检查是否为VCM股票
        if not HKVCMValidator.is_vcm_stock(symbol, query_date):
            return {
                'is_vcm_stock': False,
                'triggered': False,
                'change_pct': 0.0,
                'cooling_period_minutes': 0,
                'price_limit': None,
            }

        # 计算变动百分比
        if reference_price <= 0:
            change_pct = 0.0
        else:
            change_pct = abs(current_price - reference_price) / reference_price

        # 检查是否触发阈值
        trigger_threshold = vcm_rule.get('vcm_trigger_threshold', 0.10)
        triggered = change_pct >= trigger_threshold

        # 获取冷静期参数
        cooling_period = vcm_rule.get('vcm_cooling_period', 5)
        price_limit_pct = vcm_rule.get('vcm_price_limit_pct', 0.10)

        # 计算冷静期内价格限制
        if triggered:
            upper_limit = reference_price * (1 + price_limit_pct)
            lower_limit = reference_price * (1 - price_limit_pct)
            price_limit = {
                'upper': upper_limit,
                'lower': lower_limit,
                'reference': reference_price,
            }
        else:
            price_limit = None

        return {
            'is_vcm_stock': True,
            'triggered': triggered,
            'change_pct': change_pct,
            'cooling_period_minutes': cooling_period,
            'price_limit': price_limit,
        }

    @staticmethod
    def validate_order_in_cooling_period(symbol: str, order_price: float,
                                        side: str, vcm_trigger_price: float,
                                        query_date: date = None) -> Dict:
        """验证冷静期内的订单价格

        Args:
            symbol: 港股代码
            order_price: 订单价格
            side: 订单方向
            vcm_trigger_price: VCM触发时的参考价
            query_date: 查询日期

        Returns:
            {
                'valid': 是否有效,
                'message': 错误信息,
            }
        """
        if query_date is None:
            query_date = date.today()

        vcm_rule = RuleVersion.get_applicable_rule(
            VCM_VERSIONS, 'all', query_date, 'hkex'
        )

        if not vcm_rule or not vcm_rule.get('vcm_enabled', False):
            return {'valid': True, 'message': ''}

        # 获取价格限制
        price_limit_pct = vcm_rule.get('vcm_price_limit_pct', 0.10)
        upper_limit = vcm_trigger_price * (1 + price_limit_pct)
        lower_limit = vcm_trigger_price * (1 - price_limit_pct)

        # 验证价格
        if side == 'buy':
            if order_price > upper_limit:
                return {
                    'valid': False,
                    'message': f'VCM冷静期内买入价不能高于 {upper_limit:.2f} 港元',
                }
        else:  # sell
            if order_price < lower_limit:
                return {
                    'valid': False,
                    'message': f'VCM冷静期内卖出价不能低于 {lower_limit:.2f} 港元',
                }

        return {'valid': True, 'message': ''}


class HKStockLotSizeCalculator:
    """最小交易单位计算器"""

    @staticmethod
    def get_lot_size_info(symbol: str, price: float,
                         query_date: date) -> Dict:
        """获取最小交易单位信息

        Args:
            symbol: 港股代码
            price: 股价
            query_date: 查询日期

        Returns:
            {
                'min_buy': 最小买入数量（股）,
                'lot_size': 一手股数,
                'sell_allow_fractional': 是否允许零股卖出,
                'sell_min': 最小卖出数量,
            }
        """
        board = get_hk_stock_board_type(symbol)

        # 根据股价获取lot size
        lot_size = get_lot_size_by_price(price, board, query_date)

        return {
            'min_buy': lot_size,  # 最小买入一手
            'lot_size': lot_size,  # 一手股数
            'sell_allow_fractional': False,  # 必须整手买卖
            'sell_min': lot_size,  # 卖出也必须整手
        }

    @staticmethod
    def normalize_order_volume(symbol: str, volume: float, price: float,
                              side: str, query_date: date) -> Tuple[float, str]:
        """标准化订单数量

        Args:
            symbol: 港股代码
            volume: 原始数量
            price: 股价
            side: 订单方向 ('buy' or 'sell')
            query_date: 查询日期

        Returns:
            (标准化后的数量, 消息)
        """
        lot_info = HKStockLotSizeCalculator.get_lot_size_info(symbol, price, query_date)
        lot_size = lot_info['lot_size']

        # 检查是否为整数手
        if volume % lot_size != 0:
            # 向下取整到整手
            normalized = (volume // lot_size) * lot_size
            if normalized == 0:
                return 0, f'交易数量必须为 {lot_size} 股的整数倍'
            return normalized, f'数量已调整为 {int(normalized)} 股（{int(normalized/lot_size)}手）'

        return volume, 'OK'


class HKStockCommissionCalculator:
    """手续费计算器"""

    @staticmethod
    def get_commission_info(symbol: str, query_date: date = None) -> Dict:
        """获取手续费率信息

        Args:
            symbol: 港股代码
            query_date: 查询日期

        Returns:
            {
                'commission_rate': 佣金率,
                'min_commission': 最低佣金,
                'stamp_tax_buy': 买入印花税率,
                'stamp_tax_sell': 卖出印花税率,
                'trading_fee_rate': 交易费率,
                'trading_levy_rate': 交易征费率,
                'clearing_fee_rate': 交收费率,
                'clearing_fee_max': 交收费最高额,
            }
        """
        if query_date is None:
            query_date = date.today()

        # 获取规则
        rule = RuleVersion.get_applicable_rule(
            COMMISSION_VERSIONS, 'stock', query_date, 'hkex'
        )

        if rule is None:
            # 默认规则
            return {
                'commission_rate': 0.001,
                'min_commission': 0.0,
                'stamp_tax_buy': 0.0,
                'stamp_tax_sell': 0.001,
                'trading_fee_rate': 0.00005,
                'trading_levy_rate': 0.000027,
                'clearing_fee_rate': 0.00002,
                'clearing_fee_max': 200.0,
                'other_fees': 0.0,
            }

        return rule

    @staticmethod
    def calculate_commission(symbol: str, volume: float, price: float,
                            side: str, query_date: date = None) -> Dict[str, float]:
        """计算手续费

        Args:
            symbol: 港股代码
            volume: 数量
            price: 价格
            side: 方向 ('buy' or 'sell')
            query_date: 查询日期

        Returns:
            {
                'commission': 佣金,
                'stamp_tax': 印花税,
                'trading_fee': 交易费,
                'trading_levy': 交易征费,
                'clearing_fee': 交收费,
                'other_fees': 其他费用,
                'total_fee': 总费用,
            }
        """
        commission_info = HKStockCommissionCalculator.get_commission_info(symbol, query_date)

        amount = volume * price  # 交易金额（港元）

        # 1. 佣金
        commission = amount * commission_info['commission_rate']
        min_commission = commission_info.get('min_commission', 0.0)
        commission = max(commission, min_commission)

        # 2. 印花税（仅卖出）
        if side == 'buy':
            stamp_tax = 0.0
        else:
            stamp_tax = amount * commission_info['stamp_tax_sell']

        # 3. 交易费（买卖双方）
        trading_fee = amount * commission_info['trading_fee_rate']

        # 4. 交易征费（买卖双方）
        trading_levy = amount * commission_info['trading_levy_rate']

        # 5. 交收费（买卖双方）
        clearing_fee = amount * commission_info['clearing_fee_rate']
        clearing_fee_max = commission_info.get('clearing_fee_max', 200.0)
        clearing_fee = min(clearing_fee, clearing_fee_max)

        # 6. 其他费用
        other_fees = commission_info.get('other_fees', 0.0)

        # 总费用
        total_fee = commission + stamp_tax + trading_fee + trading_levy + clearing_fee + other_fees

        return {
            'commission': commission,
            'stamp_tax': stamp_tax,
            'trading_fee': trading_fee,
            'trading_levy': trading_levy,
            'clearing_fee': clearing_fee,
            'other_fees': other_fees,
            'total_fee': total_fee,
        }


# ============================================================================
# 随机收市时间计算器
# ============================================================================
class HKRandomClosingCalculator:
    """收市竞价时段随机收市计算器

    港股收市竞价时段（16:00-16:10）会在16:08-16:10之间随机收市
    """

    @staticmethod
    def get_random_close_time(trade_date: date) -> time:
        """获取随机收市时间（模拟用）

        Args:
            trade_date: 交易日期

        Returns:
            随机收市时间
        """
        # 在实际回测中，应该使用固定的随机种子以确保可重现性
        # 这里使用日期的哈希值作为种子
        import hashlib
        seed = int(hashlib.md5(str(trade_date).encode()).hexdigest(), 16)

        # 生成16:08:00到16:10:00之间的随机时间
        random_seconds = seed % 121  # 0-120秒
        return time(16, 8, 0) + timedelta(seconds=random_seconds)


if __name__ == '__main__':
    from datetime import timedelta

    # 测试代码
    test_date = date(2024, 1, 15)

    print("=== 港股最小交易单位测试 ===")
    test_prices = [0.10, 0.50, 5.00, 15.00, 50.00, 150.00, 300.00]
    for price in test_prices:
        lot_info = HKStockLotSizeCalculator.get_lot_size_info('00700.HK', price, test_date)
        tick = get_tick_size_by_price(price)
        print(f"价格 {price:8.2f}: 最小单位={lot_info['min_buy']:4d}股, tick={tick}")

    print("\n=== VCM机制测试 ===")
    vcm_result = HKVCMValidator.check_vcm_trigger('00700.HK', 400.0, 360.0, test_date)
    print(f"腾讯: VCM股票={vcm_result['is_vcm_stock']}, "
          f"触发={vcm_result['triggered']}, "
          f"变动={vcm_result['change_pct']:.2%}")

    print("\n=== 手续费计算测试 ===")
    fee = HKStockCommissionCalculator.calculate_commission('00700.HK', 1000, 380.0, 'buy', test_date)
    print(f"腾讯买入1000股@380港元:")
    print(f"  佣金: {fee['commission']:.2f} 港元")
    print(f"  交易费: {fee['trading_fee']:.2f} 港元")
    print(f"  交易征费: {fee['trading_levy']:.2f} 港元")
    print(f"  交收费: {fee['clearing_fee']:.2f} 港元")
    print(f"  总费用: {fee['total_fee']:.2f} 港元")

    fee_sell = HKStockCommissionCalculator.calculate_commission('00700.HK', 1000, 380.0, 'sell', test_date)
    print(f"\n腾讯卖出1000股@380港元:")
    print(f"  佣金: {fee_sell['commission']:.2f} 港元")
    print(f"  印花税: {fee_sell['stamp_tax']:.2f} 港元")
    print(f"  总费用: {fee_sell['total_fee']:.2f} 港元")
