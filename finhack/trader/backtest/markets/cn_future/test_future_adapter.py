"""
测试中国期货适配器
"""

import sys
import os

# 添加路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from datetime import date

# 直接导入模块
import importlib.util

# 加载 future_trading_rules_versions
spec = importlib.util.spec_from_file_location(
    "future_trading_rules_versions",
    "/mnt/ssd2/finhack-dev/finhack/finhack/trader/backtest/markets/cn_future/future_trading_rules_versions.py"
)
future_trading_rules_versions = importlib.util.module_from_spec(spec)
spec.loader.exec_module(future_trading_rules_versions)

# 加载 future_calculator
spec = importlib.util.spec_from_file_location(
    "future_calculator",
    "/mnt/ssd2/finhack-dev/finhack/finhack/trader/backtest/markets/cn_future/future_calculator.py"
)
future_calculator = importlib.util.module_from_spec(spec)

# 注入依赖
sys.modules['future_trading_rules_versions'] = future_trading_rules_versions
spec.loader.exec_module(future_calculator)


def test_helper_functions():
    """测试辅助函数"""
    print("=" * 50)
    print("测试辅助函数")
    print("=" * 50)

    test_symbols = [
        'IF2406.CFFEX',  # 沪深300股指期货
        'CU2406.SHFE',   # 铜期货
        'RB2406.SHFE',   # 螺纹钢期货
        'M2406.DCE',     # 豆粕期货
    ]

    for symbol in test_symbols:
        product = future_trading_rules_versions.get_future_product(symbol)
        exchange = future_trading_rules_versions.get_future_exchange(symbol)
        future_type = future_trading_rules_versions.get_future_type(symbol)
        tick_size = future_trading_rules_versions.get_tick_size(symbol)
        contract_size = future_trading_rules_versions.get_contract_size(symbol)
        price_limit = future_trading_rules_versions.get_price_limit(symbol)
        has_night = future_trading_rules_versions.has_night_session(symbol)

        print(f"\n{symbol}:")
        print(f"  品种: {product}")
        print(f"  交易所: {exchange}")
        print(f"  类型: {future_type}")
        print(f"  最小变动价位: {tick_size}")
        print(f"  合约单位: {contract_size}")
        print(f"  涨跌幅限制: {price_limit}")
        print(f"  有夜盘: {has_night}")


def test_price_calculator():
    """测试价格计算器"""
    print("\n" + "=" * 50)
    print("测试价格计算器")
    print("=" * 50)

    test_date = date(2024, 1, 15)

    test_cases = [
        ('IF2406.CFFEX', 3500.0, False),
        ('CU2406.SHFE', 68000.0, False),
        ('RB2406.SHFE', 3800.0, False),
        ('T2406.CFFEX', 105.0, False),  # 国债期货
    ]

    for symbol, prev_close, is_new in test_cases:
        result = future_calculator.FuturePriceCalculator.calculate_limit_prices(
            symbol, prev_close, test_date, is_new
        )
        print(f"\n{symbol} (前结算: {prev_close}):")
        print(f"  涨停: {result['upper_limit']}")
        print(f"  跌停: {result['lower_limit']}")
        print(f"  涨跌幅: {result['limit_ratio']}")
        print(f"  有涨跌停: {result['has_limit']}")


def test_margin_calculator():
    """测试保证金计算器"""
    print("\n" + "=" * 50)
    print("测试保证金计算器")
    print("=" * 50)

    test_date = date(2024, 1, 15)

    test_cases = [
        ('IF2406.CFFEX', 10, 3500.0, 'initial'),
        ('CU2406.SHFE', 10, 68000.0, 'initial'),
        ('RB2406.SHFE', 10, 3800.0, 'initial'),
    ]

    for symbol, volume, price, margin_type in test_cases:
        result = future_calculator.FutureMarginCalculator.calculate_margin(
            symbol, volume, price, margin_type, test_date
        )
        print(f"\n{symbol}: {volume}手 @ {price}元")
        print(f"  保证金比例: {result['margin_ratio']:.2%}")
        print(f"  合约价值: {result['contract_value']:.2f} 元")
        print(f"  保证金: {result['margin']:.2f} 元")


def test_commission_calculator():
    """测试手续费计算器"""
    print("\n" + "=" * 50)
    print("测试手续费计算器")
    print("=" * 50)

    test_date = date(2024, 1, 15)

    test_cases = [
        ('IF2406.CFFEX', 10, 3500.0, 'open'),
        ('IF2406.CFFEX', 10, 3550.0, 'close_today'),
        ('CU2406.SHFE', 10, 68000.0, 'open'),
        ('RB2406.SHFE', 10, 3800.0, 'open'),
    ]

    for symbol, volume, price, offset_flag in test_cases:
        result = future_calculator.FutureCommissionCalculator.calculate_commission(
            symbol, volume, price, offset_flag, test_date
        )
        print(f"\n{symbol}: {volume}手 @ {price}元 ({offset_flag})")
        print(f"  手续费率: {result['commission_rate']:.6%}")
        print(f"  合约价值: {result['contract_value']:.2f} 元")
        print(f"  手续费: {result['commission']:.2f} 元")


def test_delivery_validator():
    """测试交割月验证器"""
    print("\n" + "=" * 50)
    print("测试交割月验证器")
    print("=" * 50)

    symbol = 'IF2406.CFFEX'
    test_dates = [
        date(2024, 5, 1),
        date(2024, 6, 1),
        date(2024, 6, 15),
    ]

    for test_date in test_dates:
        delivery_year, delivery_month = future_calculator.FutureDeliveryValidator.parse_delivery_month(symbol)
        is_delivery = future_calculator.FutureDeliveryValidator.is_delivery_month(symbol, test_date)
        ban_info = future_calculator.FutureDeliveryValidator.check_natural_person_ban(symbol, test_date)

        print(f"\n{test_date}:")
        print(f"  交割月份: {delivery_year}-{delivery_month:02d}")
        print(f"  是否交割月: {is_delivery}")
        print(f"  是否禁止: {ban_info['is_banned']}, 原因: {ban_info['reason']}")


if __name__ == '__main__':
    test_helper_functions()
    test_price_calculator()
    test_margin_calculator()
    test_commission_calculator()
    test_delivery_validator()
    print("\n" + "=" * 50)
    print("所有测试完成!")
    print("=" * 50)
