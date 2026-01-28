"""
港股市场适配器独立测试
"""

import sys
import os
from datetime import date, datetime, time

# 直接导入模块
sys.path.insert(0, os.path.dirname(__file__))

from hk_trading_rules_versions import (
    get_hk_stock_board_type,
    is_vcm_stock,
    get_exchange_from_symbol,
    get_lot_size_by_price,
    get_tick_size_by_price,
)
from hk_calculator import (
    HKStockCommissionCalculator,
    HKStockLotSizeCalculator,
    HKVCMValidator,
)


def test_hk_rules():
    """测试港股规则"""
    print("=" * 60)
    print("港股交易规则测试")
    print("=" * 60)

    # 测试板块识别
    print("\n--- 板块识别测试 ---")
    test_symbols = ['00700.HK', '00005.HK', '80000.HK', '80888.HK']
    for symbol in test_symbols:
        board = get_hk_stock_board_type(symbol)
        exchange = get_exchange_from_symbol(symbol)
        is_vcm = is_vcm_stock(symbol)
        print(f"{symbol}: 板块={board:10s}, 交易所={exchange}, VCM={is_vcm}")

    # 测试lot size和tick size
    print("\n--- 最小交易单位和价格精度测试 ---")
    test_prices = [0.10, 0.50, 5.00, 15.00, 50.00, 150.00, 300.00, 380.00]
    for price in test_prices:
        lot_size = get_lot_size_by_price(price)
        tick = get_tick_size_by_price(price)
        print(f"  价格 {price:8.2f} 港元: 最小单位={lot_size:4d}股, tick={tick}")

    # 测试手续费
    print("\n--- 手续费计算测试 ---")
    test_date = date(2024, 1, 15)
    fee = HKStockCommissionCalculator.calculate_commission('00700.HK', 1000, 380.0, 'buy', test_date)
    print(f"腾讯买入1000股@380港元 (交易额={1000*380:.0f}港元):")
    print(f"  佣金(0.1%):     {fee['commission']:>10.2f} 港元")
    print(f"  交易费(0.005%): {fee['trading_fee']:>10.2f} 港元")
    print(f"  交易征费(0.0027%): {fee['trading_levy']:>10.2f} 港元")
    print(f"  交收费(0.002%): {fee['clearing_fee']:>10.2f} 港元")
    print(f"  总费用:         {fee['total_fee']:>10.2f} 港元")

    fee_sell = HKStockCommissionCalculator.calculate_commission('00700.HK', 1000, 380.0, 'sell', test_date)
    print(f"\n腾讯卖出1000股@380港元 (交易额={1000*380:.0f}港元):")
    print(f"  佣金(0.1%):     {fee_sell['commission']:>10.2f} 港元")
    print(f"  印花税(0.1%):   {fee_sell['stamp_tax']:>10.2f} 港元")
    print(f"  其他费用:       {fee_sell['trading_fee'] + fee_sell['trading_levy'] + fee_sell['clearing_fee']:>10.2f} 港元")
    print(f"  总费用:         {fee_sell['total_fee']:>10.2f} 港元")

    # 计算买卖总费用占比
    total_cost = fee['total_fee'] + fee_sell['total_fee']
    total_amount = 1000 * 380 * 2  # 买卖总金额
    print(f"\n买卖总费用: {total_cost:.2f} 港元")
    print(f"占交易总额比例: {total_cost/total_amount:.4%}")

    # 测试VCM机制
    print("\n--- VCM机制测试 ---")
    print("腾讯 VCM检查 (前收盘360港元, 当前400港元):")
    vcm_result = HKVCMValidator.check_vcm_trigger('00700.HK', 400.0, 360.0, test_date)
    print(f"  是VCM股票: {vcm_result['is_vcm_stock']}")
    print(f"  触发VCM: {vcm_result['triggered']}")
    print(f"  变动幅度: {vcm_result['change_pct']:.2%}")
    if vcm_result['triggered']:
        print(f"  冷静期: {vcm_result['cooling_period_minutes']}分钟")
        if vcm_result['price_limit']:
            print(f"  价格限制: {vcm_result['price_limit']['lower']:.2f} - {vcm_result['price_limit']['upper']:.2f} 港元")

    # 测试最小交易单位
    print("\n--- 最小交易单位信息测试 ---")
    lot_info = HKStockLotSizeCalculator.get_lot_size_info('00700.HK', 380.0, test_date)
    print(f"腾讯(380港元) 最小交易单位:")
    print(f"  最小买入: {lot_info['min_buy']} 股")
    print(f"  一手股数: {lot_info['lot_size']} 股")
    print(f"  可零股卖出: {lot_info['sell_allow_fractional']}")

    # 测试订单数量标准化
    print("\n--- 订单数量标准化测试 ---")
    test_volumes = [50, 100, 150, 500, 1000, 1250]
    for volume in test_volumes:
        normalized, msg = HKStockLotSizeCalculator.normalize_order_volume(
            '00700.HK', volume, 380.0, 'buy', test_date
        )
        status = "OK" if normalized > 0 else "ERROR"
        print(f"  买入 {volume:4d} 股 -> {normalized:4d} 股 ({status}): {msg}")

    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)


def test_hk_lot_size_details():
    """详细测试港股lot size规则"""
    print("\n" + "=" * 60)
    print("港股Lot Size详细规则")
    print("=" * 60)

    test_cases = [
        (0.05, "仙股"),
        (0.10, "低价股"),
        (0.25, "低价股边界"),
        (0.30, "中低价股"),
        (0.50, "中低价股边界"),
        (1.00, "中价股"),
        (5.00, "中价股"),
        (10.00, "中价股边界"),
        (15.00, "中高价股"),
        (20.00, "中高价股边界"),
        (50.00, "高价股"),
        (100.00, "高价股边界"),
        (150.00, "高价股"),
        (200.00, "高价股边界"),
        (300.00, "高价股"),
        (500.00, "高价股边界"),
        (1000.00, "超高价股"),
        (2000.00, "超高价股"),
    ]

    print("\n主板lot size规则:")
    print(f"{'价格':>10} {'lot size':>10} {'每手金额':>15}")
    print("-" * 40)
    for price, desc in test_cases:
        lot_size = get_lot_size_by_price(price, 'main_board')
        hand_value = price * lot_size
        print(f"{price:>10.2f} {lot_size:>10d} {hand_value:>15.2f}")

    print("\n创业板lot size规则:")
    print(f"{'价格':>10} {'lot size':>10} {'每手金额':>15}")
    print("-" * 40)
    for price, desc in test_cases[:10]:
        lot_size = get_lot_size_by_price(price, 'gem')
        hand_value = price * lot_size
        print(f"{price:>10.2f} {lot_size:>10d} {hand_value:>15.2f}")


if __name__ == '__main__':
    test_hk_rules()
    test_hk_lot_size_details()
