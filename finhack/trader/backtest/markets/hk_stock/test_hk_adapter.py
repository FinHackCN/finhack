"""
港股市场适配器测试
"""

import sys
import os

# 添加项目路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../'))
sys.path.insert(0, project_root)

from finhack.trader.backtest.markets.hk_stock import (
    HKStockMarketAdapter,
    get_hk_stock_board_type,
    is_vcm_stock,
)
from finhack.trader.backtest.markets.hk_stock.hk_calculator import (
    HKStockCommissionCalculator,
    HKVCMValidator,
)
from datetime import date


def test_hk_adapter():
    """测试港股适配器基本功能"""
    print("=" * 60)
    print("港股市场适配器测试")
    print("=" * 60)

    # 创建适配器
    adapter = HKStockMarketAdapter(current_date=date(2024, 1, 15))
    print(f"\n市场名称: {adapter.get_market_name()}")
    print(f"时区: {adapter.get_timezone()}")
    print(f"货币: {adapter.get_currency()}")
    print(f"结算周期: {adapter.get_settlement_cycle()}")
    print(f"支持频率: {adapter.get_supported_frequencies()}")

    # 测试板块识别
    print("\n--- 板块识别测试 ---")
    test_symbols = ['00700.HK', '00005.HK', '80000.HK', '80888.HK']
    for symbol in test_symbols:
        board = get_hk_stock_board_type(symbol)
        is_vcm = is_vcm_stock(symbol)
        print(f"{symbol}: 板块={board}, VCM={is_vcm}")

    # 测试事件生成
    print("\n--- 事件生成测试 ---")
    events = adapter.generate_daily_events(date(2024, 1, 15), '1d', '00700.HK')
    print(f"生成事件数量: {len(events)}")
    print("\n前10个事件:")
    for e in events[:10]:
        print(f"  {e.event_time.strftime('%H:%M:%S')} {e.event_type.value:20s}: {e.event_description}")

    # 测试交易时段
    print("\n--- 交易时段测试 ---")
    sessions = adapter.get_trading_sessions_for_date(date(2024, 1, 15), '00700.HK', '1d')
    for s in sessions:
        print(f"  {s['type']:15s}: {s['start']}-{s['end']} - {s['description']}")

    # 测试lot size
    print("\n--- 最小交易单位测试 ---")
    test_prices = [0.10, 0.50, 5.00, 15.00, 50.00, 150.00, 300.00, 380.00]
    for price in test_prices:
        lot_size = adapter.get_lot_size('00700.HK', price=price)
        print(f"  价格 {price:8.2f} 港元: 最小单位={lot_size:4d}股")

    # 测试手续费
    print("\n--- 手续费计算测试 ---")
    fee = HKStockCommissionCalculator.calculate_commission('00700.HK', 1000, 380.0, 'buy', date(2024, 1, 15))
    print(f"腾讯买入1000股@380港元:")
    print(f"  佣金: {fee['commission']:>10.2f} 港元")
    print(f"  交易费: {fee['trading_fee']:>10.2f} 港元")
    print(f"  交易征费: {fee['trading_levy']:>10.2f} 港元")
    print(f"  交收费: {fee['clearing_fee']:>10.2f} 港元")
    print(f"  总费用: {fee['total_fee']:>10.2f} 港元")

    fee_sell = HKStockCommissionCalculator.calculate_commission('00700.HK', 1000, 380.0, 'sell', date(2024, 1, 15))
    print(f"\n腾讯卖出1000股@380港元:")
    print(f"  佣金: {fee_sell['commission']:>10.2f} 港元")
    print(f"  印花税: {fee_sell['stamp_tax']:>10.2f} 港元")
    print(f"  总费用: {fee_sell['total_fee']:>10.2f} 港元")

    # 测试VCM机制
    print("\n--- VCM机制测试 ---")
    vcm_result = HKVCMValidator.check_vcm_trigger('00700.HK', 400.0, 360.0, date(2024, 1, 15))
    print(f"腾讯 VCM检查:")
    print(f"  是VCM股票: {vcm_result['is_vcm_stock']}")
    print(f"  触发VCM: {vcm_result['triggered']}")
    print(f"  变动幅度: {vcm_result['change_pct']:.2%}")
    if vcm_result['triggered']:
        print(f"  冷静期: {vcm_result['cooling_period_minutes']}分钟")
        print(f"  价格限制: {vcm_result['price_limit']}")

    # 测试交易时间判断
    print("\n--- 交易时间判断测试 ---")
    from datetime import datetime
    test_times = [
        datetime(2024, 1, 15, 9, 0),
        datetime(2024, 1, 15, 9, 30),
        datetime(2024, 1, 15, 12, 0),
        datetime(2024, 1, 15, 12, 30),  # 午休
        datetime(2024, 1, 15, 13, 30),
        datetime(2024, 1, 15, 16, 0),
        datetime(2024, 1, 15, 16, 5),  # 收市竞价
    ]
    for dt in test_times:
        is_trading = adapter.is_trading_time(dt, '1d', '00700.HK')
        print(f"  {dt.strftime('%H:%M')} - {'交易中' if is_trading else '非交易'}")

    # 测试价格限制
    print("\n--- 价格限制测试 ---")
    limits = adapter.get_price_limits('00700.HK', 360.0)
    print(f"腾讯价格限制:")
    print(f"  涨停价: {limits['upper_limit']}")
    print(f"  跌停价: {limits['lower_limit']}")
    print(f"  有VCM: {limits['has_vcm']}")

    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)


if __name__ == '__main__':
    test_hk_adapter()
