"""
美股市场适配器简单测试（独立运行）
"""

import sys
import os

# 直接导入本地模块
sys.path.insert(0, os.path.dirname(__file__))

from datetime import date, time, datetime
from us_trading_rules_versions import (
    get_exchange_from_symbol,
    get_stock_type,
    RuleVersion,
    TRADING_SCHEDULE_VERSIONS,
    PRICE_LIMIT_VERSIONS,
    SETTLEMENT_VERSIONS,
)
from us_calculator import (
    USCircuitBreakerCalculator,
    USLotSizeCalculator,
    USCommissionCalculator,
    USSettlementCalculator,
)


def test_basic_functions():
    """测试基础功能"""
    print("=" * 60)
    print("美股市场适配器基础功能测试")
    print("=" * 60)

    # 测试股票分类
    print("\n1. 股票分类测试")
    test_symbols = ['AAPL', 'TSLA', 'SPY', 'QQQ', 'WMT', 'BRK.A']
    for symbol in test_symbols:
        exchange = get_exchange_from_symbol(symbol)
        stock_type = get_stock_type(symbol)
        print(f"  {symbol:8s}: 交易所={exchange:6s}, 类型={stock_type}")

    # 测试零碎股
    print("\n2. 零碎股支持测试")
    lot_info = USLotSizeCalculator.get_lot_size_info('AAPL', date.today())
    print(f"  AAPL 最小交易单位信息:")
    for key, value in lot_info.items():
        print(f"    {key}: {value}")

    # 测试订单数量标准化
    print("\n  订单数量标准化测试:")
    test_volumes = [100, 10, 1, 0.5, 0.001]
    for volume in test_volumes:
        normalized, msg = USLotSizeCalculator.normalize_order_volume('AAPL', volume, 'buy', date.today())
        print(f"    买入 {volume:8.4f} 股 -> {normalized:8.4f} 股 ({msg})")

    # 测试手续费
    print("\n3. 手续费计算测试")
    commission_test_cases = [
        ('AAPL', 100, 175.0, 'buy'),
        ('AAPL', 100, 175.0, 'sell'),
        ('TSLA', 50, 250.0, 'buy'),
        ('TSLA', 50, 250.0, 'sell'),
    ]
    for symbol, volume, price, side in commission_test_cases:
        fee = USCommissionCalculator.calculate_commission(symbol, volume, price, side)
        desc = "买入" if side == 'buy' else "卖出"
        print(f"  {desc} {volume}股 {symbol} @ ${price}:")
        print(f"    佣金: ${fee['commission']:.4f}, SEC费用: ${fee['sec_fee']:.4f}, "
              f"交易活动费: ${fee['trading_activity_fee']:.4f}, 总费用: ${fee['total_fee']:.4f}")

    # 测试结算周期
    print("\n4. 结算周期测试")
    dates_to_test = [
        date(2023, 6, 1),  # T+2时期
        date(2024, 6, 1),  # T+1时期
    ]
    for test_date in dates_to_test:
        settlement_info = USSettlementCalculator.get_settlement_info(test_date)
        print(f"  {test_date}: {settlement_info['settlement_cycle']}, 结算日: {settlement_info['settlement_date']}")

    # 测试熔断机制
    print("\n5. 熔断机制测试")
    test_cases = [
        (5000, 4500, time(10, 0), "S&P 500下跌10% (上午10点)"),
        (5000, 4500, time(15, 30), "S&P 500下跌10% (下午3:30)"),
        (5000, 4350, time(10, 0), "S&P 500下跌13%"),
        (5000, 4000, time(10, 0), "S&P 500下跌20%"),
    ]
    for prev_close, current, trigger_time, desc in test_cases:
        result = USCircuitBreakerCalculator.check_circuit_breaker(current, prev_close, trigger_time, date.today())
        print(f"  {desc}:")
        print(f"    触发: {result['triggered']}, 级别: {result['level']}, "
              f"下跌: {result['decline_pct']:.2%}, 暂停: {result['pause_duration']}")

    # 测试个股限制暂停
    print("\n6. 个股限制暂停测试")
    individual_tests = [
        ('AAPL', 190.0, 175.0, "上涨8.6%"),
        ('AAPL', 184.0, 175.0, "上涨5.1%"),
        ('AAPL', 165.0, 175.0, "下跌5.7%"),
        ('AAPL', 166.0, 175.0, "下跌5.1%"),
    ]
    for symbol, current, ref, desc in individual_tests:
        result = USCircuitBreakerCalculator.check_individual_pause(symbol, current, ref, date.today())
        print(f"  {desc}: 触发={result['triggered']}, "
              f"涨跌幅={result['move_pct']:.2%}, 暂停时长={result['pause_duration']}分钟")

    # 测试规则版本查询
    print("\n7. 规则版本查询测试")
    rule_dates = [
        date(2020, 1, 1),
        date(2021, 1, 1),
        date(2023, 1, 1),
        date(2024, 6, 1),
    ]
    print("  熔断阈值规则:")
    for test_date in rule_dates:
        rule = RuleVersion.get_applicable_rule(PRICE_LIMIT_VERSIONS, test_date, None)
        if rule:
            threshold = rule.get('individual_security_pause', 'N/A')
            print(f"    {test_date}: 个股熔断阈值 = {threshold}")

    print("\n  结算周期规则:")
    for test_date in rule_dates:
        rule = RuleVersion.get_applicable_rule(SETTLEMENT_VERSIONS, test_date, None)
        if rule:
            cycle = rule.get('settlement_cycle', 'N/A')
            print(f"    {test_date}: {cycle}")

    # 测试交易时段
    print("\n8. 交易时段测试")
    rule = RuleVersion.get_applicable_rule(TRADING_SCHEDULE_VERSIONS, date(2024, 1, 1), None)
    if rule:
        print("  美股交易时段:")
        print(f"    盘前交易: {rule['pre_market_start']} - {rule['pre_market_end']}")
        print(f"    正常交易: {rule['regular_start']} - {rule['regular_end']}")
        print(f"    盘后交易: {rule['post_market_start']} - {rule['post_market_end']}")

    # 测试市场时段判断
    print("\n9. 市场时段判断测试")
    from us_trading_rules_versions import is_market_hours
    test_times = [
        (datetime(2024, 6, 1, 4, 30), 'pre_market', "盘前"),
        (datetime(2024, 6, 1, 10, 0), 'regular', "正常交易"),
        (datetime(2024, 6, 1, 16, 30), 'post_market', "盘后"),
        (datetime(2024, 6, 1, 22, 0), 'regular', "非交易时间"),
    ]
    for dt, session, desc in test_times:
        is_trading = is_market_hours(dt, session)
        print(f"  {dt.strftime('%H:%M')} ({desc}): {'是' if is_trading else '否'}")

    print("\n" + "=" * 60)
    print("基础功能测试完成")
    print("=" * 60)


if __name__ == '__main__':
    test_basic_functions()
