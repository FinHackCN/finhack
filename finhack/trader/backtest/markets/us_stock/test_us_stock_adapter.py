"""
美股市场适配器测试
"""

import sys
import os

# 添加项目路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from datetime import date, time, datetime
from finhack.trader.backtest.markets.us_stock import (
    USStockMarketAdapter,
    get_exchange_from_symbol,
    get_stock_type,
    USCircuitBreakerCalculator,
    USLotSizeCalculator,
    USCommissionCalculator,
    USSettlementCalculator,
)


def test_us_stock_adapter():
    """测试美股市场适配器基础功能"""

    print("=" * 60)
    print("美股市场适配器测试")
    print("=" * 60)

    # 创建适配器
    adapter = USStockMarketAdapter()

    # 测试市场信息
    print("\n1. 市场信息")
    market_info = adapter.get_market_info('AAPL')
    for key, value in market_info.items():
        print(f"  {key}: {value}")

    # 测试交易时段
    print("\n2. 交易时段")
    trade_date = date(2024, 6, 1)
    sessions = adapter.get_trading_sessions_for_date(trade_date, 'AAPL', '1d')
    for session in sessions:
        print(f"  {session['type']}: {session['start']} - {session['end']} ({session['description']})")

    # 测试事件生成
    print("\n3. 事件生成 (1d)")
    events = adapter.generate_daily_events(trade_date, '1d', 'AAPL')
    print(f"  生成了 {len(events)} 个事件:")
    for event in events[:10]:  # 只显示前10个
        print(f"    {event.event_time.strftime('%H:%M')} - {event.event_type.value}: {event.event_description}")

    # 测试交易时间判断
    print("\n4. 交易时间判断")
    test_times = [
        datetime(2024, 6, 1, 4, 30),   # 盘前
        datetime(2024, 6, 1, 10, 0),  # 正常交易
        datetime(2024, 6, 1, 16, 30), # 盘后
        datetime(2024, 6, 1, 22, 0),  # 非交易时间
        datetime(2024, 6, 2, 10, 0),  # 周日
    ]
    for dt in test_times:
        is_trading = adapter.is_trading_time(dt, '1d', 'AAPL')
        print(f"  {dt.strftime('%Y-%m-%d %H:%M')}: {'交易时间' if is_trading else '非交易时间'}")

    # 测试持仓限制
    print("\n5. 持仓限制 (支持零碎股)")
    limits = adapter.get_position_limits('AAPL')
    for key, value in limits.items():
        print(f"  {key}: {value}")

    # 测试手续费率
    print("\n6. 手续费率")
    buy_rates = adapter.get_commission_rate('AAPL', 'buy')
    sell_rates = adapter.get_commission_rate('AAPL', 'sell')
    print("  买入:")
    for key, value in buy_rates.items():
        print(f"    {key}: {value}")
    print("  卖出:")
    for key, value in sell_rates.items():
        print(f"    {key}: {value}")

    # 测试结算周期
    print("\n7. 结算周期")
    settlement_cycle = adapter.get_settlement_cycle()
    print(f"  当前结算周期: {settlement_cycle}")

    # 测试不同日期的结算周期
    dates_to_test = [
        date(2023, 6, 1),  # T+2时期
        date(2024, 6, 1),  # T+1时期
    ]
    print("  不同时期结算周期:")
    for test_date in dates_to_test:
        adapter.set_current_date(test_date)
        cycle = adapter.get_settlement_cycle()
        settlement_date = adapter.get_settlement_date(test_date)
        print(f"    {test_date}: {cycle}, 结算日: {settlement_date}")

    # 恢复当前日期
    adapter.set_current_date(date.today())

    # 测试熔断机制
    print("\n8. 熔断机制")
    test_cases = [
        (5000, 4500, "S&P 500下跌10%"),
        (5000, 4350, "S&P 500下跌13%"),
        (5000, 4000, "S&P 500下跌20%"),
    ]
    for prev_close, current, desc in test_cases:
        result = adapter.check_circuit_breaker(current, prev_close, time(10, 0))
        print(f"  {desc}:")
        print(f"    触发: {result['triggered']}, 级别: {result['level']}, 暂停: {result['pause_duration']}分钟")

    # 测试个股熔断
    print("\n9. 个股限制暂停")
    individual_result = adapter.check_individual_pause('AAPL', 190.0, 175.0)  # 上涨约8.6%
    print(f"  AAPL从175涨到190: 触发={individual_result['triggered']}, 涨跌幅={individual_result['move_pct']:.2%}")

    individual_result2 = adapter.check_individual_pause('AAPL', 165.0, 175.0)  # 下跌约5.7%
    print(f"  AAPL从175跌到165: 触发={individual_result2['triggered']}, 涨跌幅={individual_result2['move_pct']:.2%}")

    # 测试PDT规则
    print("\n10. PDT规则检查")
    pdt_cases = [
        (24000, 1, "账户净值$24,000, 已交易1次"),
        (24000, 3, "账户净值$24,000, 已交易3次"),
        (30000, 5, "账户净值$30,000, 已交易5次"),
    ]
    for equity, trades, desc in pdt_cases:
        result = adapter.check_pdt_restriction(equity, trades)
        print(f"  {desc}:")
        print(f"    受限: {result['restricted']}, 剩余次数: {result['remaining_trades']}")

    # 测试手续费计算
    print("\n11. 手续费计算")
    commission_test_cases = [
        ('AAPL', 100, 175.0, 'buy', "买入100股AAPL @ $175"),
        ('AAPL', 100, 175.0, 'sell', "卖出100股AAPL @ $175"),
        ('AAPL', 0.5, 175.0, 'buy', "买入0.5股AAPL @ $175 (零碎股)"),
    ]
    for symbol, volume, price, side, desc in commission_test_cases:
        fee = USCommissionCalculator.calculate_commission(symbol, volume, price, side)
        print(f"  {desc}:")
        print(f"    佣金: ${fee['commission']:.4f}, SEC费用: ${fee['sec_fee']:.4f}, "
              f"交易活动费: ${fee['trading_activity_fee']:.4f}, 总费用: ${fee['total_fee']:.4f}")

    # 测试股票分类
    print("\n12. 股票分类")
    test_symbols = ['AAPL', 'TSLA', 'SPY', 'QQQ', 'WMT']
    for symbol in test_symbols:
        exchange = get_exchange_from_symbol(symbol)
        stock_type = get_stock_type(symbol)
        print(f"  {symbol}: 交易所={exchange}, 类型={stock_type}")

    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)


if __name__ == '__main__':
    test_us_stock_adapter()
