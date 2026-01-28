"""
ETF适配器测试脚本

测试新增的规则版本控制和计算功能
"""

from datetime import date
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))


def test_etf_rules():
    """测试ETF规则功能"""
    print("=" * 60)
    print("ETF适配器规则测试")
    print("=" * 60)

    # 直接导入模块（避免触发backtest包的导入）
    import importlib.util

    # 加载etf_trading_rules_versions模块
    spec1 = importlib.util.spec_from_file_location(
        'etf_rules',
        '/mnt/ssd2/finhack-dev/finhack/finhack/trader/backtest/markets/cn_fund/etf_trading_rules_versions.py'
    )
    etf_rules = importlib.util.module_from_spec(spec1)
    spec1.loader.exec_module(etf_rules)

    # ========================================================================
    # 测试1: ETF板块识别
    # ========================================================================
    print("\n【测试1】ETF板块识别")
    print("-" * 60)
    test_symbols = [
        ('510300.SH', '沪深300ETF', 'main_board'),
        ('588000.SH', '科创50ETF', 'star_market'),
        ('159949.SZ', '创业板50ETF', 'gem'),  # 使用正确的创业板ETF代码
    ]

    for symbol, name, expected_board in test_symbols:
        board = etf_rules.get_etf_board_type(symbol)
        etf_type = etf_rules.get_etf_type(symbol)
        exchange = etf_rules.get_exchange_from_symbol(symbol)
        status = "✓" if board == expected_board else "✗"
        print(f"{status} {symbol} ({name}): board={board}, type={etf_type}, exchange={exchange}")

    # ========================================================================
    # 测试2: 涨跌停价计算
    # ========================================================================
    print("\n【测试2】涨跌停价计算")
    print("-" * 60)

    # 加载etf_calculator模块
    spec2 = importlib.util.spec_from_file_location(
        'etf_calc',
        '/mnt/ssd2/finhack-dev/finhack/finhack/trader/backtest/markets/cn_fund/etf_calculator.py'
    )
    etf_calc = importlib.util.module_from_spec(spec2)
    # 注入依赖
    sys.modules['etf_rules'] = etf_rules
    spec2.loader.exec_module(etf_calc)

    test_date = date(2024, 1, 15)
    test_cases = [
        ('510300.SH', 4.500, 0.10),  # 主板ETF 10%
        ('588000.SH', 1.200, 0.20),  # 科创板ETF 20%
        ('159949.SZ', 2.300, 0.20),  # 创业板ETF 20%
    ]

    for symbol, prev_close, expected_limit in test_cases:
        result = etf_calc.ETFPriceCalculator.calculate_limit_prices(symbol, prev_close, test_date)
        actual_limit = result['limit_ratio']
        status = "✓" if actual_limit == expected_limit else "✗"
        print(f"{status} {symbol} (前收盘: {prev_close}):")
        print(f"    涨停: {result['upper_limit']:.3f}, 跌停: {result['lower_limit']:.3f}, 涨跌幅: {actual_limit*100:.0f}%")

    # ========================================================================
    # 测试3: 不同时期的涨跌幅规则
    # ========================================================================
    print("\n【测试3】不同时期的涨跌幅规则（创业板）")
    print("-" * 60)

    # 创业板注册制前后对比
    symbol = '159949.SZ'  # 创业板50ETF
    prev_close = 2.000

    # 注册制前（2020年8月24日前）
    date_before = date(2020, 8, 20)
    result_before = etf_calc.ETFPriceCalculator.calculate_limit_prices(symbol, prev_close, date_before)
    print(f"2020-08-20 (注册制前): 涨跌幅 = {result_before['limit_ratio']*100:.0f}%")

    # 注册制后（2020年8月24日后）
    date_after = date(2020, 8, 25)
    result_after = etf_calc.ETFPriceCalculator.calculate_limit_prices(symbol, prev_close, date_after)
    print(f"2020-08-25 (注册制后): 涨跌幅 = {result_after['limit_ratio']*100:.0f}%")

    # ========================================================================
    # 测试4: 最小交易单位
    # ========================================================================
    print("\n【测试4】最小交易单位")
    print("-" * 60)

    lot_cases = [
        ('510300.SH', 100, 100),  # 主板：100股起买，100股递增
        ('588000.SH', 200, 1),    # 科创板：200股起买，1股递增
        ('159949.SZ', 100, 1),    # 创业板：100股起买，1股递增
    ]

    for symbol, expected_min_buy, expected_increment in lot_cases:
        lot_info = etf_calc.ETFLotSizeCalculator.get_lot_size_info(symbol, test_date)
        min_buy = lot_info['min_buy']
        increment = lot_info['buy_increment']
        status = "✓" if min_buy == expected_min_buy and increment == expected_increment else "✗"
        print(f"{status} {symbol}: 最小买入={min_buy}股, 递增={increment}股")

    # ========================================================================
    # 测试5: 历史规则变更日期
    # ========================================================================
    print("\n【测试5】涨跌幅规则变更日期")
    print("-" * 60)

    change_dates = etf_rules.RuleVersion.get_rule_change_dates(
        etf_rules.PRICE_LIMIT_VERSIONS
    )
    print("涨跌幅规则变更日期:")
    for d in change_dates:
        print(f"  {d.strftime('%Y-%m-%d')}")

    # ========================================================================
    # 测试6: 价格计算精度（科创板）
    # ========================================================================
    print("\n【测试6】涨跌停价计算精度（科创板）")
    print("-" * 60)

    # 科创板：价格<200元tick_size=0.01，>=200元tick_size=0.10
    star_cases = [
        ('588000.SH', 1.500, None),  # 低价格
        ('588000.SH', 250.00, None),  # 高价格
    ]

    for symbol, prev_close, _ in star_cases:
        result = etf_calc.ETFPriceCalculator.calculate_limit_prices(symbol, prev_close, test_date)
        tick_size = 0.01 if prev_close < 200 else 0.10
        print(f"{symbol} 前收盘 {prev_close:.2f}元 (tick_size={tick_size}):")
        print(f"    涨停: {result['upper_limit']:.3f}, 跌停: {result['lower_limit']:.3f}")

    # ========================================================================
    # 测试7: 手续费计算
    # ========================================================================
    print("\n【测试7】手续费计算")
    print("-" * 60)

    fee_cases = [
        ('510300.SH', 'stock', 0.001, True),    # 股票ETF有印花税
        ('511010.SH', 'bond', 0.0, False),      # 债券ETF免印花税
        ('511880.SH', 'money', 0.0, False),     # 货币ETF免手续费
    ]

    for symbol, expected_type, expected_tax, has_tax in fee_cases:
        etf_type = etf_rules.get_etf_type(symbol)
        commission_info = etf_calc.ETFCommissionCalculator.get_commission_info(symbol, test_date)
        sell_tax = commission_info['stamp_tax_sell']
        commission_rate = commission_info['commission_rate']

        type_ok = etf_type == expected_type
        tax_ok = (sell_tax > 0) == has_tax
        status = "✓" if type_ok and tax_ok else "✗"
        print(f"{status} {symbol}: type={etf_type}, 佣金={commission_rate*100:.3f}%, 印花税={sell_tax*100:.2f}%")

    print("\n" + "=" * 60)
    print("测试完成！")
    print("=" * 60)


if __name__ == '__main__':
    test_etf_rules()
