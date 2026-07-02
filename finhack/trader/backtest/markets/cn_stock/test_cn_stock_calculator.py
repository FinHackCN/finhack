# -*- coding: utf-8 -*-
"""cn_stock 涨跌停计算器/适配器 单元测试

可直接运行：python3 test_cn_stock_calculator.py
（内置 sys.path 兼容"包内运行"与"独立运行"两种模式）
"""

import sys
import os
from datetime import date, datetime

# 兼容独立运行：把 cn_stock / cn_fund 目录加入 path，走 *_trading_rules_versions 的 fallback import
_HERE = os.path.dirname(os.path.abspath(__file__))
_MARKETS = os.path.dirname(_HERE)
for _sub in ('cn_stock', 'cn_fund'):
    _p = os.path.join(_MARKETS, _sub)
    if _p not in sys.path:
        sys.path.insert(0, _p)

from cn_stock_calculator import StockPriceCalculator  # type: ignore


def test_calculate_limit_prices():
    C = StockPriceCalculator.calculate_limit_prices
    assert C("600519.SH", 100.00, date(2024, 1, 15))['upper_limit'] == 110.00
    assert C("600519.SH", 100.00, date(2024, 1, 15))['lower_limit'] == 90.00
    assert C("688981.SH", 50.00, date(2024, 1, 15))['upper_limit'] == 60.00
    assert C("688981.SH", 50.00, date(2024, 1, 15))['lower_limit'] == 40.00
    # ST 主板 5%
    r = C("000004.SZ", 5.00, date(2024, 1, 15), is_st=True)
    assert r['upper_limit'] == 5.25 and r['lower_limit'] == 4.75
    # 创业板日期感知：2019=10%, 2021=20%
    assert C("300750.SZ", 100.00, date(2019, 6, 1))['upper_limit'] == 110.00
    assert C("300750.SZ", 100.00, date(2021, 6, 1))['upper_limit'] == 120.00
    # 北交所 30%
    r = C("830799.BJ", 10.00, date(2022, 1, 1))
    assert r['upper_limit'] == 13.00 and r['lower_limit'] == 7.00
    # 新股无限制
    r = C("688111.SH", 50.00, date(2024, 1, 17), list_date=date(2024, 1, 15))
    assert r['is_new_stock'] is True and r['upper_limit'] == float('inf')
    # 取整：floor/ceil
    r = C("600000.SH", 10.03, date(2024, 1, 15))
    assert r['upper_limit'] == 11.03 and r['lower_limit'] == 9.03
    print("[OK] calculate_limit_prices")


def test_validate_order_price():
    V = StockPriceCalculator.validate_order_price
    d = date(2024, 1, 15)
    assert V("688981.SH", 59.0, "buy", 50.0, d)['valid'] is True
    assert V("688981.SH", 61.0, "buy", 50.0, d)['valid'] is False      # 超涨停
    assert V("688981.SH", 39.0, "sell", 50.0, d)['valid'] is False     # 低于跌停
    assert V("688981.SH", 60.0, "buy", 50.0, d)['valid'] is True       # 贴涨停=合法(封板语义由adapter处理)
    assert V("688981.SH", 60.0, "buy", 0.0, d)['valid'] is True        # 无前收盘→放行
    print("[OK] validate_order_price")


if __name__ == "__main__":
    test_calculate_limit_prices()
    test_validate_order_price()
    print("全部通过")
