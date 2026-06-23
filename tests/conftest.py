"""Fixtures for backtest engine tests.

Solves the import problem: the __init__.py chain imports DataCenter -> runtime.constant
which only exists in demo_project. We add the package root to sys.path and stub
runtime.constant so the full import chain works.
"""
import sys
import types
import pytest
from datetime import datetime

# Add finhack package root BEFORE any finhack imports
_PKG_ROOT = '/mnt/ssd2/finhack-dev/finhack'
if _PKG_ROOT not in sys.path:
    sys.path.insert(0, _PKG_ROOT)

# Stub runtime.constant (required by finhack.library.data -> data_center.py)
if 'runtime' not in sys.modules:
    sys.modules['runtime'] = types.ModuleType('runtime')
if 'runtime.constant' not in sys.modules:
    _rt = types.ModuleType('runtime.constant')
    for _attr in ['FRAMEWORK_DIR', 'BASE_DIR', 'DATA_DIR', 'CACHE_DIR', 'KLINE_DIR',
                  'REPORTS_DIR', 'BACKTEST_DIR', 'CONFIG_DIR', 'MODELS_DIR', 'PREDS_DIR',
                  'LOGS_DIR', 'USER_DIR', 'RUNNING_DIR', 'FACTORS_DIR', 'FACTORS_CACHE_DIR',
                  'PRICE_CACHE_DIR', 'CHOICE_CACHE_DIR', 'KV_CACHE_DIR', 'INDICATORS_DIR',
                  'CHECKPOINT_DIR', 'STRATEGIES_DIR']:
        setattr(_rt, _attr, '/tmp/dummy')
    sys.modules['runtime.constant'] = _rt


def _make_context(market='cn_stock', freq='1d', cash=1000000.0):
    return {
        'account': {
            'account_id': 'test_account',
            'platform': 'BACKTEST',
            'account_type': 'CASH',
            'currency': 'CNY',
            'total_assets': cash,
            'cash_available': cash,
        },
        'settings': {
            'market': market,
            'freq': freq,
            'slip_type': 'pricerelated',
            'slip_value': 0.001,
            'open_commission': 0.0003,
            'close_commission': 0.0003,
            'min_commission': 5.0,
            'open_tax': 0.0,
            'close_tax': 0.001,
        },
        'current_dt': datetime(2024, 1, 15, 10, 0, 0),
    }


@pytest.fixture
def cn_stock_tc():
    """TradeCenter with cn_stock context."""
    from finhack.trader.backtest.engine.backtest_engine import TradeCenter
    return TradeCenter(_make_context('cn_stock', '1d', 1000000))


@pytest.fixture
def cn_future_tc():
    """TradeCenter with cn_future context."""
    from finhack.trader.backtest.engine.backtest_engine import TradeCenter
    ctx = _make_context('cn_future', '1d', 1000000)
    ctx['settings']['close_tax'] = 0.0
    return TradeCenter(ctx)


@pytest.fixture
def cn_future_1m_tc():
    """TradeCenter with cn_future 1m context."""
    from finhack.trader.backtest.engine.backtest_engine import TradeCenter
    ctx = _make_context('cn_future', '1m', 1000000)
    ctx['settings']['close_tax'] = 0.0
    return TradeCenter(ctx)


@pytest.fixture
def buy_order():
    """A simple BUY MARKET order."""
    from finhack.trader.backtest.models.order import Order
    from finhack.trader.backtest.models.enums import Side, OrderType
    return Order(
        account_id='test_account', symbol='600000.SH', side=Side.BUY,
        order_type=OrderType.MARKET, volume=100, order_id='test_order_001',
    )


@pytest.fixture
def limit_buy_order():
    """A BUY LIMIT order."""
    from finhack.trader.backtest.models.order import Order
    from finhack.trader.backtest.models.enums import Side, OrderType
    return Order(
        account_id='test_account', symbol='600000.SH', side=Side.BUY,
        order_type=OrderType.LIMIT, volume=100, order_id='test_limit_001',
        price=10.0,
    )
