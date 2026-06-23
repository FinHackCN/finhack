"""Tests for TradeCenter.validate_order_sync."""
import pytest


class TestValidateOrderBasic:
    def test_rejects_zero_volume(self, cn_stock_tc, buy_order):
        buy_order.volume = 0
        assert cn_stock_tc.validate_order_sync(buy_order) is False

    def test_rejects_negative_volume(self, cn_stock_tc, buy_order):
        buy_order.volume = -10
        assert cn_stock_tc.validate_order_sync(buy_order) is False

    def test_rejects_limit_no_price(self, limit_buy_order, cn_stock_tc):
        limit_buy_order.price = None
        assert cn_stock_tc.validate_order_sync(limit_buy_order) is False

    def test_rejects_zero_price_limit(self, limit_buy_order, cn_stock_tc):
        limit_buy_order.price = 0
        assert cn_stock_tc.validate_order_sync(limit_buy_order) is False

    def test_market_order_no_price_ok(self, cn_stock_tc, buy_order):
        buy_order.price = None
        assert cn_stock_tc.validate_order_sync(buy_order) is True


class TestValidateOrderLotSize:
    def test_rejects_wrong_lot_size_cn_stock(self, cn_stock_tc, buy_order):
        """cn_stock lot_size=100, volume=150 should fail."""
        buy_order.volume = 150
        assert cn_stock_tc.validate_order_sync(buy_order) is False

    def test_accepts_correct_lot_size(self, cn_stock_tc, buy_order):
        buy_order.volume = 200
        assert cn_stock_tc.validate_order_sync(buy_order) is True


class TestValidateOrderFutureMargin:
    def test_rejects_insufficient_margin(self, cn_future_tc):
        from finhack.trader.backtest.models.order import Order
        from finhack.trader.backtest.models.enums import Side, OrderType
        from unittest.mock import MagicMock

        # Mock market_adapter with real IF contract multiplier=300, margin_ratio=0.15
        adapter = MagicMock()
        adapter.get_contract_size.return_value = 300
        adapter.get_margin_ratio.return_value = 0.15
        adapter.get_delivery_info.return_value = {'natural_person_ban': {'is_banned': False}}
        adapter.validate_order.return_value = (True, '')
        cn_future_tc.market_adapter = adapter

        # 1M cash, buy IF at 4000 with volume 100 → margin = 100*4000*300*0.15 = 18M >> 1M
        order = Order(
            account_id='test', symbol='IF2401.CFFEX', side=Side.BUY,
            order_type=OrderType.LIMIT, volume=100, order_id='future_1',
            price=4000.0,
        )
        assert cn_future_tc.validate_order_sync(order) is False

    def test_accepts_sufficient_margin(self, cn_future_tc):
        from finhack.trader.backtest.models.order import Order
        from finhack.trader.backtest.models.enums import Side, OrderType
        from unittest.mock import MagicMock

        # Mock market_adapter with real IF contract multiplier=300, margin_ratio=0.15
        adapter = MagicMock()
        adapter.get_contract_size.return_value = 300
        adapter.get_margin_ratio.return_value = 0.15
        adapter.get_delivery_info.return_value = {'natural_person_ban': {'is_banned': False}}
        adapter.validate_order.return_value = (True, '')
        cn_future_tc.market_adapter = adapter

        # 1M cash, buy 1 IF at 3500 → margin = 1*3500*300*0.15 = 157500 < 1M
        order = Order(
            account_id='test', symbol='IF2401.CFFEX', side=Side.BUY,
            order_type=OrderType.LIMIT, volume=1, order_id='future_2',
            price=3500.0,
        )
        assert cn_future_tc.validate_order_sync(order) is True


class TestValidateOrderShortClose:
    def test_rejects_no_position(self, cn_future_tc):
        from finhack.trader.backtest.models.order import Order
        from finhack.trader.backtest.models.enums import Side, OrderType
        order = Order(
            account_id='test', symbol='IF2401.CFFEX', side=Side.SHORT_CLOSE,
            order_type=OrderType.MARKET, volume=1, order_id='sc_1',
        )
        assert cn_future_tc.validate_order_sync(order) is False
