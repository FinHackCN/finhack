"""Tests for TradeCenter.try_match_orders_sync."""
import pytest
from finhack.trader.backtest.models.enums import OrderStatus


class TestMatchingBasic:
    def test_market_order_fills_daily(self, cn_stock_tc, buy_order):
        """Daily freq: MARKET order fills fully at market price."""
        market_data = {'600000.SH': {'close': 10.5, 'volume': 100000}}
        buy_order.update_status(OrderStatus.NEW)
        cn_stock_tc.active_orders[buy_order.order_id] = buy_order

        cn_stock_tc.try_match_orders_sync(market_data)

        assert buy_order.status == OrderStatus.FILLED
        assert buy_order.order_id not in cn_stock_tc.active_orders

    def test_limit_buy_fills_when_price_below(self, cn_stock_tc, limit_buy_order):
        """LIMIT BUY at 10.0 fills when market price is 9.5."""
        market_data = {'600000.SH': {'close': 9.5, 'volume': 10000}}
        limit_buy_order.update_status(OrderStatus.NEW)
        cn_stock_tc.active_orders[limit_buy_order.order_id] = limit_buy_order

        cn_stock_tc.try_match_orders_sync(market_data)

        assert limit_buy_order.status == OrderStatus.FILLED

    def test_limit_buy_skips_when_price_above(self, cn_stock_tc, limit_buy_order):
        """LIMIT BUY at 10.0 skips when market price is 10.5."""
        market_data = {'600000.SH': {'close': 10.5, 'volume': 10000}}
        limit_buy_order.update_status(OrderStatus.NEW)
        cn_stock_tc.active_orders[limit_buy_order.order_id] = limit_buy_order

        cn_stock_tc.try_match_orders_sync(market_data)

        assert limit_buy_order.order_id in cn_stock_tc.active_orders

    def test_empty_market_data_skips(self, cn_stock_tc, buy_order):
        """No matching when market_data is empty."""
        buy_order.update_status(OrderStatus.NEW)
        cn_stock_tc.active_orders[buy_order.order_id] = buy_order

        cn_stock_tc.try_match_orders_sync({})

        assert buy_order.order_id in cn_stock_tc.active_orders

    def test_zero_market_volume_skips(self, cn_stock_tc, buy_order):
        """Skips matching when market_volume is 0."""
        market_data = {'600000.SH': {'close': 10.0, 'volume': 0}}
        buy_order.update_status(OrderStatus.NEW)
        cn_stock_tc.active_orders[buy_order.order_id] = buy_order

        cn_stock_tc.try_match_orders_sync(market_data)

        assert buy_order.order_id in cn_stock_tc.active_orders


class TestZombieCleanup:
    def test_zombie_order_cleaned(self, cn_future_1m_tc):
        """Orders with remaining_volume < 1e-6 are force-completed."""
        from finhack.trader.backtest.models.order import Order
        from finhack.trader.backtest.models.enums import Side, OrderType, OrderStatus

        tc = cn_future_1m_tc
        # Create an order with tiny remaining_volume
        order = Order(
            account_id='test', symbol='IF2401.CFFEX', side=Side.BUY,
            order_type=OrderType.MARKET, volume=1.0, order_id='zombie_1',
        )
        order.update_status(OrderStatus.NEW)
        # Simulate 99.9999% filled
        order.filled_volume = 1.0 - 1e-9
        tc.active_orders[order.order_id] = order

        market_data = {'IF2401.CFFEX': {'close': 3500.0, 'volume': 100}}
        tc.try_match_orders_sync(market_data)

        assert order.status == OrderStatus.FILLED
        assert order.order_id not in tc.active_orders
