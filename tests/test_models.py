"""Tests for Order, Account, Position models."""
import pytest
from datetime import datetime


class TestOrderStateMachine:
    def setup_method(self):
        from finhack.trader.backtest.models.order import Order
        from finhack.trader.backtest.models.enums import Side, OrderType
        self.Order = Order
        self.Side = Side
        self.OrderType = OrderType

    def _new_order(self, **kwargs):
        from finhack.trader.backtest.models.enums import OrderStatus
        defaults = dict(
            account_id='test', symbol='600000.SH', side=self.Side.BUY,
            order_type=self.OrderType.MARKET, volume=100, order_id='o1',
        )
        defaults.update(kwargs)
        order = self.Order(**defaults)
        order.update_status(OrderStatus.NEW)
        return order

    def test_initial_status(self):
        from finhack.trader.backtest.models.enums import OrderStatus
        order = self.Order(
            account_id='test', symbol='600000.SH', side=self.Side.BUY,
            order_type=self.OrderType.MARKET, volume=100, order_id='o1',
        )
        assert order.status == OrderStatus.PENDING_NEW

    def test_valid_transition_new_to_filled(self):
        from finhack.trader.backtest.models.enums import OrderStatus
        order = self._new_order()
        order.add_fill(100, 10.0)
        assert order.status == OrderStatus.FILLED

    def test_valid_transition_new_to_cancelled(self):
        from finhack.trader.backtest.models.enums import OrderStatus
        order = self._new_order()
        order.cancel()
        assert order.status == OrderStatus.CANCELLED

    def test_invalid_transition_filled_to_new(self):
        from finhack.trader.backtest.models.enums import OrderStatus
        order = self._new_order()
        order.add_fill(100, 10.0)
        with pytest.raises(ValueError):
            order.update_status(OrderStatus.NEW)

    def test_cancel_finished_order_raises(self):
        order = self._new_order()
        order.add_fill(100, 10.0)
        with pytest.raises(ValueError):
            order.cancel()


class TestOrderProperties:
    def setup_method(self):
        from finhack.trader.backtest.models.order import Order
        from finhack.trader.backtest.models.enums import Side, OrderType, OrderStatus
        self.Order = Order
        self.Side = Side
        self.OrderType = OrderType
        self.OrderStatus = OrderStatus

    def _make_order(self, volume=100, filled=0):
        order = self.Order(
            account_id='test', symbol='TEST', side=self.Side.BUY,
            order_type=self.OrderType.MARKET, volume=volume, order_id='o1',
        )
        if filled > 0:
            order.update_status(self.OrderStatus.NEW)
            order.add_fill(filled, 10.0)
        return order

    def test_remaining_volume_unfilled(self):
        order = self._make_order(volume=100)
        assert order.remaining_volume == 100

    def test_remaining_volume_partial(self):
        order = self.Order(
            account_id='test', symbol='TEST', side=self.Side.BUY,
            order_type=self.OrderType.MARKET, volume=100, order_id='o1',
        )
        from finhack.trader.backtest.models.enums import OrderStatus; order.update_status(OrderStatus.NEW)
        order.add_fill(30, 10.0)
        assert order.remaining_volume == pytest.approx(70.0)

    def test_fill_ratio_zero(self):
        order = self._make_order(volume=100)
        assert order.fill_ratio == 0.0

    def test_fill_ratio_full(self):
        order = self.Order(
            account_id='test', symbol='TEST', side=self.Side.BUY,
            order_type=self.OrderType.MARKET, volume=100, order_id='o1',
        )
        from finhack.trader.backtest.models.enums import OrderStatus; order.update_status(OrderStatus.NEW)
        order.add_fill(100, 10.0)
        assert order.fill_ratio == 1.0

    def test_is_active(self):
        order = self._make_order(volume=100)
        from finhack.trader.backtest.models.enums import OrderStatus; order.update_status(OrderStatus.NEW)
        assert order.is_active is True

    def test_is_finished(self):
        order = self.Order(
            account_id='test', symbol='TEST', side=self.Side.BUY,
            order_type=self.OrderType.MARKET, volume=100, order_id='o1',
        )
        from finhack.trader.backtest.models.enums import OrderStatus; order.update_status(OrderStatus.NEW)
        order.add_fill(100, 10.0)
        assert order.is_finished is True

    def test_add_fill_exceeds_capped(self):
        order = self._make_order(volume=100)
        from finhack.trader.backtest.models.enums import OrderStatus; order.update_status(OrderStatus.NEW)
        order.add_fill(200, 10.0)
        assert order.filled_volume == pytest.approx(100.0)

    def test_add_fill_on_finished_raises(self):
        order = self.Order(
            account_id='test', symbol='TEST', side=self.Side.BUY,
            order_type=self.OrderType.MARKET, volume=100, order_id='o1',
        )
        from finhack.trader.backtest.models.enums import OrderStatus; order.update_status(OrderStatus.NEW)
        order.add_fill(100, 10.0)
        with pytest.raises(ValueError):
            order.add_fill(1, 10.0)


class TestAccountFromDict:
    def test_roundtrip(self):
        from finhack.trader.backtest.models.account import Account
        data = {
            'account_id': 'acc1',
            'platform': 'BACKTEST',
            'account_type': 'CASH',
            'status': 'DATA_READY',
            'currency': 'CNY',
            'total_assets': 100000.0,
            'cash_available': 80000.0,
            'cash_frozen': 5000.0,
            'market_value': 15000.0,
        }
        account = Account.from_dict(data)
        assert account.account_id == 'acc1'
        assert account.total_assets == 100000.0

        out = account.to_dict()
        assert out['account_id'] == 'acc1'
        assert out['total_assets'] == 100000.0


class TestPosition:
    def setup_method(self):
        from finhack.trader.backtest.models.position import Position
        from finhack.trader.backtest.models.enums import PositionSide
        self.Position = Position
        self.PositionSide = PositionSide

    def test_update_market_price_long(self):
        pos = self.Position(
            account_id='test', symbol='TEST', volume=100, available_volume=100,
            cost_price=10.0, contract_multiplier=1,
            position_side=self.PositionSide.LONG,
        )
        pos.update_market_price(12.0)
        assert pos.unrealized_pnl == pytest.approx(200.0)  # (12-10)*100

    def test_update_market_price_short(self):
        pos = self.Position(
            account_id='test', symbol='TEST', volume=100, available_volume=100,
            cost_price=10.0, contract_multiplier=1,
            position_side=self.PositionSide.SHORT,
        )
        pos.update_market_price(8.0)
        assert pos.unrealized_pnl == pytest.approx(200.0)  # (10-8)*100

    def test_market_value_updates(self):
        pos = self.Position(
            account_id='test', symbol='TEST', volume=100, available_volume=100,
            cost_price=10.0, contract_multiplier=1,
            position_side=self.PositionSide.LONG,
        )
        pos.update_market_price(15.0)
        assert pos.market_value == pytest.approx(1500.0)
