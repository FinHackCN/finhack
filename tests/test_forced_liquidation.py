"""Tests for forced liquidation mechanism."""
import pytest
from unittest.mock import MagicMock, patch


class TestForcedLiquidation:
    def setup_method(self):
        from finhack.trader.backtest.engine.backtest_engine import TradeCenter
        self.TradeCenter = TradeCenter

    def _make_engine_with_position(self, cash=200000, entry_price=3500, volume=1,
                                    current_price=3500):
        """Create a minimal BacktestEngine-like object with a cn_future position."""
        from finhack.trader.backtest.models.position import Position
        from finhack.trader.backtest.models.account import Account
        from finhack.trader.backtest.models.enums import PositionSide
        from datetime import datetime

        ctx = {
            'account': {
                'account_id': 'test', 'platform': 'BACKTEST',
                'account_type': 'FUTURES', 'currency': 'CNY',
                'total_assets': cash, 'cash_available': cash,
            },
            'settings': {
                'market': 'cn_future', 'freq': '1d',
                'slip_type': 'pricerelated', 'slip_value': 0.001,
                'open_commission': 0.0001, 'close_commission': 0.0001,
                'min_commission': 5.0, 'open_tax': 0.0, 'close_tax': 0.0,
            },
            'current_dt': datetime(2024, 1, 15, 15, 0, 0),
        }

        tc = self.TradeCenter(ctx)

        # Add a position
        pos = Position(
            account_id='test', symbol='IF2401.CFFEX', volume=volume,
            available_volume=volume, cost_price=entry_price,
            contract_multiplier=300,
            position_side=PositionSide.LONG,
        )
        pos.update_market_price(current_price)
        tc.positions['IF2401.CFFEX'] = pos

        # Deduct margin from cash
        margin = volume * entry_price * 300 * 0.15
        tc.account.cash_available -= margin
        tc.account.margin_used = margin

        return tc, ctx

    def test_no_margin_call_when_equity_sufficient(self):
        """Normal position with enough equity should NOT trigger forced liquidation."""
        from finhack.trader.backtest.markets.cn_future.future_calculator import FutureMarginCalculator

        tc, ctx = self._make_engine_with_position(
            cash=500000, entry_price=3500, volume=1, current_price=3500,
        )

        # equity = cash_available + unrealized_pnl
        # cash_available = 500000 - 3500*300*0.15 = 500000 - 157500 = 342500
        # unrealized_pnl = (3500-3500)*1*300 = 0
        # equity = 342500, maintenance = 3500*300*0.10 = 105000 → no margin call

        result = FutureMarginCalculator.check_margin_call(
            symbol='IF2401.CFFEX',
            position=1,  # long 1 lot
            entry_price=3500.0,
            current_price=3500.0,
            account_balance=tc.account.cash_available,
            query_date=ctx['current_dt'].date(),
        )

        assert result['need_margin_call'] is False

    def test_margin_call_triggered_on_deep_loss(self):
        """Position with large unrealized loss should trigger margin call."""
        from finhack.trader.backtest.markets.cn_future.future_calculator import FutureMarginCalculator

        tc, ctx = self._make_engine_with_position(
            cash=50000, entry_price=4000, volume=1, current_price=3000,
        )
        position = tc.positions['IF2401.CFFEX']
        position.update_market_price(3000.0)

        # cash_available = 50000 - 4000*300*0.15 = 50000 - 180000 = -130000
        # equity = cash_available + unrealized_pnl = -130000 + (3000-4000)*1*300 = -430000
        # maintenance_margin ~ 3000*300*0.10 = 90000
        # equity (-430000) < maintenance (90000) → need_margin_call

        result = FutureMarginCalculator.check_margin_call(
            symbol='IF2401.CFFEX',
            position=1,
            entry_price=4000.0,
            current_price=3000.0,
            account_balance=tc.account.cash_available,
            query_date=ctx['current_dt'].date(),
        )

        assert result['need_margin_call'] is True
        assert result['shortfall'] > 0

    def test_force_liquidate_clears_position(self):
        """_force_liquidate_position should remove the position."""
        from finhack.trader.backtest.models.enums import PositionSide, Side, OrderStatus

        tc, ctx = self._make_engine_with_position(
            cash=50000, entry_price=4000, volume=1, current_price=3000,
        )

        # Create a mock engine with the trade_center
        engine = MagicMock()
        engine.trade_center = tc
        engine.context = ctx

        # Use the real _force_liquidate_position bound to the mock engine
        from finhack.trader.backtest.engine.backtest_engine import BacktestEngine
        _force_fn = BacktestEngine._force_liquidate_position.__get__(engine)

        pos_key = 'IF2401.CFFEX'
        position = tc.positions[pos_key]
        margin_result = {
            'need_margin_call': True,
            'shortfall': 500000,
            'maintenance_margin': 90000,
            'current_equity': -430000,
        }

        _force_fn(pos_key, position, 3000.0, ctx['current_dt'], margin_result)

        # Position should be removed
        assert pos_key not in tc.positions
