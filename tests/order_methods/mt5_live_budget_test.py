"""Offline tests for MT5Client's live budget/equity and fill-simulation switch.

Regression for a live incident: the local limit/stop fill simulator ran for
the live MT5 client too. get_orders() reconciliation kept re-adding the
still-pending broker order to _open_orders, so every OHLC fetch "filled" it
again locally and deducted its margin — the agent's budget fell from 100000
to -800656 and every new order was sized at 0.0 lot. Live mode now leaves
fills to the broker and derives free margin/equity from real MT5 positions.
"""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, module_path)

from finance_client.mt5.client import MT5Client


def _make_bare_client(user_name="AUDJPY_swing", budget=30000.0, back_test=False):
    client = object.__new__(MT5Client)
    client.user_name = user_name
    client._budget_cap = budget
    client.back_test = back_test
    return client


def _pos(comment, profit, symbol="AUDJPY", volume=0.01, price_open=110.3, type_=0):
    p = MagicMock()
    p.comment = comment
    p.profit = profit
    p.symbol = symbol
    p.volume = volume
    p.price_open = price_open
    p.type = type_
    return p


class TestLiveBudget(unittest.TestCase):

    def test_live_client_does_not_simulate_fills(self):
        self.assertFalse(_make_bare_client().simulates_order_fills)
        self.assertTrue(_make_bare_client(back_test=True).simulates_order_fills)

    @patch("finance_client.mt5.client.mt5")
    def test_free_margin_is_budget_minus_own_position_margin(self, mock_mt5):
        mock_mt5.POSITION_TYPE_BUY = 0
        mock_mt5.positions_get.return_value = [
            _pos("AUDJPY_swing", 232.0),
            _pos("", 5631.0, symbol="USDJPY"),          # manual position: ignored
            _pos("EURUSD_daily", 10.0, symbol="EURUSD"),  # other agent: ignored
        ]
        mock_mt5.order_calc_margin.return_value = 4412.0
        mock_mt5.account_info.return_value = MagicMock(margin_free=107000.0, equity=118000.0)

        client = _make_bare_client()
        self.assertAlmostEqual(client.get_free_margin(), 30000.0 - 4412.0)
        mock_mt5.order_calc_margin.assert_called_once()
        self.assertAlmostEqual(client.get_equity(), 30000.0 + 232.0)

    @patch("finance_client.mt5.client.mt5")
    def test_capped_by_actual_account(self, mock_mt5):
        mock_mt5.positions_get.return_value = []
        mock_mt5.account_info.return_value = MagicMock(margin_free=5000.0, equity=6000.0)

        client = _make_bare_client()
        self.assertAlmostEqual(client.get_free_margin(), 5000.0)
        self.assertAlmostEqual(client.get_equity(), 6000.0)


if __name__ == "__main__":
    unittest.main()
