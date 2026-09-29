"""Offline tests for MT5Client's trade-result classification.

Regression: SL/TP modifications (TRADE_ACTION_SLTP) create no order, so a
successful result has order == 0. The order == 0 check ran before the
retcode check and reported "order failed due to Request executed,
retcode=10009" — the EURJPY agent was told its breakeven SL was rejected
while the broker had actually applied it (the position later closed at it).
"""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, module_path)

from finance_client.mt5.client import MT5Client

DONE, PLACED, DONE_PARTIAL, INVALID_STOPS = 10009, 10008, 10010, 10016


def _client():
    return object.__new__(MT5Client)


def _mock_mt5(mock_mt5):
    mock_mt5.TRADE_RETCODE_DONE = DONE
    mock_mt5.TRADE_RETCODE_PLACED = PLACED
    mock_mt5.TRADE_RETCODE_DONE_PARTIAL = DONE_PARTIAL
    mock_mt5.TRADE_RETCODE_REQUOTE = 10004
    mock_mt5.TRADE_RETCODE_PRICE_CHANGED = 10020
    mock_mt5.TRADE_RETCODE_TOO_MANY_REQUESTS = 10024
    mock_mt5.TRADE_RETCODE_REJECT = 10006
    mock_mt5.TRADE_RETCODE_TIMEOUT = 10012
    mock_mt5.TRADE_RETCODE_CONNECTION = 10031
    mock_mt5.TRADE_RETCODE_NO_MONEY = 10019


class TestTradeResult(unittest.TestCase):

    @patch("finance_client.mt5.client.mt5")
    def test_sltp_success_with_order_zero_is_success(self, mock_mt5):
        _mock_mt5(mock_mt5)
        mock_mt5.order_send.return_value = MagicMock(order=0, retcode=DONE, comment="Request executed")
        suc, _ = _client()._MT5Client__request_order({"action": 6})
        self.assertTrue(suc)

    @patch("finance_client.mt5.client.mt5")
    def test_invalid_stops_is_failure(self, mock_mt5):
        _mock_mt5(mock_mt5)
        mock_mt5.order_send.return_value = MagicMock(order=0, retcode=INVALID_STOPS, comment="Invalid stops")
        suc, detail = _client()._MT5Client__request_order({"action": 6})
        self.assertFalse(suc)
        self.assertEqual(detail, "Invalid stops")

    @patch("finance_client.mt5.client.mt5")
    def test_pending_order_placed_is_success(self, mock_mt5):
        _mock_mt5(mock_mt5)
        mock_mt5.order_send.return_value = MagicMock(order=12345, retcode=PLACED, comment="Request placed")
        suc, _ = _client()._MT5Client__request_order({"action": 5})
        self.assertTrue(suc)


if __name__ == "__main__":
    unittest.main()
