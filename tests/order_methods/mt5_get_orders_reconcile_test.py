"""Offline test for MT5Client.get_orders() reconciling broker-side pending
orders that aren't in the in-memory _open_orders cache (e.g. because they
were placed by a previous process instance — _open_orders is never persisted
across restarts). No real MT5 connection is made; mt5.orders_get() etc. are
mocked at the module level.
"""
import datetime
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, module_path)

from finance_client.mt5.client import MT5Client


def _make_bare_client(user_name="daily_trader", ignore_order=False):
    """Construct an MT5Client instance without running __init__ (which needs
    a real MT5 connection), setting only what get_orders() reads."""
    client = object.__new__(MT5Client)
    client.user_name = user_name
    client._open_orders = {}
    client.leverage = 25
    client._MT5Client__ignore_order = ignore_order
    return client


def _mock_mt5_order(ticket, order_type, price_open, volume_current, tp, sl, comment, time_setup, magic=0):
    o = MagicMock()
    o.ticket = ticket
    o.type = order_type
    o.price_open = price_open
    o.volume_current = volume_current
    o.tp = tp
    o.sl = sl
    o.comment = comment
    o.time_setup = time_setup
    o.magic = magic
    o.symbol = "USDJPY"
    return o


class TestGetOrdersReconciliation(unittest.TestCase):

    @patch("finance_client.mt5.client.mt5")
    def test_reconciles_order_not_in_memory_cache(self, mock_mt5):
        mock_mt5.ORDER_TYPE_BUY_LIMIT = 2
        mock_mt5.ORDER_TYPE_SELL_LIMIT = 3
        mock_mt5.ORDER_TYPE_BUY_STOP = 4
        mock_mt5.ORDER_TYPE_SELL_STOP = 5
        now_ts = datetime.datetime.now(tz=datetime.timezone.utc).timestamp()
        mock_mt5.orders_get.return_value = [
            _mock_mt5_order(42715977, 3, 159.5, 0.02, 158.8, 159.85, "daily_trader", now_ts),
        ]
        mock_mt5.symbol_info.return_value = MagicMock(trade_contract_size=100000)

        client = _make_bare_client()
        orders = client.get_orders()

        self.assertEqual(len(orders), 1)
        order = orders[0]
        self.assertEqual(order.id, "42715977")
        self.assertEqual(order.price, 159.5)
        self.assertEqual(order.volume, 0.02)
        self.assertEqual(order.tp, 158.8)
        self.assertEqual(order.sl, 159.85)
        # Reconciled order gets cached so a subsequent call doesn't re-reconcile it.
        self.assertIn("42715977", client._open_orders)

    @patch("finance_client.mt5.client.mt5")
    def test_filters_out_orders_belonging_to_a_different_user(self, mock_mt5):
        mock_mt5.ORDER_TYPE_BUY_LIMIT = 2
        mock_mt5.ORDER_TYPE_SELL_LIMIT = 3
        mock_mt5.ORDER_TYPE_BUY_STOP = 4
        mock_mt5.ORDER_TYPE_SELL_STOP = 5
        now_ts = datetime.datetime.now(tz=datetime.timezone.utc).timestamp()
        mock_mt5.orders_get.return_value = [
            _mock_mt5_order(1, 3, 159.5, 0.02, 0.0, 0.0, "", now_ts),
            _mock_mt5_order(2, 2, 158.2, 0.03, 0.0, 0.0, "some_other_agent", now_ts),
        ]
        mock_mt5.symbol_info.return_value = MagicMock(trade_contract_size=100000)

        client = _make_bare_client(user_name="daily_trader")
        orders = client.get_orders()

        self.assertEqual(orders, [])

    @patch("finance_client.mt5.client.mt5")
    def test_already_cached_order_is_returned_without_rebuilding(self, mock_mt5):
        mock_mt5.ORDER_TYPE_BUY_LIMIT = 2
        mock_mt5.ORDER_TYPE_SELL_LIMIT = 3
        mock_mt5.ORDER_TYPE_BUY_STOP = 4
        mock_mt5.ORDER_TYPE_SELL_STOP = 5
        now_ts = datetime.datetime.now(tz=datetime.timezone.utc).timestamp()
        mock_mt5.orders_get.return_value = [
            _mock_mt5_order(42715977, 3, 159.5, 0.02, 158.8, 159.85, "daily_trader", now_ts),
        ]

        client = _make_bare_client()
        cached_sentinel = MagicMock()
        client._open_orders["42715977"] = cached_sentinel

        orders = client.get_orders()

        self.assertEqual(orders, [cached_sentinel])
        mock_mt5.symbol_info.assert_not_called()

    @patch("finance_client.mt5.client.mt5")
    def test_none_from_broker_falls_back_to_cached_orders(self, mock_mt5):
        mock_mt5.orders_get.return_value = None
        mock_mt5.last_error.return_value = "connection lost"

        client = _make_bare_client()
        cached_sentinel = MagicMock()
        client._open_orders["1"] = cached_sentinel

        with patch("finance_client.client_base.ClientBase.get_orders", return_value=[cached_sentinel]) as mock_super:
            orders = client.get_orders()

        mock_super.assert_called_once()
        self.assertEqual(orders, [cached_sentinel])

    @patch("finance_client.mt5.client.mt5")
    def test_ignore_order_mode_delegates_to_base_class(self, mock_mt5):
        client = _make_bare_client(ignore_order=True)
        with patch("finance_client.client_base.ClientBase.get_orders", return_value=["base"]) as mock_super:
            orders = client.get_orders()
        mock_super.assert_called_once()
        self.assertEqual(orders, ["base"])
        mock_mt5.orders_get.assert_not_called()


if __name__ == "__main__":
    unittest.main()
