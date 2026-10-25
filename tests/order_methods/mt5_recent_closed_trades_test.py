"""Offline tests for MT5Client.get_recent_closed_trades().

Exits closed by TP/SL carry the broker's comment ("[sl 109.100]"), so
ownership must be matched per position (via the entry deal's comment), and
deal times are broker-server time that must be shifted back to UTC.
"""
import datetime
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, module_path)

from finance_client.mt5.client import MT5Client


def _deal(position_id, entry, type_, price, profit, comment, ts, symbol="AUDJPY"):
    d = MagicMock()
    d.position_id = position_id
    d.entry = entry
    d.type = type_
    d.price = price
    d.profit = profit
    d.comment = comment
    d.time = ts
    d.symbol = symbol
    return d


class TestRecentClosedTrades(unittest.TestCase):

    @patch("finance_client.mt5.client.mt5")
    def test_includes_own_sl_exit_and_converts_server_time(self, mock_mt5):
        mock_mt5.DEAL_TYPE_SELL = 1
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        server = now + datetime.timedelta(hours=3)
        mock_mt5.symbol_info_tick.return_value = MagicMock(time=server.timestamp())
        ts = int((server - datetime.timedelta(hours=1)).timestamp())
        mock_mt5.history_deals_get.return_value = [
            _deal(1, 0, 0, 110.548, 0, "AUDJPY_swing", ts - 3600),       # our long entry
            _deal(1, 1, 1, 109.689, -859, "[sl 109.690]", ts),           # its SL exit
            _deal(2, 0, 1, 157.0, 0, "", ts - 3600, symbol="USDJPY"),    # manual position
            _deal(2, 1, 0, 156.0, 500, "[tp 156.000]", ts, symbol="USDJPY"),
        ]
        client = object.__new__(MT5Client)
        client.user_name = "AUDJPY_swing"
        client.back_test = False

        trades = client.get_recent_closed_trades(hours=24)

        self.assertEqual(len(trades), 1)
        t = trades[0]
        self.assertEqual((t["side"], t["entry_price"], t["exit_price"], t["closed_by"]), ("LONG", 110.548, 109.689, "sl"))
        closed_at = datetime.datetime.fromisoformat(t["closed_at"])
        self.assertLess(abs((closed_at - (now - datetime.timedelta(hours=1))).total_seconds()), 120)

    @patch("finance_client.mt5.client.mt5")
    def test_excludes_trades_outside_window(self, mock_mt5):
        mock_mt5.DEAL_TYPE_SELL = 1
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        mock_mt5.symbol_info_tick.return_value = MagicMock(time=now.timestamp())
        old = int((now - datetime.timedelta(hours=30)).timestamp())
        mock_mt5.history_deals_get.return_value = [
            _deal(1, 0, 0, 110.5, 0, "AUDJPY_swing", old - 60),
            _deal(1, 1, 1, 110.0, -500, "[sl 110.000]", old),
        ]
        client = object.__new__(MT5Client)
        client.user_name = "AUDJPY_swing"
        client.back_test = False
        self.assertEqual(client.get_recent_closed_trades(hours=24), [])


if __name__ == "__main__":
    unittest.main()
