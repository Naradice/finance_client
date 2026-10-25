"""Tests for ClientBase._get_quote_to_account_rate() — the currency-conversion
fix for risk sizing. Root cause: PercentEquityRisk/ATRRisk/FixedAmountRisk and
RiskManager._apply_account_caps() all divide an account-currency loss budget
by a quote-currency-denominated per-lot loss with no conversion. This is a
no-op (rate=1.0) whenever the traded symbol's quote currency already matches
the account's base currency (e.g. USDJPY/EURJPY on a JPY account, the case
this system was originally exercised with) but silently mis-sizes any trade
where they differ (e.g. EURUSD on a JPY account) — caught after a real
EURUSD order sized to 1.2 lots against a Y30,000 budget (should have been
~0.01).

Uses a minimal ClientBase subclass constructed via object.__new__ to bypass
the full constructor (storage/account files aren't needed to test this one
pure helper method).
"""
import unittest
from unittest.mock import MagicMock, patch

from finance_client.client_base import ClientBase
from finance_client.config.model import AccountRiskConfig


class _MinimalClient(ClientBase):
    """Implements just enough of ClientBase's abstract surface to construct
    an instance via object.__new__ without running the real __init__."""

    def get_additional_params(self):
        return {}

    def _get_ohlc_from_client(self, length, symbols, frame, columns, index, grouped_by_symbol):
        return None

    def get_current_ask(self, symbols=None):
        raise NotImplementedError  # overridden per-test via monkeypatch/mock

    def get_current_bid(self, symbols=None):
        raise NotImplementedError

    def __len__(self):
        return 0


def _make_client(base_currency="JPY"):
    client = object.__new__(_MinimalClient)
    client.account = MagicMock()
    client.account.risk_config = AccountRiskConfig(
        base_currency=base_currency,
        max_single_trade_percent=1.0,
        max_total_risk_percent=5.0,
        daily_max_loss_percent=2.0,
        allow_aggressive_mode=False,
        aggressive_multiplier=1.0,
        enforce_volume_reduction=True,
        atr_ratio_min_stop_loss=0.1,
    )
    return client


class TestGetQuoteToAccountRate(unittest.TestCase):

    def test_returns_1_when_quote_currency_matches_account_currency(self):
        client = _make_client(base_currency="JPY")
        client.get_current_ask = MagicMock(side_effect=AssertionError("should not need a rate lookup"))
        rate = client._get_quote_to_account_rate("USDJPY")
        self.assertEqual(rate, 1.0)

    def test_uses_direct_pair_when_available(self):
        client = _make_client(base_currency="JPY")
        # EURUSD's quote currency is USD; USDJPY is the direct conversion pair.
        client.get_current_ask = MagicMock(return_value=153.42)
        rate = client._get_quote_to_account_rate("EURUSD")
        self.assertAlmostEqual(rate, 153.42)
        client.get_current_ask.assert_called_once_with("USDJPY")

    def test_falls_back_to_inverse_pair_when_direct_unavailable(self):
        client = _make_client(base_currency="USD")
        # A hypothetical JPY-quoted pair on a USD account: quote=JPY, account=USD.
        # Direct pair "JPYUSD" doesn't exist on most brokers; USDJPY (inverse) does.
        def ask_side_effect(symbol):
            if symbol == "JPYUSD":
                raise Exception("symbol not found")
            if symbol == "USDJPY":
                return 153.42
            raise AssertionError(f"unexpected symbol {symbol}")

        client.get_current_ask = MagicMock(side_effect=ask_side_effect)
        rate = client._get_quote_to_account_rate("XXXJPY")
        self.assertAlmostEqual(rate, 1.0 / 153.42)

    def test_falls_back_to_1_and_logs_when_no_rate_available(self):
        client = _make_client(base_currency="JPY")
        client.get_current_ask = MagicMock(side_effect=Exception("symbol not found"))
        with self.assertLogs("finance_client.client_base", level="ERROR"):
            rate = client._get_quote_to_account_rate("EURUSD")
        self.assertEqual(rate, 1.0)

    def test_returns_1_when_account_currency_unknown(self):
        client = _make_client(base_currency="JPY")
        client.account = None
        client.get_current_ask = MagicMock(side_effect=AssertionError("should not be called"))
        rate = client._get_quote_to_account_rate("EURUSD")
        self.assertEqual(rate, 1.0)

    def test_returns_1_for_non_standard_length_symbol(self):
        client = _make_client(base_currency="JPY")
        client.get_current_ask = MagicMock(side_effect=AssertionError("should not be called"))
        rate = client._get_quote_to_account_rate("BTC")
        self.assertEqual(rate, 1.0)


if __name__ == "__main__":
    unittest.main()
