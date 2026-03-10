import unittest

import pandas as pd

from finance_client.config.model import SymbolRiskConfig
from finance_client.fprocess.fprocess.idcprocess import ATRProcess
from finance_client.risk_manager.model import RiskContext, RiskResult
from finance_client.risk_manager.risk_manager import RiskManager
from finance_client.risk_manager.risk_options.atr import ATRRisk
from finance_client.risk_manager.risk_options.fixed_loss import FixedAmountRisk
from finance_client.risk_manager.risk_options.percent_equity import \
    PercentEquityRisk


def _make_symbol_config(min_volume=0.01, volume_step=0.01):
    return SymbolRiskConfig(
        min_volume=min_volume,
        volume_step=volume_step,
        risk_percent=1.0,
        contract_size=100000,
        leverage=25,
    )


def _make_context(is_buy=True, entry_price=150.0, stop_loss=None, take_profit=None, equity=1_000_000.0):
    return RiskContext(
        is_buy=is_buy,
        account_equity=equity,
        account_balance=equity,
        daily_realized_pnl=0.0,
        open_positions_loss_risk=0.0,
        symbol_risk_config=_make_symbol_config(),
        entry_price=entry_price,
        stop_loss=stop_loss,
        take_profit=take_profit,
        max_total_loss_risk=None,
        daily_max_loss=None,
    )


class _MockATRProcess(ATRProcess):
    """Minimal mock of ATRProcess for testing."""
    KEY_ATR = "atr"

    def __init__(self, atr_value: float):
        self._atr_value = atr_value

    def run(self, ohlc_df):
        import pandas as pd
        return pd.DataFrame({self.KEY_ATR: [self._atr_value] * len(ohlc_df)}, index=ohlc_df.index)


class TestATRRisk(unittest.TestCase):
    def test_long_sl_below_entry_manual(self):
        risk = ATRRisk(percent=1.0, atr_multiplier=2.0, rr_ratio=2.0, ohlc_columns=['Open', 'High', 'Low', 'Close'])
        ctx = _make_context(is_buy=True, entry_price=150.0)
        ohlc_df = pd.DataFrame({"Open": [150.0], "High": [151.0], "Low": [149.0], "Close": [150.5]})
        result = risk.calculate(ctx, ohlc_df=ohlc_df)

        self.assertIsInstance(result, RiskResult)
        self.assertLess(result.stop_loss_price, ctx.entry_price)
        self.assertGreater(result.take_profit_price, ctx.entry_price)
        self.assertGreater(result.volume, 0)

    def test_long_sl_below_entry_atr_process(self):
        risk = ATRRisk(percent=1.0, atr_multiplier=2.0, rr_ratio=2.0, atr_process=_MockATRProcess(0.5))
        ctx = _make_context(is_buy=True, entry_price=150.0)
        ohlc_df = pd.DataFrame({"Open": [150.0], "High": [151.0], "Low": [149.0], "Close": [150.5]})
        result = risk.calculate(ctx, ohlc_df=ohlc_df)

        self.assertIsInstance(result, RiskResult)
        self.assertLess(result.stop_loss_price, ctx.entry_price)
        self.assertGreater(result.take_profit_price, ctx.entry_price)

    def test_short_sl_above_entry(self):
        risk = ATRRisk(percent=1.0, atr_multiplier=2.0, rr_ratio=2.0, ohlc_columns=['Open', 'High', 'Low', 'Close'])
        ctx = _make_context(is_buy=False, entry_price=150.0)
        ohlc_df = pd.DataFrame({"Open": [150.0], "High": [151.0], "Low": [149.0], "Close": [150.5]})
        result = risk.calculate(ctx, ohlc_df=ohlc_df)

        self.assertIsInstance(result, RiskResult)
        self.assertGreater(result.stop_loss_price, ctx.entry_price)
        self.assertLess(result.take_profit_price, ctx.entry_price)

    def test_raises_without_atr_source(self):
        risk = ATRRisk(percent=1.0, atr_multiplier=2.0, rr_ratio=2.0)
        ctx = _make_context(is_buy=True, entry_price=150.0)
        with self.assertRaises(ValueError):
            risk.calculate(ctx)

    def test_atr_process_takes_priority(self):
        risk = ATRRisk(percent=1.0, atr_multiplier=1.0, rr_ratio=1.0, atr_process=_MockATRProcess(2.0))
        ctx = _make_context(is_buy=True, entry_price=100.0)
        ohlc_df = pd.DataFrame({"Open": [100.0], "High": [101.0], "Low": [99.0], "Close": [100.5]})
        result = risk.calculate(ctx, ohlc_df=ohlc_df)
        # SL distance should be 2.0 * 1.0 = 2.0, not 999.0
        self.assertAlmostEqual(abs(ctx.entry_price - result.stop_loss_price), 2.0)

    def test_ohlc_df_with_atr_process(self):
        """When ohlc_df is provided, atr_process.run(ohlc_df) is used instead of last_data."""
        ohlc_df = pd.DataFrame({"Open": [100.0], "High": [101.0], "Low": [99.0], "Close": [100.5]})
        process = _MockATRProcess(atr_value=3.0)

        risk = ATRRisk(percent=1.0, atr_multiplier=1.0, rr_ratio=1.0, atr_process=process)
        ctx = _make_context(is_buy=True, entry_price=100.0)
        result = risk.calculate(ctx, ohlc_df=ohlc_df)

        # SL distance should be 3.0 * 1.0 = 3.0, not 999.0
        self.assertAlmostEqual(abs(ctx.entry_price - result.stop_loss_price), 3.0)

    def test_ohlc_df_ignored_without_atr_process(self):
        """When ohlc_df is provided but no atr_process, atr_value is still used."""
        ohlc_df = pd.DataFrame({"Open": [100.0], "High": [101.0], "Low": [99.0], "Close": [100.5]})
        risk = ATRRisk(percent=1.0, atr_multiplier=1.0, rr_ratio=1.0, ohlc_columns=['Open', 'High', 'Low', 'Close'])
        ctx = _make_context(is_buy=True, entry_price=100.0)
        result = risk.calculate(ctx, ohlc_df=ohlc_df)

        self.assertAlmostEqual(abs(ctx.entry_price - result.stop_loss_price), 2.0)

    def test_rr_ratio(self):
        risk = ATRRisk(percent=1.0, atr_multiplier=2.0, rr_ratio=3.0, ohlc_columns=['Open', 'High', 'Low', 'Close'])
        ohlc_df = pd.DataFrame({"Open": [100.0], "High": [101.0], "Low": [99.0], "Close": [100.5]})
        ctx = _make_context(is_buy=True, entry_price=100.0)
        result = risk.calculate(ctx, ohlc_df=ohlc_df)

        self.assertAlmostEqual(result.risk_reward_ratio, 3.0)
        self.assertAlmostEqual(result.reward_volume, result.risk_volume * 3.0, places=5)


class TestPercentEquityRisk(unittest.TestCase):
    def test_calculate_with_sl(self):
        risk = PercentEquityRisk(percent=1.0)
        ctx = _make_context(is_buy=True, entry_price=150.0, stop_loss=149.0)
        result = risk.calculate(ctx)

        self.assertIsInstance(result, RiskResult)
        self.assertGreater(result.volume, 0)
        self.assertEqual(result.stop_loss_price, 149.0)

    def test_raises_without_sl(self):
        risk = PercentEquityRisk(percent=1.0)
        ctx = _make_context(is_buy=True, entry_price=150.0, stop_loss=None)
        with self.assertRaises(ValueError):
            risk.calculate(ctx)

    def test_volume_proportional_to_equity(self):
        risk = PercentEquityRisk(percent=1.0)
        ctx_small = _make_context(entry_price=150.0, stop_loss=149.0, equity=500_000.0)
        ctx_large = _make_context(entry_price=150.0, stop_loss=149.0, equity=1_000_000.0)
        result_small = risk.calculate(ctx_small)
        result_large = risk.calculate(ctx_large)
        self.assertGreater(result_large.volume, result_small.volume)


class TestFixedAmountRisk(unittest.TestCase):
    def test_calculate_with_sl(self):
        risk = FixedAmountRisk(allowed_loss_volume=10000.0)
        ctx = _make_context(is_buy=True, entry_price=150.0, stop_loss=149.0)
        result = risk.calculate(ctx)

        self.assertIsInstance(result, RiskResult)
        self.assertGreater(result.volume, 0)
        self.assertEqual(result.stop_loss_price, 149.0)

    def test_raises_without_sl(self):
        risk = FixedAmountRisk(allowed_loss_volume=10000.0)
        ctx = _make_context(is_buy=True, entry_price=150.0, stop_loss=None)
        with self.assertRaises(ValueError):
            risk.calculate(ctx)

    def test_larger_sl_gives_smaller_volume(self):
        risk = FixedAmountRisk(allowed_loss_volume=10000.0)
        ctx_tight = _make_context(entry_price=150.0, stop_loss=149.5)   # 0.5 distance
        ctx_wide = _make_context(entry_price=150.0, stop_loss=148.0)    # 2.0 distance
        result_tight = risk.calculate(ctx_tight)
        result_wide = risk.calculate(ctx_wide)
        self.assertGreater(result_tight.volume, result_wide.volume)


class TestApplyAccountCaps(unittest.TestCase):
    """Tests for RiskManager._apply_account_caps — the account-level volume constraints."""

    def _make_risk_manager(self):
        symbol_config = _make_symbol_config()
        # account_manager is not used by _apply_account_caps itself
        return RiskManager(account_manager=None, symbol_risk_config=symbol_config)

    def _make_context(
        self,
        daily_realized_pnl=0.0,
        open_positions_loss_risk=0.0,
        max_total_loss_risk=None,
        daily_max_loss=None,
        entry_price=150.0,
        stop_loss=149.0,
    ):
        return RiskContext(
            is_buy=True,
            account_equity=1_000_000.0,
            account_balance=1_000_000.0,
            daily_realized_pnl=daily_realized_pnl,
            open_positions_loss_risk=open_positions_loss_risk,
            symbol_risk_config=_make_symbol_config(),
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=None,
            max_total_loss_risk=max_total_loss_risk,
            daily_max_loss=daily_max_loss,
        )

    # ── daily loss cap ────────────────────────────────────────────────────────

    def test_daily_loss_cap_reduces_volume_after_losing_day(self):
        """After a losing day, volume should be reduced to stay within the daily limit."""
        rm = self._make_risk_manager()
        # daily limit = 10000, already lost 8000 (pnl = -8000), remaining = 2000
        # stop_distance = 1.0, contract_size = 100000 → max_volume = 2000 / 100000 = 0.02
        ctx = self._make_context(daily_realized_pnl=-8000.0, daily_max_loss=10000.0)
        volume = rm._apply_account_caps(1.0, stop_distance=1.0, context=ctx)
        self.assertAlmostEqual(volume, 0.02, places=5)

    def test_daily_loss_cap_zero_when_limit_exhausted(self):
        """When the daily loss limit is fully consumed, volume should be reduced to zero."""
        rm = self._make_risk_manager()
        ctx = self._make_context(daily_realized_pnl=-10000.0, daily_max_loss=10000.0)
        volume = rm._apply_account_caps(1.0, stop_distance=1.0, context=ctx)
        self.assertEqual(volume, 0.0)

    def test_daily_loss_cap_not_affected_by_profit(self):
        """A profitable day should NOT expand the daily loss budget beyond the full limit."""
        rm = self._make_risk_manager()
        # daily limit = 10000, today profitable (+5000) → daily_loss = 0, remaining = 10000
        # max_volume = 10000 / 100000 = 0.1 → does not cap a 0.05 lot request
        ctx = self._make_context(daily_realized_pnl=5000.0, daily_max_loss=10000.0)
        volume = rm._apply_account_caps(0.05, stop_distance=1.0, context=ctx)
        self.assertAlmostEqual(volume, 0.05, places=5)

    def test_daily_loss_cap_full_budget_when_no_loss(self):
        """With no losses today, the full daily budget is available."""
        rm = self._make_risk_manager()
        ctx = self._make_context(daily_realized_pnl=0.0, daily_max_loss=10000.0)
        # max_volume = 10000 / 100000 = 0.1
        volume = rm._apply_account_caps(0.1, stop_distance=1.0, context=ctx)
        self.assertAlmostEqual(volume, 0.1, places=5)

    # ── total risk cap ────────────────────────────────────────────────────────

    def test_total_risk_cap_converts_monetary_budget_to_lots(self):
        """Remaining total risk budget (monetary) must be divided by stop×contract_size to get max lots."""
        rm = self._make_risk_manager()
        # max_total_loss_risk = 5000 (already-computed remaining budget from account manager)
        # stop_distance = 1.0, contract_size = 100000 → max_volume = 5000 / 100000 = 0.05
        ctx = self._make_context(max_total_loss_risk=5000.0)
        volume = rm._apply_account_caps(1.0, stop_distance=1.0, context=ctx)
        self.assertAlmostEqual(volume, 0.05, places=5)

    def test_total_risk_cap_no_double_subtraction(self):
        """open_positions_loss_risk in context must NOT be subtracted again inside _apply_account_caps.

        get_max_total_loss_risk() on account.Manager already returns the remaining budget
        with open position risk deducted. Subtracting it again here would over-restrict volume.
        """
        rm = self._make_risk_manager()
        # Simulate: total budget = 20000, open_risk = 15000 → remaining passed in = 5000
        # If double-subtraction were present: 5000 - 15000 = 0 (wrong)
        # Correct result: max_volume = 5000 / 100000 = 0.05
        ctx = self._make_context(open_positions_loss_risk=15000.0, max_total_loss_risk=5000.0)
        volume = rm._apply_account_caps(1.0, stop_distance=1.0, context=ctx)
        self.assertAlmostEqual(volume, 0.05, places=5)

    def test_total_risk_cap_zero_when_budget_exhausted(self):
        """When no remaining total risk budget, volume should be reduced to zero."""
        rm = self._make_risk_manager()
        ctx = self._make_context(max_total_loss_risk=0.0)
        volume = rm._apply_account_caps(1.0, stop_distance=1.0, context=ctx)
        self.assertEqual(volume, 0.0)

    # ── min volume floor ──────────────────────────────────────────────────────

    def test_min_volume_floor_applied_when_uncapped(self):
        """Volume below min_volume is raised to min_volume when no risk cap overrides it."""
        rm = self._make_risk_manager()
        # No caps active; input volume 0.001 < min_volume 0.01
        ctx = self._make_context()
        volume = rm._apply_account_caps(0.001, stop_distance=1.0, context=ctx)
        self.assertAlmostEqual(volume, 0.01, places=5)

    def test_daily_limit_exhausted_overrides_min_volume(self):
        """When the daily loss limit is fully consumed, volume reaches 0 even if below min_volume.

        Risk limits take priority: if there is no budget left, no trade should be placed.
        """
        rm = self._make_risk_manager()
        ctx = self._make_context(daily_realized_pnl=-10000.0, daily_max_loss=10000.0)
        volume = rm._apply_account_caps(0.05, stop_distance=1.0, context=ctx)
        self.assertEqual(volume, 0.0)

    # ── no caps ───────────────────────────────────────────────────────────────

    def test_no_caps_returns_original_volume(self):
        """When no cap parameters are set, the input volume is returned unchanged."""
        rm = self._make_risk_manager()
        ctx = self._make_context()
        volume = rm._apply_account_caps(0.5, stop_distance=1.0, context=ctx)
        self.assertAlmostEqual(volume, 0.5, places=5)


if __name__ == "__main__":
    unittest.main()
