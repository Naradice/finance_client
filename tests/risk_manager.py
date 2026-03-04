import unittest

from finance_client.config.model import SymbolRiskConfig
from finance_client.risk_manager.model import RiskContext, RiskResult
from finance_client.risk_manager.risk_options.atr import ATRRisk
from finance_client.risk_manager.risk_options.fixed_loss import FixedAmountRisk
from finance_client.risk_manager.risk_options.percent_equity import PercentEquityRisk


def _make_symbol_config(pip_value_per_lot=10.0, min_volume=0.01, volume_step=0.01):
    return SymbolRiskConfig(
        min_volume=min_volume,
        volume_step=volume_step,
        risk_percent=1.0,
        contract_size=100000,
        leverage=25,
        pip_value_per_lot=pip_value_per_lot,
    )


def _make_context(is_buy=True, entry_price=150.0, stop_loss=None, take_profit=None, equity=1_000_000.0):
    return RiskContext(
        is_buy=is_buy,
        account_equity=equity,
        account_balance=equity,
        daily_realized_pnl=0.0,
        open_positions_risk_volume=0.0,
        symbol_risk_config=_make_symbol_config(),
        entry_price=entry_price,
        stop_loss=stop_loss,
        take_profit=take_profit,
    )


class _MockATRProcess:
    """Minimal mock of ATRProcess for testing."""
    KEY_ATR = "atr"

    def __init__(self, atr_value: float):
        import pandas as pd
        self.last_data = pd.DataFrame({self.KEY_ATR: [atr_value]})


class TestATRRisk(unittest.TestCase):
    def test_long_sl_below_entry_manual(self):
        risk = ATRRisk(percent=1.0, atr_multiplier=2.0, rr_ratio=2.0)
        risk.atr_value = 0.5
        ctx = _make_context(is_buy=True, entry_price=150.0)
        result = risk.calculate(ctx)

        self.assertIsInstance(result, RiskResult)
        self.assertLess(result.stop_loss_price, ctx.entry_price)
        self.assertGreater(result.take_profit_price, ctx.entry_price)
        self.assertGreater(result.volume, 0)

    def test_long_sl_below_entry_atr_process(self):
        risk = ATRRisk(percent=1.0, atr_multiplier=2.0, rr_ratio=2.0, atr_process=_MockATRProcess(0.5))
        ctx = _make_context(is_buy=True, entry_price=150.0)
        result = risk.calculate(ctx)

        self.assertIsInstance(result, RiskResult)
        self.assertLess(result.stop_loss_price, ctx.entry_price)
        self.assertGreater(result.take_profit_price, ctx.entry_price)

    def test_short_sl_above_entry(self):
        risk = ATRRisk(percent=1.0, atr_multiplier=2.0, rr_ratio=2.0)
        risk.atr_value = 0.5
        ctx = _make_context(is_buy=False, entry_price=150.0)
        result = risk.calculate(ctx)

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
        risk.atr_value = 999.0  # should be ignored
        ctx = _make_context(is_buy=True, entry_price=100.0)
        result = risk.calculate(ctx)
        # SL distance should be 2.0 * 1.0 = 2.0, not 999.0
        self.assertAlmostEqual(abs(ctx.entry_price - result.stop_loss_price), 2.0)

    def test_rr_ratio(self):
        risk = ATRRisk(percent=1.0, atr_multiplier=2.0, rr_ratio=3.0)
        risk.atr_value = 1.0
        ctx = _make_context(is_buy=True, entry_price=100.0)
        result = risk.calculate(ctx)

        self.assertAlmostEqual(result.risk_reward_ratio, 3.0)
        self.assertAlmostEqual(result.reward_volume, result.risk_volume * 3.0, places=5)

    def test_larger_atr_gives_smaller_volume(self):
        # Larger ATR → wider SL → smaller volume (risk is %-based so it stays ~constant)
        risk = ATRRisk(percent=1.0, atr_multiplier=1.0, rr_ratio=1.0)
        ctx = _make_context(is_buy=True, entry_price=100.0)

        risk.atr_value = 1.0
        result1 = risk.calculate(ctx)
        risk.atr_value = 2.0
        result2 = risk.calculate(ctx)

        self.assertLess(result2.volume, result1.volume)


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


if __name__ == "__main__":
    unittest.main()
