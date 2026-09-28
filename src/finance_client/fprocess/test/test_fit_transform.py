"""STDPreProcess/MinMaxPreProcess fit(mask)/transform and validation gap helpers.

Self-contained (synthetic data) -- unlike the other modules here, it does not need sample_data.csv.
"""
import os
import sys
import unittest

import numpy as np
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fprocess import preprocess, validation  # noqa: E402


def _two_runs(n=200, jump=50.0):
    rng = np.random.default_rng(0)
    idx = pd.date_range("2024-01-01", periods=n, freq="1min").append(
        pd.date_range("2024-02-01", periods=n, freq="1min"))
    close = 100 + np.cumsum(rng.normal(0, 0.1, 2 * n))
    close[n:] += jump
    return pd.DataFrame({"close": close, "other": np.arange(2 * n, dtype=float)}, index=idx)


class TestSTDFitTransform(unittest.TestCase):
    def test_run_is_unchanged_and_still_refits(self):
        df = _two_runs()
        p = preprocess.STDPreProcess(columns=["close"])
        a = p.run(df)
        b = p.run(df.iloc[:100])
        self.assertAlmostEqual(a["close"].mean(), 0.0, places=6)
        self.assertAlmostEqual(b["close"].mean(), 0.0, places=6)   # refit on the new data
        self.assertFalse(p.fitted)
        self.assertNotIn("mean_values", p.option)

    def test_fit_on_mask_then_transform_without_refitting(self):
        df = _two_runs()
        mask = np.zeros(len(df), dtype=bool)
        mask[:200] = True                                            # first run only
        p = preprocess.STDPreProcess(columns=["close"]).fit(df, mask=mask)
        out = p.transform(df)
        self.assertAlmostEqual(out["close"].iloc[:200].mean(), 0.0, places=6)
        self.assertAlmostEqual(out["close"].iloc[:200].std(), 1.0, places=6)
        self.assertGreater(out["close"].iloc[200:].mean(), 10)       # second run judged on first run's scale
        pd.testing.assert_series_equal(out["other"], df["other"])    # untouched columns kept
        again = p.transform(df.iloc[250:260])
        pd.testing.assert_series_equal(again["close"], out["close"].iloc[250:260])

    def test_revert_round_trip(self):
        df = _two_runs()
        p = preprocess.STDPreProcess(columns=["close"]).fit(df)
        back = p.revert(p.transform(df))
        np.testing.assert_allclose(back["close"].values, df["close"].values)

    def test_fitted_stats_survive_save_and_load(self):
        df = _two_runs()
        p = preprocess.STDPreProcess(columns=["close"]).fit(df.iloc[:150])
        restored = preprocess.STDPreProcess.load("std", p.option)
        self.assertTrue(restored.fitted)
        pd.testing.assert_frame_equal(restored.transform(df), p.transform(df))

    def test_transform_before_fit_raises(self):
        with self.assertRaises(RuntimeError):
            preprocess.STDPreProcess(columns=["close"]).transform(_two_runs())

    def test_partial_restore_is_rejected(self):
        with self.assertRaises(ValueError):
            preprocess.STDPreProcess(columns=["close"], mean_values={"close": 1.0})


class TestMinMaxFitMask(unittest.TestCase):
    def test_fit_on_mask_sets_min_max_used_by_run(self):
        df = _two_runs()
        mask = np.zeros(len(df), dtype=bool)
        mask[:200] = True
        p = preprocess.MinMaxPreProcess(columns=["close"], scale=(0, 1)).fit(df, mask=mask)
        out = p.run(df)
        self.assertAlmostEqual(out["close"].iloc[:200].min(), 0.0, places=6)
        self.assertAlmostEqual(out["close"].iloc[:200].max(), 1.0, places=6)
        self.assertGreater(out["close"].iloc[200:].min(), 1.0)       # second run above the first run's range


class TestGapHelpers(unittest.TestCase):
    def test_gap_mask_and_segments(self):
        df = _two_runs(n=5)
        mask = validation.gap_mask(df.index)
        self.assertEqual(mask.tolist(), [False] * 5 + [True] + [False] * 4)
        self.assertEqual(validation.contiguous_segment_ids(df.index).tolist(), [0] * 5 + [1] * 5)

    def test_explicit_expected_delta(self):
        idx = pd.DatetimeIndex(["2024-01-01 00:00", "2024-01-01 00:05", "2024-01-01 00:06"])
        self.assertEqual(validation.gap_mask(idx, pd.Timedelta(minutes=1)).tolist(), [False, True, False])

    def test_short_index(self):
        self.assertEqual(validation.gap_mask(pd.DatetimeIndex(["2024-01-01"])).tolist(), [False])


if __name__ == "__main__":
    unittest.main()
