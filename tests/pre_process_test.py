import os
import sys
import unittest

import numpy
import pandas as pd

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
print(module_path)
sys.path.append(module_path)

from finance_client import fprocess

file_path = os.path.abspath("L:/data/mt5/OANDA-Japan MT5 Live/mt5_USDJPY_min30.csv")
ohlc_columns = ["open", "high", "low", "close"]
date_column = "time"


class TestPreProcess(unittest.TestCase):
    def test_min_max(self):
        import random

        open = [random.random() * 123 for index in range(100)]
        close = [o_value + random.random() - 0.5 for o_value in open]
        ds = pd.DataFrame({"close": close, "open": open})
        mm = fprocess.MinMaxPreProcess(scale=(-1, 1))
        mm.initialize(ds)
        result = mm.run(ds)
        self.assertTrue(len(result["close"]) == 100)
        self.assertTrue(result["close"].min() >= -1)
        self.assertTrue(result["close"].max() <= 1)
        self.assertTrue(result["open"].min() >= -1)
        self.assertTrue(result["open"].max() <= 1)
        # check if revert works
        reverted_data = mm.revert(result)
        for index in range(0, len(open)):
            self.assertAlmostEqual(reverted_data["open"][index], open[index])
            self.assertAlmostEqual(reverted_data["close"][index], close[index])

        # check series cases
        # series of a column
        reverted_row = mm.revert(result["close"].iloc[:10])
        for index in reverted_row.index:  # ds is answer/org values
            self.assertAlmostEqual(reverted_row.loc[index], ds["close"].iloc[index])

        # series of columns
        reverted_row = mm.revert(result.iloc[0])
        for column in ds.iloc[0].index:  # ds is answer/org values
            self.assertAlmostEqual(reverted_row[column], ds[column].iloc[0])

        # check if update works with updating min max value
        new_open_value = ds["open"].max() + random.random() + 0.1
        new_close_value = new_open_value + random.random() - 0.1

        new_data = pd.Series({"close": new_close_value, "open": new_open_value})
        new_data_standalized = mm.update(new_data)
        new_ds = fprocess.concat(result, new_data_standalized)
        self.assertTrue(new_data_standalized["open"] == 1)
        self.assertTrue(len(new_ds["close"]) == 101)
        self.assertTrue(new_ds["close"].min() >= -1)
        self.assertTrue(new_ds["close"].max() <= 1)
        self.assertTrue(new_ds["open"].min() >= -1)
        self.assertTrue(new_ds["open"].max() <= 1)

    def test_diff(self):
        process = fprocess.DiffPreProcess()
        ds = pd.DataFrame({"input": [10, 20, 1, 5, 30], "expect": [numpy.NaN, 10, -19, 4, 25]})
        diff_dict = process.run(ds)
        for index in range(1, len(ds)):
            self.assertEqual(diff_dict["input"].iloc[index], ds["expect"].iloc[index])

        new_data = pd.Series({"input": 10, "expect": -20})
        standalized_new_data = process.update(new_data)
        standalized_ds = fprocess.concat(ds, standalized_new_data)
        self.assertEqual(len(standalized_ds), 6)
        self.assertEqual(standalized_ds["input"].iloc[-1], new_data["expect"])

        ds = pd.DataFrame({"input": [10, 20, 1, 5, 30], "expect": [numpy.NaN, numpy.NaN, numpy.NaN, -5, 10]})
        process = fprocess.DiffPreProcess(periods=3)
        diff_dict = process.run(ds)
        for index in range(3, len(ds)):
            self.assertEqual(diff_dict["input"].iloc[index], ds["expect"].iloc[index])

        new_data = pd.Series({"input": 1, "expect": 0})
        standalized_new_data = process.update(new_data)
        standalized_ds = fprocess.concat(ds, standalized_new_data)
        self.assertEqual(len(standalized_ds), 6)
        self.assertEqual(standalized_ds["input"].iloc[-1], new_data["expect"])

    def test_clip(self):
        cp = fprocess.ClipPreProcess(lower=-1.0, upper=1.0)
        srs = pd.Series([0.1, -0.2, 1.1, 0.5, -3.0])
        exp_srs = pd.Series([0.1, -0.2, 1.0, 0.5, -1.0])

        processed_srs = cp.run(srs)
        self.assertEqual(len(processed_srs), 5)
        for index in range(1, len(processed_srs)):
            self.assertEqual(processed_srs.iloc[index], exp_srs.iloc[index])

        df = pd.DataFrame.from_dict({"a": [0.1, -0.2, 1.1, 0.5, -3.0], "b": [1.1, -1.2, 0.1, 1.5, 0.0]})
        exp_df = pd.DataFrame.from_dict({"a": [0.1, -0.2, 1.0, 0.5, -1.0], "b": [1.0, -1.0, 0.1, 1.0, 0.0]})
        processed_df = cp.run(df)

        self.assertEqual(len(processed_df), 5)
        for column in processed_df:
            for index, value in enumerate(processed_df[column]):
                self.assertEqual(value, exp_df[column].iloc[index])

        cp = fprocess.ClipPreProcess(lower=-1.0, upper=1.0, columns=["a"])

        df = pd.DataFrame.from_dict({"a": [0.1, -0.2, 1.1, 0.5, -3.0], "b": [1.1, -1.2, 0.1, 1.5, 0.0]})
        exp_df = pd.DataFrame.from_dict({"a": [0.1, -0.2, 1.0, 0.5, -1.0], "b": [1.1, -1.2, 0.1, 1.5, 0.0]})

        processed_df = cp.run(df)
        self.assertEqual(len(processed_df), 5)
        for column in processed_df:
            for index, value in enumerate(processed_df[column]):
                self.assertEqual(value, exp_df[column].iloc[index])


if __name__ == "__main__":
    unittest.main()
