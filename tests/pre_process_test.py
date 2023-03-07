import datetime
import json
import os
import sys
import unittest
from logging import config, getLogger

import numpy
import pandas as pd

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
print(module_path)
sys.path.append(module_path)

from finance_client import utils

try:
    with open(os.path.join(module_path, "finance_client/settings.json"), "r") as f:
        settings = json.load(f)
except Exception as e:
    print(f"fail to load settings file: {e}")
    raise e
logger_config = settings["log"]
log_file_base_name = logger_config["handlers"]["fileHandler"]["filename"]
log_path = f'./{log_file_base_name}_indicaters_{datetime.datetime.utcnow().strftime("%Y%m%d%H")}.log'
logger_config["handlers"]["fileHandler"]["filename"] = log_path
config.dictConfig(logger_config)
logger = getLogger("finance_client.test")

file_path = os.path.abspath("L:/data/mt5/OANDA-Japan MT5 Live/mt5_USDJPY_min30.csv")
ohlc_columns = ["open", "high", "low", "close"]
date_column = "time"


class TestPreProcess(unittest.TestCase):
    def test_min_max(self):
        import random

        open = [random.random() * 123 for index in range(100)]
        close = [o_value + random.random() - 0.5 for o_value in open]
        ds = pd.DataFrame({"close": close, "open": open})
        mm = utils.MinMaxPreProcess(scale=(-1, 1))
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
        new_ds = utils.concat(result, new_data_standalized)
        self.assertTrue(new_data_standalized["open"] == 1)
        self.assertTrue(len(new_ds["close"]) == 101)
        self.assertTrue(new_ds["close"].min() >= -1)
        self.assertTrue(new_ds["close"].max() <= 1)
        self.assertTrue(new_ds["open"].min() >= -1)
        self.assertTrue(new_ds["open"].max() <= 1)

    def test_min_max_with_columns(self):
        import random

        open = [random.random() * 123 for index in range(100)]
        high = [o_value + random.random() for o_value in open]
        low = [o_value - random.random() for o_value in open]
        close = [o_value + random.random() - 0.5 for o_value in open]
        time = [index for index in range(100)]
        ds = pd.DataFrame({"open": open, "high": high, "low": low, "close": close, "time": time})

        mm = utils.MinMaxPreProcess(scale=(-1, 1), columns=["open", "high", "low", "close"])
        mm.initialize(ds)
        result = mm.run(ds)
        check = "time" in result
        self.assertFalse(check)

    def test_diff(self):
        process = utils.DiffPreProcess()
        ds = pd.DataFrame({"input": [10, 20, 1, 5, 30], "expect": [numpy.NaN, 10, -19, 4, 25]})
        diff_dict = process.run(ds)
        for index in range(1, len(ds)):
            self.assertEqual(diff_dict["input"].iloc[index], ds["expect"].iloc[index])

        new_data = pd.Series({"input": 10, "expect": -20})
        standalized_new_data = process.update(new_data)
        standalized_ds = utils.concat(ds, standalized_new_data)
        self.assertEqual(len(standalized_ds), 6)
        self.assertEqual(standalized_ds["input"].iloc[-1], new_data["expect"])

        ds = pd.DataFrame({"input": [10, 20, 1, 5, 30], "expect": [numpy.NaN, numpy.NaN, numpy.NaN, -5, 10]})
        process = utils.DiffPreProcess(floor=3)
        diff_dict = process.run(ds)
        for index in range(3, len(ds)):
            self.assertEqual(diff_dict["input"].iloc[index], ds["expect"].iloc[index])

        new_data = pd.Series({"input": 1, "expect": 0})
        standalized_new_data = process.update(new_data)
        standalized_ds = utils.concat(ds, standalized_new_data)
        self.assertEqual(len(standalized_ds), 6)
        self.assertEqual(standalized_ds["input"].iloc[-1], new_data["expect"])
    
    def 


if __name__ == "__main__":
    unittest.main()
