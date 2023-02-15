import datetime
import json
import os
import sys
import unittest

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
print(module_path)
sys.path.append(module_path)

import datetime
from logging import config, getLogger

import numpy
import pandas as pd

import finance_client.frames as Frame
from finance_client import utils
from finance_client.csv.client import CSVClient
from finance_client.mt5.client import MT5Client

try:
    with open(os.path.join(module_path, "finance_client/settings.json"), "r") as f:
        settings = json.load(f)
except Exception as e:
    print(f"fail to load settings file: {e}")
    raise e
logger_config = settings["log"]
log_file_base_name = logger_config["handlers"]["fileHandler"]["filename"]
log_path = f'./{log_file_base_name}_indicaters_{datetime.datetime.utcnow().strftime("%Y%m%d%H")}.logs'
logger_config["handlers"]["fileHandler"]["filename"] = log_path
config.dictConfig(logger_config)
logger = getLogger("finance_client.test")

file_path = os.path.abspath("L:/data/mt5/OANDA-Japan MT5 Live/mt5_USDJPY_min30.csv")
ohlc_columns = ["open", "high", "low", "close"]
date_column = "time"


class TestIndicaters(unittest.TestCase):
    client = CSVClient(files=file_path, columns=ohlc_columns, date_column=date_column, logger=logger, auto_step_index=False, start_index=200)

    def test_cci_process(self):
        ohlc = self.client.get_ohlc(110)
        cci_prc = utils.CCIProcess(14, ohlc_column=ohlc_columns)
        input_data_1 = ohlc.iloc[:100].copy()
        self.assertEqual(len(input_data_1), 100)
        cci = cci_prc.run(input_data_1)

        next_tick = ohlc.iloc[100]
        next_cci = cci_prc.update(next_tick)
        next_cci_from_run = cci_prc.run(ohlc.iloc[:101])
        cci_column = cci_prc.columns["CCI"]
        ans_value = next_cci_from_run[cci_column].iloc[-1]
        self.assertEqual(next_cci, ans_value)

    def test_macd(self):
        # input
        ds = pd.DataFrame({ohlc_columns[3]: [120.000 + i * 0.1 for i in range(30)]})
        # ans
        short_ema = ds[ohlc_columns[3]].ewm(span=12, adjust=False).mean()
        long_ema = ds[ohlc_columns[3]].ewm(span=26, adjust=False).mean()
        macd = short_ema - long_ema
        signal = macd.rolling(9).mean()

        process = utils.MACDProcess(target_column=ohlc_columns[3])
        macd_dict = process.run(ds)
        short_column = process.columns["S_EMA"]
        long_column = process.columns["L_EMA"]
        macd_column = process.columns["MACD"]
        signal_column = process.columns["Signal"]

        self.assertEqual(macd_dict[short_column].iloc[-1], short_ema.iloc[-1])
        self.assertEqual(macd_dict[long_column].iloc[-1], long_ema.iloc[-1])
        self.assertEqual(macd_dict[macd_column].iloc[-1], macd.iloc[-1])
        self.assertEqual(macd_dict[signal_column].iloc[-1], signal.iloc[-1])

    def test_macd_update(self):
        ## prerequisites
        ds = pd.DataFrame({ohlc_columns[3]: [120.000 + i * 0.1 for i in range(30)]})
        process = utils.MACDProcess(target_column=ohlc_columns[3])
        macd_dict = process.run(ds)

        ##input
        new_value = 120.000 + 30 * 0.1
        new_data = pd.Series({ohlc_columns[3]: new_value})

        ##output
        new_indicater_series = process.update(new_data)
        test_data = process.concat(ds, new_indicater_series)

        ##expect
        ex_data_org = process.concat(ds, new_data)
        another_ps = utils.MACDProcess(target_column=ohlc_columns[3])
        macd_dict = another_ps.run(ex_data_org)

        short_column = process.columns["S_EMA"]
        long_column = process.columns["L_EMA"]
        macd_column = process.columns["MACD"]
        signal_column = process.columns["Signal"]

        self.assertEqual(macd_dict[short_column].iloc[-1], test_data[short_column].iloc[-1])
        self.assertEqual(macd_dict[long_column].iloc[-1], test_data[long_column].iloc[-1])
        self.assertEqual(macd_dict[macd_column].iloc[-1], test_data[macd_column].iloc[-1])
        self.assertEqual(macd_dict[signal_column].iloc[-1], test_data[signal_column].iloc[-1])

    def test_ema_process(self):
        window = 4
        input = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 20]
        out_ex = [input[0]]
        alpha = 2 / (1 + window)

        for i in range(1, len(input)):
            out_ex.append(out_ex[i - 1] * (1 - alpha) + input[i] * alpha)

    def test_renko_process(self):
        client = CSVClient(files=file_path, columns=ohlc_columns, date_column=date_column, logger=logger, start_index=200)
        prc = utils.RenkoProcess(window=120, ohlc_column=ohlc_columns)
        column = prc.columns["NUM"]
        start_time = datetime.datetime.now()
        data = client.get_ohlc(300, idc_processes=[prc])
        end_time = datetime.datetime.now()
        logger.debug(f"renko process took {end_time - start_time}")
        self.assertEqual(column in data.columns, True)
        self.assertEqual(len(data[column]), 300)

    def test_slope_process(self):
        client = CSVClient(files=file_path, columns=ohlc_columns, date_column=date_column, logger=logger, start_index=200)
        prc = utils.SlopeProcess(window=5, target_column=ohlc_columns[3])
        slp_column = prc.columns["Slope"]
        start_time = datetime.datetime.now()
        data = client.get_ohlc(100, idc_processes=[prc])
        end_time = datetime.datetime.now()
        logger.debug(f"slope process took {end_time - start_time}")
        self.assertEqual(slp_column in data.columns, True)
        self.assertEqual(len(data[slp_column]), 100)

    def test_macdslope_process(self):
        client = CSVClient(files=file_path, columns=ohlc_columns, date_column=date_column, logger=logger, start_index=200)
        macd = utils.MACDProcess(target_column=ohlc_columns[3])
        macd_column = macd.columns["MACD"]
        signal_column = macd.columns["Signal"]
        prc = utils.SlopeProcess(key="m", window=5, target_column=macd_column)
        s_prc = utils.SlopeProcess(key="s", window=5, target_column=signal_column)
        slp_column = prc.columns["Slope"]
        s_slp_column = s_prc.columns["Slope"]
        start_time = datetime.datetime.now()
        data = client.get_ohlc(100, idc_processes=[macd, prc, s_prc])
        end_time = datetime.datetime.now()
        logger.debug(f"macd slope process took {end_time - start_time}")
        self.assertEqual(slp_column in data.columns, True)
        self.assertEqual(len(data[slp_column]), 100)
        self.assertEqual(s_slp_column in data.columns, True)
        self.assertEqual(len(data[s_slp_column]), 100)
        self.assertNotEqual(data[slp_column].iloc[-1], data[s_slp_column].iloc[-1])

    def test_range_process(self):
        client = CSVClient(files=file_path, columns=ohlc_columns, date_column=date_column, logger=logger, start_index=200)
        slope_window = 4
        range_p = utils.RangeTrendProcess(slope_window=slope_window)
        bband_p = utils.BBANDProcess(alpha=2, target_column=ohlc_columns[3])
        start_time = datetime.datetime.now()
        client.get_ohlc(100, idc_processes=[bband_p, range_p])
        end_time = datetime.datetime.now()
        logger.debug(f"range process took {end_time - start_time}")
        data = client.get_ohlc(100, idc_processes=[bband_p, range_p])
        self.assertEqual(len(data), 100)
        ran = data[range_p.columns[range_p.RANGE_KEY]]
        self.assertLessEqual(ran.max(), 1)
        self.assertGreaterEqual(ran.min(), -1)
        mv = data["BB_MV"]
        slope = data[range_p.columns[range_p.TREND_KEY]]
        for i in range(slope_window, 100):
            if slope.iloc[i] >= 0:
                self.assertGreaterEqual(mv.iloc[i], mv.iloc[i - slope_window])
            else:
                self.assertLess(mv.iloc[i], mv.iloc[i - slope_window])

    def test_min_max(self):
        import random

        open = [random.random() * 123 for index in range(100)]
        close = [o_value + random.random() - 0.5 for o_value in open]
        ds = pd.DataFrame({"close": close, "open": open})
        mm = utils.MinMaxPreProcess(scale=(-1, 1))

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

        df = pd.DataFrame(result)
        reverted_data = mm.revert(df)
        for index in range(0, len(open)):
            self.assertAlmostEqual(reverted_data["open"].iloc[index], open[index])
            self.assertAlmostEqual(reverted_data["close"].iloc[index], close[index])

        reverted_column = mm.revert(result["close"], column="close")
        for index in range(0, len(open)):
            self.assertAlmostEqual(reverted_column[index], close[index])

        reverted_row = mm.revert(df.iloc[0])
        for column in ds.iloc[0].index:  # ds is answer/org values
            self.assertAlmostEqual(reverted_row[column], ds[column].iloc[0])

        # check if update works with updating min max value
        new_open_value = ds["open"].max() + random.random() + 0.1
        new_close_value = new_open_value + random.random() - 0.5

        new_data = pd.Series({"close": new_close_value, "open": new_open_value})
        new_data_standalized = mm.update(new_data)
        new_ds = mm.concat(pd.DataFrame(result), new_data_standalized)
        self.assertTrue(new_data_standalized["open"] == 1)
        self.assertTrue(len(new_ds["close"]) == 101)
        self.assertTrue(new_ds["close"].min() >= -1)
        self.assertTrue(new_ds["close"].max() <= 1)
        self.assertTrue(new_ds["open"].min() >= -1)
        self.assertTrue(new_ds["open"].max() <= 1)

    def test_min_max_with_ignore_columns(self):
        import random

        open = [random.random() * 123 for index in range(100)]
        high = [o_value + random.random() for o_value in open]
        low = [o_value - random.random() for o_value in open]
        close = [o_value + random.random() - 0.5 for o_value in open]
        time = [index for index in range(100)]
        ds = pd.DataFrame({"open": open, "high": high, "low": low, "close": close, "time": time})

        columns_to_ignore = ["time"]
        mm = utils.MinMaxPreProcess(scale=(-1, 1), columns_to_ignore=columns_to_ignore)
        result = mm.run(ds)
        check = "time" in result
        self.assertFalse(check)

    def test_min_max_entire_mode(self):
        import random

        high = [random.random() * 123 for index in range(100)]
        low = [h_value - 1 for h_value in high]
        ds = pd.DataFrame({"high": high, "low": low})
        mm = utils.MinMaxPreProcess(scale=(-1, 1))

        result = mm.run(ds)
        self.assertEqual(result["high"].max(), 1)
        self.assertEqual(result["low"].min(), -1)
        self.assertGreater(result["high"].max(), result["low"].max())
        self.assertLess(result["low"].min(), result["high"].min())
        # check if revert works
        reverted_data = mm.revert(result)
        for index in range(0, len(low)):
            self.assertAlmostEqual(reverted_data["low"][index], low[index])
            self.assertAlmostEqual(reverted_data["high"][index], high[index])

        df = pd.DataFrame(result)
        reverted_data = mm.revert(df)
        for index in range(0, len(low)):
            self.assertAlmostEqual(reverted_data["low"].iloc[index], low[index])
            self.assertAlmostEqual(reverted_data["high"].iloc[index], high[index])

        reverted_column = mm.revert(result["high"], column="high")
        for index in range(0, len(low)):
            self.assertAlmostEqual(reverted_column[index], high[index])

        reverted_row = mm.revert(df.iloc[0])
        for column in ds.iloc[0].index:  # ds is answer/org values
            self.assertAlmostEqual(reverted_row[column], ds[column].iloc[0])

        # check if update works with updating min max value
        new_high_value = ds["high"].max() + 0.5
        new_low_value = ds["low"].min() - 0.1

        new_data = pd.Series({"high": new_high_value, "low": new_low_value})
        new_data_standalized = mm.update(new_data)
        new_ds = mm.concat(pd.DataFrame(result), new_data_standalized)
        self.assertTrue(new_data_standalized["low"] == -1)
        self.assertTrue(new_data_standalized["high"] == 1)
        self.assertTrue(len(new_ds["high"]) == 101)
        self.assertTrue(new_ds["high"].min() >= -1)
        self.assertTrue(new_ds["high"].max() <= 1)
        self.assertTrue(new_ds["low"].min() >= -1)
        self.assertTrue(new_ds["low"].max() <= 1)

    def test_diff(self):
        process = utils.DiffPreProcess()
        ds = pd.DataFrame({"input": [10, 20, 1], "expect": [numpy.NaN, 10, -19]})
        diff_dict = process.run(ds)
        for index in range(1, len(ds)):
            self.assertEqual(diff_dict["input"].iloc[index], ds["expect"].iloc[index])

        new_data = pd.Series({"input": 10, "expect": 9})
        standalized_new_data = process.update(new_data)
        standalized_ds = process.concat(ds, standalized_new_data)
        self.assertEqual(len(standalized_ds), 4)
        self.assertEqual(standalized_ds["input"].iloc[3], new_data["expect"])


if __name__ == "__main__":
    unittest.main()
