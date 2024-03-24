import datetime
import os
import sys
import unittest

import dotenv
import pandas as pd

try:
    dotenv.load_dotenv("tests/.env")
except Exception:
    pass

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
print(module_path)
sys.path.append(module_path)

from finance_client import fprocess, logger
from finance_client.csv.client import CSVClient

file_path = os.path.abspath("L:/data/mt5/OANDA-Japan MT5 Live/mt5_USDJPY_min30.csv")
ohlc_columns = ["open", "high", "low", "close"]
date_column = "time"


class TestIndicaters(unittest.TestCase):
    client = CSVClient(files=file_path, columns=ohlc_columns, date_column=date_column, auto_step_index=False, start_index=500)

    def test_cci_process(self):
        ohlc = self.client.get_ohlc(110)
        cci_prc = fprocess.CCIProcess(14, ohlc_column=ohlc_columns)
        input_data_1 = ohlc.iloc[:100].copy()
        self.assertEqual(len(input_data_1), 100)
        cci = cci_prc.run(input_data_1)

        next_tick = ohlc.iloc[100]
        next_cci = cci_prc.update(next_tick)
        next_cci_from_run = cci_prc.run(ohlc.iloc[:101])
        cci_column = cci_prc.KEY_CCI
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

        process = fprocess.MACDProcess(target_column=ohlc_columns[3])
        macd_dict = process.run(ds)
        short_column = process.KEY_SHORT_EMA
        long_column = process.KEY_LONG_EMA
        macd_column = process.KEY_MACD
        signal_column = process.KEY_SIGNAL

        self.assertEqual(macd_dict[short_column].iloc[-1], short_ema.iloc[-1])
        self.assertEqual(macd_dict[long_column].iloc[-1], long_ema.iloc[-1])
        self.assertEqual(macd_dict[macd_column].iloc[-1], macd.iloc[-1])
        self.assertEqual(macd_dict[signal_column].iloc[-1], signal.iloc[-1])

    def test_macd_update(self):
        # prerequisites
        ds = pd.DataFrame({ohlc_columns[3]: [120.000 + i * 0.1 for i in range(30)]})
        process = fprocess.MACDProcess(target_column=ohlc_columns[3])
        macd_dict = process.run(ds)

        # input
        new_value = 120.000 + 30 * 0.1
        new_data = pd.Series({ohlc_columns[3]: new_value})

        # output
        test_data = process.update(new_data)

        # expect
        ex_data_org = fprocess.concat(ds, new_data)
        another_ps = fprocess.MACDProcess(target_column=ohlc_columns[3])
        macd_dict = another_ps.run(ex_data_org)

        short_column = process.KEY_SHORT_EMA
        long_column = process.KEY_LONG_EMA
        macd_column = process.KEY_MACD
        signal_column = process.KEY_SIGNAL

        self.assertEqual(macd_dict[short_column].iloc[-1], test_data[short_column])
        self.assertEqual(macd_dict[long_column].iloc[-1], test_data[long_column])
        self.assertEqual(macd_dict[macd_column].iloc[-1], test_data[macd_column])
        self.assertEqual(macd_dict[signal_column].iloc[-1], test_data[signal_column])

    def test_ema_process(self):
        window = 4
        input = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 20]
        out_ex = [input[0]]
        alpha = 2 / (1 + window)

        for i in range(1, len(input)):
            out_ex.append(out_ex[i - 1] * (1 - alpha) + input[i] * alpha)

    def test_renko_process(self):
        client = CSVClient(files=file_path, columns=ohlc_columns, date_column=date_column, start_index=200)
        prc = fprocess.RenkoProcess(window=120, ohlc_column=ohlc_columns)
        column = prc.KEY_BRICK_NUM
        start_time = datetime.datetime.now()
        data = client.get_ohlc(300, idc_processes=[prc])
        end_time = datetime.datetime.now()
        logger.debug(f"renko process took {end_time - start_time}")
        self.assertEqual(column in data.columns, True)
        self.assertEqual(len(data[column]), 300)

    def test_slope_process(self):
        client = CSVClient(files=file_path, columns=ohlc_columns, date_column=date_column, start_index=200)
        prc = fprocess.SlopeProcess(window=5, target_column=ohlc_columns[3])
        slp_column = prc.KEY_SLOPE
        start_time = datetime.datetime.now()
        data = client.get_ohlc(100, idc_processes=[prc])
        end_time = datetime.datetime.now()
        logger.debug(f"slope process took {end_time - start_time}")
        self.assertEqual(slp_column in data.columns, True)
        self.assertEqual(len(data[slp_column]), 100)

    def test_macdslope_process(self):
        client = CSVClient(files=file_path, columns=ohlc_columns, date_column=date_column, start_index=200)
        macd = fprocess.MACDProcess(target_column=ohlc_columns[3])
        macd_column = macd.KEY_MACD
        signal_column = macd.KEY_SIGNAL
        prc = fprocess.SlopeProcess(key="m", window=5, target_column=macd_column)
        s_prc = fprocess.SlopeProcess(key="s", window=5, target_column=signal_column)
        slp_column = prc.KEY_SLOPE
        s_slp_column = s_prc.KEY_SLOPE
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
        client = CSVClient(files=file_path, columns=ohlc_columns, date_column=date_column, start_index=200)
        slope_window = 4
        range_p = fprocess.RangeTrendProcess(slope_window=slope_window)
        bband_p = fprocess.BBANDProcess(alpha=2, target_column=ohlc_columns[3], window=14)
        start_time = datetime.datetime.now()
        client.get_ohlc(100, idc_processes=[bband_p, range_p])
        end_time = datetime.datetime.now()
        logger.debug(f"range process took {end_time - start_time}")
        data = client.get_ohlc(100, idc_processes=[bband_p, range_p])
        self.assertEqual(len(data), 100)
        ran = data[range_p.KEY_RANGE]
        self.assertLessEqual(ran.max(), 1)
        self.assertGreaterEqual(ran.min(), -1)
        mv = data[bband_p.KEY_MEAN_VALUE]
        slope = data[range_p.KEY_TREND]
        for i in range(14 + slope_window, 100):
            if slope.iloc[i] >= 0:
                self.assertGreaterEqual(mv.iloc[i], mv.iloc[i - slope_window])
            else:
                self.assertLess(mv.iloc[i], mv.iloc[i - slope_window])


if __name__ == "__main__":
    unittest.main()
