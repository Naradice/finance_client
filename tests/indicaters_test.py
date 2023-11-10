import datetime
import json
import os
import sys
import unittest

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
print(module_path)
sys.path.append(module_path)

from logging import config, getLogger

import dotenv
import pandas as pd

import finance_client as fc
from finance_client.fprocess.fprocess.indicaters import technical

try:
    with open(os.path.join(module_path, "finance_client/settings.json"), "r") as f:
        settings = json.load(f)
except Exception as e:
    print(f"fail to load settings file: {e}")
    raise e
logger_config = settings["log"]
log_file_base_name = logger_config["handlers"]["fileHandler"]["filename"]
log_path = f'./{log_file_base_name}_idcprocess_{datetime.datetime.utcnow().strftime("%Y%m%d%H")}.log'
logger_config["handlers"]["fileHandler"]["filename"] = log_path
config.dictConfig(logger_config)
logger = getLogger("finance_client.test")
dotenv.load_dotenv(".env")

file_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../finance_client/data_source/mt5/OANDA-Japan MT5 Live/mt5_USDJPY_min5.csv")
)


class TestIndicaters(unittest.TestCase):
    ohlc_columns = ["open", "high", "low", "close"]
    client = fc.CSVClient(files=file_path, columns=["open", "high", "low", "close"], date_column="time", logger=logger)

    def __init__(self, methodName: str = ...) -> None:
        self.window = 4
        self.input = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 20]
        self.out_ex = [self.input[0]]
        alpha = 2 / (1 + self.window)

        for i in range(1, len(self.input)):
            self.out_ex.append(self.out_ex[i - 1] * (1 - alpha) + self.input[i] * alpha)

        super().__init__(methodName)

    def test_ema(self):
        out = technical.EMA(self.input, self.window)
        self.assertListEqual(self.out_ex, out)

    def test_ema_series(self):
        input = pd.Series(self.input)
        out = technical.EMA(input, self.window)
        success = False
        self.assertEqual(len(out), len(self.out_ex))
        for i in range(0, len(self.out_ex)):
            if out[i] == self.out_ex[i]:
                success = True
            else:
                print(f"Failed on {i}: {out[i]} isnt {self.out_ex[i]}")
                success = False
        self.assertTrue(success)

    def test_sma(self):
        window = 4
        sample_array = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 20]
        result_array = technical.SMA(sample_array, window)
        self.assertEqual(len(sample_array), len(result_array))
        self.assertEqual(result_array[-1], (11 + 12 + 13 + 20) / window)

    def test_bolinger(self):
        # print(fprocess.indicaters.BolingerFromOHLC([1,2,3,4,5,6,7,8,9,10,11,12,13,20],window=5))
        pass

    def test_revert_ema(self):
        ema = technical.EMA(self.input, self.window)
        suc, r_ema = technical.revert_EMA(ema, self.window)
        self.assertTrue(suc)
        for i in range(0, len(self.input)):
            self.assertAlmostEqual(self.input[i], r_ema[i])

    def test_renko(self):
        df = self.client.get_ohlc(100)
        try:
            technical.RenkoFromOHLC(df, ohlc_columns=self.ohlc_columns)
        except Exception as e:
            if "less than atr_length" not in str(e):
                raise e
            else:
                logger.info(f"error happened as expected: {e}")
        df = self.client.get_ohlc(120)
        renko = technical.RenkoFromOHLC(df, ohlc_columns=self.ohlc_columns)
        self.assertLessEqual(len(renko["Brick"]), 120)

    def test_slope(self):
        data_length = 1000
        df = self.client.get_ohlc(data_length)
        start_time = datetime.datetime.now()
        slope_df = technical.SlopeFromOHLC(df, window=10, column="close")
        end_time = datetime.datetime.now()
        logger.debug(f"slope took {end_time - start_time}")
        self.assertEqual(len(slope_df["Slope"]), data_length)

    def test_macdslope(self):
        data_length = 1000
        df = self.client.get_ohlc(data_length)
        start_time = datetime.datetime.now()
        slope_df = technical.SlopeFromOHLC(df, window=10, column="close")
        end_time = datetime.datetime.now()
        logger.debug(f"slope took {end_time - start_time}")
        self.assertEqual(len(slope_df["Slope"]), data_length)

    def test_cci(self):
        ohlc = self.client.get_ohlc(100)
        cci = technical.CommodityChannelIndex(ohlc, ohlc_columns=self.ohlc_columns)
        self.assertEqual(len(cci), 100)
        max_value = cci.max().values
        self.assertLess(max_value, 300)
        min_value = cci.min().values
        self.assertGreater(min_value, -300)


if __name__ == "__main__":
    unittest.main()
