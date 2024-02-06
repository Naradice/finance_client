import os
import sys
import time
import unittest

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(module_path)

import pandas as pd

from finance_client.csv.client import CSVClient
from finance_client.fprocess import fprocess
from finance_client.render.graph import Rendere

csv_file = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../finance_client/data_source/mt5/OANDA-Japan MT5 Live/mt5_USDJPY_d1.csv")
)


class TestRender(unittest.TestCase):
    def test_plot_ohlc(self):
        r = Rendere()
        for i in range(5):
            r.add_subplot()
        data = {
            "Open": [10000, 10000, 10000, 10000],
            "High": [11000, 12000, 13000, 14000],
            "Low": [7000, 7000, 7000, 7000],
            "Close": [9000, 9000, 9000, 9000],
        }
        df = pd.DataFrame(data)
        r.register_ohlc(["symbol"], df, 4, ohlc_columns=["Open", "High", "Low", "Close"])
        r.plot()
        time.sleep(5)

    def test_plot_ohlc_from_csv_client(self):
        r = Rendere()
        columns = ["high", "low", "open", "close"]
        client = CSVClient(files=csv_file, columns=columns, date_column="time", start_index=30)
        df = client.get_ohlc(30)
        r.register_ohlc(["symbol"], df, ohlc_columns=columns)
        r.plot()
        time.sleep(10)

    def test_plot_bband_from_client_out(self):
        r = Rendere()
        bban = fprocess.BBANDProcess(target_column="close", window=14)
        columns = ["high", "low", "open", "close"]
        client = CSVClient(files=csv_file, columns=columns, date_column="time", idc_process=[bban], start_index=30)
        df = client.get_ohlc(30)
        index = r.register_ohlc_with_indicaters(["symbol"], df, [bban], ohlc_columns=columns)
        r.plot()
        time.sleep(10)


if __name__ == "__main__":
    unittest.main()
