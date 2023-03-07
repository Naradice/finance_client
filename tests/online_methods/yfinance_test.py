import datetime
import json
import os
import sys
import unittest
from time import sleep

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
print(module_path)
sys.path.append(module_path)
import dotenv

from finance_client.yfinance.client import YahooClient

dotenv.load_dotenv("../.env")


class TestYFClient(unittest.TestCase):
    frame = 1
    client = YahooClient("1812.T", start_index=200, auto_step_index=True, frame=frame)

    def test_get_rates(self):
        length = 10
        rates = self.client.get_ohlc(length)
        self.assertEqual(len(rates.Close), length)

    def test_orders(self):
        id = self.client.open_trade(True, 1, "Market", "1812.T", 1000)
        sleep(10)
        self.client.close_all_positions()

    def test_get_rates_with_indicater(self):
        from finance_client.fprocess.idcprocess import (BBANDProcess, MACDProcess,
                                                     RangeTrendProcess)

        macd_p = MACDProcess(short_window=12, long_window=26, signal_window=9, target_column="Close")
        macd_column = macd_p.columns["MACD"]
        bband_process = BBANDProcess(target_column="Close", alpha=2)
        rtp_p = RangeTrendProcess(slope_window=3)
        range_column = rtp_p.columns[rtp_p.KEY_RANGE]
        data = self.client.get_ohlc(100, idc_processes=[macd_p, bband_process, rtp_p])
        self.assertTrue(macd_column in data.columns)
        self.assertTrue(range_column in data.columns)

    def test_multi_symbols(self):
        client = YahooClient(["1801.T", "1928.T"], start_index=200, auto_step_index=True, frame=1)
        df = client.get_ohlc(10)
        self.assertEqual(len(df), 10)
        self.assertEqual(len(df["1801.T"]), 10)
        self.assertEqual(len(df["1928.T"]), 10)
        print(df.columns)

        client.open_trade(True, 1, "Market", "1801.T", 1000)
        client.open_trade(True, 1, "Market", "1928.T", 1000)
        for i in range(0, 5):
            client.get_ohlc(5)
        results = client.close_all_positions()
        self.assertEqual(len(results), 2)


if __name__ == "__main__":
    unittest.main()
