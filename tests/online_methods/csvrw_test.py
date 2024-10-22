import datetime
import json
import os
import sys
import unittest

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(module_path)

import time

import dotenv
import pandas as pd
import yfinance as yf

import finance_client.fprocess.fprocess.csvrw as csvrw


class TestYFClient(unittest.TestCase):
    symbols = ["6902.T", "7202.T"]

    def test_write_read(self):
        dotenv.load_dotenv(".env")
        rates = yf.download(
            self.symbols, interval="60m", start=datetime.datetime.now() - datetime.timedelta(days=10), group_by="ticker"
        )
        csvrw.write_multi_symbol_df_to_csv(rates, "yfinance", "yfinance_60", self.symbols)
        time.sleep(3)
        dfs = csvrw.read_csvs("yfinance", "yfinance_60", self.symbols, ["Datetime"])
        self.assertEqual(type(dfs.columns), pd.MultiIndex)


if __name__ == "__main__":
    unittest.main()
