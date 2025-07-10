import os
import sys
import unittest

import dotenv

dotenv.load_dotenv("tests/.env")
os.environ["FC_DEBUG"] = "true"

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
print(module_path)
sys.path.append(module_path)
from finance_client import fprocess
from finance_client.mt5.client import MT5Client


class TestMT5Client(unittest.TestCase):

    def __init__(self, methodName="runTest"):
        super().__init__(methodName)
        self.client = MT5Client(
            id=int(os.environ["mt5_id"]),
            password=os.environ["mt5_password"],
            server=os.environ["mt5_server"],
            auto_index=False,
            simulation=True,
            frame=30,
        )

    def test_download(self):
        SYMBOLS = ["AUDJPY", "AUDUSD", "EURJPY", "EURUSD", "GBPJPY", "GBPUSD", "USDCHF"]
        df = self.client.download(symbols=SYMBOLS)
        self.assertGreater(len(df), 0)

    def test_get_columns(self):
        ohlc_columns_dict = self.client.get_ohlc_columns()
        self.assertIsInstance(ohlc_columns_dict, dict)
        self.assertEqual(len(ohlc_columns_dict), 6)
        self.assertEqual(ohlc_columns_dict["Open"], "open")

    def test_get_prices(self):
        single_ask = self.client.get_current_ask("USDJPY")
        single_bid = self.client.get_current_bid("USDJPY")
        single_spread = self.client.get_current_spread("USDJPY")
        self.assertGreaterEqual(single_ask, single_bid)
        self.assertGreaterEqual(single_spread, 0)

        multi_asks = self.client.get_current_ask(["USDJPY", "AUDUSD"])
        self.assertEqual(len(multi_asks), 2)
        multi_bids = self.client.get_current_bid(["USDJPY", "AUDUSD"])
        self.assertEqual(len(multi_bids), 2)
        self.assertGreaterEqual(multi_asks["USDJPY"], multi_bids["USDJPY"])
        self.assertGreaterEqual(multi_asks["AUDUSD"], multi_bids["AUDUSD"])
        multi_spreads = self.client.get_current_spread(["USDJPY", "AUDUSD"])
        self.assertEqual(len(multi_spreads), 2)
        self.assertGreaterEqual(multi_spreads["USDJPY"], 0)
        self.assertGreaterEqual(multi_spreads["AUDUSD"], 0)

    def test_get_symbols(self):
        symbols = self.client.get_symbols()
        self.assertEqual(len(symbols), 64)

    def test_get_rate_with_indicaters(self):
        symbols = ["USDJPY", "AUDUSD"]
        columns = self.client.get_ohlc_columns()
        close_column = columns["Close"]
        macd_p = fprocess.MACDProcess(target_column=close_column)
        data = self.client.get_ohlc(symbols=symbols, length=100, idc_processes=[macd_p])
        self.assertEqual(macd_p.KEY_MACD in data[symbols[0]].columns, True)
        self.assertEqual(len(data[symbols[0]][macd_p.KEY_MACD]), 100)


if __name__ == "__main__":
    unittest.main()
