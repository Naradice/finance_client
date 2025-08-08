import datetime
import os
import sys
import unittest
from time import sleep

import dotenv

try:
    dotenv.load_dotenv("tests/.env")
except Exception:
    pass

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
print(module_path)
sys.path.append(module_path)

import finance_client.vantage.client as vclient
import finance_client.vantage.target as target
from finance_client.coincheck.client import CoinCheckClient


class TestCCClient(unittest.TestCase):
    def test_initialize(self):
        frame = 1
        vantage_client = vclient.VantageClient(
            api_key=os.environ["vantage_api_key"], frame=frame, finance_target=target.CRYPTO_CURRENCY, symbol=("BTC", "JPY")
        )
        client = CoinCheckClient(
            ACCESS_ID=os.environ["cc_ACCESS_ID"],
            ACCESS_SECRET=os.environ["cc_ACCESS_SECRET"],
            frame=frame,
            initialized_with=vantage_client,
            return_intermidiate_data=True,
            simulation=False,
        )
        df = client.get_rates(100)
        self.assertEqual(len(df["open"]), 100)

        time_test_df = df.copy()
        time_test_df["test_time"] = time_test_df.index.copy()
        time_diffs = time_test_df["test_time"].diff().iloc[1:]  # index 0 is numpy.Null
        condition = time_diffs != datetime.timedelta(minutes=frame)
        condition = condition.values
        result = time_diffs[condition]
        self.assertEqual(len(result), 0)
        sleep(60 * 3)
        new_df = client.get_rates(100)

    def test_orders(self):
        client = CoinCheckClient(os.environ["cc_ACCESS_ID"], os.environ["cc_ACCESS_SECRET"], frame=1)
        id = client.market_buy("BTCJPY", 2952000, 0.005, None, None, None)
        sleep(120)
        client._sell_to_close("BTCJPY", 2960000, 0.005, None, id)

    def test_get_rates(self):
        length = 10
        rates = self.client.get_rates(length)
        # self.assertEqual(len(rates.Close), length)

    def test_get_next_tick(self):
        print(self.client.get_next_tick())


if __name__ == "__main__":
    unittest.main()
