import datetime
import json
import os
import random
import sys
import time
import unittest

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(module_path)
import pandas as pd

import finance_client.frames as Frame
from finance_client import utils
from finance_client.client_base import Client


class TestClient(Client):
    def __init__(
        self,
        budget=1000000,
        indicater_processes: list = [],
        do_rendere=False,
        post_processes: list = [],
        frame: int = Frame.MIN5,
        provider="Default",
        logger_name=None,
        logger=None,
    ):
        super().__init__(budget, indicater_processes, post_processes, frame, provider, do_rendere, logger_name, logger)
        self.data = pd.DataFrame.from_dict(
            {
                "Open": [*[100 + open - 1 for open in range(0, 1000)], *[100 for open in range(0, 100)]],
                "High": [*[110 + high for high in range(0, 1000)], *[150 for high in range(0, 100)]],
                "Low": [*[90 + low for low in range(0, 1000)], *[50 for low in range(0, 100)]],
                "Close": [*[100 + close for close in range(0, 1000)], *[100 for close in range(0, 100)]],
            }
        )
        self.step_index = 200

    def get_additional_params(self):
        return {}

    def _get_ohlc_from_client(self, length: int = None, symbols: list = [], frame: int = None, grouped_by_symbol=None):
        df = self.data.iloc[self.step_index - length + 1 : self.step_index + 1]
        self.step_index += 1
        return df

    def get_future_rates(self, interval) -> pd.DataFrame:
        return self.data.iloc[self.step_index + interval]

    def get_current_ask(self) -> float:
        return random.choice(self.data["Open"].iloc[self.step_index], self.data["High"].iloc[self.step_index])

    def get_current_bid(self) -> float:
        return random.choice(self.data["Low"].iloc[self.step_index], self.data["Open"].iloc[self.step_index])

    def _market_buy(self, symbol, ask_rate, amount, tp, sl, option_info):
        return True, None

    def _market_sell(self, symbol, bid_rate, amount, tp, sl, option_info):
        return True, None

    def _buy_for_settlement(self, symbol, ask_rate, amount, option_info, result):
        pass

    def _sell_for_settlment(self, symbol, bid_rate, amount, option_info, result):
        pass

    def get_params(self) -> dict:
        print("Need to implement get_params")

    # defined by the actual client for dataset or env
    def close_client(self):
        pass

    def get_next_tick(self, frame=5):
        print("Need to implement get_next_tick")

    def reset(self, mode=None):
        print("Need to implement reset")

    def get_min_max(column, data_length=0):
        pass

    @property
    def max(self):
        print("Need to implement max")
        return 1

    @property
    def min(self):
        print("Need to implement min")
        return -1

    def __getitem__(self, ndx):
        return None


class TestCSVClient(unittest.TestCase):
    def test_get_rates_wo_plot(self):
        client = TestClient(do_rendere=False)
        rates = client.get_ohlc(100)
        self.assertEqual(len(rates["Open"]), 100)

    def test_get_rates_with_plot(self):
        client = TestClient(do_rendere=True)
        for i in range(0, 10):
            client.get_ohlc(100)
            time.sleep(1)


if __name__ == "__main__":
    unittest.main()
