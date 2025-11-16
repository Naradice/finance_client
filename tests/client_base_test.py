import os
import random
import sys
import time
import unittest

import pandas as pd

os.environ["FC_DEBUG"] = "true"
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(module_path)
import finance_client.frames as Frame
from finance_client import db
from finance_client.client_base import ClientBase


class TestClient(ClientBase):
    def __init__(
        self,
        budget=1000000,
        indicater_processes: list = None,
        do_render=False,
        pre_processes: list = None,
        frame: int = Frame.MIN5,
        provider="Default",
        storage=None,
    ):
        if storage is None:
            base_path = os.path.dirname(__file__)
            self.storage_file_path = os.path.abspath(os.path.join(base_path, f"test_storage.json"))
            storage = db.FileStorage(provider="TestClient", username=None, positions_path=self.storage_file_path)
        super().__init__(
            budget=budget,
            provider=provider,
            out_ohlc_columns=["Open", "High", "Low", "Close"],
            idc_process=indicater_processes,
            pre_process=pre_processes,
            frame=frame,
            do_render=do_render,
            storage=storage,
        )
        self.data = pd.DataFrame.from_dict(
            {
                "Open": [100 + open - 1 for open in range(0, 1000)],
                "High": [110 + high for high in range(0, 1000)],
                "Low": [90 + low for low in range(0, 1000)],
                "Close": [100 + close for close in range(0, 1000)],
            }
        )
        self.step_index = 200

    def get_additional_params(self):
        return {}

    def _get_ohlc_from_client(self, length, symbols, frame, columns, index, grouped_by_symbol):
        if length is None:
            length = 10
        if index is None:
            df = self.data.iloc[self.step_index - length + 1 : self.step_index + 1]
            self.step_index += 1
        else:
            df = self.data.iloc[index - length : index]
        return df

    def get_future_rates(self, interval) -> pd.DataFrame:
        return self.data.iloc[self.step_index + interval]

    def get_current_ask(self, symbols: list = None) -> float:
        min_value = self.data.loc[self.step_index, "Open"]
        max_value = self.data.loc[self.step_index, "High"]
        return random.randrange(min_value, max_value)

    def get_current_bid(self, symbols: list = None) -> float:
        min_value = self.data.loc[self.step_index, "Low"]
        max_value = self.data.loc[self.step_index, "Open"]
        return random.randrange(min_value, max_value)

    def _market_buy(self, symbol, price, amount, tp, sl, option_info=None):
        return True, None

    def _market_sell(self, symbol, price, amount, tp, sl, option_info=None):
        return True, None

    def _buy_to_close(self, symbol, ask_rate, amount, option_info=None, result=None):
        return True

    def _sell_to_close(self, symbol, bid_rate, amount, option_info=None, result=None):
        return True

    def get_params(self) -> dict:
        print("Need to implement get_params")

    # defined by the actual client for dataset or env
    def close_client(self):
        pass

    def get_next_tick(self, frame=5):
        print("Need to implement get_next_tick")

    def reset(self, mode=None):
        print("Need to implement reset")

    @property
    def max(self):
        print("Need to implement max")
        return 1

    @property
    def min(self):
        print("Need to implement min")
        return -1

    def __len__(self):
        return super().__len__()
    
class TestMultiClient(TestClient):
    def __init__(
        self,
        budget=1000000,
        do_render=False,
    ):
        base_path = os.path.dirname(__file__)
        self.storage_file_path = os.path.abspath(os.path.join(base_path, f"test_multi_storage.json"))
        storage = db.FileStorage(provider="TestClient", username=None, positions_path=self.storage_file_path)

        super().__init__(
            budget=budget,
            provider="Default",
            indicater_processes=None,
            pre_processes=None,
            frame=Frame.MIN5,
            do_render=do_render,
            storage=storage,
        )
        uj_data = pd.DataFrame.from_dict(
            {
                "Open": [100 + open - 1 for open in range(0, 1000)],
                "High": [110 + high - 1 for high in range(0, 1000)],
                "Low": [90 + low - 1 for low in range(0, 1000)],
                "Close": [100 + close - 1 for close in range(0, 1000)],
            }
        )
        ua_data = pd.DataFrame.from_dict(
            {
                "Open": [1.0 + (open - 1) * 0.01 for open in range(0, 1000)],
                "High": [110 + high * 0.01 for high in range(0, 1000)],
                "Low": [90 + low * 0.01 for low in range(0, 1000)],
                "Close": [100 + close * 0.01 for close in range(0, 1000)],
            }
        )
        self.data = pd.concat([uj_data, ua_data], keys=["USDJPY", "USDAUD"], axis=1)
        self.step_index = 200

    def _get_ohlc_from_client(self, length, symbols, frame, columns, index, grouped_by_symbol):
        if length is None:
            length = 10
        if index is None:
            df = self.data.loc[self.step_index - length + 1 : self.step_index + 1, symbols]
            self.step_index += 1
        else:
            df = self.data[symbols].loc[index - length : index, symbols]
        return df

    def get_future_rates(self, interval) -> pd.DataFrame:
        return self.data.iloc[self.step_index + interval]

    def get_current_ask(self, symbols=None) -> pd.Series:
        if symbols is None:
            symbols = ["USDJPY", "USDAUD"]
        prices = []
        for symbol in symbols:
            price = random.randrange(self.data.loc[self.step_index, (symbol, "Open")], self.data.loc[self.step_index, (symbol, "High")])
            prices.append(price)
        return pd.Series(prices, index=symbols)

    def get_current_bid(self, symbols=None) -> float:
        return random.randrange(self.data["Low"].iloc[self.step_index], self.data["Open"].iloc[self.step_index])


class TestBaseClient(unittest.TestCase):
    def test_get_rates_wo_plot(self):
        client = TestClient(do_render=False)
        rates = client.get_ohlc(None, 100)
        self.assertEqual(len(rates["Open"]), 100)

    def test_get_ohlc_columns(self):
        client = TestClient(do_render=False)
        ohlc_dict = client.get_ohlc_columns()
        self.assertEqual(ohlc_dict["Open"], "Open")
        self.assertEqual(ohlc_dict["High"], "High")
        self.assertEqual(ohlc_dict["Low"], "Low")
        self.assertEqual(ohlc_dict["Close"], "Close")

    def test_get_symbols(self):
        client = TestClient(do_render=False)
        symbols = client.get_symbols()
        self.assertEqual(len(symbols), 0)

    def test_trading_simulation(self):
        budget = 100000
        client = TestClient(budget=budget, do_render=True)
        suc, position = client.open_trade(is_buy=True, amount=100.0, symbol="USDJPY")
        for i in range(0, 5):
            client.get_ohlc(None, 100)
            # check if diagonal graph is shown
            time.sleep(1)
        long, short = client.get_portfolio()
        self.assertEqual(len(long), 1, f"Long position should be 1: {long}")
        current_budget, in_use, profit = client.get_budget()
        # self.assertLess(current_budget, budget)
        self.assertGreater(in_use, 0)
        close_result = client.close_position(id=position.id)
        self.assertTrue(close_result.error is False)
        self.assertNotEqual(close_result.profit, 0)

    # Multiindex without symbol
    def test_multi_ohlc_columns(self):
        client = TestMultiClient(do_render=False)
        ohlc_dict = client.get_ohlc_columns()
        self.assertEqual(ohlc_dict["Open"], "Open")
        self.assertEqual(ohlc_dict["High"], "High")
        self.assertEqual(ohlc_dict["Low"], "Low")
        self.assertEqual(ohlc_dict["Close"], "Close")

    def test_multi_symbols(self):
        client = TestMultiClient(do_render=False)
        symbols = client.get_symbols()
        self.assertEqual(len(symbols), 2, f"Symbols should be 2: {symbols}")
        self.assertEqual(symbols[0], "USDJPY")
        self.assertEqual(symbols[1], "USDAUD")

    # Multiindex with 1 symbol
    def test_multi_et_rates_wo_plot(self):
        client = TestMultiClient(do_render=False)
        rates = client.get_ohlc("USDJPY", 100)
        self.assertEqual(len(rates["Open"]), 100)

    def test_multi_get_ohlc_columns(self):
        client = TestMultiClient(do_render=False)
        ohlc_dict = client.get_ohlc_columns("USDJPY")
        self.assertEqual(ohlc_dict["Open"], "Open")
        self.assertEqual(ohlc_dict["High"], "High")
        self.assertEqual(ohlc_dict["Low"], "Low")
        self.assertEqual(ohlc_dict["Close"], "Close")

    def test_multi_trading_simulation(self):
        budget = 100000
        client = TestClient(budget=budget, do_render=True)
        suc, long_position = client.open_trade(is_buy=True, amount=100.0, symbol="USDJPY")
        for i in range(0, 5):
            client.get_ohlc(None, 100)
            # check if diagonal graph is shown
            time.sleep(1)
        suc, short_position = client.open_trade(is_buy=False, amount=100.0, symbol="USDAUD")
        for i in range(0, 5):
            client.get_ohlc(None, 100)
            # check if diagonal graph is shown
            time.sleep(1)
        long, short = client.get_portfolio()
        self.assertEqual(len(long), 1)
        self.assertEqual(len(short), 1)
        current_budget, in_use, profit = client.get_budget()
        # budget caliculation is removed
        # self.assertLess(current_budget, budget)
        # self.assertGreater(in_use, 0)
        closed_result = client.close_position(id=long_position.id)
        self.assertTrue(closed_result.error is False)
        self.assertNotEqual(closed_result.profit, 0)
        short_close_result = client.close_position(id=short_position.id)
        self.assertTrue(short_close_result.error is False)
        self.assertNotEqual(short_close_result.profit, 0)


if __name__ == "__main__":
    unittest.main()
