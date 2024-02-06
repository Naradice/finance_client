import datetime
import json
import os
import sys
import unittest
from time import sleep

import dotenv
import numpy

try:
    dotenv.load_dotenv("tests/.env")
except Exception as e:
    raise e
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(module_path)

import finance_client.frames as Frame
from finance_client import logger
from finance_client.fprocess import fprocess
from finance_client.mt5 import MT5Client

id = int(os.environ["mt5_id"])
simulation = True


class TestMT5Client(unittest.TestCase):
    symbol = ["USDJPY", "CHFJPY"]
    client = MT5Client(
        id=id,
        password=os.environ["mt5_password"],
        server=os.environ["mt5_server"],
        symbols=symbol,
        auto_index=False,
        simulation=simulation,
        frame=Frame.MIN30,
        logger=logger,
    )
    client.get_ohlc()

    def test_get_current_ask(self):
        ask_value = self.client.get_current_ask()
        self.assertEqual(type(ask_value[self.symbol[0]]), numpy.float64)

    def test_get_current_bid(self):
        bid_value = self.client.get_current_bid()
        self.assertEqual(type(bid_value[self.symbol[0]]), numpy.float64)

    def test_get_rates(self):
        data = self.client.get_ohlc(100)
        columns = self.client.get_ohlc_columns()
        close_column = columns["Close"]
        self.assertEqual(len(data[self.symbol[0]][close_column]), 100)
        time_column = columns["Time"]
        self.assertEqual(time_column in data[self.symbol[0]].columns, True)
        time_sr = data[self.symbol[0]][time_column]
        first = time_sr.iloc[0]
        last = time_sr.iloc[-1]
        self.assertGreater(last, first)  # last > first

    def test_get_rate_with_indicaters(self):
        columns = self.client.get_ohlc_columns()
        close_column = columns["Close"]
        macd_p = fprocess.MACDProcess(target_column=close_column)
        macd_column = macd_p.columns["MACD"]
        data = self.client.get_ohlc(100, idc_processes=[macd_p])
        self.assertEqual(macd_column in data[self.symbol[0]].columns, True)
        self.assertEqual(len(data[self.symbol[0]][macd_column]), 100)

    def test_get_all_rates(self):
        rates = self.client.get_ohlc()
        self.assertNotEqual(type(rates), type(None))

    # takes few minutes

    def test_auto_index_5min(self):
        "check when frame time past during run"
        client = MT5Client(
            id=id,
            password=os.environ["mt5_password"],
            server=os.environ["mt5_server"],
            symbols=self.symbol,
            auto_index=True,
            back_test=True,
            frame=Frame.MIN5,
            logger=logger,
            seed=1111,
        )
        count = 0
        next_time = None
        while count < 6:
            data = client.get_ohlc(30)
            if next_time is not None:
                current_time = data[self.symbol[0]]["time"].iloc[0]
                self.assertEqual(current_time, next_time)
            next_time = data[self.symbol[0]]["time"].iloc[1]
            sleep(60)
            count += 1

    def test_auto_index_1min(self):
        "check when wait time is longer than frame"
        client = MT5Client(
            id=id,
            password=os.environ["mt5_password"],
            server=os.environ["mt5_server"],
            symbols=self.symbol,
            auto_index=True,
            back_test=True,
            frame=Frame.MIN1,
            logger=logger,
        )

        count = 0
        next_time = None
        while count < 3:
            data = client.get_ohlc(30)
            if next_time is not None:
                current_time = data[self.symbol[0]]["time"].iloc[0]
                self.assertEqual(current_time, next_time)
            next_time = data[self.symbol[0]]["time"].iloc[1]
            sleep(120)
            count += 1

    def test_auto_index_H2(self):
        "check when week change"
        client = MT5Client(
            id=id,
            password=os.environ["mt5_password"],
            server=os.environ["mt5_server"],
            symbols=self.symbol,
            auto_index=True,
            back_test=True,
            frame=Frame.H2,
            logger=logger,
            seed=1111,
        )

        count = 0
        next_time = None
        while count < 12 * 7:
            data = client.get_ohlc(30)
            if next_time is not None:
                current_time = data[self.symbol[0]]["time"].iloc[0]
                self.assertEqual(current_time, next_time)
            next_time = data[self.symbol[0]]["time"].iloc[1]
            sleep(1)
            count += 1

    def test_get_data_by_queue(self):
        count = 0
        columns = self.client.get_ohlc_columns()
        close_column = columns["Close"]
        q = self.client.get_data_queue(10)
        test = True
        while test:
            start = datetime.datetime.now()
            data = q.get()
            end = datetime.datetime.now()
            self.assertEqual(len(data[self.symbol[0]][close_column]), 10)
            delta = end - start
            print(delta.total_seconds())
            count += 1
            if count > 2:
                test = False
                break


if __name__ == "__main__":
    unittest.main()
