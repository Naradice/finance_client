import datetime
import json
import os
import sys
import unittest
from logging import config, getLogger

import dotenv

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
print(module_path)
sys.path.append(module_path)

import finance_client.frames as Frame
import finance_client.vantage.target as Target
from finance_client.vantage.apis import DIGITAL, FOREX, STOCK
from finance_client.vantage.client import VantageClient

try:
    with open(os.path.join(module_path, "finance_client/settings.json"), "r") as f:
        settings = json.load(f)
except Exception as e:
    print(f"fail to load settings file: {e}")
    raise e
logger_config = settings["log"]
log_file_base_name = logger_config["handlers"]["fileHandler"]["filename"]
log_path = f'./{log_file_base_name}_vantage-clienttest_{datetime.datetime.utcnow().strftime("%Y%m%d%H")}.logs'
logger_config["handlers"]["fileHandler"]["filename"] = log_path
config.dictConfig(logger_config)
logger = getLogger("finance_client.test")

dotenv.load_dotenv(".env")
fx = FOREX(os.environ["vantage_api_key"], logger)
stock = STOCK(os.environ["vantage_api_key"], logger)
digital = DIGITAL(os.environ["vantage_api_key"], logger)

# fx client
client = VantageClient(os.environ["vantage_api_key"], frame=Frame.D1, symbols=["USDJPY"], start_index=100, auto_step_index=True)

# bc client
bc_client = VantageClient(
    api_key=os.environ["vantage_api_key"],
    frame=Frame.D1,
    finance_target=Target.CRYPTO_CURRENCY,
    symbols=["BTCJPY"],
    start_index=100,
)


class TestVantageClient(unittest.TestCase):
    def test_fx_get_interday(self):
        """Changed to premum API
        data = fx.get_interday_rates(from_symbol="USD", to_symbol="JPY", interval=Frame.MIN1)
        self.assertEqual(type(data), dict)
        self.assertEqual("Time Series FX (1min)" in data, True)
        time.sleep(2)
        data = fx.get_interday_rates(from_symbol="USD", to_symbol="JPY", interval=Frame.MIN5)
        self.assertEqual(type(data), dict)
        self.assertEqual("Time Series FX (5min)" in data, True)
        time.sleep(2)
        data = fx.get_interday_rates(from_symbol="USD", to_symbol="JPY", interval=Frame.MIN30)
        self.assertEqual(type(data), dict)
        self.assertEqual("FX Intraday (30min)" in data, True)
        time.sleep(2)
        data = fx.get_interday_rates(from_symbol="USD", to_symbol="JPY", interval=Frame.H1)
        self.assertEqual(type(data), dict)
        self.assertEqual("FX Intraday (60min)" in data, True)
        """

    def test_fx_get_daily(self):
        data = fx.get_daily_rates(from_symbol="USD", to_symbol="JPY")
        self.assertEqual(type(data), dict)
        self.assertEqual("Time Series FX (Daily)" in data, True)

    def test_fx_get_weekly(self):
        data = fx.get_weekly_rates(from_symbol="USD", to_symbol="JPY")
        self.assertEqual(type(data), dict)
        self.assertEqual("Time Series FX (Weekly)" in data, True)

    def test_fx_get_monthly(self):
        data = fx.get_monthly_rates(from_symbol="USD", to_symbol="JPY")
        self.assertEqual(type(data), dict)
        self.assertEqual("Time Series FX (Monthly)" in data, True)

    def test_fx_get_unsupported_interday(self):
        with self.assertRaises(ValueError):
            data = fx.get_interday_rates(from_symbol="USD", to_symbol="JPY", interval=Frame.MIN10)

    def test_get_all_rates(self):
        df = client.get_ohlc()
        self.assertIn("close", df.columns)
        self.assertGreater(len(df["close"]), 99)

    def test_get_rates(self):
        df = client.get_ohlc(100)
        self.assertIn("close", df.columns)
        self.assertEqual(len(df["close"]), 100)

    def test_bc_get_all_rates(self):
        df = bc_client.get_ohlc()
        print(df)

    def test_fx_get_multi_symbols_rates(self):
        client = VantageClient(
            os.environ["vantage_api_key"], frame=Frame.D1, symbols=["USDJPY", "CHFJPY"], start_index=100, auto_step_index=True
        )
        df = client.get_ohlc(10)
        self.assertEqual(len(df["USDJPY"]), 10)
        self.assertEqual(len(df["CHFJPY"]), 10)


if __name__ == "__main__":
    unittest.main()
