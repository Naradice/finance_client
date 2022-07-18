from multiprocessing.sharedctypes import Value
import unittest, os, json, sys, datetime

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
print(module_path)
sys.path.append(module_path)

import finance_client.vantage as vantage
import finance_client.frames as Frame
from finance_client.vantage.apis import FOREX, STOCK, DIGITAL
from finance_client.vantage.client import VantageClient
from logging import getLogger, config
import time

try:
    with open(os.path.join(module_path, 'finance_client/settings.json'), 'r') as f:
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

try:
    with open(os.path.join(os.path.dirname(__file__), './env.test.json'), 'r') as f:
        env = json.load(f)
except Exception as e:
    print(f"fail to load settings file: {e}")
    raise e

fx = FOREX(env["vantage"]["api_key"], logger)
stock = STOCK(env["vantage"]["api_key"], logger)
digital = DIGITAL(env["vantage"]["api_key"], logger)

client = VantageClient(env["vantage"]["api_key"], symbol=("USD", "JPY"))

class TestVantageClient(unittest.TestCase):
    
    """
    def test_fx_params(self):
        target = vantage.Target.FX
        fname = vantage.Target.to_function_name(target, Frame.MIN5)
        self.assertEqual(fname, "FX_INTRADAY")
        
    def test_fx_get_interday(self):
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
        self.assertEqual("Time Series FX (30min)" in data, True)
        time.sleep(2)
        data = fx.get_interday_rates(from_symbol="USD", to_symbol="JPY", interval=Frame.H1)
        self.assertEqual(type(data), dict)
        self.assertEqual("Time Series FX (60min)" in data, True)
        
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
        df = client.get_rates(-1)
        self.assertIn("close", df)
        self.assertGreater(len(df["close"]), 950)
        
        """
    
    def test_get_rates(self):
        df = client.get_rates(100)
        self.assertIn("close", df)
        self.assertEqual(len(df["close"]), 100)
    
    #check after frame time
        
if __name__ == '__main__':
    unittest.main()