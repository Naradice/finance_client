from time import sleep
import unittest, os, json, sys, datetime
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
print(module_path)
sys.path.append(module_path)
from finance_client.coincheck.client import CoinCheckClient
from finance_client.coincheck.apis.ws import TradeHistory
import finance_client.vantage.target as target
import finance_client.vantage.client as vclient

try:
    with open(os.path.join(os.path.dirname(__file__), './env.test.json'), 'r') as f:
        env = json.load(f)
except Exception as e:
    print(f"fail to load settings file: {e}")
    raise e

class TestCCClient(unittest.TestCase):
    
    def test_initialize(self):
        frame = 1
        vantage_client = vclient.VantageClient(api_key=env["vantage"]["api_key"], frame=frame, finance_target=target.CRYPTO_CURRENCY, symbol=('BTC', 'JPY'))
        client = CoinCheckClient(frame=frame, initialized_with=vantage_client)
        df = client.get_rates(100)
        self.assertEqual(len(df["open"]), 100)
        
        time_test_df = df.copy()
        time_test_df["test_time"] = time_test_df.index.copy()
        time_diffs = time_test_df["test_time"].diff().iloc[1:]# index 0 is numpy.Null
        condition = time_diffs != datetime.timedelta(minutes=frame)
        condition = condition.values
        result = time_diffs[condition]
        self.assertEqual(len(result), 0)
        
    def test_orders(self):
        client = CoinCheckClient(env["cc"]["ACCESS_ID"], env["cc"]["ACCESS_SECRET"], frame=1)
        id = client.market_buy('BTCJPY', 2952000, 0.005, None,None,None)
        sleep(120)
        client.sell_for_settlment('BTCJPY', 2960000, 0.005, None, id)
        
    # def test_get_rates(self):
    #     length = 10
    #     rates = self.client.get_rates(length)
    #     #self.assertEqual(len(rates.Close), length)
    
    # def test_get_next_tick(self):
    #     print(self.client.get_next_tick())
        
if __name__ == '__main__':
    unittest.main()