from time import sleep
import unittest, os, json, sys, datetime
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
print(module_path)
sys.path.append(module_path)
from finance_client.yfinance.client import YahooClient

try:
    with open(os.path.join(os.path.dirname(__file__), './env.test.json'), 'r') as f:
        env = json.load(f)
except Exception as e:
    print(f"fail to load settings file: {e}")
    raise e

class TestYFClient(unittest.TestCase):
    frame = 1
    client = YahooClient("1812.T",auto_step_index=True, frame=frame)
        
    def test_get_rates(self):
        length = 10
        frame = 1
        rates = self.client.get_rates(length)
        self.assertEqual(len(rates.Close), length)
        
    def test_orders(self):
        id = self.client.market_buy('1812.T', 1000, 0.005, None,None,None)
        sleep(10)
        self.client.sell_for_settlment('1812.T', 1005, 0.005, None, id)
    
    # def test_get_next_tick(self):
    #     print(self.client.get_next_tick())
        
if __name__ == '__main__':
    unittest.main()