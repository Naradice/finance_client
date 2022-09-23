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
    
    def test_get_rates_with_indicater(self):
        from finance_client.utils.idcprocess import MACDpreProcess, RangeTrendProcess, BBANDpreProcess
        macd_p = MACDpreProcess(short_window=12, long_window=26, signal_window=9, target_column="Close")
        macd_column = macd_p.columns["MACD"]
        bband_process = BBANDpreProcess(target_column="Close", alpha=2)
        rtp_p = RangeTrendProcess(slope_window=3)
        range_column = rtp_p.columns[rtp_p.RANGE_KEY]
        client = YahooClient('1812.T',auto_step_index=True, frame=5, idc_processes=[macd_p, bband_process, rtp_p])
        data = client.get_rate_with_indicaters(10)
        self.assertTrue(macd_column in data.columns)
        self.assertTrue(range_column in data.columns)
    
    # def test_get_next_tick(self):
    #     print(self.client.get_next_tick())
        
if __name__ == '__main__':
    unittest.main()