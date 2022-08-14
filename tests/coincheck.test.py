import unittest, os, json, sys, datetime
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
print(module_path)
sys.path.append(module_path)
from finance_client.coincheck.apis.ws import TradeHistory, Orders
import time

class TestCCAPI(unittest.TestCase):
    
    def log(self, data):
        print(f"unit_test: {data}")
        
    
    def test_subscribe(self):
        ws_th = TradeHistory(debug=True)
        ws_th.subscribe(on_tick=self.log)
        time.sleep(60)
        ws_th.close()
        
    """
    def test_orders_subscribe(self):
        ws_th = Orders(debug=True)
        ws_th.subscribe()
        time.sleep(60)
        ws_th.close()
    """
                
if __name__ == '__main__':
    unittest.main()