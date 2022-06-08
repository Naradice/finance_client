import unittest
from finance_client.coincheck.client import CoinCheckClient

class TestCCClient(unittest.TestCase):

    client = CoinCheckClient()
    
    def test_get_rates(self):
        length = 10
        rates = self.client.get_rates(length)
        #self.assertEqual(len(rates.Close), length)
    
    def test_get_next_tick(self):
        print(self.client.get_next_tick())
        
if __name__ == '__main__':
    unittest.main()