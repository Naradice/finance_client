import unittest
from finance_client.csv.client import CSVClient
from finance_client.frames import Frame
from finance_client import utils

class TestCSVClient(unittest.TestCase):

    client = CSVClient(file='../data_source/bitcoin_5_2017T0710-2021T103022.csv')
    
    def test_get_rates(self):
        length = 10
        rates = self.client.get_rates(length)
        self.assertEqual(len(rates.Close), length)

    def test_get_next_tick(self):
        print(self.client.get_next_tick())
    
    def test_get_current_ask(self):
        print(self.client.get_current_ask())

    def test_get_current_bid(self):
        print(self.client.get_current_bid())
        
    def test_get_30min_rates(self):
        length = 10
        client  = CSVClient(file='../data_source/bitcoin_5_2017T0710-2021T103022.csv', out_frame=30)
        rates = client.get_rates(length)
        self.assertEqual(len(rates.Close), length)
    
    def test_get_indicaters(self):
        length = 10
        bband = utils.BBANDpreProcess()
        macd = utils.MACDpreProcess()
        processes = [bband, macd]
        client = CSVClient(file='../data_source/bitcoin_5_2017T0710-2021T103022.csv', out_frame=Frame.MIN5, idc_processes=processes)
        data = client.get_rate_with_indicaters(length)
        print(data.columns)
        self.assertEqual(len(data.Close), length)
    
if __name__ == '__main__':
    unittest.main()