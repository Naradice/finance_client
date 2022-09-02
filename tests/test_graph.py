import unittest, os, json, sys, datetime
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(module_path)

from finance_client.render.graph import Rendere
from finance_client.csv.client import CSVClient
from finance_client import utils
import time
import pandas as pd

csv_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '../finance_client/data_source/mt5/OANDA-Japan MT5 Live/mt5_USDJPY_d1.csv'))

class TestRender(unittest.TestCase):
    
    def test_plot_ohlc(self):
        r = Rendere()
        for i in range(5):
            r.add_subplot()
        data = {
            'Open': [10000,10000,10000,10000],
            'High': [11000,12000,13000,14000],
            'Low': [7000, 7000, 7000,7000],
            'Close': [9000, 9000, 9000, 9000]
        }
        df = pd.DataFrame(data)
        r.register_ohlc(df,4)
        r.plot()
        time.sleep(5)
        
    def test_plot_ohlc_from_csv_client(self):
        r = Rendere()
        client = CSVClient(file=csv_file, columns = ['high', 'low','open','close'], date_column="time")
        df = client.get_rates(30)
        r.register_ohlc(df)
        r.plot()
        time.sleep(10)
        
    def test_plot_bband_from_client_out(self):
        r = Rendere()
        bban = utils.BBANDpreProcess(target_column='close', window=14)
        client = CSVClient(file=csv_file, columns = ['high', 'low','open','close'], date_column="time", idc_processes=[bban])
        df = client.get_rate_with_indicaters(30)
        index = r.register_ohlc_with_indicaters(df, [bban])
        r.plot()
        time.sleep(10)
        
if __name__ == '__main__':
    unittest.main()