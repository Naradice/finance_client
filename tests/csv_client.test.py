import unittest, os, json, sys, datetime
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
print(module_path)
sys.path.append(module_path)

from finance_client.csv.client import CSVClient
from finance_client.frames import Frame
from finance_client import utils
from logging import getLogger, config

try:
    with open(os.path.join(module_path, 'finance_client/settings.json'), 'r') as f:
        settings = json.load(f)
except Exception as e:
    print(f"fail to load settings file: {e}")
    raise e
logger_config = settings["log"]
log_file_base_name = logger_config["handlers"]["fileHandler"]["filename"]
log_path = f'./{log_file_base_name}_csvclienttest_{datetime.datetime.utcnow().strftime("%Y%m%d%H")}.logs'
logger_config["handlers"]["fileHandler"]["filename"] = log_path
config.dictConfig(logger_config)
logger = getLogger("finance_client.test")

csv_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data_source/bitcoin_5_2017T0710-2021T103022.csv'))

class TestCSVClient(unittest.TestCase):
    client = CSVClient(file=csv_file, logger=logger)
    
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
        client  = CSVClient(file=csv_file, out_frame=30, logger=logger)
        rates = client.get_rates(length)
        self.assertEqual(len(rates.Close), length)
    
    def test_get_indicaters(self):
        length = 10
        bband = utils.BBANDpreProcess()
        macd = utils.MACDpreProcess()
        processes = [bband, macd]
        client = CSVClient(file=csv_file, out_frame=Frame.MIN5, idc_processes=processes, logger=logger)
        data = client.get_rate_with_indicaters(length)
        print(data.columns)
        self.assertEqual(len(data.Close), length)
        
    def test_get_indicaters(self):
        length = 10
        bband = utils.BBANDpreProcess()
        macd = utils.MACDpreProcess()
        processes = [bband, macd]
        client = CSVClient(file=csv_file, out_frame=Frame.MIN5, idc_processes=processes, logger=logger)
        data = client.get_rate_with_indicaters(length)
        print(data.columns)
        self.assertEqual(len(data.Close), length)
        
    def test_get_standalized_indicaters(self):
        length = 10
        bband = utils.BBANDpreProcess()
        macd = utils.MACDpreProcess()
        processes = [bband, macd]
        post_prs = [utils.DiffPreProcess(), utils.MinMaxPreProcess()]
        client = CSVClient(file=csv_file, out_frame=Frame.MIN5, idc_processes=processes, post_process=post_prs ,logger=logger)
        data = client.get_rate_with_indicaters(length)
        print(data)
        self.assertEqual(len(data.Close), length)
        
    def test_get_diffs_minmax(self):
        client = CSVClient(file=csv_file, out_frame=Frame.MIN5, logger=logger)
        result = client.open_trade(is_buy=True, amount=1, order_type="Market",symbol="USDJPY")
        client.get_next_tick()
        diffs = client.get_diffs()
        diffs_mm = client.get_diffs_with_minmax()
    
if __name__ == '__main__':
    unittest.main()