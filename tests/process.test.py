import unittest, os, json, sys, datetime
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
print(module_path)
sys.path.append(module_path)

from finance_client.csv.client import CSVClient
from finance_client.frames import Frame
from finance_client import utils
from logging import getLogger, config

import datetime

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
    client = CSVClient(file=csv_file, logger=logger, auto_index=False)
    
    def test_cci(self):
        ohlc = self.client.get_rates(100)
        cci = utils.indicaters.CommodityChannelIndex(ohlc)
        self.assertEqual(len(cci), 100)
        self.assertLess(cci.max(), 300)
        self.assertGreater(cci.min(), -300)
        
    def test_cci_process(self):
        ohlc = self.client.get_rates(110)
        cci_prc = utils.CCIProcess(14)
        input_data_1 = ohlc.iloc[:100].copy()
        self.assertEqual(len(input_data_1), 100)
        cci = cci_prc.run(input_data_1)
        
        next_tick = ohlc.iloc[100]
        next_cci = cci_prc.update(next_tick)
        next_cci_from_run = cci_prc.run(ohlc.iloc[:101])
        cci_column = cci_prc.columns["CCI"]
        ans_value = next_cci_from_run[cci_column].iloc[-1]
        self.assertEqual(next_cci, ans_value)
        
    
if __name__ == '__main__':
    unittest.main()