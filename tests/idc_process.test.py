import unittest, os, json, sys, datetime
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
print(module_path)
sys.path.append(module_path)

import datetime

from finance_client.csv.client import CSVClient
from finance_client.mt5.client import MT5Client
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
log_path = f'./{log_file_base_name}_indicaters_{datetime.datetime.utcnow().strftime("%Y%m%d%H")}.logs'
logger_config["handlers"]["fileHandler"]["filename"] = log_path
config.dictConfig(logger_config)
logger = getLogger("finance_client.test")


file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data_source/bitcoin_5_2017T0710-2021T103022.csv'))
class TestIndicaters(unittest.TestCase):
    """
    def test_renko_process(self):
        client = CSVClient(file=file_path, logger=logger)
        prc = utils.RenkoProcess(window=120)
        column = prc.columns["NUM"]
        client.add_indicater(prc)
        start_time = datetime.datetime.now()
        data = client.get_rate_with_indicaters(300)
        end_time = datetime.datetime.now()
        logger.debug(f"renko process took {end_time - start_time}")
        self.assertEqual(column in data.columns, True)
        self.assertEqual(len(data[column]), 300)
    
    def test_slope_process(self):
        client = CSVClient(file=file_path, logger=logger)
        prc = utils.SlopeProcess(window=5)
        slp_column = prc.columns["Slope"]
        client.add_indicater(prc)
        start_time = datetime.datetime.now()
        data = client.get_rate_with_indicaters(100)
        end_time = datetime.datetime.now()
        logger.debug(f"slope process took {end_time - start_time}")
        self.assertEqual(slp_column in data.columns, True)
        self.assertEqual(len(data[slp_column]), 100)
    """
    def test_macdslope_process(self):
        client = CSVClient(file=file_path, logger=logger)
        macd = utils.MACDpreProcess()
        macd_column = macd.columns["MACD"]
        signal_column = macd.columns["Signal"]
        prc = utils.SlopeProcess(key="m",window=5, target_column=macd_column)
        s_prc = utils.SlopeProcess(key="s",window=5, target_column=signal_column)
        slp_column = prc.columns["Slope"]
        s_slp_column = s_prc.columns["Slope"]
        client.add_indicaters([macd, prc, s_prc])
        start_time = datetime.datetime.now()
        data = client.get_rate_with_indicaters(100)
        end_time = datetime.datetime.now()
        logger.debug(f"macd slope process took {end_time - start_time}")
        self.assertEqual(slp_column in data.columns, True)
        self.assertEqual(len(data[slp_column]), 100)
        self.assertEqual(s_slp_column in data.columns, True)
        self.assertEqual(len(data[s_slp_column]), 100)
        self.assertNotEqual(data[slp_column].iloc[-1], data[s_slp_column].iloc[-1])
    
if __name__ == '__main__':
    unittest.main()