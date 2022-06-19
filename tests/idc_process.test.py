from tkinter import E
import unittest, os, json, sys, datetime
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
print(module_path)
sys.path.append(module_path)

import datetime

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
log_path = f'./{log_file_base_name}_idcprocess_{datetime.datetime.utcnow().strftime("%Y%m%d%H")}.logs'
logger_config["handlers"]["fileHandler"]["filename"] = log_path
config.dictConfig(logger_config)
logger = getLogger("finance_client.test")


file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data_source/bitcoin_5_2017T0710-2021T103022.csv'))
class TestIndicaters(unittest.TestCase):
    client = CSVClient(file=file_path, logger=logger)
    
    def test_renko(self):
        df = self.client.get_rates(100)
        try:
            utils.indicaters.renko_from_ohlc(df)
        except Exception as e:
            if "less than atr_length" not in str(e):
                raise e
            else:
                logger.info(f"error happened as expected: {e}")
        df = self.client.get_rates(120)
        renko = utils.indicaters.renko_from_ohlc(df)
        self.assertLessEqual(len(renko["bar_num"]), 120)
        
        df = self.client.get_rates(100)
        renko = utils.indicaters.renko_from_ohlc(df, atr_length=80)
        self.assertLessEqual(len(renko["bar_num"]), 80)
        
    def test_renko_timescale(self):
        df = self.client.get_rates(1000)
        start_time = datetime.datetime.now()
        renko = utils.indicaters.renko_time_scale(df)
        end_time = datetime.datetime.now()
        logger.debug(f"renko_time_scale took {end_time - start_time}")
        self.assertEqual(len(renko["bar_num"]) ,1000)
        
    def test_slope(self):
        data_length = 1000
        df = self.client.get_rates(data_length)
        start_time = datetime.datetime.now()
        slope_df = utils.indicaters.slope(df["Close"], window=10)
        end_time = datetime.datetime.now()
        logger.debug(f"slope took {end_time - start_time}")
        self.assertEqual(len(slope_df["slope"]) ,data_length)
    
if __name__ == '__main__':
    unittest.main()