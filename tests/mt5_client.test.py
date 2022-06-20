import unittest, os, json, sys, datetime
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
print(module_path)
sys.path.append(module_path)

from finance_client.mt5 import MT5Client
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
log_path = f'./{log_file_base_name}_mt5clienttest_{datetime.datetime.utcnow().strftime("%Y%m%d%H")}.logs'
logger_config["handlers"]["fileHandler"]["filename"] = log_path
config.dictConfig(logger_config)
logger = getLogger("finance_client.test")

try:
    with open(os.path.join(os.path.dirname(__file__), './env.test.json'), 'r') as f:
        env = json.load(f)
except Exception as e:
    print(f"fail to load settings file: {e}")
    raise e

mt5_settings = env['mt5']
id = int(mt5_settings["id"])
client = MT5Client(id=id, password=mt5_settings["password"], server=mt5_settings["server"], frame=Frame.MIN5, logger=logger)

class TestMT5Client(unittest.TestCase):
    
    def test_get_rates(self):
        data = client.get_rates(100)
        columns = client.get_ohlc_columns()
        close_column = columns["Close"]
        self.assertEqual(len(data[close_column]), 100)
        time_column = columns["Time"]
        self.assertEqual(time_column in data.columns, True)
        time_sr = data[time_column]
        first = time_sr.iloc[0]
        last = time_sr.iloc[-1]
        self.assertGreater(last, first)#last > first
        
    def test_get_rate_with_indicaters(self):
        columns = client.get_ohlc_columns()
        close_column = columns["Close"]
        macd_p = utils.MACDpreProcess(target_column=close_column)
        macd_column = macd_p.columns["MACD"]
        client.add_indicater(macd_p)
        data = client.get_rate_with_indicaters(100)
        self.assertEqual(macd_column in data.columns, True)
        self.assertEqual(len(data[macd_column]), 100)
        
    
if __name__ == '__main__':
    unittest.main()