from time import sleep
import unittest, os, json, sys, datetime

import numpy
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(module_path)

from finance_client.mt5 import MT5Client
import finance_client.frames as Frame
from finance_client import utils
from logging import getLogger, config
import dotenv
dotenv.load_dotenv(".env")

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

id = int(os.environ["mt5_id"])
simulation = True
client = MT5Client(id=id, password=os.environ["mt5_password"], server=os.environ["mt5_server"],auto_index=False, simulation=simulation, frame=Frame.MIN5, logger=logger)

class TestMT5Client(unittest.TestCase):
    
    def test_get_current_ask(self):
        ask_value = client.get_current_ask()
        self.assertEqual(type(ask_value), float)
        
    def test_get_current_bid(self):
        bid_value = client.get_current_bid()
        self.assertEqual(type(bid_value), float)
        
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
        macd_p = utils.MACDProcess(target_column=close_column)
        macd_column = macd_p.columns["MACD"]
        client.add_indicater(macd_p)
        data = client.get_rate_with_indicaters(100)
        self.assertEqual(macd_column in data.columns, True)
        self.assertEqual(len(data[macd_column]), 100)
        
    """ takes few minutes
    
    def test_auto_index_5min(self):
        "check when frame time past during run"
        client = MT5Client(id=id, password=os.environ["mt5_password"], server=os.environ["mt5_server"],auto_index=True, simulation=True, frame=Frame.MIN5, logger=logger, simulation_seed=1111)
        
        count = 0
        next_time = None
        while count < 6:
            data = client.get_rates(30)
            if next_time != None:
                current_time = data['time'].iloc[0]
                self.assertEqual(current_time, next_time)
            next_time = data['time'].iloc[1]
            sleep(60)
            count += 1
    
        
    def test_auto_index_1min(self):
        "check when wait time is longer than frame"
        client = MT5Client(id=id, password=os.environ["mt5_password"], server=os.environ["mt5_server"],auto_index=True, simulation=True, frame=Frame.MIN1, logger=logger)
        
        count = 0
        next_time = None
        while count < 3:
            data = client.get_rates(30)
            if next_time != None:
                current_time = data['time'].iloc[0]
                self.assertEqual(current_time, next_time)
            next_time = data['time'].iloc[1]
            sleep(120)
            count += 1
    
    def test_auto_index_H2(self):
        "check when week change"
        client = MT5Client(id=id, password=os.environ["mt5_password"], server=os.environ["mt5_server"],auto_index=True, simulation=True, frame=Frame.H2, logger=logger, simulation_seed=1111)
        
        count = 0
        next_time = None
        while count < 12*7:
            data = client.get_rates(30)
            if next_time != None:
                current_time = data['time'].iloc[0]
                self.assertEqual(current_time, next_time)
            next_time = data['time'].iloc[1]
            sleep(1)
            count += 1
    
    def test_auto_index_5min_with_indicaters(self):
        client = MT5Client(id=id, password=os.environ["mt5_password"], server=os.environ["mt5_server"],auto_index=True, simulation=True, frame=Frame.MIN5, logger=logger)
        
        count = 0
        previouse_time = None
        while count < 6:
            data = client.get_rates(30)
            if previouse_time != None:
                current_time = data['time'].iloc[0]
                delta = current_time - previouse_time
                self.assertEqual(int(delta.total_seconds()), 60*Frame.MIN5)
                previouse_time = current_time
            else:
                previouse_time = data['time'].iloc[0]
            sleep(60)
            count += 1
    
    def test_get_data_by_queue(self):
        count = 0
        columns = client.get_ohlc_columns()
        close_column = columns["Close"]
        q = client.get_data_queue(10)
        test = True
        while test:
            start = datetime.datetime.now()
            data = q.get()
            end = datetime.datetime.now()
            self.assertEqual(len(data[close_column]), 10)
            delta = (end - start)
            print(delta.total_seconds())
            count += 1
            if count > 5:
                test = False
                break
    """

    def test_get_all_rates(self):
        rates = client.get_rates()
        self.assertNotEqual(type(rates), type(None))
        
    
if __name__ == '__main__':
    unittest.main()