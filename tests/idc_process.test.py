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

import datetime
import pandas as pd

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
    client = CSVClient(file=file_path, logger=logger, auto_index=False)
    
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
        
    def test_macd(self):
        ##input
        ds = pd.DataFrame({'close':[120.000 + i*0.1 for i in range(30)]})
        ##ans
        short_ema = ds['close'].ewm(span=12, adjust=False).mean()
        long_ema = ds['close'].ewm(span=26, adjust=False).mean()
        macd = short_ema - long_ema
        signal = macd.rolling(9).mean()
        
        process = utils.MACDpreProcess(option={'column':'close'})
        macd_dict = process.run(ds)
        short_column = process.columns["S_EMA"]
        long_column = process.columns["L_EMA"]
        macd_column = process.columns["MACD"]
        signal_column = process.columns["Signal"]
        
        self.assertEqual(macd_dict[short_column].iloc[-1], short_ema.iloc[-1])
        self.assertEqual(macd_dict[long_column].iloc[-1], long_ema.iloc[-1])
        self.assertEqual(macd_dict[macd_column].iloc[-1], macd.iloc[-1])
        self.assertEqual(macd_dict[signal_column].iloc[-1], signal.iloc[-1])
    
    def test_macd_update(self):
        ### prerequisites
        ds = pd.DataFrame({'close':[120.000 + i*0.1 for i in range(30)]})
        process = utils.MACDpreProcess(option={'column':'close'})
        macd_dict = process.run(ds)
        
        ###input
        new_value = 120.000 + 30*0.1
        new_data = pd.Series({'close': new_value})
        
        ###output
        new_indicater_series = process.update(new_data)
        test_data = process.concat(ds, new_indicater_series)
        
        ###expect
        ex_data_org = process.concat(ds, new_data)
        another_ps = utils.MACDpreProcess(option={'column':'close'})
        macd_dict = another_ps.run(ex_data_org)
        
        short_column = process.columns["S_EMA"]
        long_column = process.columns["L_EMA"]
        macd_column = process.columns["MACD"]
        signal_column = process.columns["Signal"]
        
        self.assertEqual(macd_dict[short_column].iloc[-1], test_data[short_column].iloc[-1])
        self.assertEqual(macd_dict[long_column].iloc[-1], test_data[long_column].iloc[-1])
        self.assertEqual(macd_dict[macd_column].iloc[-1], test_data[macd_column].iloc[-1])
        self.assertEqual(macd_dict[signal_column].iloc[-1], test_data[signal_column].iloc[-1])
        
    def test_ema_process(self):
        window = 4
        input = [1,2,3,4,5,6,7,8,9,10,11,12,13,20]
        out_ex = [input[0]]
        alpha = 2/(1 + window)
        
        for i in range(1, len(input)):
            out_ex.append(out_ex[i -1] * (1 - alpha) + input[i]*alpha)
    
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

    def test_min_max(self):
        import random
        open = [random.random()*123 for index in range(100)]
        close = [o_value + random.random() -0.5 for o_value in open]
        ds = pd.DataFrame({'close':close, 'open': open} )
        mm = utils.MinMaxPreProcess(scale=(-1,1))
        
        result = mm.run(ds)
        self.assertTrue(len(result['close']) == 100)
        self.assertTrue(result['close'].min() >= -1)
        self.assertTrue(result['close'].max() <= 1)
        self.assertTrue(result['open'].min() >= -1)
        self.assertTrue(result['open'].max() <= 1)
        
        new_open_value = ds['open'].max() + random.random() + 0.1
        new_close_value =  new_open_value + random.random() - 0.5
        
        new_data = pd.Series({'close':new_close_value, 'open': new_open_value} )
        new_data_standalized = mm.update(new_data)
        new_ds = mm.concat(pd.DataFrame(result), new_data_standalized)
        self.assertTrue(new_data_standalized['open'] == 1)
        self.assertTrue(len(new_ds['close']) == 101)
        self.assertTrue(new_ds['close'].min() >= -1)
        self.assertTrue(new_ds['close'].max() <= 1)
        self.assertTrue(new_ds['open'].min() >= -1)
        self.assertTrue(new_ds['open'].max() <= 1)
    
if __name__ == '__main__':
    unittest.main()