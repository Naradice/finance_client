import unittest, os, json, sys, datetime
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
print(module_path)
sys.path.append(module_path)

from finance_client.csv.client import CSVClient, CSVChunkClient
import finance_client.frames as Frame
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

csv_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '../finance_client/data_source/csv/USDJPY_forex_min5.csv'))
time_column = "Time"

"""
class TestCSVClient(unittest.TestCase):
    client = CSVClient(files=csv_file, logger=logger, date_column=time_column)
    
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
        client  = CSVClient(files=csv_file, frame=Frame.MIN5, logger=logger, date_column=time_column)
        rates = client.get_rates(length)
        self.assertEqual(len(rates.Close), length)
    
    def test_get_indicaters(self):
        length = 10
        bband = utils.BBANDProcess()
        macd = utils.MACDProcess()
        processes = [bband, macd]
        client = CSVClient(files=csv_file, frame=Frame.MIN5, idc_processes=processes, logger=logger, date_column=time_column)
        data = client.get_rate_with_indicaters(length)
        print(data.columns)
        self.assertEqual(len(data.Close), length)
        
    def test_get_indicaters(self):
        length = 10
        bband = utils.BBANDProcess()
        macd = utils.MACDProcess()
        processes = [bband, macd]
        client = CSVClient(files=csv_file, frame=Frame.MIN5, idc_processes=processes, logger=logger, date_column=time_column)
        data = client.get_rate_with_indicaters(length)
        print(data.columns)
        self.assertEqual(len(data.Close), length)
        
    def test_get_standalized_indicaters(self):
        length = 10
        bband = utils.BBANDProcess()
        macd = utils.MACDProcess()
        processes = [bband, macd]
        post_prs = [utils.DiffPreProcess(), utils.MinMaxPreProcess()]
        client = CSVClient(files=csv_file, frame=Frame.MIN5, idc_processes=processes, post_process=post_prs ,logger=logger, date_column=time_column)
        data = client.get_rate_with_indicaters(length)
        print(data)
        self.assertEqual(len(data.Close), length)
        
    def test_get_diffs_minmax(self):
        client = CSVClient(files=csv_file, frame=Frame.MIN5, logger=logger, date_column=time_column)
        diffs = client.get_diffs()
        self.assertEqual(sum(diffs), 0)
        diffs_mm = client.get_diffs_with_minmax()
        self.assertEqual(sum(diffs_mm), 0)
        result = client.open_trade(is_buy=True, amount=1, order_type="Market",symbol="USDJPY")
        client.get_next_tick()
        diffs = client.get_diffs()
        diffs_mm = client.get_diffs_with_minmax()
        self.assertEqual(type(diffs), list)
        self.assertNotEqual(sum(diffs), 0)
        self.assertEqual(type(diffs_mm), list)
        self.assertNotEqual(sum(diffs_mm), 0)
"""

file_base = os.path.abspath(os.path.join(os.path.dirname(__file__), '../finance_client/data_source/yfinance'))
symbols = ['1333.T', '1332.T', '1605.T', '1963.T', '1812.T', '1801.T', '1928.T', '1802.T', '1925.T', '1808.T', '1803.T', '1721.T']
datetime_column = "Datetime"
ohlc_columns = ["Open", "High", "Low", "Close"]
additional_column = ["Adj Close", "Volume"]
csv_files = [f'{file_base}/yfinance_{symbol}_D1.csv' for symbol in symbols]

class TestCSVClientMulti(unittest.TestCase):
    
    def test_initialize_with_files(self):
        files = csv_files[:2]
        #client = CSVClient(files=files, out_frame=30)
        #del client
        client = CSVClient(files=files, auto_reset_index=True)
        del client
        
    def test_get_data_with_files_basic(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        
        client = CSVClient(files=files)
        print("warning is shown")
        df = client.get_ohlc(DATA_LENGTH)
        self.assertEqual(DATA_LENGTH, len(df))
        self.assertGreaterEqual(len(df.columns), len(ohlc_columns)*SYMBOL_COUNT)
        del df
        df = client.get_ohlc()
        self.assertGreater(len(df), 0)
        self.assertGreaterEqual(len(df.columns), len(ohlc_columns)*SYMBOL_COUNT)
        del client, df
        
        client = CSVClient(files=files, date_column=datetime_column)
        print("warning is shown")
        df = client.get_ohlc(DATA_LENGTH)
        self.assertEqual(DATA_LENGTH, len(df))
        self.assertGreaterEqual(len(df.columns), len(ohlc_columns)*SYMBOL_COUNT)
        del df
        df = client.get_ohlc()
        self.assertGreater(len(df), 0)
        self.assertGreaterEqual(len(df.columns), len(ohlc_columns)*SYMBOL_COUNT)
    
    def test_get_data_with_files_with_limited_columns(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        
        client = CSVClient(files=files, columns=ohlc_columns)
        print("warning is shown")
        df = client.get_ohlc(DATA_LENGTH)
        self.assertEqual(DATA_LENGTH, len(df))
        self.assertEqual(len(df.columns), len(ohlc_columns)*SYMBOL_COUNT)
        del df
        df = client.get_ohlc()
        self.assertGreater(len(df), 0)
        self.assertEqual(len(df.columns), len(ohlc_columns)*SYMBOL_COUNT)
        del client, df
        
    def test_get_data_with_files_from_specific_index(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        MARGIN_FACTOR = 2
        
        client = CSVClient(files=files, start_index=None)
        print("warning is shown")
        org_df = client.get_ohlc(DATA_LENGTH*MARGIN_FACTOR)
        
        client = CSVClient(files=files, start_index=DATA_LENGTH*MARGIN_FACTOR)
        df = client.get_ohlc(DATA_LENGTH)
        for index in range(0, DATA_LENGTH):
            self.assertEqual(df.index[index], org_df.index[DATA_LENGTH *(MARGIN_FACTOR-1) + index])
    
    def test_get_data_with_files_from_specific_date(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        START_DATE=datetime.datetime(year=2001, month=4, day=1, tzinfo=datetime.timezone.utc)
        
        client = CSVClient(files=files, start_date=START_DATE)
        df = client.get_ohlc(DATA_LENGTH)
        self.assertLess(df.index[-2], START_DATE)
        self.assertGreaterEqual(df.index[-1], START_DATE)
        
    def test_get_data_with_files_with_random_index(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        
        client = CSVClient(files=files, start_random_index=True, seed=100)
        df = client.get_ohlc(DATA_LENGTH)
        first_date = df.index[-1]
        client = CSVClient(files=files, start_random_index=True, seed=200)
        df = client.get_ohlc(DATA_LENGTH)
        second_date = df.index[-1]
        self.assertNotEqual(first_date, second_date)

    def test_get_data_with_files_with_skiprows(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        SKIP_LINES = 3
        files = csv_files[:SYMBOL_COUNT]
        
        client = CSVClient(files=files, start_index=10)
        df = client.get_ohlc(DATA_LENGTH)
        first_date = df.index[SKIP_LINES-1]
        client = CSVClient(files=files, start_index=10, skiprows=SKIP_LINES)
        df = client.get_ohlc(DATA_LENGTH)
        second_date = df.index[0]
        self.assertGreater(second_date, first_date)

    def test_get_data_with_indicaters(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        cci = utils.CCIProcess(ohlc_column=ohlc_columns)
        #macd = utils.MACDProcess(target_column=ohlc_columns[3])
        client = CSVClient(files=files, start_index=DATA_LENGTH*10)
        df = client.get_ohlc(DATA_LENGTH, idc_processes=[cci])
        self.assertEqual(len(df.columns), SYMBOL_COUNT * (len(ohlc_columns) + len(additional_column) + len(cci.columns)))

    def test_get_data_with_preprocess(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        cci = utils.CCIProcess(ohlc_column=ohlc_columns)
        #macd = utils.MACDProcess(target_column=ohlc_columns[3])
        client = CSVClient(files=files, start_index=DATA_LENGTH*10)
        df = client.get_ohlc(DATA_LENGTH, idc_processes=[cci], pre_processes=[utils.MinMaxPreProcess(scale=(0,1))])
        self.assertGreaterEqual(df.min().min(), 0)
        self.assertLessEqual(df.max().max(), 1)

    def test_get_current_value_with_no_slip(self):
        KEY_NONE = "none"
        
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        
        client = CSVClient(files=files, slip_type=KEY_NONE, start_index=DATA_LENGTH)
        df = client.get_ohlc(DATA_LENGTH)
        __symbols = symbols[:SYMBOL_COUNT][1:-2]
        open_values = df[[df[(symbol_, ohlc_columns[0])] for symbol_ in __symbols]].iloc[-1]
        ask_values = client.get_current_ask()
        bid_values = client.get_current_bid()
        for symbol in __symbols:
            self.assertEqual(open_values[symbol], ask_values[symbol])
            self.assertEqual(open_values[symbol], bid_values[symbol])
        
    def test_get_current_value_with_pct_slip(self):
        KEY_PCT = "pct"
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        
        client = CSVClient(files=files, slip_type=KEY_PCT, start_index=DATA_LENGTH)
        df = client.get_ohlc(DATA_LENGTH)
        __symbols = symbols[:SYMBOL_COUNT][1:-2]
        open_values = df[[df[(symbol_, ohlc_columns[0])] for symbol_ in __symbols]].iloc[-1]
        ask_values = client.get_current_ask()
        bid_values = client.get_current_bid()
        for symbol in __symbols:
            self.assertGreater(open_values[symbol], ask_values[symbol])
            self.assertLess(open_values[symbol], bid_values[symbol])
    
    def test_get_current_value_with_random_slip(self):
        KEY_RDM = "random"
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        
        client = CSVClient(files=files, slip_type=KEY_RDM, start_index=DATA_LENGTH)
        df = client.get_ohlc(DATA_LENGTH)
        __symbols = symbols[:SYMBOL_COUNT][1:-2]
        open_values = df[[df[(symbol_, ohlc_columns[0])] for symbol_ in __symbols]].iloc[-1]
        ask_values = client.get_current_ask()
        bid_values = client.get_current_bid()
        for symbol in __symbols:
            self.assertGreater(open_values[symbol], ask_values[symbol])
            self.assertLess(open_values[symbol], bid_values[symbol])

    def test_get_data_with_limited_symbols(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        
        client = CSVClient(files=files, start_index=DATA_LENGTH)
        limited_symbols_1 = client.symbols[:2]
        df_1 = client.get_ohlc(DATA_LENGTH, symbols=limited_symbols_1)
        self.assertEqual(len(df_1.columns), len(limited_symbols_1)*(len(ohlc_columns) + len(additional_column)) )

class TestCSVClientMultiChunk(unittest.TestCase):
    
    def test_initialize_with_file_chunk(self):
        files = csv_files[:2]
        #client = CSVClient(files=files, out_frame=30)
        #del client
        client = CSVChunkClient(files=files, chunksize=50, auto_reset_index=True)
        del client
        
    def test_get_data_with_chunk_basic(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        CHUNK_SIZE = 50
        files = csv_files[:SYMBOL_COUNT]
        step = 0
        
        client = CSVChunkClient(files=files, chunksize=CHUNK_SIZE)
        df = client.get_ohlc(DATA_LENGTH)
        self.assertEqual(DATA_LENGTH, len(df))
        self.assertGreaterEqual(len(df.columns), len(ohlc_columns)*SYMBOL_COUNT)
        del df
        df = client.get_ohlc(CHUNK_SIZE + DATA_LENGTH)
        self.assertEqual(CHUNK_SIZE + DATA_LENGTH, len(df))
        self.assertGreaterEqual(len(df.columns), len(ohlc_columns)*SYMBOL_COUNT)
        del df
        df = client.get_ohlc()
        self.assertGreater(len(df), DATA_LENGTH + DATA_LENGTH)
        self.assertGreaterEqual(len(df.columns), len(ohlc_columns)*SYMBOL_COUNT)
        del client, df
        
        client = CSVChunkClient(files=files, date_column=datetime_column, chunksize=CHUNK_SIZE)
        print("warning is shown")
        df = client.get_ohlc(DATA_LENGTH)
        self.assertEqual(DATA_LENGTH, len(df))
        self.assertGreaterEqual(len(df.columns), len(ohlc_columns)*SYMBOL_COUNT)
        del df
        df = client.get_ohlc()
        self.assertGreater(len(df), 0)
        self.assertGreaterEqual(len(df.columns), len(ohlc_columns)*SYMBOL_COUNT)

    def test_get_entire_data(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        CHUNK_SIZE = 50
        files = csv_files[:SYMBOL_COUNT]
        
        client = CSVChunkClient(files=files, chunksize=CHUNK_SIZE)
        # need to check the reset function
        #how can i check the end?
        # while True:
        #     df = client.get_ohlc(DATA_LENGTH)
    
    def test_get_data_with_files_with_limited_columns(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        
        client = CSVChunkClient(files=files, columns=ohlc_columns, chunksize=100)
        print("warning is shown")
        df = client.get_ohlc(DATA_LENGTH)
        self.assertEqual(DATA_LENGTH, len(df))
        self.assertEqual(len(df.columns), len(ohlc_columns)*SYMBOL_COUNT)
        del df
        df = client.get_ohlc()
        self.assertGreater(len(df), 0)
        self.assertEqual(len(df.columns), len(ohlc_columns)*SYMBOL_COUNT)
        del client, df
        
    def test_get_data_with_files_from_specific_index(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        MARGIN_FACTOR = 2
        
        client = CSVChunkClient(files=files, start_index=None, chunksize=100)
        print("warning is shown")
        org_df = client.get_ohlc(DATA_LENGTH*MARGIN_FACTOR)
        
        client = CSVChunkClient(files=files, start_index=DATA_LENGTH*MARGIN_FACTOR, chunksize=100)
        df = client.get_ohlc(DATA_LENGTH)
        for index in range(0, DATA_LENGTH):
            self.assertEqual(df.index[index], org_df.index[DATA_LENGTH *(MARGIN_FACTOR-1) + index])
    
    def test_get_data_with_files_from_specific_date(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        START_DATE=datetime.datetime(year=2001, month=4, day=1)
        
        client = CSVChunkClient(files=files, start_date=START_DATE, chunksize=100)
        df = client.get_ohlc(DATA_LENGTH)
        start_date_utc = START_DATE.astimezone(tz=datetime.timezone.utc)
        self.assertLess(df.index[-2], start_date_utc)
        self.assertGreaterEqual(df.index[-1], start_date_utc)
        
    def test_get_data_with_files_with_random_index(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        
        client = CSVChunkClient(files=files, start_random_index=True, seed=100, chunksize=100)
        df = client.get_ohlc(DATA_LENGTH)
        first_date = df.index[-1]
        client = CSVChunkClient(files=files, start_random_index=True, seed=200, chunksize=100)
        df = client.get_ohlc(DATA_LENGTH)
        second_date = df.index[-1]
        self.assertNotEqual(first_date, second_date)

    def test_get_data_with_files_with_skiprows(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        SKIP_LINES = 3
        files = csv_files[:SYMBOL_COUNT]
        
        client = CSVChunkClient(files=files, start_index=10, chunksize=100)
        df = client.get_ohlc(DATA_LENGTH)
        first_date = df.index[SKIP_LINES-1]
        client = CSVChunkClient(files=files, start_index=10, skiprows=SKIP_LINES, chunksize=100)
        df = client.get_ohlc(DATA_LENGTH)
        second_date = df.index[0]
        self.assertGreater(second_date, first_date)

    def test_get_data_with_indicaters(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        cci = utils.CCIProcess(ohlc_column=ohlc_columns)
        #macd = utils.MACDProcess(target_column=ohlc_columns[3])
        client = CSVChunkClient(files=files, start_index=DATA_LENGTH*10, chunksize=100)
        df = client.get_ohlc(DATA_LENGTH, idc_processes=[cci])
        self.assertEqual(len(df.columns), SYMBOL_COUNT * (len(ohlc_columns) + len(additional_column) + len(cci.columns)))

    def test_get_data_with_preprocess(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        cci = utils.CCIProcess(ohlc_column=ohlc_columns)
        #macd = utils.MACDProcess(target_column=ohlc_columns[3])
        client = CSVChunkClient(files=files, start_index=DATA_LENGTH*10, chunksize=100)
        df = client.get_ohlc(DATA_LENGTH, idc_processes=[cci], pre_processes=[utils.MinMaxPreProcess(scale=(0,1))])
        self.assertGreaterEqual(df.min().min(), 0)
        self.assertLessEqual(df.max().max(), 1)

    def test_get_current_value_with_no_slip(self):
        KEY_NONE = "none"
        
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        
        client = CSVChunkClient(files=files, slip_type=KEY_NONE, start_index=DATA_LENGTH, chunksize=100)
        df = client.get_ohlc(DATA_LENGTH)
        __symbols = symbols[:SYMBOL_COUNT][1:-2]
        open_values = df[[df[(symbol_, ohlc_columns[0])] for symbol_ in __symbols]].iloc[-1]
        ask_values = client.get_current_ask()
        bid_values = client.get_current_bid()
        for symbol in __symbols:
            self.assertEqual(open_values[symbol], ask_values[symbol])
            self.assertEqual(open_values[symbol], bid_values[symbol])
        
    def test_get_current_value_with_pct_slip(self):
        KEY_PCT = "pct"
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        
        client = CSVChunkClient(files=files, slip_type=KEY_PCT, start_index=DATA_LENGTH, chunksize=100)
        df = client.get_ohlc(DATA_LENGTH)
        __symbols = symbols[:SYMBOL_COUNT][1:-2]
        open_values = df[[df[(symbol_, ohlc_columns[0])] for symbol_ in __symbols]].iloc[-1]
        ask_values = client.get_current_ask()
        bid_values = client.get_current_bid()
        for symbol in __symbols:
            self.assertGreater(open_values[symbol], ask_values[symbol])
            self.assertLess(open_values[symbol], bid_values[symbol])
    
    def test_get_current_value_with_random_slip(self):
        KEY_RDM = "random"
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        
        client = CSVChunkClient(files=files, slip_type=KEY_RDM, start_index=DATA_LENGTH, chunksize=100)
        df = client.get_ohlc(DATA_LENGTH)
        __symbols = symbols[:SYMBOL_COUNT][1:-2]
        open_values = df[[df[(symbol_, ohlc_columns[0])] for symbol_ in __symbols]].iloc[-1]
        ask_values = client.get_current_ask()
        bid_values = client.get_current_bid()
        for symbol in __symbols:
            self.assertGreater(open_values[symbol], ask_values[symbol])
            self.assertLess(open_values[symbol], bid_values[symbol])

    def test_get_data_with_limited_symbols(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        
        client = CSVChunkClient(files=files, start_index=DATA_LENGTH, chunksize=100)
        limited_symbols_1 = client.symbols[:2]
        df_1 = client.get_ohlc(DATA_LENGTH, symbols=limited_symbols_1)
        self.assertEqual(len(df_1.columns), len(limited_symbols_1)*(len(ohlc_columns) + len(additional_column)) )

"""
class TestCSVClientMultiWOInit():
    pass

class TestCSVClientMultiChunkWOInit():
    pass

class TestCSVClientMultiWOInitChunk():
    pass

class TestCSVClientMultiWOInit():
    pass

class TestCSVClientMultiChunkWOInit():
    pass

"""
if __name__ == '__main__':
    unittest.main()