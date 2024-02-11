import datetime
import os
import sys
import unittest

import dotenv

try:
    dotenv.load_dotenv("tests/.env")
except Exception:
    pass

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
print(module_path)
sys.path.append(module_path)

import pandas as pd

import finance_client.frames as Frame
from finance_client import db, fprocess, logger
from finance_client.csv.client import CSVClient
from finance_client.position import ORDER_TYPE

file_base = "L:/data/yfinance"
symbols = ["1333.T", "1332.T", "1605.T", "1963.T", "1812.T", "1801.T", "1928.T", "1802.T", "1925.T", "1808.T", "1803.T", "1721.T"]
datetime_column = "Datetime"
ohlc_columns = ["Open", "High", "Low", "Close"]
additional_column = ["Adj Close", "Volume"]
csv_files = [f"{file_base}/yfinance_{symbol}_D1.csv" for symbol in symbols]
csv_file = f"{file_base}/yfinance_{symbols[0]}_D1.csv"
csv_files_5min = [f"{file_base}/yfinance_{symbol}_MIN5.csv" for symbol in symbols[:2]]
csv_file_5min = csv_files_5min[0]


class TestCSVClient(unittest.TestCase):
    def test_init(self):
        client = CSVClient(logger=logger)

    def test_get_rates(self):
        client = CSVClient(files=csv_file, logger=logger, date_column=datetime_column, start_index=10)
        length = 10
        rates = client.get_ohlc(length)
        self.assertEqual(len(rates["Close"]), length)
        rates = client.get_ohlc()
        self.assertGreater(len(rates["Close"]), length)

    def test_get_next_tick(self):
        client = CSVClient(files=csv_file, logger=logger, date_column=datetime_column)
        df, suc = client.get_next_tick()
        self.assertEqual(type(df), pd.Series)

    def test_get_current_ask(self):
        client = CSVClient(files=csv_file, logger=logger, date_column=datetime_column)
        ask_value = client.get_current_ask()
        print(ask_value)
        self.assertGreater(ask_value, 0)

    def test_get_current_bid(self):
        client = CSVClient(files=csv_file, logger=logger, date_column=datetime_column)
        bid_value = client.get_current_bid()
        print(bid_value)
        self.assertGreater(bid_value, 0)

    def test_get_indicaters(self):
        length = 10
        bband = fprocess.BBANDProcess()
        macd = fprocess.MACDProcess()
        processes = [bband, macd]
        client = CSVClient(files=csv_file, logger=logger, date_column=datetime_column)
        data = client.get_ohlc(length, idc_processes=processes)
        print(data.columns)
        self.assertEqual(len(data[ohlc_columns[3]]), length)

    def test_get_standalized_indicaters(self):
        length = 10
        bband = fprocess.BBANDProcess()
        macd = fprocess.MACDProcess()
        processes = [bband, macd]
        post_prs = [fprocess.DiffPreProcess(), fprocess.MinMaxPreProcess()]
        client = CSVClient(files=csv_file, logger=logger, date_column=datetime_column)
        data = client.get_ohlc(length, idc_processes=processes, pre_processes=post_prs)
        print(data)
        self.assertEqual(len(data[ohlc_columns[3]]), length)

    def test_get_rolled_data(self):
        client = CSVClient(files=csv_file_5min, logger=logger, date_column=datetime_column, out_frame=Frame.MIN30, start_date=10)
        df = client.get_ohlc(length=10)
        self.assertEqual(len(df), 10)
        delta = df.index[1] - df.index[0]
        delta_ex = datetime.timedelta(minutes=30)
        self.assertEqual(delta, delta_ex)

    def test_add_indicaters(self):
        client = CSVClient(
            files=csv_file_5min,
            logger=logger,
            date_column=datetime_column,
            economic_keys=["SP500"],
            start_date=datetime.datetime(2020, 1, 1),
        )
        data = client.get_ohlc(100)
        print(data)

    def get_current_date(self):
        client = CSVClient(
            files=csv_file_5min,
            logger=logger,
            date_column=datetime_column,
            economic_keys=["SP500"],
            start_date=datetime.datetime(2020, 1, 1),
        )
        date_from_function = client.get_current_date()
        data = client.get_ohlc(10)
        date_from_data = data.index[-1]
        self.assertEqual(date_from_data, date_from_function)


class TestCSVClientMulti(unittest.TestCase):
    def test_initialize_with_files(self):
        files = csv_files[:2]
        # client = CSVClient(files=files, out_frame=30)
        # del client
        client = CSVClient(files=files, auto_reset_index=True, logger=logger)
        del client

    def test_get_data_with_files_basic(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]

        client = CSVClient(files=files, logger=logger)
        print("warning is shown")
        df = client.get_ohlc(DATA_LENGTH)
        self.assertEqual(DATA_LENGTH, len(df))
        self.assertGreaterEqual(len(df.columns), len(ohlc_columns) * SYMBOL_COUNT)
        del df
        df = client.get_ohlc()
        self.assertGreater(len(df), 0)
        self.assertGreaterEqual(len(df.columns), len(ohlc_columns) * SYMBOL_COUNT)
        del client, df

        client = CSVClient(files=files, date_column="Datetime", logger=logger)
        print("warning is shown")
        df = client.get_ohlc(DATA_LENGTH)
        self.assertEqual(DATA_LENGTH, len(df))
        self.assertGreaterEqual(len(df.columns), len(ohlc_columns) * SYMBOL_COUNT)
        del df
        df = client.get_ohlc()
        self.assertGreater(len(df), 0)
        self.assertGreaterEqual(len(df.columns), len(ohlc_columns) * SYMBOL_COUNT)

    def test_get_data_with_files_with_limited_columns(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]

        client = CSVClient(files=files, columns=ohlc_columns, logger=logger)
        print("warning is shown")
        df = client.get_ohlc(DATA_LENGTH)
        self.assertEqual(DATA_LENGTH, len(df))
        self.assertEqual(len(df.columns), len(ohlc_columns) * SYMBOL_COUNT)
        del df
        df = client.get_ohlc()
        self.assertGreater(len(df), 0)
        self.assertEqual(len(df.columns), len(ohlc_columns) * SYMBOL_COUNT)
        del client, df

    def test_get_data_with_files_from_specific_index(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        MARGIN_FACTOR = 2

        client = CSVClient(files=files, start_index=None, logger=logger)
        print("warning is shown")
        org_df = client.get_ohlc(DATA_LENGTH * MARGIN_FACTOR)

        client = CSVClient(files=files, start_index=DATA_LENGTH * MARGIN_FACTOR, logger=logger)
        df = client.get_ohlc(DATA_LENGTH)
        for index in range(0, DATA_LENGTH):
            self.assertEqual(df.index[index], org_df.index[DATA_LENGTH * (MARGIN_FACTOR - 1) + index])

    def test_get_data_with_files_from_specific_date(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        START_DATE = datetime.datetime(year=2010, month=4, day=1, tzinfo=datetime.timezone.utc)

        client = CSVClient(files=files, start_date=START_DATE, logger=logger)
        df = client.get_ohlc(DATA_LENGTH)
        self.assertLess(df.index[-2], START_DATE)
        self.assertGreaterEqual(df.index[-1], START_DATE)

    def test_get_data_with_files_with_random_index(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]

        client = CSVClient(files=files, start_random_index=True, seed=100, logger=logger)
        df = client.get_ohlc(DATA_LENGTH)
        first_date = df.index[-1]
        client = CSVClient(files=files, start_random_index=True, seed=200, logger=logger)
        df = client.get_ohlc(DATA_LENGTH)
        second_date = df.index[-1]
        self.assertNotEqual(first_date, second_date)

    def test_get_data_with_files_with_skiprows(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        SKIP_LINES = 3
        files = csv_files[:SYMBOL_COUNT]

        client = CSVClient(files=files, start_index=10, logger=logger)
        df = client.get_ohlc(DATA_LENGTH)
        first_date = df.index[SKIP_LINES - 1]
        client = CSVClient(files=files, start_index=10, skiprows=SKIP_LINES, logger=logger)
        df = client.get_ohlc(DATA_LENGTH)
        second_date = df.index[0]
        self.assertGreater(second_date, first_date)

    def test_get_data_with_indicaters(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        cci = fprocess.CCIProcess(ohlc_column=ohlc_columns)
        # macd = fprocess.MACDProcess(target_column=ohlc_columns[3])
        client = CSVClient(files=files, start_index=DATA_LENGTH * 10, logger=logger)
        df = client.get_ohlc(DATA_LENGTH, idc_processes=[cci])
        self.assertEqual(len(df.columns), SYMBOL_COUNT * (len(ohlc_columns) + len(additional_column) + len(cci.columns)))

    def test_get_data_with_preprocess(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        cci = fprocess.CCIProcess(ohlc_column=ohlc_columns)
        # macd = fprocess.MACDProcess(target_column=ohlc_columns[3])
        client = CSVClient(files=files, start_index=DATA_LENGTH * 10, logger=logger)
        df = client.get_ohlc(DATA_LENGTH, idc_processes=[cci], pre_processes=[fprocess.MinMaxPreProcess(scale=(0, 1))])
        self.assertGreaterEqual(df.min().min(), 0)
        self.assertLessEqual(df.max().max(), 1)

    def test_get_current_value_with_no_slip(self):
        KEY_NONE = "none"

        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]

        client = CSVClient(files=files, slip_type=KEY_NONE, start_index=DATA_LENGTH, logger=logger)
        df = client.get_ohlc(DATA_LENGTH)
        target_symbols = client.symbols
        ask_values = client.get_current_ask(target_symbols)
        bid_values = client.get_current_bid(target_symbols)
        for symbol in target_symbols:
            if pd.isna(ask_values[symbol]) is False:
                self.assertLessEqual(bid_values[symbol], ask_values[symbol])

    def test_get_current_value_with_pct_slip(self):
        KEY_PCT = "pct"
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]

        client = CSVClient(files=files, slip_type=KEY_PCT, start_index=DATA_LENGTH, logger=logger)
        df = client.get_ohlc(DATA_LENGTH)
        target_symbols = client.symbols
        ask_values = client.get_current_ask(target_symbols)
        bid_values = client.get_current_bid(target_symbols)
        for symbol in target_symbols:
            if pd.isna(ask_values[symbol]) is False:
                self.assertLessEqual(bid_values[symbol], ask_values[symbol])

    def test_get_current_value_with_random_slip(self):
        KEY_RDM = "random"
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]

        client = CSVClient(files=files, slip_type=KEY_RDM, start_index=DATA_LENGTH, logger=logger)
        df = client.get_ohlc(DATA_LENGTH)
        target_symbols = client.symbols
        ask_values = client.get_current_ask(target_symbols)
        bid_values = client.get_current_bid(target_symbols)
        for symbol in target_symbols:
            if pd.isna(ask_values[symbol]) is False:
                self.assertLessEqual(bid_values[symbol], ask_values[symbol])

    def test_get_data_with_limited_symbols(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]

        client = CSVClient(files=files, start_index=DATA_LENGTH, logger=logger)
        limited_symbols_1 = client.symbols[:2]
        df_1 = client.get_ohlc(DATA_LENGTH, symbols=limited_symbols_1)
        self.assertEqual(len(df_1.columns), len(limited_symbols_1) * (len(ohlc_columns) + len(additional_column)))

    def test_get_rolled_data(self):
        client = CSVClient(files=csv_files_5min, logger=logger, date_column="Datetime", out_frame=Frame.MIN30, start_index=100)
        df = client.get_ohlc(length=10)
        self.assertEqual(len(df), 10)
        delta = df.index[1] - df.index[0]
        delta_ex = datetime.timedelta(minutes=30)
        self.assertEqual(delta, delta_ex)

    def test_get_rolled_data_with_less_memory(self):
        client = CSVClient(files=csv_files_5min, logger=logger, date_column="Datetime", start_index=100)
        df = client.get_ohlc(length=10, frame=30)
        self.assertEqual(len(df), 10)
        delta = df.index[1] - df.index[0]
        delta_ex = datetime.timedelta(minutes=30)
        self.assertEqual(delta, delta_ex)

    def test_get_rolled_data_twiced(self):
        client = CSVClient(files=csv_files_5min, logger=logger, date_column="Datetime", out_frame=30, start_index=100)
        df = client.get_ohlc(length=10, frame=60)
        self.assertEqual(len(df), 10)
        delta = df.index[1] - df.index[0]
        delta_ex = datetime.timedelta(minutes=60)
        self.assertEqual(delta, delta_ex)


"""
class TestCSVClientMultiChunk(unittest.TestCase):
    def test_initialize_with_file_chunk(self):
        files = csv_files[:2]
        # client = CSVClient(files=files, out_frame=30)
        # del client
        client = CSVChunkClient(files=files, chunksize=50, auto_reset_index=True, logger=logger)
        del client

    def test_get_data_with_chunk_basic(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        CHUNK_SIZE = 50
        files = csv_files[:SYMBOL_COUNT]
        step = 0

        client = CSVChunkClient(files=files, chunksize=CHUNK_SIZE, logger=logger)
        df = client.get_ohlc(DATA_LENGTH)
        self.assertEqual(DATA_LENGTH, len(df))
        self.assertGreaterEqual(len(df.columns), len(ohlc_columns) * SYMBOL_COUNT)
        del df
        df = client.get_ohlc(CHUNK_SIZE + DATA_LENGTH)
        self.assertEqual(CHUNK_SIZE + DATA_LENGTH, len(df))
        self.assertGreaterEqual(len(df.columns), len(ohlc_columns) * SYMBOL_COUNT)
        del df
        df = client.get_ohlc()
        self.assertGreater(len(df), DATA_LENGTH + DATA_LENGTH)
        self.assertGreaterEqual(len(df.columns), len(ohlc_columns) * SYMBOL_COUNT)
        del client, df

        client = CSVChunkClient(files=files, date_column=datetime_column, chunksize=CHUNK_SIZE, logger=logger)
        print("warning is shown")
        df = client.get_ohlc(DATA_LENGTH)
        self.assertEqual(DATA_LENGTH, len(df))
        self.assertGreaterEqual(len(df.columns), len(ohlc_columns) * SYMBOL_COUNT)
        del df
        df = client.get_ohlc()
        self.assertGreater(len(df), 0)
        self.assertGreaterEqual(len(df.columns), len(ohlc_columns) * SYMBOL_COUNT)

    def test_get_entire_data(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        CHUNK_SIZE = 50
        files = csv_files[:SYMBOL_COUNT]

        client = CSVChunkClient(files=files, chunksize=CHUNK_SIZE, logger=logger)
        # need to check the reset function
        # how can i check the end?
        # while True:
        #     df = client.get_ohlc(DATA_LENGTH)

    def test_get_data_with_files_with_limited_columns(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]

        client = CSVChunkClient(files=files, columns=ohlc_columns, chunksize=100, logger=logger)
        print("warning is shown")
        df = client.get_ohlc(DATA_LENGTH)
        self.assertEqual(DATA_LENGTH, len(df))
        self.assertEqual(len(df.columns), len(ohlc_columns) * SYMBOL_COUNT)
        del df
        df = client.get_ohlc()
        self.assertGreater(len(df), 0)
        self.assertEqual(len(df.columns), len(ohlc_columns) * SYMBOL_COUNT)
        del client, df

    def test_get_data_with_files_from_specific_index(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        MARGIN_FACTOR = 2

        client = CSVChunkClient(files=files, start_index=None, chunksize=100, logger=logger)
        print("warning is shown")
        org_df = client.get_ohlc(DATA_LENGTH * MARGIN_FACTOR)

        client = CSVChunkClient(files=files, start_index=DATA_LENGTH * MARGIN_FACTOR, chunksize=100, logger=logger)
        df = client.get_ohlc(DATA_LENGTH)
        for index in range(0, DATA_LENGTH):
            self.assertEqual(df.index[index], org_df.index[DATA_LENGTH * (MARGIN_FACTOR - 1) + index])

    def test_get_data_with_files_from_specific_date(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        START_DATE = datetime.datetime(year=2001, month=4, day=1, tzinfo=datetime.timezone.utc)

        client = CSVChunkClient(files=files, start_date=START_DATE, chunksize=100, logger=logger)
        df = client.get_ohlc(DATA_LENGTH)
        self.assertLess(df.index[-2], START_DATE)
        self.assertGreaterEqual(df.index[-1], START_DATE)

    def test_get_data_with_files_with_random_index(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]

        client = CSVChunkClient(files=files, start_random_index=True, seed=100, chunksize=100, logger=logger)
        df = client.get_ohlc(DATA_LENGTH)
        first_date = df.index[-1]
        client = CSVChunkClient(files=files, start_random_index=True, seed=200, chunksize=100, logger=logger)
        df = client.get_ohlc(DATA_LENGTH)
        second_date = df.index[-1]
        self.assertNotEqual(first_date, second_date)

    def test_get_data_with_files_with_skiprows(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        SKIP_LINES = 3
        files = csv_files[:SYMBOL_COUNT]

        client = CSVChunkClient(files=files, start_index=10, chunksize=100, logger=logger)
        df = client.get_ohlc(DATA_LENGTH)
        first_date = df.index[SKIP_LINES - 1]
        client = CSVChunkClient(files=files, start_index=10, skiprows=SKIP_LINES, chunksize=100, logger=logger)
        df = client.get_ohlc(DATA_LENGTH)
        second_date = df.index[0]
        self.assertGreater(second_date, first_date)

    def test_get_data_with_indicaters(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        cci = fprocess.CCIProcess(ohlc_column=ohlc_columns)
        # macd = fprocess.MACDProcess(target_column=ohlc_columns[3])
        client = CSVChunkClient(files=files, start_index=DATA_LENGTH * 10, chunksize=100, logger=logger)
        df = client.get_ohlc(DATA_LENGTH, idc_processes=[cci])
        self.assertEqual(len(df.columns), SYMBOL_COUNT * (len(ohlc_columns) + len(additional_column) + len(cci.columns)))

    def test_get_data_with_preprocess(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]
        cci = fprocess.CCIProcess(ohlc_column=ohlc_columns)
        # macd = fprocess.MACDProcess(target_column=ohlc_columns[3])
        client = CSVChunkClient(files=files, start_index=DATA_LENGTH * 10, chunksize=100, logger=logger)
        df = client.get_ohlc(DATA_LENGTH, idc_processes=[cci], pre_processes=[fprocess.MinMaxPreProcess(scale=(0, 1))])
        self.assertGreaterEqual(df.min().min(), 0)
        self.assertLessEqual(df.max().max(), 1)

    def test_get_data_with_limited_symbols(self):
        SYMBOL_COUNT = 3
        DATA_LENGTH = 10
        files = csv_files[:SYMBOL_COUNT]

        client = CSVChunkClient(files=files, start_index=DATA_LENGTH, chunksize=100, logger=logger)
        limited_symbols_1 = client.symbols[:2]
        df_1 = client.get_ohlc(DATA_LENGTH, symbols=limited_symbols_1)
        self.assertEqual(len(df_1.columns), len(limited_symbols_1) * (len(ohlc_columns) + len(additional_column)))


class TestCSVClientMultiWOInit(unittest.TestCase):
    def test_get_datas_with_file_generator(self):
        generator = lambda symbol: f"{file_base}/yfinance_{symbol}_D1.csv"
        client = CSVClient(file_name_generator=generator, logger=logger)
        ohlc_df = client.get_ohlc(length=10, symbols=symbols[:2])
        self.assertEqual(len(ohlc_df), 10)

    def test_get_datas_with_file_generator(self):
        pass
        # generator = lambda symbol: f'{file_base}/yfinance_{symbol}_D1.csv'
        # client = CSVClient(file_name_generator=generator, logger=logger)
        # ohlc_df = client.get_ohlc(length=10, symbols=symbols[3])
        # self.assertEqual(len(ohlc_df), 10)
"""


class TestCCSVClientMultiTrade(unittest.TestCase):
    def test_trade_symbol(self):
        storage = db.FileStorage(provider="csv_multi_1")
        files = csv_files[:2]
        target_symbols = symbols[:2]
        client = CSVClient(files=files, start_index=10, symbols=target_symbols, auto_step_index=True, logger=logger, storage=storage)
        client.open_trade(True, 1, symbols[1])

        for i in range(0, 5):
            df = client.get_ohlc(10)
        results = client.close_long_positions(symbols[1])
        self.assertEqual(len(results[0]), 5)
        self.assertNotEqual(results[0][0], results[0][1])

    def test_trade_symbols(self):
        storage = db.FileStorage(provider="csv_multi_2")
        files = csv_files[:3]
        target_symbols = symbols[:3]
        client = CSVClient(files=files, start_index=10, symbols=target_symbols, auto_step_index=True, logger=logger, storage=storage)
        client.open_trade(True, 1, symbols[1])

        for i in range(0, 5):
            df = client.get_ohlc(10)
        client.open_trade(True, 1, symbols[2])
        for i in range(0, 5):
            df = client.get_ohlc(10)
        results = client.close_long_positions(symbols[2])
        self.assertEqual(len(results), 1)
        self.assertNotEqual(results[0][0], results[0][1])
        for i in range(0, 5):
            df = client.get_ohlc(10)
        self.assertEqual(len(results), 1)
        self.assertNotEqual(results[0][0], results[0][1])


"""

class TestCSVClientMultiChunkWOInit:
    pass


class TestCSVClientMultiWOInit:
    pass


class TestCSVClientMultiChunkWOInit:
    pass

"""

if __name__ == "__main__":
    unittest.main()
