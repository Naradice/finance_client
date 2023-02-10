import inspect
import os
import pandas as pd
import sys
import unittest

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(module_path)

from finance_client.utils import validation


csv_file = os.path.abspath('L:/data/csv/USDJPY_forex_min30.csv')
df = pd.read_csv(csv_file, parse_dates=True, index_col=0)
OPEN_WEEKDAY = 0
CLOSE_WEEKDAY = 4
FREQ = 30

class TestValidation(unittest.TestCase):
    
    def test_get_weekly_count(self):
        sample_column = df.columns[0]
        daily_df = df[sample_column].groupby(pd.Grouper(level=0, freq="1D")).first()
        daily_df.dropna(inplace=True)
        weekday_df = validation.get_weekly_count(df)
        self.assertEqual(len(daily_df), weekday_df.sum())
        
    def __check_forex_open_close_date(self, open_wd, close_wd):
        caller = inspect.currentframe().f_back.f_code.co_name
        self.assertEqual(open_wd, OPEN_WEEKDAY, msg=f"open value is invalid: {open_wd}. called by {caller}")
        self.assertEqual(close_wd, CLOSE_WEEKDAY, msg=f"close value is invalid: {close_wd}. called by {caller}")
    
    def test_get_open_close_weekday(self):
        open_wd, close_wd = validation.get_open_close_weekday(df)
        self.__check_forex_open_close_date(open_wd, close_wd)
        weekday_df = validation.get_weekly_count(df)
        open_wd, close_wd = validation.get_open_close_weekday(weekday_df.index)
        self.__check_forex_open_close_date(open_wd, close_wd)
        local_maket_index = [0, 1, 2, 3, 4, 5]#may happen if timestamp is different from market
        open_wd, close_wd = validation.get_open_close_weekday(local_maket_index)
        self.assertEqual(open_wd, local_maket_index[0])
        self.assertEqual(close_wd, local_maket_index[-1])
    
    def test_error_get_open_close_weekday(self):
        open_wd, close_wd = validation.get_open_close_weekday([0, 1, 2, 3, 4, 5, 6])
        self.assertTrue(open_wd is None)
        self.assertTrue(close_wd is None)
        open_wd, close_wd = validation.get_open_close_weekday([])
        self.assertTrue(open_wd is None)
        self.assertTrue(close_wd is None)
        
    def test_frequence_count(self):
        freq_count_series = validation.frequence_count(df)
        self.assertEqual(type(freq_count_series), pd.Series)
        self.assertEqual(type(freq_count_series.index), pd.TimedeltaIndex)
        exp_delta = pd.Timedelta(minutes=FREQ)
        self.assertEqual(freq_count_series.index[0], exp_delta)

    def test_frequence_count_by_column(self):
        datetime_df = df.index.to_frame()
        datetime_column = datetime_df.columns[0]
        freq_count_series = validation.frequence_count(datetime_df, datetime_column)
        self.assertEqual(type(freq_count_series), pd.Series)
        self.assertEqual(type(freq_count_series.index), pd.TimedeltaIndex)
        exp_delta = pd.Timedelta(minutes=FREQ)
        self.assertEqual(freq_count_series.index[0], exp_delta)
    
    def test_get_most_frequent_delta(self):
        freq = validation.get_most_frequent_delta(df)
        self.assertEqual(freq, FREQ)
    
    def test_get_start_end_time(self):
        start_srs, end_srs = validation.get_start_end_time(df)
        self.assertEqual(type(start_srs), pd.DataFrame)
        self.assertEqual(type(end_srs), pd.DataFrame)
        self.assertEqual(len(start_srs), 5)
        self.assertEqual(len(end_srs), 5)
        
    def test_weekly_summary(self):
        weekly_summary_df = validation.weekly_summary(df)
        print(weekly_summary_df)
        self.assertEqual(len(weekly_summary_df), 5)
        
if __name__ == '__main__':
    unittest.main()