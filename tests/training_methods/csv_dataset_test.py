import datetime
import json
import os
import sys
import unittest
from logging import config, getLogger

import pandas as pd

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
print(module_path)
sys.path.append(module_path)

from finance_client import utils
from finance_client.csv.client import CSVClient

try:
    with open(os.path.join(module_path, "finance_client/settings.json"), "r") as f:
        settings = json.load(f)
except Exception as e:
    print(f"fail to load settings file: {e}")
    raise e

logger_config = settings["log"]
log_file_base_name = logger_config["handlers"]["fileHandler"]["filename"]
log_path = os.path.abspath(f'./{log_file_base_name}_training_test_{datetime.datetime.utcnow().strftime("%Y%m%d")}.log')
logger_config["handlers"]["fileHandler"]["filename"] = log_path
config.dictConfig(logger_config.copy())
logger = getLogger("finance_client.test")

file_base = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../finance_client/data_source/yfinance"))
symbols = ["1333.T", "1332.T", "1605.T", "1963.T", "1812.T", "1801.T", "1928.T", "1802.T", "1925.T", "1808.T", "1803.T", "1721.T"]
datetime_column = "Time"
ohlc_columns = ["Open", "High", "Low", "Close"]
additional_column = ["Volume"]
csv_files = [f"{file_base}/yfinance_{symbol}_D1.csv" for symbol in symbols]
csv_file = os.path.abspath("L:/data/csv/USDJPY_forex_min30.csv")

csv_files_5min = [f"{file_base}/yfinance_{symbol}_MIN5.csv" for symbol in symbols[:2]]
csv_file_5min = os.path.abspath("L:/data/csv/USDJPY_forex_min5.csv")


class TestDataset:
    def __init__(self, client, length) -> None:
        self.client = client

    def _get_output(self, idx):
        columns = None
        if type(idx) is tuple:
            columns = idx[1]
            idx = idx[0]

        if type(idx) is slice:
            start = idx.start + self.client.observation_length
            end = idx.stop + self.client.observation_length
            out_idx = slice(start, end)
        elif type(idx) is int:
            out_idx = idx + self.client.observation_length

        return self.client[out_idx, columns]

    def __getitem__(self, idx):
        return self.client[idx], self._get_output(idx)

    def __len__(self):
        return len(self.client.indices)


class TestCSVClient(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.length = 30
        self.single_client = CSVClient(
            files=csv_file, observation_length=self.length, logger=logger, date_column=datetime_column, start_index=self.length
        )

        rsi_process = utils.RSIProcess(ohlc_column_name=ohlc_columns)
        self.rsi_key = rsi_process.KEY_RSI
        self.single_tec_client = CSVClient(
            files=csv_file,
            observation_length=self.length,
            logger=logger,
            date_column=datetime_column,
            start_index=self.length,
            idc_process=[rsi_process],
        )
        
        utils.DiffPreProcess()

    def test_single_dataset(self):
        single_dataset = TestDataset(self.single_client, self.length)
        batch_size = 16
        feature_size = len(ohlc_columns)
        for index in range(0, len(single_dataset), batch_size):
            if index + batch_size > len(single_dataset):
                break
            src, tgt = single_dataset[index : index + batch_size]
            if src.shape[0] != batch_size:
                print("strange")
            self.assertEqual(src.shape, (batch_size, self.length, feature_size), f"test_signal_dataset failed at {index}")

    def test_single_dataset_with_column(self):
        single_dataset = TestDataset(self.single_client, self.length)
        batch_size = 16
        target_column = ohlc_columns[3]
        for index in range(0, len(single_dataset), batch_size):
            if index + batch_size > len(single_dataset):
                break
            src, tgt = single_dataset[index : index + batch_size, target_column]
            self.assertEqual(src.shape, (batch_size, self.length, 1))

    def test_signal_dataset_with_tec_indicater(self):
        single_dataset = TestDataset(self.single_tec_client, self.length)
        batch_size = 16
        target_column = self.rsi_key
        for index in range(0, len(single_dataset), batch_size):
            if index + batch_size > len(single_dataset):
                break
            src, tgt = single_dataset[index : index + batch_size, target_column]
            self.assertEqual(src.shape, (batch_size, self.length, 1))


if __name__ == "__main__":
    unittest.main()
