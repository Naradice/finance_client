import datetime
import json
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

from finance_client import fprocess, logger
from finance_client.csv.client import CSVClient

datetime_column = "time"
ohlc_columns = ["open", "high", "low", "close"]
csv_file = os.path.abspath("L:/data/fx/OANDA-Japan MT5 Live/mt5_USDJPY_min5.csv")


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

        rsi_process = fprocess.RSIProcess(ohlc_column_name=ohlc_columns)
        self.rsi_key = rsi_process.KEY_RSI
        self.single_tec_client = CSVClient(
            files=csv_file,
            observation_length=self.length,
            logger=logger,
            date_column=datetime_column,
            start_index=self.length,
            idc_process=[rsi_process],
        )
        self.feature_size = len(ohlc_columns) + len(rsi_process.columns)

        diff_process = fprocess.DiffPreProcess(columns=ohlc_columns)
        mm_process = fprocess.MinMaxPreProcess()
        self.single_std_client = CSVClient(
            files=csv_file,
            observation_length=self.length,
            logger=logger,
            date_column=datetime_column,
            start_index=self.length,
            pre_process=[diff_process, mm_process],
        )

    def test_single_dataset(self):
        single_dataset = TestDataset(self.single_client, self.length)
        batch_size = 16
        for index in range(0, len(single_dataset), batch_size):
            if index + batch_size > len(single_dataset):
                break
            src, tgt = single_dataset[index : index + batch_size]
            if src.shape[0] != batch_size:
                print("strange")
            self.assertEqual(src.shape, (batch_size, self.length, self.feature_size), f"test_signal_dataset failed at {index}")

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
