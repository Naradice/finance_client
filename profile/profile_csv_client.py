import cProfile
import json
import os
import pstats
import sys

import numpy as np
import pandas as pd

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
print(module_path)
sys.path.append(module_path)

import finance_client.frames as Frame
from finance_client.csv.client import CSVClient

try:
    with open(os.path.join(module_path, "finance_client/settings.json"), "r") as f:
        settings = json.load(f)
except Exception as e:
    print(f"fail to load settings file: {e}")
    raise e

file_base = os.path.abspath(os.path.join(os.path.dirname(__file__), "../finance_client/data_source/yfinance"))
symbols = ["1333.T", "1332.T", "1605.T", "1963.T", "1812.T", "1801.T", "1928.T", "1802.T", "1925.T", "1808.T", "1803.T", "1721.T"]
datetime_column = "Time"
ohlc_columns = ["Open", "High", "Low", "Close"]
additional_column = ["Volume"]
csv_files = [f"{file_base}/yfinance_{symbol}_D1.csv" for symbol in symbols]
csv_file = os.path.abspath("L:/data/csv/USDJPY_forex_min30.csv")

csv_files_5min = [f"{file_base}/yfinance_{symbol}_MIN5.csv" for symbol in symbols[:2]]
csv_file_5min = os.path.abspath("L:/data/csv/USDJPY_forex_min5.csv")


class TestDataset:
    def __init__(self, client) -> None:
        self.client = client

    def _get_output(self, idx):
        if type(idx) is slice:
            start = idx.start + self.client.observation_length
            end = idx.stop + self.client.observation_length
            out_idx = slice(start, end)
        else:
            out_idx = idx + self.client.observation_length

        return self.client[out_idx]

    def __getitem__(self, idx):
        return self.client[idx], self._get_output(idx)

    def __len__(self):
        return len(self.client)


def run_single_dataset():
    length = 30
    client = CSVClient(files=csv_file, observation_length=length, date_column=datetime_column, start_index=length)
    single_dataset = TestDataset(client)

    def getitem_wrapper(idx):
        src, tgt = single_dataset[idx]
        return src, tgt

    batch_size = 16
    for index in range(0, len(client), batch_size):
        if index + batch_size > len(client):
            break
        idx = slice(index, index + batch_size)
        src, tgt = getitem_wrapper(idx)


def run_direct_method():
    df = pd.read_csv(csv_file, index_col=0, parse_dates=True)

    def getitem_wrapper(index, batch_size, length):
        chunk_src_data = []
        chunk_tgt_data = []

        for internal_index in range(index, index + batch_size):
            src = df.iloc[internal_index - length : internal_index].values
            tgt = df.iloc[internal_index : internal_index + length].values

        chunk_src_data.append(src)
        chunk_tgt_data.append(tgt)

        src = np.array(chunk_src_data)
        tgt = np.array(chunk_tgt_data)
        return src, tgt

    batch_size = 16
    length = 10
    for index in range(length, len(df), batch_size):
        src, tgt = getitem_wrapper(index, batch_size, length)


if __name__ == "__main__":
    cProfile.run("run_single_dataset()", "csv_profile_stats.dat")
    p = pstats.Stats("csv_profile_stats.dat")
    p.strip_dirs().sort_stats(-1).print_stats()

    # cProfile.run(
    #     "run_direct_method()",
    # )
