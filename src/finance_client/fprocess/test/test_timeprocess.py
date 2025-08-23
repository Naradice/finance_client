import os
import sys
import unittest

import numpy as np
import pandas as pd

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(module_path)

from fprocess import timeprocess


class TestTimeProcess(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        df = pd.read_csv(
            os.path.join(os.path.dirname(__file__), "sample_data.csv"),
            index_col=0,
            parse_dates=True,
        )
        self.org_data = df.groupby(pd.Grouper(freq="30MIN")).first()
        self.ohlc_columns = ["open", "high", "low", "close"]
        super().__init__(methodName)

    def test_WeeklyIDProcess(self):
        wid_process = timeprocess.WeeklyIDProcess(freq=30)
        id_df = wid_process.run(self.org_data)
        id_df

    def test_SinProcess(self):
        sin_process = timeprocess.SinProcess(freq=60 * 24, time_column="index", amplifier=1)
        id_df = sin_process(self.org_data)
        self.assertLessEqual(id_df["index"].max(), 1)
        self.assertGreaterEqual(id_df["index"].min(), -1)


if __name__ == "__main__":
    unittest.main()
