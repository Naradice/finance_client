import os
import sys
import unittest

import pandas as pd

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(module_path)

from fprocess import convert


class TestConvert(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        self.org_data = pd.read_csv(
            os.path.join(os.path.dirname(__file__), "sample_data.csv"),
            index_col=0,
            parse_dates=True,
        )
        self.ohlc_columns = ["open", "high", "low", "close"]
        super().__init__(methodName)

    def test_dropna(self):
        test_df = self.org_data.groupby(pd.Grouper(level=0, freq="1D")).first()
        length_with_nan = len(test_df)
        dropped_df = convert.dropna_market_close(test_df)
        length_wo_nan = len(dropped_df)
        self.assertLess(length_wo_nan, length_with_nan)


if __name__ == "__main__":
    unittest.main()
