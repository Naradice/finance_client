import os
import sys
import unittest

import pandas as pd

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(module_path)

from finance_client.fprocess.fprocess import ohlc


class TestStandalizationUtils(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        ohlc_columns = ["Open", "High", "Low", "Close"]
        symbols = ["symbol_0", "symbol_1"]
        multi_symbols_column = pd.MultiIndex.from_arrays(
            [
                [symbol for symbol in symbols for _ in ohlc_columns],
                [column for _ in symbols for column in ohlc_columns],
            ]
        )

        revert_columns = multi_symbols_column.swaplevel(0, 1)

        sample_data = [[0, 1, 0, 1], [3, 4, 3, 4], [-1, 0, -1, 0], [1, 0, 1, 0]]
        self.symbol_df = pd.DataFrame(sample_data, columns=ohlc_columns)
        sample_data = [
            [0, 1, 0, 1, 10, 20, 10, 20],
            [3, 4, 3, 4, 30, 40, 30, 40],
            [-1, 0, -1, 0, 10, 10, 10, 10],
            [1, 0, 1, 0, 20, 10, 20, 10],
        ]
        self.multi_symbol_df = pd.DataFrame(sample_data, columns=multi_symbols_column)
        self.multi_rvt_symbol_df = pd.DataFrame(sample_data, columns=revert_columns)

    def test_is_grouped_by_symbol(self):
        is_groupd_by = ohlc.is_grouped_by_symbol(self.multi_symbol_df.columns)
        self.assertTrue(is_groupd_by)
        is_groupd_by = ohlc.is_grouped_by_symbol(self.multi_symbol_df)
        self.assertTrue(is_groupd_by)
        is_not_grouped_by = ohlc.is_grouped_by_symbol(self.multi_rvt_symbol_df.columns)
        self.assertFalse(is_not_grouped_by)
        is_not_grouped_by = ohlc.is_grouped_by_symbol(self.multi_rvt_symbol_df)
        self.assertFalse(is_not_grouped_by)

    def test_get_columns_of_symbol(self):
        columns = ohlc.get_columns_of_symbol(self.symbol_df)
        self.assertEqual(len(columns), 4)

        columns = ohlc.get_columns_of_symbol(self.multi_symbol_df)
        self.assertEqual(len(columns), 4)

        columns = ohlc.get_columns_of_symbol(self.multi_rvt_symbol_df)
        self.assertEqual(len(columns), 4)

    def test_get_ohlc_columns(self):
        columns_dict_1 = ohlc.get_ohlc_columns(self.symbol_df)
        self.assertEqual(len(columns_dict_1), 4)
        self.assertIn(ohlc.OPEN, columns_dict_1)

        columns_dict_2 = ohlc.get_ohlc_columns(self.multi_symbol_df)
        self.assertEqual(len(columns_dict_2), 4)
        self.assertIn(ohlc.OPEN, columns_dict_2)

        columns_dict_3 = ohlc.get_ohlc_columns(self.multi_symbol_df)
        self.assertEqual(len(columns_dict_3), 4)
        self.assertIn(ohlc.OPEN, columns_dict_3)


if __name__ == "__main__":
    unittest.main()
