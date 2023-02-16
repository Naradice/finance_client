import os
import sys
import unittest

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(module_path)

import pandas as pd

import finance_client.utils.standalization as std


class TestStandalizationUtils(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        self.window = 4
        self.input = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20]
        self.scale = (-1, 1)
        super().__init__(methodName)

    def test_minimax_array(self):
        input = self.input
        mm_inputs, _, _ = std.mini_max_from_array(input, scale=self.scale)
        self.assertEqual(self.scale[0], mm_inputs[0])
        self.assertEqual(self.scale[1], mm_inputs[-1])

    def test_minimax_vealue(self):
        input = self.input
        _min = min(input)
        _max = max(input)

        scaled, _, _ = std.mini_max(input[0], _min, _max, self.scale)
        self.assertEqual(scaled, self.scale[0])
        scaled, _, _ = std.mini_max(input[-1], _min, _max)
        self.assertEqual(scaled, self.scale[-1])

    def test_revert_minimax(self):
        input = self.input
        _min = min(input)
        _max = max(input)

        scaled, _, _ = std.mini_max(input[0], _min, _max, self.scale)
        row = std.revert_mini_max(scaled, _min, _max, self.scale)
        self.assertEqual(row, input[0])

        scaled, _, _ = std.mini_max(input[-3], _min, _max, self.scale)
        row = std.revert_mini_max(scaled, _min, _max, self.scale)
        self.assertEqual(row, input[-3])

    def test_mini_max_series(self):
        input = pd.Series(self.input)
        mm_inputs, _min, _max = std.mini_max_from_series(input, self.scale)
        self.assertEqual(self.scale[0], mm_inputs.iloc[0])
        self.assertEqual(self.scale[1], mm_inputs.iloc[-1])

    def test_revert_mini_max_series(self):
        input = pd.Series(self.input)
        mm_inputs, _min, _max = std.mini_max_from_series(input, self.scale)
        rows = std.revert_mini_max_from_series(mm_inputs, _min, _max, self.scale)
        self.assertEqual(rows.iloc[0], input.iloc[0])
        self.assertEqual(rows.iloc[-3], input.iloc[-3])

    def test_revert_mini_max_row_series(self):
        input = pd.Series(data=[100, 102, 98, 101], index=["open", "high", "low", "close"])
        options = {"open": (80, 120), "high": (80, 120), "low": (80, 120), "close": (80, 120)}
        scaled, _min, _max = std.mini_max_from_row_series(input, options)
        reverted = std.revert_mini_max_from_row_series(scaled, options)
        for index in range(0, len(input)):
            self.assertEqual(input[index], reverted[index])


if __name__ == "__main__":
    unittest.main()
