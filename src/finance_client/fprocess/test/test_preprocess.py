import os
import sys
import unittest

import numpy as np
import pandas as pd

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(module_path)

from fprocess import preprocess


class TestPreProcess(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        self.org_data = pd.read_csv(
            os.path.join(os.path.dirname(__file__), "sample_data.csv"),
            index_col=0,
            parse_dates=True,
        )
        self.ohlc_columns = ["open", "high", "low", "close"]
        super().__init__(methodName)

    def test_diff_process(self):
        periods = 1
        dprorcess = preprocess.DiffPreProcess(periods=periods, columns=self.ohlc_columns)
        diff_data = dprorcess(self.org_data)
        diff_exp = self.org_data.diff(periods)
        self.assertEqual(len(self.org_data.columns), len(diff_data.columns))
        self.assertTrue((self.org_data.columns == diff_data.columns).all())

        sample_column = self.org_data.columns[0]
        for i in range(0, len(diff_exp)):
            process_value = diff_data[sample_column].iloc[i]
            exp_value = diff_exp[sample_column].iloc[i]
            if np.isnan(process_value) and np.isnan(exp_value):
                continue
            self.assertTrue(process_value == exp_value, f"{process_value} != {exp_value} on {i}")

    def test_diff_with_columns(self):
        periods = 1
        dprorcess = preprocess.DiffPreProcess(periods=periods, columns=["close"])
        diff_data = dprorcess(self.org_data)
        self.assertEqual(len(self.org_data.columns), len(diff_data.columns))
        self.assertTrue((self.org_data.columns == diff_data.columns).all())

    def test_revert_diff(self):
        periods = 1
        dprorcess = preprocess.DiffPreProcess(periods=periods)
        diff_data = dprorcess(self.org_data)
        r_data = dprorcess.revert(diff_data)
        sample_column = self.org_data.columns[0]
        for i in range(0, len(self.org_data)):
            process_value = r_data[sample_column].iloc[i]
            exp_value = self.org_data[sample_column].iloc[i]
            self.assertTrue(process_value == exp_value, f"{process_value} != {exp_value} on {i}")

    def test_id_process(self):
        MIN_VALUE_DIGITS = 2
        MAX_VALUE_DIGITS = 3
        DECIMAL_DIGITS = 3
        idprocess = preprocess.IDPreProcess(columns=["open", "close"], decimals=-DECIMAL_DIGITS)
        id_df = idprocess(self.org_data)
        self.assertEqual(len(self.org_data.columns), len(id_df.columns))
        self.assertTrue((self.org_data.columns == id_df.columns).all())
        self.assertEqual(len(self.org_data), len(id_df))
        # print(id_df)
        max_value = 10 ** (MAX_VALUE_DIGITS + DECIMAL_DIGITS)
        self.assertLessEqual(idprocess.value_ranges["open"], max_value)
        self.assertLessEqual(idprocess.value_ranges["close"], max_value)
        self.assertEqual(id_df["open"].min(), 0)
        self.assertEqual(id_df["close"].min(), 0)
        for i in range(0, len(id_df)):
            process_value = id_df["open"].iloc[i]
            # should be processed expectedly
            self.assertGreaterEqual(process_value, 0)
            self.assertLessEqual(process_value, max_value)
            row_value = id_df["high"].iloc[i]
            # should be kept original value
            row_values = str(row_value).split(".")
            self.assertEqual(len(row_values), 2)
            self.assertGreater(len(row_values[0]), MIN_VALUE_DIGITS)
            self.assertGreaterEqual(len(row_values[1]), 1, f"{row_value} found")
            # original data should not be changed
            row_org_value = self.org_data["open"].iloc[i]
            row_values = str(row_org_value).split(".")
            self.assertEqual(len(row_values), 2)
            self.assertGreaterEqual(len(row_values[0]), MIN_VALUE_DIGITS)
            self.assertGreaterEqual(len(row_values[1]), 1, f"{row_org_value} found")

        r_data = idprocess.revert(id_df)
        sample_column = self.ohlc_columns[0]
        for i in range(0, len(self.org_data)):
            process_value = r_data[sample_column].iloc[i]
            exp_value = self.org_data[sample_column].iloc[i]
            if np.isnan(process_value) and np.isnan(exp_value):
                continue
            self.assertLess(abs(process_value - exp_value), 10**-DECIMAL_DIGITS, msg=f"{process_value} != {exp_value} on {i}")

    def test_column_diff_process(self):
        cdprocess = preprocess.SimpleColumnDiffPreProcess("close", ["open", "high", "low", "close"])
        cd_data = cdprocess(self.org_data)
        self.assertEqual(len(self.org_data.columns), len(cd_data.columns))
        self.assertTrue((self.org_data.columns == cd_data.columns).all())
        r_data = cdprocess.revert(cd_data)
        for i in range(1, len(self.org_data)):
            process_value = r_data["open"].iloc[i]
            exp_value = self.org_data["open"].iloc[i]
            self.assertTrue(process_value == exp_value, f"{process_value} != {exp_value} on {i}")

    def test_log_process(self):
        lprocess = preprocess.LogPreProcess(columns=self.ohlc_columns)
        log_data = lprocess(self.org_data)
        exp_data = self.org_data.apply(np.log)

        self.assertEqual(len(self.org_data.columns), len(log_data.columns))
        self.assertTrue((self.org_data.columns == log_data.columns).all())

        sample_column = self.org_data.columns[0]
        for i in range(0, len(exp_data)):
            process_value = log_data[sample_column].iloc[i]
            exp_value = exp_data[sample_column].iloc[i]
            if np.isnan(process_value) and np.isnan(exp_value):
                continue
            self.assertTrue(process_value == exp_value, f"{process_value} != {exp_value} on {i}")

    def test_revert_log(self):
        lprocess = preprocess.LogPreProcess(columns=self.ohlc_columns)
        log_data = lprocess(self.org_data)
        r_data = lprocess.revert(log_data)

        self.assertEqual(len(r_data.columns), len(log_data.columns))
        self.assertTrue((r_data.columns == log_data.columns).all())

        sample_column = self.ohlc_columns[0]
        for i in range(0, len(self.org_data)):
            process_value = r_data[sample_column].iloc[i]
            exp_value = self.org_data[sample_column].iloc[i]
            if np.isnan(process_value) and np.isnan(exp_value):
                continue
            self.assertAlmostEqual(process_value, exp_value, msg=f"{process_value} != {exp_value} on {i}")

    def test_minmax_process(self):
        mmprocess = preprocess.MinMaxPreProcess(["open", "close"])
        mm_data = mmprocess(self.org_data)
        self.assertEqual(len(self.org_data.columns), len(mm_data.columns))

        sample_column = mm_data.columns[0]
        for i in range(0, len(mm_data)):
            process_value = mm_data[sample_column].iloc[i]
            self.assertGreaterEqual(process_value, -1, f"{process_value} < -1 on {i}")
            self.assertLessEqual(process_value, 1, f"{process_value} < -1 on {i}")

    def test_revert_minmax(self):
        mmprocess = preprocess.MinMaxPreProcess(["open", "close"])
        mm_data = mmprocess(self.org_data)
        r_data = mmprocess.revert(mm_data)

        self.assertEqual(len(r_data.columns), len(mm_data.columns))
        self.assertTrue((r_data.columns == mm_data.columns).all())

        sample_column = self.ohlc_columns[0]
        for i in range(0, len(self.org_data)):
            process_value = r_data[sample_column].iloc[i]
            exp_value = self.org_data[sample_column].iloc[i]
            if np.isnan(process_value) and np.isnan(exp_value):
                continue
            self.assertAlmostEqual(process_value, exp_value, msg=f"{process_value} != {exp_value} on {i}")

    def test_revert_minmax_with_numpy(self):
        columns = ["open", "close"]
        mmprocess = preprocess.MinMaxPreProcess(columns)
        mm_data = mmprocess(self.org_data)
        r_data = mmprocess.revert(mm_data[columns].values)
        sample_column = columns[0]
        for i in range(0, len(self.org_data)):
            process_value = r_data[i, 0]
            exp_value = self.org_data[sample_column].iloc[i]
            if np.isnan(process_value) and np.isnan(exp_value):
                continue
            self.assertAlmostEqual(process_value, exp_value, msg=f"{process_value} != {exp_value} on {i}")

        sample_column = columns[1]
        r_data = mmprocess.revert(mm_data[sample_column].values, sample_column)
        for i in range(0, len(self.org_data)):
            process_value = r_data[i]
            exp_value = self.org_data[sample_column].iloc[i]
            if np.isnan(process_value) and np.isnan(exp_value):
                continue
            self.assertAlmostEqual(process_value, exp_value, msg=f"{process_value} != {exp_value} on {i}")

    def test_revert_minmax_with_np_chunk(self):
        columns = ["open", "close"]
        mmprocess = preprocess.MinMaxPreProcess(columns)
        mm_data = mmprocess(self.org_data)
        indices = [1, 5, 10]
        chunk_data = []
        data_length = 60

        # for batch_first=True
        for index in indices:
            chunk_data.append(mm_data[columns].iloc[index : index + data_length].values)
        chunk_data = np.array(chunk_data)
        r_data = mmprocess.revert(chunk_data)

        column_index = 0
        for column in columns:
            chunk_index = 0
            for index in indices:
                for length in range(data_length):
                    process_value = r_data[chunk_index, length, column_index]
                    exp_value = self.org_data[column].iloc[index + length]
                    if np.isnan(process_value) and np.isnan(exp_value):
                        continue
                    self.assertAlmostEqual(process_value, exp_value, msg=f"{process_value} != {exp_value} on {index} + {length} on {column}")
                chunk_index += 1
            column_index += 1

        # for batch_first=False
        chunk_data = []
        for index in indices:
            chunk_data.append(mm_data[columns].iloc[index : index + data_length].values)
        chunk_data = np.array(chunk_data)
        chunk_data = chunk_data.swapaxes(0, 1)
        print(chunk_data.shape)
        r_data = mmprocess.revert(chunk_data)

        column_index = 0
        for column in columns:
            chunk_index = 0
            for index in indices:
                for length in range(data_length):
                    process_value = r_data[length, chunk_index, column_index]
                    exp_value = self.org_data[column].iloc[index + length]
                    if np.isnan(process_value) and np.isnan(exp_value):
                        continue
                    self.assertAlmostEqual(process_value, exp_value, msg=f"{process_value} != {exp_value} on {index} + {length} on {column}")
                chunk_index += 1
            column_index += 1

    def test_std_preprocess(self):
        stdprocess = preprocess.STDPreProcess(["open", "close"])
        std_data = stdprocess(self.org_data)
        self.assertEqual(len(self.org_data.columns), len(std_data.columns))
        sample_column = std_data.columns[0]
        for i in range(0, len(std_data)):
            process_value = std_data[sample_column].iloc[i]
            self.assertGreaterEqual(process_value, -10, f"{process_value} < -1 on {i}")
            self.assertLessEqual(process_value, 10, f"{process_value} < -1 on {i}")

    def test_save_processes(self):
        file_name = "./preprocess.json"
        dprorcess = preprocess.DiffPreProcess(periods=1, columns=self.ohlc_columns)
        dprorcess2 = preprocess.DiffPreProcess(periods=2, columns=["volume"])
        processes = [dprorcess, dprorcess2]
        preprocess.save_preprocesses(processes)
        self.assertTrue(os.path.exists(file_name))
        loaded_processes = preprocess.load_preprocess(file_name)
        self.assertTrue(processes == loaded_processes)


if __name__ == "__main__":
    unittest.main()
