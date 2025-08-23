import json
import os
import warnings
from typing import Union

import numpy as np
import pandas as pd

from . import convert, logger, standalization
from .process import ProcessBase
from .timeprocess import WeeklyIDProcess


def get_available_processes() -> dict:
    processes = {
        "Diff": DiffPreProcess,
        "MiniMax": MinMaxPreProcess,
        "STD": STDPreProcess,
        "SCDiff": SimpleColumnDiffPreProcess,
        "ID": IDPreProcess,
        "Log": LogPreProcess,
        WeeklyIDProcess.kinds: WeeklyIDProcess,
    }
    return processes


def preprocess_to_params(processes: list) -> dict:
    """convert procese list to dict for saving params as file

    Args:
        processes (list: ProcessBase): preprocess defiend in preprocess.py

    Returns:
        dict: {key: params}
    """
    params = {}
    for process in processes:
        option = process.option
        option["kinds"] = process.kinds
        params[process.key] = option
    return params


def save_preprocesses(processes: list, file_name: str = None):
    params = preprocess_to_params(processes)
    if file_name is None:
        file_name = os.path.join(os.getcwd(), "preprocess.json")
    with open(file_name, mode="w") as fp:
        json.dump(params, fp)


def load_preprocess(arg: Union[str, dict]) -> list:
    if type(arg) is str:
        with open(arg, mode="r") as fp:
            params = json.load(fp)
    elif type(arg) is dict:
        params = arg
    else:
        raise TypeError(f"argument should be str or dict. {type(arg)} is provided.")
    ips_dict = get_available_processes()
    pss = []
    for key, param in params.items():
        kinds = param.pop("kinds")
        ps = ips_dict[kinds]
        ps = ps.load(key, param)
        pss.append(ps)
    return pss


def load_default_preprocess(key: str, columns: list):
    _key = str.lower(key)
    if _key == str.lower(SimpleColumnDiffPreProcess.kinds):
        return SimpleColumnDiffPreProcess(base_column=columns[-1], target_columns=columns)
    else:
        for dict_key, process in get_available_processes().items():
            if _key == str.lower(process.kinds) or _key == str.lower(dict_key):
                return process(columns=columns)


def _get_columns(df, columns, symbols=None, grouped_by_symbol=True):
    target_columns = []
    if symbols is not None and type(df.columns) == pd.MultiIndex:
        columns = df.columns
        target_symbols = convert.get_symbols(df, grouped_by_symbol)
        target_symbols = list(set(target_symbols) & set(symbols))
        remaining_column = []
        for i_columns in columns:
            if grouped_by_symbol:
                if i_columns[0] in target_symbols:
                    target_columns.append(i_columns)
                else:
                    remaining_column.append(i_columns)
            else:
                if i_columns[1] in target_symbols:
                    target_columns.append(i_columns)
                else:
                    remaining_column.append(i_columns)
        if len(target_columns) > 0:
            target_columns = pd.MultiIndex.from_tuples(target_columns)
        else:
            logger.warnings(
                f"specified columns {columns} is not found on {df.columns} with grouped_by_symbol: {grouped_by_symbol}"
            )
            target_columns = []
        if len(remaining_column) > 0:
            remaining_column = pd.MultiIndex.from_tuples(remaining_column)
        else:
            remaining_column = []
    elif columns is not None and type(df.columns) == pd.MultiIndex:
        target_columns = []
        remaining_column = []
        for i_columns in df.columns:
            if grouped_by_symbol:
                if i_columns[1] in columns:
                    target_columns.append(i_columns)
                else:
                    remaining_column.append(i_columns)
            else:
                if i_columns[0] in columns:
                    target_columns.append(i_columns)
                else:
                    remaining_column.append(i_columns)
        if len(target_columns) > 0:
            target_columns = pd.MultiIndex.from_tuples(target_columns)
        else:
            target_columns = []
            logger.warnings(
                f"specified columns {columns} is not found on {df.columns} with grouped_by_symbol: {grouped_by_symbol}"
            )
        if len(remaining_column) > 0:
            remaining_column = pd.MultiIndex.from_tuples(remaining_column)
        else:
            remaining_column = []
    elif columns is not None:
        target_columns = columns
        remaining_column = list(set(df.columns) - set(columns))
    else:
        target_columns = []
        remaining_column = list(df.columns)
    return target_columns, remaining_column


def _concat_target_and_remain(original_df, processed_df, remaining_columns):
    if not isinstance(original_df, pd.DataFrame):
        return processed_df
    original_columns = original_df.columns
    if remaining_columns is not None and len(remaining_columns) > 0:
        remaining_data = original_df[remaining_columns]
        data = pd.concat([processed_df, remaining_data], axis=1)
        # revert columns order to original
        data = data[original_columns]
    else:
        data = processed_df
    return data


class DiffPreProcess(ProcessBase):
    kinds = "Diff"

    def __init__(
        self,
        key: str = None,
        periods: int = 1,
        columns=None,
        dropna=False,
    ):
        if key is None:
            key = f"diff_{periods}"
        super().__init__(key)
        if hasattr(columns, "copy"):
            self.columns = columns.copy()
        else:
            self.columns = columns
        self.periods = periods
        self.last_tick = None
        self.dropna = dropna

    @property
    def option(self):
        return {"periods": self.periods, "columns": self.columns}

    @classmethod
    def load(self, key: str, params: dict):
        return DiffPreProcess(key=key, **params)

    def run(self, df: pd.DataFrame, symbols: list = None, grouped_by_symbol=False) -> dict:
        remaining_columns = None
        if self.columns is not None:
            target_columns, remaining_columns = _get_columns(df, self.columns, symbols, grouped_by_symbol)
            temp_data = df[target_columns]
        else:
            temp_data = df
        self.first_ticks = df.iloc[: self.periods]
        self.last_ticks = df.iloc[-self.periods :]
        temp_data = temp_data.diff(periods=self.periods)
        data = _concat_target_and_remain(df, temp_data, remaining_columns)
        if self.dropna:
            data.dropna(how="any", inplace=True)
        return data

    def update(self, tick: pd.Series):
        """assuming data is previous result of run()

        Args:
            data (pd.DataFrame): previous result of run()
            tick (pd.Series): new row data
            option (Any, optional): Currently no option (Floor may be added later). Defaults to None.
        """
        new_data = tick - self.last_ticks.iloc[-self.periods]
        self.last_ticks = pd.concat([self.last_ticks[-self.periods + 1 :], tick])
        return new_data

    def get_minimum_required_length(self):
        return self.periods + 1

    @property
    def revert_params(self):
        return ("data", "base_value")

    def revert(self, data, base_values=None, columns=None, discontinuity=False):
        if columns is None:
            columns = self.first_ticks.columns

        if isinstance(data, pd.DataFrame):
            available_columns = []
            for column in data.columns:
                if column in columns:
                    available_columns.append(column)

            if len(available_columns) > 0:
                if base_values is None:
                    base_values = self.first_ticks[available_columns]
                if discontinuity:
                    r_data = data.values + base_values.values
                else:
                    r_data = np.zeros_like(data)
                for start_index in range(self.periods):
                    temp_values = data[start_index :: self.periods].cumsum().fillna(0)
                    r_data[start_index :: self.periods] = temp_values + base_values.iloc[start_index]
                return pd.DataFrame(r_data, index=data.index, columns=available_columns)
            else:
                raise ValueError(f"data has different columns: {data.columns} is not part of {columns}")
        elif isinstance(data, np.ndarray):
            if len(data.shape) > 2:
                if base_values is None:
                    raise ValueError("base_value must be specified.")
                axis = 0
            else:
                axis = 0
                if base_values is None:
                    base_values = self.first_ticks.values
                    if len(data.shape) == 2 and data.shape[1] != len(columns):
                        feature_size = data.shape[1]
                        if feature_size > len(columns):
                            raise ValueError("can't determin column axis in the positions")
                        columns = columns[feature_size]
                        warnings.warn(f"assume axis=1:{data.shape[1]} is a part of columns")
            if isinstance(base_values, pd.DataFrame):
                base_values = base_values.values
            if discontinuity:
                r_data = data + base_values
            else:
                r_data = np.zeros_like(data)
                for start_index in range(self.periods):
                    temp_values = np.cumsum(data[start_index :: self.periods], axis=axis)
                    temp_values = np.nan_to_num(temp_values)
                    r_data[start_index :: self.periods] = temp_values + base_values[start_index]
            return r_data

        else:
            raise TypeError(f"type {type(data)} is not supported.")


class LogPreProcess(ProcessBase):
    kinds = "Log"

    def __log(self, df: pd.DataFrame, columns: list):
        return df[columns].apply(np.log)

    def __exp(self, values):
        return np.exp(values)

    @classmethod
    def load(self, key, params: dict):
        process = LogPreProcess(key=key, **params)
        return process

    @property
    def option(self):
        return {"columns": self.columns, "e": self.base_e}

    def __init__(self, key="log", columns: list = None, e=None):
        """Apply np.log for specified columns

        Args:
            columns (list, optional): target columns to apply np.log. Defaults to None and apply entire columns
            e (int, optional): base of log. Defaults to None and exp is used.
        """
        super().__init__(key)
        self.columns = columns
        if e is not None:
            log_base_value = np.log(e)
            if log_base_value == np.inf:
                log_base_value = np.log(float(e))
                self.__log = lambda df, columns: df[columns].apply(np.log) / np.log(float(e))
                self.__exp = lambda values: float(e) ** values
            else:
                self.__log = lambda df, columns: df[columns].apply(np.log) / np.log(e)
                self.__exp = lambda values: e**values
        self.base_e = e

    def run(self, df: pd.DataFrame):
        target_columns, remaining_columns = _get_columns(df, self.columns)
        data = self.__log(df, target_columns)
        data = _concat_target_and_remain(df, data, remaining_columns)
        return data

    @property
    def revert_params(self):
        return ("data",)

    def revert(self, data, columns=None):
        if isinstance(data, pd.DataFrame):
            target_columns, remaining_columns = _get_columns(data, self.columns)
            log_data = data[target_columns]
            r_data = self.__exp(log_data)
            r_data = _concat_target_and_remain(data, r_data, remaining_columns)
            return r_data
        return self.__exp(data)

    def get_minimum_required_length(self):
        return 1


class IDPreProcess(ProcessBase):
    kinds = "ID"

    @property
    def option(self):
        return {"columns": self.columns, "decimals": self.decimals}

    @classmethod
    def load(self, key, params: dict):
        process = IDPreProcess(key=key, **params)
        return process

    def __init__(
        self, key="id", columns: list = None, decimals=None, min_value=None, max_value=None, start_from=0, int_dtype=np.int64
    ):
        """Convert Numeric values to ID (0 to X)

        Args:
            columns (list): _description_
            decimals(int|list, optional):
                When int is specified,
                if decimals > 0, Reduce digits by rounding the value. Ex.) decimals=2 for 10324 become 103
                If decimals < 0, Recud decimal digits by rounding the value. Ex.) decimals=-3 for 3.14159265 become 3142:
                When list of int is specified, it should the same length as the columns.
                each columns are ID
        """
        super().__init__(key)
        if decimals is None or type(decimals) is int:
            self.decimals = decimals
        elif isinstance(decimals, (list, set, tuple)):
            if columns is None or len(decimals) == len(columns):
                self.decimals = decimals
            else:
                raise ValueError("decimals must have same length as columns specified.")
        else:
            raise TypeError(f"decimans should be int or list. {type(decimals)} is specified.")
        self.columns = columns
        if all([min_value is None, max_value is None]):
            self.initialization_required = True
        else:
            self.initialization_required = False

        if min_value is not None:
            self.value_ranges = abs(min_value) + max_value
            self.min_value = min_value
        else:
            self.min_value = None
            self.value_ranges = None
            self.min_value = None
        self.int_type = int_dtype
        self.start_from = start_from

    def run(self, df: pd.DataFrame):
        org_columns = df.columns
        target_columns, remaining_columns = _get_columns(df, self.columns)
        temp_data = df[target_columns]
        if self.decimals is not None and self.decimals != 0:
            if type(self.decimals) is int:
                if self.decimals >= 0:
                    temp_data = temp_data.round(self.decimals)
                temp_data = (temp_data - self.min_value) * 10**-self.decimals
            else:
                temp_dfs = []
                for index in range(len(self.decimals)):
                    decimal = self.decimals[index]
                    column = self.columns[index]
                    if decimal is not None and decimal != 0:
                        temp_df = temp_data[column]
                        if decimal >= 0:
                            temp_df = temp_df.round(decimal)
                        temp_df = (temp_df + self.min_value[column]) * 10**-decimal
                        temp_dfs.append(temp_df)
                    else:
                        temp_dfs.append(df[column])
                temp_data = pd.concat(temp_dfs, axis=1)
        id_df = temp_data + self.start_from
        id_df = id_df.astype(self.int_type)
        remaining_df = df[remaining_columns]
        df = pd.concat([id_df, remaining_df], axis=1)
        df = df[org_columns]
        return df

    def initialize(self, df: pd.DataFrame):
        # as run function would be called with partial data, caliculate min_values in advance for entire data.
        if self.columns is None:
            self.columns = df.columns
        df = df[self.columns]
        if self.decimals is not None and self.decimals != 0:
            if type(self.decimals) is int:
                if self.decimals >= 0:
                    df = df.round(self.decimals)
                df = df * 10**-self.decimals
            else:
                temp_dfs = []
                for index in range(len(self.decimals)):
                    decimal = self.decimals[index]
                    column = self.columns[index]
                    if decimal is not None and decimal != 0:
                        temp_df = df[column]
                        if decimal >= 0:
                            temp_df = temp_df.round(decimal)
                        temp_df = temp_df * 10**-decimal

                        temp_dfs.append(temp_df)
                    else:
                        temp_dfs.append(df[column])
                df = pd.concat(temp_dfs, axis=1)
        if self.min_value is None:
            min_values = df.min()
            bases = []
            columns = []
            base_value_p = min_values[min_values > 0]
            if len(base_value_p) > 0:
                bases.extend(-base_value_p.values)
                columns.extend(base_value_p.index.values)
            base_value_n = min_values[min_values <= 0]
            if len(base_value_n) > 0:
                bases.extend(base_value_n.abs().values)
                columns.extend(base_value_n.index.values)
            self.min_value = pd.Series(bases, index=columns)
            self.value_ranges = df.max() + abs(self.min_value)
        # a_max_value = self.value_ranges.max()
        # if a_max_value < 32768:
        #     int_type = "int16"
        # if a_max_value < 2147483648:
        #     int_type = "int32"
        # else:
        #     int_type = "int64"
        self.initialization_required = False

    @property
    def revert_params(self):
        return ("data",)

    @property
    def VOCAB_SIZE(self):
        return self.value_ranges

    def revert(self, data):
        if isinstance(data, pd.DataFrame):
            target_columns, remaining_columns = _get_columns(data, self.columns)
            if isinstance(self.min_value, (pd.DataFrame, pd.Series)):
                r_df = data[target_columns] - self.min_value[target_columns]
            else:
                r_df = data - self.min_value

            if self.decimals is not None and self.decimals != 0:
                if type(self.decimals) is int:
                    r_df = r_df * 10**self.decimals
                else:
                    temp_dfs = []
                    for index in range(len(self.decimals)):
                        decimal = self.decimals[index]
                        column = self.columns[index]
                        if column in target_columns:
                            if decimal is not None and decimal != 0:
                                temp_df = r_df[column] * 10**decimal

                                temp_dfs.append(temp_df)
                            else:
                                temp_dfs.append(r_df[column])
                    r_df = pd.concat(temp_dfs, axis=1)
            r_df += self.min_value
            if len(remaining_columns) > 0:
                org_columns = data.columns
                remaining_df = data[remaining_columns]
                r_df = pd.concat([r_df, remaining_df], axis=1)
                r_df = r_df[org_columns]
            return r_df
        else:
            raise TypeError(f"type {type(data)} is not supported.")


class SimpleColumnDiffPreProcess(ProcessBase):
    kinds = "SCDiff"

    @property
    def option(self):
        return {"base_column": self.base_column, "target_columns": self.columns}

    @classmethod
    def load(self, key: str, params: dict):
        process = SimpleColumnDiffPreProcess(key=key, **params)
        return process

    def __init__(
        self,
        key="scdiff",
        base_column: str = "close",
        target_columns: list = ["open", "high", "low", "close"],
    ):
        """Caliculate diff: df[target_columns] - df[base_column].shift(1).values
        Assume base_column is close and target_columns is [open, high, low, close]. Typically

        Args:
            base_column (str, optional): Defaults to "close"
            target_columns (list, optional): Defaults to ["open", "high", "low", "close"].
        """
        super().__init__(key)
        if type(target_columns) is str:
            target_columns = [target_columns]
        elif isinstance(target_columns, (list, set, tuple)):
            target_columns = list(target_columns)
        self.columns = target_columns
        self.base_column = [base_column]

    def run(self, df: pd.DataFrame):
        target_columns, remaining_columns = _get_columns(df, self.columns)
        self.first_value = df[self.base_column].iloc[0].values
        target_df = df[target_columns]
        target_df = target_df - df[self.base_column].shift(1).values
        if len(remaining_columns) > 0:
            org_columns = df.columns
            remaining_df = df[remaining_columns]
            target_df = pd.concat([target_df, remaining_df], axis=1)
            target_df = target_df[org_columns]
        return target_df

    @property
    def revert_params(self):
        return ("data", "base_value")

    def revert(self, data, base_value=None):
        if isinstance(data, pd.DataFrame):
            remaining_columns = None
            df = data
            if len(data.columns) > len(self.columns):
                target_columns, remaining_columns = _get_columns(data, self.columns)
                df = data[target_columns]
            else:
                target_columns = data.columns
            if base_value is None:
                base_value = self.first_value
            else:
                if isinstance(base_value, (list, set, tuple)):
                    if len(base_value) > 1:
                        if isinstance(base_value, pd.Series):
                            base_value = base_value[self.base_column]
                        else:
                            base_value = base_value[0]
                # else cases assume base_value can broadcast

            r_data = np.zeros_like(df)
            if pd.isna(df.iloc[0]).any():
                r_data[0] = df.iloc[0].values
            else:
                next_df = df.iloc[0] + base_value
                r_data[0] = next_df.values
                base_value = next_df[self.base_column].values

            for i in range(1, len(df)):
                next_df = df.iloc[i] + base_value
                r_data[i] = next_df.values
                base_value = next_df[self.base_column].values
            r_data = pd.DataFrame(r_data, index=data.index, columns=target_columns)
            if remaining_columns:
                org_columns = data.columns
                remaining_df = data[remaining_columns]
                r_data = pd.concat([r_data, remaining_df], axis=1)
                r_data = r_data[org_columns]
            return r_data
        else:
            raise TypeError(f"type {type(data)} is not supported.")


class MinMaxPreProcess(ProcessBase):
    kinds = "MiniMax"

    @property
    def option(self):
        return {
            "columns": self.columns,
            "scale": self.scale,
            "min_values": self.min_values.to_dict(),
            "max_values": self.max_values.to_dict(),
        }

    def __init__(
        self,
        key: str = "minmax",
        columns=None,
        scale=(-1, 1),
        min_values=None,
        max_values=None,
        grouped_by_symbols=False,
    ):
        """Apply minimax for each column of data.
        Note that if params are not specified, mini max values are detected by data on running once only.
        So if data is partial data, mini max values will be not correct.

        Args:
            key (str, optional): identification of this process. Defaults to 'minmax'.
            scale (tuple, optional): minimax scale. Defaults to (-1, 1).
            min_value, max_value (dict, optional):  {{column_name: min/max value}}. Defaults to None and caliculate
            by provided data when run this process.
            columns (list, optional): specify column to ignore applying minimax or revert process. Defaults to []
        """
        super().__init__(key)
        if isinstance(scale, (list, set, tuple)):
            if len(scale) == 2 and scale[0] < scale[1]:
                self.scale = scale
            else:
                raise ValueError("scale should have 2 elements with scale[0] < scale[1]")
        else:
            self.scale = (-1, 1)

        if type(columns) is str:
            columns = [columns]
        if isinstance(columns, (list, set, tuple)):
            columns = list(set(columns))
        elif columns is not None:
            raise TypeError("columns should be (list, set, tuple)")
        self.columns = columns

        self.initialization_required = True
        self.grouped_by_symbols = grouped_by_symbols
        if min_values is not None:
            if max_values is not None:
                self.min_values = pd.Series(min_values, dtype=np.float64)
                self.max_values = pd.Series(max_values, dtype=np.float64)
                self.initialization_required = False
            else:
                raise ValueError("Both min and max values required for init.")
        elif max_values is not None:
            raise ValueError("Both min and max values required for init.")
        else:
            self.min_values = pd.Series([], dtype=np.float64)
            self.max_values = pd.Series([], dtype=np.float64)

    @classmethod
    def load(self, key: str, params: dict):
        option = {"scale": (-1, 1)}
        for k, value in params.items():
            if type(value) == list:
                option[k] = tuple(value)
            else:
                option[k] = value
        process = MinMaxPreProcess(key=key, **option)
        return process

    def initialize(self, data: pd.DataFrame, symbols: list = None, grouped_by_symbols=None):
        if grouped_by_symbols is None:
            grouped_by_symbols = self.grouped_by_symbols

        if self.columns is None:
            self.columns = data.columns
        self.run(data, symbols, grouped_by_symbols)
        self.initialization_required = False

    def run(self, data: pd.DataFrame, symbols: list = None, grouped_by_symbol=None) -> dict:
        if grouped_by_symbol is None:
            grouped_by_symbol = self.grouped_by_symbols
        target_columns, remaining_columns = _get_columns(data, self.columns, symbols, grouped_by_symbol)

        if len(self.min_values) == 0:
            self.min_values = data[target_columns].min()
            self.max_values = data[target_columns].max()

        _df, _, _ = standalization.mini_max(data[target_columns], self.min_values, self.max_values, self.scale)
        if len(remaining_columns) > 0:
            org_columns = data.columns
            remaining_df = data[remaining_columns]
            _df = pd.concat([_df, remaining_df], axis=1)
            _df = _df[org_columns]
        return _df

    def update(self, tick: pd.Series, do_update_minmax=True):
        result = {}

        for column in self.columns:
            new_value = tick[column]
            _min = self.min_values[column]
            _max = self.max_values[column]
            if do_update_minmax:
                if new_value < _min:
                    _min = new_value
                    self.min_values[column] = _min
                if new_value > _max:
                    _max = new_value
                    self.max_values[column] = _max

            scaled_new_value, _min, _max = standalization.mini_max(new_value, _min, _max, self.scale)
            result[column] = scaled_new_value

        new_data = pd.Series(result)
        return new_data

    def get_minimum_required_length(self):
        return 1

    @property
    def revert_params(self):
        return ("data", "columns")

    def revert(self, data, columns=None):
        """revert data minimaxed by this process

        Args:
            data (DataFrame|Series): log data
            columns (list, Optional): used numpy only. specify columns to revert
        Returns:
           reverted data. type is same as input
        """

        if isinstance(data, pd.DataFrame):
            target_columns, remaining_columns = _get_columns(data, self.columns, None, None)
            target_data = data[target_columns]
            _min = self.min_values[target_columns]
            _max = self.max_values[target_columns]
            r_df = standalization.revert_mini_max(target_data, _min, _max, self.scale)
            if len(remaining_columns) > 0:
                org_columns = data.columns
                remaining_df = data[remaining_columns]
                r_df = pd.concat([r_df, remaining_df], axis=1)
                r_df = r_df[org_columns]
            return r_df
        elif isinstance(data, pd.Series):
            column = data.name
            if column in self.columns:
                _min = self.min_values[column]
                _max = self.max_values[column]
            else:
                columns = data.index
                target_columns = []
                for column in columns:
                    if column in self.columns:
                        target_columns.append(column)
                _min = self.min_values[target_columns]
                _max = self.max_values[target_columns]
            return standalization.revert_mini_max_from_series(data, _min, _max, self.scale)
        elif isinstance(data, np.ndarray):
            if columns is None:
                if data.shape[-1] != len(self.columns):
                    raise Exception("feature size mismatch. Please specify columns.")
                columns = self.columns
            if type(columns) is str:
                min_values = self.min_values[columns]
                max_values = self.max_values[columns]
            else:
                min_values = self.min_values[columns].values
                max_values = self.max_values[columns].values
            return standalization.revert_mini_max_from_value(data, min_values, max_values, self.scale)
        else:
            raise Exception(f"type{type(data)} is not supported")


class STDPreProcess(ProcessBase):
    kinds = "STD"

    @property
    def option(self):
        return {"columns": self.columns, "alpha": self.alpha}

    @classmethod
    def load(self, key, params: dict):
        option = {}
        for k, value in params.items():
            option[k] = value
        process = STDPreProcess(key=key, **option)
        return process

    def __init__(self, key="std", columns=None, alpha=1):
        super().__init__(key)
        if type(columns) is str:
            columns = [columns]
        elif isinstance(columns, (list, set, tuple)):
            columns = list(columns)
        self.columns = columns
        if type(alpha) is int or isinstance(alpha, float):
            self.alpha = alpha
        else:
            raise TypeError("Please assign int or float as alpha")

    def run(self, df):
        target_columns, remaining_columns = _get_columns(df, self.columns)
        self.mean_values = df[target_columns].mean()
        self.std_values = df[target_columns].std()
        target_df = df[target_columns] - self.mean_values
        target_df = target_df / (self.std_values * self.alpha)
        if len(remaining_columns) > 0:
            org_columns = df.columns
            remaining_df = df[remaining_columns]
            target_df = pd.concat([target_df, remaining_df], axis=1)
            target_df = target_df[org_columns]
        return target_df

    @property
    def revert_params(self):
        return ("data",)

    def revert(self, data):
        if isinstance(data, pd.DataFrame):
            target_columns, remaining_columns = _get_columns(data, self.columns)
            r_df = data[target_columns]
            r_df = r_df * self.std_values * self.alpha
            r_df = r_df + self.mean_values
            if len(remaining_columns) > 0:
                org_columns = data.columns
                remaining_df = data[remaining_columns]
                r_df = pd.concat([r_df, remaining_df], axis=1)
                r_df = r_df[org_columns]
            return r_df
        elif isinstance(data, np.ndarray):
            r_data = data * self.std_values.values * self.alpha
            r_data = r_data + self.mean_values.values
            return r_data
        else:
            print(f"type{type(data)} is not supported")


class ClipPreProcess(ProcessBase):
    kinds = "Clip"

    def __init__(
        self,
        key: str = "clip",
        lower: float = -1.0,
        upper: float = 1.0,
        columns=None,
    ):
        super().__init__(key)
        if hasattr(columns, "copy"):
            self.columns = columns.copy()
        else:
            self.columns = columns
        self._lower = lower
        self._upper = upper

    @property
    def option(self):
        return {"lower": self._lower, "upper": self._upper, "columns": self.columns}

    @classmethod
    def load(self, key: str, params: dict):
        return ClipPreProcess(key=key, **params)

    def run(self, df: pd.DataFrame, symbols: list = None, grouped_by_symbol=False) -> dict:
        remaining_columns = None
        if self.columns is not None:
            target_columns, remaining_columns = _get_columns(df, self.columns, symbols, grouped_by_symbol)
            temp_data = df[target_columns]
        else:
            temp_data = df
        temp_data = temp_data.clip(lower=self._lower, upper=self._upper)
        data = _concat_target_and_remain(df, temp_data, remaining_columns)
        return data

    def update(self, tick: pd.Series):
        """assuming data is previous result of run()

        Args:
            data (pd.DataFrame): previous result of run()
            tick (pd.Series): new row data
            option (Any, optional): Currently no option (Floor may be added later). Defaults to None.
        """
        new_data = tick.clip(lower=self._lower, upper=self._upper)
        return new_data

    def get_minimum_required_length(self):
        return 1

    @property
    def revert_params(self):
        return ("data",)

    def revert(self, data, columns=None):
        return data
