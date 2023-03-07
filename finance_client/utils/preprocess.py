from collections.abc import Iterable

import pandas as pd

from . import convert, standalization
from .process import ProcessBase


def get_available_processes() -> dict:
    processes = {"Diff": DiffPreProcess, "MiniMax": MinMaxPreProcess, "STD": STDPreProcess}
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


def load_preprocess(params: dict) -> list:
    ips_dict = get_available_processes()
    pss = []
    for key, param in params.items():
        kinds = param["kinds"]
        ps = ips_dict[kinds]
        ps = ps.load(key, param)
        pss.append(ps)
    return pss


def _get_columns(df, columns, symbols=None, grouped_by_symbol=True):
    target_columns = []
    if columns is None:
        columns = df.columns
    if type(df.columns) == pd.MultiIndex:
        target_symbols = convert.get_symbols(df, grouped_by_symbol)
        if symbols is not None:
            target_symbols = list(set(target_symbols) & set(symbols))
        for i_columns in columns:
            if type(i_columns) is str:
                if grouped_by_symbol:
                    target_columns += [(__symbol, i_columns) for __symbol in target_symbols]
                else:
                    target_columns += [(i_columns, __symbol) for __symbol in target_symbols]
            elif isinstance(i_columns, Iterable) and len(i_columns) == 2:
                target_columns.append(i_columns)
            else:
                print(f"skip {i_columns} on ignore column process of minmax")
    else:
        target_columns = columns
    return target_columns


class DiffPreProcess(ProcessBase):
    kinds = "Diff"

    def __init__(
        self,
        target_columns=None,
        floor: int = 1,
        key="diff",
    ):
        super().__init__(key)
        self.columns = target_columns
        self.floor = floor
        self.last_tick = None

    @property
    def option(self):
        return {"floor": self.floor, "target_columns": self.columns}

    @classmethod
    def load(self, key: str, params: dict):
        return DiffPreProcess(key, **params)

    def run(self, data: pd.DataFrame) -> dict:
        if self.columns is not None:
            data = data[self.columns]
        self.last_ticks = data.iloc[-self.floor :]
        return data.diff(periods=self.floor)

    def update(self, tick: pd.Series):
        """assuming data is previous result of run()

        Args:
            data (pd.DataFrame): previous result of run()
            tick (pd.Series): new row data
            option (Any, optional): Currently no option (Floor may be added later). Defaults to None.
        """
        new_data = tick - self.last_ticks.iloc[-self.floor]
        self.last_ticks = pd.concat([self.last_ticks[-self.floor + 1 :], tick])
        return new_data

    def get_minimum_required_length(self):
        return self.floor

    def revert(self, data_set: tuple):
        if self.columns is None:
            columns = self.last_tick.columns
        else:
            columns = self.columns

        result = []
        if type(data_set) == pd.DataFrame:
            data_set = tuple(data_set[column] for column in columns)
        if len(data_set) == len(columns):
            for i in range(0, len(columns)):
                last_data = self.last_tick[columns[i]]
                data = data_set[i]
                row_data = [last_data]
                for index in range(len(data) - 1, -1, -1):
                    last_data = data[index] - last_data
                    row_data.append(last_data)
                row_data = reversed(row_data)
                result.append(row_data)
            return True, result
        else:
            raise Exception("number of data is different")


class LogPreProcess(ProcessBase):
    kinds = "Log"

    def __init__(self, key: str = "log"):
        super().__init__(key)

    def run(self):
        pass

    def revert(self):
        pass

    def get_minimum_required_length(self):
        return 1


class DiffIDPreProcess(ProcessBase):
    kinds = "CandleID"

    def __init__(self, key: str):
        super().__init__(key)


class IDPreProcess(ProcessBase):
    kinds = "ID"

    def __init__(self, key: str):
        super().__init__(key)


class CloseDiffPreProcess(ProcessBase):
    kinds = "CDiff"

    def __init__(self, key: str):
        super().__init__(key)


class MinMaxPreProcess(ProcessBase):
    kinds = "MiniMax"

    def __init__(self, columns=None, scale=(-1, 1), min_values=None, max_values=None, key: str = "minmax"):
        """Apply minimax for each column of data.
        Note that if params are not specified, mini max values are detected by data on running once only.
        So if data is partial data, mini max values will be not correct.

        Args:
            key (str, optional): identification of this process. Defaults to 'minmax'.
            scale (tuple, optional): minimax scale. Defaults to (-1, 1).
            min_value, max_value (dict, optional):  {{column_name: min/max value}}. Defaults to None and caliculate by provided data when run this process.
            columns (list, optional): specify column to ignore applying minimax or revert process. Defaults to []
        """
        super().__init__(key)
        if isinstance(scale, Iterable):
            if len(scale) == 2 and scale[0] < scale[1]:
                self.scale = scale
            else:
                raise ValueError("scale should have 2 elements with scale[0] < scale[1]")
        else:
            self.scale = (-1, 1)

        if type(columns) is str:
            columns = [columns]
        if isinstance(columns, Iterable):
            columns = list(set(columns))
        elif columns is not None:
            raise TypeError("columns should be iterable")
        self.columns = columns

        self.initialization_required = True
        if min_values is not None:
            if max_values is not None:
                self.min_values = min_values
                self.max_values = max_values
                self.initialization_required = False
            else:
                raise ValueError("Both min and max values required for init.")
        elif max_values is not None:
            raise ValueError("Both min and max values required for init.")
        else:
            self.min_values = {}
            self.max_values = {}

    @classmethod
    def load(self, key: str, params: dict):
        option = {"scale": (-1, 1)}
        for k, value in params.items():
            if type(value) == list:
                option[k] = tuple(value)
            else:
                option[k] = value
        process = MinMaxPreProcess(key, **option)
        return process

    def initialize(self, data: pd.DataFrame):
        if self.columns is None:
            self.columns = data.columns
        self.run(data)
        self.initialization_required = False

    def run(self, data: pd.DataFrame, symbols: list = None, grouped_by_symbol=False) -> dict:
        target_columns = _get_columns(data, self.columns, symbols, grouped_by_symbol)
        columns = list(set(data.columns) & set(target_columns))

        if len(self.min_values) > 0:
            _min = pd.Series(self.min_values)
            _max = pd.Series(self.max_values)
        else:
            _min = data[columns].min()
            self.min_values.update(_min.to_dict())
            _max = data[columns].max()
            self.max_values.update(_max.to_dict())

        _df, _, _ = standalization.mini_max(data[columns], _min, _max, self.scale)

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

    def revert(self, data_set):
        """revert data minimaxed by this process

        Args:
            data_set (DataFrame|Series): _description_

        Returns:
           reverted data. type is same as input
        """

        if isinstance(data_set, pd.DataFrame):
            data_set = data_set[self.columns]
            _min = pd.Series(self.min_values)
            _max = pd.Series(self.max_values)
            return standalization.revert_mini_max(data_set, _min, _max, self.scale)
        elif isinstance(data_set, pd.Series):
            column = data_set.name
            if column in self.columns:
                _min = self.min_values[column]
                _max = self.max_values[column]
            else:
                columns = data_set.index
                _min = []
                _max = []
                for column in columns:
                    if column in self.columns:
                        _min.append(self.min_values[column])
                        _max.append(self.max_values[column])
                _min = pd.Series(_min, index=columns)
                _max = pd.Series(_max, index=columns)
            return standalization.revert_mini_max_from_series(data_set, _min, _max, self.scale)
        else:
            print(f"type{data_set} is not supported")


class STDPreProcess(ProcessBase):
    pass
