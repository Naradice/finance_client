from collections.abc import Iterable

import pandas as pd

from finance_client.utils import convert, standalization
from finance_client.utils.process import ProcessBase


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
    last_tick: pd.DataFrame = None
    option = {"floor": 1}

    def __init__(self, key="diff", floor: int = 1):
        super().__init__(key)
        self.option["floor"] = floor

    @classmethod
    def load(self, key: str, params: dict):
        floor = params["floor"]
        return DiffPreProcess(key, floor)

    def run(self, data: pd.DataFrame) -> dict:
        self.last_tick = data.iloc[-1]
        return data.diff()

    def update(self, tick: pd.Series):
        """assuming data is previous result of run()

        Args:
            data (pd.DataFrame): previous result of run()
            tick (pd.Series): new row data
            option (Any, optional): Currently no option (Floor may be added later). Defaults to None.
        """
        new_data = tick - self.last_tick
        self.last_tick = tick
        return new_data

    def get_minimum_required_length(self):
        return 1

    def revert(self, data_set: tuple):
        columns = self.last_tick.columns
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


class MinMaxPreProcess(ProcessBase):
    kinds = "MiniMax"

    def __init__(self, key: str = "minmax", scale=(-1, 1), init_params: dict = None, columns=None):
        """Apply minimax for each column of data.
        Note that if params are not specified, mini max values are detected by data on running once only.
        So if data is partial data, mini max values will be not correct.

        Args:
            key (str, optional): identification of this process. Defaults to 'minmax'.
            scale (tuple, optional): minimax scale. Defaults to (-1, 1).
            init_params (dict, optional): {"min": {column_name: min_value}, "max": {column_name: max_value}}. Defaults to None and caliculate by provided data when run this process.
            columns (list, optional): specify column to ignore applying minimax or revert process. Defaults to []
        """
        self.option = {"min": {}, "max": {}, "scale": scale}
        if type(columns) is str:
            columns = [columns]
        elif type(columns) is tuple and len(columns) == 2:
            columns = [columns]
        self.columns = columns
        super().__init__(key)
        self.initialization_required = True
        if type(init_params) == dict:
            self.option.update(init_params)
            self.initialization_required = False

    @classmethod
    def load(self, key: str, params: dict):
        option = {}
        scale = (-1, 1)
        for k, value in params.items():
            if type(value) == list:
                option[k] = tuple(value)
            else:
                option[k] = value
        process = MinMaxPreProcess(key, scale, option)
        return process

    def initialize(self, data: pd.DataFrame):
        self.run(data)
        self.initialization_required = False

    def run(self, data: pd.DataFrame, symbols: list = None, grouped_by_symbol=False) -> dict:
        if self.columns is None:
            self.columns = data.columns
        columns = self.columns
        target_columns = _get_columns(data, columns, symbols, grouped_by_symbol)
        columns = list(set(data.columns) & set(target_columns))

        option = self.option
        if "scale" in option:
            scale = option["scale"]
        else:
            scale = (-1, 1)
            self.option["scale"] = scale

        if len(option["min"]) > 0:
            _min = pd.Series(option["min"])
            _max = pd.Series(option["max"])
        else:
            _min = data[columns].min()
            option["min"].update(_min.to_dict())
            _max = data[columns].max()
            option["max"].update(_max.to_dict())

        _df, _, _ = standalization.mini_max(data[columns], _min, _max, scale)

        return _df

    def update(self, tick: pd.Series, do_update_minmax=True):
        columns = self.columns
        scale = self.option["scale"]
        result = {}

        for column in columns:
            if column in self.columns:
                new_value = tick[column]
                _min = self.option["min"][column]
                _max = self.option["max"][column]
                if do_update_minmax:
                    if new_value < _min:
                        _min = new_value
                        self.option[column] = (_min, _max)
                    if new_value > _max:
                        _max = new_value
                        self.option[column] = (_min, _max)

                scaled_new_value, _min, _max = standalization.mini_max(new_value, _min, _max, scale)
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
            if self.columns is None:
                columns = data_set.columns
            else:
                columns = self.columns
            data_set = data_set[columns].copy()
            _min = self.option["min"]
            _min = pd.Series(_min)
            _max = self.option["max"]
            _max = pd.Series(_max)
            return standalization.revert_mini_max(data_set, _min, _max, self.option["scale"])
        elif isinstance(data_set, pd.Series):
            column = data_set.name
            if column in self.columns:
                _min = self.option["min"][column]
                _max = self.option["max"][column]
            else:
                columns = data_set.index
                _min = []
                _max = []
                for column in columns:
                    if column in self.columns:
                        _min.append(self.option["min"][column])
                        _max.append(self.option["max"][column])
                _min = pd.Series(_min, index=columns)
                _max = pd.Series(_max, index=columns)
            return standalization.revert_mini_max_from_series(data_set, _min, _max, self.option["scale"])
        else:
            print(f"type{data_set} is not supported")


class STDPreProcess(ProcessBase):
    pass
