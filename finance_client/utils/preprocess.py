from finance_client.utils import standalization, convert
import pandas as pd
from finance_client.utils.process import ProcessBase
from collections.abc import Iterable

def get_available_processes() -> dict:
    processes = {
        'Diff':DiffPreProcess,
        'MiniMax': MinMaxPreProcess,
        'STD': STDPreProcess
        
    }
    return processes

def preprocess_to_params(processes:list) -> dict:
    """convert procese list to dict for saving params as file

    Args:
        processes (list: ProcessBase): preprocess defiend in preprocess.py

    Returns:
        dict: {key: params}
    """
    params = {}
    for process in processes:
        option = process.option
        option['kinds'] = process.kinds
        params[process.key] = option
    return params

def load_preprocess(params:dict) -> list:
    ips_dict = get_available_processes()
    pss = []
    for key, param in params.items():
        kinds = param['kinds']
        ps = ips_dict[kinds]
        ps = ps.load(key, param)
        pss.append(ps)
    return pss


class DiffPreProcess(ProcessBase):
    
    kinds = 'Diff'
    last_tick:pd.DataFrame = None
    option = {
        'floor':1
    }
    
    def __init__(self, key = "diff", floor:int = 1):
        super().__init__(key)
        self.option['floor'] = floor
        
    @classmethod
    def load(self, key:str, params:dict):
        floor = params["floor"]
        return DiffPreProcess(key, floor)
    
    def run(self, data: pd.DataFrame) -> dict:
        self.last_tick = data.iloc[-1]
        return data.diff()
    
    def update(self, tick:pd.Series):
        """ assuming data is previous result of run()

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
                for index in range(len(data)-1, -1, -1):
                    last_data = data[index] - last_data
                    row_data.append(last_data)
                row_data = reversed(row_data)
                result.append(row_data)
            return True, result
        else:
            raise Exception("number of data is different")

class MinMaxPreProcess(ProcessBase):
    
    kinds = 'MiniMax'
    
    def __init__(self, key: str='minmax', scale=(-1, 1), init_params:dict=None, columns_to_ignore=[]):
        """Apply minimax for each column of data.
        Note that if params are not specified, mini max values are detected by data on running once only.
        So if data is partial data, mini max values will be not correct.

        Args:
            key (str, optional): identification of this process. Defaults to 'minmax'.
            scale (tuple, optional): minimax scale. Defaults to (-1, 1).
            init_params (dict, optional): {"min": {column_name: min_value}, "max": {column_name: max_value}}. Defaults to None and caliculate by provided data when run this process.
            columns_to_ignore (list, optional): specify column to ignore applying minimax or revert process. Defaults to []
        """
        self.option = {
            "min": {}, "max": {}, 
            "scale": scale
        }
        if type(columns_to_ignore) is str:
            columns_to_ignore = [columns_to_ignore]
        elif type(columns_to_ignore) is tuple and len(columns_to_ignore) == 2:
            columns_to_ignore = [columns_to_ignore]
        self.option['columns_to_ignore'] = columns_to_ignore
        self.columns = []
        super().__init__(key)
        self.initialization_required = True
        if type(init_params) == dict:
            self.option.update(init_params)
            self.initialization_required = False
        
    @classmethod
    def load(self, key:str, params:dict):
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
    
    def run(self, data: pd.DataFrame, symbols:list=[], grouped_by_symbol=False) -> dict:
        columns_to_ignore = self.option['columns_to_ignore']
        __columns_to_ignore = []
        if type(data.columns) == pd.MultiIndex:
            entire_symbols = convert.get_symbols(data, grouped_by_symbol)
            for i_columns in columns_to_ignore:
                if type(i_columns) is str:
                    if grouped_by_symbol:
                        __columns_to_ignore += [(__symbol, i_columns) for __symbol in entire_symbols]
                    else:
                        __columns_to_ignore += [(i_columns, __symbol) for __symbol in entire_symbols]
                elif isinstance(i_columns, Iterable) and len(i_columns) == 2:
                    __columns_to_ignore.append(i_columns)
                else:
                    print(f"skip {i_columns} on ignore column process of minmax")
        else:
            __columns_to_ignore = columns_to_ignore
        columns = list(set(data.columns) - set(__columns_to_ignore))
        self.columns = columns
        
        option = self.option
        if 'scale' in option:
            scale = option['scale']
        else:
            scale = (-1, 1)
            self.option['scale'] = scale
            
        if len(option["min"]) > 0:
            _min = pd.Series(option["min"])
            _max = pd.Series(option["max"])
        else:
            _min = data[columns].min()
            option["min"].update(_min.to_dict())
            _max = data[columns].max()
            option["max"].update(_max.to_dict())
            
        if len(symbols) > 0:
            if grouped_by_symbol:
                target_symbols = set(data.columns.droplevel(1)) & set(symbols)
                target_columns = data[list(target_symbols)].columns
            else:
                target_symbols = set(data.columns.droplevel(0)) & set(symbols)
                unique_columns = set(data.columns.droplevel(1))
                target_columns = [(__column, __symbol) for __symbol in target_symbols for __column in unique_columns]
        else:
            target_columns = data.columns
        columns = list(set(target_columns) - set(__columns_to_ignore))        
        _df, _, _ = standalization.mini_max(data, _min, _max, scale)
        
        return _df
        
    
    def update(self, tick:pd.Series, do_update_minmax=True):
        columns = self.columns
        scale = self.option['scale']
        result = {}
        e_mode = self.option['entire_mode']
        
        for column in columns:
            new_value = tick[column]
            
            if e_mode is False:
                _min, _max = self.option[column]
                if do_update_minmax:
                    if new_value < _min:
                        _min = new_value
                        self.option[column] = (_min, _max)
                    if new_value > _max:
                        _max = new_value
                        self.option[column] = (_min, _max)
            
            scaled_new_value = standalization.mini_max(new_value, _min, _max, scale)
            result[column] = scaled_new_value
            
        new_data = pd.Series(result)
        return new_data

    def get_minimum_required_length(self):
        return 1
    
    def revert(self, data_set, column=None):
        """revert data minimaxed by this process

        Args:
            data_set (dict|DataFrame|Series|list): _description_
            column (str, optional): column to revert series data or list data. Defaults to None.

        Returns:
           reverted data. type is same as input
        """
        e_mode = self.option['entire_mode']
        
        if type(data_set) == pd.DataFrame:
            if e_mode:
                data_set = data_set[self.columns].copy()
                return standalization.revert_mini_max(data_set, *self.option[self.entire_mode_column], self.option['scale'])
            else:
                return standalization.revert_mini_max_from_iterable(data_set, self.option, self.option['scale'])
        elif type(data_set) == pd.Series:
            if e_mode:
                return standalization.revert_mini_max_from_iterable(data_set.copy(), self.option[self.entire_mode_column], self.option['scale'])
            else:
                index_set = set(data_set.index)
                column_set = set(self.columns)
                union = index_set & column_set
                if len(union) == 0:
                    if type(column) is str and column in self.option:
                        return standalization.revert_mini_max_from_series(data_set, *self.option[column], self.option['scale'])
                    else:
                        if len(self.columns) != 1:
                            raise ValueError(f"column need to be specified to revert column series data.")
                        else:
                            return standalization.revert_mini_max_from_row_series(data_set, *self.option[self.columns[0]])
                else:
                    return standalization.revert_mini_max_from_row_series(data_set, self.option, self.option['scale'])
        elif type(data_set) == dict:
            reverted = {}
            for column in data_set:
                if e_mode:
                    minmax = self.option[self.entire_mode_column]
                else:
                    minmax = self.option[column]
                r = standalization.revert_mini_max_from_iterable(data_set[column], minmax, self.option['scale'])
                reverted[column] = r
            return reverted
        else:#assume iterable like list, tuple
            if e_mode:
                return standalization.revert_mini_max_from_iterable(data_set, self.option[self.entire_mode_column], self.option['scale'])
            else:
                if type(column) is str and column in self.option:
                    return standalization.revert_mini_max_from_iterable(data_set, self.option[column], self.option['scale'])
                elif len(data_set) == len(self.columns):
                    result = []
                    for i in range(0, len(self.columns)):
                        _min, _max = self.option[self.columns[i]]
                        data = data_set[i]
                        row_data = standalization.revert_mini_max(data, _min, _max, self.option['scale'])
                        result.append(row_data)
                    return result
                else:
                    raise Exception("number of data is different. row data or columns with same length data is supported for list")

class STDPreProcess(ProcessBase):
    pass