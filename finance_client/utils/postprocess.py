import numpy
import datetime
from finance_client.utils import standalization, indicaters
import pandas as pd
from finance_client.utils.process import ProcessBase

def get_available_processes() -> dict:
    processes = {
        'Diff':DiffPreProcess,
        'MiniMax': MinMaxPreProcess,
        'STD': STDPreProcess
        
    }
    return processes

def postprocess_to_params(processes:list) -> dict:
    """convert procese list to dict for saving params as file

    Args:
        processes (list: ProcessBase): postprocess defiend in postprocess.py

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
        columns = data.columns
        result = {}
        for column in columns:
            result[column] = data[column].diff()
        
        self.last_tick = data.iloc[-1]
        return result
    
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
    opiton = {}
    
    def __init__(self, key: str='minmax', scale=(-1, 1), params:dict=None, entire_mode=False):
        """Apply minimax for each column of data.
        Note that if params are not specified, mini max values are detected by data on running once only.
        So if data is partial data, mini max values will be not correct.

        Args:
            key (str, optional): identification of this process. Defaults to 'minmax'.
            scale (tuple, optional): minimax scale. Defaults to (-1, 1).
            params (dict, optional): {column_name: (min, max)}. Defaults to None and caliculate by provided data when run this process.
            entire_mode (bool, optional): caliculate min/max from entire data (all columns). Defaults to False
        """
        self.opiton['scale'] = scale
        self.option['entire_mode'] = entire_mode
        self.entire_mode_column = '__minmax'
        if type(params) == dict:
            self.opiton.update(params)
        super().__init__(key)
        
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
    
    def run(self, data: pd.DataFrame) -> dict:
        columns = data.columns
        result = {}
        e_mode = self.option['entire_mode']
        option = self.opiton
        if 'scale' in option:
            scale = option['scale']
        else:
            scale = (-1, 1)
            self.option['scale'] = scale
            
        if e_mode:
            if self.entire_mode_column in self.opiton:
                _min, _max = self.opiton[self.entire_mode_column]
            else:
                _min = data.min().min()
                _max = data.max().max()                
                self.opiton[self.entire_mode_column] = (_min, _max)
                
            return standalization.mini_max(data, _min, _max, scale)
        else:
            for column in columns:
                if column in self.option:
                    _min, _max = self.option[column]
                    result[column], _, _ = standalization.mini_max_from_series(data[column], scale, (_min, _max))
                else:
                    result[column], _max, _min =  standalization.mini_max_from_series(data[column], scale)
                    if column not in self.option:
                        if type(_max) != pd.Timestamp:
                            self.option[column] = (_min, _max)
        self.columns = columns
        return result
    
    def update(self, tick:pd.Series, do_update_minmax=True):
        columns = self.columns
        scale = self.opiton['scale']
        result = {}
        e_mode = self.option['entire_mode']
        
        if e_mode:
            _min, _max = self.option[self.entire_mode_column]
            if do_update_minmax:
                tick_max = tick.max()
                tick_min = tick.min()
                if tick_min < _min:
                    _min = tick_min
                    self.option[self.entire_mode_column] = (_min, _max)
                if tick_max > _max:
                    _max = tick_max
                    self.option[self.entire_mode_column] = (_min, _max)
        
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
                return standalization.mini_max(data_set, *self.option[self.entire_mode_column], self.opiton['scale'])
            else:
                return standalization.revert_mini_max_from_iterable(data_set, self.option, self.opiton['scale'])
        elif type(data_set) == pd.Series:
            if e_mode:
                return standalization.revert_mini_max_from_iterable(data_set, self.option, self.opiton['scale'])
            else:
                index_set = set(data_set.index)
                column_set = set(self.columns)
                union = index_set & column_set
                if len(union) == 0:
                    if type(column) is str and column in self.option:
                        return standalization.revert_mini_max_from_series(data_set, *self.option[column], self.opiton['scale'])
                    else:
                        if len(self.columns) != 1:
                            raise ValueError(f"column need to be specified to revert column series data.")
                        else:
                            return standalization.revert_mini_max_from_row_series(data_set, *self.option[self.columns[0]])
                else:
                    return standalization.revert_mini_max_from_row_series(data_set, self.option, self.opiton['scale'])
        elif type(data_set) == dict:
            reverted = {}
            for column in data_set:
                if e_mode:
                    minmax = self.opiton[self.entire_mode_column]
                else:
                    minmax = self.option[column]
                r = standalization.revert_mini_max_from_iterable(data_set[column], minmax, self.opiton['scale'])
                reverted[column] = r
            return reverted
        else:#assume iterable like list, tuple
            if e_mode:
                return standalization.revert_mini_max_from_iterable(data_set, self.option[self.entire_mode_column], self.opiton['scale'])
            else:
                if type(column) is str and column in self.option:
                    return standalization.revert_mini_max_from_iterable(data_set, self.option[column], self.opiton['scale'])
                elif len(data_set) == len(self.columns):
                    result = []
                    for i in range(0, len(self.columns)):
                        _min, _max = self.option[self.columns[i]]
                        data = data_set[i]
                        row_data = standalization.revert_mini_max(data, _min, _max, self.opiton['scale'])
                        result.append(row_data)
                    return result
                else:
                    raise Exception("number of data is different. row data or columns with same length data is supported for list")
            
class STDPreProcess(ProcessBase):
    pass