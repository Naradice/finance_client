import pandas as pd

def revert_mini_max(scaled_value, _min, _max, scale=(0, 1)):
    if scale[0] >= scale[1]:
        raise ValueError("mini_max function scale should be (min, max)")
    std = (scaled_value - scale[0])/(scale[1] - scale[0])
    value = std * (_max - _min) + _min
    return value

def mini_max(value, _min, _max, scale=(0,1)):
    if scale[0] >= scale[1]:
        raise ValueError("mini_max function scale should be (min, max)")
    std = (value - _min)/(_max - _min)
    scaled = std * (scale[1] - scale[0]) + scale[0]
    return scaled

def mini_max_from_array(array,_min=None, _max = None, scale=(0,1)):
    if _min == None:
        _min = min(array)
    if _max == None:
        _max = max(array)
    return [ mini_max(x, _min, _max, scale) for x in array], _max, _min

def revert_mini_max_from_series(series: pd.Series, _min, _max, scale = (0 ,1)):
    std = (series - scale[0])/(scale[1] - scale[0])
    values = std * (_max - _min) + _min
    return values

def revert_mini_max_from_row_series(series: pd.Series, options, scale = (0 ,1)):
    """ assume series index have column name

    Args:
        series (pd.Series): data to revert
        options (dict): {column_name: (min, max)}
        scale (tuple, optional): Defaults to (0 ,1).

    Returns:
        reverted series data
    """
    index = series.index
    max_list = []
    min_list = []
    available_idx = []
    for column in index:
        if column in options:
            available_idx.append(column)
            min_max = options[column]
            min_list.append(min_max[0])
            max_list.append(min_max[1])
        else:
            print(f"{column} is ignored on revert process of minimax")
    s = series[available_idx]
    std = (s - scale[0])/(scale[1] - scale[0])
    _max = pd.Series(data=max_list, index=available_idx)
    _min = pd.Series(data=min_list, index=available_idx)
    values = std * (_max - _min) + _min
    return values

def mini_max_from_series(series: pd.Series, scale = (0,1), opt = None):
    if opt == None:
        _max = series.max()
        _min = series.min()
    else:
        _min = opt[0]
        _max = opt[1]
    std = (series - _min)/(_max - _min)
    scaled = std * (scale[1] - scale[0]) + scale[0]
    return scaled, _max, _min

def mini_max_from_row_series(series: pd.Series, options, scale = (0 ,1)):
    """ assume series index have column name

    Args:
        series (pd.Series): data to revert
        options (dict): {column_name: (min, max)}
        scale (tuple, optional): Defaults to (0 ,1).

    Returns:
        series data appied minimax
    """
    index = series.index
    max_list = []
    min_list = []
    available_idx = []
    for column in index:
        if column in options:
            available_idx.append(column)
            min_max = options[column]
            min_list.append(min_max[0])
            max_list.append(min_max[1])
        else:
            print(f"{column} is ignored on revert process of minimax")
    s = series[available_idx]
    _max = pd.Series(data=max_list, index=available_idx)
    _min = pd.Series(data=min_list, index=available_idx)

    std = (series - _min)/(_max - _min)
    scaled_series = std * (scale[1] - scale[0]) + scale[0]
    return scaled_series
    
def revert_mini_max_from_iterable(data, opt, scale=(0,1)):
    if type(data) == list:
        data = pd.Series(data)
    if type(data) == pd.Series:
        _min = opt[0]
        _max = opt[1]
        return revert_mini_max_from_series(data, _min, _max, scale)
    elif type(data) == pd.DataFrame:
        data_ = data.copy()
        for key in data:
            _min = opt[key][0]
            _max = opt[key][1]
            data_[key] = revert_mini_max_from_series(data[key], _min, _max, scale)
        return data_
    else:
        raise TypeError(f"this obj type is not supported for now: {type(data)}")
    
def revert_mini_max(value, min, max, scale=(0,1)):
    std = (value - scale[0])/(scale[1] - scale[0])
    reverted = std * (max - min) + min
    return reverted
            
        
def mini_max_from_data(data, scale=(0,1)):
    if type(data) == list:
        result, _max, _min = mini_max_from_array(data, scale)
        return result, (_min, _max)
    elif type(data) == pd.Series:
        result,_max, _min = mini_max_from_series(data, scale)
        return pd.Series(result), (_min, _max)
    elif type(data) == pd.DataFrame:
        #target is columns
        opt = {}
        data_ = data.copy()
        for key in data:
            data_[key], _max, _min = mini_max_from_series(data_[key])
            opt[key] = (_min, _max)
        return data_, opt
    else:
        #todo add numpy
        raise Exception(f"{type(data)} is not supported")