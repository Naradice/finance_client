import os

import pandas as pd

from .convert import multisymbols_dict_to_df

default_data_folder = f'{os.getcwd()}/data_source'
env_data_folder_key = 'data_path'

def get_datafolder_path():
    if env_data_folder_key in os.environ:
        path = os.environ[env_data_folder_key]
        if os.path.exists(path):
            return path
        else:
            try:
                os.makedirs(path)
                return path
            except Exception as e:
                return default_data_folder
    return default_data_folder

def add_extension(file_name:str, ext:str=".csv") -> str:
    names = file_name.split(ext)
    if len(names) > 1:
        return f"{''.join(names)}{ext}"
    else:
        return f"{file_name}{ext}"
    
def get_file_path(provider:str, file_name:str, create_dirs=False):
    data_folder_base = get_datafolder_path()
    file = add_extension(file_name)
    data_folder = os.path.join(data_folder_base, provider)
    if os.path.exists(data_folder) is False and create_dirs:
        os.makedirs(data_folder)
    file_path = os.path.join(data_folder, file)
    return file_path

def get_economic_path():
    data_path = get_datafolder_path()
    eco_path = os.path.join(data_path, "economic")
    if os.path.exists(eco_path):
        return eco_path
    else:
        os.makedirs(eco_path)
        return eco_path

def get_economic_state_file_path():
    base_path = get_economic_path()
    file_name = "state.json"
    file_path = os.path.join(base_path, file_name)
    return file_path

def get_economic_file_path(key:str):
    base_path = get_economic_path()
    file_name = add_extension(key)
    file_path = os.path.join(base_path, file_name)
    return file_path

def write_df_to_csv(df:pd.DataFrame, provider:str, file_name:str, panda_option:dict=None):
    file_path = get_file_path(provider, file_name, True)
    if panda_option:
        df.to_csv(file_path, **panda_option)
    else:
        df.to_csv(file_path)

def write_multi_symbol_df_to_csv(df:pd.DataFrame, provider:str, base_file_name:str, symbols:list, panda_option:dict=None):
    for symbol in symbols:
        if type(df.columns) is pd.MultiIndex:
            try:
                symbol_df = df[symbol]
            except Exception as e:
                print(f"{symbol} is not found on df. {df.columns}")
                continue
            symbol_file_base = f"{base_file_name}_{symbol}"
            write_df_to_csv(symbol_df, provider, symbol_file_base, panda_option)

def read_csv(provider:str, file_name:str, parse_dates_columns:list=None, pandas_option:dict=None):
    file_path = get_file_path(provider, file_name, True)
    if os.path.exists(file_path):
        kwargs = {
            "filepath_or_buffer": file_path
        }
        if parse_dates_columns is not None:
            kwargs["parse_dates"] = parse_dates_columns
        if pandas_option is not None:
            kwargs.update(pandas_option)
        df = pd.read_csv(**kwargs)
        return df
    else:
        #print(f"file not found: {file_path}")
        return None

def read_csvs(provider:str, base_file_name:str, symbols:list, parse_dates_columns:list=None, panda_option:dict=None):
    DFS = {}
    if type(symbols) is list and len(symbols) > 1:
        for symbol in symbols:
            symbol_file_base = f"{base_file_name}_{symbol}"
            df = read_csv(provider, symbol_file_base, parse_dates_columns, panda_option)
            if df is not None:
                DFS[symbol] = df
        return multisymbols_dict_to_df(DFS)
    elif type(symbols) is list and len(symbols) == 1:
        symbol_file_base = f"{base_file_name}_{symbols[0]}"
        df = read_csv(provider, symbol_file_base, parse_dates_columns, panda_option)
        return df
    elif type(symbols) is str:
        symbol_file_base = f"{base_file_name}_{symbol}"
        df = read_csv(provider, symbol_file_base, parse_dates_columns, panda_option)
        return df