import datetime

import pandas as pd


def dropna_market_close(data_df: pd.DataFrame, delta_hour=10) -> pd.DataFrame:
    """
    drop NaN only if index is longer than delta_hour between non NaN values

    Args:
        data_df (pd.DataFrame): dataframe to drop
        delta_hour (int): threshold hour to drop

    Returns:
        pd.DataFrame: dropped data
    """
    if isinstance(data_df.index, pd.DatetimeIndex):
        nonnullindex = data_df.dropna().index
        long_delta_cond = (nonnullindex[1:] - nonnullindex[:-1]) >= datetime.timedelta(hours=delta_hour)
        market_close_start = nonnullindex[:-1][long_delta_cond]
        market_close_end = nonnullindex[1:][long_delta_cond]
        dfs = []

        index = 0
        from_index = data_df.index[0]
        to_index = market_close_start[0]

        while True:
            market_open_df = data_df.loc[from_index:to_index]
            dfs.append(market_open_df)
            index += 1
            if index >= len(market_close_start):
                break
            from_index = market_close_end[index - 1]
            to_index = market_close_start[index]
        from_index = market_close_end[index - 1]
        to_index = data_df.index[-1]
        market_open_df = data_df.loc[from_index:to_index]
        dfs.append(market_open_df)
        return pd.concat(dfs, axis=0)
    else:
        raise ValueError("Index support DatetimeIndex only.")


def multisymbols_dict_to_df(data: dict) -> pd.DataFrame:
    """create DataFrame from {key_1: dataframe, key_2: dataframe, ...}

    Args:
        data (dict): {key_1: dataframe, key_2: dataframe, ...}

    Returns:
        pd.DataFrame: DataFrame with MultiIndexHeader grouped by key
    """
    return pd.concat(data.values(), axis=1, keys=data.keys())


def concat_df_symbols(org_dfs, dfs, symbols: list, column_name: str, grouped_by_symbol=False):
    if grouped_by_symbol:
        df_cp = org_dfs.copy()
        dfs_cp = dfs.copy()
        dfs_cp.columns = symbols
        for symbol in symbols:
            df_cp[(symbol, column_name)] = dfs_cp[symbol]
        return df_cp
    else:
        # dfs.columns = pd.MultiIndex.from_tuples([(column_name, symbol) for symbol in symbols])
        dfs.columns = [(column_name, symbol) for symbol in symbols]
        return pd.concat([org_dfs, dfs], axis=1)


def get_symbols(dfs: pd.DataFrame, grouped_by_symbol=False):
    if type(dfs.columns) == pd.MultiIndex:
        if grouped_by_symbol:
            return list(set(dfs.columns.droplevel(1)))
        else:
            column = dfs.columns[0][0]
            return list(dfs[column].columns)


def has_symbol_str(word: str):
    return False, ""


def str_to_currencies(symbol: str):
    """Convert symbol str like JPYUSD to from symbol and to symbol like JPY and USD

    Args:
        symbol (str): symbol of FX

    Returns:
        tupe(str, str): from symbol and to symbol
    """
    if type(symbol) == str:
        if "/" in symbol:
            symbol_list = symbol.split("/")  # assume USD/JPY for ex
            if len(symbol_list) == 2:
                from_symbol = symbol_list[0]
                to_symbol = symbol_list[1]
            else:
                raise ValueError("Unkown format is provided as currency symbol")
        else:
            if len(symbol) == 6:
                from_symbol = symbol[:3]
                to_symbol = symbol[3:]
            else:
                suc, from_symbol = has_symbol_str(symbol)
                if suc:
                    to_symbol = symbol.replace(from_symbol, "")
                else:
                    raise ValueError("symbol can't be recognized as currency set")
        return from_symbol, to_symbol
    else:
        raise TypeError(f"symbol should be str type. {type(symbol)} is provided")


def concat(data: pd.DataFrame, new_data: pd.Series):
    if type(data) == pd.DataFrame and type(new_data) == pd.Series:
        return pd.concat([data, pd.DataFrame.from_records([new_data])], ignore_index=True, sort=False)
    elif type(data) == pd.Series and type(new_data) == pd.DataFrame:
        return pd.concat([pd.DataFrame.from_records([data]), new_data], ignore_index=True)
    elif type(data) == pd.DataFrame and type(new_data) == pd.DataFrame:
        return pd.concat([data, new_data], ignore_index=True)
    elif type(data) == pd.Series and type(new_data) == pd.Series:
        return pd.concat([pd.DataFrame.from_records([data]), pd.DataFrame.from_records([new_data])], ignore_index=True)
    else:
        raise Exception("concat accepts dataframe or series")
