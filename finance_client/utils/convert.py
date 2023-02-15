import pandas as pd


def multisymbols_dict_to_df(data: dict) -> pd.DataFrame:
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
