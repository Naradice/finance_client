import pandas as pd

def multisymbols_dict_to_df(data:dict) -> pd.DataFrame:
    return pd.concat(data.values(), axis=1, keys=data.keys())

def concat_df_symbols(org_dfs, dfs, symbols:list, column_name:str, grouped_by_symbol=False):
    if grouped_by_symbol:
        df_cp = org_dfs.copy()
        dfs_cp = dfs.copy()
        dfs_cp.columns = symbols
        for symbol in symbols:
            df_cp[(symbol, column_name)] = dfs_cp[symbol]
        return df_cp
    else:
        #dfs.columns = pd.MultiIndex.from_tuples([(column_name, symbol) for symbol in symbols])
        dfs.columns = [(column_name, symbol) for symbol in symbols]
        return pd.concat([org_dfs, dfs], axis=1)