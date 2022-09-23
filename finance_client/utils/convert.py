import pandas as pd

def multisymbols_dict_to_df(data:dict) -> pd.DataFrame:
    return pd.concat(data.values(), axis=1, keys=data.keys()) 