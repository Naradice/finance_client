import pandas as pd

OPEN = "open"
HIGH = "high"
LOW = "low"
CLOSE = "close"
VOLUME = "volume"
SPREAD = "spread"
DATETIME = "time"

DEFAULT_OHLC_COLUMNS = [OPEN, HIGH, LOW, CLOSE]
DEFAULT_COLUMNS = [DATETIME, *DEFAULT_OHLC_COLUMNS, VOLUME, SPREAD]


def is_grouped_by_symbol(columns: pd.MultiIndex):
    """check is columns of OHLC data is grouped by symbol
    Assume data has open column

    Args:
        columns (pd.MultiIndex): columns of dataframe

    Returns:
        bool or None: if columns is Multiindex, return true/false. Otherwise None
    """
    if isinstance(columns, pd.DataFrame):
        columns = columns.columns
    if isinstance(columns, pd.MultiIndex):
        open_column = "open"
        for column in columns.droplevel(0):
            if open_column == str(column).lower():
                return True
        for column in columns.droplevel(1):
            if open_column == str(column).lower():
                return False
        print("open column is not found on column")
        return None
    else:
        print(f"{type(columns)} is not supported")
        return None


def get_columns_of_symbol(df: pd.DataFrame, symbol: str = None) -> pd.Index:
    columns = df.columns
    if isinstance(columns, pd.MultiIndex):
        if symbol is None:
            if is_grouped_by_symbol(columns):
                columns = set(columns.droplevel(0))
            else:
                columns = set(columns.droplevel(1))
                columns = pd.Index(columns)
        else:
            if symbol in columns.droplevel(0):
                # grouped_by_symbol = False
                columns = columns.swaplevel(0, 1)
            elif symbol not in columns.droplevel(1):
                raise ValueError(f"Specified symbol {symbol} not found on columns.")
            columns = columns[symbol]
    return columns


def get_ohlc_columns(df: pd.DataFrame, symbol: str = None) -> dict:
    """returns column names of ohlc data.

    Args:
        df (pd.DataFrame): OHLC DataFrame. Column should have open/high/low/close
        symbol (str, Optional): A symbol name.

    Returns:
        dict: format is {"open": ${open_column}, "high": ${high_column}, "low": ${low_column}, "close": ${close_column}, "time": ${time_column}, "volume": ${volume_column}}
    """

    ohlc_columns = {}

    def update_dict(key, item):
        if key in ohlc_columns:
            print(f"key {key} is duplicated for {ohlc_columns[key]} and {item}")
            key = item.lower()
        ohlc_columns[key] = item

    columns = get_columns_of_symbol(df, symbol)
    data_column_dict = {column.lower(): column for column in columns}

    for key, data_column in data_column_dict.items():
        if key in DEFAULT_COLUMNS:
            update_dict(key, data_column)
        elif DATETIME in key:
            update_dict(DATETIME, data_column)
        elif VOLUME in key:
            update_dict(VOLUME, data_column)
        elif SPREAD in key:
            update_dict(SPREAD, data_column)
        elif OPEN in key:
            update_dict(OPEN, data_column)
        elif HIGH in key:
            update_dict(HIGH, data_column)
        elif LOW in key:
            update_dict(LOW, data_column)
        elif CLOSE in key:
            update_dict(CLOSE, data_column)
        else:
            print("unkown column found on get_ohlc_column")
            update_dict(key, data_column)

    return ohlc_columns
