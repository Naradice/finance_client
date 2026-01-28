import numpy as np
import pandas as pd


def __create_out_lists(elements, column_names):
    out_elements = []
    out_columns = []
    for index in range(0, len(elements)):
        if column_names[index] is not None:
            out_elements.append(elements[index])
            out_columns.append(column_names[index])
    return out_elements, out_columns


def create_multi_out_lists(symbols, elements, columns, grouped_by_symbol=False):
    out_columns = []
    out_eles = []

    for index in range(0, len(columns)):
        if columns[index]:
            column = columns[index]
            out_eles.append(elements[index])
            out_columns.extend(((column, _symbol) for _symbol in symbols))
    out_index = pd.MultiIndex.from_tuples(out_columns)
    if grouped_by_symbol:
        out_index = out_index.swaplevel(0, 1)
    return out_eles, out_index


def sum(data):
    amount = 0
    if type(data) == list:
        amount = sum(data)
    else:  # check more
        amount = data.sum()
    return amount


def revert_EMA(data, interval: int):
    """revert data created by EMA function to row data

    Args:
        data (DataFrame or Series or list): data created by EMA function to row data
        interval (int): window size
    """

    if len(data) > interval:
        alpha_r = (interval + 1) / 2
        if type(data) == pd.DataFrame or type(data) == pd.Series:
            raise Exception("Not implemented")
        result = [data[0]]
        for i in range(1, len(data)):
            row = data[i] * alpha_r + data[i - 1] * (1 - alpha_r)
            result.append(row)
        return True, result
    else:
        raise Exception("data length should be greater than interval")


def update_EMA(last_ema_value: float, new_value, window: int, a=None):
    """
    update Non weighted EMA with alpha= 2/(1+window)

    Args:
        last_ema_value (float): last EMA value caluculated by EMA function
        new_value (float): new value of data
        window (int): window size
    """
    alpha = 2 / (1 + window)
    if a is not None:
        alpha = a
    return last_ema_value * (1 - alpha) + new_value * alpha


def EMA(data, interval, alpha=None):
    """
    Calculate EMA.
    Args:
        data (list or np.ndarray or pd.Series or pd.DataFrame): input data
        interval (int): window size
        alpha (float, optional): smoothing factor. Defaults to None.

    Raises:
        Exception: data list has no value

    Returns:
        list or pd.Series or pd.DataFrame: EMA of input data. return same type as input data
    """
    if isinstance(data, (pd.Series, pd.DataFrame)):
        data_cp = data.copy()
        if alpha is None:
            return data_cp.ewm(span=interval, adjust=False).mean()
        else:
            return data_cp.ewm(alpha=alpha, adjust=False).mean()

    # list or similar
    if len(data) == 0:
        raise Exception("data list has no value")

    # Even if len(data) < interval, EMA can still be computed.
    last = data[0]
    ema = [last]
    _alpha = alpha if alpha is not None else 2 / (interval + 1)

    for i in range(1, len(data)):
        last = last * (1 - _alpha) + data[i] * _alpha
        ema.append(last)

    return ema


def EMAMulti(
    symbols: list, data: pd.DataFrame, target_column: str, interval: int, alpha=None, grouped_by_symbol=False, ema_name="EMA"
):
    df = data.copy()
    if grouped_by_symbol is False:
        df = df[target_column][symbols]
    else:
        df.columns = df.columns.swaplevel(0, 1)
        df = df[target_column][symbols]
    df = df[symbols]
    ema_df = EMA(df, interval, alpha)
    elements, columns = create_multi_out_lists(symbols, [ema_df], [ema_name], grouped_by_symbol)
    ema_df.columns = columns
    return ema_df


def EWA(data, window: int, alpha=None, adjust=True):
    """
    Exponential Weighted Moving Average (EWA)
    maintain the specification of returning list → list, pandas → pandas.

    Args:
        data (list or np.ndarray or pd.Series or pd.DataFrame): input data
        window (int): window size
        alpha (float, optional): smoothing factor. Defaults to None.
        adjust (bool, optional): Defaults to True. see pandas ewm adjust parameter.
    Raises:
        Exception: data list has no value
    Returns:
        list or pd.Series or pd.DataFrame: EWA of input data. return same type as input data
    """
    # --- pandas case ---
    if isinstance(data, (pd.Series, pd.DataFrame)):
        if alpha is None:
            return data.ewm(span=window, adjust=adjust).mean()
        else:
            return data.ewm(alpha=alpha, adjust=adjust).mean()

    # --- list/array case ---
    data = np.asarray(data, dtype=float)
    n = len(data)

    if n == 0:
        raise Exception("data list has no value")

    # determine alpha
    alp = alpha if alpha is not None else 2 / (window + 1)

    # adjust=False → usual EMA (recursive formula)
    if not adjust:
        ema = [data[0]]
        for i in range(1, n):
            ema.append(ema[-1] * (1 - alp) + data[i] * alp)
        return ema

    # adjust=True → same weighted average calculation as pandas
    ema = []
    for t in range(n):
        weights = (1 - alp) ** np.arange(t, -1, -1)  # t→0
        numerator = np.sum(data[:t+1] * weights)
        denominator = np.sum(weights)
        ema.append(numerator / denominator)

    return ema


def SMA(data, window):
    """
    Calculate Simple Moving Average (SMA).

    Args:
        data (list | pd.Series | pd.DataFrame):
            Time-series data. If a list is provided, a list is returned.
            If a pandas Series/DataFrame is provided, the same type is returned.

        window (int):
            Window size for the SMA. Must be >= 2.

    Returns:
        list | pd.Series | pd.DataFrame:
            SMA values. For list input, returns a list with the first (window-1)
            values padded with NaN to align length with the input.
            For pandas input, returns a rolling mean using pandas.

    Raises:
        Exception: If window < 2 or len(data) < window when list is provided.
    """

    # --- validate window ---
    if window < 2:
        raise Exception(f"window size should be greater than 2. specified {window}")

    # --- pandas case: return pandas result directly ---
    if isinstance(data, (pd.Series, pd.DataFrame)):
        return data.rolling(window).mean()

    # --- list case ---
    if len(data) < window:
        raise Exception(
            f"data length should be greater than window. currently {len(data)} < {window}"
        )

    # Convert to numpy array for faster computation
    arr = np.asarray(data, dtype=float)

    # Prepare output list with leading NaNs
    sma = [np.nan] * (window - 1)

    # --- use cumulative sum for O(n) SMA ---
    # cumsum[i] = data[0] + data[1] + ... + data[i]
    csum = np.cumsum(arr)

    # Compute SMA from index "window-1" onward
    for i in range(window - 1, len(arr)):
        # sum of last "window" values:
        # csum[i] - csum[i-window]   except when i==window-1
        total = csum[i] - (csum[i - window] if i >= window else 0)
        sma.append(total / window)

    return sma


def update_macd(new_tick, short_ema_value, long_ema_value, column="Close", short_window=12, long_window=26):
    """caliculate latest macd with new ohlc data and previouse ema values
    you need to caliculate Signal on yourself if need it
    Args:
        new_tick (pd.Series): latest ohlc tick. length should be 1
        short_ema_value (float): previouse ema value of short window (fast ema)
        long_ema_value (float): previouse ema value of long window (late ema)
        column (str, optional): Defaults to 'Close'.
        short_window (int, optional):  Defaults to 12.
        long_window (int, optional): Defaults to 26.

    Returns:
        tuple(float, float, float): short_ema, long_ema, macd
    """
    new_data = new_tick[column]
    short_alpha = 2 / (short_window + 1)
    long_alpha = 2 / (long_window + 1)

    # TODO replace here from hard code to function
    new_short_ema = short_ema_value * (1 - short_alpha) + new_data * short_alpha
    new_long_ema = long_ema_value * (1 - long_alpha) + new_data * long_alpha
    new_macd = new_short_ema - new_long_ema

    return new_short_ema, new_long_ema, new_macd


def update_ema(new_tick, ema_value, window, column="Close"):
    new_data = new_tick[column]
    alpha = 2 / (window + 1)

    new_ema = ema_value * (1 - alpha) + new_data * alpha
    return new_ema


def MACDFromOHLC(
    data: pd.DataFrame,
    column: str = "Close",
    short_window: int = 12,
    long_window: int = 26,
    signal_window: int = 9,
    short_ema_name: str = "ShortEMA",
    long_ema_name: str = "LongEMA",
    macd_name: str = "MACD",
    signal_name: str = "Signal",
):
    """
    Calculate MACD and Signal indicators from OHLC data using EMA.

    Args:
        data (pd.DataFrame): OHLC data containing the target column.
        column (str, optional): Column name used for MACD calculation. Defaults to "Close".
        short_window (int, optional): Window size for short EMA (MACD fast line). Defaults to 12.
        long_window (int, optional): Window size for long EMA (MACD slow line). Defaults to 26.
        signal_window (int, optional): Window size for Signal EMA. Defaults to 9.
        short_ema_name (str, optional): Output column name for short EMA. Defaults to "ShortEMA".
        long_ema_name (str, optional): Output column name for long EMA. Defaults to "LongEMA".
        macd_name (str, optional): Output column name for MACD line. Defaults to "MACD".
        signal_name (str, optional): Output column name for Signal line. Defaults to "Signal".

    Returns:
        pd.DataFrame: A DataFrame containing ShortEMA, LongEMA, MACD, and Signal columns.
    """

    # --- Calculate EMA values (list or pandas Series depending on EMA implementation) ---
    short_ema = EMA(data[column], short_window)
    long_ema = EMA(data[column], long_window)

    # --- Compute MACD and Signal lines ---
    # MACD = ShortEMA - LongEMA
    macd = []
    for s, l in zip(short_ema, long_ema):
        macd.append(s - l)

    # Signal line = EMA(MACD)
    signal = EMA(macd, signal_window)

    # --- Normalize to pandas Series ---
    # If EMA returned a list, convert to Series with the same index
    def to_series(obj):
        if isinstance(obj, (pd.Series, pd.DataFrame)):
            return obj
        return pd.Series(obj, index=data.index)

    short_ema_s = to_series(short_ema)
    long_ema_s = to_series(long_ema)
    macd_s = to_series(macd)
    signal_s = to_series(signal)

    # --- Create DataFrame ---
    macd_df = pd.concat(
        [short_ema_s, long_ema_s, macd_s, signal_s],
        axis=1
    )
    macd_df.columns = [
        short_ema_name,
        long_ema_name,
        macd_name,
        signal_name,
    ]

    return macd_df


def MACDFromEMA(short_ema, long_ema, signal_window):
    """
    caliculate MACD and Signal indicaters from EMAs.
    output: macd, signal
    """
    if type(short_ema) == pd.Series and type(long_ema) == pd.Series:
        macd = short_ema - long_ema
    else:
        macd = [x - y for (x, y) in zip(short_ema, long_ema)]
    signal = SMA(macd, signal_window)
    return macd, signal


def MACDFromOHLCMulti(
    symbols: list,
    data: pd.DataFrame,
    column="Close",
    short_window=12,
    long_window=26,
    signal_window=9,
    grouped_by_symbol=False,
    short_ema_name="ShortEMA",
    long_ema_name="LongEMA",
    macd_name="MACD",
    signal_name="Signal",
):
    """caliculate MACD and Signal indicaters from OHLC. Close is used by default.

    Args:
        symbols (list<str>): symbol list. Each element should match with column.
        data (pd.DataFrame): ohlc data of symbols
        column (str, optional): target column name. Defaults to 'Close'.
        short_window (int, optional): window size for short EMA. Defaults to 12.
        long_window (int, optional): window size for long EMA. Defaults to 26.
        signal_window (int, optional): window size for Signals. Defaults to 9.
        grouped_by_symbol (bool, optional): If True, return a result with (symbol, column). Defaults to False.

    Returns:
        pd.DataFrame: DataFrame of ShortEMA, LongEMA, MACD and Signal for symbols
    """
    df = data.copy()
    if grouped_by_symbol is False:
        df.columns = df.columns.swaplevel(0, 1)

    df = df[symbols]
    short_ema = EMA(df[[(symbol, column) for symbol in symbols]], short_window)
    long_ema = EMA(df[[(symbol, column) for symbol in symbols]], long_window)
    macd = short_ema - long_ema
    signal = SMA(macd, signal_window)

    elements, columns = create_multi_out_lists(
        symbols, [short_ema, long_ema, macd, signal], [short_ema_name, long_ema_name, macd_name, signal_name], grouped_by_symbol
    )
    macd_df = pd.concat(elements, axis=1)
    macd_df.columns = columns
    macd_df.sort_index(level=0, axis=1, inplace=True)
    return macd_df


def BollingerFromSeries(data: pd.Series, window=14, alpha=2):
    """
    Calculate Bollinger Bands from a pandas Series.

    Args:
        data (pd.Series): Target time series.
        window (int, optional): Window size for moving average and standard deviation. Defaults to 14.
        alpha (float, optional): Number of standard deviations for upper/lower bands. Defaults to 2.

    Returns:
        tuple: (middle_band, upper_band, lower_band, width, std)
    """
    # rolling std and mean (ddof=0 matches pandas TA convention)
    stds = data.rolling(window).std(ddof=0)
    mas = data.rolling(window).mean()

    # Bollinger bands
    b_high = mas + stds * alpha
    b_low = mas - stds * alpha

    # band width (difference between upper and lower)
    width = b_high - b_low

    return mas, b_high, b_low, width, stds


def BollingerFromArray(data, window=14, alpha=2):
    """
    Calculate Bollinger Bands from a list-like object.

    Args:
        data (list): List of numeric values.
        window (int, optional): Window size. Defaults to 14.
        alpha (float, optional): Number of std deviations. Defaults to 2.

    Returns:
        tuple: (middle_band, upper_band, lower_band, width, std)
    """
    # Convert to pandas Series
    if isinstance(data, list):
        data = pd.Series(data)
    else:
        raise Exception(f"Unsupported type {type(data)} in BollingerFromArray")

    return BollingerFromSeries(data, window=window, alpha=alpha)


def BollingerFromOHLC(
    data: pd.DataFrame,
    column="Close",
    window=14,
    alpha=2,
    mean_name="B_MA",
    upper_name="B_High",
    lower_name="B_Low",
    width_name="B_Width",
    std_name="B_Std",
):
    """
    Calculate Bollinger Bands from an OHLC DataFrame.

    Args:
        data (pd.DataFrame): OHLC data for a symbol.
        column (str, optional): Column name to calculate the bands from. Defaults to 'Close'.
        window (int, optional): Window size. Defaults to 14.
        alpha (float, optional): Number of std deviations. Defaults to 2.
        mean_name (str, optional): Output column name for middle band.
        upper_name (str, optional): Output column name for upper band.
        lower_name (str, optional): Output column name for lower band.
        width_name (str, optional): Output column name for band width.
        std_name (str, optional): Output column name for std.

    Returns:
        pd.DataFrame: DataFrame containing Bollinger Bands.
    """
    # Calculate Bollinger Bands from the selected column
    ma, b_high, b_low, width, stds = BollingerFromSeries(
        data[column], window=window, alpha=alpha
    )

    # Combine results into a DataFrame
    b_df = pd.concat([ma, b_high, b_low, width, stds], axis=1)
    b_df.columns = (mean_name, upper_name, lower_name, width_name, std_name)

    return b_df


def BollingerFromOHLCMulti(
    symbols: list,
    data: pd.DataFrame,
    column="Close",
    window=14,
    alpha=2,
    grouped_by_symbol=False,
    mean_name="B_MA",
    upper_name="B_High",
    lower_name="B_Low",
    width_name="B_Width",
    std_name="B_Std",
):
    """Calculate Bollinger band from ohlc dataframe for symbols

    Args:
        symbols (list<str>): symbol list. Each element should match with column.
        data (pd.DataFrame): ohlc data of symbols
        column (str, optional): target column name. Defaults to 'Close'.
        window (int, optional): window size for bollinger band. Defaults to 14.
        alpha (int, optional): alph to caliculate band. Defaults to 2.
        grouped_by_symbol (bool, optional): If True, return a result with (symbol, column). Defaults to False.

    Returns:
        pd.DataFrame: B_MA, B_Hig, B_Low, B_Width, B_Std for symbols
    """
    df = data.copy()
    if grouped_by_symbol is False:
        df.columns = df.columns.swaplevel(0, 1)

    ohlc_dfs = df[[(symbol, column) for symbol in symbols]]

    ma, b_high, b_low, width, stds = BollingerFromSeries(ohlc_dfs, window=window, alpha=alpha)
    elements, columns = create_multi_out_lists(
        symbols, [ma, b_high, b_low, width, stds], [mean_name, upper_name, lower_name, width_name, std_name], grouped_by_symbol
    )
    b_df = pd.concat(elements, axis=1)
    b_df.columns = columns
    return b_df


def ATRFromMultiOHLC(
    symbols: list,
    data: pd.DataFrame,
    ohlc_columns=("Open", "High", "Low", "Close"),
    window=14,
    grouped_by_symbol=False,
    tr_name="TR",
    atr_name="ATR",
):
    high_cn = ohlc_columns[1]
    low_cn = ohlc_columns[2]
    close_cn = ohlc_columns[3]

    df = data.copy()
    if grouped_by_symbol:
        df.columns = df.columns.swaplevel(0, 1)

    df = df[[(column, symbol) for symbol in symbols for column in ohlc_columns]]

    hl_df = df[high_cn] - df[low_cn]
    hpc_df = abs(df[high_cn] - df[close_cn].shift(1))
    lpc_df = abs(df[low_cn] - df[close_cn].shift(1))

    temp_df = pd.concat([hl_df, hpc_df, lpc_df], axis=1)
    trs = {}
    for symbol in symbols:
        trs[symbol] = temp_df[symbol].max(axis=1)
    tr_df = pd.concat(trs.values(), keys=trs.keys(), axis=1)
    atr_df = EMA(tr_df, window)
    elements, columns = create_multi_out_lists(symbols, [tr_df, atr_df], [tr_name, atr_name], grouped_by_symbol)
    out_df = pd.concat(elements, axis=1)
    out_df.columns = columns
    out_df.sort_index(level=0, axis=1, inplace=True)
    return out_df


def ATRFromOHLC(
    data: pd.DataFrame,
    ohlc_columns=("Open", "High", "Low", "Close"),
    window=14,
    tr_name="TR",
    atr_name="ATR"
):
    """
    Calculate True Range (TR) and Average True Range (ATR).

    Args:
        data (pd.DataFrame): OHLC data.
        ohlc_columns (tuple, optional): Column names for (Open, High, Low, Close). 
                                        Defaults to ('Open', 'High', 'Low', 'Close').
        window (int, optional): Window size for ATR. Defaults to 14.
        tr_name (str, optional): Output column name for True Range. Defaults to "TR".
        atr_name (str, optional): Output column name for ATR. Defaults to "ATR".

    Returns:
        pd.DataFrame or pd.Series:
            If tr_name is not None:
                Returns DataFrame containing TR and ATR.
            Otherwise:
                Returns ATR as a Series.
    """
    high_cn = ohlc_columns[1]
    low_cn = ohlc_columns[2]
    close_cn = ohlc_columns[3]

    df = data.copy()

    # --- True Range components ---
    # High - Low
    df["H-L"] = df[high_cn] - df[low_cn]

    # |High - Previous Close|
    df["H-PC"] = abs(df[high_cn] - df[close_cn].shift(1))

    # |Low - Previous Close|
    df["L-PC"] = abs(df[low_cn] - df[close_cn].shift(1))

    # True Range = max(H-L, H-PC, L-PC)
    df[tr_name] = df[["H-L", "H-PC", "L-PC"]].max(axis=1)

    # --- ATR using EMA (Wilder's method uses alpha = 1/window) ---
    # Your EMA function is used here.
    df[atr_name] = EMA(df[tr_name], interval=window)

    # --- Return result ---
    if tr_name is not None:
        return df[[tr_name, atr_name]].copy()
    else:
        return df[atr_name].copy()


def update_ATR(
    pre_data: pd.Series, new_data: pd.Series, ohlc_columns=("Open", "High", "Low", "Close"), atr_column="ATR", window=14
):
    """latest caliculate atr

    Args:
        pre_data (pd.Series): ohlc + ATR
        new_data (pd.Series): ohlc
        ohlc_columns (tuple, optional): Defaults to ('Open', 'High', 'Low', 'Close').
        window (int, optional): Defaults to 14.

    Returns:
        float: new atr value
    """
    high_cn = ohlc_columns[1]
    low_cn = ohlc_columns[2]
    close_cn = ohlc_columns[3]
    pre_tr = pre_data[atr_column]

    hl = new_data[high_cn] - new_data[low_cn]
    hpc = abs(new_data[high_cn] - pre_data[close_cn])
    lpc = abs(new_data[low_cn] - pre_data[close_cn])
    tr = max([hl, hpc, lpc])
    atr = update_EMA(last_ema_value=pre_tr, new_value=tr, window=window)
    return atr


def RSIFromOHLC(
    data: pd.DataFrame, column="Close", window=14, mean_gain_name="AvgGain", mean_loss_name="AvgLoss", rsi_name="Rsi"
):
    """
    RSI is a momentum oscillator which measures the speed and change od price movements

    Args:
        data (pd.DataFrame): ohlc time series data
        column (str, optional): Defaults to 'Close'.
        window (int, optional): Defaults to 14.

    Returns:
        pd.DataFrame: 0 to 100
    """
    df = data.copy()
    df["change"] = df[column].diff()
    df["gain"] = np.where(df["change"] >= 0, df["change"], 0)
    df["loss"] = np.where(df["change"] < 0, -1 * df["change"], 0)
    avgain_df = (
        df["gain"].ewm(alpha=1 / window, adjust=False).mean()
    )  # tradeview said exponentially weighted moving average with aplpha = 1/length is used
    avgloss_df = df["loss"].ewm(alpha=1 / window, adjust=False).mean()
    rs_df = avgain_df / avgloss_df.replace(0, np.nan)
    rsi_df = 100 - (100 / (1 + rs_df))
    rsi_df = rsi_df.fillna(100)  # avgloss=0 → RS=∞ → RSI=100
    elements, columns = __create_out_lists([avgain_df, avgloss_df, rsi_df], [mean_gain_name, mean_loss_name, rsi_name])
    out_df = pd.concat(elements, axis=1)
    out_df.columns = columns
    return out_df


def RSIFromOHLCMulti(
    symbols: list,
    data: pd.DataFrame,
    column="Close",
    window=14,
    grouped_by_symbol=False,
    mean_gain_name="avgGain",
    mean_loss_name="avgLoss",
    rsi_name="rsi",
):
    df = data.copy()
    if grouped_by_symbol:
        df.columns = df.columns.swaplevel(0, 1)

    diff_df = df[column][symbols].diff()
    gain_df = diff_df[diff_df >= 0]
    gain_df[gain_df.isna()] = 0
    loss_df = -diff_df[diff_df < 0]
    loss_df[loss_df.isna()] = 0

    avgain_df = gain_df.ewm(alpha=1 / window, adjust=False).mean()
    avgloss_df = loss_df.ewm(alpha=1 / window, adjust=False).mean()
    rs_df = avgain_df / avgloss_df
    rsi_df = 100 - (100 / (1 + rs_df))
    elements, columns = create_multi_out_lists(
        symbols, [avgain_df, avgloss_df, rsi_df], [mean_gain_name, mean_loss_name, rsi_name], grouped_by_symbol
    )
    out_df = pd.concat(elements, axis=1)
    out_df.columns = columns
    out_df.sort_index(level=0, axis=1, inplace=True)
    return out_df


def update_RSI(pre_data: pd.Series, new_data: pd.Series, columns=("avgGain", "avgLoss", "rsi", "Close"), window=14):
    """caliculate lastest RSI

    Args:
        pre_data (pd.Series): assume "avgGain", "avgLoss", "rsi" and target_column are available
        new_data (pd.Series): assume [column] is available
        columns (tuple(str), optional): Defaults to ("avgGain", "avgLoss", "rsi", "Close").
        window (int, optional): alpha=1/window. Defaults to 14.
    """
    c_again = columns[0]
    c_aloss = columns[1]
    c_rsi = columns[2]
    t_column = columns[3]

    change = new_data[t_column] - pre_data[t_column]
    gain = 0
    loss = 0
    if change >= 0:
        gain = change
    else:
        loss = change

    avgGain = update_EMA(pre_data[c_again], gain, window=-1, a=1 / window)
    avgLoss = update_EMA(pre_data[c_aloss], loss, window=-1, a=1 / window)
    rs = avgGain / avgLoss
    rsi = 100 - (100 / (1 + rs))
    return avgGain, avgLoss, rsi


def RenkoFromSeries(data: pd.Series, brick_size, total_brick_name="Renko", brick_size_name="BrickSize"):
    """Caliculate brick number of Renko

    Args:
        data_sr (pd.Series): time series data like close values of a symbol
        brick_size (pd.Series|float): brick_size to caliculate the Renko.

    Returns:
        pd.DataFrame: renko bricks and brick values
    """

        # --- prepare brick_size as series ---
    if isinstance(brick_size, pd.Series):
        if len(brick_size) != len(data):
            raise Exception("brick_size must have same length as data")
        bs = brick_size.reset_index(drop=True).astype(float)
    else:
        bs = pd.Series(float(brick_size), index=data.index)

    prices = data.reset_index(drop=True).astype(float)
    n = len(prices)

    # output series     
    renko_bricks = pd.Series(0, index=prices.index, dtype=float)
    brick_values = pd.Series(0.0, index=prices.index, dtype=float)

    # --- find first valid starting price ---
    try:
        idx0 = prices[pd.notna(prices)].index[0]
    except IndexError:
        return pd.DataFrame({total_brick_name: renko_bricks, brick_size_name: brick_values})

    # initial reference
    ref_price = prices.iloc[idx0]
    current_brick = 0  # cumulative brick count

    # --- main loop ---
    for i in range(idx0, n):
        price = prices.iloc[i]
        bsize = bs.iloc[i]

        if np.isnan(price):
            renko_bricks.iloc[i] = current_brick
            brick_values.iloc[i] = np.nan
            continue

        # --- continuous brick value ---
        diff = price - ref_price
        brick_values.iloc[i] = diff / bsize

        if diff >= bsize:
            try:
                bricks_up = int(diff // bsize)
            except ZeroDivisionError:
                bricks_up = 0
            except ValueError:
                bricks_up = 0
            for _ in range(bricks_up):
                current_brick += 1
                ref_price += bsize

        elif diff <= -bsize:
            bricks_down = int((-diff) // bsize)
            for _ in range(bricks_down):
                if current_brick >= 0:  # upward
                    if current_brick == 0:
                        # reverse direction: 0 -> -1 brick
                        if diff <= -2 * bsize:
                            current_brick = -1
                            ref_price -= 2 * bsize
                    else:
                        # reverse direction: +n -> -n brick
                        current_brick -= 1
                        ref_price -= bsize
                else:
                    # reverse direction: already downward → continue stacking
                    current_brick -= 1
                    ref_price -= bsize

        # update output
        renko_bricks.iloc[i] = current_brick

    # restore index
    out = pd.DataFrame({
        total_brick_name: renko_bricks.set_axis(data.index),
        brick_size_name: brick_values.set_axis(data.index)
    })
    return out


def RenkoFromOHLC(
    df: pd.DataFrame,
    ohlc_columns=("Open", "High", "Low", "Close"),
    brick_column_name=None,
    brick_size=None,
    atr_window=14,
    total_brick_name="Renko",
    brick_size_name="Brick",
):
    """Caliculate Renko from Close column of ohlc dataframe

    Args:
        df (pd.DataFrame): time series data of Open, High, Low and Close
        ohlc_columns (tuple, optional): columns names of OHLC. Defaults to ('Open', 'High', 'Low', 'Close').
        brick_column_name (str, optional): column name of brick_size. Defaults to None.
        brick_size (pd.Series|float, optional): brick_size to caliculate the Renko. If None, ATR is used. Defaults to None.

    Raises:
        Exception: When ohlc_columns is not a subset of df.columns

    Returns:
        pd.DataFrame: brick numbers are stored on brick_num column
    """
    if set(ohlc_columns).issubset(df.columns):
        if brick_size is None:
            if brick_column_name is None:
                atr_df = ATRFromOHLC(df, ohlc_columns, window=atr_window, atr_name="ATR")
                brick_size = atr_df["ATR"]
            else:
                brick_size = df[brick_column_name]
            renko_df = RenkoFromSeries(
                df[ohlc_columns[3]], brick_size=brick_size, total_brick_name=total_brick_name, brick_size_name=brick_size_name
            )
        else:
            renko_df = RenkoFromSeries(
                df[ohlc_columns[3]], brick_size=brick_size, total_brick_name=total_brick_name, brick_size_name=brick_size_name
            )
        return renko_df
    else:
        raise Exception(f"specified ohlc_columns {ohlc_columns} doen't match with df.columns {df.columns}")


def RenkoFromMultiOHLC(
    symbols: list,
    dfs: pd.DataFrame,
    ohlc_columns=("Open", "High", "Low", "Close"),
    brick_size_column=None,
    brick_size=None,
    atr_window=14,
    grouped_by_symbol=False,
    total_brick_name="Renko",
    brick_size_name="Brick",
):
    """Caliculate Renko from Close column of ohlc dataframe of symbols

    Args:
        symbols (list): list of symbol names. It should match with column name of dfs
        dfs (pd.DataFrame): ohlc data of symbols.
        ohlc_columns (tuple, optional): columns names of OHLC. Defaults to ('Open', 'High', 'Low', 'Close').
        brick_size_column (str, optional): column name of brick_size. Defaults to None.
        brick_size (pd.DataFrame|float, optional): brick_size to caliculate the Renko. If None, ATR is used. Defaults to None.
        grouped_by_symbol (bool, optional): Flag for group handling of Input and Output. Defaults to False.

    Raises:
        Exception: _description_

    Returns:
        _type_: _description_
    """
    is_series_brick = False
    if brick_size is None:
        if brick_size_column is None:
            atr_dfs = ATRFromMultiOHLC(
                symbols, dfs, ohlc_columns, window=atr_window, grouped_by_symbol=grouped_by_symbol, atr_name="ATR"
            )
            if grouped_by_symbol:
                brick_sizes = atr_dfs[[(symbol, "ATR") for symbol in symbols]]
                brick_sizes.columns = symbols
            else:
                brick_sizes = atr_dfs["ATR"]
            is_series_brick = True
        else:
            if type(brick_size_column) is str:
                if grouped_by_symbol:
                    brick_sizes = dfs[[(symbol, brick_size_column) for symbol in symbols]]
                else:
                    brick_sizes = dfs[brick_size_column]
                is_series_brick = True
            else:
                raise Exception(f"brick_column_name should be str. {type(brick_size_column)} is provided.")
    DFS = {}
    if grouped_by_symbol:
        for symbol in symbols:
            if is_series_brick:
                DFS[symbol] = RenkoFromOHLC(
                    dfs[symbol],
                    ohlc_columns,
                    brick_size=brick_sizes[symbol],
                    total_brick_name=total_brick_name,
                    brick_size_name=brick_size_name,
                )
            else:
                DFS[symbol] = RenkoFromOHLC(
                    dfs[symbol],
                    ohlc_columns,
                    brick_size=brick_size,
                    total_brick_name=total_brick_name,
                    brick_size_name=brick_size_name,
                )
    else:
        for symbol in symbols:
            _ohlc_columns = [(column, symbol) for column in ohlc_columns]
            ohlc_df = dfs[_ohlc_columns]
            if is_series_brick:
                DFS[symbol] = RenkoFromOHLC(
                    ohlc_df,
                    _ohlc_columns,
                    brick_size=brick_sizes[symbol],
                    total_brick_name=total_brick_name,
                    brick_size_name=brick_size_name,
                )
            else:
                DFS[symbol] = RenkoFromOHLC(
                    ohlc_df,
                    _ohlc_columns,
                    brick_size=brick_size,
                    total_brick_name=total_brick_name,
                    brick_size_name=brick_size_name,
                )

    RenkoDF = pd.concat(DFS.values(), axis=1, keys=DFS.keys())
    if grouped_by_symbol is False:
        RenkoDF.columns = RenkoDF.columns.swaplevel(0, 1)
        RenkoDF.sort_index(level=0, axis=1, inplace=True)
    # RenkoDF = pd.DataFrame.from_dict(DFS)
    return RenkoDF


def SlopeFromSeries(ser: pd.Series, window: int):
    """function to calculate the slope of n consecutive points on a plot

    Args:
        ser (pd.Series): time series data
        window (int): window size for the slope

    Returns:
        pd.Series: slope values
    """
    index = ser.index
    def calc_slope(x):
        slope = np.polyfit(range(len(x)), x, 1)[0]
        return slope
    
    # raw=True to pass numpy array to calc_slope fast
    slope_ser = ser.rolling(window=window).apply(calc_slope, raw=True)
    slope_ser.index = index
    return slope_ser


def SlopeFromOHLC(ohlc_df: pd.DataFrame, window: int, column="Close", slope_name="Slope"):
    """function to calculate the slope of n consecutive points on a plot

    Args:
        ser (pd.DataFrame): OHLC time series data of a symbol
        window (int): window size for the slope
        column (str): target column name

    Returns:
        pd.DataFrame: slope value on Slope column
    """
    slope_sr = SlopeFromSeries(ohlc_df[column], window)
    return pd.DataFrame(slope_sr.values, columns=[slope_name], index=ohlc_df.index)


def SlopeFromOHLCMulti(
    symbols: list, ohlc_dfs: pd.DataFrame, window: int, column: str = "Close", grouped_by_sygnal: bool = False, slope_name="Slope"
):
    """function to calculate the slope of n consecutive points on a plot

    Args:
        symbols (list<str>): symbol list. Each element should match with column.
        ser (pd.DataFrame): OHLC time series data of symbols
        window (int): window size for the slope
        column (str): target column name
        grouped_by_symbol (bool, optional): Flag for group handling of Input and Output. Defaults to False.

    Returns:
        pd.DataFrame: slope value on Slope column
    """
    DFS = {}
    if grouped_by_sygnal:
        for symbol in symbols:
            DFS[symbol] = SlopeFromOHLC(ohlc_dfs, window, (symbol, column), slope_name)
    else:
        for symbol in symbols:
            DFS[symbol] = SlopeFromOHLC(ohlc_dfs, window, (column, symbol), (slope_name, symbol))
    slope_dfs = pd.concat(DFS.values(), axis=1, keys=DFS.keys())
    if grouped_by_sygnal is False:
        slope_dfs.columns = slope_dfs.columns.swaplevel(0, 1)
    return slope_dfs


def __CCI(ohlc: pd.DataFrame, window=14, ohlc_columns=("Open", "High", "Low", "Close")) -> pd.Series:
    """
    Internal function to calculate Commodity Channel Index (CCI)
    
    Args:
        ohlc (pd.DataFrame): OHLC data
        window (int, optional): window size for moving average. Defaults to 14.
        ohlc_columns (tuple, optional): names of OHLC columns. Defaults to ("Open", "High", "Low", "Close")
    
    Returns:
        pd.Series: CCI values
    """
    # Typical Price: (High + Low + Close) / 3
    tp = (ohlc[ohlc_columns[1]] + ohlc[ohlc_columns[2]] + ohlc[ohlc_columns[3]]) / 3
    
    # SMA of typical price
    ma_tp = tp.rolling(window).mean()
    
    # Mean deviation
    md = tp.rolling(window).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
    
    # CCI calculation
    cci = (tp - ma_tp) / (0.015 * md)
    return cci

def CommodityChannelIndex(
    ohlc: pd.DataFrame,
    window: int = 14,
    ohlc_columns: tuple = ("Open", "High", "Low", "Close"),
    cci_name: str = "CCI"
) -> pd.DataFrame:
    """
    Calculate Commodity Channel Index (CCI) for a symbol.
    CCI measures how far the typical price deviates from its moving average.
    Typically, CCI > 100 indicates strong uptrend, CCI < -100 indicates strong downtrend.
    
    Args:
        ohlc (pd.DataFrame): OHLC data
        window (int, optional): window size for CCI. Defaults to 14.
        ohlc_columns (tuple, optional): names of OHLC columns. Defaults to ("Open", "High", "Low", "Close").
        cci_name (str, optional): output column name for CCI. Defaults to "CCI".
    
    Returns:
        pd.DataFrame: DataFrame with single column for CCI
    """
    cci_series = __CCI(ohlc, window, ohlc_columns)
    return pd.DataFrame(cci_series, columns=[cci_name])


def CommodityChannelIndexMulti(
    symbols: list,
    ohlc: pd.DataFrame,
    window=14,
    ohlc_columns=("Open", "High", "Low", "Close"),
    grouped_by_sygnal: bool = False,
    cci_name="CCI",
) -> pd.DataFrame:
    """represents how much close value is far from mean value. If over 100, strong long trend for example.

    Args:
        symbols (list<str>): symbol list. Each element should match with column.
        ohlc (pd.DataFrame): Open High Low Close values
        window (int, optional): window size to caliculate EMA. Defaults to 14.
        ohlc_columns (tuple, optional): tuple of Open High Low Close column names. Defaults to ('Open', 'High', 'Low', 'Close').
        grouped_by_symbol (bool, optional): Flag for group handling of Input and Output. Defaults to False.

    Returns:
        pd.DataFrame: CCI value on CCI column
    """
    ohlc = ohlc.copy()
    if grouped_by_sygnal:
        ohlc.columns = ohlc.columns.swaplevel(0, 1)

    ohlc = ohlc[[(column, symbol) for symbol in symbols for column in ohlc_columns]]

    cci = __CCI(ohlc, window, ohlc_columns)
    cci.columns = pd.MultiIndex.from_tuples([(cci_name, symbol) for symbol in symbols])

    if grouped_by_sygnal:
        cci.columns = cci.columns.swaplevel(0, 1)

    return cci


def ADXFromOHLC(data: pd.DataFrame, window=14, ohlc_columns=("Open","High","Low","Close"),
                plus_di_name="+DI", minus_di_name="-DI", adx_name="ADX") -> pd.DataFrame:
    """
    Calculate ADX (Average Directional Index) and +DI / -DI indicators.

    Args:
        data (pd.DataFrame): OHLC time series
        window (int, optional): lookback window for calculation. Defaults to 14.
        ohlc_columns (tuple, optional): OHLC column names. Defaults to ("Open","High","Low","Close").
        plus_di_name (str, optional): output column name for +DI. Defaults to "+DI".
        minus_di_name (str, optional): output column name for -DI. Defaults to "-DI".
        adx_name (str, optional): output column name for ADX. Defaults to "ADX".

    Returns:
        pd.DataFrame: +DI, -DI, ADX
    """
    high = data[ohlc_columns[1]]
    low = data[ohlc_columns[2]]

    # True Range
    tr = ATRFromOHLC(data, ohlc_columns=ohlc_columns, window=1, tr_name="TR", atr_name="TR_temp")["TR_temp"]

    # directional movements
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)

    # smooth DM and TR
    plus_di = 100 * EMA(pd.Series(plus_dm), window) / EMA(tr, window)
    minus_di = 100 * EMA(pd.Series(minus_dm), window) / EMA(tr, window)

    # DX and ADX
    dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di)
    adx = EMA(pd.Series(dx), window)

    return pd.DataFrame({plus_di_name: plus_di, minus_di_name: minus_di, adx_name: adx})

def StochasticOscillatorFromOHLC(data: pd.DataFrame, k_window=14, d_window=3,
                                 ohlc_columns=("Open","High","Low","Close"),
                                 k_name="%K", d_name="%D") -> pd.DataFrame:
    """
    Calculate Stochastic Oscillator (%K and %D) from OHLC data.

    Args:
        data (pd.DataFrame): OHLC time series
        k_window (int, optional): lookback window for %K. Defaults to 14.
        d_window (int, optional): moving average window for %D. Defaults to 3.
        ohlc_columns (tuple, optional): OHLC column names. Defaults to ("Open","High","Low","Close").
        k_name (str, optional): column name for %K. Defaults to "%K".
        d_name (str, optional): column name for %D. Defaults to "%D".

    Returns:
        pd.DataFrame: %K and %D
    """
    high = data[ohlc_columns[1]]
    low = data[ohlc_columns[2]]
    close = data[ohlc_columns[3]]

    lowest_low = low.rolling(k_window).min()
    highest_high = high.rolling(k_window).max()

    k = 100 * (close - lowest_low) / (highest_high - lowest_low)
    d = k.rolling(d_window).mean()

    return pd.DataFrame({k_name: k, d_name: d})

def ParabolicSARFromOHLC(data: pd.DataFrame, ohlc_columns=("Open","High","Low","Close"),
                          af_start=0.02, af_increment=0.02, af_max=0.2,
                          sar_name="ParabolicSAR") -> pd.DataFrame:
    """
    Calculate Parabolic SAR for OHLC data.

    Args:
        data (pd.DataFrame): OHLC time series
        ohlc_columns (tuple, optional): OHLC column names. Defaults to ("Open","High","Low","Close").
        af_start (float, optional): initial acceleration factor. Defaults to 0.02.
        af_increment (float, optional): step increment for AF. Defaults to 0.02.
        af_max (float, optional): maximum AF. Defaults to 0.2.
        sar_name (str, optional): output column name. Defaults to "ParabolicSAR".

    Returns:
        pd.DataFrame: Parabolic SAR series
    """
    high = data[ohlc_columns[1]].values
    low = data[ohlc_columns[2]].values
    n = len(data)
    sar = np.zeros(n)

    # initial trend
    uptrend = True if high[1] > high[0] else False
    ep = low[0] if not uptrend else high[0]  # extreme point
    af = af_start
    sar[0] = low[0] if uptrend else high[0]

    for i in range(1, n):
        sar[i] = sar[i-1] + af * (ep - sar[i-1])
        if uptrend:
            if low[i] < sar[i]:
                uptrend = False
                sar[i] = ep
                ep = low[i]
                af = af_start
            else:
                if high[i] > ep:
                    ep = high[i]
                    af = min(af + af_increment, af_max)
        else:
            if high[i] > sar[i]:
                uptrend = True
                sar[i] = ep
                ep = high[i]
                af = af_start
            else:
                if low[i] < ep:
                    ep = low[i]
                    af = min(af + af_increment, af_max)

    return pd.DataFrame({sar_name: sar}, index=data.index)


def bearish_engulfing(df, open_column, close_column):
    prev = df.shift(1)
    return (
        (prev[close_column] > prev[open_column]) &
        (df[close_column] < df[open_column]) &
        (df[open_column] > prev[close_column]) &
        (df[close_column] < prev[open_column])
    )

def bullish_engulfing(df, open_column, close_column):
    prev = df.shift(1)
    return (
        (prev[close_column] < prev[open_column]) &
        (df[close_column] > df[open_column]) &
        (df[open_column] < prev[close_column]) &
        (df[close_column] > prev[open_column])
    )

def bearish_pinbar(df, ohlc_columns, ratio=2.0):  # 上ヒゲが長い陰性寄り
    open_column, high_column, low_column, close_column = ohlc_columns
    body = (df[close_column] - df[open_column]).abs()
    upper = df[high_column] - df[[open_column, close_column]].max(axis=1)
    lower = df[[open_column, close_column]].min(axis=1) - df[low_column]
    return (upper > body * ratio) & (lower < body) & (df[close_column] <= df[open_column])

def bullish_pinbar(df, ohlc_columns, ratio=2.0):  # 下ヒゲが長い陽性寄り
    open_column, high_column, low_column, close_column = ohlc_columns
    body = (df[close_column] - df[open_column]).abs()
    lower = df[[open_column, close_column]].min(axis=1) - df[low_column]
    upper = df[high_column] - df[[open_column, close_column]].max(axis=1)
    return (lower > body * ratio) & (upper < body) & (df[close_column] >= df[open_column])

def bearish_outside(df, ohlc_columns):
    open_column, high_column, low_column, close_column = ohlc_columns
    prev = df.shift(1)
    return (df[high_column] > prev[high_column]) & (df[low_column] < prev[low_column]) & (df[close_column] < df[open_column])

def bullish_outside(df, ohlc_columns):
    open_column, high_column, low_column, close_column = ohlc_columns
    prev = df.shift(1)
    return (df[high_column] > prev[high_column]) & (df[low_column] < prev[low_column]) & (df[close_column] > df[open_column])

# utilities for scoring
def _norm_clip(x, lo, hi):
    """x を [lo, hi] にクリップして 0..1 に正規化"""
    x = np.clip(x, lo, hi)
    return (x - lo) / (hi - lo)

def _trend_alignment(df, side: str, close_column, short_column, long_column):
    """
    トレンド整合性: 
    強気= Close>MA200 & MA20>MA200 でボーナス、弱気は逆。
    1.10, 1.00, 0.90 の3段で重みを返す。
    """
    above200 = df[close_column] > df[long_column]
    ma20_above = df[short_column] > df[long_column]

    if side == 'bull':
        good = above200 & ma20_above
        bad = (~above200) & (~ma20_above)
    else:  # bear
        good = (~above200) & (~ma20_above)
        bad = above200 & ma20_above

    w = pd.Series(1.00, index=df.index, dtype=float)
    w[good] = 1.10
    w[bad]  = 0.90
    return w

def _zone_confluence(df, high_column, low_column, zone=None):
    """
    ゾーン合致: 価格帯に触れていれば 1.10、近傍(±0.05円)なら 1.05、その他 1.00
    zone=(low, high) を想定。None なら 1.00。
    """
    if not zone:
        return pd.Series(1.00, index=df.index, dtype=float)
    zlo, zhi = zone
    touch = (df[high_column] >= zlo) & (df[low_column] <= zhi)
    near  = (~touch) & (
        ((zlo - df[high_column]).abs() <= 0.05) | ((df[low_column] - zhi).abs() <= 0.05)
    )
    w = pd.Series(1.00, index=df.index, dtype=float)
    w[near]  = 1.05
    w[touch] = 1.10
    return w

def _body_quality(df, body_column, atr_column):
    """実体/ATR を 0.5..2.0 にクリップして 0..1 正規化（大きいほど良い）"""
    ratio = (df[body_column] / df[atr_column]).replace([np.inf, -np.inf], np.nan)
    return _norm_clip(ratio, 0.5, 2.0)

def _closepos_quality(df, side: str, closepos_column):
    """
    終値の位置: 強気は高値寄り(=ClosePos高い)が良い、弱気は安値寄りが良い
    """
    cp = df[closepos_column].clip(0, 1)
    return cp if side == 'bull' else (1 - cp)

def _pinbar_shadow_quality(df, side: str, body_column, ohlc_columns, max_ratio=5.0):
    """ピンバー専用：長い側のヒゲ/実体 を 2..max_ratio で 0..1 に正規化"""
    body = df[body_column].replace(0, np.nan)
    open_columns, high_columns, low_columns, close_columns = ohlc_columns
    upper = df[high_columns] - df[[open_columns, close_columns]].max(axis=1)
    lower = df[[open_columns, close_columns]].min(axis=1) - df[low_columns]
    if side == 'bull':
        ratio = (lower / body)
    else:
        ratio = (upper / body)
    return _norm_clip(ratio, 2.0, max_ratio)

def _engulf_depth_quality(df, side: str, body_column, open_columns, close_columns, max_ratio=3.0):
    """
    包み足の「包み込みの力」：現足実体 / 前足実体 を 1..max_ratio で 0..1 に正規化
    """
    prev_body = (df[close_columns].shift(1) - df[open_columns].shift(1)).abs().replace(0, np.nan)
    ratio = (df[body_column] / prev_body)
    return _norm_clip(ratio, 1.0, max_ratio)

def score_price_action(df: pd.DataFrame, side: str, ohlc_columns, atr_column, short_ma_column, long_ma_column, zone=None) -> pd.DataFrame:
    """
    side: 'bull' or 'bear'
    zone: (low, high) or None
    戻り値: パターン別スコアと総合スコア
    """
    assert side in ('bull', 'bear')
    open_column, high_column, low_column, close_column = ohlc_columns
    BODY_COLUMN = 'Body'
    CLOSE_POS_COLUMN = 'ClosePos'
    # 実体サイズと終値の相対位置（0=安値寄り, 1=高値寄り）
    df[BODY_COLUMN] = (df[close_column] - df[open_column]).abs()
    df[CLOSE_POS_COLUMN] = (df[close_column] - df[low_column]) / (df[high_column] - df[low_column]).replace(0, np.nan)

    if side == 'bull':
        engulf = bullish_engulfing(df, open_column, close_column)
        pinbar = bullish_pinbar(df, ohlc_columns)
        outside = bullish_outside(df, ohlc_columns)
    else:
        engulf = bearish_engulfing(df, open_column, close_column)
        pinbar = bearish_pinbar(df, ohlc_columns)
        outside = bearish_outside(df, ohlc_columns)

    # サブスコア（0..1）
    body_q = _body_quality(df, body_column=BODY_COLUMN, atr_column=atr_column)
    close_q = _closepos_quality(df, side, CLOSE_POS_COLUMN)
    trend_w = _trend_alignment(df, side, close_column, short_ma_column, long_ma_column)
    zone_w  = _zone_confluence(df, high_column, low_column, zone)

    # パターン別の質（0..1）
    engulf_q = _engulf_depth_quality(df, side, BODY_COLUMN, open_column, close_column)
    pinbar_q = _pinbar_shadow_quality(df, side, BODY_COLUMN, ohlc_columns)

    # ベース重み
    base_w = {
        'engulf': 0.60,   # 包み足
        'outside': 0.55,  # アウトサイドバー
        'pinbar': 0.45    # ピンバー
    }

    # パターン別スコア（0..100換算前に 0..1 で作る）
    # 係数は経験則：body 0.4, close 0.2, 専用質 0.4（engulf/pinbar）。outside は専用質なしで body/close を厚めに。
    engulf_score01 = engulf.astype(float) * base_w['engulf'] * (
        0.4 * body_q + 0.2 * close_q + 0.4 * engulf_q
    )
    pinbar_score01 = pinbar.astype(float) * base_w['pinbar'] * (
        0.4 * body_q + 0.2 * close_q + 0.4 * pinbar_q
    )
    outside_score01 = outside.astype(float) * base_w['outside'] * (
        0.55 * body_q + 0.45 * close_q
    )

    # コンフルエンス重み適用
    confluence_w = trend_w * zone_w  # おおむね 0.90〜1.21
    engulf_score01 *= confluence_w
    pinbar_score01 *= confluence_w
    outside_score01 *= confluence_w

    # 総合スコア：最大を採用（「最も強いシグナル」を重視）
    total01 = np.maximum.reduce([engulf_score01, pinbar_score01, outside_score01])
    total = (total01 * 100).clip(0, 100)

    # 出力
    out = df.copy()
    out[f'{side}_engulfing'] = engulf
    out[f'{side}_pinbar'] = pinbar
    out[f'{side}_outside'] = outside
    out[f'{side}_engulf_score'] = (engulf_score01 * 100).round(1)
    out[f'{side}_pinbar_score'] = (pinbar_score01 * 100).round(1)
    out[f'{side}_outside_score'] = (outside_score01 * 100).round(1)
    out[f'{side}_confidence'] = total.round(1)
    return out