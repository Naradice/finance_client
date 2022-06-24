import numpy as np
import pandas as pd
from stocktrends import Renko
import statsmodels.api as sm
import copy

#TODO: unify output format

def sum(data):
    amount = 0
    if type(data) == list:
            amount = sum(data)
    else:#check more
        amount = data.sum()
    return amount

def revert_EMA(data, interval:int):
    """ revert data created by EMA function to row data

    Args:
        data (DataFrame or Series or list): data created by EMA function to row data
        interval (int): window size
    """
    
    if len(data) > interval:
        alpha_r = (interval+1)/2
        if type(data) == pd.DataFrame or type(data) == pd.Series:
            raise Exception("Not implemented")
        result = [data[0]]
        for i in range(1,  len(data)):
            row = data[i]* alpha_r + data[i-1]*(1-alpha_r)
            result.append(row)
        return True, result
    else:
        raise Exception("data length should be greater than interval")

def update_EMA(last_ema_value:float, new_value, window:int, a=None):
    """
    update Non weighted EMA with alpha= 2/(1+window)
    
    Args:
        last_ema_value (float): last EMA value caluculated by EMA function
        new_value (float): new value of data
        window (int): window size
    """
    alpha = 2/(1+window)
    if a != None:
        alpha = a
    return last_ema_value * (1 - alpha) + new_value*alpha
    

def EMA(data, interval, a=None):
    '''
    return list of EMA. remove interval -1 length from the data
    if data length is less than interval, return EMA with length of data as interval
    '''
    if len(data) >= interval:
        if type(data) == pd.DataFrame or type(data) == pd.Series:
            data_cp = data.copy()
            return data_cp.ewm(span=interval, adjust=False).mean()
        #ema = [np.NaN for i in range(0,interval-1)]
        lastValue = data[0]
        ema = [lastValue]
        alpha = 2/(interval+1)
        if a != None:
            alpha = a
        for i in range(1, len(data)):
            lastValue = lastValue * (1 - alpha) + data[i]*alpha
            ema.append(lastValue)
        return ema
    else:
        raise Exception("data list has no value")
    
def EWA(data:pd.DataFrame, window:int, alpha=None, adjust = True):
    """ Caliculate Exponential Weighted Moving Average

    Args:
        data (pd.DataFrame): ohlc data
        window (int): window size
        alpha(float, optional): specify weight value. Defaults to 2/(1+window). 0 < alpha <= 1.
        adjust(bool, optional): same as pandas. Detauls to True
    """
    if len(data) > window:
        if type(data) == pd.DataFrame or type(data) == pd.Series:
            data_cp = data.copy()
            if alpha == None:
                return data_cp.ewm(span=window, adjust=adjust).mean()
            else:
                return data_cp.ewa(adjust=adjust, alpha=alpha)
        ema = []
        alp = 2/(window+1)
        if alpha != None:
            alp = alpha
        if adjust:
            for y_index in range(0, len(data)):
                nume = [data[x_index] * (1-alp)**(y_index - x_index) for x_index in range(0, y_index+1) ]
                denom = [(1-alp)**(y_index - x_index) for x_index in range(0, y_index+1) ]
                y_t = nume/denom
                ema.append(y_t)
        else:
            raise NotImplemented
        return ema
    else:
        raise Exception("data list has no value")
    
def SMA(data, window):
    '''
    return list of Simple Moving Average.
    if data length is less than interval, return EMA with length of data as interval
    '''
    if window < 2:
        raise Exception(f"window size should be greater than 2. specified {window}")
    if len(data) < window:
        raise Exception(f"data length should be greater than window. currently {len(data)} < {window}")
    if type(data) == pd.DataFrame or type(data) == pd.Series:
        return data.rolling(window).mean()
    sma = [np.NaN for i in range(0, window-1)]
    ## TODO: improve the loop
    for i in range(window, len(data)+1):
        start_index = i - window
        sma_value = 0
        for j in range(start_index, start_index + window):
            sma_value += data[j]
        sma.append(sma_value/window)
    return sma

def update_macd(new_tick, short_ema_value, long_ema_value, column = 'Close', short_window=12, long_window=26):
    """ caliculate latest macd with new ohlc data and previouse ema values
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
    short_alpha = 2/(short_window+1)
    long_alpha = 2/(long_window+1)

    ##TODO replace here from hard code to function
    new_short_ema = short_ema_value* (1 - short_alpha) + new_data*short_alpha
    new_long_ema = long_ema_value * (1 - long_alpha) + new_data * long_alpha
    new_macd = new_short_ema - new_long_ema
    
    return new_short_ema, new_long_ema, new_macd

def update_ema(new_tick, ema_value, window, column='Close'):
    new_data = new_tick[column]
    alpha = 2/(window+1)
    
    new_ema = ema_value* (1 - alpha) + new_data*alpha
    return new_ema

def MACD_from_ohlc(data, column = 'Close', short_window=12, long_window=26, signal_window=9):
    '''
    caliculate MACD and Signal indicaters from OHLC. Close is used by default.
    input: data (dataframe), column (string), short_windows (int), long_window (int), signal (int)
    output: short_ema, long_ema, MACD, signal
    '''
    short_ema = EMA(data[column], short_window)
    long_ema = EMA(data[column], long_window)
    MACD, Signal = MACD_from_EMA(short_ema, long_ema, signal_window)
    return short_ema, long_ema, MACD, Signal

def MACD_from_EMA(short_ema, long_ema, signal_window):
    '''
    caliculate MACD and Signal indicaters from EMAs.
    output: macd, signal
    '''
    if type(short_ema) == pd.Series and type(long_ema) == pd.Series:
        macd = short_ema - long_ema
    else:
        macd = [x-y for (x, y) in zip(short_ema, long_ema)]
    signal = SMA(macd, signal_window)
    return macd, signal

def bolinger_from_series(data: pd.Series, window = 14, alpha=2):
    stds = data.rolling(window).std(ddof=0)
    mas = data.rolling(window).mean()
    b_high = mas + stds*alpha
    b_low = mas - stds*alpha
    return mas, b_high, b_low, stds*alpha*2

def bolinger_from_array(data, window = 14,  alpha=2):
    if type(data) == list:
        data = pd.Series(data)
    else:
        raise Exception(f'data type {type(data)} is not supported in bolinger_from_array')
    return bolinger_from_series(data, window=window, alpha=alpha)

def bolinger_from_ohlc(data: pd.DataFrame, column = 'Close', window = 14, alpha=2):
    return bolinger_from_series(data[column], window=window, alpha=alpha)

def ATR_from_ohlc(data: pd.DataFrame, ohlc_columns = ('Open', 'High', 'Low', 'Close'), window = 14):
    """
    function to calculate True Range and Average True Range
    
    Args:
        data (pd.DataFrame): ohlc data
        ohlc_columns (tuple, optional): Defaults to ('Open', 'High', 'Low', 'Close').
        window (int, optional): Defaults to 14.

    Returns:
        pd.Series: Name:ATR, dtype:float64. inlucdes Null till window size
    """
    high_cn = ohlc_columns[1]
    low_cn = ohlc_columns[2]
    close_cn = ohlc_columns[3]
    
    df = data.copy()
    df["H-L"] = df[high_cn] - df[low_cn]
    df["H-PC"] = abs(df[high_cn] - df[close_cn].shift(1))
    df["L-PC"] = abs(df[low_cn] - df[close_cn].shift(1))
    df["TR"] = df[["H-L","H-PC","L-PC"]].max(axis=1, skipna=False)
    #df["ATR"] = df["TR"].ewm(span=window, adjust=False).mean()#removed min_periods=window option
    df["ATR"] = EMA(df["TR"], interval=window)
    #df["ATR"] = df["TR"].rolling(window=n).mean()
    return df["ATR"]

def update_ATR(pre_data:pd.Series, new_data: pd.Series, ohlc_columns = ('Open', 'High', 'Low', 'Close'), atr_column = 'ATR', window = 14):
    """ latest caliculate atr

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
    atr = update_EMA(last_ema_value=pre_tr,new_value=tr, window=window)
    return atr

def RSI_from_ohlc(data:pd.DataFrame, column = 'Close', window=14):
    """
    
    RSI is a momentum oscillator which measures the speed and change od price movements

    Args:
        data (pd.DataFrame): ohlc time series data
        column (str, optional): Defaults to 'Close'.
        window (int, optional): Defaults to 14.

    Returns:
        _type_: 0 to 100
    """
    df = data.copy()
    df["change"] = df[column].diff()
    df["gain"] = np.where(df["change"]>=0, df["change"], 0)
    df["loss"] = np.where(df["change"]<0, -1*df["change"], 0)
    df["avgGain"] = df["gain"].ewm(alpha=1/window, adjust=False).mean() ##tradeview said exponentially weighted moving average with aplpha = 1/length is used
    df["avgLoss"] = df["loss"].ewm(alpha=1/window, adjust=False).mean()
    df["rs"] = df["avgGain"]/df["avgLoss"]
    df["rsi"] = 100 - (100/ (1 + df["rs"]))
    return df[["avgGain", "avgLoss", "rsi"]]

def update_RSI(pre_data:pd.Series, new_data:pd.Series, columns = ("avgGain", "avgLoss", "rsi", "Close"), window=14):
    """ caliculate lastest RSI

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
    
    avgGain = update_EMA(pre_data[c_again], gain, window=-1, a=1/window)
    avgLoss = update_EMA(pre_data[c_aloss], loss, window=-1, a=1/window)
    rs = avgGain/avgLoss
    rsi = 100 - (100/(1+rs))
    return avgGain, avgLoss, rsi
    
def renko_from_ohlc(ohlc: pd.DataFrame, atr_length:int = 120, date_column = "Timestamp", ohlc_columns = ('Open', 'High', 'Low', 'Close')):
    "function to convert ohlc data into renko bricks. please note that length is depends on results of renko"
    if len(ohlc[ohlc_columns[3]]) < atr_length:
        raise Exception("Can't caliculate block size by atr as ohlc data length is less than atr_length.")
    df = ohlc.copy()
    df.reset_index(inplace=True)
    #df = df.iloc[:,[0,1,2,3,4,5]]
    #df.columns = ["date","open","close","high","low","volume"]
    ohlc_columns = list(ohlc_columns)
    required_columns = ohlc_columns
    required_columns.insert(0, date_column)
    df = df[required_columns]
    ohlc_column_4_renko = ["open", "high", "low", "close"]
    date_column_4_renko = "date"
    columns_4_renko = ohlc_column_4_renko
    columns_4_renko.insert(0, date_column_4_renko)
    df.columns = columns_4_renko
    df2 = Renko(df)
    atr = ATR_from_ohlc(ohlc, ohlc_columns, atr_length).iloc[-1]
    df2.brick_size = round(atr,4)
    renko_df = df2.get_ohlc_data()
    #renko_df["bar_num"] = np.where(renko_df["uptrend"]==True,1,np.where(renko_df["uptrend"]==False,-1,0))
    bar_num = np.where(renko_df["uptrend"]==True,1,np.where(renko_df["uptrend"]==False,-1,0))
    for i in range(1,len(bar_num)):
        if bar_num[i]>0 and bar_num[i-1]>0:
            bar_num[i]+=bar_num[i-1]
        elif bar_num[i]<0 and bar_num[i-1]<0:
            bar_num[i]+=bar_num[i-1]
    renko_df["bar_num"] = bar_num
    #when value rise up/down multiple bricks, renko (ohlc value as brick) is stored on same date. so we need to drop duplicates and keep last item only.
    renko_df.drop_duplicates(subset="date",keep="last",inplace=True)
    renko_df = renko_df[[date_column_4_renko,"uptrend", "bar_num"]]
    renko_df.columns = [date_column, "uptrend", "bar_num"]
    return renko_df

def renko_time_scale(DF: pd.DataFrame, date_column = "Timestamp", ohlc_columns = ('Open', 'High', 'Low', 'Close'), is_date_index=False, window=120):
    "function to merging renko df with original ohlc df"
    df = copy.deepcopy(DF)
    if is_date_index:
        if type(date_column) != str:
            date_column = "date"
        df[date_column] = df.index
    else:
        if type(date_column) != str or date_column not in DF:
            raise Exception("datetime index or columns is required to scale renko result to the datetime.")
    renko = renko_from_ohlc(df, ohlc_columns=ohlc_columns, date_column=date_column, atr_length=window)
    merged_df = df.merge(renko.loc[:,[date_column,"bar_num"]],how="outer",on=date_column)
    merged_df["bar_num"].fillna(method='ffill',inplace=True)
    return merged_df

def slope(ser: pd.Series, window):
    "function to calculate the slope of n consecutive points on a plot"
    slopes = [0 for i in range(window-1)]
    for i in range(window, len(ser)+1):
        y = ser.iloc[i-window:i]
        x = np.array(range(window))
        y_scaled = (y - y.min())/(y.max() - y.min())
        x_scaled = (x - x.min())/(x.max() - x.min())
        x_scaled = sm.add_constant(x_scaled)
        model = sm.OLS(y_scaled,x_scaled)
        results = model.fit()
        slopes.append(results.params[-1])
    slope_angle = (np.rad2deg(np.arctan(np.array(slopes))))
    return pd.Series({"slope":np.array(slope_angle)})

def CommodityChannelIndex(ohlc: pd.DataFrame, window = 14, ohlc_columns = ('Open', 'High', 'Low', 'Close')) -> pd.Series:
    """ represents how much close value is far from mean value. If over 100, strong long trend for example.

    Args:
        ohlc (pd.DataFrame): Open High Low Close values
        window (int, optional): window size to caliculate EMA. Defaults to 14.
        ohlc_columns (tuple, optional): tuple of Open High Low Close column names. Defaults to ('Open', 'High', 'Low', 'Close').

    Returns:
        pd.DataFrame: _description_
    """
    close_column = ohlc_columns[3]
    low_column = ohlc_columns[2]
    high_column = ohlc_columns[1]
    
    tp = (ohlc[high_column] + ohlc[low_column] + ohlc[close_column])/3
    ma = EMA(ohlc[close_column], window)
    md = (tp - ma).std()
    cci = (tp-ma) / (0.015 * md)
    return cci