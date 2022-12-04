import numpy as np
import pandas as pd
import statsmodels.api as sm
from .convert import concat_df_symbols

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

def EMA(data, interval, alpha=None):
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
        _alpha = 2/(interval+1)
        if alpha != None:
            _alpha = alpha
        for i in range(1, len(data)):
            lastValue = lastValue * (1 - _alpha) + data[i]*_alpha
            ema.append(lastValue)
        return ema
    else:
        raise Exception("data list has no value")

def EMAMulti(symbols:list, data:pd.DataFrame, interval:int, alpha=None, grouped_by_symbol=False,
             ema_name="EMA"):
    df = data.copy()
    if grouped_by_symbol == False:
        df.columns = df.columns.swaplevel(0, 1)
    df = df[symbols]
    ema_df = EMA(df, interval, alpha)
    elements, columns = create_multi_out_lists(symbols, [ema_df], [ema_name], grouped_by_symbol)
    ema_df.columns = columns
    return ema_df
    
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

def MACDFromOHLC(data, column = 'Close', short_window=12, long_window=26, signal_window=9,
                 short_ema_name="ShortEMA", long_ema_name="LongEMA", macd_name="MACD", signal_name="Signal"):
    """caliculate MACD and Signal indicaters from OHLC. Close is used by default.

    Args:
        data (pd.DataFrame): ohlc data of a symbol
        column (str, optional): target column name. Defaults to 'Close'.
        short_window (int, optional): window size for short EMA. Defaults to 12.
        long_window (int, optional): window size for long EMA. Defaults to 26.
        signal_window (int, optional): window size for Signals. Defaults to 9.

    Returns:
        pd.DataFrame: DataFrame of ShortEMA, LongEMA, MACD and Signal
    """
    short_ema = EMA(data[column], short_window)
    long_ema = EMA(data[column], long_window)
    MACD, Signal = MACDFromEMA(short_ema, long_ema, signal_window)
    
    elements, columns = __create_out_lists([short_ema, long_ema, MACD, Signal], [short_ema_name, long_ema_name, macd_name, signal_name])
            
    macd_df = pd.concat(elements, axis=1)
    macd_df.columns = columns
    return macd_df

def MACDFromEMA(short_ema, long_ema, signal_window):
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

def MACDFromOHLCMulti(symbols:list, data: pd.DataFrame, column = 'Close', short_window=12, long_window=26, signal_window=9, grouped_by_symbol=False,
                      short_ema_name="ShortEMA", long_ema_name="LongEMA", macd_name="MACD", signal_name="Signal"):
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
    if grouped_by_symbol == False:
        df.columns = df.columns.swaplevel(0 ,1)
        
    df = df[symbols]
    short_ema = EMA(df[[(symbol, column) for symbol in symbols]], short_window)
    long_ema = EMA(df[[(symbol, column) for symbol in symbols]], long_window)
    macd = short_ema - long_ema
    signal = SMA(macd, signal_window)
        
    elements, columns = create_multi_out_lists(symbols, [short_ema, long_ema, macd, signal], [short_ema_name, long_ema_name, macd_name, signal_name], grouped_by_symbol)
    macd_df = pd.concat(elements, axis=1)
    macd_df.columns = columns
    macd_df.sort_index(level=0, axis=1, inplace=True)
    return macd_df

def BolingerFromSeries(data: pd.Series, window = 14, alpha=2):
    stds = data.rolling(window).std(ddof=0)
    mas = data.rolling(window).mean()
    b_high = mas + stds*alpha
    b_low = mas - stds*alpha
    #width = stds*alpha*2 ##deleted for test purpose as there is small error compared with diff
    width = b_high - b_low
    return mas, b_high, b_low, width, stds

def BolingerFromArray(data, window = 14,  alpha=2):
    if type(data) == list:
        data = pd.Series(data)
    else:
        raise Exception(f'data type {type(data)} is not supported in BolingerFromArray')
    return BolingerFromSeries(data, window=window, alpha=alpha)

def BolingerFromOHLC(data: pd.DataFrame, column = 'Close', window = 14, alpha=2,
                     mean_name="B_MA", upper_name="B_High", lower_name="B_Low", width_name="B_Width", std_name="B_Std"):
    """Caliculate Bolinger band from ohlc dataframe for a symbol

    Args:
        data (pd.DataFrame): ohlc data of a symbol
        column (str, optional): target column name. Defaults to 'Close'.
        window (int, optional): window size for bolinger band. Defaults to 14.
        alpha (int, optional): alph to caliculate band. Defaults to 2.

    Returns:
        pd.DataFrame: B_MA, B_Hig, B_Low, B_Width, B_Std for a symbol
    """
    ma, b_high, b_low, width, stds = BolingerFromSeries(data[column], window=window, alpha=alpha)
    b_df = pd.concat([ma, b_high, b_low, width, stds], axis=1)
    b_df.columns = (mean_name, upper_name, lower_name, width_name, std_name)
    return b_df

def BolingerFromOHLCMulti(symbols:list, data: pd.DataFrame, column = 'Close', window = 14, alpha=2, grouped_by_symbol=False,
                          mean_name="B_MA", upper_name="B_High", lower_name="B_Low", width_name="B_Width", std_name="B_Std"):
    """Caliculate Bolinger band from ohlc dataframe for symbols

    Args:
        symbols (list<str>): symbol list. Each element should match with column.
        data (pd.DataFrame): ohlc data of symbols
        column (str, optional): target column name. Defaults to 'Close'.
        window (int, optional): window size for bolinger band. Defaults to 14.
        alpha (int, optional): alph to caliculate band. Defaults to 2.
        grouped_by_symbol (bool, optional): If True, return a result with (symbol, column). Defaults to False.

    Returns:
        pd.DataFrame: B_MA, B_Hig, B_Low, B_Width, B_Std for symbols
    """
    df = data.copy()
    if grouped_by_symbol == False:
        df.columns = df.columns.swaplevel(0, 1)
        
    ohlc_dfs = df[[(symbol, column) for symbol in symbols]]
        
    ma, b_high, b_low, width, stds = BolingerFromSeries(ohlc_dfs, window=window, alpha=alpha)
    elements, columns = create_multi_out_lists(symbols, [ma, b_high, b_low, width, stds], [mean_name, upper_name, lower_name, width_name, std_name], grouped_by_symbol)
    b_df = pd.concat(elements, axis=1)
    b_df.columns = columns
    b_df.sort_index(level=0, axis=1, inplace=True)
    return b_df

def ATRFromMultiOHLC(symbols:list, data: pd.DataFrame, ohlc_columns = ('Open', 'High', 'Low', 'Close'), window = 14, grouped_by_symbol=False,
                     tr_name="TR", atr_name="ATR"):
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
    tr_df = pd.DataFrame(index=temp_df.index)
    for symbol in symbols:
        tr_df[symbol] = temp_df[symbol].max(axis=1)
    atr_df = EMA(tr_df, window)
    elements, columns = create_multi_out_lists(symbols, [tr_df, atr_df], [tr_name, atr_name], grouped_by_symbol)
    out_df = pd.concat(elements, axis=1)
    out_df.columns = columns
    out_df.sort_index(level=0, axis=1, inplace=True)
    return out_df

def ATRFromOHLC(data: pd.DataFrame, ohlc_columns = ('Open', 'High', 'Low', 'Close'), window = 14,
                tr_name="TR", atr_name="ATR"):
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
    df[tr_name] = df[["H-L","H-PC","L-PC"]].max(axis=1)
    df[atr_name] = EMA(df[tr_name], interval=window)
    return df[[tr_name, atr_name]].copy()

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

def RSIFromOHLC(data:pd.DataFrame, column = 'Close', window=14,
                mean_gain_name="AvgGain", mean_loss_name="AvgLoss", rsi_name="Rsi"):
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
    df["gain"] = np.where(df["change"]>=0, df["change"], 0)
    df["loss"] = np.where(df["change"]<0, -1*df["change"], 0)
    avgain_df = df["gain"].ewm(alpha=1/window, adjust=False).mean() ##tradeview said exponentially weighted moving average with aplpha = 1/length is used
    avgloss_df = df["loss"].ewm(alpha=1/window, adjust=False).mean()
    rs_df = avgain_df/avgloss_df
    rsi_df = 100 - (100/ (1 + rs_df))
    elements, columns = __create_out_lists([avgain_df, avgloss_df, rsi_df], [mean_gain_name, mean_loss_name, rsi_name])
    out_df = pd.concat(elements, axis=1)
    out_df.columns = columns
    return out_df

def RSIFromOHLCMulti(symbols:list, data:pd.DataFrame, column = 'Close', window=14, grouped_by_symbol=False,
                mean_gain_name="avgGain", mean_loss_name="avgLoss", rsi_name="rsi"):
    df = data.copy()
    if grouped_by_symbol:
        df.columns = df.columns.swaplevel(0, 1)
    
    diff_df = df[column][symbols].diff()
    gain_df = diff_df[diff_df >=0]
    gain_df[gain_df.isna()] = 0
    loss_df = -diff_df[diff_df < 0]
    loss_df[loss_df.isna()] = 0
    
    avgain_df = gain_df.ewm(alpha=1/window, adjust=False).mean()
    avgloss_df = loss_df.ewm(alpha=1/window, adjust=False).mean()
    rs_df = avgain_df/avgloss_df
    rsi_df = 100 - (100/ (1 + rs_df))
    elements, columns = create_multi_out_lists(symbols, [avgain_df, avgloss_df, rsi_df], [mean_gain_name, mean_loss_name, rsi_name], grouped_by_symbol)
    out_df = pd.concat(elements, axis=1)
    out_df.columns = columns
    out_df.sort_index(level=0, axis=1, inplace=True)
    return out_df

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
    
def RenkoFromSeries(data_sr:pd.Series, brick_size,
                    total_brick_name = "Renko", brick_num_name = "Brick"):
    """ Caliculate brick number of Renko

    Args:
        data_sr (pd.Series): time series data like close values of a sygnal
        brick_size (pd.Series|float, optional): brick_size to caliculate the Renko. If None, ATR is used. Defaults to None.
        
    Returns:
        pd.Series: brick_num
    """
    def get_check_df_from_series(data_sr, brick_sr, start_index, to_index, criteria_value):
        return (data_sr.iloc[start_index: to_index] - criteria_value)/brick_sr.iloc[start_index: to_index]
    
    def get_check_df_from_scalar(data_sr, brick_size, start_index, to_index, criteria_value):
        return (data_sr.iloc[start_index: to_index] - criteria_value)/brick_size
    
    if type(brick_size) == pd.Series:
        if len(data_sr) != len(brick_size):
            raise Exception(f"sr and brick_size_sr should have same length.")
        brick_size = brick_size.copy().reset_index(drop=True)
        get_check_df = get_check_df_from_series
    else:
        get_check_df = get_check_df_from_scalar
            
    org_index = data_sr.index
    sr = data_sr.copy().reset_index(drop=True)
    
    def trendy(uptrend, downtrend):
        if len(uptrend) > 0 and len(downtrend) > 0:
            if uptrend.index[0] > downtrend.index[0]:
                #mark down until criteria_index to downtrend.index[0]
                trend = -1
                brick_size = int(downtrend.iloc[0])
                next_criteria_index = downtrend.index[0]
            else:
                #mark up until criteria_index to uptrend.index[0]
                trend = 1
                brick_size = int(uptrend.iloc[0])
                next_criteria_index = uptrend.index[0]
        elif len(uptrend) > 0:
            trend = 1
            brick_size = int(uptrend.iloc[0])
            next_criteria_index = uptrend.index[0]
        elif len(downtrend) > 0:
            trend = -1
            brick_size = int(downtrend.iloc[0])
            next_criteria_index = downtrend.index[0]
        else:
            trend = None
            brick_size = None
            next_criteria_index = None
        return trend, brick_size, next_criteria_index
    
    CONST_INDEX_PLUS = 30
    
    criteria_index = sr[pd.notna(sr)].index[0]
    current_criteria = sr.iloc[criteria_index]
    total_brick_num_sr = pd.Series(0, index=sr.index)
    brick_value_sr = pd.Series(0, index=sr.index)

    trend = None
    start_index = criteria_index
    to_index = criteria_index + CONST_INDEX_PLUS
    while trend is None and to_index < len(sr):
        temp_brick_num_sr = get_check_df(sr, brick_size, start_index, to_index, current_criteria)
        brick_value_sr.iloc[start_index:to_index] = temp_brick_num_sr
        uptrend = temp_brick_num_sr[temp_brick_num_sr >= 1]
        downtrend = temp_brick_num_sr[temp_brick_num_sr <= -1]
        #if trend is None:
        if len(uptrend) == 0 and len(downtrend) == 0:
            start_index = to_index
            to_index = start_index + CONST_INDEX_PLUS
        else:
            trend, block_num, next_criteria_index = trendy(uptrend, downtrend)
    if trend is None:
        raise Exception("can't initialize renko.")
    
    global_trend = trend
    while True:
        trend, new_brick_num, next_criteria_index = trendy(uptrend, downtrend)        
        if trend is None:#didn't changed renko value
            brick_num = total_brick_num_sr.iloc[criteria_index]
            total_brick_num_sr.iloc[criteria_index:to_index] = brick_num
            next_criteria_index = criteria_index
            next_start_index = to_index
        else:
            global_trend = trend
            brick_num = total_brick_num_sr.iloc[criteria_index]
            if brick_num/trend >= 0:#continuaus trend
                next_brick_num = brick_num + new_brick_num
            else:
                next_brick_num = brick_num + new_brick_num

            total_brick_num_sr.iloc[criteria_index: next_criteria_index] = brick_num
            total_brick_num_sr.iloc[next_criteria_index] = next_brick_num
            criteria_index = next_criteria_index
            next_start_index = next_criteria_index + 1
            
        if next_start_index < len(sr):
            to_index = next_start_index + CONST_INDEX_PLUS
            if to_index > len(sr):
                to_index = len(sr)

            current_criteria = sr.iloc[next_criteria_index]
            temp_brick_num_sr = get_check_df(sr, brick_size, next_start_index, to_index, current_criteria)
            brick_value_sr.iloc[next_start_index:to_index] = temp_brick_num_sr
            uptrend = temp_brick_num_sr[temp_brick_num_sr >=  -global_trend/2 + 3/2]
            downtrend = temp_brick_num_sr[temp_brick_num_sr <= -global_trend/2 - 3/2]
        else:
            break
    #total_brick_num_sr.index = org_index
    renko_df = pd.DataFrame.from_dict({total_brick_name:total_brick_num_sr, brick_num_name:brick_value_sr})
    renko_df.index = org_index
    return renko_df

def RenkoFromOHLC(df:pd.DataFrame, ohlc_columns = ('Open', 'High', 'Low', 'Close'), brick_column_name=None, brick_size=None, atr_window=14,
                  total_brick_name = "Renko", brick_num_name = "Brick"):
    """ Caliculate Renko from Close column of ohlc dataframe

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
    if(set(ohlc_columns).issubset(df.columns)):
        if brick_size is None:
            if brick_column_name is None:
                atr_df = ATRFromOHLC(df, ohlc_columns, window=atr_window, atr_name="ATR")
                brick_size = atr_df["ATR"]
            else:
                brick_size = df[brick_column_name]
            renko_df = RenkoFromSeries(df[ohlc_columns[3]], brick_size=brick_size, total_brick_name=total_brick_name, brick_num_name=brick_num_name)
        else:
            renko_df = RenkoFromSeries(df[ohlc_columns[3]], brick_size=brick_size, total_brick_name=total_brick_name, brick_num_name=brick_num_name)
        return renko_df
    else:
        raise Exception(f"specified ohlc_columns {ohlc_columns} doen't match with df.columns {df.columns}")

def RenkoFromMultiOHLC(symbols:list, dfs:pd.DataFrame, ohlc_columns = ('Open', 'High', 'Low', 'Close'), brick_size_column=None, brick_size=None, atr_window=14, grouped_by_symbol=False,
                       total_brick_name = "Renko", brick_num_name = "Brick"):
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
            atr_dfs = ATRFromMultiOHLC(symbols, dfs, ohlc_columns, window=atr_window, grouped_by_symbol=grouped_by_symbol, atr_name="ATR")
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
                DFS[symbol] = RenkoFromOHLC(dfs[symbol], ohlc_columns, brick_size=brick_sizes[symbol], total_brick_name=total_brick_name, brick_num_name=brick_num_name)
            else:
                DFS[symbol] = RenkoFromOHLC(dfs[symbol], ohlc_columns, brick_size=brick_size, total_brick_name=total_brick_name, brick_num_name=brick_num_name)
    else:
        for symbol in symbols:
            _ohlc_columns = [(column, symbol) for column in ohlc_columns]
            ohlc_df = dfs[_ohlc_columns]
            if is_series_brick:
                DFS[symbol] = RenkoFromOHLC(ohlc_df, _ohlc_columns, brick_size=brick_sizes[symbol], total_brick_name=total_brick_name, brick_num_name=brick_num_name)
            else:
                DFS[symbol] = RenkoFromOHLC(ohlc_df, _ohlc_columns, brick_size=brick_size, total_brick_name=total_brick_name, brick_num_name=brick_num_name)
                
    RenkoDF = pd.concat(DFS.values(), axis=1, keys=DFS.keys())
    if grouped_by_symbol == False:
        RenkoDF.columns = RenkoDF.columns.swaplevel(0, 1)
        RenkoDF.sort_index(level=0, axis=1, inplace=True)
    #RenkoDF = pd.DataFrame.from_dict(DFS)
    return RenkoDF

def SlopeFromSeries(ser: pd.Series, window: int):
    """function to calculate the slope of n consecutive points on a plot

    Args:
        ser (pd.Series): time series data
        window (int): window size for the slope

    Returns:
        pd.Series: slope values
    """
    slopes = [0 for i in range(window-1)]
    index = ser.index
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
    slope_ser = pd.Series(np.array(slope_angle))
    slope_ser.index = index
    return pd.Series(np.array(slope_angle))

def SlopeFromOHLC(ohlc_df: pd.DataFrame, window: int, column="Close",
                  slope_name="Slope"):
    """function to calculate the slope of n consecutive points on a plot

    Args:
        ser (pd.DataFrame): OHLC time series data of a symbol
        window (int): window size for the slope
        column (str): target column name

    Returns:
        pd.DataFrame: slope value on Slope column
    """
    slope_sr = SlopeFromSeries(ohlc_df[column], window)
    return pd.DataFrame(slope_sr, columns=[slope_name])

def SlopeFromOHLCMulti(symbols:list, ohlc_dfs: pd.DataFrame, window: int, column:str="Close", grouped_by_sygnal:bool=False,
                       slope_name="Slope"):
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
            DFS[symbol] = SlopeFromOHLC(ohlc_dfs, window, (column, symbol), slope_name)
    slope_dfs = pd.concat(DFS.values(), axis=1, keys=DFS.keys())
    if grouped_by_sygnal == False:
        slope_dfs.columns = slope_dfs.columns.swaplevel(0, 1)
    return slope_dfs

def __CCI(ohlc: pd.DataFrame, window = 14, ohlc_columns = ('Open', 'High', 'Low', 'Close')):
    close_column = ohlc_columns[3]
    low_column = ohlc_columns[2]
    high_column = ohlc_columns[1]
    
    tp = (ohlc[high_column] + ohlc[low_column] + ohlc[close_column])/3
    ma = EMA(ohlc[close_column], window)
    md = (tp - ma).std()
    cci = (tp-ma) / (0.015 * md)
    return cci

def CommodityChannelIndex(ohlc: pd.DataFrame, window = 14, ohlc_columns = ('Open', 'High', 'Low', 'Close'), 
                          cci_name="CCI") -> pd.DataFrame:
    """ represents how much close value is far from mean value. If over 100, strong long trend for example.

    Args:
        ohlc (pd.DataFrame): Open High Low Close values
        window (int, optional): window size to caliculate EMA. Defaults to 14.
        ohlc_columns (tuple, optional): tuple of Open High Low Close column names. Defaults to ('Open', 'High', 'Low', 'Close').

    Returns:
        pd.DataFrame: CCI value on CCI column
    """
    cci = __CCI(ohlc, window, ohlc_columns)
    return pd.DataFrame(cci, columns=[cci_name])

def CommodityChannelIndexMulti(symbols:list, ohlc: pd.DataFrame, window = 14, ohlc_columns = ('Open', 'High', 'Low', 'Close'), grouped_by_sygnal:bool=False,
                               cci_name="CCI") -> pd.DataFrame:
    """ represents how much close value is far from mean value. If over 100, strong long trend for example.

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