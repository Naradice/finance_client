import pandas as pd

from .indicaters import technical


def range_detection_by_atr(prices: pd.DataFrame, mean_window: int = 100, atr_window: int = 14, range_threshold: float = 0.7, ohlc_columns = ("Open", "High", "Low", "Close")) -> pd.Series:
    """Detect range periods using Average True Range (ATR)
        If ATR is below a certain threshold, the market is considered to be in a range.
        It is advisable to use this in combination with other indicators as it may misjudge during sudden volatility changes.
    Args:
        prices (pd.DataFrame): DataFrame with 'High', 'Low', and 'Close' columns
        mean_window (int, optional): Length for ATR rolling mean calculation. Defaults to 100.
        atr_window (int, optional): Length for ATR calculation. Defaults to 14.
        range_threshold (float, optional): Threshold multiplier for range detection. Defaults to 0.7.
    """
    atr = technical.ATRFromOHLC(prices, ohlc_columns=ohlc_columns, window=atr_window)["ATR"]
    atr_mean = atr.rolling(window=mean_window).mean()
    is_range = atr < atr_mean * range_threshold
    return is_range

def range_detection_by_bollinger(prices: pd.DataFrame, std_window:int=200, window: int = 20, std_threshold: float = 0.6, ohlc_columns = ("Open", "High", "Low", "Close")) -> pd.Series:
    """Detect range periods using Bollinger Band width
        If the standard deviation is below a certain threshold, the market is considered to be in a range.
        It is advisable to use this in combination with other indicators as it may misjudge during sudden volatility changes.
    Args:
        prices (pd.DataFrame): DataFrame with 'High', 'Low', and 'Close' columns
        std_window (int, optional): Length for standard deviation rolling mean calculation. Defaults to 200.
        window (int, optional): Length for Bollinger Band calculation. Defaults to 20.
        std_threshold (float, optional): Threshold multiplier for range detection. Defaults to 0.6.
    """
    bb = technical.BollingerFromOHLC(prices, column=ohlc_columns[-1], window=window)
    std = bb["B_Std"]
    std_mean = std.rolling(window=std_window).mean()
    is_range = std < std_mean * std_threshold
    return is_range

def range_detection_by_swing_width(prices: pd.DataFrame, window: int = 50, width_threshold: float = 0.015, ohlc_columns = ("Open", "High", "Low", "Close")) -> pd.Series:
    """Detect range periods using swing width (high-low range)
        If the swing width is below a certain threshold, the market is considered to be in a range.
        It is advisable to use this in combination with other indicators as it may misjudge during sudden volatility changes.
    Args:
        prices (pd.DataFrame): DataFrame with 'High', 'Low', and 'Close' columns
        window (int, optional): Length for swing width calculation. Defaults to 50.
        width_threshold (float, optional): Threshold multiplier for range detection. Defaults to 0.015.
    """
    high = prices[ohlc_columns[1]]
    low = prices[ohlc_columns[2]]
    hh = high.rolling(window=window).max()
    ll = low.rolling(window=window).min()
    range_width = (hh - ll) / prices[ohlc_columns[3]]
    is_range = range_width < width_threshold
    return is_range

def range_detection_by_adx(prices: pd.DataFrame, adx_window: int = 14, adx_threshold: float = 25, ohlc_columns = ("Open", "High", "Low", "Close")) -> pd.Series:
    """Detect range periods using Average Directional Index (ADX)
        If ADX is below a certain threshold, the market is considered to be in a range.
        It is advisable to use this in combination with other indicators as it may misjudge during sudden volatility changes.
    Args:
        prices (pd.DataFrame): DataFrame with 'High', 'Low', and 'Close' columns
        adx_window (int, optional): Length for ADX calculation. Defaults to 14.
        adx_threshold (float, optional): Threshold for range detection. Defaults to 25.
    """
    adx = technical.ADXFromOHLC(prices, ohlc_columns=ohlc_columns, window=adx_window)["ADX"]
    is_range = adx < adx_threshold
    return is_range

def range_detection_by_ma_deviation(prices: pd.DataFrame, short_window: int = 10, long_window: int = 50, deviation_threshold: float = 0.005, ohlc_columns = ("Open", "High", "Low", "Close")) -> pd.Series:
    """Detect range periods using moving average deviation
        If the deviation between short-term and long-term moving averages is below a certain threshold, the market is considered to be in a range.
        It is advisable to use this in combination with other indicators as it may misjudge during sudden volatility changes.
    Args:
        prices (pd.DataFrame): DataFrame with 'High', 'Low', and 'Close' columns
        short_window (int, optional): Length for short-term moving average. Defaults to 10.
        long_window (int, optional): Length for long-term moving average. Defaults to 50.
        deviation_threshold (float, optional): Threshold multiplier for range detection. Defaults to 0.005.
    """
    close = prices[ohlc_columns[3]]
    ma_short = close.rolling(window=short_window).mean()
    ma_long = close.rolling(window=long_window).mean()
    deviation = abs(ma_short - ma_long) / ma_long
    is_range = deviation < deviation_threshold
    return is_range