import numpy
import pandas as pd
from scipy.stats import linregress

from .convert import concat, get_symbols
from .indicaters import technical
from .process import ProcessBase

""" process class to add indicater for data_client, dataset, env etc
    is_input, is_output are used for machine learning
"""


def get_available_processes() -> dict:
    processes = {
        "MACD": MACDProcess,
        "EMA": EMAProcess,
        "BBAND": BBANDProcess,
        "ATR": ATRProcess,
        "RSI": RSIProcess,
        # 'Roll': RollingProcess,
        "Renko": RenkoProcess,
        "Slope": SlopeProcess,
        "LRMomentum": LinearRegressionMomentumProcess,
    }
    return processes


def indicaters_to_params(processes: list) -> dict:
    """convert procese list to dict for saving params as file

    Args:
        processes (list: ProcessBase): indicaters defined in preprocess.py

    Returns:
        dict: {'input':{key:params}, 'output':{key: params}}
    """
    params = {}

    for process in processes:
        option = process.option
        option["kinds"] = process.kinds
        option["input"] = process.is_input
        option["output"] = process.is_output
        params[process.key] = option
    return params


def load_indicaters(params: dict) -> list:
    ips_dict = get_available_processes()
    ids = []
    for key, param in params.items():
        kinds = param["kinds"]
        idc = ips_dict[kinds]
        idc = idc.load(key, param)
        ids.append(idc)
    return ids


class MACDProcess(ProcessBase):
    kinds = "MACD"

    def __init__(
        self,
        key="macd",
        target_column="Close",
        short_window=12,
        long_window=26,
        signal_window=9,
        option=None,
        is_input=True,
        is_output=True,
    ):
        super().__init__(key)
        self.last_data = None
        self.option = {
            "column": target_column,
            "short_window": short_window,
            "long_window": long_window,
            "signal_window": signal_window,
        }

        if option is not None:
            self.option.update(option)

        self.KEY_SHORT_EMA = f"{key}_S_EMA"
        self.KEY_LONG_EMA = f"{key}_L_EMA"
        self.KEY_MACD = "MACD"
        self.KEY_SIGNAL = f"{key}_Signal"
        self.is_input = is_input
        self.is_output = is_output

    @property
    def columns(self):
        return [
            self.KEY_SHORT_EMA,
            self.KEY_LONG_EMA,
            self.KEY_MACD,
            self.KEY_SIGNAL,
        ]

    @classmethod
    def load(self, key: str, params: dict):
        option = {
            "column": params["column"],
            "short_window": params["short_window"],
            "long_window": params["long_window"],
            "signal_window": params["signal_window"],
        }
        is_input = params["input"]
        is_out = params["output"]
        macd = MACDProcess(key, option=option, is_input=is_input, is_output=is_out)
        return macd

    def run(self, data: pd.DataFrame, symbols: list = [], grouped_by_symbol=False):
        option = self.option
        target_column = option["column"]
        short_window = option["short_window"]
        long_window = option["long_window"]
        signal_window = option["signal_window"]

        cs_ema = self.KEY_SHORT_EMA
        cl_ema = self.KEY_LONG_EMA
        c_macd = self.KEY_MACD
        c_signal = self.KEY_SIGNAL

        if type(data.columns) == pd.MultiIndex:
            if len(symbols) == 0:
                symbols = get_symbols(data, grouped_by_symbol)
            macd_df = technical.MACDFromOHLCMulti(
                symbols,
                data,
                target_column,
                short_window,
                long_window,
                signal_window,
                grouped_by_symbol,
                short_ema_name=cs_ema,
                long_ema_name=cl_ema,
                macd_name=c_macd,
                signal_name=c_signal,
            )
        else:
            macd_df = technical.MACDFromOHLC(
                data,
                target_column,
                short_window,
                long_window,
                signal_window,
                short_ema_name=cs_ema,
                long_ema_name=cl_ema,
                macd_name=c_macd,
                signal_name=c_signal,
            )

        self.last_data = macd_df.iloc[-self.get_minimum_required_length() :]
        return pd.concat([data, macd_df], axis=1)

    def update(self, tick: pd.Series, symbols: list = []):
        if type(symbols) == list and len(symbols) > 0:
            print("update is not implemented for multi symbols yet")
        else:
            option = self.option
            target_column = option["column"]
            short_window = option["short_window"]
            long_window = option["long_window"]
            signal_window = option["signal_window"]

            cs_ema = self.KEY_SHORT_EMA
            cl_ema = self.KEY_LONG_EMA
            c_macd = self.KEY_MACD
            c_signal = self.KEY_SIGNAL

            short_ema, long_ema, MACD = technical.update_macd(
                new_tick=tick,
                short_ema_value=self.last_data[cs_ema].iloc[-1],
                long_ema_value=self.last_data[cl_ema].iloc[-1],
                column=target_column,
                short_window=short_window,
                long_window=long_window,
            )
            Signal = (self.last_data[c_macd].iloc[-signal_window + 1 :].sum() + MACD) / signal_window

            new_data = pd.Series({cs_ema: short_ema, cl_ema: long_ema, c_macd: MACD, c_signal: Signal})
            self.last_data = concat(self.last_data.iloc[1:], new_data)
            return new_data

    def get_minimum_required_length(self):
        return self.option["long_window"] + self.option["signal_window"] - 2

    def revert(self, data_set: tuple):
        cs_ema = self.KEY_SHORT_EMA

        if type(data_set) == pd.DataFrame:
            if cs_ema in data_set:
                data_set = (data_set[cs_ema],)
        # assume ShortEMA is in 1st
        short_ema = data_set[0]
        short_window = self.option["short_window"]
        out = technical.revert_EMA(short_ema, short_window)
        return out


class MAProcess(ProcessBase):
    kinds = "MA"

    def __init__(self, key="ma", window=12, column="Close", is_input=True, is_output=True, option=None):
        super().__init__(key)
        self.option = {"column": column, "window": window}
        self.last_data = None

        if option is not None:
            self.option.update(option)
        self.KEY_EMA = f"{key}_MA"
        self.is_input = is_input
        self.is_output = is_output

    @property
    def columns(self):
        return [self.KEY_EMA]

    @classmethod
    def load(self, key: str, params: dict):
        window = params["window"]
        column = params["column"]
        is_input = params["input"]
        is_out = params["output"]
        indicater = MAProcess(key, window=window, column=column, is_input=is_input, is_output=is_out)
        return indicater

    def run(self, data: pd.DataFrame, symbols: list = [], grouped_by_symbol=False):
        option = self.option
        target_column = option["column"]
        window = option["window"]
        column = self.KEY_EMA

        sma = technical.SMA(data[target_column], window)
        sma.name = column

        self.last_data = sma.iloc[-self.get_minimum_required_length() :]
        return pd.concat([data, sma], axis=1)

    def update(self, tick: pd.Series, symbols: list = []):
        option = self.option
        target_column = option["column"]
        window = option["window"]
        column = self.KEY_EMA

        short_ema, long_ema, MACD = technical.update_ema(new_tick=tick, column=target_column, window=window)

        new_data = pd.Series({column: short_ema})
        self.last_data = concat(self.last_data.iloc[1:], new_data)
        return new_data

    def get_minimum_required_length(self):
        return self.option["window"]

    def revert(self, data_set: tuple):
        # assume EMA is in 1st
        ema = data_set[0]
        window = self.option["window"]
        out = technical.revert_EMA(ema, window)
        return out


class EMAProcess(ProcessBase):
    kinds = "EMA"

    def __init__(self, key="ema", window=12, column="Close", is_input=True, is_output=True, option=None):
        super().__init__(key)
        self.option = {"column": column, "window": window}
        self.last_data = None

        if option is not None:
            self.option.update(option)
        self.KEY_EMA = key
        self.is_input = is_input
        self.is_output = is_output

    @property
    def columns(self):
        return [self.KEY_EMA]

    @classmethod
    def load(self, key: str, params: dict):
        window = params["window"]
        column = params["column"]
        is_input = params["input"]
        is_out = params["output"]
        indicater = EMAProcess(key, window=window, column=column, is_input=is_input, is_output=is_out)
        return indicater

    def run(self, data: pd.DataFrame, symbols: list = [], grouped_by_symbol=False):
        option = self.option
        target_column = option["column"]
        window = option["window"]
        column = self.KEY_EMA

        if type(data.columns) == pd.MultiIndex:
            if len(symbols) == 0:
                symbols = get_symbols(data, grouped_by_symbol)
            ema = technical.EMAMulti(symbols, data, target_column, window, grouped_by_symbol=grouped_by_symbol, ema_name=column)
        else:
            ema = technical.EMA(data[target_column], window)
            ema.name = column

        self.last_data = ema.iloc[-self.get_minimum_required_length() :]
        return pd.concat([data, ema], axis=1)

    def update(self, tick: pd.Series, symbols: list = []):
        option = self.option
        target_column = option["column"]
        window = option["window"]
        column = self.KEY_EMA

        short_ema, long_ema, MACD = technical.update_ema(new_tick=tick, column=target_column, window=window)

        new_data = pd.Series({column: short_ema})
        self.last_data = concat(self.last_data.iloc[1:], new_data)
        return new_data

    def get_minimum_required_length(self):
        return self.option["window"]

    def revert(self, data_set: tuple):
        # assume EMA is in 1st
        ema = data_set[0]
        window = self.option["window"]
        out = technical.revert_EMA(ema, window)
        return out


class BBANDProcess(ProcessBase):
    kinds = "BBAND"

    def __init__(self, key="BB", window=14, alpha=2, target_column="Close", is_input=True, is_output=True, option=None):
        super().__init__(key)
        self.option = {"column": target_column, "window": window, "alpha": alpha}
        self.last_data = None

        if option is not None:
            self.option.update(option)
        self.KEY_MEAN_VALUE = f"{key}_MV"
        self.KEY_UPPER_VALUE = f"{key}_UV"
        self.KEY_LOWER_VALUE = f"{key}_LV"
        self.KEY_WIDTH_VALUE = f"{key}_Width"
        self.KEY_STD_VALUE = f"{key}_Std"

        self.is_input = is_input
        self.is_output = is_output

    @property
    def columns(self):
        return [self.KEY_MEAN_VALUE, self.KEY_UPPER_VALUE, self.KEY_LOWER_VALUE, self.KEY_WIDTH_VALUE]

    @classmethod
    def load(self, key: str, params: dict):
        window = params["window"]
        column = params["column"]
        alpha = params["alpha"]
        is_input = params["input"]
        is_out = params["output"]
        return BBANDProcess(key, window, alpha, column, is_input, is_out)

    def run(self, data: pd.DataFrame, symbols: list = [], grouped_by_symbol=False):
        option = self.option
        target_column = option["column"]
        window = option["window"]
        alpha = option["alpha"]

        if type(data.columns) == pd.MultiIndex:
            if len(symbols) == 0:
                symbols = get_symbols(data, grouped_by_symbol)
            bb_df = technical.BollingerFromOHLCMulti(
                symbols,
                data,
                column=target_column,
                window=window,
                alpha=alpha,
                grouped_by_symbol=grouped_by_symbol,
                mean_name=self.KEY_MEAN_VALUE,
                upper_name=self.KEY_UPPER_VALUE,
                lower_name=self.KEY_LOWER_VALUE,
                width_name=self.KEY_WIDTH_VALUE,
                std_name=self.KEY_STD_VALUE
            )
        else:
            bb_df = technical.BollingerFromOHLC(
                data,
                target_column,
                window=window,
                alpha=alpha,
                mean_name=self.KEY_MEAN_VALUE,
                upper_name=self.KEY_UPPER_VALUE,
                lower_name=self.KEY_LOWER_VALUE,
                width_name=self.KEY_WIDTH_VALUE,
                std_name=self.KEY_STD_VALUE
            )

        self.last_data = bb_df.iloc[-self.get_minimum_required_length() :]
        return pd.concat([data, bb_df], axis=1)

    def update(self, tick: pd.Series, symbols: list = []):
        option = self.option
        target_column = option["column"]
        window = option["window"]
        alpha = option["alpha"]

        target_data = self.last_data[target_column].values
        target_data = numpy.append(target_data, tick[target_column])
        target_data = target_data[-window:]

        new_sma = target_data.mean()
        std = target_data.std(ddof=0)
        new_ub = new_sma + alpha * std
        new_lb = new_sma - alpha * std
        new_width = alpha * 2 * std

        c_ema = self.KEY_MEAN_VALUE
        c_ub = self.KEY_UPPER_VALUE
        c_lb = self.KEY_LOWER_VALUE
        c_width = self.KEY_WIDTH_VALUE

        new_data = pd.Series({c_ema: new_sma, c_ub: new_ub, c_lb: new_lb, c_width: new_width, target_column: tick[target_column]})
        self.last_data = concat(self.last_data.iloc[1:], new_data)
        return new_data[[c_ema, c_ub, c_lb, c_width]]

    def get_minimum_required_length(self):
        return self.option["window"]


class ATRProcess(ProcessBase):
    kinds = "ATR"

    def __init__(
        self, key="atr", window=14, ohlc_column_name=("Open", "High", "Low", "Close"), is_input=True, is_output=True, option=None
    ):
        super().__init__(key)
        self.option = {"ohlc_column": ohlc_column_name, "window": window}
        if option is not None:
            self.option.update(option)

        self.last_data = None
        self.KEY_ATR = key
        self.is_input = is_input
        self.is_output = is_output

    @property
    def columns(self):
        return [self.KEY_ATR]

    @classmethod
    def load(self, key: str, params: dict):
        window = params["window"]
        columns = tuple(params["ohlc_column"])
        is_input = params["input"]
        is_out = params["output"]
        return ATRProcess(key, window, columns, is_input, is_out)

    def run(self, data: pd.DataFrame, symbols: list = [], grouped_by_symbol=False):
        option = self.option
        target_columns = option["ohlc_column"]
        window = option["window"]
        c_atr = self.KEY_ATR

        if type(data.columns) == pd.MultiIndex:
            if len(symbols) == 0:
                symbols = get_symbols(data, grouped_by_symbol)
            atr_df = technical.ATRFromMultiOHLC(
                symbols, data, target_columns, window=window, grouped_by_symbol=grouped_by_symbol, tr_name=None, atr_name=c_atr
            )
        else:
            atr_df = technical.ATRFromOHLC(data, target_columns, window=window, tr_name=None, atr_name=c_atr)
        last_ohlc = data.iloc[-self.get_minimum_required_length() :]
        last_atr = atr_df.iloc[-self.get_minimum_required_length() :]

        self.last_data = pd.concat([last_ohlc, last_atr], axis=1)
        return pd.concat([data, atr_df], axis=1)

    def update(self, tick: pd.Series, symbols: list = []):
        option = self.option
        target_columns = option["ohlc_column"]
        window = option["window"]
        c_atr = self.KEY_ATR

        pre_data = self.last_data.iloc[-1]
        new_atr_value = technical.update_ATR(pre_data, tick, target_columns, c_atr, window)
        df = tick.copy()
        df[c_atr] = new_atr_value
        self.last_data = concat(self.last_data.iloc[1:], df)
        return df[[c_atr]]

    def get_minimum_required_length(self):
        return self.option["window"]


class RSIProcess(ProcessBase):
    kinds = "RSI"

    def __init__(
        self, key="rsi", window=14, ohlc_column_name=("Open", "High", "Low", "Close"), is_input=True, is_output=True, option=None
    ):
        super().__init__(key)
        self.option = {"ohlc_column": ohlc_column_name, "window": window}

        if option is not None:
            self.option.update(option)

        self.last_data = None
        self.KEY_RSI = key
        self.KEY_GAIN = f"{key}_Gain"
        self.KEY_LOSS = f"{key}_Loss"

        self.is_input = is_input
        self.is_output = is_output

    @property
    def columns(self):
        return [self.KEY_GAIN, self.KEY_LOSS, self.KEY_RSI]

    @classmethod
    def load(self, key: str, params: dict):
        window = params["window"]
        columns = tuple(params["ohlc_column"])
        is_input = params["input"]
        is_out = params["output"]
        return RSIProcess(key, window, columns, is_input, is_out)

    def run(self, data: pd.DataFrame, symbols: list = [], grouped_by_symbol=False):
        option = self.option
        target_column = option["ohlc_column"][0]
        window = option["window"]

        if type(data.columns) == pd.MultiIndex:
            if len(symbols) == 0:
                symbols = get_symbols(data, grouped_by_symbol)
            rsi_df = technical.RSIFromOHLCMulti(
                symbols,
                data,
                target_column,
                window=window,
                grouped_by_symbol=grouped_by_symbol,
                mean_gain_name=self.KEY_GAIN,
                mean_loss_name=self.KEY_LOSS,
                rsi_name=self.KEY_RSI,
            )
        else:
            rsi_df = technical.RSIFromOHLC(
                data,
                target_column,
                window=window,
                mean_gain_name=self.KEY_GAIN,
                mean_loss_name=self.KEY_LOSS,
                rsi_name=self.KEY_RSI,
            )

        last_ohlc = data.iloc[-self.get_minimum_required_length() :]
        last_rsi = rsi_df.iloc[-self.get_minimum_required_length() :]
        self.last_data = pd.concat([last_ohlc, last_rsi], axis=1)
        return pd.concat([data, rsi_df], axis=1)

    def update(self, tick: pd.Series, symbols: list = []):
        option = self.option
        target_column = option["ohlc_column"][0]
        window = option["window"]
        columns = (*self.columns, target_column)

        pre_data = self.last_data.iloc[-1]
        new_gain_val, new_loss_val, new_rsi_value = technical.update_RSI(pre_data, tick, columns, window)
        tick[self.KEY_GAIN] = new_gain_val
        tick[self.KEY_LOSS] = new_loss_val
        tick[self.KEY_RSI] = new_rsi_value
        self.last_data = concat(self.last_data.iloc[1:], tick)
        return tick

    def get_minimum_required_length(self):
        return self.option["window"]


class RenkoProcess(ProcessBase):
    kinds = "Renko"

    def __init__(
        self,
        key: str = "renko",
        ohlc_column=("Open", "High", "Low", "Close"),
        window=None,
        brick_size=None,
        is_input=True,
        is_output=True,
        option=None,
    ):
        """Process to calculate Renko Brick

        Args:
            key (str, optional): key value to indicate in column of output. Defaults to "renko".
            ohlc_column (tuple, optional): tuple of column names for OHLC data. Defaults to ("Open", "High", "Low", "Close").
            window (int, optional): window size to calculate ATR for brick size. Defaults to None.
            brick_size (float, optional): size of each Renko brick. Defaults to None.
            is_input (bool, optional): whether this process is an input. Defaults to True.
            is_output (bool, optional): whether this process is an output. Defaults to True.
            option (dict, optional): additional options. Defaults to None.
        Exception: either window or brick_size must be specified, but not both.
        """
        super().__init__(key)
        if window is None and brick_size is None:
            raise Exception("either window or brick_size must be specified")
        if window is not None and brick_size is not None:
            raise Exception("only one of window or brick_size must be specified")
        self.option = {"ohlc_column": ohlc_column, "window": window, "brick_size": brick_size}
        if option is not None:
            self.option.update(option)
        self.is_input = is_input
        self.is_output = is_output
        self.KEY_BRICK_SIZE = f"{key}_BrickSize"
        self.KEY_VALUE = f"{key}_Value"

    @property
    def columns(self):
        return [self.KEY_BRICK_SIZE, self.KEY_VALUE]

    @classmethod
    def load(self, key: str, params: dict):
        window = params["window"]
        columns = tuple(params["ohlc_column"])
        is_input = params["input"]
        is_out = params["output"]
        return RenkoProcess(key, columns, window, is_input, is_out)

    def run(self, data: pd.DataFrame, symbols: list = [], grouped_by_symbol=False):
        option = self.option
        ohlc_column = option["ohlc_column"]
        window = option["window"]
        brick_size = option["brick_size"]

        renko_brick_size = self.KEY_BRICK_SIZE
        renko_value = self.KEY_VALUE

        if type(data.columns) == pd.MultiIndex:
            if len(symbols) == 0:
                symbols = get_symbols(data, grouped_by_symbol)
            renko_df = technical.RenkoFromMultiOHLC(
                symbols,
                data,
                ohlc_columns=ohlc_column,
                atr_window=window,
                brick_size=brick_size,
                grouped_by_symbol=grouped_by_symbol,
                total_brick_name=renko_value,
                brick_size_name=renko_brick_size,
            )
        else:
            renko_df = technical.RenkoFromOHLC(
                data, ohlc_columns=ohlc_column, brick_size=brick_size, atr_window=window,
                total_brick_name=renko_value, brick_size_name=renko_brick_size
            )
        return pd.concat([data, renko_df], axis=1)

    def update(self, tick: pd.Series, symbols: list = []):
        raise Exception("update is not implemented yet on renko process")

    def get_minimum_required_length(self):
        if self.option["brick_size"] is not None:
            return 2
        return self.option["window"] + 30


class SlopeProcess(ProcessBase):
    kinds = "Slope"

    def __init__(self, key: str = "slope", target_column="Close", window=10, is_input=True, is_output=True, option=None):
        super().__init__(key)
        self.option = {}
        self.option["target_column"] = target_column
        self.option["window"] = window
        if option is not None:
            self.option.update(option)
        self.is_input = is_input
        self.is_output = is_output
        self.KEY_SLOPE = f"{key}_slope"

    @property
    def columns(self):
        return [self.KEY_SLOPE]

    @classmethod
    def load(self, key: str, params: dict):
        window = params["window"]
        column = tuple(params["target_column"])
        is_input = params["input"]
        is_out = params["output"]
        return SlopeProcess(key, target_column=column, window=window, is_input=is_input, is_output=is_out)

    def run(self, data: pd.DataFrame, symbols: list = [], grouped_by_symbol=False):
        option = self.option
        column = option["target_column"]
        window = option["window"]
        out_column = self.KEY_SLOPE

        org_index = data.index
        if type(data.columns) == pd.MultiIndex:
            if len(symbols) == 0:
                symbols = get_symbols(data, grouped_by_symbol)
            slope_df = technical.SlopeFromOHLCMulti(
                symbols, data, window=window, column=column, grouped_by_sygnal=grouped_by_symbol, slope_name=out_column
            )
        else:
            slope_df = technical.SlopeFromOHLC(data, window=window, column=column, slope_name=out_column)
        slope_df.index = org_index[-len(slope_df) :]
        # slope_df.columns = [out_column]
        # data = pd.concat([data, slope_df], axis=1)
        # return data
        return pd.concat([data, slope_df], axis=1)

    def update(self, tick: pd.Series, symbols: list = []):
        raise Exception("update is not implemented yet on slope process")

    def get_minimum_required_length(self):
        return self.option["window"]


class CCIProcess(ProcessBase):
    kinds = "CCI"

    def __init__(
        self,
        key: str = "cci",
        window=14,
        ohlc_column=("Open", "High", "Low", "Close"),
        is_input=True,
        is_output=False,
        option=None,
    ):
        super().__init__(key)
        self.options = {"window": window, "ohlc_column": ohlc_column}

        if option is not None:
            self.options.update(option)
        self.data = None
        self.is_input = is_input
        self.is_output = is_output
        self.KEY_CCI = key

    @property
    def columns(self):
        return [self.KEY_CCI]

    @classmethod
    def load(self, key: str, params: dict):
        option = {"window": params["window"], "ohlc_column": params["ohlc_column"]}

        is_input = params["input"]
        is_out = params["output"]
        cci = CCIProcess(key, option=option, is_input=is_input, is_output=is_out)
        return cci

    def run(self, data: pd.DataFrame, symbols: list = [], grouped_by_symbol=False):
        self.data = data
        window = self.options["window"]
        ohlc_column = self.options["ohlc_column"]

        out_column = self.KEY_CCI
        if type(data.columns) == pd.MultiIndex:
            if len(symbols) == 0:
                symbols = get_symbols(data, grouped_by_symbol)
            cci_df = technical.CommodityChannelIndexMulti(
                symbols, data, window, ohlc_column, grouped_by_sygnal=grouped_by_symbol, cci_name=out_column
            )
        else:
            cci_df = technical.CommodityChannelIndex(data, window, ohlc_column, cci_name=out_column)

        return pd.concat([data, cci_df], axis=1)

    def update(self, tick: pd.Series, symbols: list = []):
        if self.data is not None:
            out_column = self.KEY_CCI
            self.data = concat(self.data, tick)
            cci = self.run(self.data)
            return cci[out_column].iloc[-1]
        else:
            self.data = tick
            # cci = numpy.nan
            print(
                f"CCI failed to update as data length is less than window size: {len(self.data) < {self.get_minimum_required_length()}}"
            )
            return self.data

    def get_minimum_required_length(self):
        return self.options["window"]


class RangeTrendProcess(ProcessBase):
    kinds = "rtp"

    def __init__(
        self, key: str = "rtp", mode="bband", required_columns=[], slope_window=4, is_input=True, is_output=True, option=None
    ):
        """Experimental: Process to caliculate likelyfood of market state
        {key}_trend: from -1 to 1. 1 then bull (long position) state is strong, -1 then cow (short position) is strong
        {key}_range: from 0 to 1. possibility of market is in range trading

        Args:
            key (str): key to differentiate processes. Defaults to rtp
            mode (str, optional): mode to caliculate the results. Defaults to "bband".
            required_columns (list, optional): columns which needs to caliculate the results. Defaults to None then columns are obtained from data
            is_input (bool, optional): client/env use this to handle in/out. Defaults to True.
            is_output (bool, optional): client/env use this to handle in/out. Defaults to True.
            option (dict, optional): option to update params. Mostly used by load func. Defaults to None.

        """
        super().__init__(key)
        self.available_mode = ["bband"]
        if mode not in self.available_mode:
            raise ValueError(f"{mode} is not supported. Please specify one of {self.available_mode}")
        self.KEY_RANGE = f"{key}_range"
        self.KEY_TREND = f"{key}_trend"
        self.initialized = False
        self.initialization_required = True
        self.required_length = slope_window + 14 + 1
        self.options = {"mode": mode}
        if mode == "bband":
            if len(required_columns) > 1:
                self.options["required_columns"] = required_columns
            self.run = self.__range_trand_by_bb
            self.initialize = self.__bb_initialization

        if option is not None and type(option) == dict:
            self.options.update(option)
        self.data = None
        self.is_input = is_input
        self.is_output = is_output
        self.__preprocess = None
        self.slope_window = slope_window

    @property
    def columns(self):
        return [self.KEY_TREND, self.KEY_RANGE]

    def __bb_initialization(self, df: pd.DataFrame, symbols: list, grouped_by_symbol):
        data = df.copy()
        self.is_multi_mode = False
        if type(data.columns) == pd.MultiIndex:
            if len(symbols) == 0:
                symbols = get_symbols(data, grouped_by_symbol)
            if grouped_by_symbol is False:
                data.columns = data.columns.swaplevel(0, 1)
            self.is_multi_mode = True
            columns = data[symbols[0]]
        else:
            columns = data.columns
            data.columns = pd.MultiIndex.from_tuples([("Dummy", column) for column in columns])
            symbols = ["Dummy"]

        default_required_columns = ["BB_Width", "BB_MV"]
        # check required columns of default value exists in data columns
        required_columns = list(set(columns) & set(default_required_columns))
        if len(required_columns) == 2:
            self.options["required_columns"] = default_required_columns
        else:
            # check modified columns of bb exist
            required_columns = ["temp", "temp"]
            for column in columns:
                # check {key}_MV
                if "_MV" in column:
                    required_columns[1] = column
                # check {key}_Width
                elif "_Width" in column:
                    required_columns[0] = column
                elif "close" == column.lower():
                    close_column = column
            if "temp" in required_columns:
                # If bband column doesn't exists, prepare Process to add bband values in run process
                if close_column is None:
                    for column in columns:
                        if "close" in column.lower():
                            close_column = column
                if close_column:
                    self.__preprocess = BBANDProcess(target_column=close_column)
                    bb_df = self.__preprocess.run(data, symbols, grouped_by_symbol=True)
                    data = pd.concat([data, bb_df], axis=1)
                    required_columns = [
                        self.__preprocess.columns[self.__preprocess.KEY_MEAN_VALUE],
                        self.__preprocess.columns[self.__preprocess.KEY_WIDTH_VALUE],
                    ]
                else:
                    raise Exception("Neither close column nor BBand columns are missing")
            self.options["required_columns"] = required_columns

        data.columns = data.columns.swaplevel(0, 1)
        width_column = required_columns[0]
        width_diff = data[width_column].diff()
        width_diff[width_diff == 0] = numpy.nan
        pct_change = width_diff.pct_change(periods=1)
        pct_normalized = pct_change / pct_change.std()
        range_possibility_df = 1 / (1 + pct_normalized.abs())
        mean_column = required_columns[1]
        slope = (data[mean_column] - data[mean_column].shift(periods=self.slope_window)) / self.slope_window

        self.options["bband"] = {
            "slope_std": slope.std() * 2,
            "slope_mean": slope.mean(),
            "pct_thread": range_possibility_df.std(),
        }
        self.initialized = True
        # common
        self.initialization_required = False

    def initialize(self, data: pd.DataFrame, symbols: list = [], grouped_by_symbol=False):
        pass

    def __range_trand_by_bb(self, df: pd.DataFrame, symbols=[], grouped_by_symbol=False, max_period=3, thresh=0.8):
        data = df.copy()
        if type(data.columns) == pd.MultiIndex and len(symbols) == 0:
            symbols = get_symbols(data, grouped_by_symbol)
        if self.initialized is False or self.initialization_required:
            self.initialize(df, symbols, grouped_by_symbol)

        params = self.options

        if self.is_multi_mode is False:
            columns = data.columns
            data.columns = pd.MultiIndex.from_tuples([(column, "Dummy") for column in columns])
            symbols = ["Dummy"]
        elif grouped_by_symbol:
            data.columns = data.columns.swaplevel(0, 1)

        if "bb_process" in params:
            process = params["bb_process"]
            bb_df = process.run(data, symbols, False)
            data = pd.concat([data, bb_df], axis=1)

        required_columns = params["required_columns"]
        width_column = required_columns[0]
        mean_column = required_columns[1]

        # caliculate a width is how differ from previous width
        period = 1
        width_diff = data[width_column].diff()
        width_diff[width_diff == 0] = numpy.nan
        width_diff.ffill(inplace=True)
        pct_change = width_diff.pct_change(periods=period)
        pct_normalized = pct_change / pct_change.std()
        range_possibility_dfs = 1 / (1 + pct_normalized.abs())

        # remove indicies if latest tick is not a range
        range_markets = (pct_change <= thresh) & (pct_change >= -thresh)

        data.columns = data.columns.swaplevel(0, 1)

        slope_df_list = []
        possibility_df_list = []

        for symbol in symbols:
            period = 1
            range_count_df = range_markets[symbol]
            range_data = data[symbol][range_markets[symbol]]
            indices = range_data.index
            data_ = data[symbol]
            range_possibility_df = range_possibility_dfs[symbol]
            while len(indices) > 0 and period < max_period:
                period += 1
                width_dff = data_[width_column].shift(periods=period - 1).diff()
                width_dff[width_dff == 0] = numpy.nan
                width_dff.ffill(inplace=True)
                pct_change = width_dff.pct_change(periods=period)
                pct_normalized = pct_change / pct_change.std()
                pct_change = 1 / (1 + pct_normalized.abs())
                cont_pct_change = pct_change[indices]
                range_market = (cont_pct_change <= thresh) & (cont_pct_change >= -thresh)
                range_data = cont_pct_change[range_market]
                indices = range_data.index
                new_pos = range_possibility_df.copy()
                range_cont_num = range_count_df.copy()
                new_pos += pct_change
                range_cont_num = range_cont_num[indices] + 1
                range_possibility_df = new_pos
                range_count_df = range_cont_num
            range_possibility_df = range_possibility_df / max_period

            # caliculate slope by mean value
            window_for_slope = self.slope_window
            # window_for_slope = 14#bollinger window size
            shifted = data_[mean_column].shift(periods=window_for_slope)
            slope = (data_[mean_column] - shifted) / window_for_slope
            smean = params["bband"]["slope_mean"][symbol]
            sstd = params["bband"]["slope_std"][symbol]
            slope = slope.clip(smean - sstd, smean + sstd)
            slope = slope / (smean + sstd)
            slope_df_list.append(slope)
            possibility_df_list.append(range_possibility_df)
        slope_dfs = pd.concat(slope_df_list, axis=1)
        possibility_dfs = pd.concat(possibility_df_list, axis=1)
        cls = [self.KEY_TREND, self.KEY_RANGE]
        elements, columns = technical.create_multi_out_lists(
            symbols, [slope_dfs, possibility_dfs], cls, grouped_by_symbol=grouped_by_symbol
        )
        out_df = pd.concat(elements, axis=1)
        if self.is_multi_mode:
            out_df.columns = columns
        else:
            out_df.columns = cls
        return pd.concat([df, out_df], axis=1)

    @classmethod
    def load(self, key: str, params: dict):
        mode = params["mode"]
        required_columns = params["required_columns"]
        is_input = params["input"]
        is_out = params["output"]
        process = RangeTrendProcess(key, mode=mode, required_columns=required_columns, is_input=is_input, is_output=is_out)
        return process

    def run(self, data: pd.DataFrame):
        return data

    def update(self, tick: pd.Series, symbols: list = []):
        print("not supported for now")

    def get_minimum_required_length(self):
        return self.required_length

    def revert(self, data_set: tuple):
        print("not supported for now")
        return False, None


class LinearRegressionMomentumProcess:
    def __init__(self, window: int = 90, column: str = "Close", trading_days: int = None, key="lrm") -> None:
        self.window = window
        self.column = column
        self.KEY_MOMENTUM = f"{key}_momentum"
        if trading_days is None:
            self.trading_days = window
        else:
            self.trading_days = trading_days

    @property
    def columns(self):
        return [self.KEY_MOMENTUM]

    @property
    def options(self):
        return {
            "window": self.window,
            "column": self.column,
            "trading_days": self.trading_days,
            "key": self.KEY_MOMENTUM.split("_")[0],
        }

    def __get_momentum(self, data):
        log_data = numpy.log(data)
        x_data = numpy.arange(len(log_data))
        beta, intercept, rvalue, pvalue, stderr = linregress(x_data, log_data)
        return ((1 + beta) ** 252) * (rvalue**2)

    def run(self, df: pd.DataFrame, symbols: list = [], grouped_by_symbol=False) -> pd.DataFrame:
        if grouped_by_symbol == False:
            close_dfs = df[self.column]
            if isinstance(close_dfs, pd.Series):
                momentum_df = close_dfs.dropna().rolling(self.window).apply(self.__get_momentum, raw=False)
                momentum_df.name = self.KEY_MOMENTUM
            else:
                if len(symbols) == 0:
                    symbols = close_dfs.columns
                MDFS = {}
                # directry apply rolling method to multiindex dataframe works, but dropna drops data is any symbol is NaN.
                for symbol in symbols:
                    MDFS[(self.KEY_MOMENTUM, symbol)] = (
                        close_dfs[symbol].dropna().rolling(self.window).apply(self.__get_momentum, raw=False)
                    )
                momentum_df = pd.concat(MDFS.values(), keys=MDFS.keys(), axis=1)
        if grouped_by_symbol == True:
            close_dfs = df.xs(self.column, axis=1, level=1)
            if len(symbols) == 0:
                symbols = close_dfs.columns
            MDFS = {}
            for symbol in symbols:
                MDFS[(symbol, self.KEY_MOMENTUM)] = (
                    close_dfs[symbol].dropna().rolling(self.window).apply(self.__get_momentum, raw=False)
                )
            momentum_df = pd.concat(MDFS.values(), keys=MDFS.keys(), axis=1)

        return pd.concat([df, momentum_df], axis=1)

    def get_minimum_required_length(self):
        return self.options["window"]
