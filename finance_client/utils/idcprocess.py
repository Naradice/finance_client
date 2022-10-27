import numpy
from finance_client.utils import indicaters
import pandas as pd
from finance_client.utils.process import ProcessBase

""" process class to add indicater for data_client, dataset, env etc
    is_input, is_output are used for machine learning
"""

#TODO: make output DataFrame
def get_available_processes() -> dict:
    processes = {
        'MACD': MACDpreProcess,
        'EMA': EMApreProcess,
        'BBAND': BBANDpreProcess,
        'ATR': ATRpreProcess,
        'RSI': RSIpreProcess,
        #'Roll': RollingProcess,
        'Renko': RenkoProcess,
        'Slope': SlopeProcess
    }
    return processes

def indicaters_to_params(processes:list) -> dict:
    """ convert procese list to dict for saving params as file

    Args:
        processes (list: ProcessBase): indicaters defined in preprocess.py
        
    Returns:
        dict: {'input':{key:params}, 'output':{key: params}}
    """
    params = {}
    
    for process in processes:
        option = process.option
        option['kinds'] = process.kinds
        option['input'] = process.is_input
        option['output'] = process.is_output
        params[process.key]  = option
    return params

def load_indicaters(params:dict) -> list:
    ips_dict = get_available_processes()
    ids = []
    for key, param in params.items():
        kinds = param['kinds']
        idc = ips_dict[kinds]
        idc = idc.load(key, param)
        ids.append(idc)
    return ids

class MACDpreProcess(ProcessBase):
    
    kinds = 'MACD'
    last_data = None
    
    KEY_SHORT_EMA = 'S_EMA'
    KEY_LONG_EMA = 'L_EMA'
    KEY_MACD = 'MACD'
    KEY_SIGNAL = 'Signal'
    
    def __init__(self, key='macd', target_column = "Close", short_window=12, long_window=26, signal_window = 9, option=None, is_input=True, is_output=True):
        super().__init__(key)
        self.option = {
            "column": target_column,
            "short_window": short_window,
            "long_window": long_window,
            "signal_window": signal_window
        }
        
        if option != None:
            self.option.update(option)
        self.columns = {
            self.KEY_SHORT_EMA: f'{key}_S_EMA', self.KEY_LONG_EMA:f'{key}_L_EMA', self.KEY_MACD:f'{key}_MACD', self.KEY_SIGNAL:f'{key}_Signal'
        }
        self.is_input = is_input
        self.is_output = is_output
    
    @classmethod
    def load(self, key:str, params:dict):
        option = {
            "column": params["column"],
            "short_window": params["short_window"],
            "long_window": params["long_window"],
            "signal_window": params["signal_window"]
        }
        is_input = params["input"]
        is_out = params["output"]
        macd = MACDpreProcess(key, option=option, is_input=is_input, is_output=is_out)
        return macd
        
    def run(self, symbols:list, data: pd.DataFrame, grouped_by_symbol=False):
        option = self.option
        target_column = option['column']
        short_window = option['short_window']
        long_window = option['long_window']
        signal_window = option['signal_window']
        
        cs_ema = self.columns[self.KEY_SHORT_EMA]
        cl_ema = self.columns[self.KEY_LONG_EMA]
        c_macd = self.columns[self.KEY_MACD]
        c_signal = self.columns[self.KEY_SIGNAL]
        
        if type(symbols) == list and len(symbols) > 0:
            macd_df = indicaters.MACDFromOHLCMulti(symbols, data, target_column, short_window, long_window, signal_window, grouped_by_symbol,
                                                   short_ema_name=cs_ema, long_ema_name=cl_ema, macd_name=c_macd, signal_name=c_signal)
        else:
            macd_df = indicaters.MACDFromOHLC(data, target_column, short_window, long_window, signal_window,
                                              short_ema_name=cs_ema, long_ema_name=cl_ema, macd_name=c_macd, signal_name=c_signal)
        
        self.last_data = macd_df.iloc[-self.get_minimum_required_length():]
        return macd_df
    
    def update(self, symbols:list, tick:pd.Series):
        if type(symbols) == list and len(symbols) > 0:
            print("update is not implemented for multi symbols yet")
        else:
            option = self.option
            target_column = option['column']
            short_window = option['short_window']
            long_window = option['long_window']
            signal_window = option['signal_window']
            
            cs_ema = self.columns[self.KEY_SHORT_EMA]
            cl_ema = self.columns[self.KEY_LONG_EMA]
            c_macd = self.columns[self.KEY_MACD]
            c_signal = self.columns[self.KEY_SIGNAL]
            
            
            short_ema, long_ema, MACD = indicaters.update_macd(
                new_tick=tick,
                short_ema_value=self.last_data[cs_ema].iloc[-1],
                long_ema_value=self.last_data[cl_ema].iloc[-1],
                column=target_column, short_window = short_window, long_window = long_window)
            Signal = (self.last_data[c_macd].iloc[-signal_window + 1:].sum() + MACD)/signal_window
            
            new_data = pd.Series({cs_ema:short_ema, cl_ema: long_ema, c_macd:MACD, c_signal:Signal})
            self.last_data = self.concat(self.last_data.iloc[1:], new_data)
            return new_data
        
    def get_minimum_required_length(self):
        return self.option["long_window"] + self.option["signal_window"] - 2
    
    def revert(self, data_set:tuple):
        cs_ema = self.columns['S_EMA']
        
        if type(data_set) == pd.DataFrame:
            if cs_ema in data_set:
                data_set = (data_set[cs_ema],)
        #assume ShortEMA is in 1st
        short_ema = data_set[0]
        short_window = self.option['short_window']
        out = indicaters.revert_EMA(short_ema, short_window)
        return True, out

class EMApreProcess(ProcessBase):
    
    kinds = 'EMA'
    last_data = None
    KEY_EMA = "EMA"
    
    def __init__(self, key='ema', window = 12, column = 'Close', is_input=True, is_output=True, option = None):
        super().__init__(key)
        self.option = {
            "column": column,
            "window": window
        }

        if option != None:
            self.option.update(option)
        self.columns = {
            self.KEY_EMA:f'{key}_EMA'
        }
        self.is_input = is_input
        self.is_output = is_output
    
    @classmethod
    def load(self,key:str, params:dict):
        window = params['window']
        column = params['column']
        is_input = params['input']
        is_out = params['output']
        indicater = EMApreProcess(key, window=window, column = column, is_input=is_input, is_output=is_out)
        return indicater

    def run(self, symbols:list, data: pd.DataFrame, grouped_by_symbol=False):
        option = self.option
        target_column = option['column']
        window = option['window']
        column = self.columns["EMA"]
        
        if type(symbols) == list and len(symbols) > 0:
            ema = indicaters.EMAMulti(symbols, data, window, grouped_by_symbol=grouped_by_symbol, ema_name=column)
        else:
            ema = indicaters.EMA(data[target_column], window)
            ema.columns = [column]
            
        self.last_data = ema.iloc[-self.get_minimum_required_length():]
        return ema
    
    def update(self, symbols:list, tick:pd.Series):
        option = self.option
        target_column = option['column']
        window = option['window']
        column = self.columns["EMA"]
        
        
        short_ema, long_ema, MACD = indicaters.update_ema(
            new_tick=tick,
            column=target_column,
            window = window)
        
        new_data = pd.Series({column:short_ema})
        self.last_data = self.concat(self.last_data.iloc[1:], new_data)
        return new_data
        
    def get_minimum_required_length(self):
        return self.option["window"]
    
    def revert(self, data_set:tuple):
        #assume EMA is in 1st
        ema = data_set[0]
        window = self.option['window']
        out = indicaters.revert_EMA(ema, window)
        return True, out

class BBANDpreProcess(ProcessBase):
    
    kinds = 'BBAND'
    last_data = None
    KEY_MEAN_VALUE = "MV"
    KEY_UPPER_VALUE = "UV"
    KEY_LOWER_VALUE = "LV"
    KEY_WIDTH_VALUE = "Width"
    
    def __init__(self, key='bolinger', window = 14, alpha=2, target_column = 'Close', is_input=True, is_output=True, option = None):
        super().__init__(key)
        self.option = {
            "column": target_column,
            "window": window,
            'alpha': alpha
        }

        if option != None:
            self.option.update(option)
        self.columns = {
            self.KEY_MEAN_VALUE: f"{key}_MV",
            self.KEY_UPPER_VALUE: f"{key}_UV",
            self.KEY_LOWER_VALUE: f"{key}_LV",
            self.KEY_WIDTH_VALUE: f"{key}_Width"
        }
        self.is_input = is_input
        self.is_output = is_output
        
    @classmethod
    def load(self, key:str, params:dict):
        window = params["window"]
        column = params["column"]
        alpha = params["alpha"]
        is_input = params["input"]
        is_out = params["output"]
        return BBANDpreProcess(key, window, alpha, column, is_input, is_out)
    
    def run(self, symbols:list, data: pd.DataFrame, grouped_by_symbol=False):
        option = self.option
        target_column = option['column']
        window = option['window']
        alpha = option['alpha']
        c_ema = self.columns[self.KEY_MEAN_VALUE]
        c_ub = self.columns[self.KEY_UPPER_VALUE]
        c_lb = self.columns[self.KEY_LOWER_VALUE]
        c_width = self.columns[self.KEY_WIDTH_VALUE]
        
        if type(symbols) == list and len(symbols) > 0:
            bb_df = indicaters.BolingerFromOHLCMulti(symbols, data, window=window, alpha=alpha, grouped_by_symbol=grouped_by_symbol,
                                                mean_name=c_ema, upper_name=c_ub, lower_name=c_lb, width_name=c_width, std_name=None)
        else:
            bb_df = indicaters.BolingerFromOHLC(data, target_column, window=window, alpha=alpha,
                                                mean_name=c_ema, upper_name=c_ub, lower_name=c_lb, width_name=c_width, std_name=None)
        
        self.last_data = bb_df.iloc[-self.get_minimum_required_length():]
        return bb_df
    
    def update(self, symbols:list, tick:pd.Series):
        option = self.option
        target_column = option['column']
        window = option['window']
        alpha = option['alpha']
        
        target_data = self.last_data[target_column].values
        target_data = numpy.append(target_data, tick[target_column])
        target_data = target_data[-window:]
        
        
        new_sma = target_data.mean()
        std = target_data.std(ddof=0)
        new_ub = new_sma + alpha * std
        new_lb = new_sma - alpha * std
        new_width = alpha*2*std
        
        c_ema = self.columns[self.KEY_MEAN_VALUE]
        c_ub = self.columns[self.KEY_UPPER_VALUE]
        c_lb = self.columns[self.KEY_LOWER_VALUE]
        c_width = self.columns[self.KEY_WIDTH_VALUE]
        
        new_data = pd.Series({c_ema:new_sma, c_ub: new_ub, c_lb:new_lb, c_width:new_width, target_column: tick[target_column]})
        self.last_data = self.concat(self.last_data.iloc[1:], new_data)
        return new_data[[c_ema, c_ub, c_lb, c_width]]
        
    def get_minimum_required_length(self):
        return self.option['window']
    
    def revert(self, data_set:tuple):
        #pass
        return False, None

class ATRpreProcess(ProcessBase):
    
    kinds = 'ATR'
    last_data = None
    KEY_ATR = "ATR"
    
    def __init__(self, key='atr', window = 14, ohlc_column_name = ('Open', 'High', 'Low', 'Close'), is_input=True, is_output=True, option = None):
        super().__init__(key)
        self.available_columns = [self.KEY_ATR]
        self.option = {
            "ohlc_column": ohlc_column_name,
            "window": window
        }
        if option != None:
            self.option.update(option)

        self.columns = {self.KEY_ATR: f'{key}_ATR'}
        self.is_input = is_input
        self.is_output = is_output
        
    @classmethod
    def load(self, key:str, params:dict):
        window = params["window"]
        columns = tuple(params["ohlc_column"])
        is_input = params["input"]
        is_out = params["output"]
        return ATRpreProcess(key, window, columns, is_input, is_out)

    def run(self, symbols:list, data: pd.DataFrame, grouped_by_symbol=False):
        option = self.option
        target_columns = option['ohlc_column']
        window = option['window']
        c_atr = self.columns[self.KEY_ATR]
            
        if type(symbols) == list and len(symbols) > 0:
            atr_df = indicaters.ATRFromMultiOHLC(symbols, data, target_columns, window=window, grouped_by_symbol=grouped_by_symbol,
                                                 tr_name=None, atr_name=c_atr)
        else:
            atr_df = indicaters.ATRFromOHLC(data, target_columns, window=window, tr_name=None, atr_name=c_atr)
        last_ohlc = data.iloc[-self.get_minimum_required_length():]
        last_atr = atr_df.iloc[-self.get_minimum_required_length():]
    
        self.last_data = pd.concat([last_ohlc, last_atr], axis=1)
        return atr_df
        
    def update(self, symbols:list, tick:pd.Series):
        option = self.option
        target_columns = option['ohlc_column']
        window = option['window']
        c_atr = self.columns[self.KEY_ATR]
        
        pre_data = self.last_data.iloc[-1]
        new_atr_value = indicaters.update_ATR(pre_data, tick, target_columns, c_atr, window)
        df = tick.copy()
        df[c_atr] = new_atr_value
        self.last_data = self.concat(self.last_data.iloc[1:], df)
        return df[[c_atr]]
        
    def get_minimum_required_length(self):
        return self.option['window']
    
    def revert(self, data_set:tuple):
        #pass
        return False, None

class RSIpreProcess(ProcessBase):
    
    kinds = 'RSI'
    last_data = None
    
    KEY_RSI = "RSI"
    KEY_GAIN = "GAIN"
    KEY_LOSS = "LOSS"
    
    def __init__(self, key='rsi', window = 14, ohlc_column_name = ('Open', 'High', 'Low', 'Close'), is_input=True, is_output=True, option = None):
        super().__init__(key)
        self.available_columns = ["RSI", "AVG_GAIN", "AVG_LOSS"]
        self.option = {
            "ohlc_column": ohlc_column_name,
            "window": window
        }

        if option != None:
            self.option.update(option)

        self.columns = {
            self.KEY_RSI: f'{key}_RSI',
            self.KEY_GAIN: f'{key}_AVG_GAIN',
            self.KEY_LOSS: f'{key}_AVG_LOSS'
        }
        self.is_input = is_input
        self.is_output = is_output
        
    @classmethod
    def load(self, key:str, params:dict):
        window = params["window"]
        columns = tuple(params["ohlc_column"])
        is_input = params["input"]
        is_out = params["output"]
        return RSIpreProcess(key, window, columns, is_input, is_out)

    def run(self, symbols:list, data: pd.DataFrame, grouped_by_symbol=False):
        option = self.option
        target_column = option['ohlc_column'][0]
        window = option['window']
        
        c_rsi = self.columns[self.KEY_RSI]
        c_gain = self.columns[self.KEY_GAIN]
        c_loss = self.columns[self.KEY_LOSS]
        
        if type(symbols) == list and len(symbols) > 0:
            rsi_df = indicaters.RSIFromOHLCMulti(symbols, data, target_column, window=window, grouped_by_symbol=grouped_by_symbol,                                 
                                                mean_gain_name=c_gain, mean_loss_name=c_loss, rsi_name=c_rsi)
        else:
            rsi_df = indicaters.RSIFromOHLC(data, target_column, window=window,
                                                mean_gain_name=c_gain, mean_loss_name=c_loss, rsi_name=c_rsi)
            
        last_ohlc = data.iloc[-self.get_minimum_required_length():]
        last_rsi = rsi_df.iloc[-self.get_minimum_required_length():]
        self.last_data = pd.concat([last_ohlc, last_rsi], axis=1)
        return rsi_df
        
    def update(self,symbols:list, tick:pd.Series):
        option = self.option
        target_column = option['ohlc_column'][0]
        window = option['window']
        c_rsi = self.columns[self.KEY_RSI]
        c_gain = self.columns[self.KEY_GAIN]
        c_loss = self.columns[self.KEY_LOSS]
        columns = (c_gain, c_loss, c_rsi, target_column)
        
        pre_data = self.last_data.iloc[-1]
        new_gain_val, new_loss_val, new_rsi_value = indicaters.update_RSI(pre_data, tick, columns, window)
        tick[c_gain] = new_gain_val
        tick[c_loss] = new_loss_val
        tick[c_rsi] = new_rsi_value
        self.last_data = self.concat(self.last_data.iloc[1:], tick)
        return tick[[c_rsi]]
        
    def get_minimum_required_length(self):
        return self.option['window']
    
    def revert(self, data_set:tuple):
        #pass
        return False, None

class RenkoProcess(ProcessBase):
    
    kinds = "Renko"
    KEY_VALUE = "Value"
    KEY_BRICK_NUM = "NUM"
    
    def __init__(self, key: str = "renko", ohlc_column = ('Open', 'High', 'Low', 'Close'), window=10, is_input = True, is_output = True, option = None):
        super().__init__(key)
        self.option = {
            "ohlc_column": ohlc_column,
            "window": window
        }
        if option != None:
            self.option.update(option)
        self.is_input = is_input
        self.is_output = is_output
        self.columns = {
            self.KEY_BRICK_NUM: f'{key}BrickNum',
            self.KEY_VALUE: f'{key}Value',
        }
        
    @classmethod
    def load(self, key:str, params:dict):
        window = params["window"]
        columns = tuple(params["ohlc_column"])
        is_input = params["input"]
        is_out = params["output"]
        return RenkoProcess(key, columns, window, is_input, is_out)

    def run(self, symbols:list, data: pd.DataFrame, grouped_by_symbol=False):
        option = self.option
        ohlc_column = option["ohlc_column"]
        window = option['window']
        TOTAL_BRICK_NUM_KEY = "Renko"
        BRICK_NUM_KEY = "Brick"
        
        renko_block_num = self.columns[self.KEY_BRICK_NUM]
        renko_value = self.columns[self.KEY_VALUE]
        if type(symbols) == list and len(symbols) > 0:
            renko_df = indicaters.RenkoFromMultiOHLC(symbols, data, ohlc_columns=ohlc_column, atr_window=window, grouped_by_symbol=grouped_by_symbol,
                                                     total_brick_name=renko_value, brick_num_name=renko_block_num)
        else:
            renko_df = indicaters.RenkoFromOHLC(data, ohlc_columns=ohlc_column, atr_window=window, total_brick_name=renko_value, brick_num_name=renko_block_num)
        return renko_df
        
    def update(self, symbols:list, tick:pd.Series):
        raise Exception("update is not implemented yet on renko process")
        
    def get_minimum_required_length(self):
        return self.option['window']
    
    def revert(self, data_set:tuple):
        #pass
        return False, None

class SlopeProcess(ProcessBase):
    
    kinds = "Slope"
    KEY_SLOPE = "Slope"
    
    def __init__(self, key: str = "slope", target_column = "Close", window = 10, is_input = True, is_output = True, option = None):
        super().__init__(key)
        self.option = {}
        self.option["target_column"] = target_column
        self.option["window"] = window
        if option != None:
            self.option.update(option)
        self.is_input = is_input
        self.is_output = is_output
        self.columns = {
            self.KEY_SLOPE: f'{key}_slope'
        }
        
    @classmethod
    def load(self, key:str, params:dict):
        window = params["window"]
        column = tuple(params["target_column"])
        is_input = params["input"]
        is_out = params["output"]
        return SlopeProcess(key, target_column=column, window=window, is_input=is_input, is_output=is_out)

    def run(self, symbols:list, data: pd.DataFrame, grouped_by_symbol=False):
        option = self.option
        column = option["target_column"]
        window = option['window']
        out_column = self.columns[self.KEY_SLOPE]
        
        if type(symbols) == list and len(symbols) > 0:
            slope_df = indicaters.SlopeFromOHLCMulti(symbols, data, window=window, column=column, grouped_by_sygnal=grouped_by_symbol, slope_name=out_column)
        else:
            slope_df = indicaters.SlopeFromOHLC(data, window=window, column=column, slope_name=out_column)
        #slope_df.columns = [out_column]
        #data = pd.concat([data, slope_df], axis=1)
        #return data
        return slope_df
        
    def update(self, symbols:list, tick:pd.Series):
        raise Exception("update is not implemented yet on slope process")
        
    def get_minimum_required_length(self):
        return self.option['window']
    
    def revert(self, data_set:tuple):
        #pass
        return False, None

class CCIProcess(ProcessBase):
    
    kinds = "CCI"
    KEY_CCI = "CCI"
    
    def __init__(self, key: str = "cci", window=14, ohlc_column = ('Open', 'High', 'Low', 'Close'), is_input = True, is_output = False, option = None):
        super().__init__(key)
        self.options = {
            "window": window,
            "ohlc_column": ohlc_column
        }

        if option != None:
            self.options.update(option)
        self.data = None
        self.is_input = is_input
        self.is_output = is_output
        self.columns = {
            self.KEY_CCI: f'{key}_cci'
        }

    @classmethod
    def load(self, key:str, params:dict):
        option = {
            "window": params["window"],
            "ohlc_column": params["ohlc_column"]
        }

        is_input = params["input"]
        is_out = params["output"]
        cci = CCIProcess(key, option=option, is_input=is_input, is_output=is_out)
        return cci
        
    def run(self, symbols:list, data: pd.DataFrame, grouped_by_symbol=False):
        self.data = data
        window = self.options["window"]
        ohlc_column = self.options["ohlc_column"]
        
        out_column = self.columns[self.KEY_CCI]
        if type(symbols) == list and len(symbols) > 0:
            cci_df = indicaters.CommodityChannelIndexMulti(symbols, data, window, ohlc_column, grouped_by_sygnal=grouped_by_symbol,
                                                        cci_name=out_column)
        else:
            cci_df = indicaters.CommodityChannelIndex(data, window, ohlc_column, cci_name=out_column)
        
        return cci_df
    
    def update(self, symbols:list, tick: pd.Series):
        if type(self.data) != type(None):
            out_column = self.columns[self.KEY_CCI]
            self.data = self.concat(self.data, tick)
            cci = self.run(self.data)
            return cci[out_column].iloc[-1]
        else:
            self.data = tick
            #cci = numpy.nan
            return self.data
        
    def get_minimum_required_length(self):
        return self.options['window']
    
    def revert(self, data_set:tuple):
        #pass
        return False, None        

class RangeTrendProcess(ProcessBase):
    
    kinds = "rtp"
    available_mode = ["bband"]
    KEY_TREND = "trend"
    KEY_RANGE = "range"
    
    def __init__(self, key: str = "rtp", mode="bband", required_columns=[], slope_window=4,use_sample_param=False, is_input=True, is_output=True, option=None):
        """Process to caliculate likelyfood of market state
        {key}_trend: from -1 to 1. 1 then bull (long position) state is strong, -1 then cow (short position) is strong
        {key}_range: from 0 to 1. possibility of market is in range trading

        Args:
            key (str): key to differentiate processes. Defaults to rtp
            mode (str, optional): mode to caliculate the results. Defaults to "bband".
            required_columns (list, optional): columns which needs to caliculate the results. Defaults to None then columns are obtained from data
            is_input (bool, optional): client/env use this to handle in/out. Defaults to True.
            is_output (bool, optional): client/env use this to handle in/out. Defaults to True.
            use_sample_param (bool, optional): Use SAMPLE params caliculated by daily USDJPY. Defaults to False. Recommend to run with entire data if False
            option (dict, optional): option to update params. Mostly used by load func. Defaults to None.

        """
        super().__init__(key)
        if mode not in self.available_mode:
            raise ValueError(f"{mode} is not supported. Please specify one of {self.available_mode}")
        self.initialized = False
        self.initialization_required = True
        ## params for default. This is caliculated by daily USDJPY.
        self.params = {
            "bband":{
                "slope_std": 0.193937,
                "slope_mean": -0.002998,
                "pct_thread":0.359497
            }
        }
        self.required_length = slope_window + 14 + 1
        self.options = {
            "mode": mode
        }
        if mode == "bband":
            if len(required_columns) > 1:
                self.options["required_columns"] = required_columns
            self.run = self.__range_trand_by_bb
            self.initialize = self.__bb_initialization
                
        if option != None and type(option) == dict:
            self.options.update(option)
        self.data = None
        self.is_input = is_input
        self.is_output = is_output
        self.__preprocess = None
        self.use_sample_param = use_sample_param
        self.slope_window = slope_window
        self.columns = {
            self.KEY_RANGE: f'{key}_range',
            self.KEY_TREND: f'{key}_trend'
        }
        
    def __bb_initialization(self,symbols:list, data:pd.DataFrame, grouped_by_symbol):
        close_column = None
        if "required_columns" not in self.options:
            default_required_columns = ["bolinger_Width", "bolinger_MV"]
            required_columns = list(set(data.columns) & set(default_required_columns))
            if len(required_columns) == 2:
                self.options["required_columns"] = default_required_columns
            else:
                required_columns = ["temp", "temp"]
                for column in data.columns:
                    if "_MV" in column:
                        required_columns[1] = column
                    elif "_Width" in column:
                        required_columns[0] = column
                    elif "close" in column.lower():
                        close_column = column
                if "temp" in required_columns and close_column:
                    self.__preprocess = BBANDpreProcess(target_column=close_column)
                    required_columns = default_required_columns
                self.options["required_columns"] = required_columns
        if self.use_sample_param is False:
            print("assume enough data length is provided to caliculate std etc.")
            if close_column is None:
                for column in data.columns:
                    if "close" in column.lower():
                        close_column = column
            width_column = required_columns[1]
            pct_change = (data[width_column].diff()+1).pct_change(periods=1)
            pct_normalized = pct_change/pct_change.std()
            range_possibility_df = 1/(1+pct_normalized.abs())
            mean_column = required_columns[1]
            slope = (data[mean_column] - data[mean_column].shift(periods=self.slope_window))/self.slope_window
            self.params["bband"] = {
                "slope_std": slope.std()*2,
                "slope_mean": slope.mean(),
                "pct_thread":range_possibility_df.std()
            } 
        self.initialized = True
        #common
        self.initialization_required = False
    
    def initialize(self, symbols:list, data: pd.DataFrame, grouped_by_symbol=False):
        pass
    
    def __range_trand_by_bb(self, data:pd.DataFrame, max_period=1, thresh = 0.8):
        #currentry max_period > 1 don't work well
        max_period=3
        if self.initialized == False:
            self.__bb_initialization(data)
        if self.__preprocess is not None:
            data = data.copy()
            result_dict = self.__preprocess.run(data)
            for key, value in result_dict.items():
                data[key] = value
        required_columns = self.options["required_columns"]
        
        width_column = required_columns[0]
        mean_column = required_columns[1]
        
        # caliculate a width is how differ from previous width
        period = 1
        pct_change = (data[width_column].diff()+1).pct_change(periods=period)
        pct_normalized = pct_change/pct_change.std()
        range_possibility_df = 1/(1+pct_normalized.abs())
        #remove indicies if latest tick is not a range
        range_market = (pct_change <= thresh) & (pct_change >= -thresh)
        range_count_df = range_market
        range_data = data[range_market]
        indices = range_data.index

        while len(indices) > 0 and period < max_period:
            period+=1
            pct_change = (data[width_column].shift(periods=period-1).diff()+1).pct_change(periods=period)
            pct_normalized = pct_change/pct_change.std()
            pct_change= 1/(1+pct_normalized.abs())
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
        range_possibility_df = range_possibility_df/max_period
        
        # caliculate slope by mean value
        window_for_slope = self.slope_window
        #window_for_slope = 14#bolinger window size
        shifted = data[mean_column].shift(periods=window_for_slope)
        slope = (data[mean_column] - shifted)/window_for_slope
        smean = self.params["bband"]["slope_mean"]
        sstd = self.params["bband"]["slope_std"]
        slope = slope.clip(smean-sstd, smean+sstd)
        slope = slope/(smean+sstd)
        
        return {self.columns[self.KEY_RANGE]:range_possibility_df, self.columns[self.KEY_TREND]:slope}

    @classmethod
    def load(self, key:str, params:dict):
        mode = params["mode"]
        required_columns = params["required_columns"]
        is_input = params["input"]
        is_out = params["output"]
        process = RangeTrendProcess(key, mode=mode, required_columns=required_columns, is_input=is_input, is_output=is_out)
        return process
        
    def run(self, data:pd.DataFrame):
        return data
    
    def update(self, tick: pd.Series):
        print("not supported for now")
        
    def get_minimum_required_length(self):
        return self.required_length
    
    def revert(self, data_set:tuple):
        print("not supported for now")
        return False, None 

####
# Not implemented as I can't caliculate required length
"""
class RollingProcess(ProcessBase):
    kinds = 'Roll'
    last_tick:pd.DataFrame = None
    
    def __init__(self, key = "roll", frame_from:int = 5, frame_to: int = 30, is_input=True, is_output=True):
        super().__init__(key)
        self.frame_from = frame_from
        self.frame_to = frame_to
        self.is_input = is_input
        self.is_output = is_output
        raise NotImplemented
    
    @classmethod
    def load(self, key:str, params:dict):
        raise NotImplemented
        
    def run(self, data: pd.DataFrame) -> dict:
        raise NotImplemented
        return None
    
    def update(self, tick:pd.Series):
        raise NotImplemented
        return None
        
    
    def get_minimum_required_length(self):
        raise NotImplemented
    
    def revert(self, data_set: tuple):
        raise NotImplemented
"""