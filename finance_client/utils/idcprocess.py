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
            'S_EMA': f'{key}_S_EMA', 'L_EMA':f'{key}_L_EMA', 'MACD':f'{key}_MACD', 'Signal':f'{key}_Signal'
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
        

    def run(self, data: pd.DataFrame):
        option = self.option
        target_column = option['column']
        short_window = option['short_window']
        long_window = option['long_window']
        signal_window = option['signal_window']
        
        short_ema, long_ema, MACD, Signal = indicaters.MACD_from_ohlc(data, target_column, short_window, long_window, signal_window)
        
        cs_ema = self.columns['S_EMA']
        cl_ema = self.columns['L_EMA']
        c_macd = self.columns['MACD']
        c_signal = self.columns['Signal']
        
        self.last_data = pd.DataFrame({cs_ema:short_ema, cl_ema: long_ema, c_macd:MACD, c_signal:Signal}).iloc[-self.get_minimum_required_length():]
        return {cs_ema:short_ema, cl_ema: long_ema, c_macd:MACD, c_signal:Signal}
    
    def update(self, tick:pd.Series):
        option = self.option
        target_column = option['column']
        short_window = option['short_window']
        long_window = option['long_window']
        signal_window = option['signal_window']
        
        cs_ema = self.columns['S_EMA']
        cl_ema = self.columns['L_EMA']
        c_macd = self.columns['MACD']
        c_signal = self.columns['Signal']
        
        
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
    
    def __init__(self, key='ema', window = 12, column = 'Close', is_input=True, is_output=True, option = None):
        super().__init__(key)
        self.option = {
            "column": column,
            "window": window
        }

        if option != None:
            self.option.update(option)
        self.columns = {
            "EMA":f'{key}_EMA'
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

    def run(self, data: pd.DataFrame):
        option = self.option
        target_column = option['column']
        window = option['window']
        column = self.columns["EMA"]
        
        ema = indicaters.EMA(data[target_column], window)
        self.last_data = pd.DataFrame({column:ema}).iloc[-self.get_minimum_required_length():]
        return {column:ema}
    
    def update(self, tick:pd.Series):
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
            "MB": f"{key}_MB",
            "UB": f"{key}_UB",
            "LB": f"{key}_LB",
            "Width": f"{key}_Width"
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
    
    def run(self, data: pd.DataFrame):
        option = self.option
        target_column = option['column']
        window = option['window']
        alpha = option['alpha']
        
        ema, ub, lb, bwidth = indicaters.bolinger_from_ohlc(data, target_column, window=window, alpha=alpha)
        
        c_ema = self.columns['MB']
        c_ub = self.columns['UB']
        c_lb = self.columns['LB']
        c_width = self.columns['Width']
        
        self.last_data = pd.DataFrame({c_ema:ema, c_ub: ub, c_lb:lb, c_width:bwidth, target_column:data[target_column] }).iloc[-self.get_minimum_required_length():]
        return {c_ema:ema, c_ub: ub, c_lb:lb, c_width:bwidth}
    
    def update(self, tick:pd.Series):
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
        
        c_ema = self.columns['MB']
        c_ub = self.columns['UB']
        c_lb = self.columns['LB']
        c_width = self.columns['Width']
        
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
    
    available_columns = ["ATR"]
    
    def __init__(self, key='atr', window = 14, ohlc_column_name = ('Open', 'High', 'Low', 'Close'), is_input=True, is_output=True, option = None):
        super().__init__(key)
        self.option = {
            "ohlc_column": ohlc_column_name,
            "window": window
        }
        if option != None:
            self.option.update(option)

        self.columns = {'ATR': f'{key}_ATR'}
        self.is_input = is_input
        self.is_output = is_output
        
    @classmethod
    def load(self, key:str, params:dict):
        window = params["window"]
        columns = tuple(params["ohlc_column"])
        is_input = params["input"]
        is_out = params["output"]
        return ATRpreProcess(key, window, columns, is_input, is_out)

    def run(self, data: pd.DataFrame):
        option = self.option
        target_columns = option['ohlc_column']
        window = option['window']
        
        atr_series = indicaters.ATR_from_ohlc(data, target_columns, window=window)
        self.last_data = data.iloc[-self.get_minimum_required_length():].copy()
        last_atr = atr_series.iloc[-self.get_minimum_required_length():].values
        
        c_atr = self.columns['ATR']
        self.last_data[c_atr] = last_atr
        return {c_atr:atr_series.values}
        
    def update(self, tick:pd.Series):
        option = self.option
        target_columns = option['ohlc_column']
        window = option['window']
        c_atr = self.columns['ATR']
        
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
    available_columns = ["RSI", "AVG_GAIN", "AVG_LOSS"]
    
    def __init__(self, key='rsi', window = 14, ohlc_column_name = ('Open', 'High', 'Low', 'Close'), is_input=True, is_output=True, option = None):
        super().__init__(key)
        self.option = {
            "ohlc_column": ohlc_column_name,
            "window": window
        }

        if option != None:
            self.option.update(option)

        self.columns = {
            "RSI": f'{key}_RSI',
            "GAIN": f'{key}_AVG_GAIN',
            "LOSS": f'{key}_AVG_LOSS'
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

    def run(self, data: pd.DataFrame):
        option = self.option
        target_column = option['ohlc_column'][0]
        window = option['window']
        c_rsi = self.columns['RSI']
        c_gain = self.columns['GAIN']
        c_loss = self.columns['LOSS']
        
        atr_series = indicaters.RSI_from_ohlc(data, target_column, window=window)
        atr_series.columns = [c_gain, c_loss, c_rsi]
        self.last_data = data.iloc[-self.get_minimum_required_length():]
        self.last_data[c_rsi] = atr_series.iloc[-self.get_minimum_required_length():]
        return {"atr":atr_series[c_rsi].values}
        
    def update(self, tick:pd.Series):
        option = self.option
        target_column = option['ohlc_column'][0]
        window = option['window']
        c_rsi = self.columns['RSI']
        c_gain = self.columns['GAIN']
        c_loss = self.columns['LOSS']
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
    
    def __init__(self, key: str = "renko", date_column = "Timestamp", ohlc_column = ('Open', 'High', 'Low', 'Close'), window=10, is_input = True, is_output = True, option = None):
        super().__init__(key)
        self.option = {
            "date_column": date_column,
            "ohlc_column": ohlc_column,
            "window": window
        }
        if option != None:
            self.option.update(option)
        self.is_input = is_input
        self.is_output = is_output
        self.columns = {
            'NUM': f'{key}_block_num',
            "TREND": f'{key}_trend',
        }
        
    @classmethod
    def load(self, key:str, params:dict):
        window = params["window"]
        date_column = tuple(params["date_column"])
        columns = tuple(params["ohlc_column"])
        is_input = params["input"]
        is_out = params["output"]
        return RenkoProcess(key, date_column, columns, window, is_input, is_out)

    def run(self, data: pd.DataFrame):
        option = self.option
        date_column = option["date_column"]
        ohlc_column = option["ohlc_column"]
        window = option['window']
        num_column = "bar_num"
        trend_column = "uptrend"
        
        renko_block_num = self.columns["NUM"]
        renko_trend = self.columns["TREND"]
        
        renko_df = indicaters.renko_time_scale(data, date_column=date_column, ohlc_columns=ohlc_column, window=window)
        return {renko_block_num:renko_df[num_column].values, renko_trend: renko_df[trend_column].values}
        
    def update(self, tick:pd.Series):
        raise Exception("update is not implemented yet on renko process")
        
    def get_minimum_required_length(self):
        return self.option['window']
    
    def revert(self, data_set:tuple):
        #pass
        return False, None

class SlopeProcess(ProcessBase):
    
    kinds = "Slope"
    
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
            'Slope': f'{key}_slope'
        }
        
    @classmethod
    def load(self, key:str, params:dict):
        window = params["window"]
        column = tuple(params["target_column"])
        is_input = params["input"]
        is_out = params["output"]
        return SlopeProcess(key, target_column=column, window=window, is_input=is_input, is_output=is_out)

    def run(self, data: pd.DataFrame):
        option = self.option
        column = option["target_column"]
        window = option['window']
        idc_out_column = "slope"
        out_column = self.columns["Slope"]
        
        slope_df = indicaters.slope(data[column], window=window)
        slopes = slope_df[idc_out_column]
        return {out_column: slopes}
        
    def update(self, tick:pd.Series):
        raise Exception("update is not implemented yet on slope process")
        
    def get_minimum_required_length(self):
        return self.option['window']
    
    def revert(self, data_set:tuple):
        #pass
        return False, None

class CCIProcess(ProcessBase):
    
    kinds = "CCI"
    
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
            'CCI': f'{key}_cci'
        }

    @classmethod
    def load(self, key:str, params:dict):
        option = {
            "window": params["window"],
            "ohlc_column": params["ohlc_column"]
        }

        is_input = params["input"]
        is_out = params["output"]
        macd = CCIProcess(key, option=option, is_input=is_input, is_output=is_out)
        return macd
        
    def run(self, data:pd.DataFrame):
        self.data = data
        window = self.options["window"]
        ohlc_column = self.options["ohlc_column"]
        
        out_column = self.columns["CCI"]
        
        cci = indicaters.CommodityChannelIndex(data, window, ohlc_column)
        
        return {out_column: cci}
    
    def update(self, tick: pd.Series):
        if type(self.data) != type(None):
            out_column = self.columns["CCI"]
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

####
# Not implemented as I can't caliculate required length
####
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