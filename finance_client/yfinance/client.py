import datetime, os
from time import sleep
import pandas as pd
from finance_client.csv.client import CSVClient
import finance_client.frames as Frame
import yfinance as yf

class YahooClient(CSVClient):
    
    OHLC_COLUMNS = ["Open", "High", "Low", "Close"]
    VOLUME_COLUMN = ["Volume"]
    TIME_INDEX_NAME = "Datetime"
    
    kinds = "yfinance"
    
    ## max period to call with interval. Confirmed on 2022-09
    max_periods = {
        '1m':datetime.timedelta(days=6),
        '2m':datetime.timedelta(days=59),
        '5m':datetime.timedelta(days=59),
        '15m':datetime.timedelta(days=59),
        '30m':datetime.timedelta(days=59),
        '60m':datetime.timedelta(days=721),
        '90m':datetime.timedelta(days=59)
    }
    
    max_range = {
        '1m': datetime.timedelta(days=29),
        '5m': datetime.timedelta(days=59)
    }    
    available_frames = [
        Frame.MIN1, Frame.MIN5, Frame.MIN15, Frame.MIN30, Frame.H1, Frame.D1, Frame.W1, Frame.MO1
    ]
    
    def frame_to_str(self, frame:int) -> str:
        availables = {
            Frame.MIN1:'1m', Frame.MIN5:'5m', Frame.MIN15:'15m', Frame.MIN30:'30m',
            Frame.H1: 'h1', Frame.D1:'1d', Frame.W1:'1wk', Frame.MO1: '1mo'
        }
        if frame in availables:
            return availables[frame]
    
    def create_filename(self, symbol:str):
        file_name = f"yfinance_{symbol}_{Frame.to_str(self.frame)}.csv"
        return os.path.join(self.data_folder, file_name)
    
    def get_additional_params(self):
        return {}
    
    def __init__(self, symbol, auto_step_index=False, frame: int = Frame.MIN5, start_index=None, seed=1017, slip_type="random", do_render=False, idc_processes=[], post_process=[], budget=1000000, logger=None):
        """Get ohlc rate from yfinance
        Args:
            symbol (str|list): stock symbol.
            auto_step_index (bool, optional): increase step when get_rates is called. Defaults to False.
            frame (int, optional): Frame of ohlc rates. Defaults to Frame.M5.
            post_process (list, optional): process to add indicater for output when get_rate_with_indicater is called. Defaults to [].
            budget (int, optional): budget for the simulation. Defaults to 1000000.
            logger (logger, optional): you can pass your logger if needed. Defaults to None.
            seed (int, optional): random seed. Defaults to 1017.

        Raises:
            ValueError: other than 1, 5, 15, 30, 60, 60*24, 60*24*7, 60*24*7*30 is specified as frame
            ValueError: length of symbol(tuple) isn't 2 when target is FX or CRYPT_CURRENCY
            Exception: length of symol(tuple) is other than 1 when target is 
            ValueError: symbol(str) is not from_symvol/to_symbol foramt when target is FX
            TypeError: symbol is neither tuple nor str
        """
        if frame not in self.available_frames:
            raise ValueError(f"{frame} is not supported")
        self.frame = frame
        self.__frame_delta = datetime.timedelta(minutes=frame)
        self.debug = False
        
        if type(symbol) == list:
            self.logger.warn(f"multi symbols is not supported on get_rates etc for now.")
            self.symbols = symbol
        elif type(symbol) == str:
            self.symbols = [symbol]
        else:
            raise TypeError("symbol must be str or list.")
        
        self.data_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), f"../data_source/yfinance/"))
        if os.path.exists(self.data_folder) == False:
            os.makedirs(self.data_folder)
        #TODO: initialize logger 
        self.__get_all_rates()
        file_path = self.create_filename(self.symbols[0])
        #TODO: support multi symbols on CSV Client
        super().__init__(auto_step_index=auto_step_index, file=file_path, frame=frame, provider="yfinance", out_frame=None, columns=self.OHLC_COLUMNS, date_column=self.TIME_INDEX_NAME, start_index=start_index, do_render=do_render, seed=seed, slip_type=slip_type, idc_processes=idc_processes, post_process=post_process, budget=budget, logger=logger)

    def __get_all_rates(self):
        if len(self.symbols) > 0:
            interval = self.frame_to_str(self.frame)
            DFS = {}
            existing_last_date = datetime.datetime.min.date()
            for symbol in self.symbols:
                file_path = self.create_filename(symbol)
                existing_rate_df = None
                if os.path.exists(file_path):
                    existing_rate_df = pd.read_csv(file_path, parse_dates=[self.TIME_INDEX_NAME], index_col=self.TIME_INDEX_NAME)
                    if self.frame < Frame.D1:
                        existing_rate_df.index = existing_rate_df.index.tz_convert("UTC")
                    existing_rate_df = existing_rate_df.sort_index()
                    # get last date of existing data
                    existing_last_date = existing_rate_df.index[-1].date()
                        
                end = datetime.datetime.utcnow().date()
                delta = None
                kwargs = {
                    'group_by': 'ticker'
                }
                isDataRemaining = False
                if interval in self.max_periods:
                    delta = self.max_periods[interval]
                    start = end - delta
                    kwargs["end"] = end
                    kwargs["start"] = start
                    isDataRemaining = True
                
                mrange_delta = None
                mrange = datetime.datetime.min.date()
                if interval in self.max_range:
                    mrange_delta = self.max_range[interval]
                    mrange = end - mrange_delta
                # compare today - max range with last date of exsisting date to decide range                
                if existing_last_date > mrange:
                    mrange = existing_last_date
                    start = mrange
                    kwargs["start"] = start
                # compare start and range. If today- range > start, change start to range then change flag true
                if "start" in kwargs:
                    if mrange > start:
                        start = mrange
                        kwargs["start"] = start
                        isDataRemaining = False
                    
                    print(f"from {start} to {end} of {symbol}")
                df = yf.download(symbol, interval=interval, **kwargs)
                ticks_df = df.copy()
                if self.frame < Frame.D1:
                    ticks_df.index = ticks_df.index.tz_convert('UTC')
                if len(df) > 0:
                    while isDataRemaining:
                        end = end - delta
                        start = end - delta
                        if start < mrange:
                            start = mrange
                            isDataRemaining = False
                            
                        print(f"from {start} to {end} of {symbol}")
                        sleep(1)#to avoid a load
                        df = yf.download(symbol, interval=interval, group_by='ticker', start=start, end=end)
                        if len(df) != 0:                                        
                            ticks_df = pd.concat([df, ticks_df])
                            ticks_df = ticks_df[~ticks_df.index.duplicated(keep="first")]

                    if self.frame < Frame.D1:
                        if 'tz_convert' in dir(df.index):
                            df.index = df.index.tz_convert('UTC')
                        else:
                            try:
                                df.index = pd.DatetimeIndex(df.index)
                                df.index = df.index.tz_convert('UTC')
                            except Exception as e:
                                print(f'failed tz_convert of index: ({type(df.index)})')
                    ticks_df = ticks_df.sort_index()
                
                if len(ticks_df) > 0:
                    if existing_rate_df is not None:
                        ticks_df = pd.concat([existing_rate_df, ticks_df])
                        ticks_df = ticks_df[~ticks_df.index.duplicated(keep="first")]
                        ticks_df = ticks_df.sort_index()
                ticks_df.to_csv(file_path, index_label=self.TIME_INDEX_NAME)
                DFS[symbol] = ticks_df
                
            if len(DFS) > 1:
                df = pd.concat(DFS.values(), axis=1, keys=DFS.keys())
            else:
                df = ticks_df
            return df
        return pd.DataFrame()
    
    def update_rates(self):
        current_time = datetime.datetime.now()
        delta = current_time - self.__updated_time
        if delta > self.__frame_delta:
            last_date = self.data.index[-1]
            new_data_df = self.__get_all_rates()
            #new_data_df[self.TIME_INDEX_NAME] = new_data_df.index
            #new_data_df = new_data_df.reset_index()
            new_last_date = new_data_df.index[-1]
            if last_date != new_last_date:
                self.data = new_data_df
                return True
            else:
                return False
        else:
            False
    
    def cancel_order(self, order):
        pass