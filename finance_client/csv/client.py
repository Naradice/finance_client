import numpy
import pandas as pd
import random, difflib, os, datetime, math
import finance_client.frames as Frame
from finance_client.client_base import Client
from finance_client.utils.csvrw import write_df_to_csv, read_csv

class CSVClient(Client):
    kinds = 'csv'
    available_slip_type = ["random", "none", "percent", "pct"]
    
    def __update_columns(self, columns:list):
        if self.ohlc_columns is None:
            self.ohlc_columns = {}
        temp_columns = {}
        for column in columns:
            column_ = str(column).lower()
            if column_ == 'open':
                temp_columns['Open'] = column
            elif column_ == 'high':
                temp_columns['High'] = column
            elif column_ == 'low':
                temp_columns['Low'] = column
            elif 'close' in column_:
                temp_columns['Close'] = column
            elif "time" in column_:#assume time, timestamp or datetime
                temp_columns["Time"] = column
        self.ohlc_columns.update(temp_columns)
    
    def __initialize_file_name_func(self, file_names:list):
        def has_strings(file_name:str, strs_list):
            if len(strs_list) > 0:
                for strs in strs_list:
                    if file_name.find(strs) ==-1:
                        return False
                return True
            return False
        
        indices = random.sample(range(0, len(file_names)), k=len(file_names))
        base_file_name = file_names[indices[0]]

        same_strs = []
        for index in indices[1:]:
            file_name = file_names[index]
            if has_strings(file_name ,same_strs):
                break
            differ = difflib.Differ()
            ans = differ.compare(base_file_name, file_name)
            same_strs = []
            same_str = ""

            for diff in ans:
                d = list(diff)
                if d[0] == '-' or d[0] == '+':
                    if same_str != "":
                        same_strs.append(same_str)
                        same_str = ""
                else:
                    same_str += d[2]
                    
            if same_str != "":
                same_strs.append(same_str)
        self.__get_symbol_from_filename = lambda file_name: file_name[:4]
        if len(same_strs) > 0:
            if len(same_strs) > 1:
                prefix = same_strs[0]
                suffix = same_strs[-1]
                self.__get_symbol_from_filename = lambda file_name: file_name.replace(prefix, "").replace(suffix, "")[:4]
            else:
                suffix = same_strs[-1]
                self.__get_symbol_from_filename = lambda file_name: file_name.replace(suffix, "")[:4]
    
    def __initialize_date_index(self, ascending:bool, start_date:datetime.datetime=None):
        self.data = self.data.sort_index(ascending=ascending)
        if ascending:
            delta = self.data.index[1] - self.data.index[0]
        else:
            delta = self.data.index[0] - self.data.index[1]
        self.frame = int(delta.total_seconds()/60)
        
        if delta < datetime.timedelta(days=1):
            self.data.index = pd.to_datetime(self.data.index, utc=True)
            
        if start_date is not None and type(start_date) is datetime.datetime:
            #self.data[self.data.index >= start_date.astimezone(datetime.timezone.utc)].index[0]
            start_date = start_date.astimezone(datetime.timezone.utc)
            is_date_found = False
            for index in range(0, len(self.data.index)):
                if self.data.index[index] >= start_date:
                    # date is retrievd by [:step_index], so we need to plus 1
                    self.__step_index = index + 1
                    is_date_found = True
                    break
            if is_date_found is False:
                self.logger.warning(f"start date {start_date} doesn't exit in the index")
                
    def __read_csv__(self, files, columns=[], date_col=None, skiprows=None, start_date=None, chunksize=None, ascending=True):
        DFS = {}
        kwargs = {}
        is_multi_mode = False
        if len(files) > 1:
            is_multi_mode = True
            
        usecols = columns
        if date_col is not None:
            # To load date column, don't specify columns if date is not specified at first
            if len(usecols) > 0:
                usecols = set(usecols)
                usecols.add(date_col)
                kwargs["usecols"] = usecols
            kwargs["parse_dates"] = [date_col]
            kwargs["index_col"] = date_col
        self.__is_chunk_mode = False
        if chunksize is not None:
            kwargs["chunksize"] = chunksize
            self.__is_chunk_mode = True
        if is_multi_mode and skiprows is not None:
            #assume index 0 is column
            kwargs["skiprows"] = range(1, skiprows+1)
            
        for file in files:
            symbol = self.__get_symbol_from_filename(file)
            try:
                df = pd.read_csv(file, header=0, **kwargs)
                self.symbols.append(symbol)
                DFS[symbol] = df
            except PermissionError as e:
                self.logger.error(f"file, {file}, is handled by other process {e}")
                raise e
            except Exception as e:
                self.logger.error(f"error occured on read csv {e}")
                raise e
        if self.__is_chunk_mode:
            #when chunksize is specified, TextReader is stored instead of DataFrame
            TRS = DFS.copy()
            self.__chunk_mode_params = {"TRS": TRS}
            DFS = {}
            for key, tr in TRS.items():
                df = tr.get_chunk()
                DFS[key] = df
        if is_multi_mode:
            self.data = pd.concat(DFS.values(), axis=1, keys=DFS.keys())
        else:
            self.data = df.copy()
        del df
        
        if skiprows is not None and is_multi_mode:
            if skiprows > 0:
                self.data = self.data.iloc[skiprows:].copy()
        if is_multi_mode:
            __columns = self.data[self.symbols[0]].columns
        else:
            __columns = self.data.columns
        self.__update_columns(__columns)
        is_date_index = False
        if date_col is not None:
            is_date_index = True
        elif "Time" in self.ohlc_columns:
            # make Time column to index
            time_column = self.ohlc_columns["Time"]
            if time_column in __columns:
                dfs = []
                DFS = {}
                if is_multi_mode:
                    dfs = [self.data[_symbol] for _symbol in self.symbols]
                else:
                    dfs = [self.data]
                for index in range(0, len(self.symbols)):
                    df = dfs[index].dropna()
                    symbol = self.symbols[index]
                    df.set_index(time_column, inplace=True)
                    if type(df.index) != pd.DatetimeIndex:
                        df.index = pd.to_datetime(df.index, utc=True)
                    df = df.sort_index(ascending=ascending)
                    if columns is not None and len(columns) > 0:
                        df = df[columns]
                    DFS[symbol] = df.copy()
                self.data = pd.concat(DFS.values(), axis=1, keys=DFS.keys())
                is_date_index = True
        if is_date_index:
            self.__initialize_date_index(ascending, start_date)
            if self.__is_chunk_mode and is_multi_mode:
                for key, df in DFS.items():
                    last_time = df.index[-1]
                    TIMES = {key: (df.index[0], last_time)}
                self.__chunk_mode_params["TIMES"] = TIMES
        else:
            print("Couldn't daterming date column")

    def __past_date(self, target_date, from_frame, to_frame):
        diff_date = target_date.day % datetime.timedelta(minutes=to_frame).days
        diff_date_as_minutes = diff_date*60*24
        diff_hours_as_minutes = target_date.hour*60 + target_date.minute
        total_diff_minutes = diff_date_as_minutes+diff_hours_as_minutes
        past_index = int(total_diff_minutes/from_frame)
        return past_index

    def __past_hours(self, target_date, from_frame, to_frame) :
        diff_hours = target_date.hour % (to_frame/60)
        diff_hours_as_minutes = diff_hours*60
        total_diff_minutes = diff_hours_as_minutes + target_date.minute
        past_index = int(total_diff_minutes/from_frame)
        return past_index

    def __past_minutes(self, target_date, from_frame, to_frame) :
        target_minute = target_date.minute
        diff_minutes = target_minute % to_frame
        past_index = int(diff_minutes/from_frame)
        return past_index
        
    def __rolling_frame(self, data:pd.DataFrame, from_frame:int, to_frame: int, addNan=False) -> pd.DataFrame:        
        if to_frame >= Frame.MO1:
            raise Exception("not implemented")
        elif to_frame >= Frame.D1:
            get_past_window = self.__past_date
        elif to_frame >= Frame.H1:
            get_past_window = self.__past_hours
        else:
            get_past_window = self.__past_minutes
            
        open_column = self.ohlc_columns["Open"]
        high_column = self.ohlc_columns["High"]
        low_column = self.ohlc_columns["Low"]
        close_column = self.ohlc_columns["Close"]
        
        pre_index = len(data)-1
        past_index = get_past_window(data[self.date_column].iloc[pre_index], from_frame, to_frame)
        window = int(to_frame/from_frame)
        window_ = window
        next_index = pre_index - past_index
        delta = datetime.timedelta(minutes=from_frame * past_index)
        Highs = []
        Lows = []
        Opens = []
        Closes = []
        Timestamp = []
        
        expected_date = data[self.date_column].iloc[pre_index] - delta
        delta = datetime.timedelta(minutes=to_frame)
        
        while next_index >= 0:
            next_date = data[self.date_column].iloc[next_index]
            window_ = window
            while next_date != expected_date:
                if  next_date > expected_date:
                    self.logger.warning(f"{pre_index} to {next_index} has insufficient data. rolling data anyway.")
                    #when next tick doen't start with expected_date, reduce missing count from window
                    NEXT_INDEX_PAD = -1
                    temp_next_date = data[self.date_column].iloc[next_index + NEXT_INDEX_PAD]
                    next_delta = expected_date - temp_next_date
                    exclude = int((next_delta.total_seconds())/60/from_frame)-1
                    if window > exclude:
                        window_ = window - exclude
                    else:#else case the next date has no data
                        if not addNan:
                            past_index = get_past_window(temp_next_date, from_frame, to_frame)
                            window_ = past_index - NEXT_INDEX_PAD
                            expected_date = temp_next_date - datetime.timedelta(minutes=from_frame*past_index)
                    break
                else:
                    # datetime over as there is lack of data
                    next_index = next_index + 1
                    next_date = data[self.date_column].iloc[next_index]
                    
            if pre_index == next_index and addNan:
                self.logger.info(f"{pre_index} has no data. Put NaN on this datetime")
                expected_date = expected_date - delta
                next_index = pre_index - window_
                Highs.append(numpy.Nan)
                Lows.append(numpy.Nan)
                Opens.append(numpy.Nan)
                Closes.append(numpy.Nan)
                Timestamp.append(expected_date)
            else:
                Highs.append(data[high_column].iloc[next_index:pre_index].max())
                Lows.append(data[high_column].iloc[next_index:pre_index].min())
                Opens.append(data[open_column].iloc[next_index])
                Closes.append(data[close_column].iloc[pre_index-1])
                Timestamp.append(data[self.date_column].iloc[next_index])

                expected_date = expected_date - delta
                pre_index = next_index
                next_index = pre_index - window_

        rolled_data = pd.DataFrame({self.date_column:Timestamp, high_column: Highs, low_column:Lows, open_column:Opens, close_column:Closes})
        return rolled_data
    
    def get_additional_params(self):
        args = {
            "auto_step_index":self.auto_step_index, "file":self.files[self.frame]
        }        
        args.update(self.__args)
        return args

    def __init__(self, files:list = None, columns = [], date_column = None, 
                 file_name_generator=None, out_frame:int=None,
                 start_index = 0, start_date = None, start_random_index=False, auto_step_index=True, skiprows=None, auto_reset_index=False,
                 slip_type="random", chunksize=None, budget=1000000, 
                 do_render=False, seed=1017,logger=None):
        """CSV Client for bitcoin, etc. currently bitcoin in available only.
        Need to change codes to use settings file
        
        Args:
            files (list<str>, optional): You can directly specify the file names. Defaults to None.
            columns (list, optional): column names to read from CSV files. Defaults to [].
            date_column (str, optional): If specified, try to parse time columns. Otherwise search time column. Defaults to None
            file_name_generator (function, optional): function to create file name from symbol. Ex) lambda symbol: f'D:\\warehouse\\stock_D1_{symbol}.csv'
            out_frame (int, optional): output frame. Ex) Convert 5MIN data to 30MIN by out_frame=30. Defaults to None.
            start_index (int, optional): specify minimum index. If not specified, start from 0. Defauls to None.
            start_date (datetime, optional): specify start date. start_date overwrite the start_index. If not specified, start from index=0. Defaults to None.
            start_random_index (bool, optional): After init or reset_index, random index is used as initial index. Defaults to False.
            auto_step_index (bool, optional): If true, get_rate function returns data with advancing the index. Otherwise data index is advanced only when get_next_tick is called
            skiprows (int, optional): specify number to skip row of csv. For multisymbol, row is skipped after merge. Defaults None, not skipped.
            auto_reset_index ( bool, optional): refreh the index when index reach the end. Defaults False
            slip_type (str, optional): Specify how ask and bid slipped. random: random value from Close[i] to High[i] and Low[i]. prcent or pct: slip_rate=0.1 is applied. none: no slip.
            do_render (bool, optional): If true, plot OHLC and supported indicaters. 
            seed (int, optional): specify random seed. Defaults to 1017
            chunksize (int, optional): To load huge file partially, you can specify chunk size. Defaults to None.
        """
        super().__init__(budget=budget,do_render=do_render, out_ohlc_columns=columns, provider="csv", logger_name=__name__, logger=logger)
        random.seed(seed)
        self.auto_step_index = auto_step_index
        slip_type = slip_type.lower()
        if slip_type in self.available_slip_type:
            if slip_type == "percent" or slip_type == "pct":
                slip_rate = 0.1
                self.__get_current_bid = lambda open_value, low_value: open_value - (open_value - low_value) * slip_rate
                self.__get_current_ask = lambda open_value, high_value: open_value + (high_value - open_value) * slip_rate
            elif slip_type == "random":
                self.__get_current_bid = lambda open_value, low_value: random.uniform(low_value, open_value)
                self.__get_current_ask = lambda open_value, high_value: random.uniform(open_value, high_value)
            elif slip_type == "none" or slip_rate is None:
                self.__get_current_bid = lambda open_value, low_value: open_value
                self.__get_current_ask = lambda open_value, low_value: open_value
        else:
            self.logger.warn(f"{slip_type} is not in availble values: {self.available_slip_type}")
            self.logger.info(f"use random as slip_type")
            self.__get_current_bid = lambda open_value, low_value: random.uniform(low_value, open_value)
            self.__get_current_ask = lambda open_value, high_value: random.uniform(open_value, high_value)
        # store args so that we can reproduce CSV client by loading args file
        self.__args = {"out_frame": out_frame, "columns": columns, "date_column": date_column, "start_index": start_index, "start_date":start_date,
                       "start_random_index":start_random_index, "auto_step_index": auto_step_index, "auto_reset_index":auto_reset_index, "skiprows":skiprows,
                       "slip_type":slip_type, "chunksize": chunksize,"seed": seed}
        self.ask_positions = {}
        self.base_point = 0.01
        self.frame = None
        self.symbols = []
        if start_index:
            self.__step_index = start_index
        elif start_random_index:
            #assign initial value later
            self.__step_index = 0
        else:
            start_index = 0
            self.__step_index = start_index

        
        if files is not None:
            if type(files) == str:
                files = [os.path.abspath(files)]
            else:
                if type(files) != list:
                    try:
                        files = list(files)
                    except Exception as e:
                        raise Exception(f"files is specified, but can't be casted to list: {e}")
                self.files = set(files)
                files = list(self.files)
                files = [os.path.abspath(file) for file in files]
            self.__initialize_file_name_func(files)
            self.__read_csv__(files, columns, date_column, skiprows, start_date, chunksize)
            if out_frame != None:
                to_frame = int(out_frame)
                if self.frame < to_frame:
                    rolled_data = self.__rolling_frame(self.data.copy(), from_frame=self.frame, to_frame=to_frame)
                    if type(rolled_data) is pd.DataFrame and len(rolled_data) > 0:
                        self.data = rolled_data
                        self.frame = to_frame
                elif to_frame == self.frame:
                    self.logger.info("file_frame and frame are same value. row file is used.")
                else:
                    raise Exception("frame should be greater than file_frame as we can't decrease the frame.")
                self.out_frames = to_frame
                
        self.__auto_reset = auto_reset_index
        if start_random_index and self.data is not None:
            self.__step_index = random.randint(0, len(self.data))
    
    #overwrite if needed
    def update_rates(self) -> bool:
        """ you can define a function to increment length of self.data 
    
        Returns:
            bool: succeeded to update. Length should be increased 1 at least
        """
        return False
    
    def __update_chunkdata_with_time(self, chunk_size:int, symbols:list=[], interval:int=None):
        min_last_time = datetime.datetime.utcnow()
        TIMES = self.__chunk_mode_params["TIMES"]
        for symbol in symbols:
            if symbol not in TIMES:
                # if file_name_generator is defined, initialize it
                # else case, check symbol is a part of any column name
                # and else, show warning
                print(f"{symbol} is not initialized")
                pass
            first_time, last_time = TIMES[symbol]
            if min_last_time > last_time:
                min_last_time = last_time
                min_time_symbol = symbol
        
        if len(self.data[min_time_symbol]) - self.__step_index < interval:
            DFS = {}
            TRS = self.__chunk_mode_params["TRS"]
            TIMES = self.__chunk_mode_params["TIMES"]
            temp_df = self.data[min_time_symbol].dropna()
            short_length = interval - len(temp_df) + self.__step_index
            short_chunk_count = math.ceil(short_length/chunk_size)
            tr = TRS[min_time_symbol]
            temp_dfs = [temp_df]
            for count in range(0, short_chunk_count):
                temp_df = tr.get_chunk()##may happen error
                temp_dfs.append(temp_df)
            temp_df = pd.concat(temp_dfs, axis=0)
            required_last_time = temp_df.index[self.__step_index + interval -1]
            temp_df = temp_df.dropna()
            DFS[min_time_symbol] = temp_df
            TIMES[min_time_symbol] = (temp_df.index[0], temp_df.index[-1])
            min_last_time = temp_df.index[-1]
            
            remaining_symbols = set(TRS.keys()) - {min_time_symbol}
            for symbol in remaining_symbols:
                date_set = TIMES[symbol]
                symbol_last_date = date_set[1]
                tr = TRS[symbol]
                temp_dfs = [self.data[symbol].dropna()]
                while symbol_last_date < required_last_time:
                    temp_df = tr.get_chunk()
                    temp_dfs.append(temp_df)
                    symbol_last_date = temp_df.index[-1]
                if len(temp_df) > 1:
                    temp_df = pd.concat(temp_dfs, axis=0)
                    temp_df = temp_df.dropna()
                    DFS[symbol] = temp_df
                    TIMES[symbol] = (temp_df.index[0], temp_df.index[-1])
                    if min_last_time > temp_df.index[-1]:
                        min_last_time = temp_df.index[-1]
                        min_time_symbol = symbol
                else:
                    DFS[symbol] = self.data[min_time_symbol].dropna()
            self.data = pd.concat(DFS.values(), axis=1, keys=DFS.keys())
    
    def __update_chunkdata(self, chunk_size:int, symbols:list=[], interval:int=None):
        DFS = {}
        TRS = self.__chunk_mode_params["TRS"]
        for symbol in symbols:
            temp_df = self.data[symbol].dropna()
            if len(temp_df) - self.__step_index < interval:
                short_length = interval - len(temp_df) + self.__step_index
                short_chunk_count = math.ceil(short_length/chunk_size)
                tr = TRS[symbol]
                temp_dfs = [temp_df]
                for count in range(0, short_chunk_count):
                    temp_df = tr.get_chunk()##may happen error
                    temp_dfs.append(temp_df)
                temp_df = pd.concat(temp_dfs, axis=0)
                DFS[symbol] = temp_df
            else:
                DFS[symbol] = temp_df
        if len(DFS) > 0:
            dfs = pd.concat(DFS.values(), axis=1, keys=DFS.keys())
            all_columns = set(self.data.columns)
            missing_columns = all_columns - set(symbols)
            if len(missing_columns) > 0:
                missing_data_df = self.data[list(missing_columns)]
                dfs = pd.concat([dfs, missing_data_df])
            self.data = dfs
            
    def __get_rates_by_chunk(self, symbols:list=[], interval:int=None, frame:int=None):
        if interval is not None:
            chunk_size = self.__args["chunksize"]
            if "TIMES" in self.__chunk_mode_params:
                self.__update_chunkdata_with_time(chunk_size, symbols, interval)
            else:
                self.__update_chunkdata(chunk_size, symbols, interval)
        else:
            target_symbols = list(set(self.data.columns) & set(symbols))
            # todo: update missing columns
            rates = self.data.copy()
            if self.auto_step_index:
                self.__step_index += 1
            return rates
        target_symbols = list(set(self.data.columns) & set(symbols))
        symbols_dfs = self.data[target_symbols]
        is_ascending = self.__args["ascending"]
        if is_ascending:
            rates = symbols_dfs.iloc[self.__step_index - interval:self.__step_index]
        else:
            rates = symbols_dfs.iloc[len(symbols_dfs) - interval - self.__step_index - interval: len(symbols_dfs) -self.__step_index]
        if self.auto_step_index:
            self.__step_index += 1
        return rates
            
    
    def __get_rates(self, length:int=None, symbols:list=[], frame:int=None):
        if length is None:
            self.update_rates()
            rates = self.data.copy()
            if self.auto_step_index:
                self.__step_index += 1
            return rates

        elif length >= 1:
            rates = None
            if self.__step_index >= length-1:
                try:
                    #return data which have length length
                    rates = self.data.iloc[self.__step_index - length:self.__step_index]
                    return rates
                except Exception as e:
                    self.logger.error(e)
            else:
                if self.update_rates():
                    self.__get_rates(length, symbols, frame)
                else:
                    if self.__auto_reset:
                        self.__step_index = random.randint(0, len(self.data))
                        ## todo: raise index change event
                        return self.__get_rates(length, symbols, frame)
                    else:
                        self.logger.warning(f"current step index {self.__step_index} is less than length {length}. return length from index 0. Please assgin start_index.")
                        return self.data.iloc[:length]
        else:
            raise Exception("interval should be greater than 0.")
           
    def get_ohlc_from_client(self, length:int=None, symbols:list=[], frame:int=None):
        if self.__is_chunk_mode:
            return self.__get_rates_by_chunk(length, symbols, frame)
        else:
            return self.__get_rates(length, symbols, frame)

    def get_future_rates(self,length=1, back_length=0):
        if length > 1:
            rates = None
            if self.__step_index >= length-1:
                try:
                    #return data which have length length
                    rates = self.data.iloc[self.__step_index - back_length:self.__step_index+length+1].copy()
                    return rates
                except Exception as e:
                    self.logger.error(e)
            else:
                if self.__auto_reset:
                    self.__step_index = random.randint(0, len(self.data))
                    ## raise index change event
                    return self.get_future_rates(length)
                else:
                    self.logger.warning(f"not more data on index {self.__step_index}")
                return pd.DataFrame()
        else:
            raise Exception(f"length should be greater than 0. {length} is provided.")
    
    def get_current_ask(self, symbols=None):
        tick = self.data.iloc[self.__step_index-1]
        open_column = self.ohlc_columns["Open"]
        high_column = self.ohlc_columns["High"]
        if type(tick.index) is pd.MultiIndex:
            if type(symbols) is str and symbols in self.symbols:
                open_value = tick[symbols][open_column]
                high_value = tick[symbols][open_column]
            elif type(symbols) is list:
                available_symbols = set(self.symbols) & set(symbols)
                available_symbols = list(available_symbols)
                open_value = tick[[(__symbol, open_column) for __symbol in available_symbols]]
                open_value.index = available_symbols
                high_value = tick[[(__symbol, high_column) for __symbol in available_symbols]]
                high_value.index = available_symbols
            else:
                open_value = tick[[(__symbol, open_column) for __symbol in self.symbols]]
                open_value.index = self.symbols
                high_value = tick[[(__symbol, high_column) for __symbol in self.symbols]]
                high_value.index = self.symbols
        else:
            open_value = tick[open_column]
            high_value = tick[open_column]
        return self.__get_current_ask(open_value, high_value)

    def get_current_bid(self, symbols=None):
        tick = self.data.iloc[self.__step_index-1]
        open_column = self.ohlc_columns["Open"]
        low_column = self.ohlc_columns["Low"]
        
        if type(tick.index) is pd.MultiIndex:
            if type(symbols) is str and symbols in self.symbols:
                open_value = tick[symbols][open_column]
                low_value = tick[symbols][open_column]
            elif type(symbols) is list:
                available_symbols = set(self.symbols) & set(symbols)
                available_symbols = list(available_symbols)
                open_value = tick[[(__symbol, open_column) for __symbol in available_symbols]]
                open_value.index = available_symbols
                low_value = tick[[(__symbol, low_column) for __symbol in available_symbols]]
                low_value.index = available_symbols
            else:
                open_value = tick[[(__symbol, open_column) for __symbol in self.symbols]]
                open_value.index = self.symbols
                low_value = tick[[(__symbol, low_column) for __symbol in self.symbols]]
                low_value.index = self.symbols
        else:
            open_value = tick[open_column]
            low_value = tick[open_column]
        
        return self.__get_current_bid(open_value, low_value)
        
    def reset(self, mode:str = None, retry=0)-> bool:
        self.ask_positions = {}#ignore if there is position
        self.__step_index = random.randint(0, len(self.data))
        if mode != None:##start time mode: hour day, week, etc
            if retry <= 10:
                if mode == "day":
                    current_date = self.data[self.date_column].iloc[self.__step_index]
                    day_minutes = 60*24
                    total_minutes = current_date.minute + current_date.hour*60
                    add_minutes = day_minutes - total_minutes
                    add_index = add_minutes/self.frame
                    if self.__step_index + add_index >= len(self.data):
                        self.reset(mode, retry+1)
                    else:
                        candidate_index = self.__step_index + add_index
                        current_date = self.data[self.date_column].iloc[candidate_index]
                        additional_index = (current_date.hour*60 + current_date.minute)/self.frame
                        if additional_index != 0:
                            candidate_index = candidate_index - additional_index
                            current_date = self.data[self.date_column].iloc[candidate_index]
                            total_minutes = current_date.minute + current_date.hour*60
                            if total_minutes != 0:#otherwise, this day may have 1day data
                                self.reset(mode, retry+1)
            else:
                self.logger.warning("Data client reset with {mode} mode retried over 10. index was not correctly reset.")
                return False
        ## raise index change event
        return True
                                

    def get_holding_steps(self, position="ask"):
        steps_diff = []
        for key, ask_position in self.ask_positions.items():
            steps_diff.append(self.__step_index - ask_position["step"])
        return steps_diff

    ## TODO: need to check if timedelta is equal to frame
    def get_next_tick(self):
        if self.__step_index < len(self.data)-2:
            self.__step_index += 1
            tick = self.data.iloc[self.__step_index]
            ## raise index change event
            return tick, False
        else:
            if self.__auto_reset:
                self.__step_index = random.randint(0, len(self.data))
                ## raise index change event
                tick = self.data.iloc[self.__step_index]
                return tick, True
            else:
                self.logger.warning(f"not more data on index {self.__step_index}")
            return pd.DataFrame(), True
        
    @property
    def max(self):
        return self.__high_max
        
    @property
    def min(self):
        return self.__low_min
        
    def __getitem__(self, ndx):
        return self.data.iloc[ndx]
        
    def get_current_index(self):
        return self.__step_index
    
    def get_params(self) -> dict:
        self.__args["files"] = self.files
        
        return self.__args
    
    def __del__(self):
        if self.__is_chunk_mode:
            TRS = self.__chunk_mode_params["TRS"]
            for key, value in TRS.items():
                value.close()