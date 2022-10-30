import numpy
import pandas as pd
import random, difflib, os, datetime, math
import finance_client.frames as Frame
from finance_client.client_base import Client
from finance_client.utils.csvrw import write_df_to_csv, read_csv

class CSVClient(Client):
    kinds = 'csv'
    available_slip_type = ["random", "none", "percentage"]
    
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
            
    def __read_csv__(self, files, columns=None, date_col=None, start_index=None, start_date=None, chunksize=None, ascending=True):
        DFS = {}
        kwargs = {}
        is_multi_mode = False
        if len(files) > 1:
            is_multi_mode = True
            
        usecols = columns
        if date_col is not None:
            if usecols is not None:
                usecols = set(usecols)
                usecols.add(date_col)
                kwargs["usecols"] = usecols
            kwargs["parse_dates"] = [date_col]
            kwargs["index_col"] = date_col
        self.__chunk_mode = False
        if chunksize is not None:
            kwargs["chunksize"] = chunksize
            self.__is_chunk_mode = True
        if is_multi_mode and start_index is not None:
            #assume index 0 is column
            kwargs["skiprows"] = range(1, start_index+1)
            
        for file in files:
            symbol = self.__get_symbol_from_filename(file)
            df = pd.read_csv(file, header=0, **kwargs)
            DFS[symbol] = df.copy()
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
        
        if start_index is not None:
            self.data = self.data.iloc[start_index:].copy()
            
        self.__update_columns(self.data.columns)
        is_date_index = False
        if date_col is not None:
            is_date_index = True
            self.data = self.data.sort_index(ascending=ascending)
            if ascending:
                delta = self.data.index[1] - self.data.index[0]
            else:
                delta = self.data.index[0] - self.data.index[1]
            self.frame = int(delta.total_seconds()/60)
            if start_date is not None and type(start_date) is datetime:
                self.data = self.data[self.data.index >= start_date]
            
            if delta < datetime.timedelta(days=1):
                self.data.index = pd.to_datetime(self.data.index, utc=True)
        elif "Time" in self.ohlc_columns:
            time_column = self.ohlc_columns["Time"]
            if time_column in self.data.columns:
                self.data.set_index(time_column, inplace=True)
                self.data.index = pd.to_datetime(self.data.index, utc=True)
                self.data = self.data.sort_index(ascending=ascending)
                is_date_index = True
        if is_date_index:
            if self.__is_chunk_mode and is_multi_mode:
                min_last_time = datetime.datetime.utcnow()
                min_time_symbol = ""
                for key, df in DFS.items():
                    last_time = df.index[-1]
                    TIMES = {key: (df.index[0], last_time)}
                    if min_last_time > last_time:
                        min_last_time = last_time
                        min_time_symbol = key
                self.__chunk_mode_params["TIMES"] = TIMES
                self.__chunk_mode_params["min_last_time"] = (min_time_symbol, min_last_time)
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

    def __init__(self, files:list = None, columns = ['Open', 'High', 'Low', 'Close'], date_column = "Timestamp", 
                 file_name_func=None, out_frame:int=None,
                 start_index = 0, start_date = None, start_random_index=False, auto_step_index=False, auto_reset_index=False, slip_type="random", 
                 idc_processes = [], pre_process = [], ascending=True, chunksize=None, budget=1000000, 
                 do_render=False, seed=1017,logger=None):
        """CSV Client for bitcoin, etc. currently bitcoin in available only.
        Need to change codes to use settings file
        
        Args:
            files (list<str>, optional): You can directly specify the file names. Defaults to None.
            file_name_func (function, optional): function to create file name from symbol. Ex) lambda symbol: f'D:\\warehouse\\stock_D1_{symbol}.csv'
            out_frame (int, optional): output frame. Ex) Convert 5MIN data to 30MIN by out_frame=30. Defaults to None.
            columns (list, optional): column names to read from CSV files. Defaults to ['Open', 'High', 'Low', 'Close'].
            date_column (str, optional): If specified, try to parse time columns. Otherwise search time column. Defaults to Timestamp
            start_index (int, optional): specify minimum index. If not specified, start from 0. Defauls to None.
            start_date (datetime, optional): specify start date. start_date overwrite the start_index. If not specified, start from index=0. Defaults to None.
            start_random_index (bool, optional): After init or reset_index, random index is used as initial index. Defaults to False.
            auto_step_index (bool, optional): If true, get_rate function returns data with advancing the index. Otherwise data index is advanced only when get_next_tick is called
            auto_reset_index ( bool, optional): refreh the index when index reach the end. Defaults False
            slip_type (str, optional): Specify how ask and bid slipped. random: random value from Close[i] to High[i] and Low[i]. percentage: slip_rate=0.1 is applied. none: no slip.
            do_render (bool, optional): If true, plot OHLC and supported indicaters. 
            seed (int, optional): specify random seed. Defaults to 1017
            ascending (bool, optional): You can control order of get_rates result
            chunksize (int, optional): To load huge file partially, you can specify chunk size. Defaults to None.
            idc_processes (Process, optional) : list of indicater process. Dafaults to []
            pre_processes (Process, optional) : list of pre process. Defaults to []
        """
        super().__init__(budget=budget,do_render=do_render, out_ohlc_columns=columns, provider="csv", logger_name=__name__, logger=logger)
        self.auto_step_index = auto_step_index
        slip_type = slip_type.lower()
        if slip_type in self.available_slip_type:
            self.slip_type = slip_type
            if slip_type == "percentage":
                self.slip_rate = 0.1
        else:
            self.logger.warn(f"{slip_type} is not in availble values: {self.available_slip_type}")
            self.logger.info(f"use random as slip_type")
            self.slip_type = "random"
        random.seed(seed)
        # store args so that we can reproduce CSV client by loading args file
        self.__args = {"out_frame": out_frame, "olumns": columns, "date_column": date_column, "start_index": start_index, "start_date":start_date,
                       "start_random_index":start_random_index, "auto_step_index": auto_step_index, "auto_reset_index":auto_reset_index, "slip_type":slip_type,
                       "ascending":ascending, "chunksize": chunksize,"seed": seed}
        self.ask_positions = {}
        self.base_point = 0.01
        self.frame = None
        
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
            self.__read_csv__(files, columns, date_column, start_index, start_date, chunksize, ascending)
            start_index = 0
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
                
        if start_index:
            self.__step_index = start_index
        else:
            self.__step_index = random.randint(0, len(self.data))
        self.__auto_reset = auto_reset_index
        high_column = self.ohlc_columns["High"]
        low_column = self.ohlc_columns["Low"]
        self.__high_max = self.get_min_max(column = high_column)[1]
        self.__low_min = self.get_min_max(column = low_column)[0]
    
    #overwrite if needed
    def update_rates(self) -> bool:
        return False
    
    def __get_rates_by_chunk(self, symbols:list=[], interval:int=None, frame:int=None):
        DFS = {}
        chunk_size = self.__args["chunksize"]
        if len(dfs[min_time_code]) - self.__step_index < interval:
            temp_df = dfs[min_time_code].dropna()
            short_length = interval - len(temp_df) + self.__step_index
            short_chunk_count = math.ceil(short_length/chunk_size)
            tr = TRS[min_time_code]
            temp_dfs = [temp_df]
            for count in range(0, short_chunk_count):
                temp_df = tr.get_chunk()##may happen error
                temp_dfs.append(temp_df)
            temp_df = pd.concat(temp_dfs, axis=0)
            required_last_time = temp_df.index[self.__step_index + interval -1]
            temp_df = temp_df.dropna()
            DFS[min_time_code] = temp_df
            TIMES[min_time_code] = (temp_df.index[0], temp_df.index[-1])
            min_last_time = temp_df.index[-1]
            
            remaining_codes = set(TRS.keys()) - {min_time_code}
            for code in remaining_codes:
                date_set = TIMES[code]
                code_last_date = date_set[1]
                tr = TRS[code]
                temp_dfs = [dfs[code].dropna()]
                while code_last_date < required_last_time:
                    temp_df = tr.get_chunk()
                    temp_dfs.append(temp_df)
                    code_last_date = temp_df.index[-1]
                if len(temp_df) > 1:
                    temp_df = pd.concat(temp_dfs, axis=0)
                    temp_df = temp_df.dropna()
                    DFS[code] = temp_df
                    TIMES[code] = (temp_df.index[0], temp_df.index[-1])
                    if min_last_time > temp_df.index[-1]:
                        min_last_time = temp_df.index[-1]
                        min_time_code = code
                else:
                    DFS[code] = dfs[min_time_code].dropna()
            dfs = pd.concat(DFS.values(), axis=1, keys=DFS.keys())
    
    def __get_rates(self, symbols:list=[], interval:int=None, frame:int=None):
        if interval is None:
            self.update_rates()
            rates = self.data.copy()
            return rates

        elif interval >= 1:
            rates = None
            if self.__step_index >= interval-1:
                try:
                    #return data which have interval length
                    rates = self.data.iloc[self.__step_index - interval+1:self.__step_index+1].copy()
                    if self.auto_step_index:
                        self.__step_index = self.__step_index + 1
                        ## raise index change event
                    return rates
                except Exception as e:
                    self.logger.error(e)
            else:
                if self.update_rates():
                    self.get_rates_from_client(symbols, interval, frame)
                else:
                    if self.__auto_reset:
                        self.__step_index = random.randint(0, len(self.data))
                        ## raise index change event
                        return self.get_rates_from_client(symbols, interval, frame)
                    else:
                        self.logger.warning(f"not more data on index {self.__step_index}")
                    return pd.DataFrame()
        else:
            raise Exception("interval should be greater than 0.")
           
    def get_rates_from_client(self, symbols:list=[], interval:int=None, frame:int=None):
        pass

    def get_future_rates(self,interval=1, back_interval=0):
        if interval > 1:
            rates = None
            if self.__step_index >= interval-1:
                try:
                    #return data which have interval length
                    rates = self.data.iloc[self.__step_index - back_interval:self.__step_index+interval+1].copy()
                    return rates
                except Exception as e:
                    self.logger.error(e)
            else:
                if self.__auto_reset:
                    self.__step_index = random.randint(0, len(self.data))
                    ## raise index change event
                    return self.get_future_rates(interval)
                else:
                    self.logger.warning(f"not more data on index {self.__step_index}")
                return pd.DataFrame()
        else:
            raise Exception("interval should be greater than 0.")
    
    def get_current_ask(self):
        tick = self.data.iloc[self.__step_index]
        open_column = self.ohlc_columns["Open"]
        high_column = self.ohlc_columns["High"]
        
        if self.slip_type == "random":
            return random.uniform(tick[open_column], tick[high_column])
        elif self.slip_type == "none":
            return tick[open_column]
        elif self.slip_type == "percentage":
            slip = (tick[high_column] - tick[open_column]) * self.slip_rate
            return tick[open_column] + slip

    def get_current_bid(self):
        tick = self.data.iloc[self.__step_index]
        open_column = self.ohlc_columns["Open"]
        low_column = self.ohlc_columns["Low"]
        
        if self.slip_type == "random":
            return random.uniform(tick[low_column], tick[open_column])
        elif self.slip_type == "none":
            return tick[open_column]
        elif self.slip_type == "percentage":
            slip = (tick[open_column] - tick[low_column]) * self.slip_rate
            return tick[open_column] - slip
        
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
        
    def get_min_max(self, column, data_length = 0):
        if column in self.data.columns:
            if data_length == 0:
                return self.data[column].min(), self.data[column].max()
            else:
                if data_length > 0:
                    target_df = self.data[column].iloc[self.__step_index:self.__step_index + data_length]
                else:
                    target_df = self.data[column].iloc[self.__step_index + data_length + 1:self.__step_index +1]
                return target_df.min(), target_df.max()
        else:
            raise ValueError(f"{column} is not defined in {self.data.columns}")
        
    def get_current_index(self):
        return self.__step_index
    
    def get_params(self) -> dict:
        self.__args["files"] = self.files
        
        return param