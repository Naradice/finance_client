import datetime, os
import pandas as pd
from finance_client.csv.client import CSVClient
import finance_client.frames as Frame
import finance_client.vantage as vantage
from finance_client.utils.csvrw import write_df_to_csv, read_csv, get_file_path

class VantageClient(CSVClient):
    
    OHLC_COLUMNS = ["open", "high", "low", "close"]
    VOLUME_COLUMN = ["volume"]
    TIME_INDEX_NAME = "time"
    
    kinds = "vantage"
    
    def get_additional_params(self):
        self.logger.warn("parameters are not saved for vantage as credentials are included.")
        return {}
    
    def __init__(self, api_key, auto_step_index=False, frame: int = Frame.MIN5, finance_target = vantage.target.FX, symbol = ('JPY', 'USD'), start_index=None, seed=1017, slip_type="random", do_render=False, idc_processes=[], post_process=[], budget=1000000, logger=None):
        """Get ohlc rate from alpha vantage api
        Args:
            api_key (str): apikey of alpha vantage
            auto_step_index (bool, optional): increase step when get_rates is called. Defaults to False.
            finance_target (Target, optional): Target of finance market. Defaults to Target.FX.
            frame (int, optional): Frame of ohlc rates. Defaults to Frame.M5.
            symbol (tuple or str, optional): (from_symbol, to_symbol) for fx or stock symbol. Defaults to ('JPY', 'USD').
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
        if frame not in vantage.target.available_frame:
            raise ValueError(f"{frame} is not supported")
        self.frame = frame
        self.__frame_delta = datetime.timedelta(minutes=frame)
        self.debug = False
        if type(symbol) == tuple or type(symbol) == list:
            if finance_target == vantage.target.FX or finance_target == vantage.target.CRYPTO_CURRENCY:
                if len(symbol) != 2:
                    raise ValueError("symbol should have only two symbol for FX or Crypt Currency")
                self.__currency_trade = True
                self.from_symbol = symbol[0]
                self.to_symbol = symbol[1]
                symbol_name = "".join(symbol)
            else:
                self.__currency_trade = False
                if len(symbol) == 1:
                    symbol_name = symbol[0]
                    self.symbol = symbol[0]
                else:
                    #idea: create file name with UUID and caliculate hash from sorted symbol. Then create a table of filename and the hash
                    raise Exception("Client only handle one symbol")
        elif type(symbol) == str:
            if finance_target == vantage.target.FX or finance_target == vantage.target.CRYPTO_CURRENCY:
                self.__currency_trade = True
                symbol_set = symbol.split("/")#assume USD/JPY for ex
                if len(symbol_set) == 2:
                    self.from_symbol = symbol_set[0]
                    self.to_symbol = symbol_set[1]
                    symbol_name = "".join(symbol_set)
                else:
                    raise ValueError(f"Unexpected format: {symbol}")
            else:
                self.__currency_trade = False
                self.symbol = symbol
                symbol_name = symbol
        else:
            raise TypeError("symnbol should be tuple, list or str. For FX, (from, to) is expected")
        
        self.client = finance_target.create_client(api_key, logger)
        self.function_name = finance_target.to_function_name(finance_target, frame)
        self.file_name = f"vantage_{symbol_name}_{self.function_name}_{Frame.to_str(frame)}.csv"
                    
        self.__get_all_rates()
        self.__updated_time = datetime.datetime.now()
        file_path = get_file_path(self.kinds, self.file_name)
        super().__init__(auto_step_index=auto_step_index, file=file_path, frame=frame, provider="Vantage", out_frame=None, columns=self.OHLC_COLUMNS, date_column=self.TIME_INDEX_NAME, start_index=start_index, do_render=do_render, seed=seed, slip_type=slip_type, idc_processes=idc_processes, post_process=post_process, budget=budget, logger=logger)
    
    def __convert_response_to_df(self, data_json:dict):
        # handle meta data and series column
        columns = list(data_json.keys())
        if len(columns) >= 1:
            series_column = columns[1]
            time_zone = "UTC"
            meta_data_column = "Meta Data"
            if meta_data_column in data_json:
                meta_data = data_json[meta_data_column]
                for key, value in meta_data.items():
                    if "Time Zone" in key:
                        time_zone = value
                columns.remove(meta_data_column)
                if len(columns) != 1:
                    self.logger.warn(f"data key has multiple candidates unexpectedly: {columns}")
                series_column = columns[0]
            else:
                self.logger.warn("couldn't get meta data info. try to parse the response with UTC")
        else:
            self.logger.error(f"Unexpected response from vantage api: {columns}")
            
        #convert dict to data frame
        data_df = pd.DataFrame(data_json[series_column]).T
        data_df.index = pd.to_datetime(data_df.index)
        data_df = data_df.sort_index(ascending=True)
        
        # apply timezone info obtained from meta_data
        if data_df.index.tz is None:
            try:
                data_df.index = data_df.index.tz_localize(time_zone)
            except Exception as e:
                self.logger.error(f"can't localize the datetime: {e}")
                self.logger.warn(f"store datetime without timezone")
                
        # filter ohlc and volume
        desired_columns = [*self.OHLC_COLUMNS, *self.VOLUME_COLUMN]
        column_index = []
        d_index = 0
        for d_column in desired_columns:
            index = 0
            for column in data_df.columns:
                if d_column in str(column).lower():#assume 1. open
                    column_index.append((d_index, index))
                    break
                index += 1
            d_index += 1
            
        column_names = []
        target_columns = []
        for index in column_index:
            column_names.append(desired_columns[index[0]])
            target_columns.append(data_df.columns[index[1]])
        
        data_df = data_df[target_columns]
        data_df.columns = column_names
        return data_df
    
    def __get_currency_rates(self, from_symbol, to_symbol, frame, size):
        if frame <= Frame.H1:
            data = self.client.get_interday_rates(from_symbol, to_symbol, interval=frame, output_size=size)
        elif frame == Frame.D1:
            data = self.client.get_daily_rates(from_symbol, to_symbol, output_size=size)
        elif frame == Frame.W1:
            data = self.client.get_weekly_rates(from_symbol, to_symbol, output_size=size)
        elif frame == Frame.MO1:
            data = self.client.get_monthly_rates(from_symbol, to_symbol, output_size=size)
        else:
            raise ValueError(f"{Frame.to_str(frame)} is not supported")
        
        data_df = self.__convert_response_to_df(data)
        return data_df
            
    def __get_stock_rates(self, symbol, frame, size):
        if frame <= Frame.H1:
            data = self.client.get_interday_rates(symbol, interval=frame, output_size=size)
        elif frame == Frame.D1:
            data = self.client.get_daily_rates(symbol, output_size=size)
        elif frame == Frame.W1:
            data = self.client.get_weekly_rates(symbol, output_size=size)
        elif frame == Frame.MO1:
            data = self.client.get_monthly_rates(symbol, output_size=size)
        else:
            raise ValueError(f"{Frame.to_str(frame)} is not supported")

        data_df = self.__convert_response_to_df(data)
        return data_df
    
    def __get_all_rates(self):
        existing_rate_df = read_csv(self.kinds, self.file_name, [self.TIME_INDEX_NAME], pandas_option={"index_col":self.TIME_INDEX_NAME})            
        MAX_LENGTH = 950#not accurate
    
        if existing_rate_df is None:
            interval = MAX_LENGTH
            size = "full"
        else:
            delta = datetime.datetime.now(tz=datetime.timezone.utc) - existing_rate_df.index[-1]
            total_seconds = delta.total_seconds()
            if (total_seconds/(60*60*24*7)) >= 1:
                total_seconds = total_seconds * self.client.work_day_in_week/7#remove sat,sun
            interval = int((total_seconds/60)/self.frame)
            if interval <= MAX_LENGTH:
                size = "compact"# to be safe
            else:
                size = "full"
                if interval > MAX_LENGTH:
                    print("may be some time is missing")
        if self.__currency_trade:
            new_data_df = self.__get_currency_rates(self.from_symbol, self.to_symbol, self.frame, size)
        else:
            new_data_df = self.__get_stock_rates(self.symbol, self.frame, size)
            
        if existing_rate_df is not None:
            new_data_df = pd.concat([existing_rate_df, new_data_df])
            new_data_df = new_data_df[~new_data_df.index.duplicated(keep="first")]
            new_data_df = new_data_df.sort_index()
        write_df_to_csv(new_data_df, self.kinds, self.file_name, panda_option={"index_label":self.TIME_INDEX_NAME})
        return new_data_df
    
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