import datetime, os, json, time
import pandas as pd
from finance_client.client_base import Client
import finance_client.frames as Frame
from finance_client.vantage import Target

class VantageClient(Client):

    available_kinds = ["fx", "forex", "stock", "btc", "digital"]

    def __init__(self, api_key, auto_index=False, finance_target = Target.FX, frame = 5, symbol = ('JPY', 'USD'), post_process = [], budget=1000000, logger = None, seed=1017):
        if self.frame not in Target.available_frame:
            raise ValueError(f"{self.frame} is not supported")
        
        super().__init__( budget=budget, frame=frame, provider="vantage", post_processes= post_process, logger_name=__name__, logger=logger)
        self.debug = False
        self.auto_index = auto_index
        if type(symbol) == tuple or type(symbol) == list:
            if finance_target == Target.FX or finance_target == Target.CRYPTO_CURRENCY:
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
            if finance_target == Target.FX or finance_target == Target.CRYPTO_CURRENCY:
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
        file_name = f"vantage_{symbol_name}_{self.function_name}_{Frame.to_str(frame)}.csv"
        
        self.data_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), f"../data_source/alpha-vantage/"))
        if os.path.exists(self.data_folder) == False:
            os.makedirs(self.data_folder)
            
        self.file_path = os.path.join(self.data_folder, file_name)

    def get_current_ask(self):
        pass
    
    def get_current_bid(self):
        pass
        
    def get_current_spread(self):
        pass
    
    def market_sell(self, symbol, price, amount, tp=None, sl=None):
        pass
            
    def buy_for_settlement(self, symbol, price, amount, option, result):
        pass
    
    def market_buy(self, symbol, price, amount, tp=None, sl=None):
        pass
    
    def buy_order(self, value, tp=None, sl=None):
        pass
        
    def sell_order(self, value, tp=None, sl=None):
        pass
    
    def update_order(self, _type, _id, value, tp, sl):
        print("NOT IMPLEMENTED")

    def sell_for_settlment(self, symbol, price, amount, option, result):
        pass    
    
    def __convert_response_to_df(self, data_json:dict):
        # handle meta data and series column
        columns = list(data_json.keys())
        series_column = columns[1]
        time_zone = "UTC"
        meta_data_column = "Meta Data"
        if meta_data_column in data_json:
            meta_data = data_json[meta_data_column]
            for key, value in meta_data.items():
                if "Time Zone" in key:
                    time_zone = value
            columns = columns.remove(meta_data_column)
            if len(columns) != 1:
                self.logger.warn(f"data key has multiple candidates unexpectedly: {columns}")
            series_column = columns[0]
        else:
            self.logger.warn("couldn't get meta data info. try to parse the response with UTC")
            
        #convert dict to data frame
        data_df = pd.DataFrame(data_json[series_column]).T
        data_df.index = pd.to_datetime(data_df.index)
        data_df.sort_index()
        
        # apply timezone info obtained from meta_data
        if data_df.index.tz is None:
            try:
                data_df.index = data_df.index.tz_localize(time_zone)
            except Exception as e:
                self.logger.error(f"can't localize the datetime: {e}")
                self.logger.warn(f"store datetime without timezone")
                
        # filter ohlc and volume
        desired_columns = ["open", "high", "low", "close", "volume"]
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
            
        data_df.columns = target_columns
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
        existing_rate_df = None
        if os.path.exists(self.file_path):
            existing_rate_df = pd.read_csv(self.file_path, parse_dates=["time"])
            
        MAX_LENGTH = 950#not accurate
    
        if existing_rate_df is None:
            interval = MAX_LENGTH
            size = "full"
        else:
            delta = datetime.datetime.utcnow() - existing_rate_df.iloc[0].name
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
            rate_df = pd.concat([existing_rate_df, new_data_df])
            rate_df = rate_df.drop_duplicates()
            rate_df = rate_df.sort_index()
        rate_df.to_csv(self.file_path)
        return rate_df            
    
    def get_rates(self, interval):
        MAX_LENGTH = 950#not accurate
        
        if interval <= MAX_LENGTH:
            if interval <= 100:
                size = "compact"
            else:
                size = "full"
                
            if self.__currency_trade:
                new_data_df = self.__get_currency_rates(self.from_symbol, self.to_symbol, self.frame, size)
            else:
                new_data_df = self.__get_stock_rates(self.symbol, self.frame, size)
        else:
            new_data_df = self.__get_all_rates()
            
        out_size_df = new_data_df.iloc[-interval:]
        return out_size_df
    
    def cancel_order(self, order):
        pass