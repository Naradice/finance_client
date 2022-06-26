import queue
import threading
import pandas as pd
from finance_client import utils
import finance_client.market as market
from finance_client.frames import Frame
from logging import getLogger, config
import os, json, datetime
import time

class Client:
    
    def __init__(self, budget=1000000.0, indicater_processes:list = [], post_processes: list = [], frame:int = Frame.MIN5, provider = "Default", logger_name=None, logger=None):
        self.auto_index = None
        dir = os.path.dirname(__file__)
        self.__data_queue = None
        self.__timer_thread = None
        self.__data_queue_length = None
        try:
            with open(os.path.join(dir, './settings.json'), 'r') as f:
                    settings = json.load(f)
        except Exception as e:
            self.logger.error(f"fail to load settings file on client: {e}")
            raise e
        self.ohlc_columns = None
        
        if logger == None:
            logger_config = settings["log"]
        
            try:
                log_file_base_name = logger_config["handlers"]["fileHandler"]["filename"]
                log_folder = os.path.join(os.path.dirname(__file__), 'logs')
                if os.path.exists(log_folder) == False:
                    os.makedirs(log_folder)
                logger_config["handlers"]["fileHandler"]["filename"] = f'{log_folder}/{log_file_base_name}_{datetime.datetime.utcnow().strftime("%Y%m%d")}.logs'
                config.dictConfig(logger_config)
            except Exception as e:
                self.logger.error(f"fail to set configure file: {e}")
                raise e
            if logger_name == None:
                logger_name == __name__
            self.logger = getLogger(logger_name)
            self.market = market.Manager(budget)
        else:
            self.logger = logger
            self.market = market.Manager(budget, logger=logger, provider=provider)
            
        try:
            self.frame = int(frame)
        except Exception as e:
            self.logger.error(e)
        
        self.__idc_processes = []
        self.__additional_length_for_prc = 0
        self.add_indicaters(indicater_processes)
        self.__postprocesses = []
        self.add_postprocesses(post_processes)
        
    def initialize_budget(self, budget):
        self.market(budget)
        
    ## call from an actual client
        
    def open_trade(self, is_buy, amount:float, order_type:str, symbol:str, price:float=None, option_info=None):
        """ by calling this in your client, order function is called and position is stored

        Args:
            is_buy (bool): buy order or not
            amount (float): amount of trade unit
            stop (_type_): _description_
            order_type (str): Market, 
            symbol (str): currency. ex USDJPY.
            option_info (any, optional): store info you want to position. Defaults to None.

        Returns:
            Position: you can specify position or position.id to close the position
        """
        if order_type == "Market":
            self.logger.debug("market order is requested.")
            if is_buy:
                if price == None:
                    ask_rate = self.get_current_ask()
                else:
                    ask_rate = price
                result = self.market_buy(symbol, ask_rate, amount, option_info)
                return self.__open_long_position(symbol, ask_rate, amount, option_info, result)
            else:
                if price == None:
                    bid_rate = self.get_current_bid()
                else:
                    bid_rate = price
                result = self.market_sell(symbol, bid_rate, amount, option_info)
                return self.__open_short_position(symbol, bid_rate, amount, option_info, result)
        else:
            self.logger.debug(f"{order_type} is not defined.")
            
    def close_position(self, price:float=None, position:market.Position=None, id=None, amount=None):
        """ close open_position. If specified amount is less then position, close the specified amount only
        Either position or id must be specified.
        sell_for_settlement or buy_for_settlement is calleds

        Args:
            price (float, optional): price for settlement. If not specified, current value is used.
            position (Position, optional): Position returned by open_trade. Defaults to None.
            id (uuid, optional): Position.id. Ignored if position is specified. Defaults to None.
            amount (float, optional): amount of close position. use all if None. Defaults to None.
        """
        if position != None:
            id = position.id
        elif id != None:
            position = self.market.get_position(id)
        else:
            self.logger.error("Either position or id should be specified.")
            return None
        if amount == None:
            amount = position.amount
            
        if position.order_type == "ask":
            if (price == None):
                price = self.get_current_bid()
            self.sell_for_settlment(position.symbol , price, amount, position.option, position.result)
        elif position.order_type == "bid":
            if (price == None):
                price = self.get_current_ask()
            self.buy_for_settlement(position.symbol, price, amount, position.option, position.result)
        else:
            self.logger.warning(f"Unkown order_type {position.order_type} is specified on close_position.")
        return self.market.close_position(position, price, amount)
    
    def close_all_positions(self):
        """close all open_position.
        sell_for_settlement or buy_for_settlement is calleds for each position
        """
        positions = self.market.get_open_positions()
        results = []
        for position in positions:
            result = self.close_position(position=position)
            results.append(result)
        return results

    def close_long_positions(self):
        """close all open_long_position.
        sell_for_settlement is calleds for each position
        """
        positions = self.market.get_open_positions(order_type="ask")
        results = []
        for position in positions:
            result = self.close_position(position=position)
            results.append(result)
        return results

    def close_short_positions(self):
        """close all open_long_position.
        sell_for_settlement is calleds for each position
        """
        positions = self.market.get_open_positions(order_type="bid")
        results = []
        for position in positions:
            result = self.close_position(position=position)
            results.append(result)
        return results
    
    def get_positions(self) -> list:
        return self.market.get_open_positions()
    
    def __run_processes(self, data:pd.DataFrame) -> pd.DataFrame:
        """
        Ex. you can define MACD as process. The results of the process are stored as dataframe[key] = values
        """
        data_cp = data.copy()
        columns_dict = self.get_ohlc_columns()
        date_data = None
        
        for process in self.__idc_processes:
            values_dict = process.run(data_cp)
            for column, values in values_dict.items():
                data_cp[column] = values
                
        if "Time" in columns_dict and columns_dict["Time"] != None:
            date_column = columns_dict["Time"]
            columns = list(data_cp.columns.copy())
            date_data = data_cp[date_column].copy()
            #df = df.set_index(date_column)
            columns.remove(date_column)
            data_cp = data_cp[columns]
                    
        for process in self.__postprocesses:
            values_dict = process.run(data_cp)
            for column, values in values_dict.items():
                data_cp[column] = values
                
        if type(date_data) != type(None):
            data_cp[date_column] = date_data
        data_cp = data_cp.dropna(how = 'any')
        return data_cp
    
    def have_process(self, process: utils.ProcessBase):
        return process in self.__idc_processes
        
                
    def add_indicater(self, process: utils.ProcessBase):
        if self.have_process(process) == False:
            self.__idc_processes.append(process)
            required_length = process.get_minimum_required_length()
            if self.__additional_length_for_prc < required_length:
                self.__additional_length_for_prc = required_length
        else:
            self.logger.info(f"process {process.key} is already added. If you want to add it, please change key value.")
            
    def add_indicaters(self, processes: list):
        for process in processes:
            self.add_indicater(process)
    
    def add_postprocess(self, process: utils.ProcessBase):
        if self.have_process(process) == False:
            self.__postprocesses.append(process)
            additional_length = process.get_minimum_required_length()
            self.__additional_length_for_prc += additional_length
        else:
            self.logger.info(f"process {process.key} is already added. If you want to add it, please change key value.")
    
    def add_postprocesses(self, processes: list):
        for process in processes:
            self.add_postprocess(process)
        
            
    def get_rate_with_indicaters(self, interval) -> pd.DataFrame:
        required_length = interval + self.__additional_length_for_prc
        ohlc = self.get_rates(required_length)
        if type(ohlc) == pd.DataFrame and len(ohlc) == required_length:
            data = self.__run_processes(ohlc)
            return data.iloc[-interval:]
        else:
            return ohlc
        
    def get_data_queue(self, data_length = int):
        if self.__data_queue == None:
            self.__timer_thread = threading.Thread(target=self.__timer, daemon=True)
            self.__timer_thread.start()
            self.__data_queue = queue.Queue()
        if data_length > 0:
            self.__data_queue_length = data_length
        else:
            self.logger.warning(f"data_length must be greater than 1. change length to 1")
            self.__data_queue_length = 1
        return self.__data_queue
        
    
    def __timer(self):
        next_time = 0
        interval = self.frame*60

        if self.frame <= 60:
            base_time = datetime.datetime.now()
            target_min = datetime.timedelta(minutes=(self.frame- base_time.minute % self.frame))
            target_time = base_time + target_min
            sleep_time = datetime.datetime.timestamp(target_time) - datetime.datetime.timestamp(base_time) - base_time.second
        if sleep_time > 0:
            time.sleep(sleep_time)
        base_time = time.time()
        while self.__data_queue != None:
            t = threading.Thread(target=self.__put_data_to_queue, daemon=True)
            t.start()
            next_time = ((base_time - time.time()) % interval) or interval
            time.sleep(next_time)
            
    def stop_quing(self):
        self.__data_queue = None
        
    def __put_data_to_queue(self):
        if len(self.__idc_processes) > 0:
            data = self.get_rate_with_indicaters(self.__data_queue_length)
        else:
            data = self.get_rates(self.__data_queue_length)
        self.__data_queue.put(data)
        
    ## Need to implement in the actual client ##

    def get_rates(self, interval:int) -> pd.DataFrame:
        print("Overwrite get_rates in your client")
    
    def get_future_rates(self, interval) -> pd.DataFrame:
        pass
    
    def get_current_ask(self) -> float:
        print("Need to implement get_current_ask on your client")
    
    def get_current_bid(self) -> float:
        print("Need to implement get_current_bid on your client")
            
    def market_buy(self, symbol, ask_rate, amount, option_info):
        pass
    
    def market_sell(self, symbol, bid_rate, amount, option_info):
        pass
    
    def buy_for_settlement(self, symbol, ask_rate, amount, option_info, result):
        pass
    
    def sell_for_settlment(self, symbol, bid_rate, amount, option_info, result):
        pass
    
    def get_params(self) -> dict:
        print("Need to implement get_params")
    
    ## defined by the actual client for dataset or env
    def close_client(self):
        pass
    
    def get_next_tick(self, frame=5):
        print("Need to implement get_next_tick")

    def reset(self, mode=None):
        print("Need to implement reset")
    
    def get_min_max(column, data_length = 0):
        pass
    
    @property
    def max(self):
        print("Need to implement max")
        return 1
        
    @property
    def min(self):
        print("Need to implement min")
        return -1
    
    def __getitem__(self, ndx):
        return None
    
    def __get_long_position_diffs(self, standalization="minimax"):
        positions = self.market.positions["ask"]
        if len(positions) > 0:
            diffs = []
            current_bid = self.get_current_bid()
            if standalization == "minimax":
                current_bid = utils.mini_max(current_bid, self.min, self.max, (0, 1))
                for key, position in positions.items():
                    price = utils.mini_max(position.price, self.min, self.max, (0, 1))
                    diffs.append(current_bid - price)
            else:
                for key, position in positions.items():
                    diffs.append(current_bid - position.price)
            return diffs
        else:
            return []
    
    def __get_short_position_diffs(self, standalization="minimax"):
        positions = self.market.positions["bid"]
        if len(positions) > 0:
            diffs = []
            current_ask = self.get_current_ask()
            if standalization == "minimax":
                current_ask = utils.mini_max(current_ask, self.min, self.max, (0, 1))
                for key, position in positions.items():
                    price = utils.mini_max(position.price, self.min, self.max, (0, 1))
                    diffs.append(price - current_ask)
            else:
                for key, position in positions.items():
                    diffs.append(position.price - current_ask)
            return diffs
        else:
            return []

    def get_diffs(self, position_type=None) -> list:
        if position_type == 'ask' or position_type == 'long':
            return self.__get_long_position_diffs()
        elif position_type == "bid" or position_type == 'short':
            return self.__get_short_position_diffs()
        else:
            diffs = self.__get_long_position_diffs()
            diffs.append(self.__get_short_position_diffs())
            return diffs
        
    
    def get_diffs_with_minmax(self, position_type=None)-> list:
        if position_type == 'ask' or position_type == 'long':
            return self.__get_long_position_diffs(standalization="minimax")
        elif position_type == "bid" or position_type == 'short':
            return self.__get_short_position_diffs(standalization="minimax")
        else:
            diffs = self.__get_long_position_diffs(standalization="minimax")
            diffs.append(self.__get_short_position_diffs(standalization="minimax"))
            return diffs
    
    def __open_long_position(self, symbol, boughtRate, amount, option_info=None, result=None):
        self.logger.debug("open long position is created.")
        position = self.market.open_position("ask", symbol, boughtRate, amount, option_info, result)
        return position

    def __open_short_position(self, symbol, soldRate, amount, option_info=None, result = None):
        self.logger.debug("open short position is created.")
        position = self.market.open_position("bid", symbol,  soldRate, amount, option_info, result=result)
        return position
    
    def get_ohlc_columns(self) -> dict:
        if self.ohlc_columns == None:
            columns = {}
            temp = self.auto_index
            self.auto_index = False
            data = self.get_rates(1)
            self.auto_index = temp
            for column in data.columns.values:
                column_ = str(column).lower()
                if column_ == 'open':
                    columns['Open'] = column
                elif column_ == 'high':
                    columns['High'] = column
                elif column_ == 'low':
                    columns['Low'] = column
                elif column_ == 'close':
                    columns['Close'] = column
                elif "time" in column_:#assume time, timestamp or datetime
                    columns["Time"] = column
            self.ohlc_columns = columns
        return self.ohlc_columns