import pandas as pd
from pkg_resources import require
from finance_client import utils
import finance_client.market as market
from finance_client.frames import Frame

class Client:
    
    frame = None
    columns = None
    
    def __init__(self, budget=100000.0, indicater_processes:list = []):
        self.market = market.Manager(budget)
        self.__idc_processes = []
        self.__additional_length_for_idc = 0
        self.add_indicaters(indicater_processes)
        
    ## call from an actual client
        
    def open_trade(self, is_buy,  amount:float, stop, order_type:str, symbol:str, option_info=None):
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
            if is_buy:
                ask_rate = self.get_current_ask()
                self.__market_buy(symbol, ask_rate, amount, option_info)
                return self.__open_long_position(symbol, ask_rate, amount, option_info)
            else:
                bid_rate = self.get_current_bid()
                self.__market_sell(symbol, bid_rate, amount, option_info)
                return self.__open_short_position(symbol, amount, option_info)
        else:
            Exception(f"{order_type} is not defined.")
            
    def close_position(self, price:float, position:market.Position=None, id=None, amount=None):
        """ close open_position. If specified amount is less then position, close the specified amount only
        Either position or id must be specified.
        sell_for_settlement or buy_for_settlement is calleds

        Args:
            position (Position, optional): Position returned by open_trade. Defaults to None.
            id (uuid, optional): Position.id. Ignored if position is specified. Defaults to None.
            amount (float, optional): amount of close position. use all if None. Defaults to None.
        """
        if position != None:
            id = position.id
        elif id != None:
            position = self.market.get_position(id)
        else:
            Exception("either position or id should be specified.")
            
        if position.order_type == "ask":
            self.__sell_for_settlment(position.symbol , price, amount, position.option)
        elif position.order_type == "bid":
            self.__buy_for_settlement(position.symbol, price, amount, position.option)
        else:
            Exception(f"unkown order_type {position.order_type} is specified on close_position.")
        return self.market.close_position(position, price, amount)
    
    def close_all_positions(self):
        """close open_position.
        sell_for_settlement or buy_for_settlement is calleds for each position
        """
        positions = self.market.get_open_positions()
        results = []
        for position in positions:
            result = self.close_position(position)
            results.append(result)
        return results
    
    def get_positions(self) -> list:
        return self.market.get_open_positions()
    
    def __run_processes(self, data:pd.DataFrame) -> pd.DataFrame:
        """
        Ex. you can define MACD as process. The results of the process are stored as dataframe[key] = values
        """
        data_cp = data.copy()
        
        if len(self.__idc_processes) > 0:
            processes = self.__idc_processes
        else:
            return None
        
        for process in processes:
            values_dict = process.run(data_cp)
            for column, values in values_dict.items():
                data_cp[column] = values
        data_cp = data_cp.dropna(how = 'any')
        return data_cp
                
    def add_indicater(self, process: utils.ProcessBase):
        self.__idc_processes.append(process)
        required_length = process.get_minimum_required_length()
        if self.__additional_length_for_idc < required_length:
            self.__additional_length_for_idc = required_length
            
    def add_indicaters(self, processes: list):
        for process in processes:
            self.add_indicater(process)

    ## Need to implement in the actual client ##

    def get_rates(self, interval:int, frame:int =None) -> pd.DataFrame:
        raise Exception("Need to implement get_rates")
    
    def get_rate_with_indicaters(self, interval, frame:int = None) -> pd.DataFrame:
        required_length = interval + self.__additional_length_for_idc
        ohlc = self.get_rates(required_length)
        data = self.__run_processes(ohlc)
        return data.iloc[-interval:]
    
    def get_future_rates(self, interval) -> pd.DataFrame:
        pass
    
    def get_current_ask(self) -> float:
        print("Need to implement get_current_ask on your client")
    
    def get_current_bid(self) -> float:
        print("Need to implement get_current_bid on your client")
            
    def __market_buy(self, symbol, ask_rate, amount, option_info):
        pass
    
    def __market_sell(self, symbol, bid_rate, amount, option_info):
        pass
    
    def __buy_for_settlement(self, symbol, ask_rate, amount, option_info):
        pass
    
    def __sell_for_settlment(self, symbol, bid_rate, amount, option_info):
        pass
    
    def get_params(self) -> dict:
        raise Exception("Need to implement")
    
    ## defined by the actual client for dataset or env
    def close_client(self):
        pass
    
    def get_next_tick(self, frame=5):
        raise Exception("Need to implement get_next_tick")

    def reset(self, mode=None):
        raise Exception("Need to implement reset")
    
    def get_min_max(column, data_length = 0):
        pass
    
    @property
    def max(self):
        raise Exception("Need to implement max")
        
    @property
    def min(self):
        raise Exception("Need to implement min")
        
    def get_ohlc_columns(self):
        raise Exception("Need to implement")
    
    def __getitem__(self, ndx):
        return None
    
    def __get_long_position_diffs(self, standalization="minimax"):
        positions = self.market.poitions["ask"]
        if len(positions) > 0:
            diffs = []
            current_bid = self.get_current_bid()
            if standalization == "minimax":
                current_bid = utils.mini_max(current_bid, self.min, self.max, (0, 1))
                for key, position in positions.items():
                    price = standalization.mini_max(position.price, self.min, self.max, (0, 1))
                    diffs.append(current_bid - price)
            else:
                for key, position in positions.items():
                    diffs.append(current_bid - position.price)
            return diffs
        else:
            return []
    
    def __get_short_position_diffs(self, standalization="minimax"):
        positions = self.market.poitions["bid"]
        if len(positions) > 0:
            diffs = []
            current_ask = self.get_current_ask()
            if standalization == "minimax":
                current_ask = standalization.mini_max(current_ask, self.min, self.max, (0, 1))
                for key, position in positions.items():
                    price = standalization.mini_max(position.price, self.min, self.max, (0, 1))
                    diffs.append(price - current_ask)
            else:
                for key, position in positions.items():
                    diffs.append(position.price - current_ask)
            return diffs
        else:
            return []

    def get_diffs(self, position_type='ask') -> list:
        if position_type == 'ask' or position_type == 'long':
            return self.__get_long_position_diffs()
        elif position_type == "bid" or position_type == 'short':
            return self.__get_short_position_diffs()
        else:
            diffs = self.__get_long_position_diffs()
            diffs.append(self.__get_short_position_diffs())
            return diffs
        
    
    def get_diffs_with_minmax(self, position_type='ask')-> list:
        if position_type == 'ask' or position_type == 'long':
            return self.__get_long_position_diffs(standalization="minimax")
        elif position_type == "bid" or position_type == 'short':
            return self.__get_short_position_diffs(standalization="minimax")
        else:
            diffs = self.__get_long_position_diffs(standalization="minimax")
            diffs.append(self.__get_short_position_diffs(standalization="minimax"))
            return diffs
    
    def __open_long_position(self, symbol, boughtRate, amount, option_info=None):
        position = self.market.open_position("ask", boughtRate, amount, option_info)
        return position

    def __open_short_position(self, soldRate, amount, option_info=None):
        position = self.market.open_position("bid", soldRate, amount, option_info)
        return position