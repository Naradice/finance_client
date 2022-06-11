import pandas as pd
import finance_client.market as market
from finance_client.frames import Frame

class ClientBase:
    
    def __init__(self, budget=100000.0):
        self.market = market.Manager()
        
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
                self.market_buy(symbol, ask_rate, amount, option_info)
                return self.__open_long_position(symbol, ask_rate, amount, option_info)
            else:
                bid_rate = self.get_current_bid()
                self.market_sell(symbol, bid_rate, amount, option_info)
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
            self.sell_for_settlment(position.symbol , price, amount, position.option)
        elif position.order_type == "bid":
            self.buy_for_settlement(position.symbol, price, amount, position.option)
        else:
            Exception(f"unkown order_type {position.order_type} is specified on close_position.")
        self.market.close_position(position, price, amount)
    
    def close_all_positions(self):
        """close open_position.
        sell_for_settlement or buy_for_settlement is calleds for each position
        """
        positions = self.market.get_open_positions()
        for position in positions:
            self.close_position(position)
    
    def get_positions(self) -> list:
        return self.market.get_open_positions()
    
    ## Need to implement in the actual client ##

    def get_rates(self, frame:int) -> pd.DataFrame:
        raise Exception("Need to implement get_rates")
    
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
    
    def buy_for_settlement(self, symbol, ask_rate, amount, option_info):
        pass
    
    def sell_for_settlment(self, symbol, bid_rate, amount, option_info):
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
    
    @property
    def frame(self):
        raise Exception("Need to implement frame")
    
    @property
    def columns(self):
        raise Exception("Need to implement columns")
        
    def get_ohlc_columns(self):
        raise Exception("Need to implement")
    
    def __getitem__(self, ndx):
        return None
    
    def get_diffs_with_minmax(self, position='ask')-> list:
        pass
    
    def get_holding_steps(self, position="ask")-> list:
        pass
    
    def __open_long_position(self, symbol, boughtRate, amount, option_info=None):
        position = self.market.open_position("ask", boughtRate, amount, option_info)
        return position

    def __open_short_position(self, soldRate, amount, option_info=None):
        position = self.market.open_position("bid", soldRate, amount, option_info)
        return position