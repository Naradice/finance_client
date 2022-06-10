import pandas as pd
import finance_client.market as market

class ClientBase:
    
    def __init__(self, budget=100000.0):
        self.market = market.Manager()

    def get_rates(self, interval) -> pd.DataFrame:
        raise Exception("Need to implement get_rates")
    
    def get_future_rates(self, interval) -> pd.DataFrame:
        pass
    
    def get_current_ask(self):
        print("Need to implement get_current_ask on your client")
    
    def get_current_bid(self):
        print("Need to implement get_current_bid on your client")
    
    def open_trade(self, is_buy,  amount, stop, trailing_step, order_type, symbol=None, option_info=None):
        if order_type == "Market":
            if is_buy:
                self.__market_buy(amount, option_info)
            else:
                self.__market_sell(amount, option_info)
        else:
            Exception(f"{order_type} is not defined.")
            
    def close_position(position=None, id=None):
        if position != None:
            id = position.id
        elif id != None:
            id = id
        else:
            Exception("either position or id should be specified.")
            
        if position.order_type == "ask":
            pass
        elif position.order_type == "bid":
            pass
        else:
            Exception(f"unkown order_type {position.order_type} is specified on close_position.")
            
    def close_all_positions():
        positions = self.market.get_open_positions()
        for position in positions:
            self.close_position(position)
    
    def __market_buy(self, amount, option_info=None):
        boughtRate = self.get_current_ask()
        position = self.market.open_position("ask", boughtRate, amount, option_info)
        return position

    def __market_sell(self, amount, option_info=None):
        soldRate = self.get_current_bid()
        position = self.market.open_position("ask", soldRate, amount, option_info)
        return position
    
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
    
    def get_positions(self):
        pass
    
    def get_holding_steps(self, position="ask")-> list:
        pass
    
    def get_params(self):
        raise Exception("Need to implement")