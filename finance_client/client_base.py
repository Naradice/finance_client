import pandas as pd

class ClientBase:
    
    def __init__(self):
        pass

    def get_rates(self, interval) -> pd.DataFrame:
        raise Exception("Need to implement get_rates")
    
    def get_future_rates(self, interval) -> pd.DataFrame:
        pass
    
    def get_current_ask(self):
        raise Exception("Need to implement get_current_ask")
    
    def get_current_bid(self):
        raise Exception("Need to implement get_current_bid")
    
    def market_buy(self, amount):
        raise Exception("Need to implement market_buy")

    def market_sell(self, order):
        raise Exception("Need to implement market_sell")
    
    def buy_settlement(self, position, point: int=None):
        print("Need to implement buy_settlement.")
    
    def sell_settlement(self, position:dict, point: int=None):
        print("Need to implement buy_settlement.")
    
    def sell_all_settlement(self):
        raise Exception("Need to implement sell_all_setlement")

    def close(self):
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