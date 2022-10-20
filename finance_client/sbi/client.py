import os
from finance_client.sbi.sbi_util.rpa import STOCK
from finance_client.client_base import Client
from finance_client.yfinance.client import YahooClient
import finance_client.frames as Frame
import pandas as pd

class SBIClient(Client):
    
    ID_KEY="sbi_id"
    PASS_KEY="sbi_password"
    TRADE_PASS_KEY="sbi_trade_password"
    
    def __init__(self, symbol:str, id:str=None, password:str=None, trade_password:str=None, use_yfinance=True, indicater_processes: list = [], post_processes: list = [], frame: int = Frame.D1, provider="Default", do_render=True, logger_name=None, logger=None):
        if id is None:
            if self.ID_KEY in os.environ:
                id = os.environ[self.ID_KEY]
            else:
                raise Exception("Neither parameters nor os env has id value")
        if password is None:
            if self.PASS_KEY in os.environ:
                password = os.environ[self.PASS_KEY]
            else:
                raise Exception("Neither parameters nor os env has id value")
        if trade_password is None:
            if self.TRADE_PASS_KEY in os.environ:
               trade_password = os.environ[self.TRADE_PASS_KEY]
            else:
                raise Exception("Neither parameters nor os env has id value")
        self.rpa_client = STOCK(id, password, trade_password)
        budget = self.rpa_client.get_available_budget()
        if budget is None:
            raise Exception("Failed to initialize with getting budget.")
        if use_yfinance:
            self.client = YahooClient(symbol+".T", frame=frame, adjust_close=False)
        else:
            print("get_rate is not available if you specify use_yfinance=False")
        super().__init__(budget, indicater_processes, post_processes, frame, provider, do_render, logger_name, logger)
        
    def get_additional_params(self):
        return {}

    def get_rates_from_client(self, interval:int):
        return self.client.get_rates_from_client(interval)
    
    def get_future_rates(self, interval) -> pd.DataFrame:
        return self.client.get_future_rates(interval)
    
    def get_current_ask(self) -> float:
        return self.client.get_current_ask()
    
    def get_current_bid(self) -> float:
        return self.client.get_current_bid()
            
    def market_buy(self, symbol, ask_rate=None, amount=1, tp=None, sl=None, option_info=None):
        return self.rpa_client.buy_order(symbol, amount, None)
    
    def market_sell(self, symbol, bid_rate, amount, tp, sl, option_info):
        self.logger.info("market_sell is not implemented")
    
    def buy_for_settlement(self, symbol, ask_rate, amount, option_info, result):
        return self.rpa_client.sell_to_close_buy_order(symbol, amount)
    
    def sell_for_settlment(self, symbol, bid_rate, amount, option_info, result):
        return self.rpa_client.sell_to_close_buy_order(symbol, amount)
    
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