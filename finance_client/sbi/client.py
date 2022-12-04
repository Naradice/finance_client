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
    
    def __init__(self, symbols:list, id:str=None, password:str=None, trade_password:str=None, use_yfinance=True, auto_step_index=True, adjust_close=True, frame: int=Frame.D1, provider="Default", do_render=True, logger_name=None, logger=None):
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
            self.client = YahooClient(symbols, auto_step_index=auto_step_index, frame=frame, adjust_close=adjust_close)
        else:
            print("get_rate is not available if you specify use_yfinance=False")
        super().__init__(budget=budget, provider=provider, frame=frame, do_render=do_render, logger_name=logger_name, logger=logger)
        
    def get_additional_params(self):
        return {}

    def _get_ohlc_from_client(self, interval:int=None, symbols:list=[], frame:int=None):
        return self.client._get_ohlc_from_client(interval, symbols, frame)
    
    def get_future_rates(self, interval, symbols=[]) -> pd.DataFrame:
        return self.client.get_future_rates(interval, symbols=symbols)
    
    def get_current_ask(self, symbols=[]) -> float:
        return self.client.get_current_ask(symbols)
    
    def get_current_bid(self, symbols=[]) -> float:
        return self.client.get_current_bid(symbols)
            
    def _market_buy(self, symbol, ask_rate=None, amount=1, tp=None, sl=None, option_info=None):
        suc = self.rpa_client.buy_order(symbol, amount, ask_rate)
        if suc:
            return True, None
        else:
            return False, None
    
    def _market_sell(self, symbol, bid_rate, amount, tp, sl, option_info):
        err_msg = "market_sell is not implemented"
        self.logger.info(err_msg)
        return False, err_msg
    
    def _buy_for_settlement(self, symbol, ask_rate, amount, option_info, result):
        return self.rpa_client.sell_to_close_buy_order(symbol, amount, ask_rate)
    
    def _sell_for_settlment(self, symbol, bid_rate, amount, option_info, result):
        return self.rpa_client.sell_to_close_buy_order(symbol, amount, bid_rate)
    
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