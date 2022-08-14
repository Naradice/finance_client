from finance_client.coincheck.apis.servicebase import ServiceBase
from finance_client.coincheck.apis.account import Account
from finance_client.coincheck.apis.ticker import Ticker
from finance_client.coincheck.apis.ws import TradeHistory
from finance_client.client_base import Client
import pandas as pd

class CoinCheckClient(Client):
    
    def __store_ticks(self, tick):
        pass
    
    def __init__(self, budget=1000000, indicater_processes: list = [], post_processes: list = [], frame: int = 30, provider="CoinCheck", do_render=False, logger_name=None, logger=None):
        super().__init__(budget, indicater_processes, post_processes, frame, provider, do_render, logger_name, logger)
        ServiceBase()
        ac = Account()
        print(f"balance: {ac.balance()}")
        print(f"balance: {ac.leverage_balance()}")
        print(f"info: {ac.info()}")
        th = TradeHistory()
        th.subscribe(on_tick=self.__store_ticks)

## Need to implement in the actual client ##
    
    def get_additional_params(self):
        return {}

    def get_rates_from_client(self, interval:int):
        return {}
    
    def get_future_rates(self, interval) -> pd.DataFrame:
        pass
    
    def get_current_ask(self) -> float:
        print("Need to implement get_current_ask on your client")
    
    def get_current_bid(self) -> float:
        print("Need to implement get_current_bid on your client")
            
    def market_buy(self, symbol, ask_rate, amount, tp, sl, option_info):
        pass
    
    def market_sell(self, symbol, bid_rate, amount, tp, sl, option_info):
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
    
    ###