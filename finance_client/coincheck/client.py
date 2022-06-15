from finance_client.coincheck.apis.servicebase import ServiceBase
from finance_client.coincheck.apis.account import Account
from finance_client.coincheck.apis.ticker import Ticker
from finance_client.client_base import Client

class CoinCheckClient(Client):
    
    def __init__(self):
        super()
        ServiceBase()
        ac = Account()
        print(f"balance: {ac.balance()}")
        print(f"balance: {ac.leverage_balance()}")
        print(f"info: {ac.info()}")

    def get_rates(self, frame):
        pass

    def get_current_ask(self):
        raise Exception("Need to implement get_current_ask")
        pass

    def get_current_bid(self):
        raise Exception("Need to implement get_current_bid")
        pass

    def market_buy(self, amount):
        raise Exception("Need to implement market_buy")
        pass

    def market_sell(self, order):
        raise Exception("Need to implement market_sell")
        pass
    
    def buy_settlement(self, position):
        print("Need to implement buy_settlement.")
        pass
    
    def sell_all_settlement(self):
        raise Exception("Need to implement sell_all_setlement")

    def close(self):
        pass

    def get_next_tick(self, frame=5):
        ticker = Ticker()
        return ticker.get()

    def reset(self):
        raise Exception("Need to implement reset")
            
    @property
    def max(self, column):
        raise Exception("Need to implement max")
        
    @property
    def min(self, column):
        raise Exception("Need to implement min")
        
    def __getitem__(self, ndx):
        return None
    
    def get_min_max(self, column):
        pass
    
    def get_ohlc_columns(self):
        columns = {'Open':'Open', 'High':'High', 'Low':'Low', 'Close':'Close'}
        return columns
    
    def get_diffs_with_minmax(self) -> dict:
        pass