import os
from sbi_util.rpa import STOCK
from finance_client.client_base import Client
from finance_client.yfinance.client import YahooClient
import finance_client.frames as Frame

ID_KEY="sbi_id"
PASS_KEY="sbi_password"
TRADE_PASS_KEY="sbi_trade_password"

class SBIClient(Client):
    
    def __init__(self, id:str=None, password:str=None, trade_password:str=None, use_yfinance=True, indicater_processes: list = ..., post_processes: list = ..., frame: int = Frame.MIN5, provider="Default", do_render=True, logger_name=None, logger=None):
        if id is None:
            if ID_KEY in os.environ:
                id = os.environ[ID_KEY]
            else:
                raise Exception("Neither parameters nor os env has id value")
        if password is None:
            if PASS_KEY in os.environ:
                password = os.environ[PASS_KEY]
            else:
                raise Exception("Neither parameters nor os env has id value")
        if trade_password is None:
            if TRADE_PASS_KEY in os.environ:
                trade_password = os.environ[TRADE_PASS_KEY]
            else:
                raise Exception("Neither parameters nor os env has id value")
        self.rpa_client = STOCK(id, password, trade_password)
        budget = self.rpa_client.get_available_budget()
        if budget is None:
            raise Exception("Failed to initialize with getting budget.")
        if use_yfinance:
            self.client = YahooClient()
        else:
            print("get_rate is not available if you specify use_yfinance=False")
            
        super().__init__(budget, indicater_processes, post_processes, frame, provider, do_render, logger_name, logger)