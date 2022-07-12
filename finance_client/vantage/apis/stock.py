import requests
import time
from finance_client.vantage.apis.base import API_BASE
from finance_client.vantage import Target


class STOCK(API_BASE):
    
    def __init__(self, api_key, logger=None) -> None:
        super().__init__(api_key, __name__, logger)        
        
    @API_BASE.response_handler
    def get_exchange_rates(self, symbol):
        if Target.check_stock_symbol(symbol) == False:
            raise ValueError(f"{symbol} is not supported")
        url = f"{self.URL_BASE}/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={self.api_key}"
        return requests.request("GET", url)
            
    @API_BASE.response_handler
    def get_interday_rates(self, symbol, interval, adjusted = False, output_size="full"):
        if Target.check_stock_symbol(symbol) == False:
            raise ValueError(f"{symbol} is not supported")
        if interval not in Target.available_frame:
            raise ValueError(f"{interval} is not supported")
        interval = Target.available_frame[interval]
        correct, size =  self.check_outputsize(output_size)
        if correct is False:
            self.logger.warn("outsize should be either full or compact")
        url = f"{self.URL_BASE}/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&outputsize={size}&apikey={self.api_key}"
        return requests.request("GET", url)
            
    @API_BASE.response_handler
    def get_daily_rates(self, symbol, output_size="full"):
        if Target.check_stock_symbol(symbol) == False:
            raise ValueError(f"{symbol} is not supported")
        correct, size =  self.check_outputsize(output_size)
        if correct is False:
            self.logger.warn("outsize should be either full or compact")
            
        url = f"{self.URL_BASE}/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize={size}&apikey={self.api_key}"
        return requests.request("GET", url)
    
    @API_BASE.response_handler
    def get_weekly_rates(self, symbol):
        if Target.check_stock_symbol(symbol) == False:
            raise ValueError(f"{symbol} is not supported")
            
        url = f"{self.URL_BASE}/query?function=TIME_SERIES_WEEKLY&symbol={symbol}&apikey={self.api_key}"
        return requests.request("GET", url)
    
    @API_BASE.response_handler
    def get_monthly_rates(self, symbol):
        if Target.check_stock_symbol(symbol) == False:
            raise ValueError(f"{symbol} is not supported")
            
        url = f"{self.URL_BASE}/query?function=TIME_SERIES_MONTHLY&symbol={symbol}&apikey={self.api_key}"
        return requests.request("GET", url)