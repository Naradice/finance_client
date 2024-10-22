import requests

from .base import API_BASE


class FOREX(API_BASE):
    def __init__(self, api_key, logger=None) -> None:
        super().__init__(api_key, __name__, logger)

    @API_BASE.response_handler
    def get_exchange_rates(self, from_currency, to_currency, retry_ount=0):
        if self.check_physical_currency(from_currency) is False:
            raise ValueError(f"{from_currency} is not supported")
        if self.check_physical_currency(to_currency) is False:
            raise ValueError(f"{to_currency} is supported")
        url = f"{self.URL_BASE}/query?function=CURRENCY_EXCHANGE_RATE&from_currency={from_currency}&to_currentcy={to_currency}&apikey={self.api_key}"
        return requests.request("GET", url)

    @API_BASE.response_handler
    def get_interday_rates(self, from_symbol, to_symbol, interval, output_size="full"):
        if self.check_physical_currency(from_symbol) is False:
            raise ValueError(f"{from_symbol} is not supported")
        if self.check_physical_currency(to_symbol) is False:
            raise ValueError(f"{to_symbol} is supported")
        if interval not in self.available_frame:
            raise ValueError(f"{interval} is not supported")
        interval = self.available_frame[interval]
        correct, size = self.check_outputsize(output_size)
        if correct is False:
            self.logger.warn("outsize should be either full or compact")
        url = f"{self.URL_BASE}/query?function=FX_INTRADAY&from_symbol={from_symbol}&to_symbol={to_symbol}&interval={interval}&outputsize={size}&apikey={self.api_key}"
        return requests.request("GET", url)

    @API_BASE.response_handler
    def get_daily_rates(self, from_symbol, to_symbol, output_size="full"):
        if self.check_physical_currency(from_symbol) is False:
            raise ValueError(f"{from_symbol} is not supported")
        if self.check_physical_currency(to_symbol) is False:
            raise ValueError(f"{to_symbol} is supported")
        correct, size = self.check_outputsize(output_size)
        if correct is False:
            self.logger.warn("outsize should be either full or compact")

        url = f"{self.URL_BASE}/query?function=FX_DAILY&from_symbol={from_symbol}&to_symbol={to_symbol}&outputsize={size}&apikey={self.api_key}"
        return requests.request("GET", url)

    @API_BASE.response_handler
    def get_weekly_rates(self, from_symbol, to_symbol):
        if self.check_physical_currency(from_symbol) is False:
            raise ValueError(f"{from_symbol} is not supported")
        if self.check_physical_currency(to_symbol) is False:
            raise ValueError(f"{to_symbol} is supported")

        url = f"{self.URL_BASE}/query?function=FX_WEEKLY&from_symbol={from_symbol}&to_symbol={to_symbol}&apikey={self.api_key}"
        return requests.request("GET", url)

    @API_BASE.response_handler
    def get_monthly_rates(self, from_symbol, to_symbol):
        if self.check_physical_currency(from_symbol) is False:
            raise ValueError(f"{from_symbol} is not supported")
        if self.check_physical_currency(to_symbol) is False:
            raise ValueError(f"{to_symbol} is supported")

        url = f"{self.URL_BASE}/query?function=FX_MONTHLY&from_symbol={from_symbol}&to_symbol={to_symbol}&apikey={self.api_key}"
        return requests.request("GET", url)
