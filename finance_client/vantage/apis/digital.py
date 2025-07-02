import logging

import requests

from .base import API_BASE

logger = logging.getLogger(__name__)


class DIGITAL(API_BASE):
    def __init__(self, api_key) -> None:
        super().__init__(api_key)
        self.work_day_in_week = 7

    @API_BASE.response_handler
    def get_exchange_rates(self, from_currency, to_currency):
        if self.check_physical_currency(from_currency) is False:
            raise ValueError(f"{from_currency} is not supported")
        if self.check_physical_currency(to_currency) is False:
            raise ValueError(f"{to_currency} is supported")
        url = f"{self.URL_BASE}/query?function=CURRENCY_EXCHANGE_RATE&from_currency={from_currency}&to_currentcy={to_currency}&apikey={self.api_key}"
        return requests.request("GET", url)

    @API_BASE.response_handler
    def get_interday_rates(self, symbol, market, interval, output_size="full"):
        if self.check_digital_currency(symbol) is False:
            raise ValueError(f"{symbol} is not supported")
        if self.check_physical_currency(market) is False:
            raise ValueError(f"{market} is supported")
        if interval not in self.available_frame:
            raise ValueError(f"{interval} is not supported")
        interval = self.available_frame[interval]
        correct, size = self.check_outputsize(output_size)
        if correct is False:
            logger.warn("outsize should be either full or compact")
        url = f"{self.URL_BASE}/query?function=CRYPTO_INTRADAY&symbol={symbol}&market={market}&interval={interval}&outputsize={size}&apikey={self.api_key}"
        return requests.request("GET", url)

    @API_BASE.response_handler
    def get_daily_rates(self, symbol, market, output_size="full"):
        if self.check_digital_currency(symbol) is False:
            raise ValueError(f"{symbol} is not supported")
        if self.check_physical_currency(market) is False:
            raise ValueError(f"{market} is supported")
        correct, size = self.check_outputsize(output_size)
        if correct is False:
            logger.warn("outsize should be either full or compact")

        url = f"{self.URL_BASE}/query?function=DIGITAL_CURRENCY_DAILY&symbol={symbol}&market={market}&outputsize={size}&apikey={self.api_key}"
        return requests.request("GET", url)

    @API_BASE.response_handler
    def get_weekly_rates(self, symbol, market):
        if self.check_digital_currency(symbol) is False:
            raise ValueError(f"{symbol} is not supported")
        if self.check_physical_currency(market) is False:
            raise ValueError(f"{market} is supported")

        url = f"{self.URL_BASE}/query?function=DIGITAL_CURRENCY_WEEKLY&symbol={symbol}&market={market}&apikey={self.api_key}"
        return requests.request("GET", url)

    @API_BASE.response_handler
    def get_monthly_rates(self, symbol, market):
        if self.check_physical_currency(symbol) is False:
            raise ValueError(f"{symbol} is not supported")
        if self.check_physical_currency(market) is False:
            raise ValueError(f"{market} is supported")

        url = f"{self.URL_BASE}/query?function=DIGITAL_CURRENCY_MONTHLY&symbol={symbol}&market={market}&apikey={self.api_key}"
        return requests.request("GET", url)
