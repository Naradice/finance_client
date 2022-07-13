import pandas as pd
import os
import finance_client.frames as Frame
from finance_client.vantage.apis import STOCK, FOREX, DIGITAL


available_frame = {
    Frame.MIN1:"1min", Frame.MIN5:"5min", 15:"15min", Frame.MIN30:"30min", Frame.H1: "60min",
    Frame.D1:"DAILY",
    Frame.W1:"Weekly",
    Frame.MO1: "Monthly"
}

class __Target:
    
    def __init__(self, id:int, name) -> None:
        self.id = id
        self.base_name = name
    
    # TODO: reduce  if else
    def create_client(self, api_key, logger=None):
        if self.id == 0:
            return STOCK(api_key, logger)
        elif self.id == 1:
            return FOREX(api_key, logger)
        elif self.id == 2:
            return DIGITAL(api_key, logger)
        else:
            raise ValueError(f"{self.base_name} is not supported yet.")
        
    def to_function_name(self, target, frame):
        if type(frame) is int:
            global available_frame
            if frame in available_frame:
                function_name = ""
                if self.id == 0 or self.id == 1:
                    if frame  <= Frame.H1:
                        function_name = target.base_name + "_INTRADAY"
                    elif frame == Frame.D1:
                        function_name = target.base_name + "_DAILY"
                    elif frame == Frame.W1:
                        function_name = target.base_name + "_WEEKLY"
                    elif frame == Frame.MO1:
                        function_name = target.base_name + "_MONTHLY"
                elif self.id == 2:
                    if frame  <= Frame.H1:
                        function_name = target.base_name + "_INTRADAY"
                    elif frame == Frame.D1:
                        function_name = "DIGITAL_CURRENCY_DAILY"
                    elif frame == Frame.W1:
                        function_name = "DIGITAL_CURRENCY_WEEKLY"
                    elif frame == Frame.MO1:
                        function_name = "DIGITAL_CURRENCY_MONTHLY"
                else:
                    function_name = target.base_name
                return function_name                    
        else:
            raise TypeError("frame should be specified with finance_client.frames.frame")

STOCK = __Target(0, "TIME_SERIES")
FX = __Target(1, "FX")
CRYPTO_CURRENCY = __Target(2, "CRYPTO")
INCOME_STATEMENT = __Target(3, "INCOME_STATEMENT")
BALANCE_SHEET = __Target(4, "BALANCE_SHEET")
CASH_FLOW = __Target(5, "CASH_FLOW")
EARNINGS = __Target(6, "EARNINGS")
COM_OVERVIEW = __Target(7, "OVERVIEW")
LISTING_STATUS = __Target(8, "LISTING_STATUS")
EARNINGS_CALENDAR = __Target(9, "EARNINGS_CALENDAR")
IPO_CALENDAR = __Target(10, "IPO_CALENDAR")
REAL_GDP = __Target(11, "REAL_GDP")
REAL_GDP_PER_CAPITA = __Target(12, "REAL_GDP_PER_CAPITA")
TREASURY_YIELD = __Target(13, "TREASURY_YIELD")
FEDERAL_FUNDS_RATE = __Target(14, "FEDERAL_FUNDS_RATE")
CPI = __Target(15, "CPI")
INFLATION = __Target(16, "INFLATION")
INFLATION_EXPECTATION = __Target(17, "INFLATION_EXPECTATION")
CONSUMER_SENTIMENT = __Target(18, "CONSUMER_SENTIMENT")
RETAIL_SALES = __Target(19, "RETAIL_SALES")
DURABLES_GOODS_ORDER = __Target(20, "DURABLES")
UNEMPLOYMENT_RATE = __Target(21, "UNEMPLOYMENT")
NONFARM_PAYROLL = __Target(22, "NONFARM_PAYROLL")