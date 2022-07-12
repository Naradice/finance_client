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

currency_code_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "./resources/digital_currency_list.csv"))
phys_currency_code_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "./resources/physical_currency_list.csv"))

digital_code_list = None
physical_code_list = None

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

def check_digital_currency(currency_code):
    global digital_code_list
    if digital_code_list is None:
        if os.path.exists(currency_code_file_path):
            digital_code_list = pd.read_csv(currency_code_file_path, index_col="currency code")
            print("digital_code_list is loaded")
        else:
            #raise FileNotFoundError("can't check")
            print("cant check due to faile is missing. pass anyway.")
            return True
    #name = dc.loc[currency code]["currency name"]
    return currency_code in digital_code_list.index
def check_physical_currency(currency_code):
    global physical_code_list
    if physical_code_list is None:
        if os.path.exists(phys_currency_code_file_path):
            physical_code_list = pd.read_csv(phys_currency_code_file_path, index_col="currency code")
            print("physical_code_list is loaded")
            #name = dc.loc[currency code]["currency name"]
        else:
            print("cant check due to faile is missing. pass anyway.")
            return True

    return currency_code in physical_code_list.index
    
def check_currency(currency_code):
    exists_in_digital = check_digital_currency(currency_code)
    exists_in_physical = check_physical_currency(currency_code)
    return exists_in_digital or exists_in_physical