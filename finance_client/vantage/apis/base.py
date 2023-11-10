import datetime
import json
import os
import time
from logging import config, getLogger

import pandas as pd

import finance_client.frames as Frame


class API_BASE:
    work_day_in_week = 5

    currency_code_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../resources/digital_currency_list.csv"))
    phys_currency_code_file_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../resources/physical_currency_list.csv")
    )

    digital_code_list = None
    physical_code_list = None
    available_frame = {
        Frame.MIN1: "1min",
        Frame.MIN5: "5min",
        15: "15min",
        Frame.MIN30: "30min",
        Frame.H1: "60min",
        Frame.D1: "DAILY",
        Frame.W1: "Weekly",
        Frame.MO1: "Monthly",
    }

    def check_digital_currency(self, currency_code):
        if self.digital_code_list is None:
            if os.path.exists(self.currency_code_file_path):
                self.digital_code_list = pd.read_csv(self.currency_code_file_path, index_col="currency code")
                print("digital_code_list is loaded")
            else:
                # raise FileNotFoundError("can't check")
                print("cant check due to faile is missing. pass anyway.")
                return True
        # name = dc.loc[currency code]["currency name"]
        return currency_code in self.digital_code_list.index

    def check_physical_currency(self, currency_code):
        if self.physical_code_list is None:
            if os.path.exists(self.phys_currency_code_file_path):
                self.physical_code_list = pd.read_csv(self.phys_currency_code_file_path, index_col="currency code")
                print("physical_code_list is loaded")
                # name = dc.loc[currency code]["currency name"]
            else:
                print("cant check due to faile is missing. pass anyway.")
                return True

        return currency_code in self.physical_code_list.index

    def check_currency(self, currency_code):
        exists_in_digital = self.check_digital_currency(currency_code)
        exists_in_physical = self.check_physical_currency(currency_code)
        return exists_in_digital or exists_in_physical

    def __init__(self, api_key, logger_name=None, logger=None) -> None:
        self.URL_BASE = "https://www.alphavantage.co/"
        self.api_key = api_key
        if logger is None:
            try:
                with open(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../settings.json")), "r") as f:
                    settings = json.load(f)
            except Exception as e:
                print(f"fail to load settings file on client: {e}")
                raise e
            logger_config = settings["log"]

            try:
                log_file_base_name = logger_config["handlers"]["fileHandler"]["filename"]
                log_folder = os.path.join(os.path.dirname(__file__), "logs")
                if os.path.exists(log_folder) is False:
                    os.makedirs(log_folder)
                logger_config["handlers"]["fileHandler"][
                    "filename"
                ] = f'{log_folder}/{log_file_base_name}_{datetime.datetime.utcnow().strftime("%Y%m%d")}.log'
                config.dictConfig(logger_config)
            except Exception as e:
                self.logger.error(f"fail to set configure file: {e}")
                raise e
            if logger_name is None:
                logger_name == __name__
            self.logger = getLogger(logger_name)
        else:
            self.logger = logger

    def response_handler(get_rates_function):
        def response_wrapper(*args, retry_count=0, **kwargs):
            self = args[0]
            response = get_rates_function(*args, **kwargs)
            if response.status_code == 200:
                try:
                    res_j = json.loads(response.text)
                except Exception as e:
                    self.logger.error(f"can't parse the response on {get_rates_function.__name__}")
                    return e

                try:
                    error_header = "Error Message"
                    if error_header in res_j:
                        invalid_api_message = "Invalid API call"
                        if invalid_api_message in res_j[error_header]:
                            self.logger.error(f"Invalid API parameters are specified on {get_rates_function.__name__}")
                            self.logger.warn(f"Invalid API parameters: {args} and {kwargs}")
                            return ValueError(res_j[error_header])
                        else:
                            self.logger.error(f"Unkown Error Response on {get_rates_function.__name__}")
                            return Exception(res_j[error_header])
                    else:  # success case
                        return res_j
                except Exception as e:
                    self.logger.error(f"unpext response on {get_rates_function.__name__}")
                    return e
            else:
                retry_count += 1
                if retry_count <= 5:
                    time.sleep(retry_count * 3)
                    return response_wrapper(*args, retry_count=retry_count, **kwargs)
                else:
                    err_txt = f"failed 5 times on {get_rates_function.__name__}: {response.text}"
                    self.logger.error(err_txt)
                    return Exception(err_txt)

        return response_wrapper

    def response_to_dict(self, response):
        try:
            if response.text is not None:
                body = json.loads(response.text)
            else:
                body = json.loads(response)
        except Exception as e:
            print(f"filed parse respone: {e}")
        return body

    def check_outputsize(self, value):
        if value in ["full", "compact"]:
            return True, value
        else:
            return False, "full"
