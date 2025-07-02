import datetime
import logging
import os

import pandas as pd

import finance_client.frames as Frame
from finance_client.client_base import ClientBase
from finance_client.yfinance.client import YahooClient

try:
    from .sbi_util.sbi_util.rpa import STOCK
except ImportError:
    from .sbi_util.rpa import STOCK

logger = logging.getLogger(__name__)


class SBIClient(ClientBase):
    kinds = "sbi"
    ID_KEY = "sbi_id"
    PASS_KEY = "sbi_password"
    TRADE_PASS_KEY = "sbi_trade_password"
    __db_source_key = "sbi"

    def __init__(
        self,
        symbols: list = None,
        id: str = None,
        password: str = None,
        trade_password: str = None,
        use_yfinance=True,
        auto_step_index=True,
        adjust_close=True,
        start_index=0,
        frame: int = Frame.D1,
        provider="Default",
        storage=None,
        do_render=False,
        enable_trade_log=False,
    ):
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
        self.client = None
        if use_yfinance:
            self.client = YahooClient(symbols, auto_step_index=auto_step_index, frame=frame, adjust_close=adjust_close, start_index=start_index)
        else:
            print("get_rate is not available if you specify use_yfinance=False")
        super().__init__(
            budget=budget,
            provider=provider,
            frame=frame,
            do_render=do_render,
            storage=storage,
            enable_trade_log=enable_trade_log,
        )

    def get_additional_params(self):
        return {}

    def _get_ohlc_from_client(self, length: int = None, symbols: list = [], frame: int = None, columns=None, index=None, grouped_by_symbol=True):
        if self.client:
            return self.client._get_ohlc_from_client(length, symbols, frame, columns, index, grouped_by_symbol)
        else:
            return None

    def get_future_rates(self, interval, symbols=[]) -> pd.DataFrame:
        if self.client:
            return self.client.get_future_rates(interval, symbols=symbols)
        else:
            return None

    def get_current_ask(self, symbols: list = None) -> float:
        if self.client:
            return self.client.get_current_ask(symbols)
        else:
            return None

    def get_current_bid(self, symbols: list = None) -> float:
        if self.client:
            return self.client.get_current_bid(symbols)
        else:
            return None

    def _market_buy(self, symbol, ask_rate=None, amount=1, tp=None, sl=None, option_info=None):
        suc = self.rpa_client.buy_order(symbol, amount, ask_rate)
        if suc:
            return True, None
        else:
            return False, None

    def _market_sell(self, symbol, bid_rate, amount, tp, sl, option_info):
        err_msg = "market_sell is not implemented"
        logger.info(err_msg)
        return False, err_msg

    def _buy_for_settlement(self, symbol, ask_rate, amount, option_info, result):
        return False

    def _sell_for_settlment(self, symbol, bid_rate, amount, option_info, result):
        result, err = self.rpa_client.sell_to_close_buy_order(symbol, amount, bid_rate)
        return result

    def get_params(self) -> dict:
        print("Need to implement get_params")

    # defined by the actual client for dataset or env
    def close_client(self):
        self.rpa_client.driver.quit()
        super().close_client()

    def get_next_tick(self, frame=5):
        print("Need to implement get_next_tick")

    def reset(self, mode=None):
        print("Need to implement reset")

    def __convert_to_store(self, rating_dict: dict):
        db_items = []
        date = datetime.datetime.now(datetime.UTC).date()
        for symbol, rate_dict in rating_dict.items():
            if len(rate_dict) > 0:
                rates = [rate_dict[5], rate_dict[4], rate_dict[3], rate_dict[2], rate_dict[1]]
                rate_str = ",".join([str(value) for value in rates])
                item = [symbol, rate_str, date, self.__db_source_key]
                db_items.append(item)
        return db_items

    def __convert_to_rate(self, record):
        rate_str = record[0]
        date_str = record[1]
        source = record[2]

        if rate_str is None:
            return None, None, None

        try:
            date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            logger.exception(f"failed to convered date of stored rate: {date_str}")
            return None, None, None

        rates = rate_str.split(",")
        rate_dict = {}
        for index, rank in enumerate(range(len(rates), 0, -1)):
            rate_dict[rank] = float(rates[index])
        return rate_dict, date, source

    def get_rating(self, symbols):
        if isinstance(symbols, str):
            symbols = [symbols]
        existing_rate = {}
        today = datetime.datetime.now(datetime.UTC).date()
        if self.wallet.storage is not None:
            __symbols = symbols.copy()
            symbols = []
            for symbol in __symbols:
                record = self.wallet.storage.get_symbol_info(symbol, self.__db_source_key)
                rate, date, _ = self.__convert_to_rate(record)
                if date is not None and rate is not None:
                    if date < today:
                        symbols.append(symbol)
                    else:
                        existing_rate[symbol] = rate
                else:
                    symbols.append(symbol)

        ratings_dict = self.rpa_client.get_ratings(symbols)
        if self.wallet.storage is not None:
            items = self.__convert_to_store(ratings_dict)
            self.wallet.storage.store_symbols_info(items)
        ratings_dict.update(existing_rate)
        ratings_df = pd.DataFrame.from_dict(ratings_dict)
        del ratings_dict
        reviwer_number = ratings_df.sum()
        point_amounts = ratings_df.T * ratings_df.index
        point_amount = point_amounts.T.sum()
        mean = point_amount / reviwer_number
        var = point_amounts.T - mean
        var = var.pow(2).sum() / reviwer_number
        ratings_df = pd.concat([mean, var, point_amount], keys=["mean", "var", "amount"], axis=1)
        return ratings_df

    @property
    def max(self):
        print("Need to implement max")
        return 1

    @property
    def min(self):
        print("Need to implement min")
        return -1

    def __len__(self):
        if self.client is None:
            return 0
        else:
            return len(self.client)
