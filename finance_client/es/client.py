import logging

from finance_client.client_base import ClientBase

logger = logging.getLogger(__name__)


class ESClient(ClientBase):
    def __init__(self, auto_index=False, frame=5, symbol="USDJPY", post_process=[], budget=1000000, seed=1017):
        super().__init__(budget=budget, symbols=symbol, frame=frame, provider="vantage", post_processes=post_process)
        self.debug = False
        self.SYMBOL = symbol
        self.frame = frame
        self.auto_index = auto_index

    def get_current_ask(self, symbols: list = None):
        pass

    def get_current_bid(self, symbols: list = None):
        pass

    def get_current_spread(self):
        pass

    def _market_sell(self, symbol, price, amount, tp=None, sl=None):
        return False, None

    def _buy_to_close(self, symbol, price, amount, option, result):
        return True

    def _market_buy(self, symbol, price, amount, tp=None, sl=None):
        return False, None

    def buy_order(self, value, tp=None, sl=None):
        pass

    def sell_order(self, value, tp=None, sl=None):
        pass

    def update_order(self, _type, _id, value, tp, sl):
        print("NOT IMPLEMENTED")

    def _sell_to_close(self, symbol, price, amount, option, result):
        return True

    def get_rates(self, interval):
        pass

    def cancel_order(self, order):
        pass
