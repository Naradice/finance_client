import logging

from finance_client.client_base import ClientBase

logger = logging.getLogger(__name__)


class ESClient(ClientBase):
    def __init__(self, auto_index=False, frame=5, symbol="USDJPY", post_process=[], free_mergin=1000000, seed=1017, user_name=None):
        """_summary_

        Args:
            auto_index (bool, optional): _description_. Defaults to False.
            frame (int, optional): _description_. Defaults to 5.
            symbol (str, optional): _description_. Defaults to "USDJPY".
            post_process (list, optional): _description_. Defaults to [].
            free_mergin (int, optional): _description_. Defaults to 1000000.
            seed (int, optional): _description_. Defaults to 1017.
            user_name (str, optional): user name to separate info (e.g. position) within the same provider. Defaults to None. It means client doesn't care users.
        """
        super().__init__(free_mergin=free_mergin, symbols=symbol, frame=frame, provider="vantage", post_processes=post_process, user_name=None)
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

    def _market_sell(self, symbol, price, volume, tp=None, sl=None):
        return False, None

    def _buy_to_close(self, symbol, price, volume, option, result):
        return True

    def _market_buy(self, symbol, price, volume, tp=None, sl=None):
        return False, None

    def buy_order(self, value, tp=None, sl=None):
        pass

    def sell_order(self, value, tp=None, sl=None):
        pass

    def update_order(self, _type, _id, value, tp, sl):
        print("NOT IMPLEMENTED")

    def _sell_to_close(self, symbol, price, volume, option, result):
        return True

    def get_rates(self, interval):
        pass

    def cancel_order(self, order):
        pass
