from finance_client.client_base import Client


class ESClient(Client):
    def __init__(self, auto_index=False, frame=5, symbol="USDJPY", post_process=[], budget=1000000, logger=None, seed=1017):
        super().__init__(budget=budget, frame=frame, provider="vantage", post_processes=post_process, logger=logger)
        self.debug = False
        self.SYMBOL = symbol
        self.frame = frame
        self.auto_index = auto_index

    def get_current_ask(self):
        pass

    def get_current_bid(self):
        pass

    def get_current_spread(self):
        pass

    def _market_sell(self, symbol, price, amount, tp=None, sl=None):
        return False, None

    def _buy_for_settlement(self, symbol, price, amount, option, result):
        pass

    def _market_buy(self, symbol, price, amount, tp=None, sl=None):
        return False, None

    def buy_order(self, value, tp=None, sl=None):
        pass

    def sell_order(self, value, tp=None, sl=None):
        pass

    def update_order(self, _type, _id, value, tp, sl):
        print("NOT IMPLEMENTED")

    def _sell_for_settlment(self, symbol, price, amount, option, result):
        pass

    def get_rates(self, interval):
        pass

    def cancel_order(self, order):
        pass
