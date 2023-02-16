import datetime
import json
import os
import sys
import unittest

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
print(module_path)
sys.path.append(module_path)
import time

import finance_client.coincheck.apis as apis
from finance_client.coincheck.apis.ws import Orders, TradeHistory

apis.ServiceBase()


class TestCCAPI(unittest.TestCase):
    def log(self, data):
        print(f"unit_test: {data}")

    def test_subscribe(self):
        ws_th = TradeHistory(debug=True)
        ws_th.subscribe(on_tick=self.log)
        time.sleep(60)
        ws_th.close()

    def test_orders_subscribe(self):
        ws_th = Orders(debug=True)
        ws_th.subscribe()
        time.sleep(60)
        ws_th.close()

    def test_get_ticker(self):
        ticker = apis.Ticker()
        tick = ticker.get()
        self.assertEqual(type(tick), dict)
        self.assertTrue("bid" in tick)
        self.assertEqual(type(tick["bid"]), float)

    def test_buy_pending_order(self):
        response = apis.create_pending_buy_order(rate=2930000, amount=0.01, stop_loss_rate=2900000)
        self.assertTrue(response["success"])

    def test_sell_pending_order(self):
        response = apis.create_pending_sell_order(rate=2920000, amount=0.01)
        # sample response: {'success': True, 'id': 4914185035, 'amount': '0.01', 'rate': '2920000.0', 'order_type': 'sell', 'pair': 'btc_jpy', 'created_at': '2022-08-20T15:47:18.000Z', 'market_buy_amount': None, 'stop_loss_rate': None}
        self.assertTrue(response["success"])

    def test_market_buy(self):
        response = apis.create_market_buy_order(amount=20000, stop_loss_rate=2912000)
        # sample response: {'success': True, 'id': 4914201508, 'amount': None, 'rate': None, 'order_type': 'market_buy', 'pair': 'btc_jpy', 'created_at': '2022-08-20T15:55:34.000Z', 'market_buy_amount': '20000.0', 'stop_loss_rate': '2912000.0'}
        self.assertTrue(response["success"])

    def test_market_sell(self):
        response = apis.create_market_sell_order(amount=0.00684241)
        self.assertTrue(response["success"])

    def test_get_my_orders(self):
        response = apis.get_my_trade_history()
        self.assertTrue(response["success"])

    def test_get_pending_orders(self):
        response = apis.get_pending_orders()
        self.assertTrue(response["success"])


if __name__ == "__main__":
    unittest.main()
