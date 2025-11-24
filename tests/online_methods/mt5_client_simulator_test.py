import os
import sys
import unittest

import dotenv

try:
    dotenv.load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"))
except Exception as e:
    raise e
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(module_path)

from finance_client import ORDER_TYPE, db
from finance_client import frames as Frame
from finance_client.mt5 import MT5Client

id = int(os.environ["mt5_id"])
simulation = True
tgt_symbol = "USDJPY"
test_db_name = "finance_test.db"


def delete_db():
    if os.path.exists(test_db_name):
        os.remove(test_db_name)


class TestMT5ClientWithSQL(unittest.TestCase):
    @classmethod
    def setUpClass(self) -> None:
        storage = db.SQLiteStorage(test_db_name, "mt5", username="test_user")
        self.client = MT5Client(
            id=id,
            password=os.environ["mt5_password"],
            server=os.environ["mt5_server"],
            auto_index=False,
            simulation=simulation,
            frame=Frame.MIN30,
            storage=storage,
        )

    def test_01_buy_close(self):
        # amount should be float. it is auto converted to trade unit in open_trade
        suc, position = self.client.open_trade(is_buy=False, amount=1, symbol=tgt_symbol)
        self.assertTrue(suc, "please check if market opens")
        self.assertIsNotNone(position)

        result = self.client.close_position(position=position)
        self.assertTrue(not result.error, "please check if market opens")

    def test_02_sell_close(self):
        suc, position = self.client.open_trade(is_buy=True, amount=1, symbol=tgt_symbol)
        self.assertTrue(suc, "please check if market opens")
        self.assertIsNotNone(position)

        result = self.client.close_position(position=position)
        self.assertTrue(not result.error, "please check if market opens")

    def test_03_update_position(self):
        suc, position = self.client.open_trade(is_buy=True, amount=1, symbol=tgt_symbol)
        self.assertTrue(suc, "please check if market opens")
        self.assertIsNotNone(position)

        suc = self.client.update_position(position=position, tp=150.0, sl=50.0)
        self.assertTrue(suc, "update position failed")

        result = self.client.close_position(position=position)
        self.assertTrue(not result.error, "please check if market opens")

    def test_04_order(self):
        suc, position = self.client.open_trade(is_buy=True, amount=1, symbol=tgt_symbol, price=155.0, tp=160.0, sl=50.0, order_type=ORDER_TYPE.limit)
        self.assertTrue(suc, "order placement failed")
        self.assertIsNotNone(position)
        suc = self.client.cancel_order(position=position)
        self.assertTrue(suc, "canceling order failed")


    def test_05_order_cancel_invalid(self):
        suc = self.client.cancel_order(position=99999999)
        self.assertTrue(not suc, "canceling invalid order should fail")

    @classmethod
    def tearDownClass(self) -> None:
        self.client.close_client()
        delete_db()


if __name__ == "__main__":
    delete_db()
    unittest.main()
