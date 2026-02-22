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

from finance_client import db
from finance_client import frames as Frame
from finance_client.mt5 import MT5Client

id = int(os.environ["mt5_id"])
simulation = False
tgt_symbol = "USDJPY"
test_db_name = "finance_test.db"


def delete_db():
    if os.path.exists(test_db_name):
        os.remove(test_db_name)


class TestMT5Client(unittest.TestCase):
    @classmethod
    def setUpClass(self) -> None:
        symbol = [tgt_symbol]
        storage = db.PositionSQLiteStorage(test_db_name, "mt5")
        self.client = MT5Client(
            id=id,
            password=os.environ["mt5_password"],
            server=os.environ["mt5_server"],
            symbols=symbol,
            auto_index=False,
            simulation=simulation,
            frame=Frame.MIN30,
            storage=storage,
        )

    def test_01_buy(self):
        self.client.open_trade(is_buy=False, amount=1, symbol=tgt_symbol)

    def test_02_close_long(self):
        results = self.client.close_all_positions(tgt_symbol)
        self.assertEqual(len(results), 1, "please check if market opens")
        self.assertEqual(len(results[0]), 5)
        self.assertTrue(results[0][4])

    @classmethod
    def tearDownClass(self) -> None:
        self.client.close_client()
        delete_db()


if __name__ == "__main__":
    delete_db()
    unittest.main()
