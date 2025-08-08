import os
import sys
import time
import unittest

import dotenv

os.environ["FC_DEBUG"] = "true"
base_path = os.path.dirname(__file__)
try:
    dotenv.load_dotenv(f"{base_path}/../.env")
except Exception:
    pass

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
print(module_path)
sys.path.append(module_path)

from finance_client import db
from finance_client.position import ORDER_TYPE
from finance_client.sbi.client import SBIClient


class TestSBIClient(unittest.TestCase):
    frame = 1
    test_symbol = "7203"
    test_symbol2 = "7202"

    client = SBIClient(use_yfinance=False, storage=None)

    def test_01_orders(self):
        # id = self.client.open_trade(True, 1, ORDER_TYPE.market, self.test_symbol)
        # sleep(10)
        # self.client.close_position(id=id)
        pass

    def test_02_get_rating_wo_db(self):
        ratings = self.client.get_rating([self.test_symbol])
        self.client.close_client()
        self.assertTrue(self.test_symbol in ratings.index)
        self.assertTrue(ratings.loc[self.test_symbol, "mean"] >= 0)

    def test_03_get_rating_w_db(self):
        db_uri = "sbi_test.db"
        storage = db.SQLiteStorage(db_uri, "sbi")
        client = SBIClient(storage=storage, use_yfinance=False)
        start_time = time.time()
        rate = client.get_rating([self.test_symbol])
        end_time = time.time()
        elapsed_time_1 = end_time - start_time

        start_time = time.time()
        store_rate = client.get_rating([self.test_symbol])
        end_time = time.time()
        client.close_client()

        elapsed_time_2 = end_time - start_time
        self.assertGreaterEqual(elapsed_time_1, elapsed_time_2)
        self.assertSequenceEqual(rate["mean"].values, store_rate["mean"].values)

    def test_04_get_ratings(self):
        db_uri = "sbi_test.db"
        storage = db.SQLiteStorage(db_uri, "sbi")
        client = SBIClient(storage=storage, use_yfinance=False)
        rate = client.get_rating([self.test_symbol, self.test_symbol2])
        self.assertEqual(len(rate), 2)
        client.close_client()

    @classmethod
    def tearDownClass(self, retry=0):
        try:
            self.client.close_client()
            os.remove("sbi_test.db")
        except PermissionError:
            if retry < 3:
                time.sleep(3)
                self.tearDownClass(retry=retry + 1)


if __name__ == "__main__":
    unittest.main()
