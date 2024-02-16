import datetime
import json
import os
import sys
import time
import unittest

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(module_path)
from finance_client import db


class TestLogRatingWithFile(unittest.TestCase):
    rating_file_path = os.path.join(os.getcwd(), "ratings_test.json")

    def test_add_a_rating(self):
        key = "test"
        symbol = "test_symbol"
        source = "test_company"
        org_ratings = [5.0, 3.0, 0.0, 0.0, 1.0]
        new_rating_info = ",".join([str(value) for value in org_ratings])

        try:
            os.remove(self.rating_file_path)
        except Exception:
            pass
        storage = db.FileStorage(key, save_period=0, rating_log_path=self.rating_file_path)
        storage.store_symbol_info(symbol=symbol, rating=new_rating_info, source=source)
        with open(self.rating_file_path) as fp:
            data = json.load(fp)
        self.assertTrue(symbol in data)
        stored_info = data[symbol][-1]
        stored_rating_info = stored_info[0]
        ratings = [float(rate) for rate in stored_rating_info.split(",")]
        self.assertEqual(len(ratings), len(org_ratings))
        self.assertListEqual(ratings, org_ratings)

    def test_add_ratings(self):
        key = "test"
        symbol = "test_symbol"
        source = "test_company"
        market = "fx"
        org_ratings = [5.0, 3.0, 0.0, 0.0, 1.0]
        new_rating_info = ",".join([str(value) for value in org_ratings])

        try:
            os.remove(self.rating_file_path)
        except Exception:
            pass
        storage = db.FileStorage(key, save_period=0, rating_log_path=self.rating_file_path)
        info = [(f"{symbol}_{index}", new_rating_info, None, source, market) for index in range(10)]
        storage.store_symbols_info(info)
        with open(self.rating_file_path) as fp:
            data = json.load(fp)
        test_symbol = f"{symbol}_3"
        self.assertTrue(test_symbol in data)
        stored_info = data[test_symbol][-1]
        stored_rating_info = stored_info[0]
        ratings = [float(rate) for rate in stored_rating_info.split(",")]
        self.assertEqual(len(ratings), len(org_ratings))
        self.assertListEqual(ratings, org_ratings)

    def test_get_a_rating(self):
        key = "test"
        symbol = "test_symbol"
        source = "test_company"
        market = "fx"

        def create_rate(value):
            org_ratings = [value, 3.0, 0.0, 0.0, 1.0]
            new_rating_info = ",".join([str(value) for value in org_ratings])
            return new_rating_info

        try:
            os.remove(self.rating_file_path)
        except Exception:
            pass
        storage = db.FileStorage(key, save_period=0, rating_log_path=self.rating_file_path)
        info = [[symbol, create_rate(index), None, source, market] for index in range(10)]

        storage.store_symbols_info(info)
        stored_info = storage.get_symbol_info(symbol, source)
        self.assertListEqual(info[-1][1:-1], stored_info)

    @classmethod
    def tearDownClass(self, retry=0):
        try:
            os.remove(self.rating_file_path)
        except PermissionError:
            if retry < 3:
                time.sleep(3)
                self.tearDownClass(retry=retry + 1)


class TestLogRatingWithSQLite(unittest.TestCase):
    db_file_path = os.path.join(os.getcwd(), "ratings_test.db")
    key = "test"
    storage = db.SQLiteStorage(db_file_path, key)

    def test_01_add_a_rating(self):
        symbol = "test_symbol"
        source = "test_company"
        org_ratings = [5.0, 3.0, 0.0, 0.0, 1.0]
        new_rating_info = ",".join([str(value) for value in org_ratings])

        self.storage.store_symbol_info(symbol=symbol, rating=new_rating_info, source=source)

    def test_02_get_a_rating(self):
        symbol = "test_symbol"
        source = "test_company"
        org_ratings = [5.0, 3.0, 0.0, 0.0, 1.0]
        new_rating_info = ",".join([str(value) for value in org_ratings])
        rating_record = self.storage.get_symbol_info(symbol, source)
        self.assertEqual(len(rating_record), 3)
        self.assertEqual(new_rating_info, rating_record[0])

    def test_03_check_a_new_one(self):
        symbol = "test_symbol"
        source = "test_company"
        org_ratings = [4.0, 4.0, 0.0, 0.0, 1.0]
        new_rating_info = ",".join([str(value) for value in org_ratings])
        date = datetime.datetime.utcnow() + datetime.timedelta(days=1)
        date = date.date()

        self.storage.store_symbol_info(symbol=symbol, date=date, rating=new_rating_info, source=source)
        rating_record = self.storage.get_symbol_info(symbol, source)
        self.assertEqual(new_rating_info, rating_record[0])

    @classmethod
    def tearDownClass(self, retry=0):
        try:
            os.remove(self.db_file_path)
        except PermissionError:
            if retry < 3:
                time.sleep(3)
                self.tearDownClass(retry=retry + 1)


if __name__ == "__main__":
    unittest.main()
