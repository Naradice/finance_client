import json
import os
import sys
import time
import unittest

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(module_path)
from finance_client import db
from finance_client.position import POSITION_TYPE, Position
from finance_client.wallet import Manager


class TestPositionWithFile(unittest.TestCase):
    position_file_path = os.path.join(os.getcwd(), "positions_test.json")

    def test_open_a_position(self):
        key = "test"
        storage = db.FileStorage(key, save_period=0, positions_path=self.position_file_path)
        manager = Manager(10000, storage=storage)
        long_positions, short_positions = manager.storage.get_positions()
        pre_positions_count = len(long_positions) + len(short_positions)
        manager.open_position(POSITION_TYPE.long, "test", 100.0, 1)
        manager.open_position(POSITION_TYPE.short, "test", 100.0, 1)
        long_positions, short_positions = manager.storage.get_positions()
        positions_count = len(long_positions) + len(short_positions)
        self.assertGreater(positions_count, pre_positions_count)
        with open(self.position_file_path, mode="r") as fp:
            positions = json.load(fp)
        file_positions_count = len(positions[key][POSITION_TYPE.long.name]) + len(positions[key][POSITION_TYPE.short.name])
        # save period is 0. So it should be refrected
        self.assertEqual(positions_count, file_positions_count)

    def test_close_a_position(self):
        key = "close_test"
        storage = db.FileStorage(key, save_period=0, positions_path=self.position_file_path)
        manager = Manager(10000, storage=storage)
        id = manager.open_position(POSITION_TYPE.long, "test", 100.0, 1)
        self.assertNotEqual(id, None)
        result = manager.close_position(id, price=101.0)
        for item in result:
            self.assertEqual(type(item), float)

        with open(self.position_file_path, mode="r") as fp:
            positions = json.load(fp)
        file_positions_count = len(positions[key][POSITION_TYPE.long.name]) + len(positions[key][POSITION_TYPE.short.name])
        # default save period is 0. So it should be refrected
        self.assertEqual(0, file_positions_count)

    def test_frequent_trade(self):
        key = "freq_test"
        storage = db.FileStorage(key, save_period=0.1, positions_path=self.position_file_path)
        manager = Manager(10000, storage=storage)
        ids = []
        for i in range(30):
            id = manager.open_position(POSITION_TYPE.long, "symbol", 100.0, 1)
            ids.append(id)
        time.sleep(10)
        with open(self.position_file_path, mode="r") as fp:
            positions = json.load(fp)
        file_positions_count = len(positions[key][POSITION_TYPE.long.name]) + len(positions[key][POSITION_TYPE.short.name])
        self.assertEqual(file_positions_count, 30)
        for id in ids:
            manager.close_position(id, 100)

    @classmethod
    def tearDownClass(self):
        os.remove(self.position_file_path)


class TestPositionWithSQLite(unittest.TestCase):
    db_file_path = os.path.join(os.getcwd(), "positions_test.db")

    def test_open_a_position(self):
        key = "test"
        storage = db.SQLiteStorage(self.db_file_path, key)
        manager = Manager(10000, storage=storage)
        long_positions, short_positions = manager.storage.get_positions()
        pre_positions_count = len(long_positions) + len(short_positions)
        manager.open_position(POSITION_TYPE.long, "test", 100.0, 1)
        manager.open_position(POSITION_TYPE.short, "test", 100.0, 1)
        long_positions, short_positions = manager.storage.get_positions()
        positions_count = len(long_positions) + len(short_positions)
        self.assertGreater(positions_count, pre_positions_count)

    def test_close_a_position(self):
        key = "close_test"
        storage = db.SQLiteStorage(self.db_file_path, key)
        manager = Manager(10000, storage=storage)
        id = manager.open_position(POSITION_TYPE.long, "test", 100.0, 1)
        self.assertNotEqual(id, None)
        result = manager.close_position(id, price=101.0)
        for item in result:
            self.assertEqual(type(item), float)

    def test_frequent_trade(self):
        key = "freq_test"
        storage = db.SQLiteStorage(self.db_file_path, key)
        manager = Manager(10000, storage=storage)
        ids = []
        for i in range(30):
            id = manager.open_position(POSITION_TYPE.long, "symbol", 100.0, 1)
            ids.append(id)
        long_positions, short_positions = manager.storage.get_positions()
        positions_count = len(long_positions) + len(short_positions)
        self.assertEqual(positions_count, 30)
        for id in ids:
            manager.close_position(id, 100)

    def get_position(self):
        key = "test"
        storage = db.SQLiteStorage(self.db_file_path, key)
        manager = Manager(10000, storage=storage)
        id = manager.open_position(POSITION_TYPE.long, "test", 100.0, 1)
        position = manager.storage.get_position(id)
        self.assertTrue(isinstance(position, Position))

    def get_positions(self):
        key = "test"
        storage = db.SQLiteStorage(self.db_file_path, key)
        manager = Manager(10000, storage=storage)
        id = manager.open_position(POSITION_TYPE.long, "test", 100.0, 1)
        id = manager.open_position(POSITION_TYPE.long, "test", 100.0, 1)
        id = manager.open_position(POSITION_TYPE.long, "test", 100.0, 1)
        id = manager.open_position(POSITION_TYPE.short, "test", 100.0, 1)
        id = manager.open_position(POSITION_TYPE.short, "test", 100.0, 1)
        long_positions, short_positions = manager.storage.get_positions()
        self.assertGreaterEqual(len(long_positions), 3)
        self.assertGreaterEqual(len(short_positions), 2)

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
