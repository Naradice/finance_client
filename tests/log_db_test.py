import os
import unittest

import pandas as pd

from finance_client import POSITION_SIDE, Position, db


def prepare_old_log_sql():
    test_log_path = os.path.join(os.path.dirname(__file__), "finance_trade_old_log_to_test.db")
    if os.path.exists(test_log_path):
        os.remove(test_log_path)
    storage = db.LogSQLiteStorage(provider="Default", database_path=test_log_path)
    provider = storage.provider
    user = storage.username

    position1 = Position(
        position_side=POSITION_SIDE.long,
        symbol="BTCUSD",
        trade_unit=1,
        leverage=1.0,
        price=50000.0,
        volume=0.1,
        tp=55000.0,
        sl=45000.0,
        time_index=pd.Timestamp("2024-01-03"),
        id="12345678-abcd-1234-abcd-123456789aaa",
    )
    position2 = Position(
        position_side=POSITION_SIDE.short,
        symbol="ETHUSD",
        trade_unit=1,
        leverage=1.0,
        price=4000.0,
        volume=1.0,
        tp=3500.0,
        sl=4500.0,
        time_index=pd.Timestamp("2024-01-04"),
        id="12345678-abcd-1234-abcd-123456789bbb",
    )
    storage.store_log(position1, order_type=1)
    storage.store_log(position1, order_type=-1)
    storage.store_log(position2, order_type=1)
    storage.close()

class TestDB(unittest.TestCase):

    def test_get_log(self):
        if os.path.exists("test_logs.csv"):
            os.remove("test_logs.csv")
        storage = db.LogCSVStorage(provider="Default", trade_log_path="test_logs.csv")
        provider = storage.provider
        user = storage.username

        position = Position(
            position_side=POSITION_SIDE.long,
            symbol="BTCUSD",
            trade_unit=1,
            leverage=1.0,
            price=50000.0,
            volume=0.1,
            tp=55000.0,
            sl=45000.0,
            time_index=pd.Timestamp("2024-01-03"),
        )
        storage.store_log(position, order_type=1)
        storage.store_log(position, order_type=-1)

        retrieved_logs = storage.get_logs(provider, user, start="2024-01-01", end="2024-01-04")
        self.assertEqual(len(retrieved_logs), 2)
    
    def test_get_old_log(self):
        test_log_path = os.path.join(os.path.dirname(__file__), "finance_trade_old_log_to_test.csv")
        test_closed_position_id = "12345678-abcd-1234-abcd-123456789aaa"
        test_open_position_id = "12345678-abcd-1234-abcd-123456789bbb"
        provider = "Default"
        user = "__none__"
        storage = db.LogCSVStorage(provider, user, trade_log_path=test_log_path)
        # as there is no log in memory, it should read from the file and return the log with the specified id
        retrieved_log = storage.get_log(provider, user, test_closed_position_id)
        self.assertEqual(len(retrieved_log), 1)
        self.assertEqual(retrieved_log.iloc[0]["position_id"], test_closed_position_id)
        # as the log is now stored in memory, it should return the log from memory without reading the file again
        retrieved_log = storage.get_log(provider, user, test_closed_position_id)
        self.assertEqual(len(retrieved_log), 1)
        self.assertEqual(retrieved_log.iloc[0]["position_id"], test_closed_position_id)
        # the log with the open position id should also be retrievable
        retrieved_log = storage.get_log(provider, user, test_open_position_id)
        self.assertEqual(len(retrieved_log), 1)
        self.assertEqual(retrieved_log.iloc[0]["position_id"], test_open_position_id)
    
    def test_get_open_log_with_id(self):
        test_log_path = os.path.join(os.path.dirname(__file__), "finance_trade_old_log_to_test.csv")
        test_closed_position_id = "12345678-abcd-1234-abcd-123456789aaa"
        provider = "Default"
        user = "__none__"
        storage = db.LogCSVStorage(provider, user, trade_log_path=test_log_path)
        # the log with the open position id should be retrievable with order_type=1
        # closed position has two logs, one with order_type=1 and the other with order_type=-1, but only the log with order_type=1 should be retrievable with order_type=1
        retrieved_log = storage.get_log(provider, user, test_closed_position_id, order_type=1)
        self.assertEqual(len(retrieved_log), 1)
        self.assertEqual(retrieved_log.iloc[0]["position_id"], test_closed_position_id)

    def test_get_profit(self):
        if os.path.exists("test_logs.csv"):
            os.remove("test_logs.csv")
        storage = db.LogCSVStorage(provider="Default", trade_log_path="test_logs.csv")
        provider = storage.provider
        user = storage.username

        position = Position(
            position_side=POSITION_SIDE.long,
            symbol="USDJPY",
            trade_unit=100000,
            leverage=5.0,
            price=150.0,
            volume=0.1,
            tp=151.0,
            sl=149.0,
            time_index=pd.Timestamp("2026-01-03"),
        )
        storage.store_log(position, order_type=1)
        close_position = Position(
            position_side=POSITION_SIDE.long,
            symbol="USDJPY",
            trade_unit=100000,
            leverage=5.0,
            price=151.0,
            volume=0.1,
            tp=151.0,
            sl=149.0,
            time_index=pd.Timestamp("2026-01-04"),
            id=position.id,
        )
        profit = storage._get_profit(close_position)
        expected_profit = (151.0 - 150.0) * 100000 * 0.1 * 5.0
        self.assertEqual(profit, expected_profit)

        storage.store_log(close_position, order_type=-1)
        profit = storage._get_profit(close_position)
        self.assertEqual(profit, expected_profit)

    # sqlite
    def test_sqlite_storage(self):
        test_log_path = os.path.join(os.path.dirname(__file__), "test_logs.db")
        if os.path.exists(test_log_path):
            os.remove(test_log_path)
        storage = db.LogSQLiteStorage(provider="Default", database_path=test_log_path)
        provider = storage.provider
        user = storage.username

        position = Position(
            position_side=POSITION_SIDE.long,
            symbol="BTCUSD",
            trade_unit=1,
            leverage=1.0,
            price=50000.0,
            volume=0.1,
            tp=55000.0,
            sl=45000.0,
            time_index=pd.Timestamp("2024-01-03"),
        )
        storage.store_log(position, order_type=1)
        storage.store_log(position, order_type=-1)

        retrieved_logs = storage.get_logs(provider, user, start="2024-01-01", end="2024-01-04")
        self.assertEqual(len(retrieved_logs), 2)
    
    def test_sqlite_old_log(self):
        test_log_path = os.path.join(os.path.dirname(__file__), "finance_trade_old_log_to_test.db")
        test_closed_position_id = "12345678-abcd-1234-abcd-123456789aaa"
        test_open_position_id = "12345678-abcd-1234-abcd-123456789bbb"
        provider = "Default"
        user = "__none__"
        storage = db.LogSQLiteStorage(test_log_path, provider, user)
        # sqlite has no memory cache, so it should read from the file and return the log with the specified id
        retrieved_log = storage.get_log(provider, user, test_closed_position_id)
        self.assertEqual(len(retrieved_log), 1)
        self.assertEqual(retrieved_log.iloc[0]["position_id"], test_closed_position_id)
        # the log with the open position id should also be retrievable
        retrieved_log = storage.get_log(provider, user, test_open_position_id)
        self.assertEqual(len(retrieved_log), 1)
        self.assertEqual(retrieved_log.iloc[0]["position_id"], test_open_position_id)
        storage.close()
    
    def test_sqlite_get_open_log_with_id(self):
        test_log_path = os.path.join(os.path.dirname(__file__), "finance_trade_old_log_to_test.db")
        test_closed_position_id = "12345678-abcd-1234-abcd-123456789aaa"
        provider = "Default"
        user = "__none__"
        storage = db.LogSQLiteStorage(test_log_path, provider, user)
        # the log with the open position id should be retrievable with order_type=1
        # closed position has two logs, one with order_type=1 and the other with order_type=-1, but only the log with order_type=1 should be retrievable with order_type=1
        retrieved_log = storage.get_log(provider, user, test_closed_position_id, order_type=1)
        self.assertEqual(len(retrieved_log), 1)
        self.assertEqual(retrieved_log.iloc[0]["position_id"], test_closed_position_id)
        storage.close()
    
    def test_sqlite_get_profit(self):
        test_log_path = os.path.join(os.path.dirname(__file__), "test_logs.db")
        if os.path.exists(test_log_path):
            os.remove(test_log_path)
        storage = db.LogSQLiteStorage(provider="Default", database_path=test_log_path)
        provider = storage.provider
        user = storage.username

        position = Position(
            position_side=POSITION_SIDE.long,
            symbol="USDJPY",
            trade_unit=100000,
            leverage=5.0,
            price=150.0,
            volume=0.1,
            tp=151.0,
            sl=149.0,
            time_index=pd.Timestamp("2026-01-03"),
        )
        storage.store_log(position, order_type=1)
        close_position = Position(
            position_side=POSITION_SIDE.long,
            symbol="USDJPY",
            trade_unit=100000,
            leverage=5.0,
            price=151.0,
            volume=0.1,
            tp=151.0,
            sl=149.0,
            time_index=pd.Timestamp("2026-01-04"),
            id=position.id,
        )
        profit = storage._get_profit(close_position)
        expected_profit = (151.0 - 150.0) * 100000 * 0.1 * 5.0
        self.assertEqual(profit, expected_profit)

        storage.store_log(close_position, order_type=-1)
        profit = storage._get_profit(close_position)
        self.assertEqual(profit, expected_profit)

    def tearDown(self):
        base_dir = os.path.dirname(__file__)
        if os.path.exists(os.path.join(base_dir, "test_logs.csv")):
            os.remove(os.path.join(base_dir, "test_logs.csv"))
        if os.path.exists(os.path.join(base_dir, "test_logs.db")):
            os.remove(os.path.join(base_dir, "test_logs.db"))
        super().tearDown()

if __name__ == "__main__":
    unittest.main()