import datetime
import os
import unittest

from finance_client import db


class TestDB(unittest.TestCase):

    # Log CSV functionalities

    def test_log_csv_init_default(self):
        log_storage = db.LogCSVStorage(provider="test_provider", username="test_user")
        self.assertEqual(log_storage.provider, "test_provider")
        self.assertEqual(log_storage.username, "test_user")

        base_path = os.path.join(os.getcwd(), "logs")
        # expected_trade_log_path = os.path.join(base_path, "finance_trade_log.csv")
        # expected_account_log_path = os.path.join(base_path, "finance_account_history.csv")

        self.assertTrue(os.path.exists(base_path))

    def test_log_csv_init_custom_path(self):
        custom_trade_log_path = "test_log/custom_trade_log.csv"
        custom_account_log_path = "test_log/custom_account_history.csv"
        log_storage = db.LogCSVStorage(
            provider="test_provider", username="test_user", trade_log_path=custom_trade_log_path, account_history_path=custom_account_log_path
        )
        self.assertEqual(log_storage.provider, "test_provider")
        self.assertEqual(log_storage.username, "test_user")
        self.assertTrue(os.path.exists("test_log"))

    def _initialize_default_log_storage(self):
        if os.path.exists("logs/finance_trade_log.csv"):
            try:
                os.remove("logs/finance_trade_log.csv")
            except Exception as e:
                self.fail(f"Failed to remove existing test trade log file: {e}")
        if os.path.exists("logs/finance_account_history.csv"):
            try:
                os.remove("logs/finance_account_history.csv")
            except Exception as e:
                self.fail(f"Failed to remove existing test account history log file: {e}")
        log_storage = db.LogCSVStorage(provider="test_provider", username="test_user")
        return log_storage

    def test_csv_store_log(self):
        log_storage = self._initialize_default_log_storage()
        time_index = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        open_position = db.Position(
            position_side=db.POSITION_SIDE.long,
            symbol="TEST",
            trade_unit=1,
            leverage=1.0,
            price=100.0,
            volume=1.0,
            tp=110.0,
            sl=90.0,
            time_index=time_index,
            option="option_value",
            result=12345,
            id="test_position_id",
        )
        close_time_index = time_index + datetime.timedelta(hours=1)
        close_position = db.Position(
            position_side=db.POSITION_SIDE.long,
            symbol="TEST",
            trade_unit=1,
            leverage=1.0,
            price=105.0,
            volume=1.0,
            tp=115.0,
            sl=95.0,
            time_index=close_time_index,
            option="option_value",
            result=12345,
            id="test_position_id",
        )
        log_storage.store_log(position=open_position, order_type=1, save_profit=False)
        log_storage.store_log(position=close_position, order_type=-1, save_profit=True)

        logs = log_storage.get_logs(provider="test_provider", username="test_user")
        self.assertEqual(len(logs), 2)
        open_log = logs[logs["order_type"] == 1].iloc[0]
        close_log = logs[logs["order_type"] == -1].iloc[0]
        self.assertEqual(open_log["position_id"], open_position.id)
        self.assertEqual(close_log["position_id"], close_position.id)
        self.assertEqual(open_log["symbol"], open_position.symbol)
        self.assertEqual(close_log["symbol"], close_position.symbol)
        self.assertEqual(open_log["price"], open_position.price)
        self.assertEqual(close_log["price"], close_position.price)
        self.assertEqual(open_log["volume"], open_position.volume)
        self.assertEqual(close_log["volume"], close_position.volume)
        self.assertEqual(open_log["time_index"], open_position.index.isoformat())
        self.assertEqual(close_log["time_index"], close_position.index.isoformat())
        self.assertEqual(open_log["trade_unit"], open_position.trade_unit)
        self.assertEqual(close_log["trade_unit"], close_position.trade_unit)
        self.assertEqual(open_log["leverage"], open_position.leverage)
        self.assertEqual(close_log["leverage"], close_position.leverage)

        expected_profit = (close_position.price - open_position.price) * close_position.volume * close_position.trade_unit * close_position.leverage
        profit_logs = log_storage.get_profit_logs(provider="test_provider", username="test_user")
        self.assertEqual(len(profit_logs), 1)
        profit_log_item = profit_logs.iloc[0]
        self.assertEqual(profit_log_item["profit"], expected_profit)

    def test_csv_store_logs(self):
        log_storage = self._initialize_default_log_storage()
        time_index = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        items = []
        for i in range(5):
            position = db.Position(
                position_side=db.POSITION_SIDE.long,
                symbol="TEST",
                trade_unit=1,
                leverage=1.0,
                price=100.0 + i,
                volume=1.0,
                tp=110.0 + i,
                sl=90.0 + i,
                time_index=time_index + datetime.timedelta(minutes=i),
                # object is not suppoprted for now
                option="option_value",
                # result is used by mt5 provider, but it can be any value for other providers, so we will use an integer for testing
                result=12345 + i,
                id=f"test_position_id_{i}",
            )
            items.append((position, 1))
        log_storage.store_logs(items=items, save_profit=False)

    def test_csv_get_log_with_id(self):
        pass

    def test_csv_get_open_log_with_id(self):
        pass


if __name__ == "__main__":
    unittest.main()
