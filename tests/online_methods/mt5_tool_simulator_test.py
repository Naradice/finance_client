import os
import sys
import unittest

import dotenv

try:
    dotenv.load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"))
except Exception as e:
    raise e

# require "pip install -e ."
from finance_client import ORDER_TYPE, AgentTool, db
from finance_client import frames as Frame
from finance_client.mt5 import MT5Client

id = int(os.environ["mt5_id"])
simulation = True
tgt_symbol = "USDJPY"
test_db_name = "finance_tool_test.db"
provider = "MT5"


def delete_db():
    if os.path.exists(test_db_name):
        os.remove(test_db_name)


class TestMT5ClientWithSQL(unittest.TestCase):
    @classmethod
    def setUpClass(self) -> None:
        storage = db.SQLiteStorage(test_db_name, provider=provider, username="test_user")
        self.client = MT5Client(
            id=id,
            password=os.environ["mt5_password"],
            server=os.environ["mt5_server"],
            auto_index=False,
            simulation=simulation,
            frame=Frame.MIN30,
            storage=storage,
        )
        self.tool = AgentTool(client=self.client)

    def test_01_001_limit_order_valid(self):
        result = self.tool.order(is_buy=True, symbol=tgt_symbol, price=155.0, volume=0.01, order_type=1, tp=160.0, sl=150.0)
        self.assertIn("id", result)
        self.assertIn("price", result)
        self.assertTrue(isinstance(result["price"], str))

    # Sometimes Function Call use int for price, volume and so on.
    def test_01_002_limit_order_valid_int(self):
        result = self.tool.order(is_buy=True, symbol=tgt_symbol, price=155, volume=1, order_type=1, tp=160, sl=150)
        self.assertIn("id", result)
        self.assertIn("price", result)
        self.assertTrue(isinstance(result["price"], str))

    def test_01_003_limit_buy_order_invalid(self):
        result = self.tool.order(is_buy=True, symbol=tgt_symbol, price=155, volume=0.01, order_type=1, tp=150.0, sl=160)
        self.assertIn("msg", result)
        self.assertTrue(len(result["msg"]) > 0)

    def test_01_004_limit_sell_order_invalid(self):
        result = self.tool.order(is_buy=False, symbol=tgt_symbol, price=155, volume=0.01, order_type=1, tp=160.0, sl=150)
        self.assertIn("msg", result)
        self.assertTrue(len(result["msg"]) > 0)

    def test_02_get_orders(self):
        orders = self.tool.get_orders()
        self.assertIsInstance(orders, dict)
        self.assertGreater(len(orders), 0)
        for order_id, order_info in orders.items():
            self.assertIn("price", order_info)
            self.assertIn("volume", order_info)
            self.assertIn("symbol", order_info)

    def test_03_cancel_order(self):
        orders = self.tool.get_orders()
        org_order_count = len(orders)
        self.assertGreater(org_order_count, 0)
        for order_id in orders.keys():
            res = self.tool.cancel_order(order_id)
            self.assertTrue(res)
        orders = self.tool.get_orders()
        self.assertEqual(len(orders), 0)
        
    def test_get_ask_rate(self):
        ask_rate = self.tool.get_ask_rate(tgt_symbol)
        try:
            float_ask_rate = float(ask_rate)
            self.assertTrue(isinstance(ask_rate, str))
        except Exception:
            self.fail("get_ask_rate did not return a valid string representation of a float")

    def test_get_bid_rate(self):
        bid_rate = self.tool.get_bid_rate(tgt_symbol)
        try:
            float_bid_rate = float(bid_rate)
            self.assertTrue(isinstance(bid_rate, str))
        except Exception:
            self.fail("get_bid_rate did not return a valid string representation of a float")

    def test_get_current_spread(self):
        spread = self.tool.get_current_spread(tgt_symbol)
        try:
            float_spread = float(spread)
            self.assertTrue(isinstance(spread, str))
        except Exception:
            self.fail("get_current_spread did not return a valid string representation of a float")
    
    def test_get_ohlc(self):
        rates = self.tool.get_ohlc(tgt_symbol, length=10, frame=30)
        self.assertIsInstance(rates, dict)
        self.assertEqual(len(rates), 10) 
        for key, values in rates.items():
            self.assertIn("open", values)
            self.assertIn("high", values)
            self.assertIn("low", values)
            self.assertIn("close", values)

    def test_get_indicators(self):
        rates = self.tool.get_ohlc_with_indicators(tgt_symbol, length=10, frame=30)
        self.assertIsInstance(rates, dict)
        self.assertEqual(len(rates), 10)
        for key, values in rates.items():
            self.assertIn("open", values)
            self.assertIn("high", values)
            self.assertIn("low", values)
            self.assertIn("close", values)
            self.assertIn("MACD", values)
            self.assertIn("RSI", values)

    def test_get_current_datetime(self):
        dt = self.tool.get_current_datetime()
        self.assertIsNotNone(dt)

    def test_get_unit_size(self):
        unit_size = self.tool.get_unit_size(tgt_symbol)
        try:
            float_unit_size = float(unit_size)
            self.assertTrue(isinstance(unit_size, str))
        except Exception:
            self.fail("get_unit_size did not return a valid float")

    def test_10_close_position(self):
        position = self.tool.order(is_buy=True, symbol=tgt_symbol, price=155.0, volume=0.01, order_type=0, tp=160.0, sl=150.0)
        self.assertIn("id", position)
        position_id = position["id"]
        positions = self.tool.get_positions()
        self.assertIn(position_id, positions)
        self.assertEqual(positions[position_id]["volume"], '0.01')
        result = self.tool.close_position(position_id, volume=0.01)
        self.assertIn("closed_price", result)
        self.assertIn("profit", result)
        self.assertNotIn("msg", result)        

    def test_11_close_all_positions(self):
        position = self.tool.order(is_buy=True, symbol=tgt_symbol, price=155.0, volume=0.01, order_type=0, tp=160.0, sl=150.0)
        self.assertIn("id", position)
        positions = self.tool.get_positions()
        self.assertGreater(len(positions), 0)
        self.tool.close_all_positions()
        positions = self.tool.get_positions()
        self.assertEqual(len(positions), 0)


if __name__ == "__main__":
    delete_db()
    unittest.main()
