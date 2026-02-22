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
        self.budget = 100000
        storage = db.PositionSQLiteStorage(test_db_name, provider=provider, username="test_user")
        self.client = MT5Client(
            id=id,
            password=os.environ["mt5_password"],
            server=os.environ["mt5_server"],
            auto_index=False,
            simulation=simulation,
            frame=Frame.MIN30,
            storage=storage,
            budget=self.budget,
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
    
    def test_open_positions(self):
        position = self.tool.order(is_buy=True, symbol=tgt_symbol, price=155.0, volume=0.01, order_type=0, tp=160.0, sl=150.0)
        self.assertIn("id", position)
        positions = self.tool.get_positions()
        self.assertIn(position["id"], positions)
        self.assertEqual(positions[position["id"]]["volume"], '0.01')
        sec_position = self.tool.order(is_buy=False, symbol="EURUSD", volume=0.02, order_type=0)
        self.assertIn("id", sec_position)
        positions = self.tool.get_positions()
        self.assertIn(sec_position["id"], positions)
        self.assertEqual(positions[sec_position["id"]]["volume"], '0.02')
        
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

    def test_get_MACD(self):
        """test get_MACD method"""
        length = 10
        short_window = 12
        long_window = 26
        signal_window = 9
        frame = "30min"
        macd = self.tool.get_MACD(
            symbol=tgt_symbol,
            length=length,
            frame=frame,
            short_window=short_window,
            long_window=long_window,
            signal_window=signal_window,
        )
        self.assertIsInstance(macd, str)   
        lines = macd.splitlines()
        self.assertEqual(len(lines), length + 1)  # header + length
        for line in lines[1:]:
            parts = line.split(",")
            self.assertEqual(len(parts), 3)  # index, MACD, SIGNAL
            try:
                float_macd = float(parts[1])
                float_signal = float(parts[2])
            except Exception:
                self.fail("MACD or SIGNAL is not a valid float string")

    def test_get_ATR(self):
        """test get_ATR method"""
        length = 10
        period = 14
        frame = "30min"
        atr = self.tool.get_ATR(
            symbol=tgt_symbol,
            length=length,
            frame=frame,
            window=period,
        )
        self.assertIsInstance(atr, str)   
        lines = atr.splitlines()
        self.assertEqual(len(lines), length + 1)  # header + length
        for line in lines[1:]:
            parts = line.split(",")
            self.assertEqual(len(parts), 2)  # index, ATR
            try:
                float_atr = float(parts[1])
            except Exception:
                self.fail("ATR is not a valid float string")

    def test_get_BollingerBands(self):
        """test get_BollingerBands method"""
        length = 10
        window = 20
        num_std_dev = 2
        frame = "30min"
        bb = self.tool.get_BollingerBands(
            symbol=tgt_symbol,
            length=length,
            frame=frame,
            window=window,
            alpha=num_std_dev,
        )
        print(bb)
        self.assertIsInstance(bb, str)   
        lines = bb.splitlines()
        self.assertEqual(len(lines), length + 1)  # header + length
        for line in lines[1:]:
            parts = line.split(",")
            self.assertEqual(len(parts), 5)  # index, UB, LB, Width, StdDev
            try:
                float_ub = float(parts[1])
                float_lb = float(parts[2])
                float_width = float(parts[3])
                float_stddev = float(parts[4])
            except Exception:
                self.fail("MB, UB, or LB is not a valid float string")
    
    def test_get_RSI(self):
        """test get_RSI method"""
        length = 10
        window = 14
        frame = "30min"
        rsi = self.tool.get_RSI(
            symbol=tgt_symbol,
            length=length,
            frame=frame,
            window=window,
        )
        print(rsi)
        self.assertIsInstance(rsi, str)   
        lines = rsi.splitlines()
        self.assertEqual(len(lines), length + 1)  # header + length
        for line in lines[1:]:
            parts = line.split(",")
            self.assertEqual(len(parts), 4)  # index, RSI, Gain, Loss
            try:
                float_rsi = float(parts[1])
                float_gain = float(parts[2])
                float_loss = float(parts[3])
            except Exception:
                self.fail("RSI is not a valid float string")

    def test_get_MA(self):
        """test get_MA method"""
        length = 10
        window = 14
        frame = "30min"
        ma = self.tool.get_SMA(
            symbol=tgt_symbol,
            length=length,
            frame=frame,
            window=window,
        )
        print(ma)
        self.assertIsInstance(ma, str)   
        lines = ma.splitlines()
        self.assertEqual(len(lines), length + 1)  # header + length
        for line in lines[1:]:
            parts = line.split(",")
            self.assertEqual(len(parts), 2)  # index, MA
            try:
                float_ma = float(parts[1])
            except Exception:
                self.fail("MA is not a valid float string")
    
    def test_get_EMA(self):
        """test get_EMA method"""
        length = 10
        window = 14
        frame = "30min"
        ema = self.tool.get_EMA(
            symbol=tgt_symbol,
            length=length,
            frame=frame,
            window=window,
        )
        print(ema)
        self.assertIsInstance(ema, str)   
        lines = ema.splitlines()
        self.assertEqual(len(lines), length + 1)  # header + length
        for line in lines[1:]:
            parts = line.split(",")
            self.assertEqual(len(parts), 2)  # index, EMA
            try:
                float_ema = float(parts[1])
            except Exception:
                self.fail("EMA is not a valid float string")

    def test_get_CCI(self):
        """test get_CCI method"""
        length = 10
        window = 14
        frame = "30min"
        cci = self.tool.get_CCI(
            symbol=tgt_symbol,
            length=length,
            frame=frame,
            window=window,
        )
        print(cci)
        self.assertIsInstance(cci, str)   
        lines = cci.splitlines()
        self.assertEqual(len(lines), length + 1)  # header + length
        for line in lines[1:]:
            parts = line.split(",")
            self.assertEqual(len(parts), 2)  # index, CCI
            try:
                float_cci = float(parts[1])
            except Exception:
                self.fail("CCI is not a valid float string")
    
    def test_get_LinearRegressionMomentum(self):
        """test get_LinearRegressionMomentum method"""
        length = 10
        window = 14
        frame = "30min"
        lrm = self.tool.get_LinearRegressionMomentum(
            symbol=tgt_symbol,
            length=length,
            frame=frame,
            window=window,
        )
        print(lrm)
        self.assertIsInstance(lrm, str)   
        lines = lrm.splitlines()
        self.assertEqual(len(lines), length + 1)  # header + length
        for line in lines[1:]:
            parts = line.split(",")
            self.assertEqual(len(parts), 2)  # index, LRM
            try:
                float_lrm = float(parts[1])
            except Exception:
                self.fail("LRM is not a valid float string")
            
    def test_get_Renko(self):
        """test get_Renko method"""
        length = 10
        window = 14
        frame = "30min"
        renko = self.tool.get_Renko(
            symbol=tgt_symbol,
            length=length,
            frame=frame,
            window=window,
        )
        print(renko)
        self.assertIsInstance(renko, str)   
        lines = renko.splitlines()
        self.assertEqual(len(lines), length + 1)  # header + length
        for line in lines[1:]:
            parts = line.split(",")
            self.assertEqual(len(parts), 2)  # index, Renko
            try:
                float_renko = float(parts[1])
            except Exception:
                self.fail("Renko is not a valid float string")

    def test_get_budget(self):
        budget = self.tool.get_budget()
        try:
            float_budget = float(budget)
            self.assertTrue(isinstance(budget, str))
            self.assertEqual(float_budget, self.budget)
        except Exception:
            self.fail("get_budget did not return a valid float string")

    def tearDown(self) -> None:
        self.client.close_client()


if __name__ == "__main__":
    delete_db()
    unittest.main()
