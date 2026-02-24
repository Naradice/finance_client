import os
import unittest
from typing import List, Tuple

from finance_client import db
from finance_client.account import AccountRiskConfig, Manager
from finance_client.position import POSITION_SIDE, Position


class ManagerTest(unittest.TestCase):

    def test_risk_config(self):
        manager = Manager(10000)
        # valid config
        risk_config = AccountRiskConfig(base_currency="USD", max_single_trade_percent=0.1, max_total_risk_percent=0.2,
            daily_max_loss_percent=0.5, allow_aggressive_mode=False, aggressive_multiplier=None, enforce_volume_reduction=False, atr_ratio_min_stop_loss=3)
        manager.risk_config = risk_config
        self.assertEqual(manager.risk_config.daily_max_loss_percent, 50)
        self.assertEqual(manager.daily_max_loss, 5000.0)

        # invalid config: negative value
        with self.assertRaises(ValueError):
            risk_config = AccountRiskConfig(base_currency="USD", max_single_trade_percent=0.1, max_total_risk_percent=0.2,
            daily_max_loss_percent=-0.1, allow_aggressive_mode=False, aggressive_multiplier=None, enforce_volume_reduction=False, atr_ratio_min_stop_loss=3)
            manager.risk_config = risk_config

        # invalid config: value greater than 100
        with self.assertRaises(ValueError):
            risk_config = AccountRiskConfig(base_currency="USD", max_single_trade_percent=0.1, max_total_risk_percent=0.2,
            daily_max_loss_percent=150, allow_aggressive_mode=False, aggressive_multiplier=None, enforce_volume_reduction=False, atr_ratio_min_stop_loss=3)
            manager.update_risk_config(risk_config)

        # valid config: zero value (no limit)
        risk_config = AccountRiskConfig(base_currency="USD", max_single_trade_percent=0.1, max_total_risk_percent=0.2,
            daily_max_loss_percent=0, allow_aggressive_mode=False, aggressive_multiplier=None, enforce_volume_reduction=False, atr_ratio_min_stop_loss=3)
        manager.update_risk_config(risk_config)
        self.assertEqual(manager.risk_config.daily_max_loss_percent, 0)
        self.assertEqual(manager.daily_max_loss, 0)

        # valid config: value less than 1 (converted to percentage)
        risk_config = AccountRiskConfig(base_currency="USD", max_single_trade_percent=0.1, max_total_risk_percent=0.2,
            daily_max_loss_percent=1.0, allow_aggressive_mode=False, aggressive_multiplier=None, enforce_volume_reduction=False, atr_ratio_min_stop_loss=3)
        manager.update_risk_config(risk_config)
        self.assertEqual(manager.risk_config.daily_max_loss_percent, 1.0)
        self.assertEqual(manager.daily_max_loss, 100.0)

    def test_update_daily_max_loss(self):
        manager = Manager(10000)
        risk_config = AccountRiskConfig(base_currency="USD", max_single_trade_percent=0.1, max_total_risk_percent=0.2,
            daily_max_loss_percent=0.5, allow_aggressive_mode=False, aggressive_multiplier=None, enforce_volume_reduction=False, atr_ratio_min_stop_loss=3)
        manager.risk_config = risk_config
        self.assertEqual(manager.daily_max_loss, 5000.0)

        # update budget and check if daily max loss is updated accordingly
        manager.budget = 20000
        manager.update_daily_max_loss()
        self.assertEqual(manager.daily_max_loss, 10000.0)

        # update risk config and check if daily max loss is updated accordingly
        risk_config.daily_max_loss_percent = 0.25
        manager.update_risk_config(risk_config)
        manager.update_daily_max_loss()
        self.assertEqual(manager.daily_max_loss, 5000.0)
    
    def test_open_position_without_price(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=None, volume=1)
        self.assertEqual(position.price, None)
        self.assertEqual(position.volume, 1)
    
    def test_close_position_without_price(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=None, volume=1)
        closed_result = manager.close_position(position.id, price=None)
        self.assertTrue(closed_result.error)
        self.assertEqual(closed_result.msg, "either id or price is None")
    
    def test_close_position_with_nonexistent_id(self):
        manager = Manager(10000)
        closed_result = manager.close_position(id="nonexistent_id", price=100)
        self.assertTrue(closed_result.error)
        self.assertEqual(closed_result.msg, "position id is not found")
    
    def test_close_position_with_partial_volume(self):
        manager = Manager(1000000)
        trade_unit = 100000
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1, trade_unit=trade_unit)
        closed_result = manager.close_position(position.id, price=110, volume=0.5)
        self.assertFalse(closed_result.error)
        self.assertEqual(closed_result.volume, 0.5)
        self.assertEqual(closed_result.profit, trade_unit * 0.5 * 10)
    
    def test_close_position_with_excess_volume(self):
        manager = Manager(10000)
        trade_unit = 100000
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1, trade_unit=trade_unit)
        closed_result = manager.close_position(position.id, price=110, volume=2)
        self.assertFalse(closed_result.error)
        self.assertEqual(closed_result.volume, 1)
        self.assertEqual(closed_result.profit, 10 * trade_unit * 1)
    
    def test_close_position_with_exact_volume(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        closed_result = manager.close_position(position.id, price=110, volume=1)
        self.assertFalse(closed_result.error)
        self.assertEqual(closed_result.volume, 1)
        self.assertEqual(closed_result.profit, 10 * position.trade_unit * 1)
    
    def test_close_position_with_zero_volume(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        closed_result = manager.close_position(position.id, price=110, volume=0)
        self.assertFalse(closed_result.error)
        self.assertEqual(closed_result.volume, 0)
        self.assertEqual(closed_result.profit, 0)

    def test_close_position_with_negative_volume(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        closed_result = manager.close_position(position.id, price=110, volume=-1)
        self.assertFalse(closed_result.error)
        self.assertEqual(closed_result.volume, -1)
        self.assertEqual(closed_result.profit, -10 * position.trade_unit * 1)
    
    def test_close_position_with_price_better_than_open(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        closed_result = manager.close_position(position.id, price=90, volume=1)
        self.assertFalse(closed_result.error)
        self.assertEqual(closed_result.volume, 1)
        self.assertEqual(closed_result.profit, -10 * position.trade_unit * 1)
    
    def test_close_position_with_price_worse_than_open(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        closed_result = manager.close_position(position.id, price=110, volume=1)
        self.assertFalse(closed_result.error)
        self.assertEqual(closed_result.volume, 1)
        self.assertEqual(closed_result.profit, 10 * position.trade_unit * 1)
    
    def test_close_position_with_price_equal_to_open(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        closed_result = manager.close_position(position.id, price=100, volume=1)
        self.assertFalse(closed_result.error)
        self.assertEqual(closed_result.volume, 1)
        self.assertEqual(closed_result.profit, 0)
    
    def test_close_position_with_price_zero(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        closed_result = manager.close_position(position.id, price=0, volume=1)
        trade_unit = position.trade_unit
        self.assertFalse(closed_result.error)
        self.assertEqual(closed_result.volume, 1)
        self.assertEqual(closed_result.profit, -100 * trade_unit * 1)
    
    def test_close_position_with_price_negative(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        closed_result = manager.close_position(position.id, price=-10, volume=1)
        self.assertFalse(closed_result.error)
        self.assertEqual(closed_result.volume, 1)
        self.assertEqual(closed_result.profit, -110 * position.trade_unit * 1)

    def test_update_position_with_tp_sl(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        updated = manager.update_position(position, tp=110, sl=90)
        self.assertTrue(updated)
        self.assertEqual(position.tp, 110)
        self.assertEqual(position.sl, 90)
    
    def test_update_position_remove_tp_sl(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        updated = manager.update_position(position, tp=110, sl=90)
        self.assertTrue(updated)
        self.assertEqual(position.tp, 110)
        self.assertEqual(position.sl, 90)

        updated = manager.update_position(position, tp=None, sl=None)
        self.assertTrue(updated)
        self.assertEqual(position.tp, None)
        self.assertEqual(position.sl, None)
    
    def test_update_position_with_only_tp(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        updated = manager.update_position(position, tp=110)
        self.assertTrue(updated)
        self.assertEqual(position.tp, 110)
        self.assertEqual(position.sl, None)
    
    def test_update_position_with_only_sl(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        updated = manager.update_position(position, sl=90)
        self.assertTrue(updated)
        self.assertEqual(position.tp, None)
        self.assertEqual(position.sl, 90)
    
    def test_update_position_with_invalid_tp_sl(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        updated = manager.update_position(position, tp=-10, sl=-20)
        self.assertTrue(updated)
        self.assertEqual(position.tp, -10)
        self.assertEqual(position.sl, -20)
    
    def test_update_position_with_tp_sl_equal_to_price(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        updated = manager.update_position(position, tp=100, sl=100)
        self.assertTrue(updated)
        self.assertEqual(position.tp, 100)
        self.assertEqual(position.sl, 100)
    
    def test_update_position_with_tp_sl_zero(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        updated = manager.update_position(position, tp=0, sl=0)
        self.assertTrue(updated)
        self.assertEqual(position.tp, 0)
        self.assertEqual(position.sl, 0)
    
    def test_update_position_with_tp_sl_negative(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        updated = manager.update_position(position, tp=-10, sl=-20)
        self.assertTrue(updated)
        self.assertEqual(position.tp, -10)
        self.assertEqual(position.sl, -20)
    
    def test_update_position_with_nonexistent_position(self):
        manager = Manager(10000)
        position = Position(POSITION_SIDE.long, "test", trade_unit=1.0, leverage=1.0, price=100, volume=1, tp=None, sl=None, id="nonexistent_id_to_update")
        updated = manager.update_position(position, tp=110, sl=90)
        self.assertFalse(updated)
    
    def test_update_position_with_none_position(self):
        manager = Manager(10000)
        updated = manager.update_position(None, tp=110, sl=90)
        self.assertFalse(updated)
    
    def test_update_position_with_invalid_tp_sl(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        updated = manager.update_position(position, tp=-10, sl=-20)
        self.assertTrue(updated)
        self.assertEqual(position.tp, -10)
        self.assertEqual(position.sl, -20)

    def test_update_position_with_tp_sl_equal_to_price(self):
        manager = Manager(10000)
        position = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        updated = manager.update_position(position, tp=100, sl=100)
        self.assertTrue(updated)
        self.assertEqual(position.tp, 100)
        self.assertEqual(position.sl, 100)

    def test_get_positions(self):
        key = "account_test"
        storage = db.PositionSQLiteStorage("./unit_test.db", key, username=key)
        manager = Manager(10000, position_storage=storage)
        id = manager.open_position(POSITION_SIDE.long, "test", 100.0, 1)
        id = manager.open_position(POSITION_SIDE.long, "test", 100.0, 1)
        id = manager.open_position(POSITION_SIDE.long, "test", 100.0, 1)
        id = manager.open_position(POSITION_SIDE.short, "test", 100.0, 1)
        id = manager.open_position(POSITION_SIDE.short, "test", 100.0, 1)
        long_positions, short_positions = manager.storage.get_positions()
        self.assertGreaterEqual(len(long_positions), 3)
        self.assertGreaterEqual(len(short_positions), 2)

    def test_get_risk_volume_of_open_positions(self):
        key = "account_test_risk"
        storage = db.PositionSQLiteStorage("./unit_test.db", key, username=key)
        manager = Manager(10000, position_storage=storage)
        position1 = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        position2 = manager.open_position(POSITION_SIDE.short, "test", price=200, volume=2)
        manager.update_position(position1, sl=90)
        manager.update_position(position2, sl=220)
        total_risk_volume = manager.get_open_positions_risk_volume()
        expected_risk_volume = (100 - 90) * position1.trade_unit * position1.leverage * position1.volume + \
                               (220 - 200) * position2.trade_unit * position2.leverage * position2.volume
        self.assertEqual(total_risk_volume, expected_risk_volume)

    def test_get_daily_realized_pnl(self):
        key = "account_test_pnl"
        storage = db.PositionSQLiteStorage("./unit_test.db", key, username=key)
        manager = Manager(10000, position_storage=storage)
        position1 = manager.open_position(POSITION_SIDE.long, "test", price=100, volume=1)
        position2 = manager.open_position(POSITION_SIDE.short, "test", price=200, volume=2)
        manager.close_position(position1.id, price=110, volume=1)
        manager.close_position(position2.id, price=190, volume=2)
        daily_realized_pnl = manager.get_daily_realized_pnl()
        expected_realized_pnl = (110 - 100) * position1.trade_unit * position1.leverage * position1.volume + \
                                (200 - 190) * position2.trade_unit * position2.leverage * position2.volume
        self.assertEqual(daily_realized_pnl, expected_realized_pnl)

if __name__ == "__main__":
    unittest.main()