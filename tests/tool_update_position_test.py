"""Tests for AgentTool.update_position() — lets an agent modify the stop-loss
and/or take-profit of an already-open position in place (e.g. move SL to
breakeven once a position is sufficiently profitable), without closing and
re-opening it.

Root cause this closes: agentic_trade's executor tool set previously had no
way to modify SL/TP on a held position at all (only open/close), so a
monitoring-plan instruction like "once price passes X, raise SL to entry
price" could only ever be described, never actually executed — confirmed via
a live retrospective finding (+56 pip winner never got its SL moved, later
gave back the profit and closed at a loss). ClientBase/MT5Client already
implement update_position(position, tp, sl) at the client layer; this was
just never exposed through AgentTool.

Uses a bare MagicMock as the client (AgentTool.__init__ only needs a `client`
attribute for this method), avoiding the full ClientBase/MT5 setup.
"""
import unittest
from unittest.mock import MagicMock

from finance_client.tool import AgentTool


class TestUpdatePosition(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock()
        self.tool = object.__new__(AgentTool)
        self.tool.client = self.client

    def test_updates_stop_loss_only(self):
        self.client.update_position.return_value = True
        result = self.tool.update_position("123", stop_loss=155.65)
        self.client.update_position.assert_called_once_with("123", tp=None, sl=155.65)
        self.assertEqual(result, {"result": True, "message": "update_position success"})

    def test_updates_take_profit_only(self):
        self.client.update_position.return_value = True
        result = self.tool.update_position("123", take_profit=160.0)
        self.client.update_position.assert_called_once_with("123", tp=160.0, sl=None)
        self.assertTrue(result["result"])

    def test_updates_both_sl_and_tp(self):
        self.client.update_position.return_value = True
        result = self.tool.update_position("123", stop_loss=155.65, take_profit=160.0)
        self.client.update_position.assert_called_once_with("123", tp=160.0, sl=155.65)
        self.assertTrue(result["result"])

    def test_neither_sl_nor_tp_given_is_rejected_without_calling_client(self):
        result = self.tool.update_position("123")
        self.client.update_position.assert_not_called()
        self.assertFalse(result["result"])
        self.assertIn("stop_loss", result["message"])

    def test_client_returning_false_is_reported_as_failure(self):
        self.client.update_position.return_value = False
        result = self.tool.update_position("999", stop_loss=155.65)
        self.assertEqual(result, {"result": False, "message": "position not found or update rejected"})

    def test_client_exception_is_caught_and_reported(self):
        self.client.update_position.side_effect = RuntimeError("broker unreachable")
        result = self.tool.update_position("123", stop_loss=155.65)
        self.assertFalse(result["result"])
        self.assertIn("broker unreachable", result["message"])


if __name__ == "__main__":
    unittest.main()
