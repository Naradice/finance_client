"""Exercise the live-check runner using mocks only, never a broker connection."""

import importlib.util
from pathlib import Path
import unittest
import tempfile
import json
from types import SimpleNamespace
from threading import Event
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location("sbi_cycle_sample", Path(__file__).resolve().parents[2] / "samples/sbi_order_cycle.py")
cycle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cycle)


class OrderCycleTests(unittest.TestCase):
    def setUp(self):
        self.stock = Mock()
        self.stock.buy_order.return_value = True
        self.stock.cancel_order.return_value = True
        self.order = {"symbol_index": 1111}
        self.pause = Mock()

    def test_success_submits_and_cancels_once_and_checks_remaining_orders(self):
        self.stock.get_orders.side_effect = [[], [self.order], []]
        result = cycle.run_cycle(self.stock, "1111", 2, 123, pause=self.pause)
        self.assertTrue(result["cancellation_confirmed"])
        self.stock.buy_order.assert_called_once_with("1111", 2, order_price=123)
        self.stock.cancel_order.assert_called_once_with("1111")

    def test_existing_order_stops_before_submission(self):
        self.stock.get_orders.return_value = [self.order]
        with self.assertRaises(cycle.CycleStopped):
            cycle.run_cycle(self.stock, "1111", 2, 123, pause=self.pause)
        self.stock.buy_order.assert_not_called()
        self.stock.cancel_order.assert_not_called()

    def test_preparation_does_not_submit_or_cancel_even_with_an_existing_order(self):
        self.stock.get_orders.return_value = [self.order]
        result = cycle.run_preparation(self.stock, "1111", 2, 123)
        self.stock.buy_order.assert_called_once_with("1111", 2, order_price=123, submit=False)
        self.stock.cancel_order.assert_not_called()
        self.assertFalse(result["order_submitted"])

    def test_preparation_failure_does_not_fall_back_to_submission(self):
        self.stock.buy_order.return_value = False
        with self.assertRaises(cycle.CycleStopped):
            cycle.run_preparation(self.stock, "1111", 2, 123)
        self.stock.buy_order.assert_called_once_with("1111", 2, order_price=123, submit=False)
        self.stock.cancel_order.assert_not_called()

    def test_exercise_recovery_restores_legacy_lookup_after_failure(self):
        from selenium.webdriver.common.by import By
        raw = self.stock.driver.raw_driver
        original = raw.find_elements
        original.return_value = []
        diagnostic = Mock()
        with self.assertRaises(RuntimeError):
            with cycle.exercise_preparation_recovery(self.stock, diagnostic, True):
                self.assertEqual(raw.find_elements(By.NAME, "input_quantity"), [])
                raw.find_elements(By.TAG_NAME, "p")
                raise RuntimeError("fixture stop")
        self.assertIs(raw.find_elements, original)
        original.assert_any_call(By.TAG_NAME, "p")

    def test_rejected_submission_does_not_attempt_cancellation(self):
        self.stock.get_orders.return_value = []
        self.stock.buy_order.return_value = False
        with self.assertRaises(cycle.CycleStopped):
            cycle.run_cycle(self.stock, "1111", 2, 123, pause=self.pause)
        self.stock.cancel_order.assert_not_called()

    def test_missing_order_only_retries_reads_not_submission(self):
        self.stock.get_orders.return_value = []
        with self.assertRaises(cycle.CycleStopped):
            cycle.run_cycle(self.stock, "1111", 2, 123, pause=self.pause)
        self.stock.buy_order.assert_called_once()
        self.stock.cancel_order.assert_not_called()
        self.assertEqual(self.pause.call_count, 5)

    def test_duplicate_matching_orders_do_not_cancel_an_arbitrary_order(self):
        self.stock.get_orders.side_effect = [[], [self.order, self.order]]
        with self.assertRaises(cycle.CycleStopped):
            cycle.run_cycle(self.stock, "1111", 2, 123, pause=self.pause)
        self.stock.cancel_order.assert_not_called()

    def test_unknown_submission_result_is_not_retried(self):
        self.stock.get_orders.return_value = []
        self.stock.buy_order.side_effect = RuntimeError("unknown")
        with self.assertRaises(RuntimeError):
            cycle.run_cycle(self.stock, "1111", 2, 123, pause=self.pause)
        self.stock.buy_order.assert_called_once()
        self.stock.cancel_order.assert_not_called()

    def test_unverified_cancellation_is_not_retried(self):
        self.stock.get_orders.side_effect = [[], [self.order]]
        self.stock.cancel_order.return_value = False
        with self.assertRaises(cycle.CycleStopped):
            cycle.run_cycle(self.stock, "1111", 2, 123, pause=self.pause)
        self.stock.cancel_order.assert_called_once()

    def test_without_execute_only_checks_configuration_and_never_imports_broker(self):
        import builtins
        import os
        import sys
        original = builtins.__import__
        def guard(name, *args, **kwargs):
            if name.startswith("finance_client"):
                raise AssertionError("Configuration-only mode imported the broker")
            return original(name, *args, **kwargs)
        settings = {name: "fixture" for name in ("sbi_id", "sbi_password", "sbi_trade_password", "SBI_AI_MODEL", "OPENAI_API_KEY")}
        settings.update(SBI_AI_HEALING="1", SBI_AI_OPERATION_HEALING="1")
        with patch.dict(os.environ, settings, clear=True), patch.object(cycle, "load_dotenv") as loader, \
             patch.object(sys, "argv", ["sample", "--symbol", "1111", "--units", "2", "--price", "123", "--env-file", "fixture.env"]), \
             patch("builtins.__import__", side_effect=guard):
            self.assertEqual(cycle.main(), 0)
            loader.assert_called_once_with(Path("fixture.env"), override=False, encoding="utf-8-sig", interpolate=False)

    def test_diagnostics_preserve_failure_stage_without_exception_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            diagnostic = cycle.RunDiagnostics(directory)
            diagnostic.set_stage("login")
            try:
                raise RuntimeError("private-auth-code-and-account-value")
            except RuntimeError as error:
                diagnostic.error(error)
            text = diagnostic.path.read_text(encoding="utf-8")
            record = json.loads(text.splitlines()[-1])
            self.assertEqual(record["stage"], "login")
            self.assertEqual(record["error_type"], "RuntimeError")
            self.assertTrue(record["frames"])
            self.assertNotIn("private-auth-code-and-account-value", text)

    def test_ai_diagnostics_record_result_without_prompt_or_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            diagnostic = cycle.RunDiagnostics(directory)
            recovery = Mock()
            recovery.proposer.return_value = {"private": "private-ai-reply"}
            diagnostic.observe_ai(recovery, "selenium")
            self.assertEqual(recovery.proposer("private-ai-input"), {"private": "private-ai-reply"})
            text = diagnostic.path.read_text(encoding="utf-8")
            self.assertIn("ai_request", text)
            self.assertIn("ai_response", text)
            self.assertNotIn("private-ai", text)
            self.assertEqual(diagnostic.ai_requests["selenium"], 1)

    def test_phone_call_confirmation_handles_enter_and_closed_input(self):
        with patch("builtins.input", return_value=""):
            self.assertTrue(cycle.wait_for_phone_call(1))
        with patch("builtins.input", side_effect=EOFError):
            self.assertFalse(cycle.wait_for_phone_call(1))

    def test_phone_call_detects_manual_browser_completion_without_terminal_enter(self):
        required = {"required": True, "expired": False, "button_count": 1}
        completed = {"required": False, "expired": False, "button_count": 0}
        with patch("builtins.input", side_effect=EOFError), \
             patch("finance_client.sbi.sbi_util.sbi_util.phone_auth.phone_authentication_state",
                   side_effect=[required, required, completed]) as state:
            self.assertTrue(cycle.wait_for_phone_call(2, driver=object()))
        self.assertEqual(state.call_count, 3)

    def test_phone_call_stops_on_explicit_screen_expiry_without_waiting_full_timeout(self):
        with patch("builtins.input", side_effect=EOFError), \
             patch("finance_client.sbi.sbi_util.sbi_util.phone_auth.phone_authentication_state",
                   return_value={"required": True, "expired": True, "button_count": 1}):
            self.assertFalse(cycle.wait_for_phone_call(180, driver=object()))

    def test_manual_phone_completion_reuses_one_blocked_reader_for_next_challenge(self):
        entered, release = Event(), Event()
        input_state = {}
        def delayed_enter():
            entered.set()
            release.wait(2)
            return ""
        try:
            with patch("builtins.input", side_effect=delayed_enter) as read, \
                 patch("finance_client.sbi.sbi_util.sbi_util.phone_auth.phone_authentication_state",
                       return_value={"required": False, "expired": False, "button_count": 0}):
                self.assertTrue(cycle.wait_for_phone_call(1, driver=object(), input_state=input_state))
                self.assertTrue(entered.wait(1))
                reader = input_state["reader"]
                self.assertTrue(cycle.wait_for_phone_call(1, driver=object(), input_state=input_state))
                self.assertIs(input_state["reader"], reader)
                read.assert_called_once()
        finally:
            release.set()
            if input_state.get("reader") is not None:
                input_state["reader"].join(timeout=2)

    def test_phone_diagnostic_distinguishes_timeout_without_phone_details(self):
        with tempfile.TemporaryDirectory() as directory:
            diagnostic = cycle.RunDiagnostics(directory)
            error = RuntimeError("private-phone-and-account-details")
            error.phone_authentication_state = "timeout"
            diagnostic.error(error)
            text = diagnostic.path.read_text(encoding="utf-8")
            self.assertEqual(json.loads(text)["reason"], "phone_authentication_timeout")
            self.assertNotIn("private-phone", text)

    def test_connection_failure_message_distinguishes_before_and_during_submission(self):
        from selenium.common.exceptions import InvalidSessionIdException
        import sys
        for phase in ("before", "preparation", "submission"):
            during_submission = phase != "before"
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as directory:
                stock = Mock()
                stock.get_orders.return_value = []
                target = stock.buy_order if during_submission else stock.get_orders
                failure = InvalidSessionIdException("private-session-details")
                if phase == "preparation":
                    failure.order_not_submitted = True
                target.side_effect = failure
                reads = Mock()
                reads.from_env.return_value = None
                operations = Mock()
                operations.from_env.return_value = None
                modules = {
                    "finance_client.sbi.sbi_util.sbi_util.rpa": SimpleNamespace(STOCK=Mock(return_value=stock)),
                    "finance_client.sbi.sbi_util.sbi_util.read_recovery": SimpleNamespace(ReadRecovery=reads),
                    "finance_client.sbi.sbi_util.sbi_util.selenium_recovery": SimpleNamespace(SeleniumRecovery=operations),
                }
                args = SimpleNamespace(prepare_only=False, exercise_recovery=False,
                                       symbol="1111", units=1, price=123)
                with patch.dict(sys.modules, modules), patch.object(cycle, "ROOT", Path(directory)), \
                     patch("builtins.print") as output:
                    self.assertEqual(cycle.execute_cycle(args), 1)
                stopped = [call.args[0] for call in output.call_args_list
                           if call.args and str(call.args[0]).startswith("Stopped:")]
                self.assertEqual(len(stopped), 1)
                expected = ("Check the order status in SBI" if phase == "submission"
                            else "No order or cancellation was submitted")
                self.assertIn(expected, stopped[0])
                self.assertNotIn("private-session-details", stopped[0])
                if not during_submission:
                    stock.buy_order.assert_not_called()
                stock.cancel_order.assert_not_called()


if __name__ == "__main__":
    unittest.main()
