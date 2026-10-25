"""Local DOM fixtures only; network, broker actions, mail and model calls are mocked."""

import json
import logging
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.parse import quote

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/finance_client/sbi/sbi_util"))
from sbi_util import auth
from sbi_util.rpa import STOCK, OrderPreparationError
from sbi_util.read_recovery import ReadRecovery
from sbi_util.screening import StatefulScreening
from sbi_util import sbi_enum
from sbi_util.phone_auth import (PhoneAuthenticationRequired, PhoneAuthenticationTimeout,
                                PhoneAuthenticationFailed, phone_authentication_state,
                                complete_phone_authentication)
from sbi_util.selenium_recovery import (
    ActionOutcomeUnknown, HealingDriver, HealingElement, OpenAILocatorProposer,
    OperationRecoveryError, SEEDS, SeleniumRecovery, enable_recovery,
)


class FixtureDriver:
    """Use a data document while simulating only the origin check, never navigation."""
    current_url = "https://site.sbisec.co.jp/offline-fixture"

    def __init__(self, browser):
        self.browser = browser

    def __getattr__(self, key):
        return getattr(self.browser, key)

    def execute_script(self, script, *args):
        if script == "return window.location.href":
            return self.current_url
        if script == "return window.location.pathname":
            return "/offline-fixture"
        return self.browser.execute_script(script, *args)

    def quit(self):
        pass  # STOCK destructor does not own the shared fixture browser.


class SeleniumRecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        cls.browser = webdriver.Chrome(options=options)
        cls.browser.execute_cdp_cmd("Network.enable", {})
        cls.browser.execute_cdp_cmd("Network.setBlockedURLs", {"urls": ["http://*", "https://*"]})

    @classmethod
    def tearDownClass(cls):
        cls.browser.quit()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.proposer = Mock(side_effect=self.propose)
        self.recovery = SeleniumRecovery(self.proposer, self.temporary.name)
        self.driver = HealingDriver(FixtureDriver(self.browser), self.recovery)

    @staticmethod
    def propose(contract, nodes, many):
        matches = [n for n in nodes if SeleniumRecovery.matches(n, contract)]
        if contract.get("tag") == "content":
            matches = [n for n in matches if not any(o["path"].startswith(n["path"] + "/") for o in matches)]
        return {"paths": [n["path"] for n in matches]}

    def load(self, html):
        self.browser.get("data:text/html;charset=utf-8," + quote("<meta charset='utf-8'><main>" + html + "</main>"))

    def stock(self):
        stock = object.__new__(STOCK)
        stock.driver = self.driver
        stock.logger = logging.getLogger("offline-selenium-recovery")
        stock.read_only = False
        stock.read_recovery = None
        stock.trading_pass = "fixture-trade-secret"
        return stock

    @staticmethod
    def phone_page(result="注文受付（現物買）"):
        return ("<section><h1>追加認証</h1><p>ご登録の電話番号から認証用電話番号へ発信してください。</p>"
                "<p>0800-000-0000</p><button id='changed-phone-confirm' onclick=\""
                "window.authClicks=(window.authClicks||0)+1;document.querySelector('main').innerHTML="
                f"'<h1>{result}</h1>'\">認証する</button></section>")

    def test_phone_authentication_is_not_mistaken_for_logged_in_or_an_email_code(self):
        self.load(self.phone_page())
        self.assertTrue(phone_authentication_state(self.driver)["required"])
        self.assertFalse(auth.is_logged_in(self.driver))
        with self.assertRaises(PhoneAuthenticationRequired):
            auth.login(self.driver, "fixture-user", "fixture-password")
        self.proposer.assert_not_called()

    def test_phone_help_text_does_not_trigger_phone_authentication(self):
        self.load("<h1>電話番号認証</h1><p>設定をご確認ください。</p><a>認証する</a>")
        self.assertFalse(phone_authentication_state(self.driver)["required"])

    def test_phone_authentication_after_order_resumes_result_without_resubmission(self):
        for identifier in ("changed-phone-confirm", "deviceButton"):
            with self.subTest(identifier=identifier):
                self.load(self.phone_page().replace("changed-phone-confirm", identifier))
                stock = self.stock()
                stock._pending_trade_outcome = True
                stock.phone_auth_handler = Mock(return_value=True)
                self.assertEqual(stock._wait_submission_result("order"), (True, None))
                stock.phone_auth_handler.assert_called_once()
                self.assertEqual(self.browser.execute_script("return window.authClicks"), 1)
                self.assertFalse(stock._pending_trade_outcome)
        payload = json.dumps([call.args for call in self.proposer.call_args_list], ensure_ascii=False)
        self.assertNotIn("0800", payload)

    def test_phone_authentication_after_cancel_resumes_cancel_result(self):
        self.load(self.phone_page("取消完了"))
        stock = self.stock()
        stock._pending_trade_outcome = True
        stock.phone_auth_handler = Mock(return_value=True)
        self.assertEqual(stock._wait_submission_result("cancel"), (True, None))
        self.assertEqual(self.browser.execute_script("return window.authClicks"), 1)

    def test_phone_authentication_without_handler_preserves_pending_order(self):
        self.load(self.phone_page())
        stock = self.stock()
        stock._pending_trade_outcome = True
        with self.assertRaises(PhoneAuthenticationRequired):
            stock._wait_submission_result("order")
        self.assertTrue(stock._pending_trade_outcome)
        self.assertFalse(self.browser.execute_script("return !!window.authClicks"))
        with self.assertRaises(ActionOutcomeUnknown):
            stock.buy_order("1111", 1)

    def test_phone_confirmation_failure_is_not_retried(self):
        self.load(self.phone_page())
        self.browser.execute_script("document.querySelector('button').onclick=()=>{window.authClicks=(window.authClicks||0)+1}")
        stock = self.stock()
        stock._pending_trade_outcome = True
        stock.phone_auth_handler = Mock(return_value=True)
        with patch("sbi_util.phone_auth.WebDriverWait") as wait:
            wait.return_value.until.side_effect = TimeoutException()
            with self.assertRaises(PhoneAuthenticationFailed):
                stock._wait_submission_result("order")
        self.assertEqual(self.browser.execute_script("return window.authClicks"), 1)
        self.assertTrue(stock._pending_trade_outcome)

    def test_phone_confirmation_wait_uses_remaining_deadline_after_manual_delay(self):
        clock = [0.0]
        confirmed = [False]
        driver, button = Mock(), Mock()
        driver.find_elements.return_value = [button]
        button.find_elements.return_value = []
        button.text = "認証する"
        button.is_displayed.return_value = True
        button.click.side_effect = lambda: confirmed.__setitem__(0, True)
        state = lambda _: {"required": not confirmed[0], "expired": False, "button_count": 1}
        def manual_call(seconds):
            self.assertEqual(seconds, 180)
            clock[0] += 90
            return True
        def after_more_than_twenty_seconds(predicate):
            clock[0] += 25
            return predicate(driver)
        with patch("sbi_util.phone_auth.time.monotonic", side_effect=lambda: clock[0]), \
             patch("sbi_util.phone_auth.phone_authentication_state", side_effect=state), \
             patch("sbi_util.phone_auth.WebDriverWait") as wait:
            wait.return_value.until.side_effect = after_more_than_twenty_seconds
            complete_phone_authentication(driver, manual_call, timeout=180)
        self.assertEqual(wait.call_args.args[1], 90)
        self.assertEqual(clock[0], 115)
        button.click.assert_called_once()

    def test_phone_expiry_after_confirmation_stops_without_repeating_click(self):
        self.load(self.phone_page())
        self.browser.execute_script("""
            document.querySelector('button').onclick=()=>{
              window.authClicks=(window.authClicks||0)+1;
              document.querySelector('section').insertAdjacentHTML('beforeend','<p>認証の有効期限が切れました。</p>');
            };
        """)
        stock = self.stock()
        stock._pending_trade_outcome = True
        stock.phone_auth_handler = Mock(return_value=True)
        with self.assertRaises(PhoneAuthenticationTimeout):
            stock._wait_submission_result("order")
        self.assertEqual(self.browser.execute_script("return window.authClicks"), 1)
        self.assertTrue(stock._pending_trade_outcome)

    def test_phone_authentication_alone_does_not_count_as_order_acceptance(self):
        self.load(self.phone_page("非公開の結果画面"))
        stock = self.stock()
        stock._pending_trade_outcome = True
        stock.phone_auth_handler = Mock(return_value=True)
        def observe_once(predicate):
            value = predicate(self.driver)
            if not value:
                raise TimeoutException()
            return value
        with patch("sbi_util.rpa.WebDriverWait") as wait:
            wait.return_value.until.side_effect = observe_once
            with self.assertRaises(ActionOutcomeUnknown):
                stock._wait_submission_result("order")
        self.assertEqual(self.browser.execute_script("return window.authClicks"), 1)
        self.assertTrue(stock._pending_trade_outcome)

    def test_manual_phone_confirmation_is_not_clicked_again(self):
        self.load(self.phone_page())
        stock = self.stock()
        stock._pending_trade_outcome = True
        def completed_by_user(_):
            self.browser.find_element(By.TAG_NAME, "button").click()
            return True
        stock.phone_auth_handler = completed_by_user
        self.assertEqual(stock._wait_submission_result("order"), (True, None))
        self.assertEqual(self.browser.execute_script("return window.authClicks"), 1)

    def test_phone_timeout_does_not_click_authenticate_or_clear_pending_order(self):
        self.load(self.phone_page())
        stock = self.stock()
        stock._pending_trade_outcome = True
        stock.phone_auth_handler = Mock(return_value=False)
        with self.assertRaises(PhoneAuthenticationTimeout):
            stock._wait_submission_result("order")
        self.assertTrue(stock._pending_trade_outcome)
        self.assertFalse(self.browser.execute_script("return !!window.authClicks"))

    def test_expired_phone_challenge_does_not_request_call_or_click(self):
        self.load(self.phone_page() + "<p>認証の有効期限が切れました。</p>")
        stock = self.stock()
        stock._pending_trade_outcome = True
        stock.phone_auth_handler = Mock(return_value=True)
        with self.assertRaises(PhoneAuthenticationTimeout):
            stock._wait_submission_result("order")
        stock.phone_auth_handler.assert_not_called()
        self.assertFalse(self.browser.execute_script("return !!window.authClicks"))

    def test_ambiguous_phone_authentication_buttons_are_not_clicked(self):
        self.load(self.phone_page() + "<button>認証する</button>")
        stock = self.stock()
        stock._pending_trade_outcome = True
        stock.phone_auth_handler = Mock(return_value=True)
        with self.assertRaises(PhoneAuthenticationFailed):
            stock._wait_submission_result("order")
        stock.phone_auth_handler.assert_not_called()
        self.assertFalse(self.browser.execute_script("return !!window.authClicks"))

    def test_prepare_only_does_not_complete_a_preexisting_phone_challenge(self):
        self.load(self.phone_page())
        stock = self.stock()
        stock.phone_auth_handler = Mock(return_value=True)
        with self.assertRaises(PhoneAuthenticationRequired):
            stock.filling_order_page(1, "fixture-secret", price=123, submit=False)
        stock.phone_auth_handler.assert_not_called()
        self.assertFalse(self.browser.execute_script("return !!window.authClicks"))

    def test_full_order_form_phone_and_receipt_flow_sends_order_only_once(self):
        self.load("<p>売買単位：100</p><label>注文数量<input></label><label>指値<input></label>"
                  "<label>取引パスワード<input type='password'></label>"
                  "<label>確認画面を省略<input type='checkbox'></label><button>発注</button>")
        self.browser.execute_script("""document.querySelector('button').onclick=()=>{
            window.orderClicks=(window.orderClicks||0)+1;
            document.querySelector('main').innerHTML=arguments[0];
        };""", self.phone_page())
        stock = self.stock()
        stock._check_device_verification = Mock(return_value=False)
        stock.phone_auth_handler = Mock(return_value=True)
        self.assertEqual(stock.filling_order_page(1, "fixture-secret", price=123), (True, None))
        self.assertEqual(self.browser.execute_script("return [window.orderClicks, window.authClicks]"), [1, 1])

    def test_all_seeded_login_auth_and_trade_controls_recover_changed_identifiers(self):
        for locator, (tag, kind, labels) in SEEDS.items():
            with self.subTest(locator=locator):
                if tag == "action":
                    html = f"<button id='new-id'>{labels[0]}</button>"
                elif tag == "content":
                    html = f"<h1 id='new-id'>{labels[0]}</h1>"
                else:
                    html = f"<label for='new-id'>{labels[0]}</label><input id='new-id' type='{kind}'>"
                self.load(html)
                element = self.driver.find_element(*locator)
                self.assertEqual(element.get_attribute("id"), "new-id")
                self.assertIsInstance(element, HealingElement)

    def test_cache_survives_new_adapter_and_input_values_do_not_invalidate_it(self):
        self.load("<label for='q'>注文数量</label><input id='q' value='private-1234'>")
        self.driver.find_element(By.NAME, "input_quantity").send_keys("5")
        other = HealingDriver(self.driver.raw_driver, SeleniumRecovery(self.proposer, self.temporary.name))
        other.find_element(By.NAME, "input_quantity").clear()
        self.proposer.assert_called_once()
        serialized = json.dumps(self.proposer.call_args.args, ensure_ascii=False)
        self.assertNotIn("private-1234", serialized)

    def test_layout_change_invalidates_cache(self):
        for wrapper in ("div", "section"):
            self.load(f"<{wrapper}><label for='q'>注文数量</label><input id='q'></{wrapper}>")
            self.driver.find_element(By.NAME, "input_quantity")
        self.assertEqual(self.proposer.call_count, 2)

    def test_relative_lookup_stays_inside_its_row(self):
        self.load("<table><tbody><tr><td>1111</td><td><button>取消</button></td></tr>"
                  "<tr><td>2222</td><td><button>取消</button></td></tr></tbody></table>")
        row = self.driver.find_elements(By.TAG_NAME, "tr")[1]
        action = self.driver.find_semantic(("取消",), root=row)
        self.assertEqual(action.find_element(By.XPATH, "../..").text, "2222 取消")
        with self.assertRaises(NoSuchElementException):
            self.driver.find_semantic(("取消",))

    def test_baseline_learns_unknown_screening_locator_then_recovers(self):
        self.load("<label class='SelectionBox'>東証P<input type='checkbox'></label>")
        self.driver.find_elements(By.CLASS_NAME, "SelectionBox")
        self.load("<section><label class='new-selection'>東証P<input type='checkbox'></label></section>")
        boxes = self.driver.find_elements(By.CLASS_NAME, "SelectionBox")
        self.assertEqual(len(boxes), 1)
        boxes[0].click()
        self.assertTrue(boxes[0].find_element(By.TAG_NAME, "input").is_selected())
        self.proposer.assert_called_once()

    def test_baseline_contract_is_available_to_new_process_adapter(self):
        self.load("<button class='old-close'>閉じる</button>")
        self.driver.find_element(By.CLASS_NAME, "old-close")
        self.load("<article><button>閉じる</button></article>")
        other = HealingDriver(self.driver.raw_driver, SeleniumRecovery(self.proposer, self.temporary.name))
        self.assertEqual(other.find_element(By.CLASS_NAME, "old-close").text, "閉じる")

    def test_screening_container_and_heterogeneous_options_learn_semantics(self):
        self.load("<div class='old-container'><label class='SelectionBox'>東証P<input type='checkbox'></label>"
                  "<label class='SelectionBox'>東証S<input type='checkbox'></label></div>")
        self.driver.find_element(By.CLASS_NAME, "old-container")
        self.driver.find_elements(By.CLASS_NAME, "SelectionBox")
        self.load("<div><div class='new-container'><label>東証P<input type='checkbox'></label>"
                  "<label>東証S<input type='checkbox'></label></div></div>")
        container = self.driver.find_element(By.CLASS_NAME, "old-container")
        self.assertEqual(container.get_attribute("class"), "new-container")
        self.assertEqual(len(self.driver.find_elements(By.CLASS_NAME, "SelectionBox")), 2)

    def test_hidden_and_ambiguous_fields_do_not_recover(self):
        for html in ("<label for='q'>注文数量</label><input id='q' hidden>",
                     "<label>注文数量<input></label><label>注文数量<input></label>"):
            self.load(html)
            with self.assertRaises(NoSuchElementException):
                self.driver.find_element(By.NAME, "input_quantity")
        self.proposer.assert_not_called()

    def test_adjacent_price_label_cannot_identify_quantity(self):
        self.load("<form><label for='price'>注文価格</label><input id='price'><label>注文数量</label><input></form>")
        with self.assertRaises(NoSuchElementException):
            self.driver.find_element(By.NAME, "input_quantity")
        self.proposer.assert_not_called()

    def test_nested_table_header_binds_its_only_quantity_field(self):
        self.load("<table><tr><th>株数</th><td><table><tr><td><input id='q'></td>"
                  "<td>売買単位：100株</td></tr></table></td></tr></table>")
        self.assertEqual(self.driver.find_element(By.NAME, "input_quantity").get_attribute("id"), "q")

    def test_table_row_with_multiple_numeric_fields_does_not_guess_quantity(self):
        self.load("<table><tr><th>注文数量</th><td><input id='q'><input id='p'></td></tr></table>")
        with self.assertRaises(NoSuchElementException):
            self.driver.find_element(By.NAME, "input_quantity")
        self.proposer.assert_not_called()

    def test_table_cell_binds_price_without_using_neighboring_quantity(self):
        self.load("<table><tr><th>株数</th><td><input id='q'></td></tr>"
                  "<tr><td>指値</td><td><input id='p'></td></tr></table>")
        self.assertEqual(self.driver.find_element(By.NAME, "input_price").get_attribute("id"), "p")

    def test_image_caption_identifies_action_and_receipt_without_private_alt(self):
        self.load("<button><img alt='注文発注'></button><h1><img alt='注文受付（現物買）'></h1>"
                  "<img alt='private-account-1234'>")
        self.driver.find_element(By.ID, "botton2")
        self.assertTrue(self.driver.find_semantic(("注文受付(現物買)",), tag="content", many=True))
        self.assertNotIn("private-account", json.dumps([call.args for call in self.proposer.call_args_list], ensure_ascii=False))
        self.load("<nav><a><img alt='注文受付（現物買）'></a></nav>")
        self.assertEqual(self.driver.find_semantic(("注文受付(現物買)",), tag="content", many=True), [])

    def test_native_image_submit_handler_is_located_without_dispatch(self):
        self.load("<form name='FORM' method='post' action='https://site.sbisec.co.jp/ETGate'>"
                  "<input name='_ActionID' type='hidden' value='before'>"
                  "<input id='skip-new' type='checkbox'><label for='skip-new'>注文確認画面を省略</label>"
                  "<a href=\"javascript:document.FORM._ActionID.value='place';document.FORM.submit();\">"
                  "<img alt='注文発注'></a></form>")
        self.assertEqual(self.driver.find_element(By.ID, "shouryaku").get_attribute("id"), "skip-new")
        self.assertEqual(self.driver.find_element(By.ID, "botton2").tag_name, "a")
        self.assertEqual(self.browser.find_element(By.NAME, "_ActionID").get_property("value"), "before")

    def test_native_submit_handler_rejects_external_forms_and_other_scripts(self):
        for href, action in (("javascript:document.FORM._ActionID.value='place';document.FORM.submit();", "https://evil.example/"),
                             ("javascript:window.location='https://evil.example/'", "https://site.sbisec.co.jp/ETGate")):
            with self.subTest(href=href):
                self.load(f"<form name='FORM' method='post' action='{action}'>"
                          f"<input name='_ActionID' type='hidden'><a href=\"{href}\"><img alt='注文発注'></a></form>")
                with self.assertRaises(OperationRecoveryError):
                    self.driver.find_element(By.ID, "botton2")

    def test_model_cannot_supply_code_or_wrong_paths_and_failure_is_bounded(self):
        self.load("<label for='q'>注文数量</label><input id='q'>")
        self.proposer.side_effect = None
        self.proposer.return_value = {"paths": ["//input[@id='q']"]}
        for _ in range(2):
            with self.assertRaises(OperationRecoveryError):
                self.driver.find_element(By.NAME, "input_quantity")
        self.proposer.assert_called_once()

    def test_provider_error_does_not_disclose_payload(self):
        self.load("<label for='q'>注文数量</label><input id='q'>")
        self.proposer.side_effect = RuntimeError("private-code-9876")
        with self.assertRaises(OperationRecoveryError) as caught:
            self.driver.find_element(By.NAME, "input_quantity")
        self.assertNotIn("9876", str(caught.exception))

    def test_snapshot_and_cache_do_not_disclose_secrets_or_attributes(self):
        self.load("<label for='secret'>ログインパスワード</label><input id='secret' type='password' value='secret-98765'>"
                  "<p>口座氏名-123456</p><a href='https://site.sbisec.co.jp/?token=secret-token'>private-name</a>")
        self.driver.find_element(By.NAME, "password")
        payload = json.dumps(self.proposer.call_args.args, ensure_ascii=False)
        payload += "".join(p.read_text(encoding="utf-8") for p in Path(self.temporary.name).glob("*.json"))
        for secret in ("secret-98765", "口座氏名", "123456", "secret-token", "private-name", "href", "placeholder"):
            self.assertNotIn(secret, payload)

    def test_untrusted_page_or_frame_does_not_call_model(self):
        self.load("<label>注文数量<input></label>")
        self.driver.raw_driver.current_url = "https://sbisec.co.jp.evil.example/"
        with self.assertRaises(OperationRecoveryError):
            self.driver.find_element(By.NAME, "input_quantity")
        self.proposer.assert_not_called()
        self.driver.raw_driver.current_url = "https://site.sbisec.co.jp/offline-fixture"
        with patch.object(self.driver.raw_driver, "execute_script", return_value="https://evil.example/"):
            with self.assertRaises(OperationRecoveryError):
                self.driver.find_element(By.NAME, "input_quantity")

    def test_action_dispatch_error_is_not_replayed(self):
        self.load("<button id='pw-btn'>ログイン</button>")
        button = self.driver.find_element(By.ID, "pw-btn")
        with patch.object(WebElementForTest, "click", side_effect=WebDriverException("transport error")) as click:
            with self.assertRaises(ActionOutcomeUnknown):
                button.click()
            click.assert_called_once()

    def test_stale_collection_target_is_not_rebound_to_another_row(self):
        self.load("<button>取消</button><button>取消</button>")
        buttons = self.driver.find_elements(By.TAG_NAME, "button")
        self.browser.execute_script("document.querySelector('button').remove()")
        with self.assertRaises(OperationRecoveryError):
            buttons[0].click()

    def test_login_stage_and_login_detection_handle_changed_otp_ids(self):
        self.load("<h1>認証</h1><label>認証コード<input></label><button>認証する</button>")
        self.assertFalse(auth.is_logged_in(self.driver))
        self.assertEqual(auth._login_stage(self.driver), "otp")

    def test_otp_stage_recognizes_disabled_submit_before_code_is_entered(self):
        self.load("<label>認証コード<input id='otp'></label><button disabled>認証する</button>")
        self.assertEqual(auth._login_stage(self.driver), "otp")
        submit = self.driver.find_element(By.ID, "submit-otp")
        self.assertFalse(submit.is_enabled())
        self.browser.execute_script("document.querySelector('button').disabled=false")
        self.assertTrue(submit.is_enabled())

    def test_login_enters_code_before_waiting_for_submit_enable(self):
        otp = ("<label>認証コード<input id='otp' oninput=\"document.querySelector('button').disabled=!this.value\"></label>"
               "<button id='submit-otp' disabled onclick=\"document.querySelector('main').innerHTML='<h1>ホーム</h1>'\">認証する</button>")
        self.load("<label>ユーザー名<input name='username'></label>"
                  "<label>ログインパスワード<input name='password' type='password'></label><button id='pw-btn'>ログイン</button>")
        self.browser.execute_script("const html=arguments[0];document.querySelector('button').onclick=()=>document.querySelector('main').innerHTML=html", otp)
        with patch("sbi_util.auth.gmail.retrieve_sbi_device_code", return_value="123456"):
            self.assertTrue(auth.login(self.driver, "fixture-user", "fixture-password"))

    def test_known_promotion_popup_is_closed_before_otp_confirmation(self):
        self.load("<button id='submit-otp' onclick='this.dataset.clicked=1'>認証する</button>"
                  "<aside style='position:fixed;inset:0;z-index:100;background:white'>"
                  "<a class='karte-close' onclick='this.parentElement.remove()'>閉じる</a></aside>")
        auth._close_known_popup(self.driver)
        self.driver.find_element(By.ID, "submit-otp").click()
        self.assertEqual(self.browser.find_element(By.ID, "submit-otp").get_attribute("data-clicked"), "1")

    def test_full_login_uses_changed_form_and_never_sends_credentials_to_model(self):
        self.load("<label>ユーザー名<input id='new-user'></label><label>ログインパスワード<input type='password'></label>"
                  "<button onclick=\"document.querySelector('main').innerHTML='<h1>ホーム</h1>'\">ログイン</button>")
        self.assertTrue(auth.login(self.driver, "fixture-user-secret", "fixture-login-secret"))
        request = json.dumps([x.args for x in self.proposer.call_args_list], ensure_ascii=False)
        self.assertNotIn("fixture-user-secret", request)
        self.assertNotIn("fixture-login-secret", request)

    def test_stock_order_form_and_cancel_confirmation_use_changed_controls(self):
        self.load("<p class='mtext'>売買単位：100</p>"
                  "<label>注文数量<input id='new-quantity'></label><label>指値<input id='new-price'></label>"
                  "<label>取引パスワード<input type='password'></label>"
                  "<label>確認画面を省略<input type='checkbox'></label>"
                  "<button onclick=\"document.querySelector('main').innerHTML='<h1>注文受付</h1>'\">発注</button>")
        stock = self.stock()
        stock._check_device_verification = Mock(return_value=False)
        self.assertEqual(stock.filling_order_page(1, "fixture-trade-secret", price=123), (True, None))
        self.assertFalse(stock._pending_trade_outcome)
        self.load("<label>取引パスワード<input type='password'></label>"
                  "<button onclick=\"document.querySelector('main').innerHTML='<h1>取消完了</h1>'\">取消する</button>")
        self.assertTrue(stock._STOCK__click_cancel_on_cancel_page("fixture-trade-secret"))
        self.assertFalse(stock._pending_trade_outcome)

    def test_unknown_order_result_latches_and_prevents_second_submission(self):
        stock = self.stock()
        stock._pending_trade_outcome = True
        with patch("sbi_util.rpa.WebDriverWait") as wait:
            wait.return_value.until.side_effect = WebDriverException("unknown")
            with self.assertRaises(ActionOutcomeUnknown):
                stock._wait_submission_result("order")
        with self.assertRaises(ActionOutcomeUnknown):
            stock.buy_order("1111", 1)

    def test_prepare_only_fills_and_verifies_changed_form_without_final_click(self):
        self.load("<p>売買単位：100</p><label>注文数量<input></label><label>指値<input></label>"
                  "<label>取引パスワード<input type='password'></label>"
                  "<label>確認画面を省略<input type='checkbox'></label>"
                  "<button onclick='window.submitted=true'>発注</button>")
        stock = self.stock()
        stock._check_device_verification = Mock(return_value=False)
        self.assertEqual(stock.filling_order_page(2, "fixture-trade-secret", price=123, submit=False), (True, None))
        self.assertEqual(self.driver.find_element(By.NAME, "input_quantity").get_property("value"), "200")
        self.assertFalse(self.browser.execute_script("return !!window.submitted"))
        self.assertFalse(getattr(stock, "_pending_trade_outcome", False))

    def test_prepare_only_rejects_rewritten_values_without_final_click(self):
        self.load("<p>売買単位：100</p><label>注文数量<input></label><label>指値<input></label>"
                  "<label>取引パスワード<input type='password'></label>"
                  "<label>確認画面を省略<input type='checkbox' onclick=\"document.querySelector('input').value=999\"></label>"
                  "<button onclick='window.submitted=true'>発注</button>")
        stock = self.stock()
        stock._check_device_verification = Mock(return_value=False)
        with self.assertRaises(OperationRecoveryError):
            stock.filling_order_page(2, "fixture-trade-secret", price=123, submit=False)
        self.assertFalse(self.browser.execute_script("return !!window.submitted"))

    def test_submission_result_handles_public_receipt_titles_and_table_text(self):
        for kind, html in (("order", "<table><tr><td><b>注文受付（現物買）</b></td></tr></table>"),
                           ("order", "<table><tr><td><font>ご注文を受け付けました。</font></td></tr></table>"),
                           ("order", "<table><tr><td>ご注文を受け付けました。<br>受付番号：fixture-private-1234</td></tr></table>"),
                           ("cancel", "<table><tr><td><b>注文取消（受付）</b></td></tr></table>")):
            with self.subTest(kind=kind, html=html):
                self.load(html)
                stock = self.stock()
                stock._pending_trade_outcome = True
                self.assertEqual(stock._wait_submission_result(kind), (True, None))
                self.assertFalse(stock._pending_trade_outcome)

    def test_unknown_result_diagnostics_record_only_public_metadata(self):
        self.load("<h1>注文入力</h1><p>fixture-private-account-1234</p><input value='fixture-private-password'>")
        stock = self.stock()
        self.recovery.on_submission_result = Mock()
        with patch("sbi_util.rpa.WebDriverWait") as wait:
            wait.return_value.until.side_effect = WebDriverException("fixture-private-error")
            with self.assertRaises(ActionOutcomeUnknown):
                stock._wait_submission_result("order")
        record = self.recovery.on_submission_result.call_args.kwargs
        self.assertEqual(record["outcome"], "unknown")
        self.assertIn("layout_fingerprint", record)
        self.assertNotIn("fixture-private", json.dumps(record, ensure_ascii=False))

    def test_buy_order_propagates_failure_instead_of_returning_true(self):
        stock = self.stock()
        stock._STOCK__check_login = Mock(return_value=True)
        stock._STOCK__open_symbol_page = Mock(return_value=True)
        stock._STOCK__transit_symbol_page_to_order_page = Mock(return_value=True)
        stock.filling_order_page = Mock(return_value=(False, "fixture rejection"))
        self.assertFalse(stock.buy_order("1111", 1))

    def test_duplicate_orders_cannot_cancel_first_matching_security(self):
        stock = self.stock()
        stock.open_ordered_position_page = Mock(return_value=True)
        link = Mock()
        stock._get_orders_legacy = Mock(return_value=[{"symbol_name": "fixture", "symbol_index": 1111, "cancel_link": link}] * 2)
        with self.assertRaises(OperationRecoveryError):
            stock.cancel_order("1111")
        link.click.assert_not_called()

    def test_offline_order_inquiry_cancel_lifecycle_recovers_each_changed_page(self):
        form = ("<p class='changed-unit'>売買単位：100</p><label>注文数量<input></label><label>指値<input></label>"
                "<label>取引パスワード<input type='password'></label>"
                "<label>確認画面を省略<input type='checkbox'></label><button>発注</button>")
        orders = ("<section><h1>注文照会</h1><h2>注文受付</h2><table><thead><tr>"
                  "<th>銘柄名</th><th>銘柄コード</th><th>操作</th></tr></thead><tbody>"
                  "<tr><td>非公開銘柄</td><td>1111</td><td><button>取消</button></td></tr></tbody></table></section>")
        cancel = "<label>取引パスワード<input type='password'></label><button>取消する</button>"
        empty = "<section><h1>注文照会</h1><p>注文はありません</p><h2>取消完了</h2></section>"
        self.load(form)
        self.browser.execute_script("""
            const pages=arguments[0]; document.addEventListener('click', event => {
              const next=pages[event.target.textContent];
              if(next) document.querySelector('main').innerHTML=next;
            });
        """, {"発注": orders, "取消": cancel, "取消する": empty})
        stock = self.stock()
        stock._check_device_verification = Mock(return_value=False)
        stock.open_ordered_position_page = Mock(return_value=True)
        stock._get_orders_legacy = Mock(side_effect=NoSuchElementException("changed table"))
        base = "/html[1]/body[1]/main[1]/section[1]"
        def read_recipe(task, snapshot):
            if any(n["text"] == "注文はありません" for n in snapshot):
                return {"page": base + "/h1[1]", "rows": None, "empty": base + "/p[1]", "fields": []}
            return {"page": base + "/h1[1]", "rows": base + "/table[1]/tbody[1]/tr", "empty": None,
                    "fields": [{"name": name, "label": base + f"/table[1]/thead[1]/tr[1]/th[{n}]", "value": f"./td[{n}]"}
                               for n, name in enumerate(("symbol_name", "symbol_index"), 1)]}
        reader = Mock(side_effect=read_recipe)
        stock.read_recovery = ReadRecovery(reader, Path(self.temporary.name) / "reads")
        self.assertEqual(stock.filling_order_page(1, "fixture-trade-secret", price=123), (True, None))
        self.assertEqual(stock.get_orders()[0]["symbol_index"], 1111)
        self.assertTrue(stock.cancel_order("1111"))
        self.assertEqual(stock.get_orders(), [])
        self.assertEqual(reader.call_count, 2)  # nonempty and empty layouts; cancellation reused the cache

    def test_external_action_and_form_targets_are_rejected(self):
        for html in ("<a href='https://evil.example/'>発注</a>",
                     "<form action='https://evil.example/'><label>ログインパスワード<input type='password'></label></form>"):
            self.load(html)
            locator = (By.ID, "botton2") if html.startswith("<a") else (By.NAME, "password")
            with self.assertRaises(OperationRecoveryError):
                self.driver.find_element(*locator)

    def test_hidden_legacy_control_can_recover_visible_replacement(self):
        self.load("<input name='input_quantity' hidden><label>注文数量<input id='visible'></label>")
        self.assertEqual(self.driver.find_element(By.NAME, "input_quantity").get_attribute("id"), "visible")

    def test_stale_single_button_is_recovered_before_dispatch(self):
        self.load("<button id='pw-btn'>ログイン</button>")
        button = self.driver.find_element(By.ID, "pw-btn")
        self.browser.execute_script("document.querySelector('button').outerHTML="
                                    "'<button onclick=\"this.dataset.clicked=1\">ログイン</button>'")
        button.click()
        self.assertEqual(self.browser.find_element(By.TAG_NAME, "button").get_attribute("data-clicked"), "1")

    def test_script_action_exception_is_not_replayed(self):
        self.load("<label>注文数量<input></label>")
        element = self.driver.find_element(By.NAME, "input_quantity")
        original = self.driver.raw_driver.execute_script
        calls = []
        def failure(script, *args):
            if script == "arguments[0].click();":
                calls.append(script)
                raise WebDriverException("response lost")
            return original(script, *args)
        with patch.object(self.driver.raw_driver, "execute_script", side_effect=failure):
            with self.assertRaises(ActionOutcomeUnknown):
                self.driver.execute_script("arguments[0].click();", element)
        self.assertEqual(len(calls), 1)

    def test_selenium_expected_conditions_and_script_arguments_accept_wrapped_element(self):
        self.load("<label>注文数量<input></label>")
        element = EC.presence_of_element_located((By.NAME, "input_quantity"))(self.driver)
        self.assertEqual(self.driver.execute_script("return arguments[0].tagName", element), "INPUT")

    def test_screening_navigation_recovers_changed_menus(self):
        self.load("<button onclick=\"document.querySelector('main').innerHTML='<button>銘柄検索</button>'\">国内株式</button>")
        self.browser.execute_script("""
            document.addEventListener('click', e => {
              if(e.target.textContent==='銘柄検索') document.querySelector('main').innerHTML='<button>銘柄スクリーニング</button>';
              else if(e.target.textContent==='銘柄スクリーニング') document.querySelector('main').innerHTML='<h1>銘柄スクリーニング</h1>';
            });
        """)
        screening = object.__new__(StatefulScreening)
        screening.driver = self.driver
        self.assertTrue(screening.transit_to_screening_page())
        self.assertEqual(self.browser.find_element(By.TAG_NAME, "h1").text, "銘柄スクリーニング")

    def test_common_header_recovers_changed_container(self):
        self.load("<button onclick='this.dataset.clicked=1'>ホーム</button>")
        stock = self.stock()
        self.assertTrue(stock._STOCK__header_bar_handler(sbi_enum.COMMON_HEADER.HOME))
        self.assertEqual(self.browser.find_element(By.TAG_NAME, "button").get_attribute("data-clicked"), "1")

    def test_security_search_and_buy_navigation_recover_changed_controls(self):
        self.load("<form onsubmit=\"event.preventDefault();document.querySelector('main').innerHTML="
                  "'<h1>国内株式</h1><button>現物買</button>'\"><label>銘柄検索<input></label></form>")
        stock = self.stock()
        with patch("sbi_util.rpa.sleep"):
            self.assertTrue(stock._STOCK__open_symbol_page("1111"))
        self.assertTrue(stock._STOCK__transit_symbol_page_to_order_page(sbi_enum.buy))

    def test_security_search_replaces_a_remembered_symbol_instead_of_appending(self):
        self.load("<h1 class='head01'>注文照会</h1><form>"
                  "<label>銘柄検索<input id='brand-search-text' value='9999'></label></form>")
        self.browser.execute_script("""
            document.querySelector('form').onsubmit=e=>{
              e.preventDefault(); window.searchCalls=(window.searchCalls||0)+1;
              window.lastQuery=document.querySelector('input').value;
              document.querySelector('main').innerHTML=window.lastQuery==='1111'
                ? '<h1 class="head01">国内株式</h1><button>現物買</button>'
                : '<h1 class="head01">検索結果</h1>';
            };
        """)
        stock = self.stock()
        with patch("sbi_util.rpa.sleep"):
            self.assertTrue(stock._STOCK__open_symbol_page("1111"))
        self.assertEqual(self.browser.execute_script("return window.lastQuery"), "1111")
        self.assertEqual(self.browser.execute_script("return window.searchCalls"), 1)

    def test_security_search_waits_for_response_instead_of_reading_old_heading(self):
        self.load("<h1 class='head01'>注文照会</h1><form>"
                  "<label>銘柄検索<input id='brand-search-text'></label></form>")
        self.browser.execute_script("""
            document.querySelector('form').onsubmit=e=>{
              e.preventDefault(); window.searchCalls=(window.searchCalls||0)+1;
              setTimeout(()=>document.querySelector('main').innerHTML=
                '<h1 class="head01">国内株式</h1><button>現物買</button>',300);
            };
        """)
        with patch("sbi_util.rpa.sleep"):
            self.assertTrue(self.stock()._STOCK__open_symbol_page("1111"))
        self.assertEqual(self.browser.execute_script("return window.searchCalls"), 1)

    def test_security_search_does_not_accept_a_previous_security_page(self):
        self.load("<h1 class='head01'>国内株式</h1><p>9999</p><form>"
                  "<label>銘柄検索<input id='brand-search-text'></label></form>")
        self.browser.execute_script("""
            document.querySelector('form').onsubmit=e=>{
              e.preventDefault(); window.searchCalls=(window.searchCalls||0)+1;
              setTimeout(()=>{
                document.querySelector('main').innerHTML='<h1 class="head01">国内株式</h1><p>1111</p>';
                window.responseLoaded=true;
              },300);
            };
        """)
        self.assertTrue(self.stock()._STOCK__open_symbol_page("1111"))
        self.assertTrue(self.browser.execute_script("return window.responseLoaded===true"))
        self.assertEqual(self.browser.execute_script("return window.searchCalls"), 1)

    def test_missing_security_page_is_reported_before_any_order_form_action(self):
        stock = self.stock()
        stock._STOCK__check_login = Mock(return_value=True)
        stock._STOCK__open_symbol_page = Mock(return_value=False)
        stock._STOCK__transit_symbol_page_to_order_page = Mock()
        stock.filling_order_page = Mock()
        with self.assertRaises(OrderPreparationError) as caught:
            stock.buy_order("1111", 1, order_price=123)
        self.assertTrue(caught.exception.order_not_submitted)
        stock._STOCK__transit_symbol_page_to_order_page.assert_not_called()
        stock.filling_order_page.assert_not_called()


# Patch the base implementation; wrapper must call it at most once.
from selenium.webdriver.remote.webelement import WebElement as WebElementForTest


class AdapterTests(unittest.TestCase):
    def test_request_is_structured_without_storage_or_tools(self):
        client = Mock()
        client.responses.create.return_value = Mock(status="completed", output_text='{"paths": []}')
        self.assertEqual(OpenAILocatorProposer("fixture-model", client)({}, [], False), {"paths": []})
        request = client.responses.create.call_args.kwargs
        self.assertFalse(request["store"])
        self.assertTrue(request["text"]["format"]["strict"])
        self.assertNotIn("tools", request)

    def test_env_switch_and_explicit_disable(self):
        import os
        with patch.dict(os.environ, {"SBI_AI_HEALING": "1", "SBI_AI_OPERATION_HEALING": "0"}):
            self.assertIsNone(SeleniumRecovery.from_env())
        driver = Mock()
        self.assertIs(enable_recovery(driver, False), driver)


if __name__ == "__main__":
    unittest.main()
