import os
import re
import hashlib
import json
from decimal import Decimal, InvalidOperation
from time import sleep

from selenium import webdriver
from selenium.common import exceptions
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from . import auth, sbi_enum, utils
from .mail import gmail
from .env import load_sbi_env
from .read_recovery import ReadRecovery, ReadRecoveryError
from .selenium_recovery import ActionOutcomeUnknown, HealingDriver, OperationRecoveryError, SUBMISSION_LABELS, enable_recovery
from .phone_auth import (PhoneAuthenticationRequired, complete_phone_authentication,
                         phone_authentication_pending)
from .chrome_session import open_chrome


class OrderPreparationError(OperationRecoveryError):
    """Order-page preparation stopped before the final submission action."""
    order_not_submitted = True


class STOCK:
    order_types = {}

    def __init__(self, id=None, password=None, trading_pass=None, read_recovery=None, read_only=None, auto_login=True, operation_recovery=None, phone_auth_handler=None) -> None:
        load_sbi_env()
        id = os.getenv("sbi_id") if id is None else id
        password = os.getenv("sbi_password") if password is None else password
        trading_pass = os.getenv("sbi_trade_password") if trading_pass is None else trading_pass
        self.read_recovery = read_recovery if read_recovery is not None else ReadRecovery.from_env()
        self.read_only = (self.read_recovery is not None) if read_only is None else read_only
        self.auto_login = auto_login
        self.operation_recovery = operation_recovery
        self.phone_auth_handler = phone_auth_handler
        self.id, self.pa = id, password
        self.logger = utils.setup_logger()
        self.driver = open_chrome()
        self.driver = enable_recovery(self.driver, operation_recovery)
        self.driver.execute_cdp_cmd("Network.setBlockedURLs", {
            "urls": ["*analytics.tiktok.com*"]
        })
        self.driver.execute_cdp_cmd("Network.enable", {})
        self.driver.implicitly_wait(5)
        self.driver.get(utils.URL)
        self.logger.debug("start initial login process.")
        if auto_login:
            auth.login(driver=self.driver, id=id, password=password)
        self.trading_pass = trading_pass

    def __check_login(self, retry_count=0):
        """
        return False if newly logged in
        """
        if not getattr(self, "auto_login", True):
            try:
                ReadRecovery._host(self.driver)
                return True
            except ReadRecoveryError:
                return False
        # this take 10 sec.
        # if self.is_driver_available() is False:
        #     self.open()
        if auth.is_logged_in(self.driver) is False:
            if auth.login(driver=self.driver, id=self.id, password=self.pa):
                return True
            elif retry_count < 5:
                # try opining selenium again
                self.driver.quit()
                self.__init__(self.id, self.pa, self.trading_pass,
                              read_recovery=self.read_recovery, read_only=self.read_only,
                              operation_recovery=self.operation_recovery, phone_auth_handler=self.phone_auth_handler)
                return self.__check_login(retry_count + 1)
            else:
                print("Failed to check login state.")
                return False
        return True

    def is_driver_available(self):
        try:
            self.driver.current_url
            return True
        except ActionOutcomeUnknown:
            raise
        except Exception:
            print("driver is unavailable.")
            return False

    def is_login_page(self):
        target_name = "ACT_login"
        try:
            login_ele = self.driver.find_element(By.NAME, target_name)
            if login_ele:
                return True
        except ActionOutcomeUnknown:
            raise
        except Exception:
            return False
    
    def _check_device_verification(self):
        try:
            sleep(3)  # wait for device verification element to load
            if isinstance(self.driver, HealingDriver) and not self.driver.find_elements(By.ID, "deviceCodeInput"):
                return False
            device_element = self.driver.find_element(By.ID, "deviceButton")
            if device_element:
                self.logger.debug("need to pass device verification")
                return True
            return False
        except exceptions.NoSuchElementException:
            return False
        except ActionOutcomeUnknown:
            raise
        except Exception as e:
            self.logger.error(f"error happened on _check_device_verification: {e}")
            return False
    
    def _input_additional_device_code(self):
        # If anyone want to use other mail provider, need to overwrite the method
        sleep(5)  # wait for email to arrive
        code = gmail.retrieve_sbi_device_code()
        if code:
            self.logger.debug("got device code from email")
            code_input = self.driver.find_element(By.ID, "deviceCodeInput")
            code_input.clear()
            code_input.send_keys(str(code).strip())
            verify_btn = self.driver.find_element(By.ID, "deviceButton")
            verify_btn.click()

            self.logger.debug("waiting for device verification")
            error_txt_ele = self.driver.find_elements(By.CLASS_NAME, "fRed01")
            if error_txt_ele:
                self.logger.error(f"failed to pass device verification: {error_txt_ele[0].text}")
                return False
            # wait until device verification is completed
            try:
                section = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//h1"))
                )
                if "失敗" in section.text:
                    self.logger.error("failed to pass device verification. It may be timed out.")
                    return False
            except ActionOutcomeUnknown:
                raise
            except Exception as e:
                pass
            self.logger.info("device verification is completed.")
            return True
        self.logger.error("failed to get device code")
        return False

    def __open_symbol_page(self, symbol: str) -> bool:
        try:
            wait = WebDriverWait(self.driver, 20)
            form_ele = wait.until(
                EC.presence_of_element_located((By.ID, "brand-search-text"))
            )
            self.logger.debug("found an element to search symbol")
            form_ele.clear()
            form_ele.send_keys(str(symbol))
            if form_ele.get_property("value") != str(symbol):
                raise OrderPreparationError("The security search field did not retain the supplied value")
            raw = self.driver.raw_driver if isinstance(self.driver, HealingDriver) else self.driver
            previous = {(e.id, e.text) for e in raw.find_elements(By.CLASS_NAME, "head01") if e.is_displayed()}
            form_ele.send_keys(Keys.ENTER)
            # Presence alone may return the previous screen's heading. Wait for
            # a new/changed result heading; dispatch the search only once.
            def search_result(driver):
                try:
                    for heading in driver.find_elements(By.CLASS_NAME, "head01"):
                        text = heading.text
                        if not heading.is_displayed() or (heading.id, text) in previous:
                            continue
                        if "検索結果" in text:
                            return "candidates"
                        if "国内株式" in text:
                            return "security"
                except exceptions.StaleElementReferenceException:
                    pass  # Navigation replaced the document while it was read.
                return False
            result = wait.until(search_result)
            # If there are some candidates or no candidate for a symbol text, search result page is shown
            if result == "candidates":
                self.logger.warning("Security search returned a candidates page")
                return False
            # If one candidate
            elif result == "security":
                self.logger.debug("found a symbol")
                return True
            else:
                self.logger.warning("failed to load the symbol page")
                return False
        except ActionOutcomeUnknown:
            raise
        except Exception as e:
            self.logger.error("Security search failed: %s", type(e).__name__)
            self._record_preparation_error(e)
            return False

    def _record_preparation_error(self, error):
        callback = (getattr(self.driver.recovery, "on_preparation_error", None)
                    if isinstance(self.driver, HealingDriver) else None)
        if callable(callback):
            try:
                callback(error)
            except Exception:
                pass

    def __transit_symbol_page_to_order_page(self, order_type) -> bool:
        result = False
        try:
            wait = WebDriverWait(self.driver, 20)
            header_ele = wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "head01"))
            )
            # header_ele = self.driver.find_element(By.CLASS_NAME, "head01")
            if header_ele.text == "国内株式":
                candidate_eles = self.driver.find_elements(By.CLASS_NAME, sbi_enum.ORDER_BUY_CLASS)
                target_txt = sbi_enum.symbol_page_to_order_element[order_type]
                for candidate in candidate_eles:
                    text = candidate.text
                    if text:
                        if text == target_txt:
                            candidate.click()
                            result = True
                            break
                if result is False:
                    if isinstance(self.driver, HealingDriver):
                        self.driver.find_semantic((target_txt,)).click()
                        return True
                    print(f"no elemnt for {order_type}")
            else:
                print("header is not found.")
            return result
        except ActionOutcomeUnknown:
            raise
        except Exception as e:
            self.logger.error("Order-page navigation failed: %s", type(e).__name__)
            self._record_preparation_error(e)
            return False

    def filling_order_page(self, volume, password, market=False, price=None, *, submit=True):
        """order the symbol
            if market is True or price is None, order market buy/sell

        Args:
            volume (int): number of unit to order. Ex) If Unit 100, volume=3 means 300.
            password (str): password to order
            market (bool, optional): Option to order with market price. Defaults to False.
            price (float, optional): Price to order. Defaults to None.
            submit (bool): False verifies the filled form and stops before clicking submit.

        Returns:
            bool: suc or not
            str: error text. if succeed, None
        """
        self._require_trading()
        if phone_authentication_pending(self.driver):
            raise PhoneAuthenticationRequired("Phone authentication is already pending; no new order was sent")
        if self._check_device_verification():
            success = self._input_additional_device_code()
            if success is False:
                error_txt = "failed to pass device verification"
                self.logger.error(error_txt)
                return False, error_txt
        target_class_name = "mtext"
        wait = WebDriverWait(self.driver, 10)
        mtexts_eles = wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, target_class_name))
        )
        if mtexts_eles:
            # input volume
            self.logger.debug("start filling to sell")
            mtexts_eles = self.driver.find_elements(By.CLASS_NAME, target_class_name)
            order_unit = None
            for ele in mtexts_eles:
                if "売買単位" in ele.text:
                    texts = [ele.text]
                    if isinstance(self.driver, HealingDriver):
                        texts.append(ele.find_element(By.XPATH, "..").text)
                    for text in texts:
                        match = re.fullmatch(r"\s*売買単位\s*[：:]?\s*([0-9,]+)\s*株?\s*", text)
                        if match:
                            order_unit = int(match[1].replace(",", ""))
                            break
                    if order_unit is not None:
                        break
            if order_unit is None or order_unit <= 0:
                raise OperationRecoveryError("Order unit could not be verified")
            target_name = "input_quantity"
            input_ele = self.driver.find_element(By.NAME, target_name)
            input_ele.clear()
            input_ele.send_keys(volume * order_unit)
            self.logger.debug("filled order volume")
            if market is True or price is None:
                market_rbutton = "nariyuki"
                market_rb_ele = self.driver.find_element(By.ID, market_rbutton)
                market_rb_ele.click()
                self.logger.debug("nariyuki order")
            else:
                # input price
                target_name = "input_price"
                price_ele = self.driver.find_element(By.NAME, target_name)
                price_ele.clear()
                price_ele.send_keys(price)
                self.logger.debug("filled order price")
            pwd3_input = "pwd3"
            pwd3_rb_ele = self.driver.find_element(By.ID, pwd3_input)
            pwd3_rb_ele.clear()
            pwd3_rb_ele.send_keys(password)
            self.logger.debug("filled order password")

            omit_cbox = "shouryaku"
            omit_cb_ele = self.driver.find_element(By.ID, omit_cbox)
            if not omit_cb_ele.is_selected():
                omit_cb_ele.click()
            self.logger.debug("enabled omit option")

            order_wo_conf = "botton2"
            order_btn_ele = self.driver.find_elements(By.ID, order_wo_conf)
            if not submit:
                if len(order_btn_ele) != 1:
                    raise OperationRecoveryError("Preparation requires one unambiguous submission button")
                def same_number(element, expected):
                    try:
                        return Decimal(str(element.get_property("value")).replace(",", "")) == Decimal(str(expected))
                    except InvalidOperation:
                        return False
                quantity = self.driver.find_element(By.NAME, "input_quantity")
                password_field = self.driver.find_element(By.ID, pwd3_input)
                if not same_number(quantity, volume * order_unit) or password_field.get_property("value") != password:
                    raise OperationRecoveryError("Prepared quantity or password did not retain the supplied value")
                if market or price is None:
                    price_verified = self.driver.find_element(By.ID, "nariyuki").is_selected()
                else:
                    price_verified = same_number(self.driver.find_element(By.NAME, "input_price"), price)
                if not price_verified or not self.driver.find_element(By.ID, omit_cbox).is_selected():
                    raise OperationRecoveryError("Prepared price or confirmation setting was not verified")
                WebDriverWait(self.driver, 10).until(
                    lambda _: order_btn_ele[0].is_displayed() and order_btn_ele[0].is_enabled())
                # This branch never dispatches the final click or changes the outcome latch.
                return True, None
            if isinstance(self.driver, HealingDriver):
                if len(order_btn_ele) != 1:
                    raise OperationRecoveryError("Order submission requires one unambiguous button")
                self._pending_trade_outcome = True
            order_btn_ele[0].click()
            self.logger.debug("clicked to order")
            if self._check_device_verification():
                success = self._input_additional_device_code()
                if success is False:
                    error_txt = "failed to pass device verification"
                    self.logger.error(error_txt)
                    return False, error_txt

            if isinstance(self.driver, HealingDriver):
                return self._wait_submission_result("order")
            if phone_authentication_pending(self.driver):
                complete_phone_authentication(self.driver, getattr(self, "phone_auth_handler", None))

            # check if order is completed
            target_name = "md-l-table-01"
            error_class = "fRed01"
            invalid_ele = self.driver.find_elements(By.CLASS_NAME, error_class)
            if invalid_ele:
                error_txt = invalid_ele[0].text
                if sbi_enum.ERROR_BUDGET_IS_SHORT in error_txt:
                    return False, sbi_enum.ERROR_BUDGET_IS_SHORT
                else:
                    return False, error_txt
            result_ele = self.driver.find_elements(By.CLASS_NAME, target_name)
            if result_ele:
                self.logger.info("order is completed.")
                return True, None

            error_txt = "failed to order with unkown issue."
            self.logger.error(error_txt)
            return False, error_txt
        else:
            error_txt = "can't find unit text"
            self.logger.error(f"failed to order: {error_txt}")
            return False, error_txt

    # Need to add try catch to avoid an error when handlers are called on different pages
    def __trade_page_handler(self, index: sbi_enum.TRADE_HEADER):
        """click header of trade page

        Args:
            index (TRADE_HEADER): index number of header position
        """
        if index.value >= 0 and index.value <= 9:
            self.__check_login()
            try:
                self.logger.debug("try to click header of trade page")
                # open trade page from header
                header_ele = self.driver.find_element(
                    By.CLASS_NAME, "slc-header-nav-lower-menu"
                )
                header_eles = header_ele.find_element(By.XPATH, "ul").find_elements(
                    By.XPATH, "li"
                )
                # 0: ポートフォリト, 1: 口座管理, 2: 取引,
                header_eles[2].find_element(By.XPATH, ".//div/a").click()
            except ActionOutcomeUnknown:
                raise
            except Exception as e:
                self.logger.error(f"failed to open trade page: {e}")
                if not isinstance(self.driver, HealingDriver):
                    return False
                self.driver.find_semantic(("取引",)).click()

            if getattr(self, "read_recovery", None) is not None and index in (
                sbi_enum.TRADE_HEADER.OWNED_STOCKS, sbi_enum.TRADE_HEADER.ORDER_INQUIRY
            ):
                task = "positions" if index == sbi_enum.TRADE_HEADER.OWNED_STOCKS else "orders"
                try:
                    self.read_recovery.navigate(self.driver, task)
                    return True
                except ReadRecoveryError:
                    return False

            try:
                wait = WebDriverWait(self.driver, 10)
                table_ele = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "md-l-mainarea-01"))
                )
                tr_ele = table_ele.find_element(By.XPATH, ".//table/tbody/tr")
                td_eles = tr_ele.find_elements(By.XPATH, "td")
                href = td_eles[index.value].find_element(By.XPATH, "a")
                # sometimes header dropdown keep shown. close it by click top left
                ActionChains(self.driver).move_by_offset(0, 0).click().perform()
                sleep(0.3)
                href.click()
            except ActionOutcomeUnknown:
                raise
            except Exception as e:
                self.logger.error(f"failed to open {index} item: {e}")
                if not isinstance(self.driver, HealingDriver):
                    return False
                labels = {
                    0: ("新規注文",), 1: ("PTS",), 2: ("積立",), 3: ("信用返済",),
                    4: ("保有株式", "保有銘柄"), 5: ("注文照会", "注文照会(取消・訂正)"),
                    6: ("IPO・PO",), 7: ("立会外",), 8: ("単元未満株",), 9: ("テーマ投資",),
                }
                self.driver.find_semantic(labels[index.value]).click()

            return True
        self.logger.error(f"index should be 0 to 9: {index}")
        return False

    def __header_bar_handler(self, option: sbi_enum.COMMON_HEADER):
        index = option.value

        if index >= 0 and index <= 13:
            self.logger.debug(f"open {index} of header bar")
            # header may exist on any page, so click it without page transition.
            target_class_name = "slc-global-nav-container"
            try:
                header_ele = self.driver.find_element(By.CLASS_NAME, target_class_name)
                lis = header_ele.find_element(By.XPATH, "ul").find_elements(
                    By.XPATH, "li"
                )
                lis[index].click()
                return True
            except ActionOutcomeUnknown:
                raise
            except Exception as e:
                self.logger.error(e)
                if not isinstance(self.driver, HealingDriver):
                    return False
                labels = (("ホーム",), ("マーケット",), ("国内株式",), ("外国株式",),
                          ("投資信託",), ("債券",), ("FX",), ("オプション",), ("CFD",),
                          ("金",), ("NISA",), ("iDeCo",), ("その他",))
                if index >= len(labels):
                    return False
                self.driver.find_semantic(labels[index]).click()
                return True
        else:
            self.logger.error("index should be 0 to 13.")
            return False

    def __handle_symbol_page_header(self, index: int):
        if index <= 8 and index >= 0:
            if isinstance(self.driver, HealingDriver):
                labels = (("株価",), ("ニュース",), ("チャート",), ("投資情報",), ("四季報",),
                          ("業績",), ("コンセンサス",), ("財務",), ("分析",))
                self.driver.find_semantic(labels[index]).click()
                return True
            wait = WebDriverWait(self.driver, 5)
            header_tr = wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "tab02T"))
            )
            self.logger.debug("found header for symbol information")
            first_tr = header_tr.find_element(By.XPATH, ".//table/tbody/tr")
            tds = first_tr.find_elements(By.XPATH, "td")
            tds[index].click()
            if index == 0:
                ele = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "kabuNowStatus"))
                )
            elif index == 1:
                ele = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "newsMain01"))
                )
            elif index == 2:
                ele = wait.until(
                    EC.presence_of_element_located((By.ID, "CONTENTSAREA01"))
                )
            elif index == 3:
                ele = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "mgt20"))
                )
                # case when review is not provided
                # wait.until(EC.presence_of_element_located((By.CLASS_NAME, "mtext-info-w")))
            elif index == 4:
                ele = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "shikihouBox01"))
                )
            elif index == 5:
                ele = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "summary_left"))
                )
            elif index == 6:
                ele = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "stock_chart_disc"))
                )
            elif index == 7:
                ele = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "accTbl01"))
                )
            elif index == 8:
                ele = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "Analysis"))
                )
            return True
        else:
            print("index should be 0 to 8")
            return False

    def open_position_page(self):
        return self.__trade_page_handler(sbi_enum.TRADE_HEADER.OWNED_STOCKS)

    def open_ordered_position_page(self):
        return self.__trade_page_handler(sbi_enum.TRADE_HEADER.ORDER_INQUIRY)

    def get_positions(self, symbols=None) -> list:
        recovery = getattr(self, "read_recovery", None)
        if recovery is None:
            return self._get_positions_legacy(symbols)
        try:
            positions = self._get_positions_legacy()
            self._validate_legacy_read("positions", positions)
        except (exceptions.WebDriverException, ReadRecoveryError, ValueError, TypeError, IndexError, KeyError, UnboundLocalError):
            positions = self._recover_read("positions")
        # A recovered read must never hand actionable elements to trade methods.
        positions = [dict(position, buy_link=None, sell_link=None) for position in positions]
        if symbols is None:
            return positions
        requested = symbols if isinstance(symbols, list) else [symbols]
        return [p for p in positions if any(str(s) in (p["symbol_name"], str(p["symbol_index"])) for s in requested)]

    def _recover_read(self, task):
        if not self.read_recovery.is_page(self.driver, task):
            self.read_recovery.navigate(self.driver, task)
        self.logger.info("Attempting validated layout recovery for %s", task)
        return self.read_recovery.read(self.driver, task)

    @staticmethod
    def _validate_legacy_read(task, result):
        if task == "budget":
            if type(result) is not int or result < 0:
                raise ReadRecoveryError("Legacy budget could not be read")
            return
        if not result:
            raise ReadRecoveryError("An empty list requires an explicit empty-state check")
        for item in result:
            code = str(item.get("symbol_index", ""))
            if len(code) != 4 or not code[0].isdigit() or not code.isalnum() or not item.get("symbol_name"):
                raise ReadRecoveryError("Invalid security identification")
            if task == "positions":
                for key in ("holding", "bought_price", "current_price"):
                    value = item.get(key)
                    if type(value) not in (int, float) or not 0 <= value <= 10**15:
                        raise ReadRecoveryError("Invalid position value")
                if type(item["holding"]) is not int:
                    raise ReadRecoveryError("Invalid holding quantity")

    def _require_trading(self):
        if getattr(self, "read_only", False):
            raise ReadRecoveryError("Trading is disabled in read-only mode")
        if getattr(self, "_pending_trade_outcome", False):
            raise ActionOutcomeUnknown("The previous submission outcome must be checked before another trade")

    def _security_action(self, symbol, labels):
        """Bind the action to exactly one security row locally; never disclose it to AI."""
        token = str(symbol)
        pattern = re.compile(r"(?<![0-9A-Za-z])" + re.escape(token) + r"(?![0-9A-Za-z])")
        rows = [row for row in self.driver.find_elements(By.CSS_SELECTOR, "tr,[role='row'],.seeds-table-row")
                if row.is_displayed() and pattern.search(row.text)]
        if len(rows) != 1:
            raise OperationRecoveryError("Security action requires exactly one matching row")
        return self.driver.find_semantic(labels, root=rows[0])

    def _wait_submission_result(self, kind):
        labels = SUBMISSION_LABELS[kind]
        def result(driver):
            if phone_authentication_pending(driver):
                raise PhoneAuthenticationRequired("Phone authentication is required for the pending submission")
            errors = driver.find_elements(By.CLASS_NAME, "fRed01")
            for error in errors:
                if error.is_displayed() and error.text.strip():
                    return False, error.text
            if driver.find_semantic(labels, tag="content", many=True):
                return True, None
            return False
        handled_phone = False
        while True:
            try:
                outcome = WebDriverWait(self.driver, 10).until(result)
                break
            except PhoneAuthenticationRequired:
                if handled_phone:
                    self._record_submission_result(kind, "phone_authentication_repeated")
                    raise PhoneAuthenticationRequired("Phone authentication is still pending; no submission was replayed") from None
                handled_phone = True
                complete_phone_authentication(self.driver, getattr(self, "phone_auth_handler", None),
                    notify=lambda event: self._record_submission_result(kind, "phone_authentication_" + event))
                # The phone wait has its own deadline. Start a fresh result wait,
                # continuing the original submission rather than sending another.
            except (exceptions.WebDriverException, ReadRecoveryError) as error:
                self._record_submission_result(kind, "unknown", error)
                raise ActionOutcomeUnknown("Submission result is not verified; inspect orders before retrying") from None
        self._record_submission_result(kind, "accepted" if outcome[0] else "rejected")
        self._pending_trade_outcome = False
        return outcome

    def _record_submission_result(self, kind, outcome, error=None):
        """Capture public labels and structure before the result page is closed."""
        recovery = self.driver.recovery
        callback = recovery.on_submission_result
        if callback is None:
            return
        record = {"kind": kind, "outcome": outcome}
        if error is not None:
            record["error_type"] = type(error).__name__
        try:
            recovery.trusted(self.driver.raw_driver)
            nodes = recovery.snapshot(self.driver.raw_driver, None)
            record.update(public_labels=sorted({label for node in nodes for label in node["labels"]}),
                          node_count=len(nodes),
                          layout_fingerprint=hashlib.sha256(json.dumps(nodes, sort_keys=True).encode()).hexdigest())
        except Exception as snapshot_error:
            record["snapshot_error_type"] = type(snapshot_error).__name__
        try:
            callback(**record)
        except Exception:
            pass  # Diagnostics must never change or replay a submitted operation.

    def _get_positions_legacy(self, symbols=None) -> list:
        if self.open_position_page():
            positions = []
            li_eles = self.driver.find_elements(By.CLASS_NAME, "seeds-table-row")
            self.logger.debug(f"found {len(li_eles)} symbols.")
            for li in li_eles:
                position = {}
                tds = li.find_elements(By.XPATH, "div")
                # get symbol name
                name_ele = tds[0].find_element(By.XPATH, ".//div/a")
                symbol_name = name_ele.text
                self.logger.debug(f"got symbol name: {symbol_name}")
                symbol_name = symbol_name.replace(" ", "")
                # get index number of the symbol
                index_ele = tds[0].find_element(By.XPATH, ".//div/div/div")
                symbol_index = index_ele.text
                try:
                    symbol_index = int(symbol_index)
                except ActionOutcomeUnknown:
                    raise
                except Exception as e:
                    self.logger.error(f"failed to cast symbol index: {e}")

                # get holding number
                holding_number_ele = tds[1].find_element(
                    By.XPATH, ".//div/div[1]/div[1]"
                )
                total_holding_number = holding_number_ele.text
                ordering_number_ele = tds[1].find_element(
                    By.XPATH, ".//div/div[2]/div[1]"
                )
                ordering_number = ordering_number_ele.text
                self.logger.debug(
                    f"got holding info: {total_holding_number}, {ordering_number}"
                )
                ordering_number = ordering_number.replace("(", "")
                holding_number = None
                try:
                    holding_number = int(total_holding_number)
                    ordering_number = int(ordering_number)
                    holding_number -= ordering_number
                except ActionOutcomeUnknown:
                    raise
                except Exception as e:
                    self.logger.error(f"failed to cast holding numbers: {e}")

                # get prices
                price_ele = tds[2].find_element(By.XPATH, ".//div/div[1]/div[1]")
                bought_price = price_ele.text
                current_price_ele = tds[2].find_element(
                    By.XPATH, ".//div/div[2]/div[1]"
                )
                current_price = current_price_ele.text
                self.logger.debug(f"got prices: {bought_price}")
                try:
                    bought_price = bought_price.replace(",", "")
                    bought_price = float(bought_price)
                    current_price = current_price.replace(",", "")
                    current_price = float(current_price)
                except ActionOutcomeUnknown:
                    raise
                except Exception as e:
                    self.logger.error(f"failed to convert prices: {e}")

                links_container = tds[5].find_element(By.XPATH, ".//div/div")
                links = links_container.find_elements(By.XPATH, "a")
                buy_ele = links[0]
                sell_ele = links[1]
                self.logger.debug("got links")

                position["symbol_name"] = symbol_name
                position["symbol_index"] = symbol_index
                position["holding"] = holding_number
                position["bought_price"] = bought_price
                position["current_price"] = current_price
                position["buy_link"] = buy_ele
                position["sell_link"] = sell_ele

                if symbols is not None:
                    if isinstance(symbols, list):
                        if symbol_name in symbols or str(symbol_index) in symbols or int(symbol_index) in symbols:
                            positions.append(position)
                    else:
                        if symbol_name == symbols or str(symbol_index) == symbols or int(symbol_index) == symbols:
                            positions.append(position)
                else:
                    positions.append(position)
            return positions
        else:
            return []

    def get_rating(self, symbol: str) -> dict:
        """
        Args:
            symbol (str): company name or index

        Returns:
            dict: key: number of star, value: number of traders who advocate the key
        """
        try:
            self.logger.debug("start getting rate")
            if self.__open_symbol_page(symbol):
                if self.__handle_symbol_page_header(3):
                    tr_eles = self.driver.find_elements(By.CLASS_NAME, "vaT")
                    if len(tr_eles) != 8:
                        if len(tr_eles) == 3:
                            print(f"rating is not provided for {symbol}")
                            return {}
                        print(
                            f"number of element doesn't match with assumption on {symbol}. HP may be updated"
                        )
                        return {}
                    rating = {}
                    for index in range(3, 8):
                        tds = tr_eles[index].find_elements(By.XPATH, "td")
                        rating[8 - index] = int(tds[2].text)
                    return rating
                else:
                    print("couldn't click header on sygnal page.")
                    return {}
            else:
                print("failed to open symbol page.")
                return {}

        except ActionOutcomeUnknown:
            raise
        except Exception as e:
            self.logger.error(f"error happened on get_rating: {e}")

    def get_ratings(self, symbols):
        """
        Args:
            symbols (list): list of company name or index

        Returns:
            dict: key: sygnal, value: rating. If not provided, {}
        """
        ratings = {}
        if len(symbols) > 0:
            for symbol in symbols:
                self.logger.info("start getting rating")
                rating = self.get_rating(symbol)
                if type(rating) is dict:
                    ratings[symbol] = rating
                    self.logger.debug("added rating")
                else:
                    self.logger.debug(f"rating is not available for {symbol}")
        return ratings

    def buy_order(self, symbol: str, volume: int, order_price: float = None, *, submit=True):
        """

        Args:
            symbol (str): symbol to order. Allows if search result shows a page of symbol.
            volume (int): volume of order. try to buy with (volume * unit) * order_price
            order_price (float, optional): price to order. Defaults to None.
            submit (bool): False checks preparation only, without placing an order.

        Returns:
            bool: Return True if order is completed
        """
        self._require_trading()
        self.__check_login()
        if self.__open_symbol_page(symbol):
            if self.__transit_symbol_page_to_order_page(sbi_enum.buy):
                success, error = self.filling_order_page(
                    volume, password=self.trading_pass, price=order_price, submit=submit
                )
                return success
            else:
                raise OrderPreparationError("The order page was not reached; no order was submitted")
        else:
            raise OrderPreparationError("The security page was not verified; no order was submitted")

    def buy_less_than_unit(self, symbol, volume: int):
        raise Exception("Not implemented")

    def sell_to_close_buy_order(self, symbol, volume, order_price=None):
        """
        Sell to close

        Args:
            symbol (str): symbol name or id
            volume (int): volume of order. try to buy with (volume * unit) * order_price
            order_price (float, optional): price to order. Defaults to None.

        Returns:
            _type_: _description_
        """
        self._require_trading()
        try:
            positions = self._get_positions_legacy()
        except (exceptions.WebDriverException, IndexError, ValueError, KeyError):
            if not isinstance(self.driver, HealingDriver):
                raise
            positions = self.get_positions()
        if len(positions) > 0:
            matching = [p for p in positions if str(symbol) in (p["symbol_name"], str(p["symbol_index"]))]
            if len(matching) > 1:
                raise OperationRecoveryError("Multiple holdings match; selling requires an unambiguous holding")
            is_clicked = False
            for position in positions:
                name = position["symbol_name"]
                index = position["symbol_index"]
                if symbol == name or symbol == str(index) or symbol == int(index):
                    if position.get("sell_link") is None and isinstance(self.driver, HealingDriver):
                        self._security_action(symbol, ("現物売",)).click()
                    else:
                        position["sell_link"].click()
                    self.logger.debug("found symbol in position page.")
                    is_clicked = True
                    break
            if is_clicked:
                return self.filling_order_page(
                    volume, self.trading_pass, price=order_price
                )
            else:
                self.logger.warning(f"{symbol} is not found on symbol column.")
                return False, None
        else:
            self.logger.info("No position available")
            return False, None

    def get_orders(self) -> list:
        recovery = getattr(self, "read_recovery", None)
        if recovery is None:
            return self._get_orders_legacy()
        try:
            if not self.open_ordered_position_page():
                raise ReadRecoveryError("Could not open the order inquiry page")
            orders = self._get_orders_legacy()
            self._validate_legacy_read("orders", orders)
        except (exceptions.WebDriverException, ReadRecoveryError, ValueError, TypeError, IndexError, KeyError):
            orders = self._recover_read("orders")
        return [dict(order, cancel_link=None, modify_link=None) for order in orders]

    def _get_orders_legacy(self) -> list:
        target_row_class_name = "md-l-tr-01"
        wait = WebDriverWait(self.driver, 5)
        wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, target_row_class_name))
        )
        trs = self.driver.find_elements(By.CLASS_NAME, target_row_class_name)
        orders = []
        for index in range(0, len(trs), 2):
            order = {}
            tds = trs[index].find_elements(By.XPATH, "td")
            names = tds[3].text.split(" ")
            symbol_name = names[0]
            symbol_index = int(names[2])

            links = tds[5].find_elements(By.XPATH, "a")
            cancel_ele = links[0]
            modify_ele = links[1]

            order["symbol_name"] = symbol_name
            order["symbol_index"] = symbol_index
            order["cancel_link"] = cancel_ele
            order["modify_link"] = modify_ele
            orders.append(order)

        return orders

    def __click_cancel_on_cancel_page(self, password, do_print=False):
        self._require_trading()
        if phone_authentication_pending(self.driver):
            raise PhoneAuthenticationRequired("Phone authentication is already pending; no cancellation was sent")
        wait = WebDriverWait(self.driver, 5)
        try:
            pass_ele = wait.until(EC.presence_of_element_located((By.ID, "pwd3")))
        except ActionOutcomeUnknown:
            raise
        except Exception:
            print("password field is not found within timeout")
            return False
        if do_print:
            tds = self.driver.find_elements(By.CLASS_NAME, "vaM")
            for index in range(0, len(tds), 2):
                print(f"{tds[index].text}:{tds[index+1].text}")
        pass_ele.clear()
        pass_ele.send_keys(password)
        try:
            submit = self.driver.find_element(By.NAME, "ACT_place")
            if isinstance(self.driver, HealingDriver):
                self._pending_trade_outcome = True
            submit.click()
            if isinstance(self.driver, HealingDriver):
                return self._wait_submission_result("cancel")[0]
            if phone_authentication_pending(self.driver):
                complete_phone_authentication(self.driver, getattr(self, "phone_auth_handler", None))
            return True
        except ActionOutcomeUnknown:
            raise
        except Exception:
            print("cancel button is not found")
            return False

    def cancel_order(self, symbol):
        self._require_trading()
        if self.open_ordered_position_page():
            try:
                orders = self._get_orders_legacy()
            except (exceptions.WebDriverException, IndexError, ValueError, KeyError):
                if not isinstance(self.driver, HealingDriver):
                    raise
                orders = self.get_orders()
            matching = [o for o in orders if str(symbol) in (o["symbol_name"], str(o["symbol_index"]))]
            if len(matching) > 1:
                raise OperationRecoveryError("Multiple orders match; cancellation requires an unambiguous order")
            if len(orders) > 0:
                is_clicked = False
                for order in orders:
                    if (
                        (order["symbol_name"] == symbol)
                        or (str(order["symbol_index"]) == symbol)
                        or (int(order["symbol_index"]) == symbol)
                    ):
                        if order.get("cancel_link") is None and isinstance(self.driver, HealingDriver):
                            self._security_action(symbol, ("取消", "注文取消")).click()
                        else:
                            order["cancel_link"].click()
                        is_clicked = True
                        break
                if is_clicked:
                    return self.__click_cancel_on_cancel_page(self.trading_pass)
                else:
                    print(f"{symbol} is not found on symbol column.")
            else:
                print("No orders available.")
        return False

    def get_available_budget(self) -> int:
        recovery = getattr(self, "read_recovery", None)
        if recovery is None:
            return self._get_available_budget_legacy()
        try:
            budget = self._get_available_budget_legacy()
            self._validate_legacy_read("budget", budget)
            return budget
        except (exceptions.WebDriverException, ReadRecoveryError, ValueError, TypeError, IndexError, KeyError):
            return self._recover_read("budget")

    def _get_available_budget_legacy(self):
        self.logger.debug("start getting available buget")
        if self.__check_login():
            self.logger.debug("logged in state")
            if self.__header_bar_handler(sbi_enum.COMMON_HEADER.HOME):
                div = self.driver.find_element(By.CLASS_NAME, "seeds-list-form-lg")
                buget_text_element = div.find_element(
                    By.XPATH, ".//div/div[1]/div[3]/span/div/div[1]"
                )
                self.logger.debug(f"retrieved {buget_text_element.text}. Try parse it.")
                budget_txt = buget_text_element.text.replace(",", "")
                try:
                    return int(budget_txt)
                except ActionOutcomeUnknown:
                    raise
                except Exception as e:
                    print(f"unable to convert budget {budget_txt} to int: {e}")
                    return None
            else:
                self.logger.error("failed to open header bar[0]")
                return None
        else:
            return None
        
    def screening(self, keyword: str):
        pass

    def __del__(self):
        if hasattr(self, "driver"):
            self.driver.quit()
