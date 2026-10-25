"""Observe SBI phone authentication; never initiate a call or replay a trade."""

import math
import os
import time
from urllib.parse import urlparse

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from .selenium_recovery import ActionOutcomeUnknown, HealingDriver, OperationRecoveryError
from .read_recovery import normalize


class PhoneAuthenticationRequired(ActionOutcomeUnknown):
    phone_authentication_state = "required"


class PhoneAuthenticationTimeout(PhoneAuthenticationRequired):
    phone_authentication_state = "timeout"


class PhoneAuthenticationFailed(PhoneAuthenticationRequired):
    phone_authentication_state = "failed"


PHONE_STATE = r"""
const visible = e => e.getClientRects().length && getComputedStyle(e).visibility !== 'hidden';
const norm = s => (s || '').normalize('NFKC').replace(/\s+/g,'').replace(/[。:]+$/,'');
// Text and numbers stay in the browser. Return classification flags only.
const text = norm(document.body?.innerText);
const phone = /認証用電話番号|電話番号による認証|電話による認証/.test(text);
const instruction = /発信|電話をかけ|お電話/.test(text);
const buttons = [...document.querySelectorAll('button,a,input[type=submit],input[type=button],[role=button]')]
  .filter(visible).filter(e => ['認証する','認証'].includes(norm(e.innerText || e.value || e.getAttribute('aria-label') || e.querySelector('img')?.alt)));
const content = [...document.querySelectorAll('h1,h2,h3,p,div,span,td,b,strong,img')]
  .filter(visible).filter(e => !e.closest('a,button,nav,header,[role=navigation]'));
const heading = content.some(e => ['追加認証','電話番号認証','電話による認証','取引時認証','取引時認証(リスクベース認証)']
  .includes(norm(e.innerText || e.getAttribute('alt'))));
const required = phone && instruction && (heading || buttons.length > 0);
return {required, expired: required && /有効期限切れ|認証期限切れ|認証時間を超過|認証の有効期限が切れ/.test(text), button_count: buttons.length};
"""


def phone_authentication_state(driver):
    raw = driver.raw_driver if isinstance(driver, HealingDriver) else driver
    for location in (raw.current_url, raw.execute_script("return window.location.href")):
        url = urlparse(location)
        host = url.hostname or ""
        if (url.scheme != "https" or not (host == "sbisec.co.jp" or host.endswith(".sbisec.co.jp"))
                or url.username or url.password or url.port not in (None, 443)):
            raise OperationRecoveryError("Phone authentication requires an HTTPS SBI document and frame")
    state = raw.execute_script(PHONE_STATE)
    if (not isinstance(state, dict) or set(state) != {"required", "expired", "button_count"}
            or type(state["required"]) is not bool or type(state["expired"]) is not bool
            or type(state["button_count"]) is not int or state["button_count"] < 0):
        raise OperationRecoveryError("Phone authentication state could not be read")
    return state


def phone_authentication_pending(driver):
    return phone_authentication_state(driver)["required"]


def _auth_button_caption(element):
    images = element.find_elements(By.TAG_NAME, "img")
    return normalize(element.text or element.get_attribute("value") or element.get_attribute("aria-label")
                     or (images[0].get_attribute("alt") if len(images) == 1 else ""))


def complete_phone_authentication(driver, handler, *, notify=None, timeout=None):
    """User confirms their phone call; click authentication once, then observe only."""
    def report(event):
        if notify is not None:
            try:
                notify(event)
            except Exception:
                pass
    state = phone_authentication_state(driver)
    if not state["required"]:
        return
    report("required")
    if state["expired"]:
        report("timeout")
        raise PhoneAuthenticationTimeout("Phone authentication timed out; no submission was replayed")
    if handler is None:
        raise PhoneAuthenticationRequired("Phone authentication requires a call from the registered phone")
    if state["button_count"] != 1:
        report("failed")
        raise PhoneAuthenticationFailed("Phone authentication button is missing or ambiguous")
    seconds = float(os.getenv("SBI_PHONE_AUTH_TIMEOUT_SECONDS", "180")) if timeout is None else timeout
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("Phone authentication timeout must be positive and finite")
    deadline = time.monotonic() + min(seconds, 180)
    if handler(max(0, deadline - time.monotonic())) is not True or time.monotonic() >= deadline:
        report("timeout")
        raise PhoneAuthenticationTimeout("Phone authentication timed out; no submission was replayed")
    state = phone_authentication_state(driver)
    if state["required"]:
        if state["expired"]:
            report("timeout")
            raise PhoneAuthenticationTimeout("Phone authentication timed out; no submission was replayed")
        if state["button_count"] != 1:
            report("failed")
            raise PhoneAuthenticationFailed("Phone authentication button is missing or ambiguous")
        if isinstance(driver, HealingDriver):
            originals = [e for e in driver.raw_driver.find_elements(By.ID, "deviceButton")
                         if e.is_displayed() and _auth_button_caption(e) in ("認証する", "認証")]
            button = (driver.find_element(By.ID, "deviceButton") if len(originals) == 1
                      else driver.find_semantic(("認証する", "認証")))
        else:
            buttons = [e for e in driver.find_elements(By.CSS_SELECTOR, "button,a,input[type=submit],input[type=button],[role=button]")
                       if e.is_displayed() and _auth_button_caption(e) in ("認証する", "認証")]
            if len(buttons) != 1:
                raise PhoneAuthenticationFailed("Phone authentication button is missing or ambiguous")
            button = buttons[0]
        latest = phone_authentication_state(driver)
        if time.monotonic() >= deadline or latest["expired"]:
            report("timeout")
            raise PhoneAuthenticationTimeout("Phone authentication timed out; no submission was replayed")
        if not latest["required"]:
            report("completed")
            return  # The user may have confirmed in the browser during recovery.
        button.click()  # This can complete a pending order. Never repeat it.
        report("confirmation_clicked")
        def completed(document):
            current = phone_authentication_state(document)
            if current["expired"]:
                raise PhoneAuthenticationTimeout("Phone authentication expired on the SBI page")
            return not current["required"]
        try:
            WebDriverWait(driver, max(0, deadline - time.monotonic())).until(completed)
        except PhoneAuthenticationTimeout:
            report("timeout")
            raise
        except TimeoutException:
            if time.monotonic() >= deadline:
                report("timeout")
                raise PhoneAuthenticationTimeout("Phone authentication timed out; no submission was replayed") from None
            report("failed")
            raise PhoneAuthenticationFailed("Phone authentication did not complete; no submission was replayed") from None
    report("completed")
