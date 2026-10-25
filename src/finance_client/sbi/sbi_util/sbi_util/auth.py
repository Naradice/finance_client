import logging
import time
from datetime import datetime, timezone

from selenium import webdriver
from selenium.common import exceptions
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .mail import gmail
from .mail.recovery import valid_sbi_url
from .read_recovery import ReadRecovery, ReadRecoveryError
from .selenium_recovery import ActionOutcomeUnknown, HealingDriver
from .phone_auth import PhoneAuthenticationRequired, phone_authentication_pending

logger = logging.getLogger("sbirpa.auth")


def _record_failure(driver, error):
    if isinstance(driver, HealingDriver) and callable(driver.recovery.on_auth_error):
        driver.recovery.on_auth_error(error)


def _close_known_popup(driver):
    # The existing SBI mail-link flow already handles this specific promotion popup.
    buttons = [e for e in driver.find_elements(By.CSS_SELECTOR, "a.karte-close") if e.is_displayed()]
    if len(buttons) == 1:
        buttons[0].click()

def _get_device_auth_element(driver: webdriver.Chrome):
    try:
        logger.debug("checking device authorization element")
        element = driver.find_element(By.ID, "sendEmailButton")
        return element
    except Exception:
        logger.debug("device authorization element is not found")
        return None
    
def input_device_code(driver: webdriver.Chrome, code: str, *, not_before=None):
    # If anyone want to use other mail provider, need to overwrite the method
    url = gmail.retrieve_code_input_url(not_before=not_before)
    if url:
        if not valid_sbi_url(url):
            return False
        original = driver.current_window_handle
        driver.switch_to.new_window("tab")
        driver.get(url)
        if not valid_sbi_url(driver.current_url):
            driver.close()
            driver.switch_to.window(original)
            return False
        # close pop up
        try:
            close_btn = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a.karte-close"))
            )
            close_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "a.karte-close"))
            )
            close_btn.click()
        except (exceptions.NoSuchElementException, exceptions.TimeoutException):
            logger.debug("no popup to close")
            pass
        code_input = driver.find_element(By.ID, "verifyCode")
        code_input.send_keys(code)
        verify_btn = driver.find_element(By.ID, "verification")
        verify_btn.click()
        # close the tab
        driver.close()
        # switch to the original tab
        driver.switch_to.window(original)
        return True
    return False

def handle_otp(driver: webdriver.Chrome, *, not_before=None) -> bool:
    try:
        logger.debug("start input device code with gmail")
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.ID, "authCode")))
        device_element = driver.find_element(By.ID, "authCode")
        device_code = device_element.text
        checkbox = driver.find_element(By.ID, "authCheckBox")
        if checkbox and checkbox.is_selected() is False:
            checkbox.send_keys(Keys.SPACE)
        if input_device_code(driver, device_code, not_before=not_before):
            register_button = driver.find_element(By.ID, "otpRegisterButton")
            register_button.click()
            return True
        logger.error("failed to get device code")
        return False
    except gmail.GmailConfigurationError:
        raise
    except ActionOutcomeUnknown:
        raise
    except exceptions.NoSuchElementException as e:
        logger.warning("Authentication failed: %s", type(e).__name__)
        return is_logged_in(driver)
    except Exception as e:
        logger.warning("Authentication failed: %s", type(e).__name__)
        _record_failure(driver, e)
        return False

def is_logged_in(driver: webdriver.Chrome) -> bool:
    try:
        ReadRecovery._host(driver)
        return True
    except ReadRecoveryError:
        return False
    except Exception as e:
        logger.warning("Login state check failed: %s", type(e).__name__)
        return False


def _login_stage(driver):
    if isinstance(driver, HealingDriver):
        if driver.find_elements(By.ID, "otp") and driver.find_elements(By.ID, "submit-otp"):
            return "otp"
        if driver.find_elements(By.ID, "sendEmailButton"):
            return "device"
    stage = driver.execute_script("""
        const visible = id => { const el = document.getElementById(id); return el && el.getClientRects().length; };
        if (visible('otp') && visible('submit-otp')) return 'otp';
        if (visible('sendEmailButton')) return 'device';
        return null;
    """)
    return stage or ("logged_in" if is_logged_in(driver) else False)


def login(driver: webdriver.Chrome, id: str, password: str) -> bool:
    try:
        if phone_authentication_pending(driver):
            raise PhoneAuthenticationRequired("Phone authentication is pending; credentials were not re-entered")
        if is_logged_in(driver):
            return True
        id_ele = driver.find_element(By.NAME, "username")
        id_ele.clear()
        id_ele.send_keys(id)
        pa_ele = driver.find_element(By.NAME, "password")
        pa_ele.clear()
        pa_ele.send_keys(password)
        log_ele = driver.find_element(By.ID, "pw-btn")
        requested_at = datetime.now(timezone.utc)
        log_ele.click()
        stage = WebDriverWait(driver, 15).until(_login_stage)
        if stage == "otp":
            code = gmail.retrieve_sbi_device_code(not_before=requested_at)
            if code is None:
                return False
            code_input = driver.find_element(By.ID, "otp")
            code_input.clear()
            code_input.send_keys(code)
            if code_input.get_attribute("value") != code:
                logger.warning("Authentication code input did not match the extracted code")
                return False
            _close_known_popup(driver)
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "submit-otp"))).click()
            return bool(WebDriverWait(driver, 20).until(is_logged_in))
        if stage == "device":
            device_element = driver.find_element(By.ID, "sendEmailButton")
            requested_at = datetime.now(timezone.utc)
            device_element.click()
            if handle_otp(driver=driver, not_before=requested_at):
                return bool(WebDriverWait(driver, 20).until(is_logged_in))
            return False
        return stage == "logged_in"
    except gmail.GmailConfigurationError:
        raise
    except ActionOutcomeUnknown:
        raise
    except Exception as e:
        logger.warning("Login failed: %s", type(e).__name__)
        _record_failure(driver, e)
        return False
