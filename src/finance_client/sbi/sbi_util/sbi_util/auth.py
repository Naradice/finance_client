import logging
import time

from selenium import webdriver
from selenium.common import exceptions
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .mail import gmail

logger = logging.getLogger(__name__)

def _get_device_code_element(driver: webdriver.Chrome):
    try:
        logger.debug("getting device element")
        element = driver.find_element(By.NAME, "ACT_deviceotpcall")
        return element
    except Exception:
        return None
    
def input_device_code(driver: webdriver.Chrome, code: str):
    # If anyone want to use other mail provider, need to overwrite the method
    url = gmail.retrieve_code_input_url()
    if url:
        # open a new tab
        driver.execute_script(f"window.open('{url}');")
        handles = driver.window_handles
        driver.switch_to.window(handles[-1])
        # close pop up
        try:
            close_btn = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a.karte-close"))
            )
            close_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "a.karte-close"))
            )
            close_btn.click()
        except exceptions.NoSuchElementException:
            logger.debug("no popup to close")
            pass
        code_input = driver.find_element(By.ID, "verifyCode")
        code_input.send_keys(code)
        verify_btn = driver.find_element(By.ID, "verification")
        verify_btn.click()
        # close the tab
        driver.close()
        # switch to the original tab
        driver.switch_to.window(handles[0])
        return True
    return False

def handle_otp(driver: webdriver.Chrome) -> bool:
    try:
        logger.debug("start input device code with gmail")
        device_element = driver.find_element(By.ID, "code-display")
        device_code = device_element.text
        checkbox = driver.find_element(By.ID, "device-checkbox")
        if checkbox and checkbox.is_selected() is False:
            checkbox.click()
        if input_device_code(driver, device_code):
            register_button = driver.find_element(By.ID, "device-auth-otp")
            register_button.click()
            return True
        logger.error("failed to get device code")
        return False
    except exceptions.NoSuchElementException as e:
        logger.error(e)
        return is_logged_in(driver)
    except Exception as e:
        logger.error(e)
        return False

def is_logged_in(driver: webdriver.Chrome) -> bool:
    try:
        driver.find_element(By.NAME, "user_password")
        return False
    except exceptions.NoSuchElementException:
        # If user_password is not found, it means logged in
        return True
    except Exception as e:
        print(f"error happened on is_logged_in: {e}")
        return False


def login(driver: webdriver.Chrome, id: str, password: str) -> bool:
    try:
        id_ele = driver.find_element(By.NAME, "user_id")
        id_ele.send_keys(id)
        pa_ele = driver.find_element(By.NAME, "user_password")
        pa_ele.send_keys(password)
        log_ele = driver.find_element(By.NAME, "ACT_login")
        log_ele.click()
        device_element = _get_device_code_element(driver)
        if device_element is None:
            logger.debug(
                "no device element. check logged in or encountered an error."
            )
            if is_logged_in(driver):
                # store creds to utilize them when login life time end
                logger.info("login is completed.")
                return True
            else:
                logger.warning(
                    "Failed to login. Please check your id and password."
                )
                return False
        else:
            # wait to receive device code
            device_element.click()
            time.sleep(3)
            if handle_otp(driver=driver):
                logger.debug("device code was available. check login state.")
                if is_logged_in():
                    return True
                else:
                    logger.warning("Failed to login. Please try again.")
                    return False
            else:
                logger.warning("Failed to login. Please try again.")
    except Exception as e:
        logger.error(e)
        return False