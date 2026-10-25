"""Start a persistent SBI Chrome, or attach to a Chrome the user has allowed."""

import os
from pathlib import Path
import re
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, build_opener

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from .env import load_sbi_env
from . import utils


class ChromeConfigurationError(ValueError):
    pass


def _debugger_address(value):
    if value == "auto":
        directory = os.getenv("SBI_CHROME_ATTACH_USER_DATA_DIR")
        if not directory:
            local = os.getenv("LOCALAPPDATA")
            if not local:
                raise ChromeConfigurationError("Set SBI_CHROME_ATTACH_USER_DATA_DIR for this system")
            directory = str(Path(local) / "Google/Chrome/User Data")
        try:
            with (Path(directory).expanduser() / "DevToolsActivePort").open(encoding="ascii") as stream:
                port = stream.readline().strip()
        except (OSError, UnicodeError):
            raise ChromeConfigurationError(
                "Open chrome://inspect/#remote-debugging in your usual Chrome and enable remote debugging. "
                "Allow the connection when Chrome asks. No new browser or order was started.") from None
        value = "127.0.0.1:" + port
    match = re.fullmatch(r"(127\.0\.0\.1|localhost|\[::1\]):([0-9]{1,5})", value)
    if not match or not 1 <= int(match[2]) <= 65535:
        raise ChromeConfigurationError("SBI_CHROME_DEBUGGER_ADDRESS must be auto or a loopback host:port")
    return value


def _permission_websocket(address):
    directory = os.getenv("SBI_CHROME_ATTACH_USER_DATA_DIR")
    if not directory:
        directory = str(Path(os.environ["LOCALAPPDATA"]) / "Google/Chrome/User Data")
    try:
        lines = (Path(directory).expanduser() / "DevToolsActivePort").read_text(encoding="ascii").splitlines()
        path = lines[1].strip()
        if address.rsplit(":", 1)[1] != lines[0].strip() or not re.fullmatch(r"/devtools/browser(?:/[A-Za-z0-9_-]+)?", path):
            raise ValueError()
        return "ws://" + address + path
    except (OSError, UnicodeError, IndexError, ValueError):
        raise ChromeConfigurationError("The permission-based Chrome connection file is missing or invalid") from None


def chrome_options():
    load_sbi_env()
    options = Options()
    address = os.getenv("SBI_CHROME_DEBUGGER_ADDRESS", "").strip()
    if address:
        options.debugger_address = _debugger_address(address)
    else:
        directory = os.getenv("SBI_CHROME_USER_DATA_DIR", str(Path(utils.BASE_PATH) / "profile"))
        if not directory.strip():
            raise ChromeConfigurationError("SBI_CHROME_USER_DATA_DIR cannot be empty")
        options.add_argument("--user-data-dir=" + str(Path(directory).expanduser().resolve()))
        profile = os.getenv("SBI_CHROME_PROFILE_DIRECTORY", "").strip()
        if profile:
            if profile in (".", "..") or any(c in profile for c in "/\\\x00"):
                raise ChromeConfigurationError("SBI_CHROME_PROFILE_DIRECTORY must be a profile folder name")
            options.add_argument("--profile-directory=" + profile)
    binary = os.getenv("SBI_CHROME_BINARY", "").strip()
    if binary:
        options.binary_location = str(Path(binary).expanduser().resolve())
    # Use Chrome's real user agent and normal rendering settings.
    return options


def _disconnect(driver):
    try:
        driver.command_executor.close()
    finally:
        driver.service.stop()


class AttachedChrome:
    """Own the automation tab and driver connection, preserving the user's browser."""

    def __init__(self, driver, owned_handle, previous_handle, bridge=None):
        self._driver = driver
        self._owned_handle = owned_handle
        self._previous_handle = previous_handle
        self._closed = False
        self._bridge = bridge

    def __getattr__(self, name):
        return getattr(self._driver, name)

    def quit(self):
        if self._closed:
            return
        self._closed = True
        try:
            if self._owned_handle in self._driver.window_handles:
                self._driver.switch_to.window(self._owned_handle)
                self._driver.close()
            if self._previous_handle in self._driver.window_handles:
                self._driver.switch_to.window(self._previous_handle)
        finally:
            # WebDriver.quit would close borrowed browser windows too.
            try:
                _disconnect(self._driver)
            finally:
                if self._bridge is not None:
                    self._bridge.close()


def open_chrome():
    options = chrome_options()
    bridge = None
    if options.debugger_address:
        try:
            with build_opener(ProxyHandler({})).open("http://" + options.debugger_address + "/json/version", timeout=3):
                pass
        except HTTPError as error:
            if error.code != 404:
                raise ChromeConfigurationError("Chrome debugging discovery failed") from None
            from .chrome_bridge import ChromePermissionBridge, ChromeBridgeError
            try:
                bridge = ChromePermissionBridge(_permission_websocket(options.debugger_address)).start()
            except ChromeBridgeError:
                raise ChromeConfigurationError("Chromeへの接続を許可してください。ブラウザーの接続許可が完了していません。") from None
            options.debugger_address = bridge.address
        except (URLError, TimeoutError):
            raise ChromeConfigurationError("Chrome debugging is not reachable; enable chrome://inspect/#remote-debugging") from None
    driver = None
    try:
        driver = webdriver.Chrome(options=options)
        if not options.debugger_address:
            return driver
        previous = driver.current_window_handle
        driver.switch_to.new_window("tab")
        return AttachedChrome(driver, driver.current_window_handle, previous, bridge)
    except Exception:
        try:
            if driver is not None:
                _disconnect(driver)
        finally:
            if bridge is not None:
                bridge.close()
        raise
