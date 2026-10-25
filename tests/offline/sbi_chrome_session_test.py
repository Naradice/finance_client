"""Chrome configuration and borrowed-browser ownership; no SBI or account access."""

import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from selenium import webdriver
from selenium.common.exceptions import WebDriverException

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/finance_client/sbi/sbi_util"))
from sbi_util.chrome_session import AttachedChrome, ChromeConfigurationError, chrome_options, open_chrome


class ChromeOptionsTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        self.loader = patch("sbi_util.chrome_session.load_sbi_env")
        self.loader.start()
        self.addCleanup(self.loader.stop)
        self.discovery = patch("sbi_util.chrome_session.build_opener")
        self.discovery.start()
        self.addCleanup(self.discovery.stop)

    def test_managed_profile_is_persistent_without_user_agent_or_image_override(self):
        options = chrome_options()
        self.assertTrue(any(arg.startswith("--user-data-dir=") for arg in options.arguments))
        self.assertFalse(any("user-agent" in arg for arg in options.arguments))
        self.assertNotIn("prefs", options.experimental_options)

    def test_custom_profile_and_binary_are_configurable(self):
        with tempfile.TemporaryDirectory() as directory:
            os.environ.update(SBI_CHROME_USER_DATA_DIR=directory, SBI_CHROME_PROFILE_DIRECTORY="Profile 1",
                              SBI_CHROME_BINARY=str(Path(directory) / "chrome.exe"))
            options = chrome_options()
            self.assertIn("--user-data-dir=" + str(Path(directory).resolve()), options.arguments)
            self.assertIn("--profile-directory=Profile 1", options.arguments)
            self.assertEqual(options.binary_location, str(Path(directory).resolve() / "chrome.exe"))

    def test_auto_attach_reads_only_local_chrome_port(self):
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "DevToolsActivePort").write_text("12345\n/devtools/browser/fixture\n", encoding="ascii")
            os.environ.update(SBI_CHROME_DEBUGGER_ADDRESS="auto", SBI_CHROME_ATTACH_USER_DATA_DIR=directory)
            options = chrome_options()
            self.assertEqual(options.debugger_address, "127.0.0.1:12345")
            self.assertEqual(options.arguments, [])

    def test_missing_auto_port_stops_before_starting_chrome(self):
        with tempfile.TemporaryDirectory() as directory, patch("sbi_util.chrome_session.webdriver.Chrome") as launch:
            os.environ.update(SBI_CHROME_DEBUGGER_ADDRESS="auto", SBI_CHROME_ATTACH_USER_DATA_DIR=directory)
            with self.assertRaisesRegex(ChromeConfigurationError, "chrome://inspect"):
                open_chrome()
            launch.assert_not_called()

    def test_external_or_invalid_debugger_addresses_are_rejected(self):
        for address in ("evil.example:9222", "0.0.0.0:9222", "127.0.0.1:0", "127.0.0.1:65536", "http://127.0.0.1:9222"):
            with self.subTest(address=address):
                os.environ["SBI_CHROME_DEBUGGER_ADDRESS"] = address
                with self.assertRaises(ChromeConfigurationError):
                    chrome_options()

    def test_attach_closes_only_owned_tab_and_disconnects_without_browser_quit(self):
        raw = Mock()
        raw.current_window_handle = "user-tab"
        raw.window_handles = ["user-tab", "our-tab", "another-user-tab"]
        def new_window(_):
            raw.current_window_handle = "our-tab"
        raw.switch_to.new_window.side_effect = new_window
        with patch("sbi_util.chrome_session.webdriver.Chrome", return_value=raw):
            os.environ["SBI_CHROME_DEBUGGER_ADDRESS"] = "127.0.0.1:9222"
            attached = open_chrome()
        self.assertIsInstance(attached, AttachedChrome)
        attached.quit()
        attached.quit()
        raw.close.assert_called_once()
        raw.switch_to.window.assert_any_call("our-tab")
        raw.switch_to.window.assert_any_call("user-tab")
        raw.quit.assert_not_called()
        raw.service.stop.assert_called_once()

    def test_failed_new_tab_disconnects_without_closing_existing_browser(self):
        raw = Mock()
        raw.switch_to.new_window.side_effect = RuntimeError("fixture failure")
        with patch("sbi_util.chrome_session.webdriver.Chrome", return_value=raw):
            os.environ["SBI_CHROME_DEBUGGER_ADDRESS"] = "127.0.0.1:9222"
            with self.assertRaises(RuntimeError):
                open_chrome()
        raw.close.assert_not_called()
        raw.quit.assert_not_called()
        raw.service.stop.assert_called_once()


class SharedBrowserTests(unittest.TestCase):
    def test_second_driver_reuses_profile_and_preserves_existing_tab(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        directory = temporary.name
        with patch("sbi_util.chrome_session.load_sbi_env"):
            options = webdriver.ChromeOptions()
            options.add_argument("--headless=new")
            options.add_argument("--user-data-dir=" + directory)
            owner = webdriver.Chrome(options=options)
            self.addCleanup(owner.quit)
            owner.execute_cdp_cmd("Network.enable", {})
            owner.execute_cdp_cmd("Network.setBlockedURLs", {"urls": ["http://*", "https://*"]})
            owner.get("data:text/html,<title>original-user-tab</title>")
            original = owner.current_window_handle
            native_agent = owner.execute_script("return navigator.userAgent")
            with patch.dict(os.environ, {"SBI_CHROME_DEBUGGER_ADDRESS": "auto",
                                         "SBI_CHROME_ATTACH_USER_DATA_DIR": directory}), \
                 patch("sbi_util.chrome_session.load_sbi_env"):
                attached = open_chrome()
            try:
                self.assertNotEqual(attached.current_window_handle, original)
                attached.get("data:text/html,<title>owned-sbi-fixture-tab</title>")
                self.assertEqual(attached.execute_script("return navigator.userAgent"), native_agent)
                self.assertEqual(len(owner.window_handles), 2)
            finally:
                attached.quit()
            self.assertEqual(owner.window_handles, [original])
            self.assertEqual(owner.title, "original-user-tab")

    def test_permission_websocket_bridge_preserves_browser_and_blocks_browser_close(self):
        from sbi_util.chrome_bridge import ChromePermissionBridge
        from websockets.asyncio.server import serve
        def fast_defaults(*args, **kwargs):
            # Accelerate the library defaults; explicit disabled keepalive must
            # preserve the driver across an idle interval without CDP traffic.
            kwargs.setdefault("ping_interval", 1)
            kwargs.setdefault("ping_timeout", 1)
            return serve(*args, **kwargs)
        trace = []
        def bridge_factory(url):
            bridge = ChromePermissionBridge(url)
            bridge.trace = trace.append
            return bridge
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--user-data-dir=" + temporary.name)
        owner = webdriver.Chrome(options=options)
        self.addCleanup(owner.quit)
        owner.execute_cdp_cmd("Network.enable", {})
        owner.execute_cdp_cmd("Network.setBlockedURLs", {"urls": ["http://*", "https://*"]})
        owner.get("data:text/html,<title>preserved-user-tab</title>")
        original = owner.current_window_handle
        with patch.dict(os.environ, {"SBI_CHROME_DEBUGGER_ADDRESS": "auto", "SBI_CHROME_ATTACH_USER_DATA_DIR": temporary.name}), \
             patch("sbi_util.chrome_session.load_sbi_env"), patch("sbi_util.chrome_session.build_opener") as opener, \
             patch("sbi_util.chrome_bridge.ChromePermissionBridge", side_effect=bridge_factory), \
             patch("websockets.asyncio.server.serve", side_effect=fast_defaults):
            opener.return_value.open.side_effect = HTTPError("fixture", 404, "Not Found", {}, None)
            try:
                attached = open_chrome()
            except Exception:
                print(trace)
                raise
        try:
            attached.get("data:text/html,<label>quantity<input id=q></label>")
            attached.find_element("id", "q").send_keys("123")
            time.sleep(4)
            self.assertEqual(attached.find_element("id", "q").get_attribute("value"), "123")
            with self.assertRaises(WebDriverException):
                attached.execute_cdp_cmd("Browser.close", {})
            self.assertEqual(len(owner.window_handles), 2)
        finally:
            attached.quit()
        self.assertEqual(owner.window_handles, [original])
        self.assertEqual(owner.title, "preserved-user-tab")


if __name__ == "__main__":
    unittest.main()
