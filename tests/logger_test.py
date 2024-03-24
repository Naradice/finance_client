import os
import sys
import unittest

import dotenv

try:
    dotenv.load_dotenv("tests/.env")
except Exception:
    pass

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
print(module_path)
sys.path.append(module_path)

from finance_client import logger


class TestLoggerWrapper(unittest.TestCase):
    def test_log_functions(self):
        logger.debug("test debug message")
        logger.info("test info message")
        logger.warning("test warning message")
        logger.error("test error message")


if __name__ == "__main__":
    unittest.main()
