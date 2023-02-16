import datetime
import json
import os
import sys
import unittest
from logging import config, getLogger

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(module_path)

import dotenv

from finance_client.utils import addprocess

try:
    with open(os.path.join(module_path, "finance_client/settings.json"), "r") as f:
        settings = json.load(f)
except Exception as e:
    print(f"fail to load settings file: {e}")
    raise e

dotenv.load_dotenv("../.env")

logger_config = settings["log"]
log_file_base_name = logger_config["handlers"]["fileHandler"]["filename"]
log_path = f'./{log_file_base_name}_economic_{datetime.datetime.utcnow().strftime("%Y%m%d%H")}.log'
logger_config["handlers"]["fileHandler"]["filename"] = log_path
config.dictConfig(logger_config)
logger = getLogger("finance_client.test")


class TestIndicaters(unittest.TestCase):
    def test_get_SP500(self):
        start_date = datetime.datetime(2020, 1, 1)
        sp500 = addprocess.get_indicater("SP500", start_date, datetime.datetime.now(), frame=5)
        print(sp500)
        self.assertGreaterEqual(sp500.index[0], start_date)

    def test_get_PMI(self):
        start_date = datetime.datetime(2020, 1, 1)
        PMI = addprocess.get_indicater("PMI", start_date, datetime.datetime.now(), frame=5)
        print(PMI)
        self.assertGreaterEqual(PMI.index[0], start_date)


if __name__ == "__main__":
    unittest.main()
