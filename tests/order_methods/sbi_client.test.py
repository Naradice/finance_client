import datetime
import json
import os
import sys
import unittest

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
print(module_path)
sys.path.append(module_path)

from logging import config, getLogger

import dotenv

import finance_client.frames as Frame
from finance_client.sbi.client import SBIClient

dotenv.load_dotenv("../.env")

try:
    with open(os.path.join(module_path, "finance_client/settings.json"), "r") as f:
        settings = json.load(f)
except Exception as e:
    print(f"fail to load settings file: {e}")
    raise e
logger_config = settings["log"]
log_file_base_name = logger_config["handlers"]["fileHandler"]["filename"]
log_path = f'./{log_file_base_name}_sbiclienttest_{datetime.datetime.utcnow().strftime("%Y%m%d%H")}.logs'
logger_config["handlers"]["fileHandler"]["filename"] = log_path
config.dictConfig(logger_config)
logger = getLogger("finance_client.test")

id = os.environ[SBIClient.ID_KEY]
pswd = os.environ[SBIClient.PASS_KEY]
trade_pswd = os.environ[SBIClient.TRADE_PASS_KEY]


class TestSBIClient(unittest.TestCase):
    client = SBIClient(
        symbol="4042",
        id=id,
        password=pswd,
        trade_password=trade_pswd,
        frame=Frame.D1,
    )

    """
    def test_get_rates(self):
        length = 10
        rates = self.client.get_rates(length)
        self.assertEqual(len(rates.Close), length)
    
    # need to read and store positions
    def test_close_buy_position(self):
        self.client.close_long_positions()
    """

    def test_buy(self):
        self.client.market_buy("4042", None, amount=1, tp=None, sl=None, option_info=None)


if __name__ == "__main__":
    unittest.main()
