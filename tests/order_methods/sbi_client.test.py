import os
import sys
import unittest

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
print(module_path)
sys.path.append(module_path)

import dotenv

import finance_client.frames as Frame
from finance_client.sbi.client import SBIClient

try:
    dotenv.load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"))
except Exception as e:
    raise e

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
        self.client.open_trade(True, symbol="4042", amount=1, tp=None, sl=None, option_info=None)


if __name__ == "__main__":
    unittest.main()
