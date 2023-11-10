import os
import sys
import unittest

import dotenv

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
print(module_path)
sys.path.append(module_path)
from finance_client import MT5Client

dotenv.load_dotenv("../.env")


class TestMT5ClientDL(unittest.TestCase):
    def test_download(self):
        SYMBOLS = ["AUDJPY", "AUDUSD", "EURJPY", "EURUSD", "GBPJPY", "GBPUSD", "USDCHF"]
        client = MT5Client(
            id=int(os.environ["mt5_id"]),
            password=os.environ["mt5_password"],
            server=os.environ["mt5_server"],
            symbols=SYMBOLS,
            auto_index=False,
            simulation=True,
            frame=30,
        )
        client.download(symbols=SYMBOLS)


if __name__ == "__main__":
    unittest.main()
