import datetime
import os
import sys
import unittest

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(module_path)

from finance_client.fprocess.fprocess import addprocess


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
