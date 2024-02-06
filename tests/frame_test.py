import os
import sys
import unittest

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
print(module_path)
sys.path.append(module_path)

import finance_client.frames as Frame


class TestVantageClient(unittest.TestCase):
    def test_get_varname(self):
        name = Frame.to_str(Frame.MIN1)
        self.assertEqual("MIN1", name)
        name = Frame.to_str(Frame.MIN5)
        self.assertEqual("MIN5", name)


if __name__ == "__main__":
    unittest.main()
