import datetime
import json
import os
import sys
import unittest

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
print(module_path)
sys.path.append(module_path)

from logging import config, getLogger

import finance_client.frames as Frame

try:
    with open(os.path.join(module_path, "finance_client/settings.json"), "r") as f:
        settings = json.load(f)
except Exception as e:
    print(f"fail to load settings file: {e}")
    raise e
logger_config = settings["log"]
log_file_base_name = logger_config["handlers"]["fileHandler"]["filename"]
log_path = f'./{log_file_base_name}_unittest_{datetime.datetime.utcnow().strftime("%Y%m%d%H")}.log'
logger_config["handlers"]["fileHandler"]["filename"] = log_path
config.dictConfig(logger_config)
logger = getLogger("finance_client.test")


class TestVantageClient(unittest.TestCase):
    def test_get_varname(self):
        name = Frame.to_str(Frame.MIN1)
        self.assertEqual("MIN1", name)
        name = Frame.to_str(Frame.MIN5)
        self.assertEqual("MIN5", name)


if __name__ == "__main__":
    unittest.main()
