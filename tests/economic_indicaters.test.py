import unittest, os, json, sys, datetime
from logging import getLogger, config
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(module_path)

print(module_path)

from finance_client.utils import addprocess

try:
    with open(os.path.join(module_path, 'finance_client/settings.json'), 'r') as f:
        settings = json.load(f)
except Exception as e:
    print(f"fail to load settings file: {e}")
    raise e
    
logger_config = settings["log"]
log_file_base_name = logger_config["handlers"]["fileHandler"]["filename"]
log_path = f'./{log_file_base_name}_economic_{datetime.datetime.utcnow().strftime("%Y%m%d%H")}.log'
logger_config["handlers"]["fileHandler"]["filename"] = log_path
config.dictConfig(logger_config)
logger = getLogger("finance_client.test")


class TestIndicaters(unittest.TestCase):
    
    def test_get_SP500(self):
        sp500 = addprocess.get_indicater("SP500", datetime.datetime(2020, 1, 1), datetime.datetime.now())
        print(sp500)
    
if __name__ == '__main__':
    unittest.main()