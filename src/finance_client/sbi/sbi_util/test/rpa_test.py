import os
import sys
import unittest
from functools import wraps

import dotenv

import setup

setup.set_debug_console_logging("sbirpa")
base_dir = os.path.dirname(__file__)
env_file = os.path.join(base_dir, ".env")
dotenv.load_dotenv(env_file)


if "sbi_id" not in os.environ:
    raise Exception("env file is required")

try:
    import sbi_util
except ImportError:
    try:
        module_path = os.path.abspath(os.path.join(base_dir, ".."))
        sys.path.append(module_path)
    except Exception as e:
        raise e

import sbi_util.rpa as rpa


def with_asserts(func):
    @wraps(func)
    def wrapper():
        tc = unittest.TestCase()
        func(tc)

    return wrapper


client = rpa.STOCK(
    os.environ["sbi_id"],
    os.environ["sbi_password"],
    os.environ["sbi_trade_password"],
)


@with_asserts
def test_get_buget(tc: unittest.TestCase):
    buget = client.get_available_budget()
    tc.assertGreaterEqual(buget, 0)


@with_asserts
def test_get_a_rating(tc: unittest.TestCase):
    rating = client.get_rating("8031")
    tc.assertIsInstance(rating, dict)
    print(rating)


@with_asserts
def test_get_ratings(tc: unittest.TestCase):
    rating = client.get_ratings(["1928", "8031", "5108"])
    tc.assertIsInstance(rating, dict)
    print(rating)


@with_asserts
def test_close(tc: unittest.TestCase):
    suc, result = client.sell_to_close_buy_order("3103", 1)


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(unittest.FunctionTestCase(test_get_buget))
    suite.addTest(unittest.FunctionTestCase(test_get_a_rating))
    suite.addTest(unittest.FunctionTestCase(test_get_ratings))
    suite.addTest(unittest.FunctionTestCase(test_close))
    unittest.TextTestRunner().run(suite)
