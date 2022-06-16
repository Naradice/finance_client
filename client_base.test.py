import unittest
from finance_client.client_base import Client
from finance_client.frames import Frame
from finance_client import utils

class TestCSVClient(unittest.TestCase):
    
    def test_process(self):
        client = Client()
        process = utils.ProcessBase("base")
        invalid = client.have_process(process)
        self.assertEqual(invalid, False)
        client.add_indicater(process)
        process2 = utils.ProcessBase("base")
        valid = client.have_process(process2)
        self.assertEqual(valid, True)
        client.add_indicaters([process, process2])

    
if __name__ == '__main__':
    unittest.main()