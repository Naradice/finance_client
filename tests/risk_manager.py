import unittest

from finance_client.risk_manager import RiskManager


class TestRiskManager(unittest.TestCase):
    def test_risk_manager_initialization(self):
        risk_manager = RiskManager()
        self.assertIsInstance(risk_manager, RiskManager)


if __name__ == "__main__":
    unittest.main()
