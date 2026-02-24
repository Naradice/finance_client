"""
Responsibility: RiskManager
- Load config
- Refer account information
- Refer position information
- Generate risk option
- Calculate risk for current positions
- Decide parameters for trading
"""

from pyparsing import ABC, abstractmethod
from websockets import Side

from finance_client import POSITION_SIDE, ClientBase
from finance_client.account import Manager as AccountManager
from finance_client.config.model import RiskConfig
from finance_client.risk_manager.model import RiskResult
from finance_client.risk_manager.risk_options.atr import ATRRisk
from finance_client.risk_manager.risk_options.fixed_loss import FixedAmountRisk
from finance_client.risk_manager.risk_options.percent_equity import \
    PercentEquityRisk
from finance_client.risk_manager.risk_options.risk_option import RiskOption


class RiskManager:

    def __init__(
        self,
        config: RiskConfig,
        account_manager: AccountManager,
        market_client: ClientBase,
    ):
        self.config = config
        self.account_manager = account_manager
        self.market_client = market_client


    def calculate_total_risk(self) -> float:
        pass

    def create_percent_equity_option(self, percent: float) -> RiskOption:
        return PercentEquityRisk(percent)
    
    def create_fixed_loss_option(self, loss: float) -> RiskOption:
        return FixedAmountRisk(loss)
    
    def create_atr_option(self, coefficient: float, timeframe: str) -> RiskOption:
        return ATRRisk(coefficient, timeframe)