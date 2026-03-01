"""
Responsibility: RiskManager
- Load config
- Refer account information
- Refer position information
- Generate risk option
- Calculate risk for current positions
- Decide parameters for trading
"""

from finance_client.account import Manager as AccountManager
from finance_client.risk_manager.model import RiskContext, RiskResult
from finance_client.risk_manager.risk_options.atr import ATRRisk
from finance_client.risk_manager.risk_options.fixed_loss import FixedAmountRisk
from finance_client.risk_manager.risk_options.percent_equity import PercentEquityRisk
from finance_client.risk_manager.risk_options.risk_option import RiskOption


class RiskManager:

    def __init__(self, account_manager: AccountManager):
        self.account_manager = account_manager

    def calculate_total_risk(self) -> float:
        pass

    def create_percent_equity_option(self, percent: float) -> RiskOption:
        return PercentEquityRisk(percent)

    def create_fixed_loss_option(self, loss: float) -> RiskOption:
        return FixedAmountRisk(loss)

    def create_atr_option(self, percent: float, atr_value: float, atr_multiplier: float, rr_ratio: float) -> RiskOption:
        return ATRRisk(percent=percent, atr_value=atr_value, atr_multiplier=atr_multiplier, rr_ratio=rr_ratio)

    def _apply_account_caps(
        self,
        volume: float,
        context: RiskContext,
    ) -> float:
        if context.symbol_risk_config.max_volume is not None:
            volume = min(volume, context.symbol_risk_config.max_volume)

        if context.account_risk_config.max_total_volume is not None:
            remaining_volume = max(0.0, context.account_risk_config.max_total_volume - context.open_positions_risk_volume)
            volume = min(volume, remaining_volume)

        if context.account_risk_config.max_daily_loss is not None:
            remaining_loss = max(0.0, context.account_risk_config.max_daily_loss - context.daily_realized_pnl)
            max_volume_by_loss = remaining_loss / context.pip_value_per_lot
            volume = min(volume, max_volume_by_loss)

        return volume

    def evaluate_risk(self, risk_option: RiskOption) -> RiskResult:
        context = self._build_risk_context()
        risk_result = risk_option.calculate(context)
        self._apply_account_caps(
            risk_result,
            context,
        )
        return risk_result
