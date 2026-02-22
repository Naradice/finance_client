"""
固定金額

例：15000円

SL幅 = 固定損失 ÷ 通貨数量
"""

from finance_client.risk_manager.model import RiskContext
from finance_client.risk_manager.risk_options.risk_option import RiskOption


class FixedAmountRisk(RiskOption):

    def __init__(self, allowed_loss_amount: float):
        self.allowed_loss_amount = allowed_loss_amount

    def calculate_volume(self, context: RiskContext, entry_price: float, stop_loss: float, take_profit: float) -> float:

        sl_diff = abs(
            entry_price - stop_loss
        )
        # TODO: currency exchange if needed
        raw_volume = self.allowed_loss_amount / sl_diff

        volume = self._round_volume(raw_volume, context)

        volume = self._apply_account_caps(
            volume,
            context,
        )

        return volume
