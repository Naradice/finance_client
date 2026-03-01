"""
固定金額

例：15000円

SL幅 = 固定損失 ÷ 通貨数量
"""

from finance_client.risk_manager.model import RiskContext, RiskResult
from finance_client.risk_manager.risk_options.risk_option import RiskOption


class FixedAmountRisk(RiskOption):

    def __init__(self, allowed_loss_volume: float):
        self.allowed_loss_volume = allowed_loss_volume

    def calculate(self, context: RiskContext) -> RiskResult:

        sl_diff = abs(context.entry_price - context.stop_loss)
        # TODO: currency exchange if needed
        raw_volume = self.allowed_loss_volume / sl_diff

        volume = self._round_volume(raw_volume, context)

        return volume
