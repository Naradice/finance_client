"""
Equity × %

責務：
- equity × percent
- SL幅逆算
- Volume丸め
"""

from finance_client.risk_manager.model import RiskContext
from finance_client.risk_manager.risk_options.risk_option import RiskOption


class PercentEquityRisk(RiskOption):

    def __init__(self, percent: float):
        self.percent = percent
        super().__init__()


    def calculate_volume(self, context: RiskContext, entry_price: float, stop_loss: float) -> float:
        
        # ① 許容損失額
        allowed_loss = (
            context.account_equity *
            (self.percent / 100.0)
        )

        # ② SL距離
        sl_pips = abs(
            entry_price - stop_loss
        ) / context.pip_size

        # ③ 1通貨あたりの損失
        loss_per_unit = sl_pips * context.pip_value_per_lot
        raw_volume = allowed_loss / loss_per_unit

        # ④ volume丸め
        volume = self._round_volume(raw_volume, context)

        # ⑤ 口座制限適用
        volume = self._apply_account_caps(
            volume,
            context,
            self.config
        )

        return volume
