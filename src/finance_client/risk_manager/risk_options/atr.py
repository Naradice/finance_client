"""
ATR × 係数

指定時間足

定期再計算可能
"""

from finance_client.risk_manager.model import RiskContext, RiskResult
from finance_client.risk_manager.risk_options.risk_option import RiskOption


class ATRRisk(RiskOption):

    def __init__(self, percent: float, atr_value: float, atr_multiplier: float, rr_ratio: float):
        self.percent = percent
        self.atr_value = atr_value
        self.atr_multiplier = atr_multiplier
        self.rr_ratio = rr_ratio

    def calculate(self, context: RiskContext) -> RiskResult:

        # ① SL距離決定
        sl_distance = self.atr_value * self.atr_multiplier

        if context.entry_price > context.current_price:
            stop_loss = context.entry_price + sl_distance
        else:
            stop_loss = context.entry_price - sl_distance

        # ② 許容損失
        allowed_loss = (
            context.account_equity *
            (self.percent / 100)
        )

        loss_per_unit = (
            sl_distance *
            context.pip_value_per_lot
        )

        raw_volume = allowed_loss / loss_per_unit

        volume = self._round_volume(raw_volume, context)

        # ③ TP決定
        tp_distance = sl_distance * self.rr_ratio

        if context.entry_price > stop_loss:
            take_profit = context.entry_price + tp_distance
        else:
            take_profit = context.entry_price - tp_distance

        risk_volume = self._calc_loss(volume, context, stop_loss)
        reward_volume = risk_volume * self.rr_ratio

        return RiskResult(
            volume=volume,
            stop_loss_price=stop_loss,
            take_profit_price=take_profit,
            risk_volume=risk_volume,
            reward_volume=reward_volume,
            risk_reward_ratio=self.rr_ratio
        )
