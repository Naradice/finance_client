"""
Equity × %

責務：
- equity × percent
- SL幅逆算
- Volume丸め
"""

from finance_client.risk_manager.model import RiskContext, RiskResult
from finance_client.risk_manager.risk_options.risk_option import RiskOption


class PercentEquityRisk(RiskOption):
    """Risk sizing that risks a fixed percentage of account equity per trade.

    Volume is back-calculated from the allowed monetary loss (equity × percent)
    divided by the monetary loss per lot at the given stop loss distance.
    The stop loss price must be supplied in RiskContext; this strategy does not
    compute its own SL.
    """

    def __init__(self, percent: float):
        """
        Args:
            percent (float): Percentage of account equity to risk per trade (e.g. 1.0 = 1%).
        """
        self.percent = percent

    def calculate(self, context: RiskContext) -> RiskResult:
        """Calculate position size based on a percentage of account equity.

        Args:
            context (RiskContext): Current account state and trade parameters.
                context.stop_loss must be set; context.take_profit is optional.

        Returns:
            RiskResult: Volume sized to risk percent% of equity, with stop_loss_price
                and take_profit_price taken directly from context.

        Raises:
            ValueError: If context.stop_loss is None.
        """
        if context.stop_loss is None:
            raise ValueError("stop_loss must be provided in RiskContext for PercentEquityRisk.")

        # ① 許容損失額
        allowed_loss = context.account_equity * (self.percent / 100.0)

        # ② SL距離
        sl_distance = abs(context.entry_price - context.stop_loss)

        # ③ 1通貨あたりの損失
        loss_per_unit = sl_distance * context.symbol_risk_config.pip_value_per_lot
        raw_volume = allowed_loss / loss_per_unit

        # ④ volume丸め
        volume = self._round_volume(raw_volume, context)

        risk_volume = self._calc_loss(volume, context, context.stop_loss)
        reward_volume = self._calc_loss(volume, context, context.take_profit) if context.take_profit is not None else None
        rr_ratio = (reward_volume / risk_volume) if reward_volume is not None else None

        return RiskResult(
            volume=volume,
            stop_loss_price=context.stop_loss,
            take_profit_price=context.take_profit,
            risk_volume=risk_volume,
            reward_volume=reward_volume,
            risk_reward_ratio=rr_ratio,
        )
