"""
固定金額

例：15000円

SL幅 = 固定損失 ÷ 通貨数量
"""

from finance_client.risk_manager.model import RiskContext, RiskResult
from finance_client.risk_manager.risk_options.risk_option import RiskOption


class FixedAmountRisk(RiskOption):
    """Risk sizing that risks a fixed monetary amount per trade.

    Volume is back-calculated so that hitting the stop loss costs exactly
    allowed_loss_volume in account currency.
    The stop loss price must be supplied in RiskContext; this strategy does not
    compute its own SL.
    """

    def __init__(self, allowed_loss_volume: float):
        """
        Args:
            allowed_loss_volume (float): Fixed monetary amount to risk per trade
                (e.g. 15000 for ¥15,000).
        """
        self.allowed_loss_volume = allowed_loss_volume

    def calculate(self, context: RiskContext, ohlc_df=None) -> RiskResult:
        """Calculate position size based on a fixed allowed loss amount.

        Args:
            context (RiskContext): Current account state and trade parameters.
                context.stop_loss must be set; context.take_profit is optional.
            ohlc_df (pd.DataFrame, optional): OHLC data available for subclasses
                or custom indicator calculations. Not used by this strategy.

        Returns:
            RiskResult: Volume sized so that SL hit costs allowed_loss_volume,
                with stop_loss_price and take_profit_price taken from context.

        Raises:
            ValueError: If context.stop_loss is None.
        """
        if context.stop_loss is None:
            raise ValueError("stop_loss must be provided in RiskContext for FixedAmountRisk.")

        sl_diff = abs(context.entry_price - context.stop_loss)
        # TODO: currency exchange if needed
        raw_volume = self.allowed_loss_volume / sl_diff

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
