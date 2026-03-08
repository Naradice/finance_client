"""
Alt options:
- Kellyベース
- ボラティリティ調整型
- 相関考慮型
"""

from abc import ABC, abstractmethod

from finance_client.risk_manager.model import RiskContext, RiskResult


class RiskOption(ABC):
    """Abstract base class for position sizing strategies.

    Subclasses implement calculate() to determine trade volume, stop loss,
    and take profit based on account state and symbol configuration provided
    via RiskContext.
    """

    @abstractmethod
    def calculate(self, context: RiskContext, ohlc_df=None) -> RiskResult:
        """Calculate position size and risk/reward levels for a trade.

        Args:
            context (RiskContext): Current account state and trade parameters,
                including equity, entry price, stop loss, and symbol config.
            ohlc_df (pd.DataFrame, optional): Recent OHLC data for the symbol.
                Subclasses may use this to compute indicators (e.g. ATR) on the
                fly instead of relying on pre-computed values.

        Returns:
            RiskResult: Computed volume, stop_loss_price, take_profit_price,
                and risk/reward metrics.
        """
        ...

    def get_required_ohlc_length(self) -> int:
        """Return the number of recent OHLC bars required for this risk option's calculations.

        This allows the caller to ensure that sufficient historical data is provided
        when calling calculate().

        Returns:
            int: Number of recent OHLC bars needed, or 0 if no historical data is required.
        """
        return 0

    def _round_volume(self, volume: float, context: RiskContext) -> float:
        """Round volume down to the nearest volume_step, with a floor of min_volume.

        Args:
            volume (float): Raw calculated volume.
            context (RiskContext): Provides symbol_risk_config.volume_step and min_volume.

        Returns:
            float: Adjusted volume that respects the broker's lot-size constraints.
        """
        stepped = (volume // context.symbol_risk_config.volume_step) * context.symbol_risk_config.volume_step
        return max(stepped, context.symbol_risk_config.min_volume)

    def _calc_loss(self, volume: float, context: RiskContext, stop_loss_price: float) -> float:
        """Calculate the monetary loss (in account currency) for a given volume and stop level.

        Args:
            volume (float): Trade volume in lots.
            context (RiskContext): Provides entry_price and symbol_risk_config.contract_size.
            stop_loss_price (float): Price at which the loss is realised.

        Returns:
            float: Monetary loss = volume × |entry - stop| × contract_size.
        """
        sl_distance = abs(context.entry_price - stop_loss_price)
        return volume * sl_distance * context.symbol_risk_config.contract_size
    
    def _calc_margin(self, volume: float, context: RiskContext, entry_price: float) -> float:
        """Calculate the margin required for a given volume and entry price.

        Args:
            volume (float): Trade volume in lots.
            context (RiskContext): Provides symbol_risk_config.leverage and contract_size.
            entry_price (float): Price at which the position is opened.
        Returns:
            float: Margin required = volume × entry_price × contract_size / leverage.
        """
        return volume * entry_price * context.symbol_risk_config.contract_size / context.symbol_risk_config.leverage
