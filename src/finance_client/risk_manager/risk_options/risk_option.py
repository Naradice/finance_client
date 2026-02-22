"""
Alt options:
- Kellyベース
- ボラティリティ調整型
- 相関考慮型
"""

from abc import ABC, abstractmethod

from finance_client.risk_manager.model import RiskContext


class RiskOption(ABC):

    @abstractmethod
    def calculate_volume(
        self,
        risk_context: RiskContext,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
    ) -> float:
        ...
        
    def _round_volume(self, volume: float, context: RiskContext) -> float:
        stepped = (
            volume // context.symbol_risk_config.volume_step
        ) * context.symbol_risk_config.volume_step
        
        return max(stepped, context.symbol_risk_config.min_volume)

    def _apply_account_caps(
        self,
        volume: float,
        context: RiskContext,
    ) -> float:
        if context.symbol_risk_config.max_volume is not None:
            volume = min(volume, context.symbol_risk_config.max_volume)
        
        if context.account_risk_config.max_total_volume is not None:
            remaining_volume = max(
                0.0,
                context.account_risk_config.max_total_volume - context.open_positions_risk_amount
            )
            volume = min(volume, remaining_volume)
        
        if context.account_risk_config.max_daily_loss is not None:
            remaining_loss = max(
                0.0,
                context.account_risk_config.max_daily_loss - context.daily_realized_pnl
            )
            max_volume_by_loss = remaining_loss / context.pip_value_per_lot
            volume = min(volume, max_volume_by_loss)

        return volume