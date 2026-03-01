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
    ) -> float: ...

    def _round_volume(self, volume: float, context: RiskContext) -> float:
        stepped = (volume // context.symbol_risk_config.volume_step) * context.symbol_risk_config.volume_step

        return max(stepped, context.symbol_risk_config.min_volume)
