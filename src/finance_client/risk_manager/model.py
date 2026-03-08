from dataclasses import dataclass

from finance_client.config import SymbolRiskConfig


@dataclass
class RiskResult:
    volume: float
    stop_loss_price: float
    take_profit_price: float | None
    risk_volume: float
    reward_volume: float | None
    risk_reward_ratio: float | None


@dataclass
class RiskContext:
    """
    this is to decouple RiskOption from ClientBase and make it more testable
        - account_equity: current account equity
        - account_balance: current account balance
        - daily_realized_pnl: today's realized PnL, which is used to consider max daily loss limit
        - open_positions_loss_risk: total risk volume of open positions, which is used to consider max concurrent position limit
        - entry_price: price at which the new position is intended to be opened
        - stop_loss: intended stop loss price for the new position
        - take_profit: intended take profit price for the new position
        - max_total_loss_risk: max total loss risk volume, which is used to consider max concurrent position limit together with open_positions_loss_risk
        - daily_max_loss: max daily loss volume, which is used to calculate remaining risk capacity for the day
    """

    is_buy: bool
    account_equity: float
    account_balance: float
    daily_realized_pnl: float
    open_positions_loss_risk: float
    symbol_risk_config: SymbolRiskConfig
    entry_price: float
    stop_loss: float | None
    take_profit: float | None
    max_total_loss_risk: float | None
    daily_max_loss: float | None