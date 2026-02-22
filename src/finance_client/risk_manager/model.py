from dataclasses import dataclass

from finance_client.config import SymbolRiskConfig


@dataclass
class RiskResult:
    volume: float
    stop_loss_price: float
    take_profit_price: float | None
    risk_amount: float
    reward_amount: float | None
    risk_reward_ratio: float | None

@dataclass
class RiskContext:
    """
    this is to decouple RiskOption from ClientBase and make it more testable
        - account_equity: current account equity
        - account_balance: current account balance
        - daily_realized_pnl: today's realized PnL, which is used to consider max daily loss limit
        - open_positions_risk_amount: total risk amount of open positions, which is used to consider max concurrent position limit
        - max_daily_loss: max daily loss amount, which is used to calculate remaining risk capacity for the day
    """
    account_equity: float
    account_balance: float
    daily_realized_pnl: float
    open_positions_risk_amount: float
    symbol_risk_config: SymbolRiskConfig
