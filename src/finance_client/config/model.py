

from dataclasses import dataclass


@dataclass
class AccountRiskConfig:
    base_currency: str  # "JPY"
    max_single_trade_percent: float
    max_total_risk_percent: float
    daily_max_loss_percent: float
    allow_aggressive_mode: bool
    aggressive_multiplier: float
    enforce_volume_reduction: bool
    atr_ratio_min_stop_loss: float

@dataclass
class SymbolRiskConfig:
    min_volume: float
    volume_step: float
    risk_percent: float
    contract_size: float
    leverage: float