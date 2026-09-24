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
        - quote_to_account_rate: conversion rate from the traded symbol's quote currency into the
            account's base currency (e.g. ~153 for a JPY-based account trading a USD-quoted pair
            like EURUSD — 1 USD of stop-loss distance is worth ~153 JPY). Defaults to 1.0, which is
            exactly correct when the symbol's quote currency already matches the account currency
            (e.g. USDJPY/EURJPY on a JPY account) — the common case, and the only one this system
            was originally sized for. Every monetary calculation in this module (loss_per_unit,
            _apply_account_caps' volume caps) is quote-currency-denominated via stop_distance *
            contract_size; without this factor, sizing a trade on a pair not quoted in the account's
            own currency silently mixes units and can produce a wildly oversized (or undersized)
            volume — this was caught after a real EURUSD order sized to 1.2 lots against a
            ¥30,000 budget (should have been ~0.01) on a JPY account.
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
    quote_to_account_rate: float = 1.0