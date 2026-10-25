# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

**Install dependencies:**
```bash
pip install -e .
```

**Run all tests:**
```bash
python -m unittest discover -s tests -p "*test.py"
python -m unittest discover -s tests/backtest_methods -p "*test.py"
```

**Run a single test file:**
```bash
python -m unittest tests/risk_manager.py
```

**Code formatting:** Black with line-length 150, target Python 3.12+.

## Architecture

This is a trading client library that abstracts multiple data/broker backends behind a common interface.

### Core Layer

- **`client_base.py`** – Abstract `ClientBase` class that all exchange/broker clients implement. Handles OHLC data fetching, position management, and order submission.
- **`account.py`** – `Manager` class tracks equity, margin, open/closed positions, and daily PnL. The central state object passed to risk management.
- **`position.py`** – `Position` and `Order` dataclasses with `POSITION_SIDE` / `ORDER_TYPE` enums.
- **`db.py`** – Pluggable storage backends: `PositionSQLiteStorage`, `PositionFileStorage` (JSON), `LogCSVStorage`.

### Client Implementations

Each subdirectory under `src/finance_client/` is a concrete client: `csv/`, `mt5/` (MetaTrader5), `coincheck/`, `yfinance/`, `vantage/`, `sbi/`, `es/` (ElasticSearch). All extend `ClientBase`.

### Risk Management (`risk_manager/`)

Strategy-pattern risk sizing system (in active development on `support_risk_option` branch):

- **`RiskOption`** (abstract) – Base class for risk strategies. Subclasses: `PercentEquityRisk`, `FixedAmountRisk`, `ATRRisk`.
- **`RiskContext`** – Decouples risk logic from `ClientBase`. Contains account equity/balance, daily PnL, open position volume, entry/SL/TP prices, and `SymbolRiskConfig`.
- **`RiskResult`** – Output: volume, stop_loss_price, take_profit_price, and risk/reward metrics.
- **`RiskManager`** – Factory for `RiskOption` instances; `evaluate_risk()` applies account-level caps (max volume, max daily loss).

### Configuration (`config/`)

- **`AccountRiskConfig`** / **`SymbolRiskConfig`** – Dataclasses loaded from YAML (e.g., `config/user.yaml`, `config/oanda_standard.yaml`).

### Data Processing (`fprocess/`)

Pipeline of technical indicator classes: MACD, EMA, Bollinger Bands, ATR, RSI, Renko, Slope. Applied via composition to OHLC DataFrames.

### Symbols (`symbols.py`)

Defines tradeable instruments. `frames.py` handles timeframe constants used across clients.
