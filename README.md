# finance_client

Core library for an algorithmic trading platform. Provides abstract broker/data clients, risk management, technical indicators, and storage backends.

## Installation

```bash
pip install -e .
```

Or install dependencies directly:

```bash
pip install -r requirements.txt
```

## Project Structure

```
src/finance_client/
├── client_base.py        # Abstract base class for all broker/exchange clients
├── account.py            # Account state: equity, margin, positions, daily PnL
├── position.py           # Position/Order dataclasses and enums
├── db.py                 # Pluggable storage backends (SQLite, JSON, CSV)
├── risk_manager/         # Strategy-pattern risk sizing system
│   ├── risk_manager.py   # RiskManager: evaluates risk, applies account caps
│   ├── model.py          # RiskContext, RiskResult, config dataclasses
│   └── risk_options/     # PercentEquityRisk, FixedAmountRisk, ATRRisk
├── fprocess/             # Technical indicator pipeline (MACD, EMA, BB, ATR, RSI, ...)
├── config/               # YAML config loaders (AccountRiskConfig, SymbolRiskConfig)
├── csv/                  # CSV-based data client
├── mt5/                  # MetaTrader5 broker client
├── coincheck/            # Coincheck crypto exchange client
├── yfinance/             # yfinance data client
├── vantage/              # Alpha Vantage data client
├── sbi/                  # SBI securities client (Selenium-based RPA)
└── es/                   # ElasticSearch client
```

## Architecture

### ClientBase

All broker/exchange clients extend `ClientBase`. It composes:
- `account.Manager` — live account state
- `RiskManager` — evaluates risk before order submission
- Storage backends — persists positions and trade logs

Order flow: `open_trade()` → `RiskManager.evaluate_risk()` → `RiskOption.calculate()` → account caps applied → broker order submitted.

### Risk Management

Strategy-pattern design decoupling risk sizing from execution:

- `RiskOption` subclasses implement `calculate(RiskContext) → RiskResult`
- `RiskContext` — read-only snapshot of account state + trade intent
- `RiskManager` — applies account-level caps (max daily loss %, max total risk %)

Config is loaded from YAML:
- `config/user.yaml` — account-level limits
- `config/oanda_standard.yaml` — per-symbol parameters

### Data Processing (`fprocess/`)

Composable pipeline of indicator classes applied to OHLC DataFrames: MACD, EMA, Bollinger Bands, ATR, RSI, Renko, Slope, plus roll/reframe utilities and MinMax normalization.

### Storage (`db.py`)

Backends scoped by `(provider, username)`:
- `PositionSQLiteStorage` / `PositionFileStorage` (JSON)
- `LogSQLiteStorage` / `LogCSVStorage`

## Running Tests

```bash
# All tests
python -m unittest discover -s tests -p "*test.py"
python -m unittest discover -s tests/backtest_methods -p "*test.py"

# Single test file
python -m unittest tests/risk_manager.py
```

## Code Style

Black with `line-length = 150`, target Python 3.12+.

```bash
black --line-length 150 src/
```
