# ClientBase Specification

## Overview

`ClientBase` (`src/finance_client/client_base.py`) is the abstract base class for all exchange/broker client implementations. It provides a unified interface for OHLC data fetching, position management, order submission, and risk-managed trading. Concrete clients (CSV, MT5, Coincheck, yfinance, etc.) extend this class and implement the required abstract methods.

---

## Constructor

```python
ClientBase(
    free_margin: float = 1000000.0,
    used_margin: float = 0.0,
    provider: str = "Default",
    account_risk_config: str | AccountRiskConfig = None,
    symbol_risk_config: str | SymbolRiskConfig = None,
    symbols: List[str] = None,
    ohlc_columns: tuple | list = None,
    idc_process = None,
    pre_process = None,
    economic_keys = None,
    frame = None,
    start_index: int = 0,
    observation_length: int = None,
    user_name: str = None,
    do_render: bool = False,
    enable_trade_log: bool = False,
    storage: db.PositionStorageBase = None,
    log_storage: db.LogStorageBase = None,
    risk_option: RiskOption = None,
)
```

### Key Parameters

| Parameter | Type | Description |
|---|---|---|
| `free_margin` | float | Initial free margin available for trading |
| `used_margin` | float | Initial used margin (already allocated) |
| `provider` | str | Broker/exchange identifier, used to namespace position data in storage |
| `account_risk_config` | str \| AccountRiskConfig | Account-level risk limits (file path or dataclass). Falls back to `config/user.yaml` if `None` |
| `symbol_risk_config` | str \| SymbolRiskConfig | Symbol-level trading parameters (file path or dataclass). Falls back to `config/oanda_standard.yaml` if `None` |
| `risk_option` | RiskOption | Default risk sizing strategy used when `volume=None` in `open_trade()` |
| `storage` | PositionStorageBase | Position persistence backend. Defaults to SQLite (`finance_client.db`) |
| `log_storage` | LogStorageBase | Trade log backend. Defaults to CSV |
| `idc_process` | list | Technical indicator pipeline applied to OHLC data |
| `pre_process` | list | Normalization/preprocessing pipeline applied to OHLC data |
| `frame` | int \| Frame | Default timeframe for OHLC requests |

### Initialization Side Effects

- Creates an `account.Manager` instance bound to `self.account`
- Creates a `RiskManager` instance bound to `self.risk_manager`
- Configures position storage (SQLite by default) and trade log storage (CSV by default)

---

## Trading Methods

### `open_trade()`

```python
def open_trade(
    is_buy: bool,
    symbol: str,
    volume: float = None,
    price: float = None,
    tp: float = None,
    sl: float = None,
    order_type: int = 0,
    risk_option: RiskOption = None,
    *args, **kwargs,
) -> tuple[bool, Position | None]
```

Primary method for opening positions. Supports market, limit, and stop orders.

**Volume resolution:**
- If `volume` is provided, it is used directly.
- If `volume=None`, a `RiskOption` must be available (via the `risk_option` argument or `self.risk_option`). The volume is then computed automatically via `RiskManager.evaluate_risk()`.

**TP/SL resolution:**
- Caller-provided `tp`/`sl` are used if set.
- If `None`, the values computed by the `RiskOption` (from `RiskResult`) are used as fallback.

**Order types (`order_type`):**

| Value | Enum | Behaviour |
|---|---|---|
| `0` | `ORDER_TYPE.market` | Execute at current ask (buy) or bid (sell) price |
| `1` | `ORDER_TYPE.limit` | Pending order; executes when price reaches `price` |
| `2` | `ORDER_TYPE.stop` | Pending stop order at `price` |

Returns `(True, Position)` on success, `(False, error_message)` on failure.

---

### `smart_order()`

```python
def smart_order(
    is_buy: bool,
    symbol: str,
    risk_option: RiskOption = None,
    entry_price: float = None,
    tp: float = None,
    sl: float = None,
    order_type: int = 0,
    ohlc_df=None,
) -> tuple[bool, Position | None]
```

Convenience wrapper around `open_trade()` that **always** uses risk-based volume sizing. Raises `ValueError` if no `RiskOption` is available.

Accepts an optional `ohlc_df` so the caller can pass pre-fetched OHLC data directly to the risk option (useful for backtesting scenarios where data is already available).

---

### `order()`

Alias of `open_trade()`. `volume` is required (no auto-sizing).

---

### `close_position()`

```python
def close_position(
    position: Position = None,
    id = None,
    volume: float = None,
    symbol: str = None,
    position_side = None,
    price: float = None,
) -> ClosedResult
```

Closes an open position fully or partially.

- Specify `position` or `id` to close by identity.
- Specify `symbol` + `position_side` to close without a stored reference.
- `volume=None` closes the full position.
- Returns a `ClosedResult` with `profit`, `price_diff`, and `error` flag.

---

## Risk Management Architecture

Risk handling is split across several cooperating objects. The call chain is:

```
open_trade() / smart_order()
    └── RiskManager.evaluate_risk(risk_option, ...)
            ├── risk_option.calculate(RiskContext, ohlc_df)  →  RiskResult (raw)
            └── _apply_account_caps(volume, stop_distance, context)  →  final volume
```

### Objects Involved

| Class | Location | Role |
|---|---|---|
| `RiskOption` (ABC) | `risk_manager/risk_options/risk_option.py` | Strategy interface; subclasses compute raw volume + SL/TP |
| `RiskContext` | `risk_manager/model.py` | Read-only snapshot of account state + trade intent |
| `RiskResult` | `risk_manager/model.py` | Output of `calculate()`: volume, SL price, TP price, R:R metrics |
| `RiskManager` | `risk_manager/risk_manager.py` | Orchestrates context construction, calls strategy, applies caps |
| `account.Manager` | `account.py` | Source of live account data (balance, equity, daily PnL, open risk) |
| `AccountRiskConfig` | `config/model.py` | Account-level limits loaded from YAML |
| `SymbolRiskConfig` | `config/model.py` | Symbol-level parameters (contract size, leverage, lot step) |

---

## RiskContext

`RiskContext` is assembled by `RiskManager._build_risk_context()` before every trade evaluation. It decouples the risk strategies from `ClientBase`.

| Field | Source | Description |
|---|---|---|
| `is_buy` | caller | Trade direction |
| `account_equity` | `ClientBase.get_equity()` | Current mark-to-market equity |
| `account_balance` | `account.Manager.get_balance()` | Cash balance including open position cost |
| `daily_realized_pnl` | `account.Manager.get_daily_realized_pnl()` | Today's closed P&L (used for daily loss cap) |
| `open_positions_loss_risk` | `account.Manager.get_open_positions_risk_loss()` | Sum of `volume × |entry - SL| × contract_size` across all open positions with SL |
| `entry_price` | caller / market price | Intended entry price |
| `stop_loss` | caller | Intended SL price (may be `None`; ATRRisk overrides it) |
| `take_profit` | caller | Intended TP price (may be `None`; ATRRisk overrides it) |
| `symbol_risk_config` | `RiskManager.get_symbol_config(symbol)` | Per-symbol limits/parameters |
| `max_total_loss_risk` | `account.Manager.get_max_total_loss_risk()` | Remaining budget = `max_total_risk_percent × balance − open_risk − daily_pnl` |
| `daily_max_loss` | `account.Manager.get_daily_max_loss()` | `daily_max_loss_percent × balance` |

---

## RiskResult

Output of `RiskOption.calculate()`.

| Field | Description |
|---|---|
| `volume` | Position size in lots (may be further reduced by account caps) |
| `stop_loss_price` | Final SL price used (may differ from `context.stop_loss` for ATRRisk) |
| `take_profit_price` | Final TP price (`None` if not applicable) |
| `risk_volume` | Monetary risk if SL is hit: `volume × |entry − SL| × contract_size` |
| `reward_volume` | Monetary reward if TP is hit (`None` if TP not set) |
| `risk_reward_ratio` | `reward_volume / risk_volume` (`None` if TP not set) |

---

## Built-in RiskOption Strategies

### `PercentEquityRisk`

**File:** `risk_manager/risk_options/percent_equity.py`

Risks a fixed **percentage of account equity** per trade.

```
allowed_loss    = account_equity × (percent / 100)
sl_distance     = |entry_price − stop_loss|
loss_per_unit   = sl_distance × contract_size
raw_volume      = allowed_loss / loss_per_unit
volume          = floor(raw_volume / volume_step) × volume_step
```

- Requires `context.stop_loss` to be set by the caller.
- `take_profit` is passed through from the caller unchanged.

**Constructor:**

```python
PercentEquityRisk(percent: float)  # e.g. 1.0 for 1%
```

---

### `FixedAmountRisk`

**File:** `risk_manager/risk_options/fixed_loss.py`

Risks a fixed **monetary amount** per trade (e.g. ¥15,000).

```
sl_distance  = |entry_price − stop_loss|
raw_volume   = allowed_loss_volume / sl_distance
volume       = floor(raw_volume / volume_step) × volume_step
```

- Requires `context.stop_loss` to be set by the caller.
- Does **not** adjust for contract size in the raw volume calculation (currency exchange not yet implemented).

**Constructor:**

```python
FixedAmountRisk(allowed_loss_volume: float)  # e.g. 15000 for ¥15,000
```

---

### `ATRRisk`

**File:** `risk_manager/risk_options/atr.py`

Derives both SL and TP automatically from **ATR** (Average True Range). The caller does **not** need to supply `stop_loss` or `take_profit`; both are overridden.

```
atr           = ATRProcess.run(ohlc_df).last_value
sl_distance   = atr × atr_multiplier

# Long
stop_loss     = entry_price − sl_distance
take_profit   = entry_price + sl_distance × rr_ratio

# Short
stop_loss     = entry_price + sl_distance
take_profit   = entry_price − sl_distance × rr_ratio

allowed_loss  = account_equity × (percent / 100)
loss_per_unit = sl_distance × contract_size
raw_volume    = allowed_loss / loss_per_unit
volume        = floor(raw_volume / volume_step) × volume_step
```

- Requires `ohlc_df` to be passed into `calculate()`. The minimum required OHLC length can be queried via `get_required_ohlc_length()`.
- `ClientBase.open_trade()` fetches OHLC automatically when `risk_option.get_required_ohlc_length() > 0`.

**Constructor:**

```python
ATRRisk(
    percent: float,          # equity risk % per trade
    atr_multiplier: float,   # SL = ATR × this
    rr_ratio: float,         # TP = SL distance × this
    atr_window: int = 14,
    ohlc_columns: list[str] = None,
    atr_process: ATRProcess = None,
)
```

---

## Account-Level Caps (`_apply_account_caps`)

After `RiskOption.calculate()` returns a raw volume, `RiskManager._apply_account_caps()` applies additional constraints in this order:

1. **Minimum volume** (`SymbolRiskConfig.min_volume`): `volume = max(volume, min_volume)`
2. **Maximum total risk** (`AccountRiskConfig.max_total_risk_percent`): caps volume so that adding this trade does not exceed the remaining total-risk budget across all open positions
3. **Daily maximum loss** (`AccountRiskConfig.daily_max_loss_percent`): caps volume so that, if SL is hit, today's total realized loss does not exceed the daily limit

> Note: `max_volume` cap (upper bound on lots) exists in the config but is currently commented out.

---

## Configuration

### `AccountRiskConfig` (`config/model.py`)

Loaded from a YAML file (default: `config/user.yaml`).

| Field | Type | Description |
|---|---|---|
| `base_currency` | str | Account currency (e.g. `"JPY"`) |
| `max_single_trade_percent` | float | Max equity % risked on a single trade |
| `max_total_risk_percent` | float | Max total equity % at risk across all open positions |
| `daily_max_loss_percent` | float | Max equity % that can be lost in a single trading day |
| `allow_aggressive_mode` | bool | Enable/disable aggressive sizing mode |
| `aggressive_multiplier` | float | Volume multiplier when aggressive mode is active |
| `enforce_volume_reduction` | bool | Whether to strictly enforce volume reduction from caps |
| `atr_ratio_min_stop_loss` | float | Minimum SL distance expressed as ATR ratio |

### `SymbolRiskConfig` (`config/model.py`)

Loaded from a YAML file (default: `config/oanda_standard.yaml`). One config per traded symbol.

| Field | Type | Description |
|---|---|---|
| `min_volume` | float | Minimum tradeable lot size (e.g. `0.01`) |
| `volume_step` | float | Lot-size increment (e.g. `0.01`) |
| `risk_percent` | float | Default risk % for this symbol (used by strategy if not overridden) |
| `contract_size` | float | Units per lot (e.g. `100000` for standard FX lot) |
| `leverage` | float | Leverage ratio (e.g. `25.0`) |

---

## `account.Manager`

`account.Manager` (`account.py`) is the live account state object. `ClientBase` creates one at init (`self.account`) and `RiskManager` reads from it to build `RiskContext`.

### Key Methods

| Method | Returns | Description |
|---|---|---|
| `get_balance()` | float | Free margin + sum of used margin of all open positions |
| `get_free_margin()` | float | Cash not tied up in margin |
| `get_used_margin()` | float | Sum of `volume × entry × contract_size / leverage` for all open positions |
| `get_daily_realized_pnl(date=None)` | float | Sum of closed trade profits for the current day (UTC by default) |
| `get_open_positions_risk_loss()` | float | Sum of `volume × |entry − SL| × trade_unit` for all open positions that have a SL set |
| `get_max_total_loss_risk()` | float | Remaining risk budget: `max_total_risk_percent × balance − open_risk − daily_pnl` |
| `get_daily_max_loss()` | float | `daily_max_loss_percent × balance` |
| `open_position(...)` | Position | Persists a new position, deducts margin, registers TP/SL listeners |
| `close_position(id, price, volume)` | ClosedResult | Removes/reduces position, returns margin, logs trade |
| `update_position(position, tp, sl)` | bool | Updates TP/SL on an existing position |

---

## Data Models

### `Position`

| Field | Type | Description |
|---|---|---|
| `id` | str (UUID) | Unique position identifier |
| `position_side` | POSITION_SIDE | `long` or `short` |
| `symbol` | str | Traded instrument |
| `price` | float | Entry price |
| `volume` | float | Position size in lots |
| `trade_unit` | int | Contract size (units per lot) |
| `leverage` | float | Leverage applied |
| `tp` | float \| None | Take profit price |
| `sl` | float \| None | Stop loss price |
| `index` | datetime \| None | Timestamp of the bar when opened |
| `timestamp` | datetime | Wall-clock time of position creation (UTC) |

### `Order`

Pending limit/stop order. Shares the same fields as `Position` plus:

| Field | Type | Description |
|---|---|---|
| `order_type` | ORDER_TYPE | `limit` or `stop` |
| `magic_number` | int | UUID-derived integer linking order to position |

### `ClosedResult`

Returned by `close_position()`.

| Field | Description |
|---|---|
| `id` | Position ID |
| `price` | Close price |
| `entry_price` | Original entry price |
| `volume` | Volume closed |
| `price_diff` | `close − entry` (positive = profitable long / loss short) |
| `profit` | `price_diff × volume × trade_unit` |
| `error` | `True` if close failed |
| `msg` | Error message when `error=True` |

---

## Abstract Methods (must be implemented by subclasses)

| Method | Description |
|---|---|
| `get_current_ask(symbol)` | Current ask price for `symbol` |
| `get_current_bid(symbol)` | Current bid price for `symbol` |
| `get_ohlc(symbol, length, ...)` | Fetch OHLC DataFrame |
| `get_equity()` | Current account equity (free margin + unrealized P&L) |
| `_market_buy(symbol, price, volume, tp, sl, ...)` | Submit market buy to broker/exchange |
| `_market_sell(symbol, price, volume, tp, sl, ...)` | Submit market sell to broker/exchange |
| `_buy_limit(...)` / `_sell_limit(...)` | Submit limit orders |
| `_buy_stop(...)` / `_sell_stop(...)` | Submit stop orders |

---

## Class Attributes

| Attribute | Default | Description |
|---|---|---|
| `simulation` | `False` | When `True`, orders are simulated locally (no broker API calls) |
| `back_test` | `False` | When `True`, client operates in historical backtesting mode |

---

## Storage Backends (`db.py`)

| Class | Type | Description |
|---|---|---|
| `PositionSQLiteStorage` | Position | SQLite-backed position persistence (default) |
| `PositionFileStorage` | Position | JSON file-backed position persistence |
| `LogCSVStorage` | Log | CSV trade log (default for `account.Manager`) |

---

## Known Issues and TODO Items

### `AccountRiskConfig` Fields Not Connected to Risk Logic

Five fields in `AccountRiskConfig` are defined and loaded from YAML but are **never read** anywhere in `RiskManager`, `_apply_account_caps`, or any `RiskOption`. They have no effect on current behaviour.

| Field | Intended Purpose | Status |
|---|---|---|
| `max_single_trade_percent` | Cap equity % risked on a single trade | Defined, never read |
| `allow_aggressive_mode` | Enable a higher-risk sizing mode | Defined, never read |
| `aggressive_multiplier` | Volume multiplier when aggressive mode is on | Defined, never read |
| `enforce_volume_reduction` | Strictly enforce cap-driven volume reduction | Defined, never read |
| `atr_ratio_min_stop_loss` | Minimum SL distance expressed as ATR multiple | Defined, never read |

**Impact:** Setting these values in a config file silently has no effect. Any strategy relying on `max_single_trade_percent` as a per-trade guard, or on `allow_aggressive_mode` to boost sizing, will not behave as expected until these are wired into `_apply_account_caps` or the relevant `RiskOption`.

---

### `SymbolRiskConfig` max_volume Cap Commented Out

In `RiskManager._apply_account_caps()` the upper-volume cap block is commented out:

```python
# if context.symbol_risk_config.max_volume is not None:
#     volume = min(volume, context.symbol_risk_config.max_volume)
```

Additionally, `max_volume` is not a field of `SymbolRiskConfig` at all — the field definition is missing from `config/model.py`. Until both the field and the cap logic are added, there is no upper bound on lot size from symbol configuration.

---

### ~~Double Subtraction of Open-Position Risk in `_apply_account_caps`~~ ✓ Fixed

Previously `_apply_account_caps` subtracted `open_positions_loss_risk` from `context.max_total_loss_risk` even though `get_max_total_loss_risk()` already returns the remaining budget with open risk deducted. Additionally, the remaining monetary budget was compared directly to `volume` (lots) without converting via `stop_distance × contract_size`.

**Fix applied** (`risk_manager.py`): the cap now correctly converts the remaining monetary budget to a max lot size:

```python
max_volume_by_total_risk = context.max_total_loss_risk / (stop_distance * context.symbol_risk_config.contract_size)
volume = min(volume, max_volume_by_total_risk)
```

---

### ~~Sign Convention Bug in Daily Loss Cap~~ ✓ Fixed

`account.Manager.get_daily_realized_pnl()` returns the **signed** sum of closed-trade profits. Losses produce a negative value. The old formula subtracted this negative PnL, making the remaining budget grow on a losing day.

**Fix applied** in both `risk_manager.py` (`_apply_account_caps`) and `account.py` (`get_max_total_loss_risk`):

```python
daily_loss = max(0.0, -context.daily_realized_pnl)   # convert signed PnL to a positive loss amount
remaining_loss = max(0.0, context.daily_max_loss - daily_loss)
```

---

### `FixedAmountRisk` Missing `contract_size` in Volume Calculation

`PercentEquityRisk` correctly divides by `contract_size`:

```python
loss_per_unit = sl_distance * context.symbol_risk_config.contract_size
raw_volume = allowed_loss / loss_per_unit
```

`FixedAmountRisk` does not:

```python
raw_volume = self.allowed_loss_volume / sl_diff   # contract_size omitted
```

For instruments with `contract_size != 1` (e.g., a standard FX lot at 100,000), the resulting volume will be inflated by a factor of `contract_size`. A currency exchange step is also flagged with a `# TODO` comment in the same method.

---

### Pending Orders Not Persisted

`ClientBase._open_orders` is an in-memory `dict`. Limit and stop orders placed via `open_trade()` are stored there but are **never written to the position storage backend**. A `# TODO: persist orders` comment marks the insertion point. On process restart all pending orders are lost.

---

### TP/SL Auto-Close State Not Persisted

When a position is closed automatically by a TP/SL trigger, the result is stored in the in-memory dict `ClientBase.__closed_position_with_exist`. A `# TODO: use history data instead of dict` comment marks the location. This state does not survive a restart, which can cause a subsequent `close_position()` call on the same ID to misreport the outcome.
