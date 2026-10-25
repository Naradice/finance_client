# Database Layer Specification (`db.py`)

## Overview

`db.py` provides two independent storage hierarchies:

- **Position storage** — persists open `Position` objects so they survive restarts and are shared across users/providers.
- **Log storage** — records every order event (open, update, close) and stores realized profit per closed trade for daily PnL queries.

Both hierarchies expose a common abstract base class. Two concrete backends are provided for each: a file-based implementation and a SQLite implementation. They can be mixed freely (e.g., SQLite for positions + CSV for logs).

```
PositionStorageBase (in-memory)
├── PositionFileStorage   (JSON file, persisted)
└── PositionSQLiteStorage (SQLite, persisted)

LogStorageBase (abstract)
├── LogCSVStorage         (two CSV files)
└── LogSQLiteStorage      (SQLite, same or separate DB)
```

---

## Namespacing

Every storage object is scoped by two keys that are stored alongside each record:

| Key | Purpose |
|---|---|
| `provider` | Identifies the broker/exchange (e.g. `"mt5"`, `"coincheck"`, `"Default"`) |
| `username` | Identifies the user within a provider. If `None` at construction time it is stored as the literal string `"__none__"` |

All queries filter by `(provider, username)` automatically.

---

## Position Storage

### `PositionStorageBase`

In-memory base class. Stores positions in a dict-of-dicts keyed by `POSITION_SIDE`:

```python
self._positions = {
    POSITION_SIDE.long:  { position_id: Position, ... },
    POSITION_SIDE.short: { position_id: Position, ... },
}
```

All concrete subclasses inherit the in-memory dict and add persistence on top.

#### Methods

| Method | Returns | Description |
|---|---|---|
| `store_position(position)` | — | Add or overwrite a position in memory |
| `store_positions(positions)` | — | Bulk insert |
| `get_position(id)` | Position \| None | Look up by UUID |
| `get_positions(symbols=None)` | (long_list, short_list) | Return all positions, optionally filtered by symbol |
| `get_long_positions(symbols=None)` | list[Position] | Long side only |
| `get_short_positions(symbols=None)` | list[Position] | Short side only |
| `has_position(id)` | bool | Membership check |
| `update_position(position)` | — | Overwrite existing position in memory (base implementation calls `store_position`) |
| `delete_position(id)` | (bool, Position \| None) | Remove from memory; returns success flag and the removed object |
| `_get_listening_positions()` | dict[id, Position] | Return all positions that have `tp` or `sl` set |
| `store_symbol_info(...)` | — | No-op in base; subclasses may persist symbol metadata |
| `close()` | — | No-op in base |

---

### `PositionFileStorage`

JSON file backend. Positions are serialised via `Position.to_dict()` and written to a single JSON file structured as:

```json
{
  "provider_name": {
    "long":  [ { ...position_dict... }, ... ],
    "short": [ { ...position_dict... }, ... ]
  }
}
```

Multiple providers can coexist in the same file. Multiple users each get their own file.

#### Constructor

```python
PositionFileStorage(
    provider: str,
    username: str,
    positions_path: str = None,    # defaults to {cwd}/positions.json (no username)
                                   # or {cwd}/user/{username}/positions.json (with username)
    rating_log_path: str = None,   # defaults to {cwd}/symbols.json
    save_period: float = 0,        # minutes between periodic saves; ≤0 = save immediately
)
```

#### File paths (defaults)

| Scenario | Path |
|---|---|
| `username=None` | `{cwd}/positions.json` |
| `username="alice"` | `{cwd}/user/alice/positions.json` |
| Rating/symbol log | `{cwd}/symbols.json` |

#### Save behaviour

| `save_period` | Behaviour |
|---|---|
| `> 0` | Background thread flushes every `save_period` minutes when dirty |
| `<= 0` (default) | Writes to disk immediately on every mutation |

A class-level `threading.Lock` (`__position_lock`) serialises all file reads and writes.

#### Differences from base

| Method | Behaviour added |
|---|---|
| `store_position` | Triggers immediate or deferred file save |
| `update_position` | Returns `False` if ID does not already exist (unlike base which always upserts) |
| `delete_position` | Triggers file save after removal |
| `store_positions` | Saves asynchronously in a new thread |
| `store_symbol_info` | Appends to `symbols.json` keyed by symbol |
| `_load_positions` | Reads the JSON file at init time; if `username=None` also scans all `user/*/positions.json` files |
| `close` | Stops the periodic-save thread and writes the final state |

---

### `PositionSQLiteStorage`

SQLite backend. All operations go through `sqlite3` with a class-level `threading.Lock` (`__lock`) to serialise connections.

#### Constructor

```python
PositionSQLiteStorage(
    database_path: str,   # path to the .db file
    provider: str,
    username: str,
)
```

#### Database schema

Three tables are created on first use (`CREATE TABLE IF NOT EXISTS`):

**`position`**

| Column | SQLite type | Notes |
|---|---|---|
| `id` | `TEXT PRIMARY KEY` | UUID string from `Position.id` |
| `provider` | `TEXT` | |
| `username` | `TEXT` | |
| `symbol` | `TEXT` | |
| `position_side` | `INTEGER` | `1` = long, `-1` = short |
| `trade_unit` | `REAL` | |
| `leverage` | `REAL` | |
| `price` | `REAL` | Can be `NULL` for market orders placed while closed |
| `tp` | `REAL` | Nullable |
| `sl` | `REAL` | Nullable |
| `time_index` | `TEXT` | ISO-8601 string |
| `volume` | `REAL` | |
| `timestamp` | `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` | Wall-clock open time |
| `result` | `TEXT` | Broker-returned ticket/result (nullable) |
| `option` | `TEXT` | JSON-serialised extra data (nullable) |

**`symbol`**

| Column | SQLite type |
|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` |
| `code` | `TEXT` |
| `name` | `TEXT` |
| `market` | `TEXT` |

**`rating`**

| Column | SQLite type | Notes |
|---|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | |
| `symbol_id` | `INTEGER` | FK → `symbol.id` |
| `ratings` | `TEXT` | |
| `created_at` | `DATE DEFAULT CURRENT_DATE` | |
| `source` | `TEXT` | |

#### Differences from base

| Method | Behaviour |
|---|---|
| `store_position` | `INSERT INTO position ...` |
| `store_positions` | Bulk `INSERT` via `executemany` |
| `update_position` | `UPDATE position SET ... WHERE id = ?` — does **not** return False for nonexistent IDs (unlike `PositionFileStorage`) |
| `delete_position` | `DELETE FROM position WHERE id = ? ...` |
| `get_position(id)` | `SELECT` with `provider` + optional `username` filter |
| `get_positions(symbols, position_side)` | `SELECT` with optional symbol list and side filter |
| `_get_listening_positions` | `SELECT WHERE tp IS NOT NULL OR sl IS NOT NULL` |
| `store_symbol_info` | Upsert into `symbol` + insert into `rating` (no duplicate per day) |

#### Known issue — `update_position` return value

`PositionFileStorage.update_position()` returns `False` when the position ID does not exist. `PositionSQLiteStorage.update_position()` always returns `None` (does not check rowcount), so a silent no-op occurs for nonexistent IDs. Code that relies on the `False` return to detect missing positions will behave differently depending on which backend is used.

---

## Log Storage

### `LogStorageBase`

Abstract base. Provides common utilities used by both concrete backends.

#### Log record fields (`_convert_position_to_log`)

| Field | Source |
|---|---|
| `position_id` | `position.id` |
| `provider` | `self.provider` |
| `username` | `self.username` |
| `symbol` | `position.symbol` |
| `time_index` | `position.index` (via `_index_to_str`, ISO-8601) |
| `price` | `position.price` |
| `tp` | `position.tp` |
| `sl` | `position.sl` |
| `trade_unit` | `position.trade_unit` |
| `leverage` | `position.leverage` |
| `volume` | `position.volume` |
| `position_side` | `position.position_side.value` |
| `order_type` | `1` open / `0` update / `-1` close |
| `logged_at` | UTC wall-clock at time of call |

#### `order_type` values

| Value | Meaning |
|---|---|
| `1` | Position opened |
| `0` | Position updated (TP/SL change) |
| `-1` | Position closed |

#### Abstract methods

| Method | Description |
|---|---|
| `store_log(position, order_type, profit=None)` | Write one log entry; if `order_type == -1`, also write profit |
| `store_logs(items, save_profit=False)` | Bulk write |
| `_get_log_with_id(provider, username, id)` | All log rows for a position ID |
| `_get_open_log_with_id(provider, username, id)` | The open (`order_type==1`) row for a position ID |
| `get_logs(provider, username, start, end)` | All rows in a date range |
| `get_profit_logs(provider, username, start, end)` | Profit rows in a date range (returns a DataFrame with a `profit` column) |

#### Concrete helper methods (on base)

| Method | Description |
|---|---|
| `get_log(provider, username, id=None, order_type=None)` | Convenience router: delegates to `_get_open_log_with_id`, `_get_log_with_id`, or `get_logs` depending on arguments |
| `_get_profit(close_position)` | Back-calculates profit from the open log row if `profit` was not passed directly |

---

### `LogCSVStorage`

Two CSV files: one for all trade events, one for realized profits only.

#### Constructor

```python
LogCSVStorage(
    provider: str,
    username: str = None,
    trade_log_path: str = None,       # defaults to {cwd}/logs/finance_trade_log.csv
    account_history_path: str = None, # defaults to {cwd}/logs/finance_account_history.csv
)
```

#### File paths (defaults)

| File | Default path |
|---|---|
| Trade log | `{cwd}/logs/finance_trade_log.csv` |
| Profit log | `{cwd}/logs/finance_account_history.csv` |

#### In-memory cache

An in-memory `pd.DataFrame` (`__trade_logs`) caches the rows appended in the current session. `_get_log_with_id` checks this cache first before falling back to reading the CSV file.

#### Date filtering limitation (known issue)

`__filter_logs` compares `time_index >= start` directly. When the CSV is read without `parse_dates`, `time_index` columns are raw strings. Comparing a string column to a `pd.Timestamp` produces the warning:

```
'>=' not supported between instances of 'str' and 'Timestamp'
```

and filtering silently fails, returning all rows. `get_profit_logs` uses `parse_dates=["time_index", "logged_at"]` and is not affected. `get_logs` and `_get_log_with_id` are affected. Use `LogSQLiteStorage` for reliable date-range filtering.

#### Cross-run accumulation

Because CSV files are appended (`mode="a"`) and never truncated by the storage class itself, data from previous process runs accumulates in the same file. Tests and tools that rely on `get_profit_logs` returning only today's trades must either provide a fresh file path or clean the file between runs.

---

### `LogSQLiteStorage`

SQLite backend for log data. Operates on two tables within the same database file.

#### Constructor

```python
LogSQLiteStorage(
    database_path: str,   # path to the .db file (can be the same file as PositionSQLiteStorage)
    provider: str,
    username: str = None,
)
```

#### Database schema

**`trade`** (all order events)

| Column | SQLite type |
|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` |
| `position_id` | `TEXT` |
| `provider` | `TEXT` |
| `username` | `TEXT` |
| `symbol` | `TEXT` |
| `time_index` | `TEXT` (ISO-8601) |
| `price` | `REAL` |
| `volume` | `REAL` |
| `tp` | `REAL` |
| `sl` | `REAL` |
| `trade_unit` | `REAL` |
| `leverage` | `REAL` |
| `position_side` | `INT` |
| `order_type` | `INT` |
| `logged_at` | `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` |

**`profit`** (realized profit per close)

| Column | SQLite type |
|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` |
| `position_id` | `TEXT` |
| `provider` | `TEXT` |
| `username` | `TEXT` |
| `symbol` | `TEXT` |
| `time_index` | `TEXT` (ISO-8601) |
| `profit` | `REAL` |
| `logged_at` | `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` |

#### Thread safety

A class-level `threading.Lock` (`__lock`) wraps every `sqlite3.connect / execute / commit / close` sequence. All connections are opened and closed per operation (no persistent connection).

#### Date filtering

`get_logs` and `get_profit_logs` use `pd.read_sql_query(..., parse_dates=["time_index", "logged_at"])` so the returned DataFrame has proper `Timestamp` columns. Date-range filtering (`df[df["time_index"] >= start]`) works correctly.

---

## Utility Functions

### `_check_path(file_path, default_file_name)`

Resolves a storage file path. If `file_path` is `None`, falls back to `{cwd}/{default_file_name}`. Validates that the file extension matches the expected type. Creates the parent directory if it does not exist.

### `_index_to_str(index)`

Converts a bar timestamp of any supported type to an ISO-8601 string for storage.

| Input type | Conversion |
|---|---|
| `None` | UTC now |
| `datetime.datetime` | `.isoformat()` |
| `str` | passed through |
| `int` / `float` | `datetime.fromtimestamp(..., UTC).isoformat()` |
| `datetime.date` | `.isoformat()` |
| `pd.Timestamp` | `.to_pydatetime().isoformat()` |

---

## Backend Comparison

| Feature | `PositionFileStorage` | `PositionSQLiteStorage` |
|---|---|---|
| Format | JSON | SQLite |
| Multi-provider in one file | Yes | Yes (filtered by `provider`) |
| Thread safety | Class-level lock on file I/O | Class-level lock on each DB operation |
| `update_position` (missing ID) | Returns `False` | Returns `None` (silent no-op) |
| Symbol/rating metadata | `symbols.json` side-file | `symbol` + `rating` tables |
| Suitable for production | Simple single-user use | Multi-user / concurrent access |

| Feature | `LogCSVStorage` | `LogSQLiteStorage` |
|---|---|---|
| Format | Two CSV files | Two tables in one SQLite file |
| Date filtering | Broken when CSV read without `parse_dates` | Works correctly |
| Cross-run accumulation | Yes (append-only) | Yes (insert-only, but per-run queries are isolated by date) |
| Can share file with position storage | No | Yes (same `.db` file) |
| Recommended for | Simple logging / manual review | Programmatic queries, daily PnL, testing |

---

## Default Backends Used by `ClientBase`

`ClientBase.__init__` creates storage if none is provided:

```python
# Position storage — SQLite at {cwd}/finance_client.db
storage = db.PositionSQLiteStorage(db_path, provider, user_name)

# Log storage — delegated to account.Manager
# account.Manager defaults to LogCSVStorage(provider, username=position_storage.username)
```

To override, pass explicit `storage` and `log_storage` arguments to the client constructor.
