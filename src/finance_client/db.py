import datetime
import glob
import json
import logging
import os
import sqlite3
import threading
import time
from abc import ABCMeta, abstractmethod
from typing import List

import pandas as pd

from finance_client.position import POSITION_SIDE, Position

logger = logging.getLogger(__name__)


def _check_path(file_path, default_file_name: str):
    if file_path is None:
        file_path = os.path.join(os.getcwd(), default_file_name)
    else:
        extension = default_file_name.split(".")[-1]
        if extension not in file_path:
            raise ValueError(f"only {extension} is supported. specified {file_path}")
    base_path = os.path.dirname(file_path)
    if base_path:
        if os.path.exists(base_path) is False:
            os.makedirs(base_path)
    return file_path


def _get_all_user_position_filepaths():
    any_user_folder = os.path.join(os.getcwd(), "user", "*", "positions.json")
    all_user_folders = glob.glob(any_user_folder)
    return all_user_folders


def _index_to_str(index):
    if index is None:
        time_index = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    elif isinstance(index, datetime.datetime):
        time_index = index.isoformat()
    elif isinstance(index, str):
        time_index = index
    elif isinstance(index, (int, float)):
        time_index = datetime.datetime.fromtimestamp(index, tz=datetime.timezone.utc).isoformat()
    elif isinstance(index, datetime.date):
        time_index = index.isoformat()
    elif isinstance(index, pd.Timestamp):
        time_index = index.to_pydatetime().isoformat()
    else:
        raise ValueError(f"Unsupported index type: {type(index)}. index must be datetime, str, int, float, date or pd.Timestamp.")
    return time_index


class LogStorageBase(metaclass=ABCMeta):

    def __init__(self, provider: str, username: str) -> None:
        self.provider = provider
        if username is None:
            username = "__none__"
        if isinstance(username, str) is False:
            raise ValueError("username must be str")

        self.username = username

    def _convert_position_to_log(self, position: Position, order_type: int) -> dict:
        """
        Args:
            position (Position): position object to convert log item
            order_type (int): type of the order (1 for open, -1 for close, 0 for update)
        Returns:
            dict: log item dictionary
        """

        time_index = _index_to_str(position.index)
        log_item = {}
        log_item["position_id"] = position.id
        log_item["provider"] = self.provider
        log_item["username"] = self.username
        log_item["symbol"] = position.symbol
        log_item["time_index"] = time_index
        log_item["price"] = position.price
        log_item["volume"] = position.volume
        log_item["position_side"] = position.position_side.value
        log_item["order_type"] = order_type
        log_item["logged_at"] = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        return log_item

    @abstractmethod
    def store_log(self, position: Position, order_type: int, save_profit: bool = False):
        logger.warning("store_log is not implemented in LogStorageBase.")
        pass

    @abstractmethod
    def store_logs(self, items: dict, save_profit: bool = False):
        logger.warning("store_logs is not implemented in LogStorageBase.")
        pass

    # prepare various _get_log functions for different query types (by id, by date, etc.)
    @abstractmethod
    def _get_log_with_id(self, provider, username, id) -> pd.DataFrame:
        logger.warning("get_log is not implemented in LogStorageBase.")
        return pd.DataFrame()

    @abstractmethod
    def _get_open_log_with_id(self, provider, username, id) -> pd.DataFrame:
        logger.warning("get_log is not implemented in LogStorageBase.")
        return pd.DataFrame()

    @abstractmethod
    def get_logs(self, provider, username, start=None, end=None) -> pd.DataFrame:
        logger.warning("get_logs is not implemented in LogStorageBase.")
        return pd.DataFrame()

    @abstractmethod
    def get_profit_logs(self, provider, username, start=None, end=None) -> pd.DataFrame:
        logger.warning("get_profit_logs is not implemented in LogStorageBase.")
        return pd.DataFrame()

    def get_log(self, provider, username, id=None, order_type=None) -> pd.DataFrame:
        if id is not None and order_type == 1:
            return self._get_open_log_with_id(provider, username, id)
        if id is not None:
            return self._get_log_with_id(provider, username, id)
        else:
            all_logs = self.get_logs(provider, username)
            if len(all_logs) > 0:
                if order_type is not None:
                    all_logs = all_logs[all_logs["order_type"] == order_type]
                log_df = all_logs.sort_values(by="logged_at", ascending=False)
                log_df = log_df.iloc[:-1]
            else:
                log_df = pd.DataFrame()
            return log_df

    def _get_profit(self, close_position: Position):
        log_df = self.get_log(provider=self.provider, username=self.username, id=close_position.id, order_type=1)
        if len(log_df) > 0:
            log_df = log_df.sort_values(by="logged_at", ascending=False)
            open_log = log_df[log_df["order_type"] == 1].iloc[0]
            if close_position.position_side == POSITION_SIDE.long:
                profit = (close_position.price - open_log["price"]) * close_position.volume * close_position.trade_unit * close_position.leverage
            else:
                profit = (open_log["price"] - close_position.price) * close_position.volume * close_position.trade_unit * close_position.leverage
            return profit
        logger.warning(f"no log found for position id {close_position.id}. profit cannot be calculated.")
        return None

    def _create_place_holder(self, num: int):
        place_holders = f"({'?,' * (num -1)}?)"
        return place_holders

    def _create_basic_query(self, table_schema_keys: list):
        keys = ",".join(table_schema_keys)
        place_holders = self._create_place_holder(len(table_schema_keys))
        return keys, place_holders

    def close(self):
        pass


class LogCSVStorage(LogStorageBase):

    def __init__(self, provider, username=None, trade_log_path: str = None, account_history_path: str = None) -> None:
        super().__init__(provider=provider, username=username)
        self.provider = provider
        self.trade_log_path = _check_path(trade_log_path, "finance_trade_log.csv")
        self.account_history_path = _check_path(account_history_path, "finance_account_history.csv")
        self.__trade_logs = pd.DataFrame()

    def store_log(self, position: Position, order_type: int, save_profit: bool = False):
        log_item = self._convert_position_to_log(position, order_type)
        df = pd.DataFrame.from_dict([log_item])
        save_header = not os.path.exists(self.trade_log_path)
        df.to_csv(self.trade_log_path, mode="a", header=save_header, index_label=None, index=False)

        # store log in memory for later retrieval
        try:
            self.__trade_logs = pd.concat([self.__trade_logs, df], ignore_index=True)
        except Exception as e:
            logger.warning(f"Failed to store log in memory: {e}")

        if save_profit:
            profit = self._get_profit(position)
            if profit is not None:
                account_history_item = {
                    "position_id": position.id,
                    "provider": self.provider,
                    "username": self.username,
                    "symbol": position.symbol,
                    "time_index": _index_to_str(position.index),
                    "profit": profit,
                    "logged_at": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
                }
                account_history_df = pd.DataFrame.from_dict([account_history_item])
                save_header = not os.path.exists(self.account_history_path)
                account_history_df.to_csv(self.account_history_path, mode="a", header=save_header, index_label=None, index=False)
            else:
                logger.warning(f"Profit is None for position id {position.id}. account history log cannot be saved.")

    def store_logs(self, items: dict, save_profit: bool = False):
        if len(items) > 0:
            if isinstance(items, dict) is False:
                log_items = {}
                for item in items:
                    log_item = self._convert_position_to_log(item)
                    log_items[log_item["position_id"]] = log_item
            else:
                log_items = items
            df = pd.DataFrame.from_dict(log_items)
            save_header = not os.path.exists(self.trade_log_path)
            df.to_csv(self.trade_log_path, mode="a", header=save_header, index_label=None, index=False)

            # store log in memory for later retrieval
            try:
                self.__trade_logs = pd.concat([self.__trade_logs, df], ignore_index=True)
            except Exception as e:
                logger.warning(f"Failed to store log in memory: {e}")

            if save_profit:
                account_history_items = []
                for position_id, log_item in log_items.items():
                    profit = self._get_profit(log_item)
                    if profit is not None:
                        account_history_item = {
                            "position_id": log_item["position_id"],
                            "provider": self.provider,
                            "username": self.username,
                            "symbol": log_item["symbol"],
                            "time_index": _index_to_str(log_item["time_index"]),
                            "profit": profit,
                            "logged_at": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
                        }
                        account_history_items.append(account_history_item)
                    else:
                        logger.warning(f"Profit is None for position id {log_item['position_id']}. account history log cannot be saved.")
                if len(account_history_items) > 0:
                    account_history_df = pd.DataFrame.from_dict(account_history_items)
                    save_header = not os.path.exists(self.account_history_path)
                    account_history_df.to_csv(self.account_history_path, mode="a", header=save_header, index_label=None, index=False)

    def __filter_logs(self, logs, provider=None, username=None, start=None, end=None, id=None):
        if len(logs) == 0:
            return logs
        if provider is not None:
            try:
                logs = logs[logs["provider"] == provider]
            except Exception as e:
                logger.warning(f"Failed to filter logs by provider: {e}")
        if username is not None:
            try:
                logs = logs[logs["username"] == username]
            except Exception as e:
                logger.warning(f"Failed to filter logs by username: {e}")
        if start is not None:
            try:
                logs = logs[logs["time_index"] >= start]
            except Exception as e:
                logger.warning(f"Failed to filter logs by start time: {e}")
        if end is not None:
            try:
                logs = logs[logs["time_index"] <= end]
            except Exception as e:
                logger.warning(f"Failed to filter logs by end time: {e}")
        if id is not None:
            try:
                logs = logs[logs["position_id"] == id]
            except Exception as e:
                logger.warning(f"Failed to filter logs by id: {e}")
        return logs

    def __get_log_with_cache(
        self,
        provider,
        username,
        id,
    ) -> pd.DataFrame:
        log_df = self.__filter_logs(self.__trade_logs, provider=provider, username=username, id=id)
        if len(log_df) > 0:
            log_df = log_df.sort_values(by="logged_at", ascending=False)
            return log_df
        if os.path.exists(self.trade_log_path):
            df = pd.read_csv(self.trade_log_path)
            self.__trade_logs = df.copy()
            log_df = self.__filter_logs(df, provider=provider, username=username, id=id)
            if len(log_df) > 1:
                log_df = log_df.sort_values(by="logged_at", ascending=False)
            return log_df
        else:
            return pd.DataFrame()

    def _get_log_with_id(self, provider, username, id) -> pd.DataFrame:
        log_df = self.__get_log_with_cache(provider, username, id)
        if len(log_df) > 1:
            log_df = log_df.iloc[:-1]
        return log_df

    def _get_open_log_with_id(self, provider, username, id) -> pd.DataFrame:
        log_df = self.__get_log_with_cache(provider, username, id)
        if len(log_df) > 0:
            open_log_df = log_df[log_df["order_type"] == 1]
            if len(open_log_df) > 1:
                return open_log_df.iloc[-1:]
            else:
                return open_log_df
        return pd.DataFrame()

    def get_logs(self, provider=None, username=None, start=None, end=None) -> pd.DataFrame:
        if provider is None:
            provider = self.provider
        if username is None:
            username = self.username
        df = pd.read_csv(self.trade_log_path)
        log_df = self.__filter_logs(df, provider=provider, username=username, start=start, end=end)
        return log_df

    def get_profit_logs(self, provider=None, username=None, start=None, end=None) -> pd.DataFrame:
        if provider is None:
            provider = self.provider
        if username is None:
            username = self.username
        if os.path.exists(self.account_history_path):
            df = pd.read_csv(self.account_history_path, parse_dates=["time_index", "logged_at"])
            profit_log_df = self.__filter_logs(df, provider=provider, username=username, start=start, end=end)
            return profit_log_df
        else:
            return pd.DataFrame()


class LogSQLiteStorage(LogStorageBase):
    TRADE_TABLE_NAME = "trade"
    _TRADE_TABLE_KEYS = {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "position_id": "TEXT",
        "provider": "TEXT",
        "username": "TEXT",
        "symbol": "TEXT",
        "time_index": "TEXT",
        "price": "REAL",
        "volume": "REAL",
        "position_side": "INT",
        "order_type": "INT",
        "logged_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    }

    _PROFIT_TABLE_KEYS = {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "position_id": "TEXT",
        "provider": "TEXT",
        "username": "TEXT",
        "symbol": "TEXT",
        "time_index": "TEXT",
        "profit": "REAL",
        "logged_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    }

    __lock = threading.Lock()

    def __init__(self, database_path, provider: str, username=None) -> None:
        super().__init__(provider=provider, username=username)

        self.__database_path = database_path
        self._table_init()

    def _table_init(self):
        conn = sqlite3.connect(self.__database_path)
        cursor = conn.cursor()
        table_schema = ",".join([f"{key} {attr}" for key, attr in self._TRADE_TABLE_KEYS.items()])
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.TRADE_TABLE_NAME} (
                {table_schema}
            )
            """
        )
        conn.commit()

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS profit (
                {','.join([f"{key} {attr}" for key, attr in self._PROFIT_TABLE_KEYS.items()])}
            )
            """
        )
        conn.commit()
        cursor.close()
        conn.close()

    def __commit(self, query, params: tuple):
        with self.__lock:
            conn = sqlite3.connect(self.__database_path)
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            cursor.close()
            conn.close()

    def __multi_commit(self, query, params_list: list):
        with self.__lock:
            conn = sqlite3.connect(self.__database_path)
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
            cursor.close()
            conn.close()

    def store_log(self, position: Position, order_type: int, save_profit: bool = False):
        values_dict = self._convert_position_to_log(position, order_type)
        keys, place_holders = self._create_basic_query(list(self._TRADE_TABLE_KEYS.keys())[1:])
        query = f"INSERT INTO {self.TRADE_TABLE_NAME} ({keys}) VALUES {place_holders}"
        self.__commit(query, tuple(values_dict.values()))

        if save_profit:
            profit = self._get_profit(position)
            if profit is not None:
                logger.info(f"profit for position id {position.id}: {profit}")
                profit_log_item = {
                    "position_id": position.id,
                    "provider": self.provider,
                    "username": self.username,
                    "symbol": position.symbol,
                    "time_index": _index_to_str(position.index),
                    "profit": profit,
                    "logged_at": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
                }
                keys, place_holders = self._create_basic_query(list(self._PROFIT_TABLE_KEYS.keys())[1:])
                query = f"INSERT INTO profit ({keys}) VALUES {place_holders}"
                self.__commit(query, tuple(profit_log_item.values()))
            else:
                logger.warning(f"Profit is None for position id {position.id}. profit cannot be calculated.")

    def store_logs(self, items: dict, save_profit: bool = False):
        log_values = [tuple(self._convert_position_to_log(p, order_type).values()) for p, order_type in items.items()]
        keys, place_holders = self._create_basic_query(list(self._TRADE_TABLE_KEYS.keys())[1:])
        query = f"INSERT INTO {self.TRADE_TABLE_NAME} ({keys}) VALUES {place_holders}"
        self.__multi_commit(query, log_values)

        if save_profit:
            profit_log_items = []
            for position, order_type in items.items():
                profit = self._get_profit(position)
                if profit is not None:
                    logger.info(f"profit for position id {position.id}: {profit}")
                    profit_log_item = {
                        "position_id": position.id,
                        "provider": self.provider,
                        "username": self.username,
                        "symbol": position.symbol,
                        "time_index": _index_to_str(position.index),
                        "profit": profit,
                        "logged_at": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
                    }
                    profit_log_items.append(tuple(profit_log_item.values()))
                else:
                    logger.warning(f"Profit is None for position id {position.id}. profit cannot be calculated.")
            if len(profit_log_items) > 0:
                keys, place_holders = self._create_basic_query(list(self._PROFIT_TABLE_KEYS.keys())[1:])
                query = f"INSERT INTO profit ({keys}) VALUES {place_holders}"
                self.__multi_commit(query, profit_log_items)

    def _get_log_with_id(self, provider, username, id):
        with self.__lock:
            conn = sqlite3.connect(self.__database_path)
            query = f"SELECT * FROM {self.TRADE_TABLE_NAME} WHERE provider=? AND username=? AND position_id=?"
            df = pd.read_sql_query(
                query,
                conn,
                params=(provider, username, id),
            )
            conn.close()
            if len(df) > 0:
                df = df.sort_values(by="logged_at", ascending=False)
                df = df.iloc[-1:]
            return df

    def _get_open_log_with_id(self, provider, username, id):
        with self.__lock:
            conn = sqlite3.connect(self.__database_path)
            query = f"SELECT * FROM {self.TRADE_TABLE_NAME} WHERE provider=? AND username=? AND position_id=? AND order_type=1"
            df = pd.read_sql_query(query, conn, params=(provider, username, id), parse_dates=["time_index", "logged_at"])
            conn.close()
            if len(df) > 0:
                df = df.sort_values(by="logged_at", ascending=False)
                df = df.iloc[-1:]
            return df

    def get_logs(self, provider, username, start=None, end=None) -> pd.DataFrame:
        with self.__lock:
            conn = sqlite3.connect(self.__database_path)
            query = f"SELECT * FROM {self.TRADE_TABLE_NAME} WHERE provider=? AND username=?"
            df = pd.read_sql_query(query, conn, params=(provider, username), parse_dates=["time_index", "logged_at"])
            conn.close()
        if start is not None:
            df = df[df["time_index"] >= start]
        if end is not None:
            df = df[df["time_index"] <= end]
        return df

    def get_profit_logs(self, provider=None, username=None, start=None, end=None) -> pd.DataFrame:
        with self.__lock:
            if provider is None:
                provider = self.provider
            if username is None:
                username = self.username
            conn = sqlite3.connect(self.__database_path)
            query = "SELECT * FROM profit WHERE provider=? AND username=?"
            df = pd.read_sql_query(query, conn, params=(provider, username), parse_dates=["time_index", "logged_at"])
            conn.close()
        if start is not None:
            df = df[df["time_index"] >= start]
        if end is not None:
            df = df[df["time_index"] <= end]
        return df

    def close(self):
        return super().close()


class PositionStorageBase:
    """Base Storage for position. Store position in memory."""

    def __init__(self, provider: str, username: str = None) -> None:
        self.provider = provider
        if username is None:
            username = "__none__"
        if isinstance(username, str) is False:
            raise ValueError("username must be str")

        self.username = username
        self._positions = {POSITION_SIDE.long: {}, POSITION_SIDE.short: {}}

    def store_position(self, position: Position):
        self._positions[position.position_side][position.id] = position

    def store_positions(self, positions: List[Position]):
        for position in positions:
            self.store_position(position)

    def store_symbol_info(self, symbol, rating=None, date=None, source=None, market=None):
        pass

    def has_position(self, id) -> bool:
        return (id in self._positions[POSITION_SIDE.long]) or (id in self._positions[POSITION_SIDE.short])

    def get_position(self, id) -> Position:
        if id in self._positions[POSITION_SIDE.long]:
            return self._positions[POSITION_SIDE.long][id]
        elif id in self._positions[POSITION_SIDE.short]:
            return self._positions[POSITION_SIDE.short][id]
        return None

    def get_positions(self, symbols: list = None):
        return self.get_long_positions(symbols=symbols), self.get_short_positions(symbols=symbols)

    def get_long_positions(self, symbols: list = None) -> list:
        long_positions = self._positions[POSITION_SIDE.long].values()
        if len(long_positions) == 0:
            return long_positions
        if symbols is None or len(symbols) == 0:
            return long_positions
        else:
            if isinstance(symbols, str):
                symbols = [symbols]
            return list(filter(lambda position: position.symbol in symbols, long_positions))

    def get_short_positions(self, symbols: list = None) -> list:
        short_positions = self._positions[POSITION_SIDE.short].values()
        if symbols is None or len(symbols) == 0:
            return short_positions
        else:
            if isinstance(symbols, str):
                symbols = [symbols]
            return list(filter(lambda position: position.symbol in symbols, short_positions))

    def get_symbol_info(self, symbol, source):
        return []

    def get_trade_logs(self):
        logger.warning("get_trade_logs is not implemented in PositionStorageBase. return empty list.")
        return []

    def _get_listening_positions(self) -> List[Position]:
        listening_positions = {}
        for positions in self._positions.values():
            for position in positions.values():
                if position.tp is not None or position.sl is not None:
                    listening_positions[position.id] = position
        return listening_positions

    def delete_position(self, id):
        if id in self._positions[POSITION_SIDE.long]:
            position = self._positions[POSITION_SIDE.long].pop(id)
            return True, position
        elif id in self._positions[POSITION_SIDE.short]:
            position = self._positions[POSITION_SIDE.short].pop(id)
            return True, position
        return False, None

    def update_position(self, position):
        self.store_position(position)

    def close(self):
        pass

    def _create_place_holder(self, num: int):
        place_holders = f"({'?,' * (num -1)}?)"
        return place_holders

    def _create_basic_query(self, table_schema_keys: list):
        keys = ",".join(table_schema_keys)
        place_holders = self._create_place_holder(len(table_schema_keys))
        return keys, place_holders


class PositionFileStorage(PositionStorageBase):
    __position_lock = threading.Lock()
    __symbol_loc = threading.Lock()

    def __init__(
        self,
        provider: str,
        username: str,
        positions_path: str = None,
        rating_log_path: str = None,
        save_period: float = 0,
    ) -> None:
        """File Handler to store objects. This class doesn't care a bout accesses from multiple instances.

        Args:
            provider (str): provider id to separate position information
            username (str): username to separate position information
            positions_path (str, optional): custom position file path. Defaults to None.
            save_period (float, optional): minutes to periodically save positions. less than 0 save them immedeately when it is updated. Defaults to 1.0.
        """
        super().__init__(provider, username)
        self.rating_log_path = _check_path(rating_log_path, "symbols.json")
        if self.username is None:
            self.positions_path = _check_path(positions_path, "positions.json")
        else:
            self.positions_path = _check_path(positions_path, os.path.join("user", f"_{self.username}_", "positions.json"))
        if not os.path.exists(self.positions_path):
            os.makedirs(os.path.dirname(self.positions_path), exist_ok=True)
            with open(self.positions_path, mode="w") as fp:
                json.dump({}, fp)
        self._load_positions()

        self.__update_required = False
        self.save_period = save_period * 60.0
        self.__running = True
        if save_period > 0:
            self.__immediate_save = False
            threading.Thread(target=self._prerodical_update, daemon=True).start()
        else:
            self.__immediate_save = True

    def _save_json(self, obj, file_path):
        try:
            with open(file_path, mode="w") as fp:
                json.dump(obj, fp)
        except Exception:
            logger.exception("failed to save")

    def _load_json(self, file_path):
        try:
            with open(file_path, mode="r") as fp:
                return json.load(fp)
        except Exception as e:
            logger.debug(f"failed to load: {e}")

    def __update_positions_file(self):
        new_positions = self._positions.copy()
        with self.__position_lock:
            _positions = self._load_json(self.positions_path)
        if _positions is None:
            _positions = {}

        # since we have loaded existing position in init
        provider_positions = {
            POSITION_SIDE.long.name: [position.to_dict() for position in new_positions[POSITION_SIDE.long].values()],
            POSITION_SIDE.short.name: [position.to_dict() for position in new_positions[POSITION_SIDE.short].values()],
        }
        _positions[self.provider] = provider_positions
        with self.__position_lock:
            self._save_json(_positions, self.positions_path)

    def __update_rating_file(self, rating_info: List[List]):
        """
        Args:
            rating_info (List[List[Union[str, str, str, str, str]]]): List of [symbol, rating_info, date, source, str]
        """
        with self.__symbol_loc:
            data = None
            if os.path.exists(self.rating_log_path):
                data = self._load_json(self.rating_log_path)
            if data is None:
                data = {}
            for info in rating_info:
                symbol = info[0]
                if symbol in data:
                    data[symbol].append(info[1:])
                else:
                    data[symbol] = [info[1:]]
            self._save_json(data, self.rating_log_path)

    def _prerodical_update(self):
        while self.__running:
            time.sleep(self.save_period)
            if self.__update_required is True:
                self.__update_positions_file()
                self.__update_required = False

    def get_symbol_info(self, symbol, source):
        with self.__symbol_loc:
            data = self._load_json(self.rating_log_path)
        if symbol in data:
            data = data[symbol]
            df = pd.DataFrame(data, columns=["rating", "date", "source", "market"])
            source_df = df[df["source"] == source]
            rating_info = source_df.iloc[-1].values.tolist()
            return rating_info[:3]
        else:
            return None

    def delete_position(self, id):
        suc, p = super().delete_position(id)
        if self.__immediate_save is True:
            self.__update_positions_file()
        self.__update_required = True
        return suc, p

    def store_position(self, position: Position):
        super().store_position(position)
        if self.__immediate_save is True:
            self.__update_positions_file()
        self.__update_required = True

    def store_positions(self, positions: List[Position]):
        super().store_positions(positions)
        t = threading.Thread(target=self.__update_positions_file)
        t.start()

    def store_symbol_info(self, symbol, rating=None, date=None, source=None, market=None):
        if date is not None and isinstance(date, datetime.datetime):
            date = date.isoformat()
        symbol_info = [symbol, rating, date, source, market]
        self.__update_rating_file([symbol_info])

    def store_symbols_info(self, symbols_info_list: List[List]):
        self.__update_rating_file(symbols_info_list)

    def update_position(self, position):
        if self.has_position(position.id) is False:
            return False
        super().store_position(position)
        if self.__immediate_save is True:
            self.__update_positions_file()
            # save log file separately as position doesn't have closed price
        self.__update_required = True
        return True

    def _load_positions(self):
        """

        Returns:
            Tuple(Dict[id:Position], Dict[id:Position], Dict[id:Position]): return long, short, listening tp/sl positions
        """
        long_positions = {}
        short_positions = {}
        listening_positions = {}
        if os.path.exists(self.positions_path):
            with self.__position_lock:
                positions_dict = self._load_json(self.positions_path)
            if positions_dict is None:
                positions_dict = {}
            if self.username is None:
                user_position_paths = _get_all_user_position_filepaths()
                for user_path in user_position_paths:
                    user_positions_dict = self._load_json(user_path)
                    for user_provider, user_positions in user_positions_dict.items():
                        if user_provider in positions_dict:
                            positions_dict[user_provider].update(user_positions)
                        else:
                            positions_dict[user_provider] = user_positions
            if len(positions_dict) == 0:
                return long_positions, short_positions, listening_positions
            if self.provider in positions_dict:
                _position = positions_dict[self.provider]
                long_position_list = _position[POSITION_SIDE.long.name]
                short_position_list = _position[POSITION_SIDE.short.name]

                for _position in long_position_list:
                    position = Position(**_position)
                    self._positions[POSITION_SIDE.long][position.id] = position

                for _position in short_position_list:
                    position = Position(**_position)
                    self._positions[POSITION_SIDE.short][position.id] = position

    def close(self):
        self.__running = False
        self.__update_positions_file()


class PositionSQLiteStorage(PositionStorageBase):
    POSITION_TABLE_NAME = "position"
    _POSITION_TABLE_KEYS = {
        "id": "TEXT PRIMARY KEY",
        "provider": "TEXT",
        "username": "TEXT",
        "symbol": "TEXT",
        "position_side": "INTEGER",
        "trade_unit": "REAL",
        "leverage": "REAL",
        "price": "REAL",
        "tp": "REAL",
        "sl": "REAL",
        "time_index": "TEXT",
        "volume": "REAL",
        "timestamp": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        "result": "TEXT",
        "option": "TEXT",
    }
    SYMBOL_TABLE_NAME = "symbol"
    _SYMBOL_TABLE_KEYS = {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "code": "TEXT",
        "name": "TEXT",
        "market": "TEXT",
    }

    RATING_TABLE_NAME = "rating"
    _RATING_TABLE_KEYT = {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "symbol_id": "INTEGER",
        "ratings": "TEXT",
        "created_at": "DATE DEFAULT CURRENT_DATE",
        "source": "TEXT",
    }
    TRADE_TABLE_NAME = "trade"
    __lock = threading.Lock()

    def _table_init(self):
        conn = sqlite3.connect(self.__database_path)
        cursor = conn.cursor()
        table_schema = ",".join([f"{key} {attr}" for key, attr in self._POSITION_TABLE_KEYS.items()])
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.POSITION_TABLE_NAME} (
                {table_schema}
            )
            """
        )
        table_schema = ",".join([f"{key} {attr}" for key, attr in self._SYMBOL_TABLE_KEYS.items()])
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.SYMBOL_TABLE_NAME} (
                {table_schema}
            )
            """
        )
        table_schema = ",".join([f"{key} {attr}" for key, attr in self._RATING_TABLE_KEYT.items()])
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.RATING_TABLE_NAME} (
                {table_schema},
                FOREIGN KEY (symbol_id) REFERENCES {self.SYMBOL_TABLE_NAME}(id)
            )
            """
        )
        cursor.close()
        conn.close()

    def __init__(self, database_path, provider: str, username: str) -> None:
        super().__init__(provider, username)
        if username is None:
            self.username = "__none__"

        self.__database_path = database_path
        self._table_init()

    def __commit(self, query, params):
        with self.__lock:
            conn = sqlite3.connect(self.__database_path)
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            cursor.close()
            conn.close()

    def __multi_commit(self, query, params_list):
        with self.__lock:
            conn = sqlite3.connect(self.__database_path)
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
            cursor.close()
            conn.close()

    def __fetch(self, query, params):
        with self.__lock:
            conn = sqlite3.connect(self.__database_path)
            cursor = conn.cursor()
            cursor.execute(query, params)
            records = cursor.fetchall()
            cursor.close()
            conn.close()
        return records

    def __records_to_positions(self, records, keys) -> List[Position]:
        positions = []
        keys = list(keys)
        for record in records:
            kwargs = {}
            for index, value in enumerate(record):
                kwargs[keys[index]] = value
            positions.append(Position(**kwargs))
        return positions

    def store_position(self, position: Position):
        keys, place_holders = self._create_basic_query(self._POSITION_TABLE_KEYS.keys())
        time_index = _index_to_str(position.index)
        if position.timestamp is None:
            position.timestamp = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        values = (
            position.id,
            self.provider,
            self.username,
            position.symbol,
            position.position_side.value,
            position.trade_unit,
            position.leverage,
            position.price,
            position.tp,
            position.sl,
            time_index,
            position.volume,
            position.timestamp,
            position.result,
            position.option,
        )
        query = f"INSERT INTO {self.POSITION_TABLE_NAME} ({keys}) VALUES {place_holders}"
        self.__commit(query, values)

    def store_positions(self, positions: List[Position]):
        keys, place_holders = self._create_basic_query(self._POSITION_TABLE_KEYS.keys())
        values = [
            (
                position.id,
                self.provider,
                self.username,
                position.symbol,
                position.position_side.value,
                position.trade_unit,
                position.leverage,
                position.price,
                position.tp,
                position.sl,
                position.index,
                position.volume,
                position.timestamp,
                position.result,
                position.option,
            )
            for position in positions
        ]
        query = f"INSERT INTO {self.POSITION_TABLE_NAME} ({keys}) VALUES {place_holders}"
        self.__multi_commit(query, values)

    def __get_symbol_id(self, symbol, name=None, market=None, retry=0):
        if symbol is not None and isinstance(symbol, (str, int)):
            get_query = f"SELECT id FROM {self.SYMBOL_TABLE_NAME} WHERE code = ?"
            ids = self.__fetch(get_query, (str(symbol),))
            if len(ids) > 0:
                return ids[0][0]
            else:
                keys, place_holders = self._create_basic_query(list(self._SYMBOL_TABLE_KEYS.keys())[1:])
                query = f"INSERT INTO {self.SYMBOL_TABLE_NAME} ({keys}) VALUES {place_holders}"
                values = (symbol, name, market)
                self.__commit(query, values)
                ids = self.__fetch(get_query, (str(symbol),))
                if len(ids) > 0:
                    return ids[0][0]
                else:
                    return None

    def store_symbol_info(self, symbol, rating: str = None, date: datetime.date = None, source=None, market=None):
        id = self.__get_symbol_id(symbol, name=None, market=market)
        if id is not None:
            if date is None:
                date = datetime.datetime.now(tz=datetime.timezone.utc).date()
            key_list = list(self._RATING_TABLE_KEYT.keys())
            keys, place_holders = self._create_basic_query(key_list[1:])
            query = f"""
            INSERT INTO {self.RATING_TABLE_NAME} ({keys})
            SELECT ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM {self.RATING_TABLE_NAME}
                WHERE symbol_id = ?
                AND created_at >= ?
            );
            """
            values = (id, rating, date, source, id, date)
            self.__commit(query, values)

    def store_symbols_info(self, symbols_info_list: List[List]):
        for item in symbols_info_list:
            self.store_symbol_info(*item)

    def update_position(self, position: Position):
        keys = self._POSITION_TABLE_KEYS.keys()
        targets = [f"{key} = ?" for key in keys]
        if isinstance(position.index, datetime.datetime):
            index = position.index.isoformat()
        else:
            index = position.index
        if position.timestamp is None:
            timestamp = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        elif isinstance(position.timestamp, datetime.datetime):
            timestamp = position.timestamp.isoformat()
        else:
            timestamp = position.timestamp
        values = (
            position.id,
            self.provider,
            self.username,
            position.symbol,
            position.position_side.value,
            position.trade_unit,
            position.leverage,
            position.price,
            position.tp,
            position.sl,
            index,
            position.volume,
            timestamp,
            position.result,
            position.option,
            position.id,
        )
        query = f"UPDATE {self.POSITION_TABLE_NAME} SET {', '.join(targets)} WHERE id = ?"
        self.__commit(query, values)

    def get_position(self, id) -> Position:
        if self.username == "__none__":
            query = f"SELECT * FROM {self.POSITION_TABLE_NAME} WHERE id = ? AND provider = ?"
            records = self.__fetch(query, (id, self.provider))
        else:
            query = f"SELECT * FROM {self.POSITION_TABLE_NAME} WHERE id = ? AND provider = ? AND username = ?"
            records = self.__fetch(query, (id, self.provider, self.username))
        positions = self.__records_to_positions(records, self._POSITION_TABLE_KEYS.keys())
        if len(positions) == 0:
            logger.info(f"no record found for position_id: {id}")
            return None
        else:
            return positions[0]

    def get_positions(self, symbols: list = None, position_side: POSITION_SIDE = None):
        if self.username == "__none__":
            query = f"SELECT * FROM {self.POSITION_TABLE_NAME} WHERE provider = ?"
            params = [self.provider]
        else:
            query = f"SELECT * FROM {self.POSITION_TABLE_NAME} WHERE provider = ? AND username = ?"
            params = [self.provider, self.username]
        if symbols is not None and len(symbols) > 0:
            place_holders = self._create_place_holder(len(symbols))
            query = f"{query} AND symbol in {place_holders}"
            params.extend(symbols)
        if position_side is not None:
            query = f"{query} AND position_side = ?"
            params.append(position_side.value)
        records = self.__fetch(query, params)
        positions = self.__records_to_positions(records, self._POSITION_TABLE_KEYS.keys())
        if len(positions) == 0:
            return [], []
        else:
            long_positions = []
            short_positions = []
            for position in positions:
                if position.position_side == POSITION_SIDE.long:
                    long_positions.append(position)
                else:
                    short_positions.append(position)
            return long_positions, short_positions

    def get_long_positions(self, symbols: List[str] = None) -> List[Position]:
        long_positions, _ = self.get_positions(symbols, POSITION_SIDE.long)
        return long_positions

    def get_short_positions(self, symbols: List[str] = None) -> List[Position]:
        _, short_positions = self.get_positions(symbols, POSITION_SIDE.short)
        return short_positions

    def get_symbol_info(self, symbol, source):
        id = self.__get_symbol_id(symbol)
        if id is not None:
            query = f"SELECT ratings,MAX(created_at),source FROM {self.RATING_TABLE_NAME} WHERE symbol_id = ? AND source = ?"
            params = (id, source)
            result = self.__fetch(query, params)
            return result[0]
        else:
            return []

    def _get_listening_positions(self):
        if self.username == "__none__":
            query = f"SELECT * FROM {self.POSITION_TABLE_NAME} WHERE provider = ? AND (tp IS NOT NULL OR sl IS NOT NULL)"
            records = self.__fetch(query, (self.provider,))
        else:
            query = f"SELECT * FROM {self.POSITION_TABLE_NAME} WHERE provider = ? AND username= ? AND (tp IS NOT NULL OR sl IS NOT NULL)"
            records = self.__fetch(query, (self.provider, self.username))
        positions = self.__records_to_positions(records, self._POSITION_TABLE_KEYS.keys())
        positions_dict = {}
        for position in positions:
            positions_dict[position.id] = position
        return positions_dict

    def delete_position(self, id):
        p = self.get_position(id)
        query = f"DELETE FROM {self.POSITION_TABLE_NAME}"
        if self.username == "__none__":
            cond = "WHERE id = ? AND provider = ?"
            params = (id, self.provider)
        else:
            cond = "WHERE id = ? AND provider = ? AND username = ?"
            params = (id, self.provider, self.username)
        query = f"{query} {cond}"
        try:
            self.__commit(query, params)
            return True, p
        except Exception:
            return False, p
