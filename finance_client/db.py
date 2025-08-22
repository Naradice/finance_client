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

from finance_client.position import POSITION_TYPE, Position

logger = logging.getLogger(__name__)


def _check_path(file_path, default_file_name: str):
    if file_path is None:
        file_path = os.path.join(os.getcwd(), default_file_name)
    else:
        extension = default_file_name.split(".")[-1]
        if extension not in file_path:
            raise ValueError(f"only {extension} is supported. specified {file_path}")
    base_path = os.path.dirname(file_path)
    if os.path.exists(base_path) is False:
        os.makedirs(base_path)
    return file_path


def _get_all_user_position_filepaths():
    any_user_folder = os.path.join(os.getcwd(), "user", "*", "positions.json")
    all_user_folders = glob.glob(any_user_folder)
    return all_user_folders


class LogStorageBase(metaclass=ABCMeta):
    @classmethod
    @abstractmethod
    def _convert_position_to_log(self, provider, p: Position, is_open):
        return {}

    @classmethod
    @abstractmethod
    def store_log(self, symbol, p: Position, is_open):
        pass

    @classmethod
    @abstractmethod
    def store_logs(self, items):
        pass

    @classmethod
    @abstractmethod
    def get(self):
        return []

    def _create_place_holder(self, num: int):
        place_holders = f"({'?,' * (num -1)}?)"
        return place_holders

    def _create_basic_query(self, table_schema_keys: list):
        keys = ",".join(table_schema_keys)
        place_holders = self._create_place_holder(len(table_schema_keys))
        return keys, place_holders


class LogCSVStorage(LogStorageBase):
    __log_lock = threading.Lock()

    def __init__(self, trade_log_path: str = None) -> None:
        super().__init__()
        self.trade_log_path = _check_path(trade_log_path, "finance_trade_log.csv")

    def _convert_position_to_log(self, provider, username, p: Position, is_open):
        log_item = {}
        log_item["provider"] = provider
        log_item["username"] = username
        log_item["symbol"] = p.symbol
        log_item["time"] = p.index
        log_item["price"] = p.price
        log_item["amount"] = p.amount
        log_item["position_type"] = p.position_type.value
        if is_open is True:
            log_item["order_type"] = 1
        else:
            log_item["order_type"] = -1
        log_item["logged_at"] = datetime.datetime.now(datetime.UTC).isoformat()
        return log_item

    def store_log(self, provider, username, position: Position, is_open):
        log_item = self._convert_position_to_log(provider, username, position, is_open)
        df = pd.DataFrame.from_dict([log_item])
        save_header = not os.path.exists(self.trade_log_path)
        df.to_csv(self.trade_log_path, mode="a", header=save_header, index_label=None, index=False)

    def store_logs(self, items):
        if len(items) > 0:
            if isinstance(items[0], dict) is False:
                log_items = []
                for item in items:
                    log_item = self._convert_position_to_log(*item)
                    log_items.append(log_item)
            else:
                log_items = items
            df = pd.DataFrame.from_dict(log_items)
            save_header = not os.path.exists(self.trade_log_path)
            df.to_csv(self.trade_log_path, mode="a", header=save_header, index_label=None, index=False)

    def get(self, provider, username):
        df = pd.read_csv(self.trade_log_path)
        log_df = df[df["provider"] == provider]
        log_df = log_df[log_df["username"] == username]
        return log_df


class BaseStorage:
    """Base Storage for position. Store position in memory."""

    def __init__(self, provider: str, username: str) -> None:
        self.provider = provider
        self.username = username
        self._positions = {POSITION_TYPE.long: {}, POSITION_TYPE.short: {}}

    def store_position(self, position: Position):
        self._positions[position.position_type][position.id] = position

    def store_positions(self, positions: List[Position]):
        for position in positions:
            self.store_position(position)

    def store_symbol_info(self, symbol, rating=None, date=None, source=None, market=None):
        pass

    def has_position(self, id) -> bool:
        return (id in self._positions[POSITION_TYPE.long]) or (id in self._positions[POSITION_TYPE.short])

    def get_position(self, id) -> Position:
        if id in self._positions[POSITION_TYPE.long]:
            return self._positions[POSITION_TYPE.long][id]
        elif id in self._positions[POSITION_TYPE.short]:
            return self._positions[POSITION_TYPE.short][id]
        return None

    def get_positions(self, symbols: list = None):
        return self.get_long_positions(symbols=symbols), self.get_short_positions(symbols=symbols)

    def get_long_positions(self, symbols: list = None) -> list:
        long_positions = self._positions[POSITION_TYPE.long].values()
        if len(long_positions) == 0:
            return long_positions
        if symbols is None or len(symbols) == 0:
            return long_positions
        else:
            if isinstance(symbols, str):
                symbols = [symbols]
            return list(filter(lambda position: position.symbol in symbols, long_positions))

    def get_short_positions(self, symbols: list = None) -> list:
        short_positions = self._positions[POSITION_TYPE.short].values()
        if symbols is None or len(symbols) == 0:
            return short_positions
        else:
            if isinstance(symbols, str):
                symbols = [symbols]
            return list(filter(lambda position: position.symbol in symbols, short_positions))

    def get_symbol_info(self, symbol, source):
        return []

    def get_trade_logs(self):
        return []

    def _get_listening_positions(self):
        listening_positions = {}
        for positions in self._positions.values():
            for position in positions.values():
                if position.tp is not None or position.sl is not None:
                    listening_positions[position.id] = position
        return listening_positions

    def delete_position(self, id, price=None, amount=None, index=None):
        if id in self._positions[POSITION_TYPE.long]:
            position = self._positions[POSITION_TYPE.long].pop(id)
            return True, position
        elif id in self._positions[POSITION_TYPE.short]:
            position = self._positions[POSITION_TYPE.short].pop(id)
            return True, position
        return False, None

    def update_position(self, position):
        self.store_position(position)

    def _convert_position_to_log(self, position: Position, closed_price=None, amount=None, index=None):
        if closed_price is None:
            order_type = 1
            price = position.price
            amount = position.amount
            index = position.index
        else:
            order_type = -1
            price = closed_price
            amount = amount
            index = index
        log_item = {}
        log_item["provider"] = self.provider
        log_item["username"] = self.username
        log_item["symbol"] = position.symbol
        log_item["time"] = index
        log_item["price"] = price
        log_item["amount"] = amount
        log_item["position_type"] = position.position_type.value
        log_item["order_type"] = order_type
        log_item["logged_at"] = datetime.datetime.now(datetime.UTC).isoformat()
        return log_item

    def _store_log(self, symbol, time_index, price, amount, position_type, is_open):
        pass

    def close(self):
        pass

    def _create_place_holder(self, num: int):
        place_holders = f"({'?,' * (num -1)}?)"
        return place_holders

    def _create_basic_query(self, table_schema_keys: list):
        keys = ",".join(table_schema_keys)
        place_holders = self._create_place_holder(len(table_schema_keys))
        return keys, place_holders


class FileStorage(BaseStorage):
    __position_lock = threading.Lock()
    __symbol_loc = threading.Lock()

    def __init__(
        self,
        provider: str,
        username: str,
        positions_path: str = None,
        rating_log_path: str = None,
        trade_log_db: LogStorageBase = None,
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
        if trade_log_db is None:
            self._trade_log_db = LogCSVStorage()
        else:
            self._trade_log_db = trade_log_db
        self._load_positions()

        self.__update_required = False
        self.save_period = save_period * 60.0
        self.__running = True
        self.__log_queue = []
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
        except Exception:
            logger.exception("failed to load")

    def __update_positions_file(self):
        new_positions = self._positions.copy()
        with self.__position_lock:
            _positions = self._load_json(self.positions_path)
        if _positions is None:
            _positions = {}

        # since we have loaded existing position in init
        provider_positions = {
            POSITION_TYPE.long.name: [position.to_dict() for position in new_positions[POSITION_TYPE.long].values()],
            POSITION_TYPE.short.name: [position.to_dict() for position in new_positions[POSITION_TYPE.short].values()],
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

    def _store_log(self, position: Position, is_open):
        if self.__immediate_save is True:
            self._trade_log_db.store_log(self.provider, position, is_open)
        else:
            log_item = self._trade_log_db._convert_position_to_log(self.provider, position, is_open)
            self.__log_queue.append(log_item)
            self.__update_required = True

    def __update_trade_log(self):
        log_items = self.__log_queue.copy()
        self.__log_queue = []
        self._trade_log_db.store_logs(log_items)

    def _prerodical_update(self):
        while self.__running:
            time.sleep(self.save_period)
            if self.__update_required is True:
                self.__update_positions_file()
                self.__update_trade_log()
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

    def get_trade_logs(self):
        return self._trade_log_db.get()

    def delete_position(self, id, price, amount, index):
        suc, p = super().delete_position(id)
        log_item = self._convert_position_to_log(p, price, amount, index)
        self.__log_queue.append(log_item)
        if self.__immediate_save is True:
            self.__update_positions_file()
            self.__update_trade_log()
        self.__update_required = True
        return suc

    def store_position(self, position: Position):
        super().store_position(position)
        self.__log_queue.append(self._convert_position_to_log(position))
        if self.__immediate_save is True:
            self.__update_positions_file()
            self.__update_trade_log()
        self.__update_required = True

    def store_positions(self, positions: List[Position]):
        super().store_positions(positions)
        self.__log_queue.extend([self._convert_position_to_log(position) for position in positions])
        t = threading.Thread(target=self.__update_positions_file)
        t.start()
        t = threading.Thread(target=self.__update_trade_log)
        t.start()

    def store_symbol_info(self, symbol, rating=None, date=None, source=None, market=None):
        if date is not None and isinstance(date, datetime.datetime):
            date = date.isoformat()
        symbol_info = [symbol, rating, date, source, market]
        self.__update_rating_file([symbol_info])

    def store_symbols_info(self, symbols_info_list: List[List]):
        self.__update_rating_file(symbols_info_list)

    def update_position(self, position):
        super().store_position(position)
        if self.__immediate_save is True:
            self.__update_positions_file()
            # save log file separately as position doesn't have closed price
        self.__update_required = True

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
                long_position_list = _position[POSITION_TYPE.long.name]
                short_position_list = _position[POSITION_TYPE.short.name]

                for _position in long_position_list:
                    position = Position(**_position)
                    self._positions[POSITION_TYPE.long][position.id] = position

                for _position in short_position_list:
                    position = Position(**_position)
                    self._positions[POSITION_TYPE.short][position.id] = position

    def close(self):
        self.__running = False
        self.__update_positions_file()
        self.__update_trade_log()


class SQLiteStorage(BaseStorage):
    POSITION_TABLE_NAME = "position"
    _POSITION_TABLE_KEYS = {
        "id": "TEXT PRIMARY KEY",
        "provider": "TEXT",
        "username": "TEXT",
        "symbol": "TEXT",
        "position_type": "INTEGER",
        "price": "REAL",
        "tp": "REAL",
        "sl": "REAL",
        "time_index": "TEXT",
        "amount": "REAL",
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

    def __init__(self, database_path, provider: str, username: str, log_storage=None) -> None:
        super().__init__(provider, username)
        if username is None:
            self.username = "__none__"

        self.__database_path = database_path
        if log_storage is None:
            self.log_storage = LogCSVStorage()
        else:
            self.log_storage = log_storage
        self._table_init()
    
    def __commit(self, query, params=()):
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


    def __fetch(self, query, params=()):
        with self.__lock:
            conn = sqlite3.connect(self.__database_path)
            cursor = conn.cursor()
            cursor.execute(query, params)
            records = cursor.fetchall()
            cursor.close()
            conn.close()
        return records

    def __records_to_positions(self, records, keys):
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
        values = (
            position.id,
            self.provider,
            self.username,
            position.symbol,
            position.position_type.value,
            position.price,
            position.tp,
            position.sl,
            position.index,
            position.amount,
            position.timestamp,
            position.result,
            position.option,
        )
        query = f"INSERT INTO {self.POSITION_TABLE_NAME} ({keys}) VALUES {place_holders}"
        self.__commit(query, values)
        self.log_storage.store_log(self.provider, self.username, position, True)

    def store_positions(self, positions: List[Position]):
        keys, place_holders = self._create_basic_query(self._POSITION_TABLE_KEYS.keys())
        values = [
            (
                position.id,
                self.provider,
                self.username,
                position.symbol,
                position.position_type.value,
                position.price,
                position.tp,
                position.sl,
                position.index,
                position.amount,
                position.timestamp,
                position.result,
                position.option,
            )
            for position in positions
        ]
        query = f"INSERT INTO {self.POSITION_TABLE_NAME} ({keys}) VALUES {place_holders}"
        self.__multi_commit(query, values)
        self.log_storage.store_logs(positions, True)

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
                date = datetime.datetime.now(datetime.UTC).date()
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

    def update_position(self, position):
        keys, place_holders = self._create_basic_query(self._POSITION_TABLE_KEYS.keys())
        values = (
            position.id,
            self.provider,
            self.username,
            position.symbol,
            position.position_type.value,
            position.price,
            position.tp,
            position.sl,
            position.index,
            position.amount,
            position.timestamp,
            position.id,
            position.result,
            position.option,
        )
        query = f"UPDATE {self.POSITION_TABLE_NAME} ({keys}) VALUES {place_holders} WHERE id = ?"
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

    def get_positions(self, symbols: list = None, position_type: POSITION_TYPE = None):
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
        if position_type is not None:
            query = f"{query} AND position_type = ?"
            params.append(position_type.value)
        records = self.__fetch(query, params)
        positions = self.__records_to_positions(records, self._POSITION_TABLE_KEYS.keys())
        if len(positions) == 0:
            return [], []
        else:
            long_positions = []
            short_positions = []
            for position in positions:
                if position.position_type == POSITION_TYPE.long:
                    long_positions.append(position)
                else:
                    short_positions.append(position)
            return long_positions, short_positions

    def get_long_positions(self, symbols: list = None) -> list:
        long_positions, _ = self.get_positions(symbols, POSITION_TYPE.long)
        return long_positions

    def get_short_positions(self, symbols: list = None) -> list:
        _, short_positions = self.get_positions(symbols, POSITION_TYPE.short)
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

    def _convert_position_to_log(self, p: Position, is_open):
        if is_open is True:
            order_type = 1
        else:
            order_type = -1
        values = (
            self.provider,
            p.symbol,
            p.index,
            p.price,
            p.amount,
            p.position_type.value,
            order_type,
            datetime.datetime.now(datetime.UTC),
        )
        return values

    def _store_log(self, position: Position, is_open):
        self.log_storage.store_log(self.provider, position, is_open)

    def delete_position(self, id, price=None, amount=None, index=None):
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
            p.price = price
            p.amount = amount
            p.index = index
            self._store_log(p, False)
            return False
        except Exception:
            return False

class LogSQLiteStorage(BaseStorage):
    TRADE_TABLE_NAME = "trade"
    _TRADE_TABLE_KEYS = {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "provider": "TEXT",
        "username": "TEXT",
        "symbol": "TEXT",
        "time_index": "TEXT",
        "price": "REAL",
        "amount": "REAL",
        "position_type": "INT",
        "order_type": "INT",
        "logged_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    }

    def __init__(self, database_path, provider: str, username) -> None:
        super().__init__(provider=provider, username=username)
        self.provider = provider
        self.username = username
        if username is None:
            self.username = "__none__"

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
        cursor.close()
        cursor.close()

    def __commit(self, query, params=()):
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

    def store_log(self, position: Position, is_open):
        values = self._convert_position_to_log(position, is_open)
        keys, place_holders = self._create_basic_query(list(self._TRADE_TABLE_KEYS.keys())[1:])
        query = f"INSERT INTO {self.TRADE_TABLE_NAME} ({keys}) VALUES {place_holders}"
        self.__commit(query, values)

    def store_logs(self, positions: List[Position], is_open):
        log_values = [self._convert_position_to_log(p, is_open) for p in positions]
        keys, place_holders = self._create_basic_query(list(self._TRADE_TABLE_KEYS.keys())[1:])
        query = f"INSERT INTO {self.TRADE_TABLE_NAME} ({keys}) VALUES {place_holders}"
        self.__multi_commit(query, log_values)
