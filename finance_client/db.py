import datetime
import json
import os
import threading
import time
from typing import List

import pandas as pd

from finance_client import logger
from finance_client.position import POSITION_TYPE, Position


class BaseStorage:
    def __init__(self, provider: str) -> None:
        self.provider = provider
        self._positions = {POSITION_TYPE.long: {}, POSITION_TYPE.short: {}}

    def store_position(self, position: Position):
        self._positions[position.position_type][position.id] = position

    def store_positions(self, positions: List[Position]):
        for position in positions:
            self.store_position(position)

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

    def _get_listening_positions(self):
        listening_positions = {}
        for positions in self._positions.values():
            for position in positions.values():
                if position.tp is not None or position.sl is not None:
                    listening_positions[position.id] = position
        return listening_positions

    def delete_position(self, id, price=None, amount=None):
        if id in self._positions[POSITION_TYPE.long]:
            position = self._positions[POSITION_TYPE.long].pop(id)
            return True, position
        elif id in self._positions[POSITION_TYPE.short]:
            position = self._positions[POSITION_TYPE.short].pop(id)
            return True, position
        return False, None

    def update_position(self, position):
        self.store_position(position)

    def _convert_position_to_log(self, position: Position, closed_price=None, amount=None):
        if closed_price is None:
            order_type = 1
            price = position.price
            amount = position.amount
        else:
            order_type = -1
            price = closed_price
            amount = amount
        log_item = {}
        log_item["provider"] = self.provider
        log_item["symbol"] = position.symbol
        log_item["time"] = position.index
        log_item["price"] = price
        log_item["amount"] = amount
        log_item["position_type"] = position.position_type.value
        log_item["order_type"] = order_type
        log_item["logged_at"] = datetime.datetime.utcnow().isoformat()
        return log_item

    def _store_log(self, symbol, time_index, price, amount, position_type, is_open):
        pass

    def load_trade_logs(self):
        return []

    def close(self):
        pass


class FileStorage(BaseStorage):
    __position_lock = threading.Lock()
    __log_lock = threading.Lock()

    def _check_path(self, file_path, default_file_name: str):
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

    def __init__(self, provider: str, positions_path: str = None, trade_log_path: str = None, save_period: float = 0) -> None:
        """File Handler to store objects. This class doesn't care a bout accesses from multiple instances.

        Args:
            provider (str): provider id to separate position information
            positions_path (str, optional): custom position file path. Defaults to None.
            save_period (float, optional): minutes to periodically save positions. less than 0 save them immedeately when it is updated. Defaults to 1.0.
        """
        super().__init__(provider)
        self.positions_path = self._check_path(positions_path, "positions.json")
        self.trade_log_path = self._check_path(trade_log_path, "finance_trade_log.csv")
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

    def __update_log_file(self):
        with self.__log_lock:
            logs = self.__log_queue.copy()
            self.__log_queue = []

            df = pd.DataFrame.from_dict(logs)
            save_header = not os.path.exists(self.trade_log_path)
            df.to_csv(self.trade_log_path, mode="a", header=save_header, index_label=None, index=False)

    def _store_log(self, symbol, time_index, price, amount, position_type, is_open):
        log_item = {}
        log_item["provider"] = self.provider
        log_item["symbol"] = symbol
        log_item["time"] = time_index
        log_item["price"] = price
        log_item["amount"] = amount
        log_item["position_type"] = position_type.value
        if is_open is True:
            log_item["order_type"] = 1
        else:
            log_item["order_type"] = -1
        log_item["logged_at"] = datetime.datetime.utcnow().isoformat()
        if self.__immediate_save is True:
            df = pd.DataFrame.from_dict([log_item])
            save_header = not os.path.exists(self.trade_log_path)
            df.to_csv(self.trade_log_path, mode="a", header=save_header, index_label=None, index=False)
        else:
            self.__log_queue.append(log_item)
            self.__update_required = True

    def _prerodical_update(self):
        while self.__running:
            time.sleep(self.save_period)
            if self.__update_required is True:
                self.__update_positions_file()
                self.__update_log_file()
                self.__update_required = False

    def delete_position(self, id, price, amount):
        suc, p = super().delete_position(id)
        log_item = self._convert_position_to_log(p, price, amount)
        self.__log_queue.append(log_item)
        if self.__immediate_save is True:
            self.__update_positions_file()
            self.__update_log_file()
        self.__update_required = True
        return suc

    def store_position(self, position: Position):
        super().store_position(position)
        self.__log_queue.append(self._convert_position_to_log(position))
        if self.__immediate_save is True:
            self.__update_positions_file()
            self.__update_log_file()
        self.__update_required = True

    def store_positions(self, positions: List[Position]):
        super().store_positions(positions)
        self.__log_queue.extend([self._convert_position_to_log(position) for position in positions])
        t = threading.Thread(target=self.__update_positions_file)
        t = threading.Thread(target=self.__update_log_file)
        t.start()

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
            with self.__log_lock:
                positions_dict = self._load_json(self.positions_path)
            if positions_dict is None:
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

    def load_trade_logs(self):
        df = pd.read_csv(self.trade_log_path)
        provider_logs = df[df["provider"] == self.provider]
        return provider_logs

    def close(self):
        self.__running = False
        self.__update_positions_file()


class SQLiteStorage(BaseStorage):
    POSITION_TABLE_NAME = "position"
    _POSITION_TABLE_KEYS = {
        "id": "TEXT PRIMARY KEY",
        "provider": "TEXT",
        "symbol": "TEXT",
        "position_type": "INTEGER",
        "price": "REAL",
        "tp": "REAL",
        "sl": "REAL",
        "time_index": "TEXT",
        "amount": "REAL",
        "timestamp": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    }
    TRADE_TABLE_NAME = "trade"
    _TRADE_TABLE_KEYS = {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "provider": "TEXT",
        "symbol": "TEXT",
        "time_index": "TEXT",
        "price": "REAL",
        "amount": "REAL",
        "position_type": "INT",
        "order_type": "INT",
        "logged_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    }
    __lock = threading.Lock()

    def _table_init(self):
        cursor = self.__conn.cursor()
        table_schema = ",".join([f"{key} {attr}" for key, attr in self._POSITION_TABLE_KEYS.items()])
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.POSITION_TABLE_NAME} (
                {table_schema}
            )
            """
        )
        table_schema = ",".join([f"{key} {attr}" for key, attr in self._TRADE_TABLE_KEYS.items()])
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.TRADE_TABLE_NAME} (
                {table_schema}
            )
            """
        )
        cursor.close()

    def __init__(self, database_path, provider: str) -> None:
        super().__init__(provider)
        import sqlite3

        self.__conn = sqlite3.connect(database_path)
        self._table_init()

    def __create_place_holder(self, num: int):
        place_holders = f"({'?,' * (num -1)}?)"
        return place_holders

    def __create_basic_query(self, table_schema_keys: list):
        keys = ",".join(table_schema_keys)
        place_holders = self.__create_place_holder(len(table_schema_keys))
        return keys, place_holders

    def __commit(self, query, params=()):
        with self.__lock:
            cursor = self.__conn.cursor()
            cursor.execute(query, params)
            self.__conn.commit()
            cursor.close()

    def __multi_commit(self, query, params_list):
        with self.__lock:
            cursor = self.__conn.cursor()
            cursor.executemany(query, params_list)
            self.__conn.commit()
            cursor.close()

    def __fetch(self, query, params=()):
        with self.__lock:
            cursor = self.__conn.cursor()
            cursor.execute(query, params)
            records = cursor.fetchall()
            cursor.close()
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
        keys, place_holders = self.__create_basic_query(self._POSITION_TABLE_KEYS.keys())
        values = (
            position.id,
            self.provider,
            position.symbol,
            position.position_type.value,
            position.price,
            position.tp,
            position.sl,
            position.index,
            position.amount,
            position.timestamp,
        )
        query = f"INSERT INTO {self.POSITION_TABLE_NAME} ({keys}) VALUES {place_holders}"
        self.__commit(query, values)
        self.__store_log(position, True)

    def store_positions(self, positions: List[Position]):
        keys, place_holders = self.__create_basic_query(self._POSITION_TABLE_KEYS.keys())
        values = [
            (
                position.id,
                self.provider,
                position.symbol,
                position.position_type.value,
                position.price,
                position.tp,
                position.sl,
                position.index,
                position.amount,
                position.timestamp,
            )
            for position in positions
        ]
        query = f"INSERT INTO {self.POSITION_TABLE_NAME} ({keys}) VALUES {place_holders}"
        self.__multi_commit(query, values)
        self.__store_logs(positions, True)

    def update_position(self, position):
        keys, place_holders = self.__create_basic_query(self._POSITION_TABLE_KEYS.keys())
        values = (
            position.id,
            self.provider,
            position.symbol,
            position.position_type.value,
            position.price,
            position.tp,
            position.sl,
            position.index,
            position.amount,
            position.timestamp,
            position.id,
        )
        query = f"UPDATE {self.POSITION_TABLE_NAME} ({keys}) VALUES {place_holders} WHERE id = ?"
        self.__commit(query, values)

    def has_position(self, id) -> bool:
        return super().has_position(id)

    def get_position(self, id) -> Position:
        query = f"SELECT * FROM {self.POSITION_TABLE_NAME} WHERE id = ? AND provider = ?"
        records = self.__fetch(query, (id, self.provider))
        positions = self.__records_to_positions(records, self._POSITION_TABLE_KEYS.keys())
        if len(positions) == 0:
            logger.info(f"no record found for position_id: {id}")
            return None
        else:
            return positions[0]

    def get_positions(self, symbols: list = None, position_type: POSITION_TYPE = None):
        query = f"SELECT * FROM {self.POSITION_TABLE_NAME} WHERE provider = ?"
        params = [self.provider]
        if symbols is not None and len(symbols) > 0:
            place_holders = self.__create_place_holder(len(symbols))
            query = f"{query} AND symbol in {place_holders}"
            params.append(symbols)
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

    def _get_listening_positions(self):
        query = f"SELECT * FROM {self.POSITION_TABLE_NAME} WHERE provider = ? AND (tp IS NOT NULL OR sl IS NOT NULL)"
        records = self.__fetch(query, (self.provider,))
        positions = self.__records_to_positions(records, self._POSITION_TABLE_KEYS.keys())
        return positions

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
            datetime.datetime.utcnow(),
        )
        return values

    def _store_log(self, symbol, time_index, price, amount, position_type, is_open):
        if is_open is True:
            order_type = 1
        else:
            order_type = -1
        keys, place_holders = self.__create_basic_query(list(self._TRADE_TABLE_KEYS.keys())[1:])
        values = (self.provider, symbol, time_index, price, amount, position_type.value, order_type, datetime.datetime.utcnow())
        query = f"INSERT INTO {self.TRADE_TABLE_NAME} ({keys}) VALUES {place_holders}"
        self.__commit(query, values)

    def __store_log(self, position: Position, is_open):
        values = self._convert_position_to_log(position, is_open)
        keys, place_holders = self.__create_basic_query(list(self._TRADE_TABLE_KEYS.keys())[1:])
        query = f"INSERT INTO {self.TRADE_TABLE_NAME} ({keys}) VALUES {place_holders}"
        self.__commit(query, values)

    def __store_logs(self, positions: List[Position], is_open):
        log_values = [self._convert_position_to_log(p, is_open) for p in positions]
        keys, place_holders = self.__create_basic_query(list(self._TRADE_TABLE_KEYS.keys())[1:])
        query = f"INSERT INTO {self.TRADE_TABLE_NAME} ({keys}) VALUES {place_holders}"
        self.__multi_commit(query, log_values)

    def delete_position(self, id, price=None, amount=None):
        p = self.get_position(id)
        query = f"DELETE FROM {self.POSITION_TABLE_NAME}"
        cond = "WHERE id = ? AND provider = ?"
        query = f"{query} {cond}"
        try:
            self.__commit(query, (id, self.provider))
            self._store_log(p.symbol, None, price, amount, p.position_type, False)
            return False
        except Exception:
            return False

    def close(self):
        self.__conn.close()
