import json
import os
import threading
import time
from typing import List

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

    def get_position(self, id):
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

    def get_listening_positions(self):
        listening_positions = {}
        for positions in self._positions.values():
            for position in positions:
                if position.tp is not None or position.sl is not None:
                    listening_positions[position.id] = position
        return listening_positions

    def store_trade_log(self, log: list):
        pass

    def load_trade_logs(self):
        return []

    def close(self):
        pass


class FileStorage(BaseStorage):
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

    def __init__(self, provider: str, positions_path: str = None) -> None:
        super().__init__(provider)
        self.__position_lock = threading.Lock()
        self.__log_lock = threading.Lock()
        self.positions_path = self._check_path(positions_path, "positions.json")
        self._load_positions()
        self.__update_required = False
        self.save_span = 60.0
        threading.Thread(target=self._prerodical_update, daemon=True).start()

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
        provider_positions = {
            POSITION_TYPE.long.name: [position.to_dict() for position in new_positions[POSITION_TYPE.long].values()],
            POSITION_TYPE.short.name: [position.to_dict() for position in new_positions[POSITION_TYPE.short].values()],
        }
        _positions[self.provider] = provider_positions
        with self.__position_lock:
            self._save_json(_positions, self.positions_path)

    def _prerodical_update(self):
        while True:
            time.sleep(self.save_span)
            if self.__update_required is True:
                self.__update_positions_file()
                self.__update_required = False

    def store_position(self, position: Position):
        super().store_position(position)
        self.__update_required = True

    def store_positions(self, positions: List[Position]):
        super().store_positions(positions)
        t = threading.Thread(target=self.__update_positions_file)
        t.start()

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
                    long_positions[position.id] = position
                    if position.tp is not None or position.sl is not None:
                        listening_positions[position.id] = position

                for _position in short_position_list:
                    position = Position(**_position)
                    short_positions[position.id] = position
                    if position.tp is not None or position.sl is not None:
                        listening_positions[position.id] = position
        long_positions, short_positions, listening_positions

    def store_trade_log(self, log: list):
        pass

    def load_trade_logs(self):
        return []


class SQLiteStorage(BaseStorage):
    POSITION_TABLE = "position"

    def __table_init(self):
        cursor = self.__conn.cursor()
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.POSITION_TABLE} (
                    id TEXT PRIMARY KEY,
                    symbol_id TEXT,
                    position_type TEXT,
                    position_type INTEGER,
                    price REAL,
                    tp REAL,
                    sl REAL,
                    amount NUMERIC,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def __init__(self, database, agent_id: str) -> None:
        import sqlite3

        self.__conn = sqlite3.connect(database)
        self.__table_init()
        self.agent_id = agent_id

    def _signal_to_str(self, signal: Position, open_at):
        return (
            self.agent_id,
            signal.symbol,
            signal.id,
            signal.position_type,
            signal.position_state,
            signal.possibility,
            signal.dev,
            signal.order_price,
            signal.tp,
            signal.sl,
            signal.amount,
        )

    def close(self):
        self.__conn.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
