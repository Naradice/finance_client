import datetime
import json
import os
from typing import List

from finance_client.position import Position


class BaseConnector:
    def __init__(self, provider: str) -> None:
        self.provider = provider

    def store_positions(self, positions: List[Position]):
        pass

    def load_positions(self):
        return {}, {}, {}

    def store_trade_log(self, log: list):
        pass

    def load_trade_logs(self):
        return []

    def close(self):
        pass


class FileConnector(BaseConnector):
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
        self.positions_path = self._check_path(positions_path, "positions.json")

    def _save_json(self, obj, file_path):
        try:
            with open(file_path, mode="w") as fp:
                json.dump(obj, fp)
        except Exception as e:
            print(f"failed to save due to {e}")

    def _load_json(self, file_path):
        try:
            with open(file_path, mode="r") as fp:
                return json.load(fp)
        except Exception as e:
            print(f"failed to load due to {e}")

    def store_positions(self, long_positions: List[Position], short_positions: List[Position]):
        _positions = self._load_json(self.positions_path)
        if _positions is None:
            _positions = {}

        provider_positions = {
            "long": [position.to_dict() for position in long_positions],
            "short": [position.to_dict() for position in short_positions],
        }

        _positions[self.provider] = provider_positions
        self._save_json(_positions)

    def load_positions(self):
        """

        Returns:
            Tuple(Dict[id:Position], Dict[id:Position], Dict[id:Position]): return long, short, listening tp/sl positions
        """
        long_positions = {}
        short_positions = {}
        listening_positions = {}
        if os.path.exists(self.positions_path):
            positions_dict = self._load_json(self.positions_path)
            if positions_dict is None:
                return long_positions, short_positions, listening_positions
            if self.provider in positions_dict:
                _position = positions_dict[self.provider]
                long_position_list = _position["long"]
                short_position_list = _position["short"]

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
            else:
                return long_positions, short_positions, listening_positions
        else:
            return long_positions, short_positions, listening_positions

    def store_trade_log(self, log: list):
        pass

    def load_trade_logs(self):
        return []


class SQLiteConnector(BaseConnector):
    POSITION_TABLE = "position"

    def __table_init(self):
        cursor = self.__conn.cursor()
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.POSITION_TABLE} (
                    id TEXT PRIMARY KEY,
                    symbol_id TEXT,
                    order_type TEXT,
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
            signal.order_type,
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
