import datetime
import json
import uuid
from enum import Enum


class POSITION_SIDE(Enum):
    long = 1
    short = -1


class ORDER_TYPE(Enum):
    market = 0
    limit = 1
    stop = 2


def _value_to_position_side(value: int):
    if isinstance(value, type(POSITION_SIDE.long)):
        return value
    elif isinstance(value, int):
        return POSITION_SIDE(value)
    elif isinstance(value, str):
        for member in POSITION_SIDE:
            if member.name == value:
                return member
        if "ask" == value:
            return POSITION_SIDE.long
        if "bid" == value:
            return POSITION_SIDE.short
    raise ValueError(f"{value} is not supported as POSITION_SIDE")


class Position:
    def __init__(
        self,
        position_side: POSITION_SIDE,
        symbol: str,
        trade_unit: int,
        leverage: float,
        price: float,
        volume: float,
        tp: float,
        sl: float,
        time_index=None,
        option=None,
        result=None,
        id=None,
        timestamp=None,
        **kwargs,
    ):
        if id is None:
            self.id = str(uuid.uuid4())
        else:
            self.id = id
        self.position_side = _value_to_position_side(position_side)
        self.price = price
        self.volume = volume
        self.option = option
        self.trade_unit = trade_unit
        self.leverage = leverage
        if option == "null":
            self.option = None
        if result == "null" or result is None:
            self.result = None
        else:
            if "provider" in kwargs:
                provider = kwargs["provider"]
                if isinstance(provider, str):
                    provider = provider.lower()
                    if "mt5" in provider:
                        self.result = int(result)
                    else:
                        try:
                            self.result = int(result)
                        except Exception:
                            self.result = result
            else:
                self.result = result
        self.symbol = symbol
        if time_index is None:
            self.index = time_index
        elif isinstance(time_index, datetime.datetime):
            self.index = time_index
        else:
            try:
                self.index = datetime.datetime.fromisoformat(time_index)
            except Exception:
                self.index = time_index

        self.tp = tp
        if tp == "null":
            self.tp = None
        self.sl = sl
        if sl == "null":
            self.sl = None
        if timestamp is None:
            self.timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        else:
            if isinstance(timestamp, datetime.datetime):
                self.timestamp = timestamp
            else:
                try:
                    self.timestamp = datetime.datetime.fromisoformat(timestamp)
                except Exception:
                    self.timestamp = timestamp

    def __str__(self):
        return f"(position_side:{self.position_side}, price:{self.price}, volume:{self.volume}, tp: {self.tp}, sl:{self.sl}, symbol:{self.symbol}, resilt: {self.result}, time_index:{self.index}, id:{self.id})"

    def __repr__(self):
        return self.__str__()

    def to_dict(self):
        if isinstance(self.index, datetime.datetime):
            index = self.index.isoformat()
        else:
            index = str(self.index)
        return {
            "position_side": self.position_side.value,
            "price": self.price,
            "volume": self.volume,
            "option": json.dumps(self.option),
            "result": json.dumps(self.result),
            "symbol": self.symbol,
            "tp": self.tp,
            "sl": self.sl,
            "trade_unit": self.trade_unit,
            "leverage": self.leverage,
            "time_index": index,
            "timestamp": self.timestamp.isoformat(),
            "id": self.id,
            "trade_unit": self.trade_unit,
        }


class Order:
    def __init__(
        self,
        order_type: ORDER_TYPE,
        position_side: POSITION_SIDE,
        symbol: str,
        price: float,
        volume: float,
        tp: float,
        sl: float,
        magic_number: int,
        id=None,
    ):
        if id is None:
            self.id = str(uuid.uuid4())
        else:
            self.id = id
        self.order_type = order_type
        self.position_side = _value_to_position_side(position_side)
        self.symbol = symbol
        self.price = price
        self.volume = volume
        self.tp = tp
        self.sl = sl
        self.created = datetime.datetime.now(tz=datetime.timezone.utc)

    def to_dict(self):
        return {
            "order_type": self.order_type.value,
            "position_side": self.position_side.value,
            "symbol": self.symbol,
            "price": self.price,
            "volume": self.volume,
            "tp": self.tp,
            "sl": self.sl,
            "id": self.id,
        }

    def __str__(self):
        return f"Order(id={self.id}, order_type={self.order_type}, position_side={self.position_side}, symbol={self.symbol}, price={self.price}, volume={self.volume}, tp={self.tp}, sl={self.sl})"


class ClosedResult:

    def __init__(self, id=None, price=0.0, entry_price=0.0, volume=0, price_diff=0.0, profit=0.0, msg="undefined error"):
        self.id = id
        self.price = price
        self.entry_price = entry_price
        self.volume = volume
        self.price_diff = price_diff
        self.profit = profit
        self.msg = msg
        self.error = True

    def update(self, id=None, price=None, entry_price=None, price_diff=None, volume=None, profit=None, msg=None):
        if id is not None:
            self.id = id
        if price is not None:
            self.price = price
        if entry_price is not None:
            self.entry_price = entry_price
        if price_diff is not None:
            self.price_diff = price_diff
        if profit is not None:
            self.profit = profit
        if msg is not None:
            self.msg = msg
        if volume is not None:
            self.volume = volume

    def to_dict(self):
        return {
            "id": self.id,
            "price": self.price,
            "entry_price": self.entry_price,
            "price_diff": self.price_diff,
            "profit": self.profit,
            "msg": self.msg,
        }

    def __str__(self):
        return f"ClosedResult(id={self.id}, price={self.price}, entry_price={self.entry_price}, volume={self.volume}, price_diff={self.price_diff}, profit={self.profit}, msg={self.msg})"
