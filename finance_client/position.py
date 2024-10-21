import datetime
import json
import uuid
from enum import Enum


class POSITION_TYPE(Enum):
    long = 1
    short = -1


class ORDER_TYPE(Enum):
    market = 0


def _value_to_position_type(value: int):
    if isinstance(value, type(POSITION_TYPE.long)):
        return value
    elif isinstance(value, int):
        return POSITION_TYPE(value)
    elif isinstance(value, str):
        for member in POSITION_TYPE:
            if member.name == value:
                return member
        if "ask" == value:
            return POSITION_TYPE.long
        if "bid" == value:
            return POSITION_TYPE.short
    raise ValueError(f"{value} is not supported as POSITION_TYPE")


class Position:
    def __init__(
        self,
        position_type: POSITION_TYPE,
        symbol: str,
        price: float,
        amount: float,
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
        self.position_type = _value_to_position_type(position_type)
        self.price = price
        self.amount = amount
        self.option = option
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
            self.timestamp = datetime.datetime.now(datetime.UTC)
        else:
            if isinstance(timestamp, datetime.datetime):
                self.timestamp = timestamp
            else:
                try:
                    self.timestamp = datetime.datetime.fromisoformat(timestamp)
                except Exception:
                    self.timestamp = timestamp

    def __str__(self):
        return f"(position_type:{self.position_type}, price:{self.price}, amount:{self.amount}, tp: {self.tp}, sl:{self.sl}, symbol:{self.symbol})"

    def __repr__(self):
        return self.__str__()

    def to_dict(self):
        if isinstance(self.index, datetime.datetime):
            index = self.index.isoformat()
        else:
            index = str(self.index)
        return {
            "position_type": self.position_type.value,
            "price": self.price,
            "amount": self.amount,
            "option": json.dumps(self.option),
            "result": json.dumps(self.result),
            "symbol": self.symbol,
            "tp": self.tp,
            "sl": self.sl,
            "time_index": index,
            "timestamp": self.timestamp.isoformat(),
            "id": self.id,
        }
