import datetime
import json
import uuid
from enum import Enum


class POSITION_TYPE(Enum):
    long = 1
    short = -1


class ORDER_TYPE(Enum):
    market = 0


def __value_to_position_type(value: int):
    if isinstance(value, POSITION_TYPE.long):
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
        index: int,
        option,
        result,
        id=None,
    ):
        if id is None:
            self.id = str(uuid.uuid4())
        else:
            self.id = id
        self.position_type = __value_to_position_type(position_type)
        self.price = price
        self.amount = amount
        self.option = option
        if option == "null":
            self.option = None
        self.result = result
        if result == "null":
            self.result = None
        self.symbol = symbol
        self.tp = tp
        self.index = index
        if tp == "null":
            self.tp = None
        self.sl = sl
        if sl == "null":
            self.sl = None
        self.timestamp = datetime.datetime.utcnow()

    def __str__(self):
        return f"(position_type:{self.position_type}, price:{self.price}, amount:{self.amount}, tp: {self.tp}, sl:{self.sl}, symbol:{self.symbol})"

    def __repr__(self):
        return self.__str__()

    def to_dict(self):
        return {
            "position_type": self.position_type,
            "price": self.price,
            "amount": self.amount,
            "option": json.dumps(self.option),
            "result": json.dumps(self.result),
            "symbol": self.symbol,
            "tp": self.tp,
            "sl": self.sl,
            "timestamp": self.timestamp.isoformat(),
            "id": self.id,
        }
