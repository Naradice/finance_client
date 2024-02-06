import datetime
import json
import uuid


class Position:
    def __init__(
        self, order_type: str, symbol: str, price: float, amount: float, tp: float, sl: float, index: int, option, result, id=None
    ):
        if id is None:
            self.id = str(uuid.uuid4())
        else:
            self.id = id
        self.order_type = order_type
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
        return f"(order_type:{self.order_type}, price:{self.price}, amount:{self.amount}, tp: {self.tp}, sl:{self.sl}, symbol:{self.symbol})"

    def __repr__(self):
        return self.__str__()

    def to_dict(self):
        return {
            "order_type": self.order_type,
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
