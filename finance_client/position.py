import uuid
import datetime

class Position:
    id = uuid.uuid4()
    
    def __init__(self, order_type:str, price:float, amount: float, option):
        self.order_type = order_type
        self.price = price
        self.amount = amount
        self.option = option
        self.timestamp = datetime.datetime.now()
        

class PositionManager:
    
    poitions = {}
    
    def __init__(self, budget):
        self.budget = budget
    
    def open_position(self, order_type:str, price:float, amount: float, option):
        if order_type:
            if order_type == "ask" or order_type == "buy" or order_type == "long":
                ## check if budget has enough amount
                ## if enough, add position
                self.positions[order_type] = Position(order_type=order_type, price=price, amount=amount, option=option)
                ## then reduce budget
                pass
            elif order_type == "bid" or order_type == "sell" or order_type == "short":
                ## check if budget has enough amount
                ## if enough, add position
                self.positions[order_type] = Position(order_type=order_type, price=price, amount=amount, option=option)
                ## then reduce budget
        else:
            raise Exception(f"order_type should be specified: {order_type}")
    
    def close_position(self, position, price: float, amount: float = None):
        pass