import uuid
import datetime
import json
import os

class Position:
    id = uuid.uuid4()
    
    def __init__(self, order_type:str, symbol:str, price:float, amount: float, option):
        self.order_type = order_type
        self.price = price
        self.amount = amount
        self.option = option
        self.symbol = symbol
        self.timestamp = datetime.datetime.now()
    

class Manager:
    
    poitions = {
        "ask": {},
        "bid": {}
    }
    
    def __init__(self, budget, provider="Default"):
        self.budget = budget
        self.__start_budget = budget
        dir = os.path.dirname(__file__)
        try:
            with open(os.path.join(dir, "settings.json"), "r", encoding="utf-8") as f:
                contents = json.load(f)
        except Exception as e:
            print(f"couldn't load symbol file. {e}")
        
        if provider in contents:
            SymbolInfo = contents[provider]
        else:
            raise Exception(f"provider {provider} is not defined in settings.json")
        
        self.trade_unit = SymbolInfo["trade_unit"]
    
    def __check_order_type(order_type:str):
        if order_type and type(order_type) == str:
            order_type = str.lower(order_type)
            #if order_type == "ask" or order_type == "buy" or order_type == "long" or order_type == "bid" or order_type == "sell" or order_type == "short":
            if order_type == "ask" or order_type == "bid":
                return order_type
            else:
                raise Exception("unkown order_type: {order_type}")
        else:
            raise Exception(f"order_type should be specified: {order_type}")
    
    def open_position(self, order_type:str, symbol:str, price:float, amount: float, option):
        order_type = self.__check_order_type(order_type)
        ## check if budget has enough amount
        required_budget = self.trade_unit * amount * price
        ## if enough, add position
        if required_budget <= self.budget:
            position = Position(order_type=order_type, symbol=symbol, price=price, amount=amount, option=option)
            self.positions[order_type][position.id] = position
            ## then reduce budget
            self.budget = self.budget - required_budget
            return position
        else:
            print(f"current budget {self.budget} is less than required {required_budget}")
            
    def get_position(self, id):
        if id in self.poitions["ask"]:
            return self.poitions["ask"][id]
        elif id in self.positions["bid"]:
            return self.positions["bid"][id]
        return None
            
    
    def get_open_positions(self, order_type:str) -> list:
        order_type = self.__check_order_type()
        positions = []
        for key, position_type in self.poitions.items():
            for key, position in position_type.items():
                positions.append(position)
        return positions
    
    def close_position(self, position:Position, price: float, amount: float = None):
        if position.amount <= amount:
            if position.order_type == "ask":
                price_diff = (price - position.price)
            elif position.order_type == "bid":
                price_diff = position.price - price
            
            profit = self.trade_unit * amount * price_diff
            self.budget += profit
            
            if position.amount == amount:
                self.poitions[position.order_type].pop(position.id)
            else:
                position.amount = position.amount - amount
                self.poitions[position.order_type][position.id] = position
            return price, position.price, price_diff, profit
                
    def check_order_in_tick(ask:float, bid:float):
        pass