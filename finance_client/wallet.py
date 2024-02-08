import json
import os
from time import sleep

from finance_client import logger as lg
from finance_client.db import BaseStorage
from finance_client.position import POSITION_TYPE, Position


class Manager:
    def __init__(self, storage: BaseStorage, budget, provider="Default", logger=None):
        dir = os.path.dirname(__file__)
        try:
            with open(os.path.join(dir, "./settings.json"), "r") as f:
                settings = json.load(f)
        except Exception as e:
            print(f"fail to load settings file: {e}")
            raise e

        if logger is None:
            self.logger = lg
        else:
            self.logger = logger

        self.budget = budget

        providers = settings["providers"]
        if provider not in providers:
            error_txt = f"provider {provider} is not defined in settings.json. use default settings instead."
            self.logger.error(error_txt)
            SymbolInfo = providers[provider]
            self.provider = provider
        else:
            SymbolInfo = providers["Default"]
            self.provider = provider

        self.trade_unit = SymbolInfo["trade_unit"]
        self.listening_positions = storage.get_listening_positions()
        self.storage = storage
        self.logger.info(f"MarketManager is initialized with budget:{budget}, provider:{provider}")

    def open_position(
        self,
        position_type: POSITION_TYPE,
        symbol: str,
        price: float,
        amount: float,
        tp=None,
        sl=None,
        index=None,
        option=None,
        result=None,
    ):
        # Market buy without price is ordered during market is closed
        if price is None:
            position = Position(
                position_type=position_type,
                symbol=symbol,
                price=price,
                amount=amount,
                tp=tp,
                sl=sl,
                index=index,
                option=option,
                result=result,
            )
            self.storage.store_position(position)
            return position
        else:
            # check if budget has enough amount
            required_budget = self.trade_unit * amount * price
            # if enough, add position
            if required_budget <= self.budget:
                position = Position(
                    position_type=position_type,
                    symbol=symbol,
                    price=price,
                    amount=amount,
                    tp=tp,
                    sl=sl,
                    index=index,
                    option=option,
                    result=result,
                )
                self.storage.store_position(position)
                # then reduce budget
                self.budget -= required_budget
                # check if tp/sl exists
                if tp is not None or sl is not None:
                    self.listening_positions[position.id] = position
                    self.logger.debug("position is stored to listening list")
                return position
            else:
                self.logger.info(f"current budget {self.budget} is less than required {required_budget}")
                return None

    def close_position(self, position: Position, price: float, amount: float = None):
        if price is None or position is None:
            return None, None, None, None
        if type(position) == Position and type(position.amount) == float or type(position.amount) == int:
            if amount is None:
                amount = position.amount
            if position.amount < amount:
                self.logger.info(f"specified amount is greater than position. use position value. {position.amount} < {amount}")
                amount = position.amount
            if position.position_type == POSITION_TYPE.long:
                price_diff = price - position.price
            elif position.position_type == POSITION_TYPE.short:
                price_diff = position.price - price

            profit = self.trade_unit * amount * price_diff
            return_budget = self.trade_unit * amount * position.price + profit
            # close position
            if position.id in self.positions[position.position_type]:
                if position.amount == amount:
                    self.positions[position.position_type].pop(position.id)
                else:
                    position.amount = position.amount - amount
                    self.positions[position.position_type][position.id] = position
                self.logger.info(f"closed result:: profit {profit}, return_budget: {return_budget}")
                self.budget += return_budget
                return price, position.price, price_diff, profit
            else:
                self.logger.info("info: position is already removed.")

            # remove position from listening
            self.remove_position_from_listening(position)
        else:
            self.logger.warning(f"position amount is invalid: {position.amount}")

    def check_order_in_tick(ask: float, bid: float):
        pass

    def remove_position_from_listening(self, position):
        if position.id in self.listening_positions:
            self.listening_positions.pop(position.id)
