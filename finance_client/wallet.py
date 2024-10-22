import json
import os

from finance_client import logger as lg
from finance_client.db import BaseStorage, FileStorage
from finance_client.position import POSITION_TYPE, Position


class Manager:
    def __init__(self, budget, storage: BaseStorage = None, provider="Default", logger=None):
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
            SymbolInfo = providers["Default"]
            self.provider = provider
        else:
            SymbolInfo = providers[provider]
            self.provider = provider

        self.trade_unit = SymbolInfo["trade_unit"]
        if storage is None:
            storage = FileStorage(provider, save_period=0)
        self.listening_positions = storage._get_listening_positions()
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
                time_index=index,
                tp=tp,
                sl=sl,
                option=option,
                result=result,
            )
            self.storage.store_position(position)
            return position.id
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
                    time_index=index,
                    tp=tp,
                    sl=sl,
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
                return position.id
            else:
                self.logger.info(f"current budget {self.budget} is less than required {required_budget}")
                return None

    def close_position(self, id, price: float, amount: float = None, position=None, index=None):
        """close a position based on the id generated when the positions is opened.

        Args:
            id (uuid): positions id of finance_client
            price (float): price to close
            amount (float, optional): amount to close the position. Defaults to None and close all of the amount the position has

        Returns:
            float, float, float, float: closed_price, position_price, price_diff, profit
            (profit = price_diff * amount * trade_unit(pips etc))
        """
        if price is None or id is None:
            self.logger.error(f"either id or price is None: {id}, {price}")
            return None, None, None, None
        if position is None:
            position = self.storage.get_position(id)
        if position is not None:
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
            if position.amount == amount:
                self.storage.delete_position(id, price, amount, index)
            else:
                position.amount -= amount
                self.storage.update_position(position)
                self.storage._store_log(
                    Position(position.position_type, position.symbol, price, position.amount, position.tp, position.sl, index),
                    False,
                )
            self.logger.info(f"closed result:: profit {profit}, budget: {return_budget}")
            self.budget += return_budget
            self.remove_position_from_listening(id)
            return price, position.price, price_diff, profit
        else:
            self.remove_position_from_listening(id)
            self.logger.error("position id is not found")
            return None, None, None, None

    def remove_position_from_listening(self, id):
        if id in self.listening_positions:
            self.listening_positions.pop(id)
