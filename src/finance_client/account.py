import datetime
import logging
from typing import List, Tuple

import pandas as pd

from finance_client.config.model import AccountRiskConfig
from finance_client.db import LogCSVStorage, LogStorageBase, PositionFileStorage, PositionStorageBase
from finance_client.position import POSITION_SIDE, ClosedResult, Position

logger = logging.getLogger(__name__)


class Manager:
    def __init__(
        self,
        budget,
        position_storage: PositionStorageBase = None,
        log_storage: LogStorageBase = None,
        account_risk_config: AccountRiskConfig = None,
        tz_info=None,
        provider="Default",
    ):
        """

        Args:
            budget (float): initial budget for trading
            position_storage (PositionStorageBase, optional): storage backend for positions. Defaults to None.
            log_storage (LogStorageBase, optional): storage backend for logs. Defaults to None.
            account_risk_config (AccountRiskConfig, optional): risk configuration for the account. Defaults to None.
            tz_info (datetime.timezone, optional): timezone information. Defaults to None, which means UTC.
            provider (str, optional): provider name. Defaults to "Default".
        """
        self.budget = budget
        if position_storage is None:
            position_storage = PositionFileStorage(provider, None, save_period=0)
        # initialize positions which have tp or sl
        self.listening_positions = position_storage._get_listening_positions()
        self.storage = position_storage
        if log_storage is None:
            self._trade_log_db = LogCSVStorage(provider, username=position_storage.username)
        else:
            self._trade_log_db = log_storage

        self.__account_risk_config = account_risk_config
        self.update_daily_max_loss()
        if tz_info is None:
            tz_info = datetime.timezone.utc
        self.tz_info = tz_info
        logger.info(f"MarketManager is initialized with budget:{budget}, provider:{provider}, tz_info:{tz_info}")

    @property
    def risk_config(self):
        if self.__account_risk_config is not None:
            return self.__account_risk_config
        else:
            return None

    @risk_config.setter
    def risk_config(self, config: AccountRiskConfig):
        if config.daily_max_loss_percent is not None:
            if config.daily_max_loss_percent < 0:
                raise ValueError("daily_max_loss_percent cannot be negative")
            elif config.daily_max_loss_percent > 100:
                raise ValueError("daily_max_loss_percent cannot be greater than 100")
            elif config.daily_max_loss_percent == 0:
                logger.warning("daily_max_loss_percent is set to 0, which means no daily loss limit")
            elif config.daily_max_loss_percent < 1:
                daily_max_loss_percent = config.daily_max_loss_percent * 100.0
                logger.info(f"daily_max_loss_percent is set to {config.daily_max_loss_percent}, which is converted to {daily_max_loss_percent}%")
                config.daily_max_loss_percent = daily_max_loss_percent
            self.__account_risk_config = config
            self.update_daily_max_loss()
        else:
            self.__account_risk_config = config

    def open_position(
        self,
        position_side: POSITION_SIDE,
        symbol: str,
        price: float,
        volume: float,
        trade_unit: float = 1.0,
        leverage: float = 1.0,
        tp=None,
        sl=None,
        index=None,
        option=None,
        result=None,
    ):
        # Market buy without price is ordered during market is closed
        if price is None:
            position = Position(
                position_side=position_side,
                symbol=symbol,
                price=price,
                volume=volume,
                trade_unit=trade_unit,
                leverage=leverage,
                time_index=index,
                tp=tp,
                sl=sl,
                option=option,
                result=result,
            )
            self.storage.store_position(position)
            self._trade_log_db.store_log(position, order_type=1)
            return position
        else:
            # check if budget has enough volume
            required_budget = (trade_unit * volume * price) / leverage
            # if enough, add position
            # if required_budget <= self.budget:
            position = Position(
                position_side=position_side,
                symbol=symbol,
                price=price,
                volume=volume,
                trade_unit=trade_unit,
                leverage=leverage,
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
                logger.debug("position is stored to listening list")
            self._trade_log_db.store_log(position, order_type=1)
            return position
            # else:
            #     logger.info(f"current budget {self.budget} is less than required {required_budget}")
            #     return None

    def update_position(self, position: Position, tp=None, sl=None):
        """update tp/sl of a position

        Args:
            position (Position): position to update
            tp (float, optional): new take profit value. Defaults to None.
            sl (float, optional): new stop loss value. Defaults to None.

        Returns:
            bool: True if update is successful
        """
        if position is None:
            logger.error("position is None")
            return False
        if tp is not None:
            position.tp = tp
        elif position.tp is not None:
            position.tp = None
        if sl is not None:
            position.sl = sl
        elif position.sl is not None:
            position.sl = None
        if self.storage.update_position(position) is False:
            logger.error(f"failed to update position with id {position.id}")
            return False
        self._trade_log_db.store_log(position, order_type=0)
        # check if tp/sl exists
        if position.tp is not None or position.sl is not None:
            self.listening_positions[position.id] = position
            logger.debug("position is stored to listening list")
        else:
            self.remove_position_from_listening(position.id)
        return True

    def close_position(self, id, price: float, volume: float = None, position=None, index=None):
        """close a position based on the id generated when the positions is opened.

        Args:
            id (uuid): positions id of finance_client
            price (float): price to close
            volume (float, optional): volume to close the position. Defaults to None and close all of the volume the position has
            position (Position, optional): position object to close. If not specified, it will be retrieved from storage based on the id. Defaults to None.
            index (int, optional): time index for logging. Defaults to None.

        Returns:
            float, float, float, float: closed_price, position_price, price_diff, profit
            (profit = price_diff * volume * trade_unit(pips etc))
        """
        closed_result = ClosedResult()
        if price is None or id is None:
            logger.error(f"either id or price is None: {id}, {price}")
            closed_result.msg = "either id or price is None"
            closed_result.error = True
            return closed_result
        if position is None:
            position = self.storage.get_position(id)
        elif id is None:
            id = position.id
        if position is not None:
            if volume is None:
                volume = position.volume
            if position.volume < volume:
                logger.info(f"specified volume is greater than position. use position value. {position.volume} < {volume}")
                volume = position.volume
            if position.position_side == POSITION_SIDE.long:
                price_diff = price - position.price
            elif position.position_side == POSITION_SIDE.short:
                price_diff = position.price - price
            trade_unit = position.trade_unit
            profit = trade_unit * volume * price_diff
            return_budget = (trade_unit * volume * position.price) / position.leverage + profit
            if position.volume == volume:
                self.storage.delete_position(id)
            else:
                position.volume -= volume
                self.storage.update_position(position)
            logger.info(f"closed result:: profit {profit}, budget: {return_budget}")
            self.budget += return_budget
            self.remove_position_from_listening(id)
            closed_result.update(id=id, price=price, volume=volume, profit=profit, price_diff=price_diff)
            closed_result.error = False
            self._trade_log_db.store_log(
                # create position with closed price and volume for logging
                Position(
                    position.position_side,
                    position.symbol,
                    position.trade_unit,
                    position.leverage,
                    price,
                    volume,
                    position.tp,
                    position.sl,
                    index,
                    id=position.id,
                ),
                order_type=-1,
                save_profit=True,
            )
            return closed_result
        else:
            self.remove_position_from_listening(id)
            logger.error("position id is not found")
            closed_result.msg = "position id is not found"
            closed_result.error = True
            return closed_result

    def remove_position_from_listening(self, id):
        if id in self.listening_positions:
            self.listening_positions.pop(id)

    def update_risk_config(self, risk_config: AccountRiskConfig):
        self.risk_config = risk_config

    def update_daily_max_loss(self):
        if self.risk_config is not None and self.risk_config.daily_max_loss_percent is not None:
            self.daily_max_loss = self.risk_config.daily_max_loss_percent * self.budget / 100.0

    def get_positions(self, symbols: List[str] = None) -> Tuple[List[Position], List[Position]]:
        """
            get all positions and separate long and short positions

        Args:
            symbols (List[str], optional): filter by symbols. Defaults to None, which means get all positions.

        Returns:
            Tuple[List[Position], List[Position]]: long_positions, short_positions
        """
        long_positions, short_positions = self.storage.get_positions(symbols=symbols)
        return list(long_positions), list(short_positions)

    def get_open_positions_risk_volume(self) -> float:
        """
            calculate total risk volume of open positions, which is used to consider max concurrent position limit
        Returns:
            float: total risk volume of open positions
        """
        long_positions, short_positions = self.get_positions()
        total_risk_volume = 0.0
        for position in long_positions + short_positions:
            if position.sl is not None:
                price_diff = abs(position.price - position.sl)
                risk_volume = position.trade_unit * position.volume * price_diff
                total_risk_volume += risk_volume
            else:
                logger.warning(f"position {position.id} has no stop loss, add X to risk volume")

        return total_risk_volume

    def get_daily_realized_pnl(self, date: str = None) -> float:
        """
            calculate today's realized PnL, which is used to consider max daily loss limit
        Returns:
            float: today's realized PnL
        """
        if date is None:
            today = pd.Timestamp.now(tz=self.tz_info).normalize()
            end_date = today + pd.Timedelta(days=1)
        else:
            today = pd.Timestamp(date, tz=self.tz_info).normalize()
            end_date = today + pd.Timedelta(days=1)
        logs = self._trade_log_db.get_profit_logs(start=today, end=end_date)
        daily_realized_pnl = sum(log.profit for _, log in logs.iterrows())
        return daily_realized_pnl

    def __del__(self):
        if hasattr(self, "storage"):
            self.storage.close()
        if hasattr(self, "_trade_log_db"):
            self._trade_log_db.close()
