from .client_base import ClientBase
from .position import ORDER_TYPE, POSITION_TYPE, Position


class AgentTool:

    def __init__(self, client: ClientBase):
        self.client = client

    def market_order(self, is_buy: bool, volume: float, symbol: str, tp: float, sl: float):
        """order to open a position
        Args:
            is_buy (bool): buy order or not
            volume (float): amount of trade unit
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            tp (float): specify take profit price. if less than 0 is specified, order without tp
            sl (float): specify stop loss price. if less than 0 is specified, order without sl

        Returns:
            price (float): price returned from client. 0 if order is failed.
            id (str): id of position or order. None if order is failed
        """
        if tp is None or tp <= 0:
            tp = None
        if sl is None or sl <= 0:
            sl = None
        suc, position = self.client.open_trade(is_buy=is_buy, price=None, symbol=symbol, order_type=ORDER_TYPE.market, amount=volume, tp=tp, sl=sl)
        if suc and position is not None:
            return {"price": position.price, "id": position.id}
        else:
            return {"price": 0, "id": None}

    def pending_order(self, is_buy: bool, volume: float, symbol: str, order_type: int, tp: float, sl: float, price: float):
        """pending order to open a position when price condition match
        Args:
            is_buy (bool): buy order or not
            price (float): order price
            volume (float): amount of trade unit
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            order_type (int): 1: Limit, 2: Stop
            tp (float): specify take profit price. if less than 0 is specified, order without tp
            sl (float): specify stop loss price. if less than 0 is specified, order without sl

        Returns:
            price (float): price returned from client. 0 if order is failed.
            id (str): id of position or order. None if order is failed
        """
        if tp is None or tp <= 0:
            tp = None
        if sl is None or sl <= 0:
            sl = None
        if price is None or price <= 0:
            price = None
        suc, position = self.client.open_trade(is_buy=is_buy, price=price, symbol=symbol, order_type=order_type, amount=volume, tp=tp, sl=sl)
        if suc and position is not None:
            return {"price": position.price, "id": position.id}
        else:
            return {"price": 0, "id": None}

    def close_posiion(self, price: float, id: str, volume: float, symbol: str):
        """closed a position based on id. id should be specified which is returned when order it.
        Args:
            price (float): price to close position. if 0 is specified, close by market price.
            id (str): id if position
            volume (float): volume of position to close. if 0 is specified, close all volume
            symbol (str): symbol of currency, stock etc. ex USDJPY.
        Returns:
            closed_price (float): closed price. 0 if order is failed.
            profit(float): profit of your trade result. 0 if order is failed.
        """
        close_price, position_price, prifit_unit, profit, suc = self.client.close_position(price=price, id=id, amount=volume, symbol=symbol)
        if suc:
            return {"closed_price": close_price, "profit": profit}
        else:
            return {"closed_price": 0, "profit": 0}

    def close_all_positions(self):
        """
        Returns:
            {
                $id:{
                    closed_price (float): price,
                    profit(float): profit by your order
                }
            }
        """
        results = self.client.close_all_positions()
        # convert result to dict for agent
        result_dict = {}
        for result in results:
            if result is not None:
                id = result[-1]
                suc = result[-2]
                if suc:
                    result_dict[id] = {"closed_price": result[0], "profit": result[3]}
        return result_dict

    def get_positions(self):
        """
        Returns:
            {
                $id: {
                    price (float): price of position,
                    volume (float): volume of position,
                    symbol (str): symbol of position,
                    is_buy (bool): True if long position,
                    tp (float): take profit price of position,
                    sl (float): stop loss price of position,
                }
            }
        """
        positions = self.client.get_positions()
        Position()
        return_positions_dict = {}
        for position in positions:
            return_positions_dict[position.id] = {
                "price": position.price,
                "volume": position.amount,
                "symbol": position.symbol,
                "is_buy": True if position.position_type == POSITION_TYPE.long else False,
                "tp": 0 if position.tp is None else position.tp,
                "sl": 0 if position.sl is None else position.sl,
            }

        return return_positions_dict

    def cancel_order(self, id: str):
        """
        Args:
            id(str): id of order
        Returns:
            {
                result(bool): True if Success
            }
        """
        suc = self.client.cancel_order(id)
        return {"result": suc}

    def get_ohlc(self, symbol: str, length: int, frame: str):
        """
        Args:
            symbol (str): symbol of currency, stock etc. ex USDJPY.
            length (int): specify data length > 1. If less than 0 is specified, return all date.
            frame (str): specify frame to get time series data. any of Xmin (e.g. 1min), Xh(e.g. 1h), XD(e.g. 1D), WX(e.g. W1), MOX(e.g. MO1)

        Returns:
            {
                "open": {
                    $index: $value
                },
                "high": {
                    $index: $value
                },
                "low":{
                    $index: $value
                }
                "close":{
                    $index: $value
                }
            }
        """
        ohlc_df = self.client.get_ohlc(symbol, length, frame)
        return ohlc_df.to_dict()
