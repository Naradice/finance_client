from . import enum
from . import frames as Frame
from . import utils
from .client_base import ClientBase
from .position import Position


class Client(ClientBase):

    def __init__(
        self,
        budget=1000000,
        provider="Default",
        symbols=...,
        out_ohlc_columns=...,
        idc_process=None,
        pre_process=None,
        economic_keys=None,
        frame=None,
        start_index=0,
        observation_length=None,
        do_render=False,
        enable_trade_log=False,
        storage=None,
        fx_client="auto",
        stock_client="auto",
    ):
        super().__init__(
            budget,
            provider,
            symbols,
            out_ohlc_columns,
            idc_process,
            pre_process,
            economic_keys,
            frame,
            start_index,
            observation_length,
            do_render,
            enable_trade_log,
            storage,
        )
        symbol_to_type = {}
        for symbol in self.symbols:
            client_type = utils.symbol_to_client_type(symbol)
            symbol_to_type[symbol] = client_type
