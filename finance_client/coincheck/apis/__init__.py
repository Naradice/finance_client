from .account import Account
from .bankaccount import BankAccount
from .borrow import Borrow
from .deposit import Deposit
from .leverage import Leverage
from .order import *
from .orderbook import OrderBook
from .send import Send
from .servicebase import *
from .servicebase import ServiceBase
from .ticker import Ticker
from .trade import Trade
from .transfer import Transfer
from .withdraw import Withdraw
from .ws import Orders, TradeHistory

AVAILABLE_PAIRS = ["btc_jpy" "etc_jpy" "mona_jpy" "plt_jpy" "lsk_jpy" "omg_jpy"]
# TODO: make classes one file
