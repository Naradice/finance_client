from finance_client.coincheck.apis.servicebase import *
from finance_client.coincheck.apis.account import Account
from finance_client.coincheck.apis.bankaccount import BankAccount
from finance_client.coincheck.apis.borrow import Borrow
from finance_client.coincheck.apis.deposit import Deposit
from finance_client.coincheck.apis.leverage import Leverage
from finance_client.coincheck.apis.orderbook import OrderBook
from finance_client.coincheck.apis.send import Send
from finance_client.coincheck.apis.servicebase import ServiceBase
from finance_client.coincheck.apis.ticker import Ticker
from finance_client.coincheck.apis.trade import Trade
from finance_client.coincheck.apis.transfer import Transfer
from finance_client.coincheck.apis.withdraw import Withdraw
from finance_client.coincheck.apis.ws import TradeHistory, Orders
from finance_client.coincheck.apis.order import *

AVAILABLE_PAIRS = ["btc_jpy" "etc_jpy" "mona_jpy" "plt_jpy" "lsk_jpy" "omg_jpy"]
##TODO: make classes one file