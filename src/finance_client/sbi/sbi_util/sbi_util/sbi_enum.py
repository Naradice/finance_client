from enum import Enum

buy = "buy"
sell = "sell"
margin_buy = "margin_buy"
margin_sell = "margin_sell"

order_types = [buy, sell, margin_buy, margin_sell]

symbol_page_to_order_element = {
    buy: "現物買",
    sell: "現物売",
    margin_buy: "信用買",
    margin_sell: "信用売",
}

ERROR_BUDGET_IS_SHORT = "WECEK00210"


class COMMON_HEADER(Enum):
    HOME = 0
    MARKET = 1
    JP_STOCK = 2
    STOCK = 3
    TRUST = 4
    CLAIM = 5
    FX = 6
    OPTION = 7
    CFD = 8
    GOLD = 9
    NISA = 10
    IDECO = 11
    OTHER = 12


class TRADE_HEADER(Enum):
    TRADE_EXCHANGE = 0  # 新規注文取引所
    TRADE_PTS = 1  # 新規注文PTS
    ACCUMULATION = 2  # 積立
    CREDIT_SETTLEMENT = 3  # 信用返済・取引現渡
    OWNED_STOCKS = 4  # 保有株式
    ORDER_INQUIRY = 5  # 注文照会・注文訂正
    IPO_PO = 6  # IPO・PO
    OFF_HOURS_TRADING = 7  # 立会外
    FRACTIONAL_STOCK = 8  # 単元未満株
    THEME_INVESTMENT = 9  # テーマ投資
