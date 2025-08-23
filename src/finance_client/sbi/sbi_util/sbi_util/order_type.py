CONDITION_NO = "None"
CONDITION_YORIASHI = "Y"
CONDITION_HIKESASHI = "H"
CONDITION_FUNARI = "F"
IOC = "I"

PendingOrderConditions = [CONDITION_NO, CONDITION_YORIASHI, CONDITION_HIKESASHI, CONDITION_FUNARI, IOC]
MarketOrderConditions = [CONDITION_NO, CONDITION_YORIASHI, CONDITION_HIKESASHI, IOC]

PERIOD_TODAY = "TODAY"
PERIOD_THIS_WEEK = "WEEK"
PERIODS = [PERIOD_TODAY, PERIOD_THIS_WEEK]

GNERAL_DEPOSIT = "deposit"
TOKUTEI_DEPOSIT = "sp_deposit"


class PendingOrder:
    def __init__(
        self,
        amount_of_stock_unit: int,
        price: float,
        condition: str = CONDITION_NO,
        SOR: bool = True,
        period: str = PERIOD_TODAY,
        deposit_type: str = TOKUTEI_DEPOSIT,
    ) -> None:
        if condition not in PendingOrderConditions:
            print(f"{condition} is not recognaized as condition for Pending Order. Will use no condition instead.")
            condition = CONDITION_NO
        elif SOR:
            if condition != CONDITION_NO:
                print("SOR can't specify the condition. Will use no condition.")
                condition = CONDITION_NO

        if period in PERIODS:
            print(f"{period} is not recognized as period. Will use today instead")
            period = PERIOD_TODAY

        self.condition = condition
        self.price = price
        self.amount = amount_of_stock_unit
        self.period = period


class MarketOrder:
    def __init__(
        self,
        amount_of_stock_unit: int,
        condition: str = CONDITION_NO,
        SOR: bool = True,
        period: str = PERIOD_TODAY,
        deposit_type: str = TOKUTEI_DEPOSIT,
    ) -> None:
        if condition not in MarketOrderConditions:
            print(f"{condition} is not recognaized as condition for Pending Order. Will use no condition instead.")
            condition = CONDITION_NO
        elif SOR:
            if condition != CONDITION_NO:
                print("SOR can't specify the condition. Will use no condition.")
                condition = CONDITION_NO

        if period in PERIODS:
            print(f"{period} is not recognized as period. Will use today instead")
            period = PERIOD_TODAY

        self.condition = condition
        self.amount = amount_of_stock_unit
        self.period = period


class LessUnitOrder:
    def __init__(self, amount_of_stock_unit: int, deposit_type: str = TOKUTEI_DEPOSIT) -> None:
        self.amount = amount_of_stock_unit


# PendingStopOrder(逆値指)、OCO, IFD、IFDOCOは利用しないため実装せず。必要になったら作成。


class CancelOrder:
    key = "cancel"

    def __init__(self):
        pass


class UpdateOrderWithPending:
    key = "update_p"

    def __init__(self, price: float) -> None:
        self.price = price


class UpdateOrderWithMarket:
    key = "update_m"

    def __init__(self, price: float) -> None:
        self.price = price


class SettlementPendingOrder:
    def __init__(self, amount_of_stock_unit: int, price: float, condition: str = CONDITION_NO, SOR: bool = True, period: str = PERIOD_TODAY) -> None:
        if condition not in PendingOrderConditions:
            print(f"{condition} is not recognaized as condition for Pending Order. Will use no condition instead.")
            condition = CONDITION_NO
        elif SOR:
            if condition != CONDITION_NO:
                print("SOR can't specify the condition. Will use no condition.")
                condition = CONDITION_NO

        if period in PERIODS:
            print(f"{period} is not recognized as period. Will use today instead")
            period = PERIOD_TODAY

        self.condition = condition
        self.price = price
        self.amount = amount_of_stock_unit
        self.period = period


class SettlementMarketOrder:
    def __init__(self, amount_of_stock_unit: int, condition: str = CONDITION_NO, SOR: bool = True, period: str = PERIOD_TODAY) -> None:
        if condition not in MarketOrderConditions:
            print(f"{condition} is not recognaized as condition for Pending Order. Will use no condition instead.")
            condition = CONDITION_NO
        elif SOR:
            if condition != CONDITION_NO:
                print("SOR can't specify the condition. Will use no condition.")
                condition = CONDITION_NO

        if period in PERIODS:
            print(f"{period} is not recognized as period. Will use today instead")
            period = PERIOD_TODAY

        self.condition = condition
        self.amount = amount_of_stock_unit
        self.period = period


class SettlementLessUnitOrder:
    def __init__(self, amount_of_stock_unit: int) -> None:
        self.amount = amount_of_stock_unit
