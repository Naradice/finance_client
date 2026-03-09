"""
Responsibility: RiskManager
- Load config
- Refer account information
- Refer position information
- Generate risk option
- Calculate risk for current positions
- Decide parameters for trading
"""

import logging
import os

from finance_client.account import Manager as AccountManager
from finance_client.config.loader import load_symbol_risk_config
from finance_client.config.model import SymbolRiskConfig
from finance_client.risk_manager.model import RiskContext, RiskResult
from finance_client.risk_manager.risk_options.atr import ATRRisk
from finance_client.risk_manager.risk_options.fixed_loss import FixedAmountRisk
from finance_client.risk_manager.risk_options.percent_equity import \
    PercentEquityRisk
from finance_client.risk_manager.risk_options.risk_option import RiskOption

logger = logging.getLogger(__name__)

def create_percent_equity_option(percent: float) -> RiskOption:
    return PercentEquityRisk(percent)

def create_fixed_loss_option(loss: float) -> RiskOption:
    return FixedAmountRisk(loss)

def create_atr_option(percent: float, atr_multiplier: float, rr_ratio: float, atr_window=14, ohlc_columns=None) -> RiskOption:
    return ATRRisk(percent=percent, atr_multiplier=atr_multiplier, rr_ratio=rr_ratio, atr_window=atr_window)



class RiskManager:

    def __init__(self, account_manager: AccountManager, symbol_risk_config: str | SymbolRiskConfig):
        self.account_manager = account_manager
        self.symbol_risk_config = symbol_risk_config
        if isinstance(symbol_risk_config, str):
            if not os.path.exists(symbol_risk_config):
                raise FileNotFoundError(f"Symbol risk config file not found: {symbol_risk_config}")
            self.symbol_risk_config = load_symbol_risk_config(symbol_risk_config)
        elif isinstance(symbol_risk_config, SymbolRiskConfig):
            self.symbol_risk_config = symbol_risk_config
        else:
            raise ValueError("Invalid type for symbol_risk_config. Expected str (file path) or SymbolRiskConfig object.")

    def get_symbol_config(self, symbol):
        if self.symbol_risk_config is not None:
            if symbol in self.symbol_risk_config:
                return self.symbol_risk_config[symbol]
            else:
                logger.warning(f"symbol {symbol} is not found in symbol_risk_config.")
                return None
        else:
            logger.warning("symbol_risk_config is not initialized.")
            return None
        
    def _build_risk_context(self, account_equity:float, symbol: str, is_buy: bool, entry_price: float, stop_loss: float, take_profit: float) -> RiskContext:
        symbol_risk_config = self.get_symbol_config(symbol)
        return RiskContext(
            is_buy=is_buy,
            account_equity=account_equity,
            account_balance=self.account_manager.get_balance(),
            daily_realized_pnl=self.account_manager.get_daily_realized_pnl(),
            open_positions_loss_risk=self.account_manager.get_open_positions_risk_loss(),
            symbol_risk_config=symbol_risk_config,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            max_total_loss_risk=self.account_manager.get_max_total_loss_risk(),
            daily_max_loss=self.account_manager.get_daily_max_loss(),
        )
    
    def evaluate_risk(self, risk_option: RiskOption, account_equity:float, symbol: str, is_buy: bool, 
                      entry_price: float, stop_loss: float, take_profit: float, ohlc_df=None) -> RiskResult:
        """Evaluate the risk of a potential trade and determine position sizing and SL/TP levels.

        Args:
            risk_option (RiskOption): The risk option strategy to use for calculation (e.g. PercentEquityRisk, ATRRisk).
            account_equity (float): Current total equity of the trading account.
            symbol (str): The trading symbol (e.g. "USDJPY") for which to evaluate risk.
            is_buy (bool): True if the trade is a buy order, False if it's a sell order.
            entry_price (float): The intended entry price for the trade.
            stop_loss (float): The intended stop loss price for the trade.
            take_profit (float): The intended take profit price for the trade.
            ohlc_df (pd.DataFrame, optional): DataFrame containing OHLC data for the symbol. Defaults to None.

        Returns:
            RiskResult: The result of the risk evaluation, including position size and SL/TP levels.
        """
        context = self._build_risk_context(account_equity, symbol, is_buy, entry_price, stop_loss, take_profit)
        risk_result = risk_option.calculate(context, ohlc_df=ohlc_df)
        stop_distance = abs(entry_price - context.stop_loss) if context.stop_loss is not None else 0.0
        final_volume = self._apply_account_caps(risk_result.volume, stop_distance, context)
        risk_result.volume = final_volume
        return risk_result
    
    def _apply_account_caps(
        self,
        volume: float,
        stop_distance: float,
        context: RiskContext,
    ) -> float:
        # if context.symbol_risk_config.max_volume is not None:
        #     volume = min(volume, context.symbol_risk_config.max_volume)
        #     logger.info(f"Applied max volume cap: {context.symbol_risk_config.max_volume}, volume after cap: {volume}")

        if context.symbol_risk_config.min_volume is not None:
            volume = max(volume, context.symbol_risk_config.min_volume)
            logger.info(f"Applied min volume cap: {context.symbol_risk_config.min_volume}, volume after cap: {volume}")

        if context.max_total_loss_risk is not None:
            remaining_volume = max(0.0, context.max_total_loss_risk - context.open_positions_loss_risk)
            volume = min(volume, remaining_volume)
            logger.info(f"Applied max total loss risk cap: {context.max_total_loss_risk}, volume after cap: {volume}")
    
        if context.daily_max_loss is not None and stop_distance > 0:
            remaining_loss = max(0.0, context.daily_max_loss - context.daily_realized_pnl)
            max_volume_by_loss = remaining_loss / (stop_distance * context.symbol_risk_config.contract_size)
            volume = min(volume, max_volume_by_loss)
            logger.info(f"Applied daily max loss cap: {context.daily_max_loss}, volume after cap: {volume}")

        return volume