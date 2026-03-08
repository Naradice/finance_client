"""
ATR × 係数

指定時間足

定期再計算可能
"""

import logging

from finance_client.fprocess.fprocess.idcprocess import ATRProcess
from finance_client.risk_manager.model import RiskContext, RiskResult
from finance_client.risk_manager.risk_options.risk_option import RiskOption

logger = logging.getLogger(__name__)

class ATRRisk(RiskOption):
    """Risk sizing based on ATR-derived stop loss distance.

    Stop loss is placed at entry ± (ATR × atr_multiplier).
    Take profit is placed at entry ± (SL distance × rr_ratio).
    Volume is sized so that the monetary risk equals percent% of account equity.

    ATR value source (in priority order):
    1. atr_process: an ATRProcess instance — ATR is read automatically from
       atr_process.last_data on each calculate() call.
    2. atr_value: assign manually before calling calculate() when no
       atr_process is provided.
    """

    def __init__(self, percent: float, atr_multiplier: float, rr_ratio: float,
                 atr_window: int = 14, ohlc_columns:list[str] = None,
                 atr_process=None):
        """
        Args:
            percent (float): Percentage of account equity to risk per trade (e.g. 1.0 = 1%).
            atr_multiplier (float): Multiplier applied to ATR to determine SL distance.
            rr_ratio (float): Risk-to-reward ratio used to set take profit distance.
            atr_window (int, optional): Window size for ATR calculation. Defaults to 14.
            ohlc_columns (list[str], optional): Column names for OHLC data if using atr_process. Defaults to None, then use ['open', 'high', 'low', 'close'].
            atr_process (ATRProcess, optional): ATRProcess instance whose latest value is
                read automatically. If None, set atr_value manually before each trade.
        """
        self.atr_multiplier = atr_multiplier
        self.percent = percent
        self.rr_ratio = rr_ratio

        if atr_process is not None and isinstance(atr_process, ATRProcess):
            self.atr_process = atr_process
            logging.info(f"Initialized ATRRisk with ATRProcess specified. other parameters will be ignored.")
        else:
            self.atr_window = atr_window
            self.ohlc_columns = ohlc_columns if ohlc_columns is not None else ['open', 'high', 'low', 'close']
            self.atr_process = ATRProcess(window=atr_window, ohlc_column_name=self.ohlc_columns)

    def get_required_ohlc_length(self):
        return super().get_required_ohlc_length() + self.atr_process.get_minimum_required_length()

    def _get_atr(self, ohlc_df) -> float:
        """Return the current ATR value.

        Resolution order:
        1. ohlc_df + atr_process: run atr_process.run(ohlc_df) and take the last value.
        2. atr_process alone: read from atr_process.last_data (pre-computed).
        3. atr_value: manually assigned float.

        Args:
            ohlc_df (pd.DataFrame, optional): OHLC data to compute ATR from.
                Only used when atr_process is also set.

        Returns:
            float: Current ATR value.

        Raises:
            ValueError: If none of the above sources is available.
        """
        if self.atr_process is not None:
            result = self.atr_process.run(ohlc_df)
            return result[self.atr_process.KEY_ATR].iloc[-1]
        raise ValueError("ATR value is not available. Provide atr_process at init or set atr_value before calling calculate().")

    def calculate(self, context: RiskContext, ohlc_df=None) -> RiskResult:
        """Calculate position size and SL/TP prices using ATR-based risk sizing.

        Args:
            context (RiskContext): Current account state and trade parameters.
                context.stop_loss and context.take_profit are ignored; both are
                computed from the ATR value instead.
            ohlc_df (pd.DataFrame, optional): OHLC data passed to atr_process.run()
                to compute a fresh ATR. Ignored if atr_process is not set.

        Returns:
            RiskResult: Volume, stop_loss_price, take_profit_price, risk_volume,
                reward_volume, and risk_reward_ratio.

        Raises:
            ValueError: If no ATR value is available (see _get_atr).
        """
        # SL距離決定
        if ohlc_df is None:
            raise ValueError("ohlc_df is required for ATRRisk calculation to obtain ATR value.")
        atr = self._get_atr(ohlc_df)
        sl_distance = atr * self.atr_multiplier

        if context.is_buy:
            stop_loss = context.entry_price - sl_distance
            take_profit = context.entry_price + sl_distance * self.rr_ratio
        else:
            stop_loss = context.entry_price + sl_distance
            take_profit = context.entry_price - sl_distance * self.rr_ratio

        # 許容損失
        allowed_loss = context.account_equity * (self.percent / 100)
        loss_per_unit = sl_distance * context.symbol_risk_config.contract_size
        raw_volume = allowed_loss / loss_per_unit

        volume = self._round_volume(raw_volume, context)

        risk_volume = self._calc_loss(volume, context, stop_loss)
        reward_volume = risk_volume * self.rr_ratio

        return RiskResult(
            volume=volume,
            stop_loss_price=stop_loss,
            take_profit_price=take_profit,
            risk_volume=risk_volume,
            reward_volume=reward_volume,
            risk_reward_ratio=self.rr_ratio,
        )
