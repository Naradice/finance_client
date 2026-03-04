"""
ATR × 係数

指定時間足

定期再計算可能
"""

from finance_client.risk_manager.model import RiskContext, RiskResult
from finance_client.risk_manager.risk_options.risk_option import RiskOption


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

    def __init__(self, percent: float, atr_multiplier: float, rr_ratio: float, atr_process=None):
        """
        Args:
            percent (float): Percentage of account equity to risk per trade (e.g. 1.0 = 1%).
            atr_multiplier (float): Multiplier applied to ATR to determine SL distance.
            rr_ratio (float): Risk-to-reward ratio used to set take profit distance.
            atr_process (ATRProcess, optional): ATRProcess instance whose latest value is
                read automatically. If None, set atr_value manually before each trade.
        """
        self.percent = percent
        self.atr_multiplier = atr_multiplier
        self.rr_ratio = rr_ratio
        self.atr_process = atr_process  # ATRProcess instance; ATR is read from last_data automatically
        self.atr_value: float | None = None  # manual fallback; set before calculate() if no atr_process

    def _get_atr(self, ohlc_df=None) -> float:
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
            if ohlc_df is not None:
                result = self.atr_process.run(ohlc_df)
                return result[self.atr_process.KEY_ATR].iloc[-1]
            return self.atr_process.last_data[self.atr_process.KEY_ATR].iloc[-1]
        if self.atr_value is not None:
            return self.atr_value
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
        # ① SL距離決定
        atr = self._get_atr(ohlc_df)
        sl_distance = atr * self.atr_multiplier

        if context.is_buy:
            stop_loss = context.entry_price - sl_distance
            take_profit = context.entry_price + sl_distance * self.rr_ratio
        else:
            stop_loss = context.entry_price + sl_distance
            take_profit = context.entry_price - sl_distance * self.rr_ratio

        # ② 許容損失
        allowed_loss = context.account_equity * (self.percent / 100)
        loss_per_unit = sl_distance * context.symbol_risk_config.pip_value_per_lot
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
