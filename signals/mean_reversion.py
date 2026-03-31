from dataclasses import dataclass
import pandas as pd

from signals.base import SignalBase, SignalOutput


@dataclass
class MeanReversionSignalConfig:
    lookback_days: int = 20
    """Window for computing the moving average and standard deviation."""

    min_periods: int = 15
    """Minimum observations required to emit a score."""


class MeanReversionSignal(SignalBase):
    """
    Bollinger-band style mean-reversion signal.

    Score = -(adj_close - SMA(lookback)) / rolling_std(lookback)

    Negative sign: a stock trading *below* its moving average gets a
    *positive* score (buy oversold), and vice-versa (short overbought).
    """

    def __init__(self, config: MeanReversionSignalConfig | None = None):
        self.config = config or MeanReversionSignalConfig()

    def generate(self, df: pd.DataFrame) -> SignalOutput:
        df = df.copy()
        df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

        g = df.groupby("symbol", sort=False)["adj_close"]

        lb = self.config.lookback_days
        mp = self.config.min_periods

        sma = g.transform(lambda x: x.rolling(lb, min_periods=mp).mean())
        std = g.transform(lambda x: x.rolling(lb, min_periods=mp).std(ddof=1))

        # Negative z-score: oversold → positive score (buy), overbought → negative (sell)
        df["score"] = -(df["adj_close"] - sma) / (std + 1e-8)

        scores = df[["date", "symbol", "score", "tradable"]].copy()
        scores = scores[scores["tradable"]]
        scores = scores.dropna(subset=["score"])
        scores = scores[["date", "symbol", "score"]].reset_index(drop=True)

        return SignalOutput(scores=scores)

