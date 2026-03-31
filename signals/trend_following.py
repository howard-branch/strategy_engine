from dataclasses import dataclass
import pandas as pd

from signals.base import SignalBase, SignalOutput


@dataclass
class TrendFollowingSignalConfig:
    fast_span: int = 12
    """EMA span for the fast moving average."""

    slow_span: int = 50
    """EMA span for the slow moving average."""

    min_periods: int = 50
    """Minimum observations before emitting a score."""

    normalise_by_price: bool = True
    """If True, express the crossover gap as a fraction of price."""


class TrendFollowingSignal(SignalBase):
    """
    Moving-average crossover trend-following signal.

    Score = (EMA_fast - EMA_slow) / price   (if normalise_by_price)

    A positive score means the fast EMA is above the slow EMA (uptrend);
    a negative score means the opposite (downtrend).
    """

    def __init__(self, config: TrendFollowingSignalConfig | None = None):
        self.config = config or TrendFollowingSignalConfig()

    def generate(self, df: pd.DataFrame) -> SignalOutput:
        df = df.copy()
        df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

        g = df.groupby("symbol", sort=False)["adj_close"]

        ema_fast = g.transform(
            lambda x: x.ewm(span=self.config.fast_span, min_periods=self.config.min_periods).mean()
        )
        ema_slow = g.transform(
            lambda x: x.ewm(span=self.config.slow_span, min_periods=self.config.min_periods).mean()
        )

        gap = ema_fast - ema_slow

        if self.config.normalise_by_price:
            df["score"] = gap / (df["adj_close"] + 1e-8)
        else:
            df["score"] = gap

        scores = df[["date", "symbol", "score", "tradable"]].copy()
        scores = scores[scores["tradable"]]
        scores = scores.dropna(subset=["score"])
        scores = scores[["date", "symbol", "score"]].reset_index(drop=True)

        return SignalOutput(scores=scores)

