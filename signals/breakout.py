from dataclasses import dataclass
import pandas as pd

from signals.base import SignalBase, SignalOutput


@dataclass
class BreakoutSignalConfig:
    lookback_days: int = 252
    """Window for computing the high/low range (default ~1 year)."""

    min_periods: int = 126
    """Minimum observations required."""


class BreakoutSignal(SignalBase):
    """
    Breakout / 52-week range signal.

    Score = (adj_close - rolling_low) / (rolling_high - rolling_low)

    Rescaled to [-1, +1] by subtracting 0.5 and multiplying by 2:
        score = 2 * pct_range - 1

    Stocks near their 52-week high receive a positive score (long),
    stocks near their 52-week low receive a negative score (short).

    Captures the empirical tendency for stocks breaking to new highs
    to continue appreciating (price-level momentum / breakout effect).
    """

    def __init__(self, config: BreakoutSignalConfig | None = None):
        self.config = config or BreakoutSignalConfig()

    def generate(self, df: pd.DataFrame) -> SignalOutput:
        df = df.copy()
        df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

        g = df.groupby("symbol", sort=False)["adj_close"]

        lb = self.config.lookback_days
        mp = self.config.min_periods

        rolling_high = g.transform(lambda x: x.rolling(lb, min_periods=mp).max())
        rolling_low = g.transform(lambda x: x.rolling(lb, min_periods=mp).min())

        range_width = rolling_high - rolling_low
        pct_range = (df["adj_close"] - rolling_low) / (range_width + 1e-8)

        # Rescale [0, 1] → [-1, +1]
        df["score"] = 2.0 * pct_range - 1.0

        # Zero out scores where the range is essentially flat (avoid noise)
        df.loc[range_width < 1e-6, "score"] = 0.0

        scores = df[["date", "symbol", "score", "tradable"]].copy()
        scores = scores[scores["tradable"]]
        scores = scores.dropna(subset=["score"])
        scores = scores[["date", "symbol", "score"]].reset_index(drop=True)

        return SignalOutput(scores=scores)

