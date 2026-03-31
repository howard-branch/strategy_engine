from dataclasses import dataclass
import pandas as pd

from signals.base import SignalBase, SignalOutput


@dataclass
class LowVolatilitySignalConfig:
    lookback_days: int = 60
    """Window over which to compute realised volatility."""

    min_periods: int = 40
    """Minimum observations required."""


class LowVolatilitySignal(SignalBase):
    """
    Low-volatility anomaly signal.

    Score = -realised_volatility

    Low-vol stocks receive positive scores (long), high-vol stocks
    receive negative scores (short).  This exploits the well-documented
    empirical finding that low-volatility equities deliver higher
    risk-adjusted returns than high-volatility equities.
    """

    def __init__(self, config: LowVolatilitySignalConfig | None = None):
        self.config = config or LowVolatilitySignalConfig()

    def generate(self, df: pd.DataFrame) -> SignalOutput:
        df = df.copy()
        df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

        g = df.groupby("symbol", sort=False)["ret_1d"]

        lb = self.config.lookback_days
        mp = self.config.min_periods

        vol = g.transform(lambda x: x.rolling(lb, min_periods=mp).std(ddof=1))

        # Negate: lower vol → higher score
        df["score"] = -vol

        scores = df[["date", "symbol", "score", "tradable"]].copy()
        scores = scores[scores["tradable"]]
        scores = scores.dropna(subset=["score"])
        scores = scores[["date", "symbol", "score"]].reset_index(drop=True)

        return SignalOutput(scores=scores)

