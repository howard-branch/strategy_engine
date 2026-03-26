from dataclasses import dataclass
import pandas as pd

from signals.base import SignalBase, SignalOutput


@dataclass
class MomentumSignalConfig:
    lookback_days: int = 120
    skip_days: int = 5
    min_history_days: int = 126


class MomentumSignal(SignalBase):
    def __init__(self, config: MomentumSignalConfig | None = None):
        self.config = config or MomentumSignalConfig()

    def generate(self, df: pd.DataFrame) -> SignalOutput:
        df = df.copy()
        df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

        g = df.groupby("symbol", sort=False)["adj_close"]

        lb = self.config.lookback_days
        skip = self.config.skip_days

        df["score"] = g.shift(skip) / g.shift(lb + skip) - 1.0

        scores = df[["date", "symbol", "score", "tradable"]].copy()
        scores = scores[scores["tradable"]].copy()
        scores = scores.dropna(subset=["score"])
        scores = scores[["date", "symbol", "score"]].reset_index(drop=True)

        return SignalOutput(scores=scores)