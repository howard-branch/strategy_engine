from dataclasses import dataclass
import pandas as pd

from signals.base import SignalBase, SignalOutput


@dataclass
class ReversalSignalConfig:
    use_vol_adjust: bool = False


class ReversalSignal(SignalBase):
    def __init__(self, config: ReversalSignalConfig | None = None):
        self.config = config or ReversalSignalConfig()

    def generate(self, df: pd.DataFrame) -> SignalOutput:
        df = df.copy()
        df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

        if self.config.use_vol_adjust:
            df["score"] = -df["ret_5d"] / df["vol_20d"]
        else:
            df["score"] = -df["ret_5d"]

        scores = df[["date", "symbol", "score", "tradable"]].copy()
        scores = scores[scores["tradable"]]
        scores = scores.dropna(subset=["score"])
        scores = scores[["date", "symbol", "score"]]

        return SignalOutput(scores=scores.reset_index(drop=True))