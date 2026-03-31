"""
Insider-sentiment signal — net insider buying over trailing 90 days.

Requires the ``insider`` enrichment (insider_net_buy_90d column).

Score = cross-sectional z-score of insider_net_buy_90d.
Positive → net insider buying (bullish), negative → net selling (bearish).
"""
from dataclasses import dataclass
import pandas as pd

from signals.base import SignalBase, SignalOutput


@dataclass
class InsiderSignalConfig:
    column: str = "insider_net_buy_90d"


class InsiderSignal(SignalBase):
    """
    Long names with insider buying / short names with insider selling.
    """

    REQUIRED_ENRICHMENTS = ["insider"]

    def __init__(self, config: InsiderSignalConfig | None = None):
        self.config = config or InsiderSignalConfig()

    def generate(self, df: pd.DataFrame) -> SignalOutput:
        df = df.copy()
        col = self.config.column

        if col not in df.columns:
            raise ValueError(
                f"InsiderSignal requires '{col}' in the DataFrame.  "
                f"Did you add 'insider' to enrichments?"
            )

        df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
        df["score"] = self.zscore_by_date(df, col)

        scores = df[["date", "symbol", "score", "tradable"]].copy()
        scores = scores[scores["tradable"]]
        scores = scores.dropna(subset=["score"])
        scores = scores[["date", "symbol", "score"]].reset_index(drop=True)

        return SignalOutput(scores=scores)

