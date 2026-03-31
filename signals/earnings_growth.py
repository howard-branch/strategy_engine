"""
Earnings-growth signal — YoY revenue and earnings momentum.

Requires the ``fundamentals`` enrichment (revenue_yoy, earnings_yoy columns).

Score = equal-weight z-score blend of revenue_yoy and earnings_yoy.
"""
from dataclasses import dataclass, field
import pandas as pd

from signals.base import SignalBase, SignalOutput


@dataclass
class EarningsGrowthSignalConfig:
    components: dict[str, float] = field(
        default_factory=lambda: {
            "revenue_yoy": 1.0,
            "earnings_yoy": 1.0,
        }
    )
    required_components: int = 1


class EarningsGrowthSignal(SignalBase):
    """
    Long fast growers / short shrinkers.
    """

    REQUIRED_ENRICHMENTS = ["fundamentals"]

    def __init__(self, config: EarningsGrowthSignalConfig | None = None):
        self.config = config or EarningsGrowthSignalConfig()

    def generate(self, df: pd.DataFrame) -> SignalOutput:
        df = df.copy()
        df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

        z_parts = []
        for comp, sign in self.config.components.items():
            if comp not in df.columns:
                continue
            col = f"_z_{comp}"
            df[col] = sign * self.zscore_by_date(df, comp)
            z_parts.append(col)

        if not z_parts:
            return SignalOutput(
                scores=pd.DataFrame(columns=["date", "symbol", "score"])
            )

        z_df = df[z_parts]
        df["_n_valid"] = z_df.notna().sum(axis=1)
        df["score"] = z_df.mean(axis=1)
        df.loc[df["_n_valid"] < self.config.required_components, "score"] = float("nan")

        scores = df[["date", "symbol", "score", "tradable"]].copy()
        scores = scores[scores["tradable"]]
        scores = scores.dropna(subset=["score"])
        scores = scores[["date", "symbol", "score"]].reset_index(drop=True)

        return SignalOutput(scores=scores)

