"""
Quality signal — profitability and balance-sheet strength.

Requires the ``fundamentals`` enrichment (roe, roa, gross_margin,
net_margin, debt_equity columns).

Score = equal-weight z-score blend of:
    +ROE           (higher → better)
    +ROA           (higher → better)
    +gross_margin  (higher → better)
    -debt_equity   (lower leverage → better)

Cross-sectionally z-scored per date.
"""
from dataclasses import dataclass, field
import pandas as pd

from signals.base import SignalBase, SignalOutput


@dataclass
class QualitySignalConfig:
    components: dict[str, float] = field(
        default_factory=lambda: {
            "roe": 1.0,
            "roa": 1.0,
            "gross_margin": 1.0,
            "debt_equity": -1.0,   # negated: lower leverage = better
        }
    )
    """Map of column name → sign (+1 = higher is better, -1 = lower is better)."""

    required_components: int = 2
    """Minimum non-NaN components required to emit a score."""


class QualitySignal(SignalBase):
    """
    Long high-quality / short low-quality — profitability + balance sheet.
    """

    REQUIRED_ENRICHMENTS = ["fundamentals"]

    def __init__(self, config: QualitySignalConfig | None = None):
        self.config = config or QualitySignalConfig()

    def generate(self, df: pd.DataFrame) -> SignalOutput:
        df = df.copy()
        df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

        self._validate_columns(df)

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

    def _validate_columns(self, df: pd.DataFrame) -> None:
        available = [c for c in self.config.components if c in df.columns]
        if not available:
            raise ValueError(
                f"QualitySignal requires at least one of "
                f"{list(self.config.components)} in the DataFrame.  "
                f"Did you add 'fundamentals' to enrichments?"
            )

