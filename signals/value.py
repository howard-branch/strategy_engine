"""
Value signal — composite cheapness score.

Requires the ``valuations`` enrichment (pe, pb, ps, ev_ebitda columns).

Score = equal-weight z-score blend of:
    -1/PE  (lower PE → higher score)
    -PB    (lower PB → higher score)
    -PS    (lower PS → higher score)
    -EV/EBITDA (lower → higher score)

Cross-sectionally z-scored per date so each component contributes equally.
"""
from dataclasses import dataclass, field
import pandas as pd

from signals.base import SignalBase, SignalOutput


@dataclass
class ValueSignalConfig:
    components: list[str] = field(
        default_factory=lambda: ["pe", "pb", "ps", "ev_ebitda"]
    )
    """Which valuation metrics to include in the composite."""

    required_components: int = 2
    """Minimum non-NaN components required to emit a score."""


class ValueSignal(SignalBase):
    """
    Long cheap / short expensive — multi-metric value composite.
    """

    REQUIRED_ENRICHMENTS = ["valuations"]

    def __init__(self, config: ValueSignalConfig | None = None):
        self.config = config or ValueSignalConfig()

    def generate(self, df: pd.DataFrame) -> SignalOutput:
        df = df.copy()
        df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

        self._validate_columns(df)

        # Build component z-scores (negated: lower ratio → higher score)
        z_parts = []
        for comp in self.config.components:
            if comp not in df.columns:
                continue
            col = f"_z_{comp}"
            # Negate: cheap (low ratio) should score high
            df[col] = -self.zscore_by_date(df, comp)
            z_parts.append(col)

        if not z_parts:
            return SignalOutput(
                scores=pd.DataFrame(columns=["date", "symbol", "score"])
            )

        # Composite: mean of available z-scores
        z_df = df[z_parts]
        df["_n_valid"] = z_df.notna().sum(axis=1)
        df["score"] = z_df.mean(axis=1)

        # Require minimum number of non-NaN components
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
                f"ValueSignal requires at least one of {self.config.components} "
                f"in the DataFrame.  Did you add 'valuations' to enrichments?"
            )

