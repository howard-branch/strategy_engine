"""
VIX-regime conditioned signal wrapper.

Requires the ``market_regime`` enrichment (vix, vix_regime columns).

Wraps any inner signal and scales its scores based on VIX regime:
    low     → full weight (regime favours risk-on strategies)
    normal  → full weight
    high    → dampened
    extreme → strongly dampened or inverted

This lets you tilt momentum/breakout signals down in high-vol regimes
and tilt mean-reversion/reversal signals up.
"""
from dataclasses import dataclass, field
import pandas as pd

from signals.base import SignalBase, SignalOutput


@dataclass
class VixRegimeSignalConfig:
    regime_multipliers: dict[str, float] = field(
        default_factory=lambda: {
            "low": 1.0,
            "normal": 1.0,
            "high": 0.5,
            "extreme": 0.0,
        }
    )
    """Score multiplier per VIX regime bucket."""


class VixRegimeSignal(SignalBase):
    """
    Wraps another signal and modulates its scores by VIX regime.

    Usage::

        inner = MomentumSignal()
        conditioned = VixRegimeSignal(inner)
        conditioned.generate(bars)  # scores dampened in high-VIX regimes
    """

    REQUIRED_ENRICHMENTS = ["market_regime"]

    def __init__(
        self,
        inner: SignalBase,
        config: VixRegimeSignalConfig | None = None,
    ):
        self.inner = inner
        self.config = config or VixRegimeSignalConfig()

    def generate(self, df: pd.DataFrame) -> SignalOutput:
        if "vix_regime" not in df.columns:
            raise ValueError(
                "VixRegimeSignal requires 'vix_regime' column.  "
                "Did you add 'market_regime' to enrichments?"
            )

        inner_output = self.inner.generate(df)
        scores = inner_output.scores.copy()

        # Merge regime onto scores
        regime_map = df[["date", "symbol", "vix_regime"]].drop_duplicates(
            subset=["date", "symbol"]
        )
        scores = scores.merge(regime_map, on=["date", "symbol"], how="left")

        # Apply multiplier
        multipliers = self.config.regime_multipliers
        scores["_mult"] = scores["vix_regime"].map(multipliers).fillna(1.0)
        scores["score"] = scores["score"] * scores["_mult"]

        scores = scores[["date", "symbol", "score"]].reset_index(drop=True)
        scores = scores.dropna(subset=["score"])

        return SignalOutput(scores=scores)

