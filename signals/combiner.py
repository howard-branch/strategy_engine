"""
CombinedSignal — weighted blend of multiple signals.

Each component signal is z-scored cross-sectionally (per date) before
combining, so raw-score magnitudes don't dominate the blend.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import pandas as pd

from signals.base import SignalBase, SignalOutput


@dataclass
class CombinedSignalConfig:
    """
    Map of signal name → weight.

    Example
    -------
    >>> CombinedSignalConfig(weights={"momentum": 1.0, "reversal": 0.3})
    """
    weights: dict[str, float] = field(default_factory=dict)

    def normalised(self) -> dict[str, float]:
        """Return weights that sum to 1.0 (by absolute value)."""
        total = sum(abs(w) for w in self.weights.values())
        if total == 0:
            return self.weights.copy()
        return {k: v / total for k, v in self.weights.items()}


class CombinedSignal(SignalBase):
    """
    Blends an arbitrary number of signals with configurable weights.

    Steps
    -----
    1. Generate each component signal.
    2. Cross-sectionally z-score each signal per date.
    3. Weighted sum of z-scores → combined score.
    """

    def __init__(
        self,
        signals: dict[str, SignalBase],
        config: CombinedSignalConfig,
    ):
        self.signals = signals
        self.config = config
        missing = set(config.weights) - set(signals)
        if missing:
            raise ValueError(f"Weights reference unknown signals: {sorted(missing)}")

    def generate(self, df: pd.DataFrame) -> SignalOutput:
        component_scores: list[tuple[str, float, pd.DataFrame]] = []

        for name, sig in self.signals.items():
            weight = self.config.weights.get(name, 0.0)
            if weight == 0.0:
                continue
            out = sig.generate(df)
            s = out.scores.rename(columns={"score": f"score_{name}"})
            component_scores.append((name, weight, s))

        if not component_scores:
            return SignalOutput(
                scores=pd.DataFrame(columns=["date", "symbol", "score"])
            )

        # Merge all component scores on (date, symbol) via inner join
        merged = component_scores[0][2]
        for _, _, s in component_scores[1:]:
            merged = merged.merge(s, on=["date", "symbol"], how="inner")

        # Z-score each component cross-sectionally, then blend
        merged["score"] = 0.0
        for name, weight, _ in component_scores:
            col = f"score_{name}"
            z = self.zscore_by_date(merged, col)
            merged["score"] = merged["score"] + weight * z

        scores = merged[["date", "symbol", "score"]].copy()
        scores = scores.dropna(subset=["score"]).reset_index(drop=True)
        return SignalOutput(scores=scores)

