"""Signal registry — canonical name → factory function."""

from __future__ import annotations

from typing import Any, Callable

from signals.base import SignalBase, SignalOutput
from signals.breakout import BreakoutSignal, BreakoutSignalConfig
from signals.earnings_growth import EarningsGrowthSignal, EarningsGrowthSignalConfig
from signals.insider import InsiderSignal, InsiderSignalConfig
from signals.low_volatility import LowVolatilitySignal, LowVolatilitySignalConfig
from signals.mean_reversion import MeanReversionSignal, MeanReversionSignalConfig
from signals.momentum import MomentumSignal, MomentumSignalConfig
from signals.quality import QualitySignal, QualitySignalConfig
from signals.reversal import ReversalSignal, ReversalSignalConfig
from signals.trend_following import TrendFollowingSignal, TrendFollowingSignalConfig
from signals.value import ValueSignal, ValueSignalConfig

# Each entry: name → callable(**kwargs) → SignalBase
SIGNAL_REGISTRY: dict[str, Callable[..., SignalBase]] = {
    # ── price/volume only (no enrichment needed) ──
    "momentum": lambda cfg=None: MomentumSignal(cfg or MomentumSignalConfig()),
    "reversal": lambda cfg=None: ReversalSignal(cfg or ReversalSignalConfig()),
    "mean_reversion": lambda cfg=None: MeanReversionSignal(cfg or MeanReversionSignalConfig()),
    "breakout": lambda cfg=None: BreakoutSignal(cfg or BreakoutSignalConfig()),
    "low_volatility": lambda cfg=None: LowVolatilitySignal(cfg or LowVolatilitySignalConfig()),
    "trend_following": lambda cfg=None: TrendFollowingSignal(cfg or TrendFollowingSignalConfig()),
    # ── enrichment-dependent signals ──
    "value": lambda cfg=None: ValueSignal(cfg or ValueSignalConfig()),
    "quality": lambda cfg=None: QualitySignal(cfg or QualitySignalConfig()),
    "earnings_growth": lambda cfg=None: EarningsGrowthSignal(cfg or EarningsGrowthSignalConfig()),
    "insider": lambda cfg=None: InsiderSignal(cfg or InsiderSignalConfig()),
}


def make_signal(name: str, config: Any = None) -> SignalBase:
    """Instantiate a signal by name, optionally overriding its config."""
    if name not in SIGNAL_REGISTRY:
        raise ValueError(
            f"Unknown signal '{name}'. Available: {sorted(SIGNAL_REGISTRY)}"
        )
    return SIGNAL_REGISTRY[name](config)


def list_signals() -> list[str]:
    return sorted(SIGNAL_REGISTRY)

