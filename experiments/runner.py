"""
Shared experiment runner — loads data, generates signal, builds portfolio,
runs backtest, and prints a performance summary.

Every ``run_*.py`` now delegates to this module so the boilerplate lives
in exactly one place.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from backtest.simulator import simulate_backtest, SimulatorConfig
from config.settings import DatabaseConfig
from data.market_data import MarketData, MarketDataConfig
from features.basic_features import add_basic_features
from features.enrichments import apply_enrichments
from portfolio.construct import construct_rank_based_portfolio, PortfolioConfig
from reports.performance import performance_summary
from signals.base import SignalBase, SignalOutput


# ── defaults ────────────────────────────────────────────────────────────
DEFAULT_MARKET_DATA_CONFIG = MarketDataConfig(
    min_price=5.0,
    min_dollar_volume=5_000_000.0,
    min_history_days=126,
    schema="strategy_engine",
)

DEFAULT_PORTFOLIO_CONFIG = PortfolioConfig(
    top_n=10,
    entry_n=10,
    exit_n=20,
    gross_long=0.5,
    gross_short=0.5,
    rebalance_frequency="W-FRI",
    min_names_per_side=5,
)

DEFAULT_SIMULATOR_CONFIG = SimulatorConfig(cost_per_unit_turnover=0.0005)


@dataclass
class ExperimentConfig:
    """All the knobs for a single-signal experiment."""
    start_date: str = "2018-01-01"
    end_date: str = "2025-12-31"
    symbols: list[str] | None = None
    market_data_config: MarketDataConfig = field(
        default_factory=lambda: DEFAULT_MARKET_DATA_CONFIG
    )
    portfolio_config: PortfolioConfig = field(
        default_factory=lambda: DEFAULT_PORTFOLIO_CONFIG
    )
    simulator_config: SimulatorConfig = field(
        default_factory=lambda: DEFAULT_SIMULATOR_CONFIG
    )
    enrichments: list[str] = field(default_factory=list)


# ── data loading (cached across runs) ──────────────────────────────────

def load_bars(cfg: ExperimentConfig) -> pd.DataFrame:
    """Load market data + basic features + optional enrichments.  Expensive — call once."""
    connection_string = DatabaseConfig.get_connection_string()
    md = MarketData(
        connection_string=connection_string,
        config=cfg.market_data_config,
    )
    bars = md.load_bars(
        start_date=cfg.start_date,
        end_date=cfg.end_date,
        symbols=cfg.symbols,
    )
    bars = add_basic_features(bars)

    if cfg.enrichments:
        bars = apply_enrichments(
            engine=md.engine,
            bars=bars,
            enrichments=cfg.enrichments,
            schema=cfg.market_data_config.schema,
        )

    return bars


# ── core pipeline ───────────────────────────────────────────────────────

def run_signal(
    signal: SignalBase,
    bars: pd.DataFrame,
    portfolio_config: PortfolioConfig | None = None,
    simulator_config: SimulatorConfig | None = None,
) -> tuple[dict, pd.DataFrame]:
    """
    Generate signal → portfolio → backtest → summary.

    Returns (summary_dict, backtest_daily_df).
    """
    portfolio_config = portfolio_config or DEFAULT_PORTFOLIO_CONFIG
    simulator_config = simulator_config or DEFAULT_SIMULATOR_CONFIG

    signal_output: SignalOutput = signal.generate(bars)
    scores = signal_output.scores

    weights = construct_rank_based_portfolio(scores, portfolio_config)

    backtest_daily = simulate_backtest(
        weights=weights,
        bars=bars,
        config=simulator_config,
    )

    summary = performance_summary(backtest_daily)
    return summary, backtest_daily


def run_experiment(
    signal: SignalBase,
    cfg: ExperimentConfig | None = None,
    *,
    label: str = "Strategy",
    bars: pd.DataFrame | None = None,
    verbose: bool = True,
) -> tuple[dict, pd.DataFrame]:
    """
    End-to-end: load data (if not provided) → signal → portfolio → backtest → print.

    Parameters
    ----------
    signal : SignalBase
        Any signal that implements ``.generate(bars)``.
    cfg : ExperimentConfig, optional
        Experiment-level settings (dates, portfolio, simulator).
    label : str
        Header printed before results.
    bars : DataFrame, optional
        Pre-loaded bars — avoids reloading when sweeping.
    verbose : bool
        If True print the summary to stdout.

    Returns
    -------
    (summary_dict, backtest_daily)
    """
    cfg = cfg or ExperimentConfig()

    if bars is None:
        bars = load_bars(cfg)

    summary, backtest_daily = run_signal(
        signal=signal,
        bars=bars,
        portfolio_config=cfg.portfolio_config,
        simulator_config=cfg.simulator_config,
    )

    if verbose:
        print(f"\n=== {label} ===")
        print("\nPerformance summary")
        print("-------------------")
        for k, v in summary.items():
            print(f"{k:20s} {v:.6f}")
        print("\nBacktest sample")
        print(backtest_daily.tail(10))

    return summary, backtest_daily

