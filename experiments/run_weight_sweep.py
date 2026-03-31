"""
Weight-sweep experiment.

Sweeps over a grid of signal weights and records performance for each
combination.  Results are saved to CSV and the best combo (by Sharpe) is
printed.

Usage
-----
    python -m experiments.run_weight_sweep          # default grid
    python -m experiments.run_weight_sweep --help   # see options

The default grid varies each signal weight in [0.0, 0.5, 1.0] — that's
3^6 = 729 combos for 6 signals.  Narrow the grid or select a subset of
signals to speed things up.
"""

from __future__ import annotations

import argparse
import itertools
from dataclasses import dataclass, field

import pandas as pd

from experiments.runner import ExperimentConfig, load_bars, run_signal
from signals import list_signals, make_signal
from signals.combiner import CombinedSignal, CombinedSignalConfig


@dataclass
class SweepConfig:
    """Defines the weight grid to sweep."""

    signal_names: list[str] = field(default_factory=list_signals)
    weight_values: list[float] = field(default_factory=lambda: [0.0, 0.5, 1.0])
    output_csv: str = "weight_sweep_results.csv"
    optimise_metric: str = "sharpe"


def _build_grid(cfg: SweepConfig) -> list[dict[str, float]]:
    """Cartesian product of weights — skip the all-zero combo."""
    combos: list[dict[str, float]] = []
    for wts in itertools.product(cfg.weight_values, repeat=len(cfg.signal_names)):
        if all(w == 0.0 for w in wts):
            continue
        combos.append(dict(zip(cfg.signal_names, wts)))
    return combos


def run_weight_sweep(
    sweep_cfg: SweepConfig | None = None,
    experiment_cfg: ExperimentConfig | None = None,
) -> pd.DataFrame:
    """
    Execute the full sweep and return a results DataFrame.

    Each row contains the weight vector plus the performance metrics
    (total_return, annualised_return, annualised_vol, sharpe,
    max_drawdown, avg_daily_turnover).
    """
    sweep_cfg = sweep_cfg or SweepConfig()
    experiment_cfg = experiment_cfg or ExperimentConfig()

    print("Loading bars (once) ...")
    bars = load_bars(experiment_cfg)

    grid = _build_grid(sweep_cfg)
    print(f"Sweep grid: {len(grid)} combinations over {sweep_cfg.signal_names}")

    # Pre-build all signal instances (they're lightweight)
    signal_instances = {name: make_signal(name) for name in sweep_cfg.signal_names}

    rows: list[dict] = []
    for i, weights in enumerate(grid, 1):
        label = " | ".join(f"{k}={v:.2f}" for k, v in weights.items())
        print(f"[{i}/{len(grid)}] {label}", end=" … ", flush=True)

        # Only include signals with non-zero weight
        active = {k: v for k, v in weights.items() if v != 0.0}
        active_signals = {k: signal_instances[k] for k in active}

        combined = CombinedSignal(
            signals=active_signals,
            config=CombinedSignalConfig(weights=active),
        )

        try:
            summary, _ = run_signal(
                signal=combined,
                bars=bars,
                portfolio_config=experiment_cfg.portfolio_config,
                simulator_config=experiment_cfg.simulator_config,
            )
            row = {**weights, **summary}
            sharpe = summary.get("sharpe", float("nan"))
            print(f"sharpe={sharpe:.4f}")
        except Exception as exc:
            row = {**weights, "error": str(exc)}
            print(f"ERROR: {exc}")

        rows.append(row)

    results = pd.DataFrame(rows)

    # Sort by optimisation metric descending
    metric = sweep_cfg.optimise_metric
    if metric in results.columns:
        results = results.sort_values(metric, ascending=False).reset_index(drop=True)

    # Save
    results.to_csv(sweep_cfg.output_csv, index=False)
    print(f"\nResults saved to {sweep_cfg.output_csv}")

    # Print best
    if metric in results.columns and not results[metric].isna().all():
        best = results.iloc[0]
        print(f"\n{'='*60}")
        print(f"Best by {metric}: {best[metric]:.6f}")
        print("-" * 60)
        for name in sweep_cfg.signal_names:
            print(f"  {name:20s} weight = {best[name]:.2f}")
        for k in ("total_return", "annualised_return", "annualised_vol",
                   "sharpe", "max_drawdown", "avg_daily_turnover"):
            if k in best:
                print(f"  {k:20s} {best[k]:.6f}")
        print("=" * 60)

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Signal weight sweep")
    parser.add_argument(
        "--signals",
        nargs="*",
        default=None,
        help=f"Signals to include (default: all). Choices: {list_signals()}",
    )
    parser.add_argument(
        "--weights",
        nargs="*",
        type=float,
        default=[0.0, 0.5, 1.0],
        help="Weight values to sweep (default: 0.0 0.5 1.0)",
    )
    parser.add_argument(
        "--metric",
        default="sharpe",
        help="Metric to optimise (default: sharpe)",
    )
    parser.add_argument(
        "--output",
        default="weight_sweep_results.csv",
        help="Output CSV path",
    )
    args = parser.parse_args()

    sweep_cfg = SweepConfig(
        signal_names=args.signals or list_signals(),
        weight_values=args.weights,
        output_csv=args.output,
        optimise_metric=args.metric,
    )
    run_weight_sweep(sweep_cfg)


if __name__ == "__main__":
    main()

