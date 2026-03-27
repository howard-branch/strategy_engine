"""
Advanced Parameter Sweep Engine for reversal_ratio optimization.

This module provides a flexible parameter sweep framework to find the optimal
reversal_ratio value based on various performance metrics.
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Dict, Optional
from config.settings import DatabaseConfig
from data.market_data import MarketData, MarketDataConfig
from features.basic_features import add_basic_features
from signals.momentum import MomentumSignal, MomentumSignalConfig
from signals.base import SignalBase
from portfolio.construct import construct_rank_based_portfolio, PortfolioConfig
from backtest.simulator import simulate_backtest, SimulatorConfig
from reports.performance import performance_summary
from signals.reversal import ReversalSignal, ReversalSignalConfig


@dataclass
class SweepConfig:
    """Configuration for parameter sweep."""
    ratio_min: float = 0.0
    ratio_max: float = 1.0
    ratio_step: float = 0.1
    optimization_metric: str = "sharpe"  # "sharpe", "total_return", "ann_return"
    show_details: bool = True


class ReversalRatioSweep:
    """Parameter sweep engine for reversal_ratio optimization."""

    def __init__(self, sweep_config: SweepConfig = SweepConfig()):
        self.config = sweep_config
        self.results = []

    def load_signals(
        self,
        connection_string: str,
        start_date: str = "2018-01-01",
        end_date: str = "2025-12-31",
    ) -> tuple:
        """Load market data and generate momentum/reversal signals."""
        md = MarketData(
            connection_string=connection_string,
            config=MarketDataConfig(
                min_price=5.0,
                min_dollar_volume=5_000_000.0,
                min_history_days=126,
                schema="strategy_engine",
            ),
        )

        print("Loading market data...")
        bars = md.load_bars(
            start_date=start_date,
            end_date=end_date,
            symbols=None,
        )
        bars = add_basic_features(bars)

        print("Generating momentum signal...")
        momentum_signal = MomentumSignal(
            MomentumSignalConfig(
                lookback_days=120,
                skip_days=5,
                min_history_days=126,
            )
        )

        print("Generating reversal signal...")
        reversal_signal = ReversalSignal(
            ReversalSignalConfig(use_vol_adjust=False)
        )

        mom = momentum_signal.generate(bars).scores
        rev = reversal_signal.generate(bars).scores

        return bars, mom, rev

    def run_single_backtest(
        self,
        bars: pd.DataFrame,
        mom: pd.DataFrame,
        rev: pd.DataFrame,
        reversal_ratio: float,
    ) -> Dict:
        """Run a single backtest with a specific reversal_ratio."""
        try:
            # Merge momentum and reversal scores
            df = mom.merge(rev, on=["date", "symbol"], suffixes=("_mom", "_rev"))

            # Z-score normalize
            df["z_mom"] = SignalBase.zscore_by_date(df, "score_mom")
            df["z_rev"] = SignalBase.zscore_by_date(df, "score_rev")

            # Combine scores
            df["score"] = df["z_mom"] + reversal_ratio * df["z_rev"]
            combined_scores = df[["date", "symbol", "score"]]

            # Weekly aggregation
            combined_scores["week"] = combined_scores["date"].dt.to_period("W")
            last_date_per_week = combined_scores.groupby("week")["date"].max()
            weekly_scores = combined_scores[
                combined_scores["date"].isin(last_date_per_week.values)
            ][["date", "symbol", "score"]].copy()

            if len(weekly_scores) == 0:
                return {"reversal_ratio": reversal_ratio, "error": "No weekly data"}

            # Portfolio construction
            config = PortfolioConfig(
                top_n=10,
                entry_n=10,
                exit_n=20,
                gross_long=0.5,
                gross_short=0.5,
                rebalance_frequency="W-FRI",
                min_names_per_side=5,
                allow_flips_same_day=False,
            )
            weights = construct_rank_based_portfolio(weekly_scores, config)

            if len(weights) == 0:
                return {
                    "reversal_ratio": reversal_ratio,
                    "error": "No portfolio positions",
                }

            # Backtest
            backtest_daily = simulate_backtest(
                weights=weights,
                bars=bars,
                config=SimulatorConfig(cost_per_unit_turnover=0.0005),
            )

            # Performance summary
            summary = performance_summary(backtest_daily)
            summary["reversal_ratio"] = reversal_ratio
            return summary

        except Exception as e:
            return {"reversal_ratio": reversal_ratio, "error": str(e)}

    def sweep(
        self,
        bars: pd.DataFrame,
        mom: pd.DataFrame,
        rev: pd.DataFrame,
    ) -> pd.DataFrame:
        """Execute parameter sweep."""
        ratios = np.arange(
            self.config.ratio_min,
            self.config.ratio_max + self.config.ratio_step / 2,
            self.config.ratio_step,
        )

        print(f"\nRunning parameter sweep for reversal_ratio in [{self.config.ratio_min}, {self.config.ratio_max}]")
        print(f"Step size: {self.config.ratio_step}, Optimization metric: {self.config.optimization_metric}")
        print("-" * 80)

        self.results = []
        for i, ratio in enumerate(ratios, 1):
            print(f"\n[{i}/{len(ratios)}] Testing reversal_ratio = {ratio:.2f}...")
            result = self.run_single_backtest(bars, mom, rev, ratio)

            if "error" not in result:
                self.results.append(result)
                if self.config.show_details:
                    print(f"  ✓ Sharpe: {result.get('sharpe', np.nan):.4f}")
                    print(f"    Return: {result.get('annualised_return', np.nan):.4f}")
                    print(f"    Vol: {result.get('annualised_vol', np.nan):.4f}")
            else:
                print(f"  ✗ Error: {result['error']}")

        return pd.DataFrame(self.results)

    def get_best(self) -> Optional[Dict]:
        """Get the best performing parameter combination."""
        if not self.results:
            return None

        df = pd.DataFrame(self.results)
        metric = self.config.optimization_metric

        if metric not in df.columns:
            print(f"Metric '{metric}' not found in results")
            return None

        best_idx = df[metric].idxmax()
        return df.iloc[best_idx].to_dict()

    def print_summary(self):
        """Print detailed summary of results."""
        if not self.results:
            print("No valid results to summarize")
            return

        df = pd.DataFrame(self.results)

        print("\n" + "=" * 100)
        print("Parameter Sweep Results Summary")
        print("=" * 100)
        print(df[["reversal_ratio", "sharpe", "annualised_return", "annualised_vol", "total_return", "max_drawdown", "avg_daily_turnover"]].to_string(index=False))

        best = self.get_best()
        if best:
            metric = self.config.optimization_metric
            print(f"\n✓ Best by {metric.upper()}: reversal_ratio = {best['reversal_ratio']:.2f}")
            print(f"  Sharpe Ratio: {best.get('sharpe', np.nan):.6f}")
            print(f"  Annualised Return: {best.get('annualised_return', np.nan):.6f}")
            print(f"  Annualised Vol: {best.get('annualised_vol', np.nan):.6f}")
            print(f"  Total Return: {best.get('total_return', np.nan):.6f}")
            print(f"  Max Drawdown: {best.get('max_drawdown', np.nan):.6f}")
            print(f"  Avg Daily Turnover: {best.get('avg_daily_turnover', np.nan):.6f}")

    def save_results(self, filename: str = "reversal_ratio_sweep_results.csv"):
        """Save results to CSV."""
        if not self.results:
            print("No results to save")
            return

        df = pd.DataFrame(self.results)
        df.to_csv(filename, index=False)
        print(f"Results saved to {filename}")


def main():
    """Main execution function."""
    # Configuration
    sweep_config = SweepConfig(
        ratio_min=0.0,
        ratio_max=1.0,
        ratio_step=0.1,
        optimization_metric="sharpe",
        show_details=True,
    )

    # Initialize sweep engine
    sweep = ReversalRatioSweep(sweep_config)

    # Load data and signals
    connection_string = DatabaseConfig.get_connection_string()
    bars, mom, rev = sweep.load_signals(connection_string)

    # Run parameter sweep
    results_df = sweep.sweep(bars, mom, rev)

    # Print summary and save results
    sweep.print_summary()
    sweep.save_results()


if __name__ == "__main__":
    main()

