import pandas as pd
import numpy as np
from config.settings import DatabaseConfig
from data.market_data import MarketData, MarketDataConfig
from features.basic_features import add_basic_features
from signals.momentum import MomentumSignal, MomentumSignalConfig
from signals.base import SignalBase
from portfolio.construct import construct_rank_based_portfolio, PortfolioConfig
from backtest.simulator import simulate_backtest, SimulatorConfig
from reports.performance import performance_summary
from signals.reversal import ReversalSignal, ReversalSignalConfig


def run_backtest_with_reversal_ratio(
    bars: pd.DataFrame,
    mom: pd.DataFrame,
    rev: pd.DataFrame,
    reversal_ratio: float,
) -> dict:
    """
    Run backtest with a specific reversal_ratio value.
    
    Returns a dict with performance metrics and the ratio used.
    """
    # Merge momentum and reversal scores
    df = mom.merge(rev, on=["date", "symbol"], suffixes=("_mom", "_rev"))

    # Z-score normalize
    df["z_mom"] = SignalBase.zscore_by_date(df, "score_mom")
    df["z_rev"] = SignalBase.zscore_by_date(df, "score_rev")

    # Combine scores with reversal ratio
    df["score"] = df["z_mom"] + reversal_ratio * df["z_rev"]

    combined_scores = df[["date", "symbol", "score"]]

    # Group by week and take last day of each week
    combined_scores["week"] = combined_scores["date"].dt.to_period("W")
    last_date_per_week = combined_scores.groupby("week")["date"].max()
    weekly_scores = combined_scores[combined_scores["date"].isin(last_date_per_week.values)][
        ["date", "symbol", "score"]
    ].copy()

    if len(weekly_scores) == 0:
        return {"reversal_ratio": reversal_ratio, "error": "No data after weekly aggregation"}

    # Construct portfolio
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
            "error": "No positions after portfolio construction",
        }

    # Run backtest
    backtest_daily = simulate_backtest(
        weights=weights,
        bars=bars,
        config=SimulatorConfig(cost_per_unit_turnover=0.0005),
    )

    # Get performance metrics
    summary = performance_summary(backtest_daily)
    summary["reversal_ratio"] = reversal_ratio

    return summary


def main() -> None:
    # Load data (once, for all parameter combinations)
    connection_string = DatabaseConfig.get_connection_string()

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
        start_date="2018-01-01",
        end_date="2025-12-31",
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
        ReversalSignalConfig(
            use_vol_adjust=False,
        )
    )

    mom = momentum_signal.generate(bars).scores
    rev = reversal_signal.generate(bars).scores

    # Parameter sweep
    reversal_ratios = np.arange(0.0, 1.1, 0.1)
    print(f"\nRunning parameter sweep for reversal_ratio in {reversal_ratios}")
    print("-" * 80)

    results = []

    for ratio in reversal_ratios:
        print(f"\nTesting reversal_ratio = {ratio:.2f}...")
        result = run_backtest_with_reversal_ratio(bars, mom, rev, ratio)

        if "error" not in result:
            results.append(result)
            print(f"  Sharpe Ratio: {result.get('sharpe', np.nan):.4f}")
            print(f"  Total Return: {result.get('total_return', np.nan):.4f}")
            print(f"  Ann. Vol: {result.get('annualised_vol', np.nan):.4f}")
        else:
            print(f"  Error: {result['error']}")

    # Create results dataframe and find best
    results_df = pd.DataFrame(results)

    print("\n" + "=" * 80)
    print("Parameter Sweep Results")
    print("=" * 80)
    print(results_df.to_string(index=False))

    # Find best by Sharpe ratio
    if "sharpe" in results_df.columns:
        best_idx = results_df["sharpe"].idxmax()
        best_result = results_df.loc[best_idx]
        print(f"\n✓ Best reversal_ratio by Sharpe Ratio: {best_result['reversal_ratio']:.2f}")
        print(f"  Sharpe Ratio: {best_result['sharpe']:.6f}")
        print(f"  Annualised Return: {best_result['annualised_return']:.6f}")
        print(f"  Annualised Vol: {best_result['annualised_vol']:.6f}")
        print(f"  Total Return: {best_result['total_return']:.6f}")
        print(f"  Max Drawdown: {best_result['max_drawdown']:.6f}")
        print(f"  Avg Daily Turnover: {best_result['avg_daily_turnover']:.6f}")

    # Save results
    results_df.to_csv("reversal_ratio_sweep_results.csv", index=False)
    print(f"\nResults saved to reversal_ratio_sweep_results.csv")


if __name__ == "__main__":
    main()

