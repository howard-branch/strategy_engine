import pandas as pd
from config.settings import DatabaseConfig
from data.market_data import MarketData, MarketDataConfig
from features.basic_features import add_basic_features
from signals.momentum import MomentumSignal, MomentumSignalConfig
from signals.base import SignalBase
from portfolio.construct import construct_rank_based_portfolio, PortfolioConfig
from backtest.simulator import simulate_backtest, SimulatorConfig
from reports.performance import performance_summary
from signals.reversal import ReversalSignal, ReversalSignalConfig


def main() -> None:
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

    bars = md.load_bars(
        start_date="2018-01-01",
        end_date="2025-12-31",
        symbols=None,
    )

    bars = add_basic_features(bars)

    momentum_signal = MomentumSignal(
        MomentumSignalConfig(
            lookback_days=120,
            skip_days=5,
            min_history_days=126,
        )
    )

    reversal_signal = ReversalSignal(
        ReversalSignalConfig(
            use_vol_adjust=False,
        )
    )

    mom = momentum_signal.generate(bars).scores
    rev = reversal_signal.generate(bars).scores

    df = mom.merge(rev, on=["date", "symbol"], suffixes=("_mom", "_rev"))

    df["z_mom"] = SignalBase.zscore_by_date(df, "score_mom")
    df["z_rev"] = SignalBase.zscore_by_date(df, "score_rev")

    df["score"] = df["z_mom"] + 0.1 * df["z_rev"]

    combined_scores = df[["date", "symbol", "score"]]
    print(f"Combined scores: {len(combined_scores)} rows, {combined_scores['date'].nunique()} unique dates")

    # Group by week and take last day of each week
    combined_scores["week"] = combined_scores["date"].dt.to_period("W")
    
    # Get the last trading date in each week, then filter to that date
    last_date_per_week = combined_scores.groupby("week")["date"].max()
    weekly_scores = combined_scores[combined_scores["date"].isin(last_date_per_week.values)][["date", "symbol", "score"]].copy()
    
    if len(weekly_scores) == 0:
        print("No data after weekly aggregation")
        return
    
    print(f"Weekly scores: {len(weekly_scores)} rows, {weekly_scores['date'].nunique()} unique dates")
    print(f"Stocks per date - min: {weekly_scores.groupby('date').size().min()}, max: {weekly_scores.groupby('date').size().max()}, mean: {weekly_scores.groupby('date').size().mean():.1f}")

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

    print(f"Weights: {len(weights)} rows, {weights['date'].nunique() if len(weights) > 0 else 0} unique dates")

    if len(weights) == 0:
        print("No positions after portfolio construction. Portfolio requires at least 10 stocks per date.")
        return

    backtest_daily = simulate_backtest(
        weights=weights,
        bars=bars,
        config=SimulatorConfig(cost_per_unit_turnover=0.0005),
    )

    summary = performance_summary(backtest_daily)

    print("\nPerformance summary")
    print("-------------------")
    for k, v in summary.items():
        print(f"{k:20s} {v:.6f}")

    print("\nBacktest sample")
    print(backtest_daily.tail(10))


if __name__ == "__main__":
    main()