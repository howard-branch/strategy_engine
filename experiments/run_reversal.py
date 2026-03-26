from backtest.simulator import simulate_backtest, SimulatorConfig
from config.settings import DatabaseConfig
from data.market_data import MarketData, MarketDataConfig
from features.basic_features import add_basic_features
from portfolio.construct import construct_rank_based_portfolio, PortfolioConfig
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

    signal = ReversalSignal(
        ReversalSignalConfig(
            use_vol_adjust=False,
        )
    )
    signal_output = signal.generate(bars)

    weights = construct_rank_based_portfolio(
        signal_output.scores,
        PortfolioConfig(
            top_n=5,
            gross_long=0.5,
            gross_short=0.5,
        ),
    )

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