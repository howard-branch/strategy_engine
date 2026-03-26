from pathlib import Path
import sys

# Allow running this file directly: `python experiments/test_load_bars.py`.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data.market_data import MarketData, MarketDataConfig
from config.settings import DatabaseConfig


def main() -> None:
    # Load configuration from environment variables
    connection_string = DatabaseConfig.get_connection_string()
    
    md = MarketData(
        connection_string=connection_string,
        config=MarketDataConfig(
            min_price=5.0,
            min_dollar_volume=10_000_000.0,
            min_history_days=126,
            schema=DatabaseConfig.get_schema(),
        ),
    )

    df = md.load_bars(
        start_date="2020-01-01",
        end_date="2020-12-31",
        symbols=["SPY", "QQQ", "IWM"],
    )

    print(df.head(20))
    print(df.dtypes)
    print(df[["date", "symbol", "close", "adj_close", "volume", "dollar_volume", "tradable"]].tail(20))
    print(df["symbol"].value_counts())


if __name__ == "__main__":
    main()
