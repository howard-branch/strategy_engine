from dataclasses import dataclass
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


@dataclass
class MarketDataConfig:
    min_price: float = 5.0
    min_dollar_volume: float = 10_000_000.0
    min_history_days: int = 126
    schema: str = "strategy_engine"


class MarketData:
    REQUIRED_COLUMNS = [
        "date",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
    ]

    def __init__(
            self,
            connection_string: str,
            config: Optional[MarketDataConfig] = None,
    ):
        self.config = config or MarketDataConfig()
        self.engine: Engine = create_engine(connection_string)

    def load_bars(
            self,
            start_date: str,
            end_date: str,
            symbols: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        schema = self.config.schema

        sql = f"""
            SELECT
                b.trade_date AS date,
                i.symbol,
                b.open,
                b.high,
                b.low,
                b.close,
                b.adj_close,
                b.volume
            FROM {schema}.daily_bars b
            JOIN {schema}.instruments i
              ON i.instrument_id = b.instrument_id
            WHERE b.trade_date >= :start_date
              AND b.trade_date <= :end_date
              AND i.is_active = true
        """

        params: dict = {
            "start_date": start_date,
            "end_date": end_date,
        }

        if symbols:
            sql += " AND i.symbol = ANY(:symbols)"
            params["symbols"] = symbols

        sql += " ORDER BY i.symbol, b.trade_date"

        with self.engine.connect() as conn:
            df = pd.read_sql(text(sql), conn, params=params)

        df = self.clean_bars(df)
        df = self.build_universe(df)
        return df

    def clean_bars(self, df: pd.DataFrame) -> pd.DataFrame:
        self._validate_columns(df)

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])

        numeric_cols = ["open", "high", "low", "close", "adj_close", "volume"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = (
            df.sort_values(["symbol", "date"])
            .drop_duplicates(subset=["symbol", "date"], keep="last")
            .reset_index(drop=True)
        )

        df = df.dropna(subset=["date", "symbol", "close", "adj_close", "volume"])
        df = df[df["close"] > 0]
        df = df[df["adj_close"] > 0]
        df = df[df["volume"] >= 0]

        df["dollar_volume"] = df["close"] * df["volume"]
        df["ret_1d"] = df.groupby("symbol", sort=False)["adj_close"].pct_change()

        return df.reset_index(drop=True)

    def build_universe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        enough_price = df["close"] > self.config.min_price
        enough_liquidity = df["dollar_volume"] > self.config.min_dollar_volume

        history_count = df.groupby("symbol", sort=False).cumcount() + 1
        enough_history = history_count >= self.config.min_history_days

        df["tradable"] = enough_price & enough_liquidity & enough_history
        return df

    def _validate_columns(self, df: pd.DataFrame) -> None:
        missing = [c for c in self.REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")