from __future__ import annotations

from pathlib import Path
import sys

# Allow running this file directly: `python data/load_history.py`.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dataclasses import dataclass
from typing import Iterable, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from data.vendor.yahoo import YahooHistoryClient
from config.settings import DatabaseConfig


@dataclass
class LoadHistoryConfig:
    schema: str = "strategy_engine"
    default_asset_type: str = "STOCK"


class HistoryIngestor:
    def __init__(self, connection_string: str, config: Optional[LoadHistoryConfig] = None):
        self.engine: Engine = create_engine(connection_string)
        self.config = config or LoadHistoryConfig()

    def upsert_instrument(
            self,
            symbol: str,
            asset_type: str,
            name: str | None = None,
            exchange: str | None = None,
            sector: str | None = None,
    ) -> None:
        schema = self.config.schema
        sql = text(f"""
            INSERT INTO {schema}.instruments (
                symbol, asset_type, name, exchange, sector, is_active
            )
            VALUES (
                :symbol, :asset_type, :name, :exchange, :sector, true
            )
            ON CONFLICT (symbol) DO UPDATE
            SET
                asset_type = EXCLUDED.asset_type,
                name = COALESCE(EXCLUDED.name, {schema}.instruments.name),
                exchange = COALESCE(EXCLUDED.exchange, {schema}.instruments.exchange),
                sector = COALESCE(EXCLUDED.sector, {schema}.instruments.sector),
                is_active = true
        """)
        with self.engine.begin() as conn:
            conn.execute(
                sql,
                {
                    "symbol": symbol,
                    "asset_type": asset_type,
                    "name": name,
                    "exchange": exchange,
                    "sector": sector,
                },
            )

    def get_instrument_id(self, symbol: str) -> int:
        schema = self.config.schema
        sql = text(f"""
            SELECT instrument_id
            FROM {schema}.instruments
            WHERE symbol = :symbol
        """)
        with self.engine.begin() as conn:
            row = conn.execute(sql, {"symbol": symbol}).fetchone()

        if row is None:
            raise ValueError(f"No instrument_id found for symbol={symbol}")
        return int(row[0])

    def upsert_daily_bars(self, instrument_id: int, bars: pd.DataFrame) -> int:
        schema = self.config.schema
        if bars.empty:
            return 0

        sql = text(f"""
            INSERT INTO {schema}.daily_bars (
                instrument_id, trade_date, open, high, low, close, adj_close, volume
            )
            VALUES (
                :instrument_id, :trade_date, :open, :high, :low, :close, :adj_close, :volume
            )
            ON CONFLICT (instrument_id, trade_date) DO UPDATE
            SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                adj_close = EXCLUDED.adj_close,
                volume = EXCLUDED.volume
        """)

        records = []
        for row in bars.itertuples(index=False):
            records.append(
                {
                    "instrument_id": instrument_id,
                    "trade_date": pd.Timestamp(row.date).date(),
                    "open": float(row.open),
                    "high": float(row.high),
                    "low": float(row.low),
                    "close": float(row.close),
                    "adj_close": float(row.adj_close),
                    "volume": int(row.volume),
                }
            )

        with self.engine.begin() as conn:
            conn.execute(sql, records)

        return len(records)


def infer_asset_type(symbol: str) -> str:
    etfs = {
        "SPY", "QQQ", "IWM", "DIA", "VTI", "IVV", "VOO",
        "XLF", "XLK", "XLE", "XLV", "XLY", "XLI", "XLP", "XLB", "XLU",
        "SMH", "ARKK", "TLT", "GLD"
    }
    return "ETF" if symbol.upper() in etfs else "STOCK"


def main() -> None:
    connection_string = DatabaseConfig.get_connection_string()

    symbols: list[str] = [
        "SPY", "QQQ", "IWM", "DIA",
        "XLK", "XLF", "XLE", "XLV",
        "AAPL", "MSFT", "AMZN", "NVDA", "JPM", "XOM", "JNJ",
    ]

    start_date = "2018-01-01"
    end_date = "2026-01-01"

    client = YahooHistoryClient()
    ingestor = HistoryIngestor(connection_string=connection_string)

    for symbol in symbols:
        asset_type = infer_asset_type(symbol)
        ingestor.upsert_instrument(symbol=symbol, asset_type=asset_type)

        instrument_id = ingestor.get_instrument_id(symbol)
        bars = client.fetch_daily_bars(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )
        row_count = ingestor.upsert_daily_bars(instrument_id=instrument_id, bars=bars)
        print(f"{symbol}: loaded {row_count} rows")


if __name__ == "__main__":
    main()