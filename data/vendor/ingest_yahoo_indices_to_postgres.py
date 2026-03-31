#!/usr/bin/env python3
"""
Ingest market-level index data (VIX, etc.) from Yahoo Finance into Postgres.

This script is intentionally generic: pass any Yahoo ticker via --symbols.
The default is ^VIX (CBOE Volatility Index).

Usage:
    # Download VIX history (full backfill on first run, incremental after)
    python data/vendor/ingest_yahoo_indices_to_postgres.py

    # Explicit symbols and date range
    python data/vendor/ingest_yahoo_indices_to_postgres.py \
        --symbols ^VIX ^VIX3M ^TNX \
        --start 2004-01-01

    # Force full overwrite
    python data/vendor/ingest_yahoo_indices_to_postgres.py --overwrite
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import psycopg
import yfinance as yf
import pandas as pd

# ── project imports ───────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))

from sharadar_common import load_env

# ── defaults ──────────────────────────────────────────────────────────────────
DEFAULT_SYMBOLS = ["^VIX"]
DEFAULT_START = "2004-01-02"          # VIX history starts around here on Yahoo
DEFAULT_SOURCE = "yahoo"


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Yahoo Finance index data into Postgres"
    )
    parser.add_argument(
        "--symbols", nargs="+", default=DEFAULT_SYMBOLS,
        help="Yahoo tickers to download (default: ^VIX)",
    )
    parser.add_argument("--start", default=DEFAULT_START, help="Backfill start date")
    parser.add_argument("--schema", default=os.getenv("DB_SCHEMA", "strategy_engine"))
    parser.add_argument("--overwrite", action="store_true",
                        help="Drop and re-download all history")
    parser.add_argument("--batch-size", type=int, default=5_000)
    return parser.parse_args()


# ── DDL ───────────────────────────────────────────────────────────────────────

def ensure_table(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.market_indices (
                symbol       TEXT    NOT NULL,
                trade_date   DATE    NOT NULL,
                open         NUMERIC(20, 6),
                high         NUMERIC(20, 6),
                low          NUMERIC(20, 6),
                close        NUMERIC(20, 6),
                adj_close    NUMERIC(20, 6),
                volume       BIGINT,
                source       TEXT    NOT NULL DEFAULT 'yahoo',
                created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (symbol, trade_date)
            )
        """)
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS market_indices_trade_date_idx "
            f"ON {schema}.market_indices (trade_date)"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS market_indices_symbol_date_idx "
            f"ON {schema}.market_indices (symbol, trade_date)"
        )
    conn.commit()


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_max_date(conn: psycopg.Connection, schema: str, symbol: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT MAX(trade_date) FROM {schema}.market_indices "
            f"WHERE symbol = %s",
            (symbol,),
        )
        result = cur.fetchone()[0]
        return str(result) if result else None


def delete_symbol(conn: psycopg.Connection, schema: str, symbol: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {schema}.market_indices WHERE symbol = %s",
            (symbol,),
        )
        n = cur.rowcount
    conn.commit()
    return n


def upsert_rows(
    conn: psycopg.Connection,
    schema: str,
    rows: list[tuple],
    batch_size: int = 5_000,
) -> int:
    """Upsert rows into market_indices.  Returns total rows upserted."""
    sql = f"""
        INSERT INTO {schema}.market_indices
            (symbol, trade_date, open, high, low, close, adj_close, volume, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, trade_date)
        DO UPDATE SET
            open       = EXCLUDED.open,
            high       = EXCLUDED.high,
            low        = EXCLUDED.low,
            close      = EXCLUDED.close,
            adj_close  = EXCLUDED.adj_close,
            volume     = EXCLUDED.volume,
            source     = EXCLUDED.source,
            updated_at = NOW()
    """
    total = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            cur.executemany(sql, batch)
            total += len(batch)
    conn.commit()
    return total


# ── Yahoo download ────────────────────────────────────────────────────────────

def download_yahoo(
    symbol: str,
    start_date: str,
    end_date: str,
) -> list[tuple]:
    """Download daily OHLCV from Yahoo and return list of row tuples."""
    print(f"  Downloading {symbol} from {start_date} to {end_date} ...")

    df = yf.download(
        tickers=symbol,
        start=start_date,
        end=end_date,
        interval="1d",
        auto_adjust=False,
        actions=False,
        progress=False,
        repair=False,
        threads=False,
    )

    if df is None or df.empty:
        print(f"  [warn] No data returned for {symbol}")
        return []

    # Flatten MultiIndex columns (yfinance quirk with single ticker)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    df = df.reset_index()

    # Normalise column names
    date_col = "Date" if "Date" in df.columns else df.columns[0]
    rename_map = {
        date_col: "trade_date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    }
    df = df.rename(columns=rename_map)

    if "adj_close" not in df.columns:
        df["adj_close"] = df["close"]

    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    rows: list[tuple] = []
    for _, r in df.iterrows():
        rows.append((
            symbol,
            r["trade_date"],
            _safe_float(r.get("open")),
            _safe_float(r.get("high")),
            _safe_float(r.get("low")),
            _safe_float(r.get("close")),
            _safe_float(r.get("adj_close")),
            _safe_int(r.get("volume")),
            DEFAULT_SOURCE,
        ))
    return rows


def _safe_float(v) -> float | None:
    if v is None or pd.isna(v):
        return None
    return float(v)


def _safe_int(v) -> int | None:
    if v is None or pd.isna(v):
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    load_env()
    args = parse_args()

    db_dsn = os.getenv("DATABASE_URL") or os.getenv("DB_DSN")
    if not db_dsn:
        print("[error] Missing DATABASE_URL (or DB_DSN) in .env or environment")
        return 1

    schema = args.schema
    symbols = args.symbols
    overwrite = args.overwrite
    batch_size = args.batch_size
    backfill_start = args.start
    end_date = str(date.today())

    print(f"{'=' * 60}")
    print(f"  Yahoo index ingest — {len(symbols)} symbol(s)")
    print(f"  Schema: {schema}")
    print(f"  Symbols: {symbols}")
    print(f"  Overwrite: {overwrite}")
    print(f"{'=' * 60}")

    conn = psycopg.connect(db_dsn)
    try:
        ensure_table(conn, schema)

        for symbol in symbols:
            print(f"\n── {symbol} ──")

            if overwrite:
                n = delete_symbol(conn, schema, symbol)
                print(f"  Deleted {n} existing rows (overwrite mode)")
                start = backfill_start
            else:
                max_dt = get_max_date(conn, schema, symbol)
                if max_dt:
                    # Re-fetch last 3 days to catch any Yahoo revisions
                    start = str(
                        date.fromisoformat(max_dt) - timedelta(days=3)
                    )
                    print(f"  Incremental from {start} (max date was {max_dt})")
                else:
                    start = backfill_start
                    print(f"  Full backfill from {start}")

            rows = download_yahoo(symbol, start, end_date)
            if not rows:
                print(f"  No rows to load for {symbol}")
                continue

            n = upsert_rows(conn, schema, rows, batch_size)
            print(f"  Upserted {n} rows for {symbol}")

    finally:
        conn.close()

    print(f"\n{'=' * 60}")
    print("  Done.")
    print(f"{'=' * 60}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

