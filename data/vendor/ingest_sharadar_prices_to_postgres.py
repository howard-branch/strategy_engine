#!/usr/bin/env python3
"""
Ingest SHARADAR/SEP (equity prices) or SHARADAR/SFP (fund prices) into Postgres.

Both datasets share identical column structure (OHLCV + adjusted close) and
write to the same ``daily_bars`` table, differentiated by ``source_table``.

Usage:
    python ingest_sharadar_prices_to_postgres.py                    # default: SEP
    python ingest_sharadar_prices_to_postgres.py --table SHARADAR/SFP
"""
from __future__ import annotations

import csv
import io
import sys
from pathlib import Path
from typing import Iterator

import psycopg

# Ensure vendor dir is importable when run as a standalone script
sys.path.insert(0, str(Path(__file__).resolve().parent))

from sharadar_common import (
    IngestConfig,
    chunked,
    copy_rows,
    ensure_instruments_table,
    normalise_header,
    parse_decimal,
    parse_ingest_args,
    parse_int,
    resolve_credentials,
    row_get,
    run_ingest,
)

# ── Dataset config ────────────────────────────────────────────────────────────

_DATASETS = {
    "SHARADAR/SEP": {"asset_type": "STOCK", "desc": "equity daily prices"},
    "SHARADAR/SFP": {"asset_type": "FUND", "desc": "fund daily prices"},
}

REQUIRED_PRICE_COLS: set[str] = {"ticker", "date", "close"}
KNOWN_PRICE_COLS: set[str] = {
    "ticker", "date", "open", "high", "low", "close",
    "volume", "closeadj", "closeunadj", "lastupdated",
}


# ── Schema / DDL ─────────────────────────────────────────────────────────────

def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    ensure_instruments_table(conn, schema)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.daily_bars (
                instrument_id BIGINT NOT NULL
                    REFERENCES {schema}.instruments(instrument_id),
                trade_date DATE NOT NULL,
                open NUMERIC(30, 6),
                high NUMERIC(30, 6),
                low NUMERIC(30, 6),
                close NUMERIC(30, 6),
                adj_close NUMERIC(30, 6),
                volume BIGINT,
                source_table TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (instrument_id, trade_date)
            )
            """
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS daily_bars_trade_date_idx "
            f"ON {schema}.daily_bars (trade_date)"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS daily_bars_instrument_date_idx "
            f"ON {schema}.daily_bars (instrument_id, trade_date)"
        )
    conn.commit()


# ── Row parsing ──────────────────────────────────────────────────────────────

def _bar_row_to_tuple(
    row: dict[str, str], default_source_table: str,
) -> tuple | None:
    symbol = row_get(row, "ticker", "symbol")
    trade_date = row_get(row, "date", "trade_date")
    if not symbol or not trade_date:
        return None

    close = parse_decimal(row_get(row, "close"))
    if close is None:
        return None

    adj_close = parse_decimal(
        row_get(row, "closeadj", "adj_close", "adjusted_close", "close_adjusted")
    )
    if adj_close is None:
        adj_close = close

    return (
        symbol,
        trade_date,
        parse_decimal(row_get(row, "open")),
        parse_decimal(row_get(row, "high")),
        parse_decimal(row_get(row, "low")),
        close,
        adj_close,
        parse_int(row_get(row, "volume")),
        row_get(row, "table") or default_source_table,
    )


# ── Callables wired per table_code ───────────────────────────────────────────

_STAGING_COLUMNS = [
    "symbol", "trade_date", "open", "high", "low",
    "close", "adj_close", "volume", "source_table",
]


def _build_callbacks(table_code: str, asset_type: str):  # noqa: C901
    """Return the dataset-specific callables for *table_code*."""

    def get_max_trade_date(conn: psycopg.Connection, schema: str) -> str | None:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT MAX(trade_date) FROM {schema}.daily_bars "
                f"WHERE source_table = %s",
                (table_code,),
            )
            result = cur.fetchone()[0]
            return str(result) if result else None

    def _create_staging_table(conn: psycopg.Connection) -> None:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS stg_bars")
            cur.execute(
                """
                CREATE TEMP TABLE stg_bars (
                    symbol TEXT,
                    trade_date DATE,
                    open NUMERIC(30, 6),
                    high NUMERIC(30, 6),
                    low NUMERIC(30, 6),
                    close NUMERIC(30, 6),
                    adj_close NUMERIC(30, 6),
                    volume BIGINT,
                    source_table TEXT
                ) ON COMMIT PRESERVE ROWS
                """
            )

    def _upsert(conn: psycopg.Connection, schema: str) -> None:
        with conn.cursor() as cur:
            # instruments
            cur.execute(
                f"""
                INSERT INTO {schema}.instruments
                    (symbol, asset_type, source_table, is_active, updated_at)
                SELECT DISTINCT s.symbol, %(at)s, %(tc)s, TRUE, NOW()
                FROM stg_bars s WHERE s.symbol IS NOT NULL
                ON CONFLICT (symbol) DO UPDATE
                SET updated_at = NOW(),
                    source_table = COALESCE(
                        {schema}.instruments.source_table,
                        EXCLUDED.source_table
                    )
                """,
                {"at": asset_type, "tc": table_code},
            )
            # daily_bars
            cur.execute(
                f"""
                INSERT INTO {schema}.daily_bars
                    (instrument_id, trade_date, open, high, low, close,
                     adj_close, volume, source_table, updated_at)
                SELECT DISTINCT ON (i.instrument_id, s.trade_date)
                    i.instrument_id, s.trade_date,
                    s.open, s.high, s.low, s.close, s.adj_close,
                    s.volume, s.source_table, NOW()
                FROM stg_bars s
                JOIN {schema}.instruments i ON i.symbol = s.symbol
                WHERE s.symbol IS NOT NULL AND s.trade_date IS NOT NULL
                ORDER BY i.instrument_id, s.trade_date
                ON CONFLICT (instrument_id, trade_date) DO UPDATE SET
                    open         = EXCLUDED.open,
                    high         = EXCLUDED.high,
                    low          = EXCLUDED.low,
                    close        = EXCLUDED.close,
                    adj_close    = EXCLUDED.adj_close,
                    volume       = EXCLUDED.volume,
                    source_table = EXCLUDED.source_table,
                    updated_at   = NOW()
                """
            )
        conn.commit()

    def stage_and_upsert(
        conn: psycopg.Connection,
        rows_iter: Iterator[tuple],
        config: IngestConfig,
    ) -> int:
        _create_staging_table(conn)
        total = 0
        for batch in chunked(rows_iter, config.batch_size):
            copied = copy_rows(conn, "stg_bars", _STAGING_COLUMNS, batch)
            total += copied
            print(f"[info] staged {copied:,} bar rows (running total {total:,})")
        if total == 0:
            print(f"[warn] zero rows staged for {table_code}")
        else:
            print(f"[info] total staged: {total:,} bar rows")
        _upsert(conn, config.schema)
        return total

    def iter_rows_csv(csv_file: io.TextIOBase) -> Iterator[tuple]:
        reader = csv.DictReader(csv_file)
        reader.fieldnames = [
            normalise_header(x) for x in (reader.fieldnames or [])
        ]
        debug_printed = False
        for row in reader:
            if not debug_printed:
                print(f"[debug] CSV columns: {reader.fieldnames}")
                debug_printed = True
            result = _bar_row_to_tuple(row, table_code)
            if result is not None:
                yield result

    def iter_rows_dicts(rows: list[dict[str, str]]) -> Iterator[tuple]:
        for row in rows:
            result = _bar_row_to_tuple(row, table_code)
            if result is not None:
                yield result

    def truncate_target(conn: psycopg.Connection, schema: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {schema}.daily_bars WHERE source_table = %s",
                (table_code,),
            )
        conn.commit()
        print(f"[info] deleted {table_code} rows from {schema}.daily_bars")

    def count_target(conn: psycopg.Connection, schema: str) -> int:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {schema}.daily_bars "
                f"WHERE source_table = %s",
                (table_code,),
            )
            return cur.fetchone()[0]

    return dict(
        get_max_date_fn=get_max_trade_date,
        stage_and_upsert_fn=stage_and_upsert,
        iter_rows_csv_fn=iter_rows_csv,
        iter_rows_dicts_fn=iter_rows_dicts,
        truncate_fn=truncate_target,
        count_fn=count_target,
    )


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> int:
    args = parse_ingest_args(
        "Ingest SHARADAR/SEP or SHARADAR/SFP into Postgres",
        extra_args=lambda p: p.add_argument(
            "--table",
            choices=list(_DATASETS),
            default="SHARADAR/SEP",
            help="Which price dataset to ingest (default: SHARADAR/SEP)",
        ),
    )
    table_code = args.table
    ds = _DATASETS[table_code]
    api_key, db_dsn = resolve_credentials()
    config = IngestConfig(
        api_key=api_key,
        db_dsn=db_dsn,
        schema=args.schema,
        batch_size=args.batch_size,
        overwrite=args.overwrite,
    )

    return run_ingest(
        table_code=table_code,
        required_cols=REQUIRED_PRICE_COLS,
        known_cols=KNOWN_PRICE_COLS,
        date_filter_col="date",
        ensure_schema_fn=ensure_schema,
        config=config,
        **_build_callbacks(table_code, ds["asset_type"]),
    )


if __name__ == "__main__":
    raise SystemExit(main())

