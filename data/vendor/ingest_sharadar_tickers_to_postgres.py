#!/usr/bin/env python3
"""Ingest SHARADAR/TICKERS (instrument metadata) into Postgres."""
from __future__ import annotations

import csv
import io
import sys
from pathlib import Path
from typing import Iterator

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent))

from sharadar_common import (
    IngestConfig,
    copy_rows,
    ensure_instruments_table,
    normalise_header,
    parse_bool,
    parse_ingest_args,
    resolve_credentials,
    row_get,
    run_ingest,
)

TABLE_CODE = "SHARADAR/TICKERS"
REQUIRED_TICKERS_COLS: set[str] = {"ticker", "name", "exchange", "isdelisted"}
KNOWN_TICKERS_COLS: set[str] = {
    "table", "permaticker", "ticker", "name", "exchange",
    "isdelisted", "category", "cusips",
    "siccode", "sicsector", "sicindustry",
    "famasector", "famaindustry",
    "sector", "industry",
    "scalemarketcap", "scalerevenue",
    "relatedtickers", "currency", "location",
    "lastupdated", "firstadded", "firstpricedate", "lastpricedate",
    "firstquarter", "lastquarter",
    "secfilings", "companysite",
}

_STAGING_COLUMNS = [
    "symbol", "asset_type", "name", "exchange",
    "instrument_type", "source_table", "category", "is_active",
]


# ── Schema / DDL ─────────────────────────────────────────────────────────────

def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    ensure_instruments_table(conn, schema)


def get_max_lastupdated(conn: psycopg.Connection, schema: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(updated_at::date) FROM {schema}.instruments")
        result = cur.fetchone()[0]
        return str(result) if result else None


# ── Row parsing ──────────────────────────────────────────────────────────────

def _infer_asset_type(source_table, instrument_type, category):
    st = (source_table or "").upper()
    it = (instrument_type or "").upper()
    cat = (category or "").upper()
    if st == "SFP":
        return "FUND"
    if st in {"SEP", "DAILY"}:
        return "STOCK"
    if "ETF" in it or "ETF" in cat:
        return "ETF"
    if "FUND" in it or "FUND" in cat:
        return "FUND"
    return "STOCK"


def _ticker_row_to_tuple(row: dict[str, str]) -> tuple | None:
    symbol = row_get(row, "ticker", "symbol")
    if not symbol:
        return None
    name = row_get(row, "name")
    exchange = row_get(row, "exchange")
    source_table = row_get(row, "table")
    category = row_get(row, "category", "sector")
    instrument_type = row_get(row, "type", "security_type", "asset_type", "category")
    asset_type = _infer_asset_type(source_table, instrument_type, category)
    is_delisted = row_get(row, "isdelisted", "is_delisted", "delisted")
    is_active = not parse_bool(is_delisted, default=False)
    return (
        symbol, asset_type, name, exchange,
        instrument_type, source_table, category, is_active,
    )


def iter_rows_csv(csv_file: io.TextIOBase) -> Iterator[tuple]:
    reader = csv.DictReader(csv_file)
    reader.fieldnames = [normalise_header(x) for x in (reader.fieldnames or [])]
    debug_printed = False
    for row in reader:
        if not debug_printed:
            print(f"[debug] CSV columns: {reader.fieldnames}")
            debug_printed = True
        result = _ticker_row_to_tuple(row)
        if result is not None:
            yield result


def iter_rows_dicts(rows: list[dict[str, str]]) -> Iterator[tuple]:
    for row in rows:
        result = _ticker_row_to_tuple(row)
        if result is not None:
            yield result


# ── Staging + upsert ─────────────────────────────────────────────────────────

def stage_and_upsert(
    conn: psycopg.Connection,
    rows_iter: Iterator[tuple],
    config: IngestConfig,
) -> int:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg_tickers")
        cur.execute(
            """
            CREATE TEMP TABLE stg_tickers (
                symbol TEXT, asset_type TEXT, name TEXT,
                exchange TEXT, instrument_type TEXT,
                source_table TEXT, category TEXT, is_active BOOLEAN
            ) ON COMMIT PRESERVE ROWS
            """
        )
    total = copy_rows(conn, "stg_tickers", _STAGING_COLUMNS, rows_iter)
    print(f"[info] staged {total:,} ticker rows")

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {config.schema}.instruments (
                symbol, asset_type, name, exchange,
                instrument_type, source_table, category, is_active, updated_at
            )
            SELECT DISTINCT ON (s.symbol)
                s.symbol, s.asset_type, s.name, s.exchange,
                s.instrument_type, s.source_table, s.category,
                s.is_active, NOW()
            FROM stg_tickers s WHERE s.symbol IS NOT NULL
            ORDER BY s.symbol
            ON CONFLICT (symbol) DO UPDATE SET
                name            = EXCLUDED.name,
                asset_type      = COALESCE(EXCLUDED.asset_type,
                                           {config.schema}.instruments.asset_type),
                exchange        = EXCLUDED.exchange,
                instrument_type = EXCLUDED.instrument_type,
                source_table    = EXCLUDED.source_table,
                category        = EXCLUDED.category,
                is_active       = EXCLUDED.is_active,
                updated_at      = NOW()
            """
        )
    conn.commit()
    return total


def truncate_target(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {schema}.instruments CASCADE")
    conn.commit()
    print(f"[info] truncated {schema}.instruments")


def count_target(conn: psycopg.Connection, schema: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.instruments")
        return cur.fetchone()[0]


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> int:
    args = parse_ingest_args("Ingest SHARADAR/TICKERS into Postgres")
    api_key, db_dsn = resolve_credentials()
    config = IngestConfig(
        api_key=api_key, db_dsn=db_dsn,
        schema=args.schema, batch_size=args.batch_size,
        overwrite=args.overwrite,
    )
    return run_ingest(
        table_code=TABLE_CODE,
        required_cols=REQUIRED_TICKERS_COLS,
        known_cols=KNOWN_TICKERS_COLS,
        date_filter_col=None,  # reference table → always full reload
        ensure_schema_fn=ensure_schema,
        get_max_date_fn=get_max_lastupdated,
        stage_and_upsert_fn=stage_and_upsert,
        iter_rows_csv_fn=iter_rows_csv,
        iter_rows_dicts_fn=iter_rows_dicts,
        truncate_fn=truncate_target,
        count_fn=count_target,
        config=config,
    )


if __name__ == "__main__":
    raise SystemExit(main())
