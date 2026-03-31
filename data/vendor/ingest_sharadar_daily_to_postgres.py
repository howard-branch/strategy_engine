#!/usr/bin/env python3
"""Ingest SHARADAR/DAILY (daily metrics) into Postgres."""
from __future__ import annotations

import csv
import io
import sys
from pathlib import Path
from typing import Iterator, Optional

import psycopg

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

TABLE_CODE = "SHARADAR/DAILY"
REQUIRED_DAILY_COLS: set[str] = {"ticker", "date", "lastupdated"}
KNOWN_DAILY_COLS: set[str] = {
    "ticker", "date", "lastupdated",
    "ev", "evebit", "evebitda", "marketcap",
    "pb", "pe", "pe1", "ps", "ps1",
    "assetturnover", "commonequity", "debt", "dps",
    "ebitda", "ebitdamargin", "ebit",
    "eps", "epsdil", "epsusd", "epsdilgrowth1yr",
    "fcf", "fcfps", "gp", "grossmargin",
    "intangibles", "investedcapital", "inventory",
    "ncf", "ncfbus", "ncfcommon", "ncfdebt", "ncfdiv", "ncff", "ncfi", "ncfinv", "ncfo", "ncfx",
    "netinc", "netinccomstock", "netincnci", "netmargin",
    "opex", "opinc",
    "payables", "receivables", "retearn",
    "revenue", "revenueusd", "rnd",
    "ros", "roic", "roe", "roa",
    "sgna", "sharefactor", "sharesbas", "shareswa", "shareswadil",
    "sps", "tangibles", "taxassets", "taxliabilities",
    "tbvps", "workingcapital",
    "price", "volume",
}

# Column order used for staging + COPY
_METRIC_COLS = [
    "ev", "evebit", "evebitda", "marketcap",
    "pb", "pe", "pe1", "ps", "ps1",
    "price", "volume",
    "ebitda", "ebitdamargin", "ebit",
    "grossmargin", "netmargin", "ros", "roic", "roe", "roa",
    "eps", "epsdil", "epsusd", "epsdilgrowth1yr", "fcfps", "sps", "tbvps", "dps",
    "revenue", "revenueusd", "gp", "opex", "opinc", "sgna", "rnd",
    "netinc", "netinccomstock", "netincnci",
    "assetturnover", "commonequity", "debt", "intangibles",
    "investedcapital", "inventory", "payables", "receivables",
    "retearn", "tangibles", "taxassets", "taxliabilities", "workingcapital",
    "fcf", "ncf", "ncfbus", "ncfcommon", "ncfdebt", "ncfdiv",
    "ncff", "ncfi", "ncfinv", "ncfo", "ncfx",
    "sharefactor", "sharesbas", "shareswa", "shareswadil",
]

_STAGING_COLUMNS = ["symbol", "trade_date", "last_updated"] + _METRIC_COLS


# ── Schema / DDL ─────────────────────────────────────────────────────────────

def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    ensure_instruments_table(conn, schema)
    with conn.cursor() as cur:
        cols_ddl = ",\n                ".join(
            f"{c} {'BIGINT' if c == 'volume' else 'NUMERIC'}" for c in _METRIC_COLS
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.sharadar_daily (
                instrument_id BIGINT NOT NULL
                    REFERENCES {schema}.instruments(instrument_id),
                trade_date DATE NOT NULL,
                last_updated TIMESTAMPTZ,
                {cols_ddl},
                source_table TEXT NOT NULL DEFAULT 'DAILY',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (instrument_id, trade_date)
            )
            """
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS sharadar_daily_trade_date_idx "
            f"ON {schema}.sharadar_daily (trade_date)"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS sharadar_daily_instrument_date_idx "
            f"ON {schema}.sharadar_daily (instrument_id, trade_date)"
        )
    conn.commit()


def get_max_trade_date(conn: psycopg.Connection, schema: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(trade_date) FROM {schema}.sharadar_daily")
        result = cur.fetchone()[0]
        return str(result) if result else None


# ── Row parsing ──────────────────────────────────────────────────────────────

def _parse_daily_row(row: dict[str, str]) -> Optional[tuple]:
    symbol = row_get(row, "ticker", "symbol")
    trade_date = row_get(row, "date", "trade_date")
    if not symbol or not trade_date:
        return None
    last_updated = row_get(row, "lastupdated", "last_updated")
    metrics = []
    for c in _METRIC_COLS:
        val = row_get(row, c)
        if c == "volume":
            metrics.append(parse_int(val))
        else:
            metrics.append(parse_decimal(val))
    return (symbol, trade_date, last_updated, *metrics)


def iter_rows_csv(csv_file: io.TextIOBase) -> Iterator[tuple]:
    reader = csv.DictReader(csv_file)
    reader.fieldnames = [normalise_header(x) for x in (reader.fieldnames or [])]
    debug_printed = False
    for row in reader:
        if not debug_printed:
            print(f"[debug] CSV columns: {reader.fieldnames}")
            debug_printed = True
        parsed = _parse_daily_row(row)
        if parsed is not None:
            yield parsed


def iter_rows_dicts(rows: list[dict[str, str]]) -> Iterator[tuple]:
    for row in rows:
        parsed = _parse_daily_row(row)
        if parsed is not None:
            yield parsed


# ── Staging + upsert ─────────────────────────────────────────────────────────

def stage_and_upsert(
    conn: psycopg.Connection,
    rows_iter: Iterator[tuple],
    config: IngestConfig,
) -> int:
    stg_cols_ddl = ",\n                ".join(
        f"{c} {'BIGINT' if c == 'volume' else 'NUMERIC'}" for c in _METRIC_COLS
    )
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg_sharadar_daily")
        cur.execute(
            f"""
            CREATE TEMP TABLE stg_sharadar_daily (
                symbol TEXT,
                trade_date DATE,
                last_updated TIMESTAMPTZ,
                {stg_cols_ddl}
            ) ON COMMIT PRESERVE ROWS
            """
        )
    total = 0
    for batch in chunked(rows_iter, config.batch_size):
        copied = copy_rows(conn, "stg_sharadar_daily", _STAGING_COLUMNS, batch)
        total += copied
        print(f"[info] staged {copied:,} daily rows (running total {total:,})")
    print(f"[info] staged sharadar daily rows: {total:,}")

    # instruments upsert
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {config.schema}.instruments
                (symbol, asset_type, source_table, is_active, updated_at)
            SELECT DISTINCT s.symbol, 'STOCK', 'DAILY', TRUE, NOW()
            FROM stg_sharadar_daily s WHERE s.symbol IS NOT NULL
            ON CONFLICT (symbol) DO UPDATE
            SET updated_at = NOW(),
                source_table = COALESCE(
                    {config.schema}.instruments.source_table,
                    EXCLUDED.source_table
                )
            """
        )

    # daily metrics upsert
    metric_insert = ", ".join(_METRIC_COLS)
    metric_select = ", ".join(f"s.{c}" for c in _METRIC_COLS)
    metric_update = ",\n                ".join(
        f"{c} = EXCLUDED.{c}" for c in _METRIC_COLS
    )
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {config.schema}.sharadar_daily (
                instrument_id, trade_date, last_updated,
                {metric_insert}, updated_at
            )
            SELECT
                i.instrument_id, s.trade_date, s.last_updated,
                {metric_select}, NOW()
            FROM stg_sharadar_daily s
            JOIN {config.schema}.instruments i ON i.symbol = s.symbol
            WHERE s.symbol IS NOT NULL AND s.trade_date IS NOT NULL
            ON CONFLICT (instrument_id, trade_date) DO UPDATE SET
                last_updated = EXCLUDED.last_updated,
                {metric_update},
                updated_at = NOW()
            """
        )
    conn.commit()
    return total


def truncate_target(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {schema}.sharadar_daily")
    conn.commit()
    print(f"[info] truncated {schema}.sharadar_daily")


def count_target(conn: psycopg.Connection, schema: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.sharadar_daily")
        return cur.fetchone()[0]


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> int:
    args = parse_ingest_args("Ingest SHARADAR/DAILY into Postgres")
    api_key, db_dsn = resolve_credentials()
    config = IngestConfig(
        api_key=api_key, db_dsn=db_dsn,
        schema=args.schema, batch_size=args.batch_size,
        overwrite=args.overwrite,
    )
    return run_ingest(
        table_code=TABLE_CODE,
        required_cols=REQUIRED_DAILY_COLS,
        known_cols=KNOWN_DAILY_COLS,
        date_filter_col="date",
        ensure_schema_fn=ensure_schema,
        get_max_date_fn=get_max_trade_date,
        stage_and_upsert_fn=stage_and_upsert,
        iter_rows_csv_fn=iter_rows_csv,
        iter_rows_dicts_fn=iter_rows_dicts,
        truncate_fn=truncate_target,
        count_fn=count_target,
        config=config,
    )


if __name__ == "__main__":
    raise SystemExit(main())

