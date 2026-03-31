#!/usr/bin/env python3
"""Ingest SHARADAR/SF3 (institutional holdings) into Postgres."""
from __future__ import annotations

import csv
import io
import json
import sys
from pathlib import Path
from typing import Iterator

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent))

from sharadar_common import (
    IngestConfig,
    chunked,
    copy_rows,
    ensure_instruments_table,
    normalise_header,
    parse_ingest_args,
    parse_numeric,
    resolve_credentials,
    row_get,
    run_ingest,
)

TABLE_CODE = "SHARADAR/SF3"
REQUIRED_SF3_COLS: set[str] = {"ticker", "investorname", "securitytype", "calendardate"}
KNOWN_SF3_COLS: set[str] = {
    "ticker", "investorname", "securitytype", "calendardate",
    "value", "units", "price",
}

_STAGING_COLUMNS = [
    "ticker", "investor_name", "security_type", "calendar_date",
    "value", "units", "price", "raw_record",
]


# ── Schema / DDL ─────────────────────────────────────────────────────────────

def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    ensure_instruments_table(conn, schema)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.sharadar_sf3 (
                ticker TEXT NOT NULL,
                investor_name TEXT NOT NULL,
                security_type TEXT NOT NULL,
                calendar_date DATE NOT NULL,
                value NUMERIC,
                units NUMERIC,
                price NUMERIC,
                raw_record JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (ticker, investor_name, security_type, calendar_date)
            )
            """
        )
        for idx_col in ("calendar_date", "ticker", "investor_name"):
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS sharadar_sf3_{idx_col}_idx "
                f"ON {schema}.sharadar_sf3 ({idx_col})"
            )
    conn.commit()


def get_max_calendar_date(conn: psycopg.Connection, schema: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(calendar_date) FROM {schema}.sharadar_sf3")
        result = cur.fetchone()[0]
        return str(result) if result else None


# ── Row parsing ──────────────────────────────────────────────────────────────

def _sf3_row_to_tuple(row: dict[str, str]) -> tuple | None:
    r = {normalise_header(k): (v.strip() if isinstance(v, str) else v)
         for k, v in row.items()}
    ticker = row_get(r, "ticker", "symbol")
    investor_name = row_get(r, "investorname", "investor_name")
    security_type = row_get(r, "securitytype", "security_type")
    calendar_date = row_get(r, "calendardate", "calendar_date", "date")
    if not ticker or not investor_name or not security_type or not calendar_date:
        return None
    return (
        ticker, investor_name, security_type, calendar_date,
        parse_numeric(row_get(r, "value")),
        parse_numeric(row_get(r, "units")),
        parse_numeric(row_get(r, "price")),
        json.dumps(r, ensure_ascii=False),
    )


def iter_rows_csv(csv_file: io.TextIOBase) -> Iterator[tuple]:
    reader = csv.DictReader(csv_file)
    reader.fieldnames = [normalise_header(x) for x in (reader.fieldnames or [])]
    debug_printed = False
    for row in reader:
        if not debug_printed:
            print(f"[debug] CSV columns: {reader.fieldnames}")
            debug_printed = True
        result = _sf3_row_to_tuple(row)
        if result is not None:
            yield result


def iter_rows_dicts(rows: list[dict[str, str]]) -> Iterator[tuple]:
    for row in rows:
        result = _sf3_row_to_tuple(row)
        if result is not None:
            yield result


# ── Staging + upsert ─────────────────────────────────────────────────────────

def stage_and_upsert(
    conn: psycopg.Connection,
    rows_iter: Iterator[tuple],
    config: IngestConfig,
) -> int:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg_sharadar_sf3")
        cur.execute(
            """
            CREATE TEMP TABLE stg_sharadar_sf3 (
                ticker TEXT, investor_name TEXT, security_type TEXT,
                calendar_date DATE, value NUMERIC, units NUMERIC,
                price NUMERIC, raw_record JSONB
            ) ON COMMIT PRESERVE ROWS
            """
        )
    total = 0
    for batch in chunked(rows_iter, config.batch_size):
        copied = copy_rows(conn, "stg_sharadar_sf3", _STAGING_COLUMNS, batch)
        total += copied
        print(f"[info] staged {copied:,} SF3 rows (running total {total:,})")
    print(f"[info] staged sharadar SF3 rows: {total:,}")

    with conn.cursor() as cur:
        # instruments
        cur.execute(
            f"""
            INSERT INTO {config.schema}.instruments
                (symbol, asset_type, source_table, is_active, updated_at)
            SELECT DISTINCT s.ticker, 'STOCK', 'SF3', TRUE, NOW()
            FROM stg_sharadar_sf3 s WHERE s.ticker IS NOT NULL
            ON CONFLICT (symbol) DO UPDATE
            SET updated_at = NOW(),
                source_table = COALESCE(
                    {config.schema}.instruments.source_table,
                    EXCLUDED.source_table
                )
            """
        )
        # sf3
        cur.execute(
            f"""
            INSERT INTO {config.schema}.sharadar_sf3 (
                ticker, investor_name, security_type, calendar_date,
                value, units, price, raw_record, updated_at
            )
            SELECT s.ticker, s.investor_name, s.security_type,
                   s.calendar_date, s.value, s.units, s.price,
                   s.raw_record, NOW()
            FROM stg_sharadar_sf3 s
            WHERE s.ticker IS NOT NULL AND s.investor_name IS NOT NULL
              AND s.security_type IS NOT NULL AND s.calendar_date IS NOT NULL
            ON CONFLICT (ticker, investor_name, security_type, calendar_date)
            DO UPDATE SET
                value      = EXCLUDED.value,
                units      = EXCLUDED.units,
                price      = EXCLUDED.price,
                raw_record = EXCLUDED.raw_record,
                updated_at = NOW()
            """
        )
    conn.commit()
    return total


def truncate_target(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {schema}.sharadar_sf3")
    conn.commit()
    print(f"[info] truncated {schema}.sharadar_sf3")


def count_target(conn: psycopg.Connection, schema: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.sharadar_sf3")
        return cur.fetchone()[0]


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> int:
    args = parse_ingest_args("Ingest SHARADAR/SF3 into Postgres")
    api_key, db_dsn = resolve_credentials()
    config = IngestConfig(
        api_key=api_key, db_dsn=db_dsn,
        schema=args.schema, batch_size=args.batch_size,
        overwrite=args.overwrite,
    )
    return run_ingest(
        table_code=TABLE_CODE,
        required_cols=REQUIRED_SF3_COLS,
        known_cols=KNOWN_SF3_COLS,
        date_filter_col="calendardate",
        ensure_schema_fn=ensure_schema,
        get_max_date_fn=get_max_calendar_date,
        stage_and_upsert_fn=stage_and_upsert,
        iter_rows_csv_fn=iter_rows_csv,
        iter_rows_dicts_fn=iter_rows_dicts,
        truncate_fn=truncate_target,
        count_fn=count_target,
        config=config,
    )


if __name__ == "__main__":
    raise SystemExit(main())
