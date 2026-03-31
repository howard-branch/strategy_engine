#!/usr/bin/env python3
"""Ingest SHARADAR/SP500 (S&P 500 membership changes) into Postgres."""
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
    parse_ingest_args,
    resolve_credentials,
    row_get,
    run_ingest,
)

TABLE_CODE = "SHARADAR/SP500"
REQUIRED_SP500_COLS: set[str] = {"ticker", "date", "action"}
KNOWN_SP500_COLS: set[str] = {
    "ticker", "date", "action", "name", "contraticker", "contraname", "note",
}

_STAGING_COLUMNS = [
    "symbol", "membership_date", "action", "name",
    "contra_ticker", "contra_name", "note", "source_table",
]


# ── Schema / DDL ─────────────────────────────────────────────────────────────

def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    ensure_instruments_table(conn, schema)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.sp500_membership (
                instrument_id BIGINT NOT NULL
                    REFERENCES {schema}.instruments(instrument_id),
                membership_date DATE NOT NULL,
                action TEXT NOT NULL,
                name TEXT,
                contra_ticker TEXT,
                contra_name TEXT,
                note TEXT,
                source_table TEXT NOT NULL DEFAULT 'SHARADAR/SP500',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (instrument_id, membership_date, action)
            )
            """
        )
        cur.execute(
            f"ALTER TABLE {schema}.sp500_membership "
            f"ADD COLUMN IF NOT EXISTS contra_ticker TEXT"
        )
        cur.execute(
            f"ALTER TABLE {schema}.sp500_membership "
            f"ADD COLUMN IF NOT EXISTS contra_name TEXT"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS sp500_membership_date_idx "
            f"ON {schema}.sp500_membership (membership_date)"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS sp500_membership_instrument_date_idx "
            f"ON {schema}.sp500_membership (instrument_id, membership_date)"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS sp500_membership_action_idx "
            f"ON {schema}.sp500_membership (action)"
        )
    conn.commit()


def get_max_membership_date(conn: psycopg.Connection, schema: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(membership_date) FROM {schema}.sp500_membership")
        result = cur.fetchone()[0]
        return str(result) if result else None


# ── Row parsing ──────────────────────────────────────────────────────────────

def _sp500_row_to_tuple(row: dict[str, str]) -> tuple | None:
    symbol = row_get(row, "ticker", "symbol")
    membership_date = row_get(row, "date", "membership_date")
    action = row_get(row, "action")
    if not symbol or not membership_date or not action:
        return None
    return (
        symbol, membership_date, action,
        row_get(row, "name"),
        row_get(row, "contraticker", "contra_ticker"),
        row_get(row, "contraname", "contra_name"),
        row_get(row, "note"),
        row_get(row, "table") or TABLE_CODE,
    )


def iter_rows_csv(csv_file: io.TextIOBase) -> Iterator[tuple]:
    reader = csv.DictReader(csv_file)
    reader.fieldnames = [normalise_header(x) for x in (reader.fieldnames or [])]
    debug_printed = False
    for row in reader:
        if not debug_printed:
            print(f"[debug] CSV columns: {reader.fieldnames}")
            debug_printed = True
        result = _sp500_row_to_tuple(row)
        if result is not None:
            yield result


def iter_rows_dicts(rows: list[dict[str, str]]) -> Iterator[tuple]:
    for row in rows:
        result = _sp500_row_to_tuple(row)
        if result is not None:
            yield result


# ── Staging + upsert ─────────────────────────────────────────────────────────

def stage_and_upsert(
    conn: psycopg.Connection,
    rows_iter: Iterator[tuple],
    config: IngestConfig,
) -> int:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg_sp500")
        cur.execute(
            """
            CREATE TEMP TABLE stg_sp500 (
                symbol TEXT, membership_date DATE, action TEXT,
                name TEXT, contra_ticker TEXT, contra_name TEXT,
                note TEXT, source_table TEXT
            ) ON COMMIT DROP
            """
        )
    total = copy_rows(conn, "stg_sp500", _STAGING_COLUMNS, rows_iter)
    print(f"[info] staged {total:,} SP500 rows")

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {config.schema}.sp500_membership (
                instrument_id, membership_date, action,
                name, contra_ticker, contra_name, note,
                source_table, updated_at
            )
            SELECT DISTINCT ON (i.instrument_id, s.membership_date, s.action)
                i.instrument_id, s.membership_date, s.action,
                s.name, s.contra_ticker, s.contra_name, s.note,
                s.source_table, NOW()
            FROM stg_sp500 s
            JOIN {config.schema}.instruments i ON i.symbol = s.symbol
            WHERE s.symbol IS NOT NULL
              AND s.membership_date IS NOT NULL AND s.action IS NOT NULL
            ORDER BY i.instrument_id, s.membership_date, s.action,
                     s.name DESC NULLS LAST, s.note DESC NULLS LAST
            ON CONFLICT (instrument_id, membership_date, action) DO UPDATE SET
                name          = EXCLUDED.name,
                contra_ticker = EXCLUDED.contra_ticker,
                contra_name   = EXCLUDED.contra_name,
                note          = EXCLUDED.note,
                source_table  = EXCLUDED.source_table,
                updated_at    = NOW()
            """
        )
    conn.commit()
    return total


def truncate_target(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {schema}.sp500_membership")
    conn.commit()
    print(f"[info] truncated {schema}.sp500_membership")


def count_target(conn: psycopg.Connection, schema: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.sp500_membership")
        return cur.fetchone()[0]


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> int:
    args = parse_ingest_args("Ingest SHARADAR/SP500 into Postgres")
    api_key, db_dsn = resolve_credentials()
    config = IngestConfig(
        api_key=api_key, db_dsn=db_dsn,
        schema=args.schema, batch_size=args.batch_size,
        overwrite=args.overwrite,
    )
    return run_ingest(
        table_code=TABLE_CODE,
        required_cols=REQUIRED_SP500_COLS,
        known_cols=KNOWN_SP500_COLS,
        date_filter_col="date",
        ensure_schema_fn=ensure_schema,
        get_max_date_fn=get_max_membership_date,
        stage_and_upsert_fn=stage_and_upsert,
        iter_rows_csv_fn=iter_rows_csv,
        iter_rows_dicts_fn=iter_rows_dicts,
        truncate_fn=truncate_target,
        count_fn=count_target,
        config=config,
    )


if __name__ == "__main__":
    raise SystemExit(main())
