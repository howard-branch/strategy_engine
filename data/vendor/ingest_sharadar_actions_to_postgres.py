#!/usr/bin/env python3
"""Ingest SHARADAR/ACTIONS (corporate actions) into Postgres."""
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
    chunked,
    copy_rows,
    ensure_instruments_table,
    normalise_header,
    parse_decimal,
    parse_ingest_args,
    resolve_credentials,
    row_get,
    run_ingest,
)

TABLE_CODE = "SHARADAR/ACTIONS"
REQUIRED_ACTIONS_COLS: set[str] = {"ticker", "date", "action"}
KNOWN_ACTIONS_COLS: set[str] = {
    "ticker", "date", "action", "name", "value", "contraticker", "contraname",
}

_STAGING_COLUMNS = [
    "symbol", "action_date", "action", "name", "value",
    "contra_ticker", "contra_name", "source_table",
]


# ── Schema / DDL ─────────────────────────────────────────────────────────────

def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    ensure_instruments_table(conn, schema)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.corporate_actions (
                instrument_id BIGINT NOT NULL
                    REFERENCES {schema}.instruments(instrument_id),
                action_date DATE NOT NULL,
                action TEXT NOT NULL,
                name TEXT,
                value NUMERIC(30, 10),
                contra_ticker TEXT NOT NULL DEFAULT '',
                contra_name TEXT,
                source_table TEXT NOT NULL DEFAULT 'SHARADAR/ACTIONS',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (instrument_id, action_date, action, contra_ticker)
            )
            """
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS corporate_actions_action_date_idx "
            f"ON {schema}.corporate_actions (action_date)"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS corporate_actions_instrument_date_idx "
            f"ON {schema}.corporate_actions (instrument_id, action_date)"
        )
    conn.commit()


def get_max_action_date(conn: psycopg.Connection, schema: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(action_date) FROM {schema}.corporate_actions")
        result = cur.fetchone()[0]
        return str(result) if result else None


# ── Row parsing ──────────────────────────────────────────────────────────────

def _action_row_to_tuple(row: dict[str, str]) -> tuple | None:
    symbol = row_get(row, "ticker", "symbol")
    action_date = row_get(row, "date", "action_date")
    action = row_get(row, "action")
    if not symbol or not action_date or not action:
        return None
    return (
        symbol,
        action_date,
        action,
        row_get(row, "name"),
        parse_decimal(row_get(row, "value")),
        row_get(row, "contraticker", "contra_ticker") or "",
        row_get(row, "contraname", "contra_name"),
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
        result = _action_row_to_tuple(row)
        if result is not None:
            yield result


def iter_rows_dicts(rows: list[dict[str, str]]) -> Iterator[tuple]:
    for row in rows:
        result = _action_row_to_tuple(row)
        if result is not None:
            yield result


# ── Staging + upsert ─────────────────────────────────────────────────────────

def stage_and_upsert(
    conn: psycopg.Connection,
    rows_iter: Iterator[tuple],
    config: IngestConfig,
) -> int:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg_actions")
        cur.execute(
            """
            CREATE TEMP TABLE stg_actions (
                symbol TEXT, action_date DATE, action TEXT,
                name TEXT, value NUMERIC(30, 10), contra_ticker TEXT,
                contra_name TEXT, source_table TEXT
            ) ON COMMIT DROP
            """
        )
    total = 0
    for batch in chunked(rows_iter, config.batch_size):
        total += copy_rows(conn, "stg_actions", _STAGING_COLUMNS, batch)
    print(f"[info] staged {total:,} action rows")

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {config.schema}.corporate_actions (
                instrument_id, action_date, action, name, value,
                contra_ticker, contra_name, source_table, updated_at
            )
            SELECT DISTINCT ON (
                i.instrument_id, s.action_date, s.action,
                COALESCE(s.contra_ticker, '')
            )
                i.instrument_id, s.action_date, s.action, s.name, s.value,
                COALESCE(s.contra_ticker, ''), s.contra_name,
                s.source_table, NOW()
            FROM stg_actions s
            JOIN {config.schema}.instruments i ON i.symbol = s.symbol
            WHERE s.symbol IS NOT NULL
              AND s.action_date IS NOT NULL AND s.action IS NOT NULL
            ORDER BY
                i.instrument_id, s.action_date, s.action,
                COALESCE(s.contra_ticker, ''),
                s.value DESC NULLS LAST, s.contra_name DESC NULLS LAST
            ON CONFLICT (instrument_id, action_date, action, contra_ticker)
            DO UPDATE SET
                name         = EXCLUDED.name,
                value        = EXCLUDED.value,
                contra_name  = EXCLUDED.contra_name,
                source_table = EXCLUDED.source_table,
                updated_at   = NOW()
            """
        )
    conn.commit()
    return total


def truncate_target(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {schema}.corporate_actions")
    conn.commit()
    print(f"[info] truncated {schema}.corporate_actions")


def count_target(conn: psycopg.Connection, schema: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.corporate_actions")
        return cur.fetchone()[0]


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> int:
    args = parse_ingest_args("Ingest SHARADAR/ACTIONS into Postgres")
    api_key, db_dsn = resolve_credentials()
    config = IngestConfig(
        api_key=api_key, db_dsn=db_dsn,
        schema=args.schema, batch_size=args.batch_size,
        overwrite=args.overwrite,
    )
    return run_ingest(
        table_code=TABLE_CODE,
        required_cols=REQUIRED_ACTIONS_COLS,
        known_cols=KNOWN_ACTIONS_COLS,
        date_filter_col="date",
        ensure_schema_fn=ensure_schema,
        get_max_date_fn=get_max_action_date,
        stage_and_upsert_fn=stage_and_upsert,
        iter_rows_csv_fn=iter_rows_csv,
        iter_rows_dicts_fn=iter_rows_dicts,
        truncate_fn=truncate_target,
        count_fn=count_target,
        config=config,
    )


if __name__ == "__main__":
    raise SystemExit(main())
