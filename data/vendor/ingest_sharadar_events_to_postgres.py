#!/usr/bin/env python3
"""Ingest SHARADAR/EVENTS (corporate events) into Postgres."""
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
    parse_ingest_args,
    resolve_credentials,
    row_get,
    run_ingest,
)

TABLE_CODE = "SHARADAR/EVENTS"
REQUIRED_EVENTS_COLS: set[str] = {"ticker", "date", "eventcodes"}
KNOWN_EVENTS_COLS: set[str] = {"ticker", "date", "eventcodes"}

_STAGING_COLUMNS = [
    "symbol", "event_date", "event_code", "source_table",
]


# ── Schema / DDL ─────────────────────────────────────────────────────────────

def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    ensure_instruments_table(conn, schema)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.sharadar_events (
                instrument_id BIGINT NOT NULL
                    REFERENCES {schema}.instruments(instrument_id),
                event_date DATE NOT NULL,
                event_code INTEGER NOT NULL,
                source_table TEXT NOT NULL DEFAULT 'EVENTS',
                last_updated TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (instrument_id, event_date, event_code)
            )
            """
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS sharadar_events_event_date_idx "
            f"ON {schema}.sharadar_events (event_date)"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS sharadar_events_event_code_idx "
            f"ON {schema}.sharadar_events (event_code)"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS sharadar_events_instrument_date_idx "
            f"ON {schema}.sharadar_events (instrument_id, event_date)"
        )
    conn.commit()


def get_max_event_date(conn: psycopg.Connection, schema: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(event_date) FROM {schema}.sharadar_events")
        result = cur.fetchone()[0]
        return str(result) if result else None


# ── Row parsing ──────────────────────────────────────────────────────────────

def _parse_event_codes(value: Optional[str]) -> list[int]:
    if not value or not value.strip():
        return []
    parts = value.strip().replace(",", "|").split("|")
    out: list[int] = []
    for part in parts:
        token = part.strip()
        if not token:
            continue
        try:
            out.append(int(float(token)))
        except ValueError:
            print(f"[warn] could not parse event code: {token!r}")
    return out


def _event_rows_from_row(row: dict[str, str]) -> Iterator[tuple]:
    symbol = row_get(row, "ticker", "symbol")
    event_date = row_get(row, "date", "event_date")
    event_codes = _parse_event_codes(row_get(row, "eventcodes", "eventcode"))
    source_table = row_get(row, "table") or "EVENTS"
    if not symbol or not event_date or not event_codes:
        return
    for code in event_codes:
        yield (symbol, event_date, code, source_table)


def iter_rows_csv(csv_file: io.TextIOBase) -> Iterator[tuple]:
    reader = csv.DictReader(csv_file)
    reader.fieldnames = [normalise_header(x) for x in (reader.fieldnames or [])]
    debug_printed = False
    for row in reader:
        if not debug_printed:
            print(f"[debug] CSV columns: {reader.fieldnames}")
            debug_printed = True
        yield from _event_rows_from_row(row)


def iter_rows_dicts(rows: list[dict[str, str]]) -> Iterator[tuple]:
    for row in rows:
        yield from _event_rows_from_row(row)


# ── Staging + upsert ─────────────────────────────────────────────────────────

def stage_and_upsert(
    conn: psycopg.Connection,
    rows_iter: Iterator[tuple],
    config: IngestConfig,
) -> int:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg_sharadar_events")
        cur.execute(
            """
            CREATE TEMP TABLE stg_sharadar_events (
                symbol TEXT, event_date DATE, event_code INTEGER,
                source_table TEXT
            ) ON COMMIT PRESERVE ROWS
            """
        )
    total = 0
    for batch in chunked(rows_iter, config.batch_size):
        copied = copy_rows(conn, "stg_sharadar_events", _STAGING_COLUMNS, batch)
        total += copied
        print(f"[info] staged {copied:,} event rows (running total {total:,})")
    print(f"[info] staged sharadar events rows: {total:,}")

    with conn.cursor() as cur:
        # instruments
        cur.execute(
            f"""
            INSERT INTO {config.schema}.instruments
                (symbol, asset_type, source_table, is_active, updated_at)
            SELECT DISTINCT s.symbol, 'STOCK', 'EVENTS', TRUE, NOW()
            FROM stg_sharadar_events s WHERE s.symbol IS NOT NULL
            ON CONFLICT (symbol) DO UPDATE
            SET updated_at = NOW(),
                source_table = COALESCE(
                    {config.schema}.instruments.source_table,
                    EXCLUDED.source_table
                )
            """
        )
        # events
        cur.execute(
            f"""
            INSERT INTO {config.schema}.sharadar_events (
                instrument_id, event_date, event_code,
                source_table, updated_at
            )
            SELECT i.instrument_id, s.event_date, s.event_code,
                   s.source_table, NOW()
            FROM stg_sharadar_events s
            JOIN {config.schema}.instruments i ON i.symbol = s.symbol
            WHERE s.symbol IS NOT NULL
              AND s.event_date IS NOT NULL AND s.event_code IS NOT NULL
            ON CONFLICT (instrument_id, event_date, event_code) DO UPDATE SET
                source_table = EXCLUDED.source_table,
                updated_at   = NOW()
            """
        )
    conn.commit()
    return total


def truncate_target(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {schema}.sharadar_events")
    conn.commit()
    print(f"[info] truncated {schema}.sharadar_events")


def count_target(conn: psycopg.Connection, schema: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.sharadar_events")
        return cur.fetchone()[0]


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> int:
    args = parse_ingest_args("Ingest SHARADAR/EVENTS into Postgres")
    api_key, db_dsn = resolve_credentials()
    config = IngestConfig(
        api_key=api_key, db_dsn=db_dsn,
        schema=args.schema, batch_size=args.batch_size,
        overwrite=args.overwrite,
    )
    return run_ingest(
        table_code=TABLE_CODE,
        required_cols=REQUIRED_EVENTS_COLS,
        known_cols=KNOWN_EVENTS_COLS,
        date_filter_col=None,  # EVENTS API does not support date filtering → always full reload
        ensure_schema_fn=ensure_schema,
        get_max_date_fn=get_max_event_date,
        stage_and_upsert_fn=stage_and_upsert,
        iter_rows_csv_fn=iter_rows_csv,
        iter_rows_dicts_fn=iter_rows_dicts,
        truncate_fn=truncate_target,
        count_fn=count_target,
        config=config,
    )


if __name__ == "__main__":
    raise SystemExit(main())
