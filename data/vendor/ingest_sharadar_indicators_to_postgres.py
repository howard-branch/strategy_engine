#!/usr/bin/env python3
"""Ingest SHARADAR/INDICATORS (field metadata reference table) into Postgres."""
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
    normalise_header,
    parse_ingest_args,
    resolve_credentials,
    row_get,
    run_ingest,
)

TABLE_CODE = "SHARADAR/INDICATORS"
REQUIRED_INDICATORS_COLS: set[str] = {"indicator", "table"}
KNOWN_INDICATORS_COLS: set[str] = {
    "indicator", "table", "name", "description", "unittype",
}

_STAGING_COLUMNS = [
    "table_name", "indicator", "name", "description", "unit_type", "raw_record",
]


# ── Schema / DDL ─────────────────────────────────────────────────────────────

def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.sharadar_indicators (
                table_name TEXT NOT NULL,
                indicator TEXT NOT NULL,
                name TEXT,
                description TEXT,
                unit_type TEXT,
                raw_record JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (table_name, indicator)
            )
            """
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS sharadar_indicators_indicator_idx "
            f"ON {schema}.sharadar_indicators (indicator)"
        )
    conn.commit()


def get_max_date(_conn: psycopg.Connection, _schema: str) -> str | None:
    # Reference table — never used; date_filter_col=None skips this.
    return None


# ── Row parsing ──────────────────────────────────────────────────────────────

def iter_rows_csv(csv_file: io.TextIOBase) -> Iterator[tuple]:
    reader = csv.DictReader(csv_file)
    reader.fieldnames = [normalise_header(x) for x in (reader.fieldnames or [])]
    debug_printed = False
    for row in reader:
        if not debug_printed:
            print(f"[debug] CSV columns: {reader.fieldnames}")
            debug_printed = True
        normalised_row = {
            normalise_header(k): (v.strip() if isinstance(v, str) else v)
            for k, v in row.items()
        }
        table_name = row_get(normalised_row, "table", "datatable", "dataset", "sf1_table") or "SF1"
        indicator = row_get(normalised_row, "indicator", "code", "field")
        if not indicator:
            continue
        yield (
            table_name,
            indicator,
            row_get(normalised_row, "name", "title", "label"),
            row_get(normalised_row, "description", "desc"),
            row_get(normalised_row, "unit_type", "unittype", "unit"),
            json.dumps(normalised_row, ensure_ascii=False),
        )


def iter_rows_dicts(rows: list[dict[str, str]]) -> Iterator[tuple]:
    for row in rows:
        indicator = row_get(row, "indicator", "code", "field")
        if not indicator:
            continue
        yield (
            row_get(row, "table", "datatable", "dataset") or "SF1",
            indicator,
            row_get(row, "name", "title", "label"),
            row_get(row, "description", "desc"),
            row_get(row, "unit_type", "unittype", "unit"),
            json.dumps(row, ensure_ascii=False),
        )


# ── Staging + upsert ─────────────────────────────────────────────────────────

def stage_and_upsert(
    conn: psycopg.Connection,
    rows_iter: Iterator[tuple],
    config: IngestConfig,
) -> int:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg_sharadar_indicators")
        cur.execute(
            """
            CREATE TEMP TABLE stg_sharadar_indicators (
                table_name TEXT, indicator TEXT, name TEXT,
                description TEXT, unit_type TEXT, raw_record JSONB
            ) ON COMMIT PRESERVE ROWS
            """
        )
    total = 0
    for batch in chunked(rows_iter, config.batch_size):
        copied = copy_rows(conn, "stg_sharadar_indicators", _STAGING_COLUMNS, batch)
        total += copied
        print(f"[info] staged {copied:,} indicator rows (running total {total:,})")
    print(f"[info] staged sharadar indicator rows: {total:,}")

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {config.schema}.sharadar_indicators (
                table_name, indicator, name, description,
                unit_type, raw_record, updated_at
            )
            SELECT s.table_name, s.indicator, s.name, s.description,
                   s.unit_type, s.raw_record, NOW()
            FROM stg_sharadar_indicators s WHERE s.indicator IS NOT NULL
            ON CONFLICT (table_name, indicator) DO UPDATE SET
                name        = EXCLUDED.name,
                description = EXCLUDED.description,
                unit_type   = EXCLUDED.unit_type,
                raw_record  = EXCLUDED.raw_record,
                updated_at  = NOW()
            """
        )
    conn.commit()
    return total


def truncate_target(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {schema}.sharadar_indicators")
    conn.commit()
    print(f"[info] truncated {schema}.sharadar_indicators")


def count_target(conn: psycopg.Connection, schema: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.sharadar_indicators")
        return cur.fetchone()[0]


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> int:
    args = parse_ingest_args("Ingest SHARADAR/INDICATORS into Postgres")
    api_key, db_dsn = resolve_credentials()
    config = IngestConfig(
        api_key=api_key, db_dsn=db_dsn,
        schema=args.schema, batch_size=args.batch_size,
        overwrite=args.overwrite,
    )
    return run_ingest(
        table_code=TABLE_CODE,
        required_cols=REQUIRED_INDICATORS_COLS,
        known_cols=KNOWN_INDICATORS_COLS,
        date_filter_col=None,  # reference table → always full reload
        ensure_schema_fn=ensure_schema,
        get_max_date_fn=get_max_date,
        stage_and_upsert_fn=stage_and_upsert,
        iter_rows_csv_fn=iter_rows_csv,
        iter_rows_dicts_fn=iter_rows_dicts,
        truncate_fn=truncate_target,
        count_fn=count_target,
        config=config,
    )


if __name__ == "__main__":
    raise SystemExit(main())
