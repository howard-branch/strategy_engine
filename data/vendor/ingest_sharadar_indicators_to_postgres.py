#!/usr/bin/env python3
from __future__ import annotations

import csv
import io
import json
import sys
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Optional

import dotenv
import psycopg
import requests

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ENV_FILE = _PROJECT_ROOT / ".env"
if _ENV_FILE.exists():
    dotenv.load_dotenv(_ENV_FILE)

# Shared Sharadar helpers
_VENDOR_DIR = Path(__file__).resolve().parent
if str(_VENDOR_DIR) not in sys.path:
    sys.path.insert(0, str(_VENDOR_DIR))

from sharadar_common import (
    fetch_api_sample,
    validate_sample_shape,
    detect_schema_drift,
)

DATASET_CODE = "SHARADAR/INDICATORS"

# Columns the API must return (rule 2c) and the full known set (rule 2b).
REQUIRED_INDICATORS_COLS: set[str] = {"indicator", "table"}
KNOWN_INDICATORS_COLS: set[str] = {
    "indicator", "table", "name", "description", "unittype",
}


@dataclass
class Config:
    api_key: str
    db_dsn: str
    schema: str = "strategy_engine"
    batch_size: int = 10_000
    download_retries: int = 5
    request_timeout_connect: int = 30
    request_timeout_read: int = 600
    overwrite: bool = False


def normalise_header(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


def row_get(row: dict[str, str], *names: str) -> Optional[str]:
    for name in names:
        key = normalise_header(name)
        if key in row:
            value = row[key]
            if value is None:
                return None
            value = value.strip()
            return value if value != "" else None
    return None


def chunked(rows: Iterable[tuple], size: int) -> Iterator[list[tuple]]:
    buf: list[tuple] = []
    for row in rows:
        buf.append(row)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def request_with_retries(
    session: requests.Session,
    url: str,
    *,
    max_attempts: int,
    timeout: tuple[int, int],
) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt == max_attempts:
                break
            sleep_s = min(2 ** attempt, 30)
            print(f"[warn] request failed ({attempt}/{max_attempts}): {exc}")
            print(f"[info] retrying in {sleep_s}s...")
            time.sleep(sleep_s)
    assert last_exc is not None
    raise last_exc


def get_bulk_export_link(
    session: requests.Session,
    datatable_code: str,
    api_key: str,
    *,
    poll_seconds: int = 5,
    max_wait_seconds: int = 300,
) -> str:
    url = (
        f"https://data.nasdaq.com/api/v3/datatables/{datatable_code}.csv"
        f"?qopts.export=true&api_key={api_key}"
    )
    deadline = time.time() + max_wait_seconds

    while True:
        resp = session.get(url, timeout=(30, 300))
        resp.raise_for_status()

        reader = csv.DictReader(io.StringIO(resp.text))
        row = next(reader, None)
        if row is None:
            raise RuntimeError(f"No metadata row returned for bulk export {datatable_code}")

        link = (row.get("file.link") or "").strip()
        status = (row.get("file.status") or "").strip()

        print(f"[debug] bulk export {datatable_code}: status={status!r}, link_present={bool(link)}")

        if status.lower() == "fresh":
            if not link:
                raise RuntimeError(f"Bulk export {datatable_code} is Fresh but file.link is empty")
            return link

        if status.lower() in {"creating", "regenerating"}:
            if time.time() >= deadline:
                raise TimeoutError(f"Timed out waiting for Nasdaq export {datatable_code} to become Fresh")
            time.sleep(poll_seconds)
            continue

        raise RuntimeError(f"Unexpected bulk export status for {datatable_code}: {status!r}; row={row}")


def first_csv_from_bytes(data: bytes) -> io.TextIOBase:
    if zipfile.is_zipfile(io.BytesIO(data)):
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            csv_names = [name for name in zf.namelist() if name.lower().endswith(".csv")]
            if not csv_names:
                raise RuntimeError(f"ZIP download contained no CSV files: {zf.namelist()}")
            name = csv_names[0]
            print(f"[debug] extracted CSV from ZIP: {name}")
            return io.StringIO(zf.read(name).decode("utf-8", errors="replace"))

    text = data.decode("utf-8", errors="replace")
    if text.lstrip().startswith("{"):
        raise RuntimeError(f"Nasdaq returned JSON instead of CSV: {text[:500]}")
    if "<html" in text[:500].lower():
        raise RuntimeError(f"Nasdaq returned HTML instead of CSV: {text[:500]}")
    return io.StringIO(text)


def download_csv_file(session: requests.Session, config: Config) -> io.TextIOBase:
    link = get_bulk_export_link(session, DATASET_CODE, config.api_key)
    resp = request_with_retries(
        session,
        link,
        max_attempts=config.download_retries,
        timeout=(config.request_timeout_connect, config.request_timeout_read),
    )
    return first_csv_from_bytes(resp.content)


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
            f"""
            CREATE INDEX IF NOT EXISTS sharadar_indicators_indicator_idx
            ON {schema}.sharadar_indicators (indicator)
            """
        )

    conn.commit()


def truncate_tables(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {schema}.sharadar_indicators")
    conn.commit()
    print("[info] truncated existing sharadar_indicators data")


def create_staging_tables(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg_sharadar_indicators")
        cur.execute(
            """
            CREATE TEMP TABLE stg_sharadar_indicators (
                table_name TEXT,
                indicator TEXT,
                name TEXT,
                description TEXT,
                unit_type TEXT,
                raw_record JSONB
            ) ON COMMIT PRESERVE ROWS
            """
        )


def copy_rows(
    conn: psycopg.Connection,
    table_name: str,
    columns: list[str],
    rows: Iterable[tuple],
) -> int:
    total = 0
    with conn.cursor() as cur:
        with cur.copy(f"COPY {table_name} ({', '.join(columns)}) FROM STDIN") as copy:
            for row in rows:
                copy.write_row(row)
                total += 1
    return total


def iter_indicator_rows(csv_file: io.TextIOBase) -> Iterator[tuple]:
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
        name = row_get(normalised_row, "name", "title", "label")
        description = row_get(normalised_row, "description", "desc")
        unit_type = row_get(normalised_row, "unit_type", "unittype", "unit")

        if not indicator:
            continue

        yield (
            table_name,
            indicator,
            name,
            description,
            unit_type,
            json.dumps(normalised_row, ensure_ascii=False),
        )


def upsert_indicators(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {schema}.sharadar_indicators (
                table_name, indicator, name, description, unit_type, raw_record, updated_at
            )
            SELECT s.table_name, s.indicator, s.name, s.description, s.unit_type, s.raw_record, NOW()
            FROM stg_sharadar_indicators s
            WHERE s.indicator IS NOT NULL
            ON CONFLICT (table_name, indicator) DO UPDATE
            SET name = EXCLUDED.name,
                description = EXCLUDED.description,
                unit_type = EXCLUDED.unit_type,
                raw_record = EXCLUDED.raw_record,
                updated_at = NOW()
            """
        )


def _full_bulk_load(
    session: requests.Session,
    conn: psycopg.Connection,
    config: Config,
    *,
    truncate: bool = False,
) -> int:
    """Download the full bulk CSV/ZIP and load into sharadar_indicators. Returns staged count."""
    if truncate:
        truncate_tables(conn, config.schema)

    create_staging_tables(conn)

    csv_file = download_csv_file(session, config)
    total = 0

    for batch in chunked(iter_indicator_rows(csv_file), config.batch_size):
        copied = copy_rows(
            conn,
            "stg_sharadar_indicators",
            ["table_name", "indicator", "name", "description", "unit_type", "raw_record"],
            batch,
        )
        total += copied
        print(f"[info] staged {copied:,} indicator rows (running total {total:,})")

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM stg_sharadar_indicators")
        staged_count = cur.fetchone()[0]
    print(f"[info] staged sharadar indicator rows: {staged_count:,}")

    upsert_indicators(conn, config.schema)
    conn.commit()
    return total


def parse_args() -> Config:
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Ingest SHARADAR/INDICATORS into Postgres")
    parser.add_argument("--api-key", default=os.getenv("NDL_API_KEY") or os.getenv("NASDAQ_DATA_LINK_API_KEY"))
    parser.add_argument("--db-dsn", default=os.getenv("DATABASE_URL") or os.getenv("DB_DSN"))
    parser.add_argument("--schema", default=os.getenv("DB_SCHEMA", "strategy_engine"))
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--overwrite", action="store_true")

    args = parser.parse_args()

    if not args.api_key:
        raise SystemExit("Missing --api-key (or set NDL_API_KEY in .env)")
    if not args.db_dsn:
        raise SystemExit("Missing --db-dsn (or set DATABASE_URL / DB_DSN in .env)")

    return Config(
        api_key=args.api_key,
        db_dsn=args.db_dsn,
        schema=args.schema,
        batch_size=args.batch_size,
        overwrite=args.overwrite,
    )


def main() -> int:
    config = parse_args()

    with requests.Session() as session, psycopg.connect(config.db_dsn) as conn:
        ensure_schema(conn, config.schema)

        # ── STEP 1: sample validation (rule 2c) ───────────────────────────────
        # INDICATORS is a small reference table with no date column, so we always
        # do a full reload.  We still validate the sample shape first.
        sample_cols, sample_rows = fetch_api_sample(
            session, DATASET_CODE, config.api_key, n=75
        )
        validate_sample_shape(DATASET_CODE, sample_cols, sample_rows, REQUIRED_INDICATORS_COLS)

        # ── STEP 2: schema-drift check (rule 2b) ──────────────────────────────
        # If the API has changed columns, truncate first to avoid stale metadata.
        drift = detect_schema_drift(DATASET_CODE, set(sample_cols), KNOWN_INDICATORS_COLS)

        truncate = config.overwrite or drift
        if drift:
            print(f"[info] {DATASET_CODE}: schema drift detected – truncating before reload (rule 2b)")
        elif config.overwrite:
            print(f"[info] {DATASET_CODE}: --overwrite requested – truncating before reload")
        else:
            print(f"[info] {DATASET_CODE}: full reload (reference table, no incremental)")

        # INDICATORS has no date dimension → always do a full reload
        total = _full_bulk_load(session, conn, config, truncate=truncate)

        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {config.schema}.sharadar_indicators")
            final_count = cur.fetchone()[0]

    print(f"[info] {config.schema}.sharadar_indicators now contains {final_count:,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
