#!/usr/bin/env python3
from __future__ import annotations

import csv
import io
import json
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


@dataclass
class Config:
    api_key: str
    db_dsn: str
    schema: str = "strategy_engine"
    batch_size: int = 100_000
    download_retries: int = 5
    request_timeout_connect: int = 30
    request_timeout_read: int = 600
    overwrite: bool = False


DATASET_CODE = "SHARADAR/SF3"


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


def parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(float(value))
    except ValueError:
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
            sleep_s = min(2**attempt, 30)
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
            CREATE TABLE IF NOT EXISTS {schema}.instruments (
                instrument_id BIGSERIAL PRIMARY KEY,
                symbol TEXT NOT NULL UNIQUE,
                asset_type TEXT NOT NULL DEFAULT 'STOCK',
                name TEXT,
                exchange TEXT,
                instrument_type TEXT,
                source_table TEXT,
                category TEXT,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

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

        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS sharadar_sf3_calendar_date_idx
            ON {schema}.sharadar_sf3 (calendar_date)
            """
        )

        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS sharadar_sf3_ticker_idx
            ON {schema}.sharadar_sf3 (ticker)
            """
        )

        cur.execute(
            f""" CREATE INDEX IF NOT EXISTS sharadar_sf3_investor_name_idx
            ON {schema}.sharadar_sf3 (investor_name) """)

        conn.commit()


def recreate_sf3_table(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.sharadar_sf3")
    conn.commit()
    print("[info] dropped existing sharadar_sf3 table")


def create_staging_tables(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg_sharadar_sf3")
        cur.execute(
            """
            CREATE TEMP TABLE stg_sharadar_sf3 (
                ticker TEXT,
                investor_name TEXT,
                security_type TEXT,
                calendar_date DATE,
                value NUMERIC,
                units NUMERIC,
                price NUMERIC,
                raw_record JSONB
            ) ON COMMIT PRESERVE ROWS
            """
        )

def parse_numeric(value: Optional[str]):
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


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


def iter_sf3_rows(csv_file: io.TextIOBase) -> Iterator[tuple]:
    reader = csv.DictReader(csv_file)
    reader.fieldnames = [normalise_header(x) for x in (reader.fieldnames or [])]

    debug_printed = False

    for row in reader:
        if not debug_printed:
            print(f"[debug] CSV columns: {reader.fieldnames}")
            debug_printed = True

        r = {
            normalise_header(k): (v.strip() if isinstance(v, str) else v)
            for k, v in row.items()
        }

        ticker = row_get(r, "ticker", "symbol")
        investor_name = row_get(r, "investorname", "investor_name")
        security_type = row_get(r, "securitytype", "security_type")
        calendar_date = row_get(r, "calendardate", "calendar_date", "date")

        if not ticker or not investor_name or not security_type or not calendar_date:
            continue

        yield (
            ticker,
            investor_name,
            security_type,
            calendar_date,
            parse_numeric(row_get(r, "value")),
            parse_numeric(row_get(r, "units")),
            parse_numeric(row_get(r, "price")),
            json.dumps(r, ensure_ascii=False),
        )

def upsert_instruments_from_sf3(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {schema}.instruments (
                symbol,
                asset_type,
                source_table,
                is_active,
                updated_at
            )
            SELECT DISTINCT
                s.ticker,
                'STOCK',
                'SF3',
                TRUE,
                NOW()
            FROM stg_sharadar_sf3 s
            WHERE s.ticker IS NOT NULL
            ON CONFLICT (symbol) DO UPDATE
            SET
                updated_at = NOW(),
                source_table = COALESCE({schema}.instruments.source_table, EXCLUDED.source_table)
            """
        )


def upsert_sf3(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {schema}.sharadar_sf3 (
                ticker,
                investor_name,
                security_type,
                calendar_date,
                value,
                units,
                price,
                raw_record,
                updated_at
            )
            SELECT
                s.ticker,
                s.investor_name,
                s.security_type,
                s.calendar_date,
                s.value,
                s.units,
                s.price,
                s.raw_record,
                NOW()
            FROM stg_sharadar_sf3 s
            WHERE s.ticker IS NOT NULL
              AND s.investor_name IS NOT NULL
              AND s.security_type IS NOT NULL
              AND s.calendar_date IS NOT NULL
            ON CONFLICT (ticker, investor_name, security_type, calendar_date) DO UPDATE
            SET
                value = EXCLUDED.value,
                units = EXCLUDED.units,
                price = EXCLUDED.price,
                raw_record = EXCLUDED.raw_record,
                updated_at = NOW()
            """
        )

def parse_args() -> Config:
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Ingest SHARADAR/SF3 into Postgres")
    parser.add_argument("--api-key", default=os.getenv("NDL_API_KEY") or os.getenv("NASDAQ_DATA_LINK_API_KEY") or os.getenv("NASDAQ_API_KEY"))
    parser.add_argument("--db-dsn", default=os.getenv("DATABASE_URL") or os.getenv("DB_DSN"))
    parser.add_argument("--schema", default=os.getenv("DB_SCHEMA", "project_quant"))
    parser.add_argument("--batch-size", type=int, default=100_000)
    parser.add_argument("--overwrite", action="store_true", help="Drop and recreate sharadar_sf3 before loading")

    args = parser.parse_args()

    if not args.api_key:
        raise SystemExit("Missing --api-key (or NDL_API_KEY / NASDAQ_DATA_LINK_API_KEY / NASDAQ_API_KEY in environment)")
    if not args.db_dsn:
        raise SystemExit("Missing --db-dsn (or DATABASE_URL / DB_DSN in environment)")

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
        if config.overwrite:
            recreate_sf3_table(conn, config.schema)

        ensure_schema(conn, config.schema)
        create_staging_tables(conn)

        csv_file = download_csv_file(session, config)
        total = 0

        for batch in chunked(iter_sf3_rows(csv_file), config.batch_size):
            copied = copy_rows(
                conn,
                "stg_sharadar_sf3",
                [
                    "ticker",
                    "investor_name",
                    "security_type",
                    "calendar_date",
                    "value",
                    "units",
                    "price",
                    "raw_record",
                ],
                batch,
            )
            total += copied
            print(f"[info] copied {copied:,} SF3 rows to staging (running total {total:,})")

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM stg_sharadar_sf3")
            staged_count = cur.fetchone()[0]
        print(f"[info] staged sharadar SF3 rows: {staged_count:,}")

        upsert_instruments_from_sf3(conn, config.schema)
        upsert_sf3(conn, config.schema)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {config.schema}.sharadar_sf3")
            final_count = cur.fetchone()[0]

        print(f"[info] sharadar_sf3 row count: {final_count:,}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
