#!/usr/bin/env python3
from __future__ import annotations

import csv
import io
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Optional

import dotenv
import psycopg
import requests


# Load .env from project root if present
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ENV_FILE = _PROJECT_ROOT / ".env"
if _ENV_FILE.exists():
    dotenv.load_dotenv(_ENV_FILE)


@dataclass
class Config:
    api_key: str
    db_dsn: str
    schema: str = "project_quant"
    batch_size: int = 100_000
    download_retries: int = 5
    request_timeout_connect: int = 30
    request_timeout_read: int = 600
    overwrite: bool = False


DATASET_CODE = "SHARADAR/EVENTS"


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


def parse_event_codes(value: Optional[str]) -> list[int]:
    if value is None:
        return []

    raw = value.strip()
    if not raw:
        return []

    parts = raw.replace(",", "|").split("|")
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
    # Nasdaq bulk exports are often ZIP files even when the metadata endpoint is .csv
    if zipfile.is_zipfile(io.BytesIO(data)):
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            csv_names = [name for name in zf.namelist() if name.lower().endswith(".csv")]
            if not csv_names:
                raise RuntimeError(f"ZIP download contained no CSV files: {zf.namelist()}")
            name = csv_names[0]
            print(f"[debug] extracted CSV from ZIP: {name}")
            # Use TextIOWrapper with newline='' for proper CSV handling
            return io.TextIOWrapper(io.BytesIO(zf.read(name)), encoding="utf-8", errors="replace", newline="")

    text = data.decode("utf-8", errors="replace")
    if text.lstrip().startswith("{"):
        raise RuntimeError(f"Nasdaq returned JSON instead of CSV: {text[:500]}")
    if "<html" in text[:500].lower():
        raise RuntimeError(f"Nasdaq returned HTML instead of CSV: {text[:500]}")
    # Use TextIOWrapper with newline='' for proper CSV handling
    return io.TextIOWrapper(io.BytesIO(data), encoding="utf-8", errors="replace", newline="")


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
            CREATE TABLE IF NOT EXISTS {schema}.sharadar_events (
                instrument_id BIGINT NOT NULL REFERENCES {schema}.instruments(instrument_id),
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
            f"""
            CREATE INDEX IF NOT EXISTS sharadar_events_event_date_idx
            ON {schema}.sharadar_events (event_date)
            """
        )

        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS sharadar_events_event_code_idx
            ON {schema}.sharadar_events (event_code)
            """
        )

        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS sharadar_events_instrument_date_idx
            ON {schema}.sharadar_events (instrument_id, event_date)
            """
        )

    conn.commit()


def truncate_tables(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {schema}.sharadar_events")
    conn.commit()
    print("[info] truncated existing sharadar_events data")


def create_staging_tables(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg_sharadar_events")
        cur.execute(
            """
            CREATE TEMP TABLE stg_sharadar_events (
                symbol TEXT,
                event_date DATE,
                event_code INTEGER,
                source_table TEXT,
                last_updated TIMESTAMPTZ
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


def iter_event_rows(csv_file: io.TextIOBase) -> Iterator[tuple]:
    reader = csv.DictReader(csv_file)
    reader.fieldnames = [normalise_header(x) for x in (reader.fieldnames or [])]

    debug_printed = False
    for row in reader:
        if not debug_printed:
            print(f"[debug] CSV columns: {reader.fieldnames}")
            debug_printed = True

        symbol = row_get(row, "ticker", "symbol")
        event_date = row_get(row, "date", "event_date")
        event_codes = parse_event_codes(row_get(row, "eventcodes", "eventcode"))
        source_table = row_get(row, "table") or "EVENTS"
        last_updated = row_get(row, "lastupdated", "last_updated")

        if not symbol or not event_date or not event_codes:
            continue

        for event_code in event_codes:
            yield (
                symbol,
                event_date,
                event_code,
                source_table,
                last_updated,
            )


def upsert_instruments_from_events(conn: psycopg.Connection, schema: str) -> None:
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
                s.symbol,
                'STOCK',
                'EVENTS',
                TRUE,
                NOW()
            FROM stg_sharadar_events s
            WHERE s.symbol IS NOT NULL
            ON CONFLICT (symbol) DO UPDATE
            SET
                updated_at = NOW(),
                source_table = COALESCE({schema}.instruments.source_table, EXCLUDED.source_table)
            """
        )


def upsert_events(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {schema}.sharadar_events (
                instrument_id,
                event_date,
                event_code,
                source_table,
                last_updated,
                updated_at
            )
            SELECT
                i.instrument_id,
                s.event_date,
                s.event_code,
                s.source_table,
                s.last_updated,
                NOW()
            FROM stg_sharadar_events s
            JOIN {schema}.instruments i
              ON i.symbol = s.symbol
            WHERE s.symbol IS NOT NULL
              AND s.event_date IS NOT NULL
              AND s.event_code IS NOT NULL
            ON CONFLICT (instrument_id, event_date, event_code) DO UPDATE
            SET
                source_table = EXCLUDED.source_table,
                last_updated = EXCLUDED.last_updated,
                updated_at = NOW()
            """
        )


def parse_args() -> Config:
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Ingest SHARADAR/EVENTS into Postgres")
    parser.add_argument("--api-key", default=os.getenv("NDL_API_KEY") or os.getenv("NASDAQ_DATA_LINK_API_KEY") or os.getenv("NASDAQ_API_KEY"))
    parser.add_argument("--db-dsn", default=os.getenv("DATABASE_URL") or os.getenv("DB_DSN"))
    parser.add_argument("--schema", default=os.getenv("DB_SCHEMA", "project_quant"))
    parser.add_argument("--batch-size", type=int, default=100_000)
    parser.add_argument("--overwrite", action="store_true")

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
        ensure_schema(conn, config.schema)
        if config.overwrite:
            truncate_tables(conn, config.schema)

        create_staging_tables(conn)

        csv_file = download_csv_file(session, config)
        total = 0

        for batch in chunked(iter_event_rows(csv_file), config.batch_size):
            copied = copy_rows(
                conn,
                "stg_sharadar_events",
                ["symbol", "event_date", "event_code", "source_table", "last_updated"],
                batch,
            )
            total += copied
            print(f"[info] copied {copied:,} event rows to staging (running total {total:,})")

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM stg_sharadar_events")
            staged_count = cur.fetchone()[0]
            print(f"[info] staged sharadar events rows: {staged_count:,}")


        upsert_instruments_from_events(conn, config.schema)
        upsert_events(conn, config.schema)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {config.schema}.sharadar_events")
            final_count = cur.fetchone()[0]
            print(f"[info] final sharadar_events rows in {config.schema}: {final_count:,}")


    return 0


if __name__ == "__main__":
    raise SystemExit(main())
