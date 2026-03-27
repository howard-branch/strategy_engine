#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

import dotenv
import psycopg
import requests
from psycopg.types.json import Json

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ENV_FILE = _PROJECT_ROOT / '.env'
if _ENV_FILE.exists():
    dotenv.load_dotenv(_ENV_FILE)

BASE_URL = 'https://data.nasdaq.com/api/v3/datatables/{table}.csv'
DEFAULT_TABLE = 'SHARADAR/SF1'
DEFAULT_SCHEMA = 'strategy_engine'

META_KEYS = {
    'ticker',
    'calendardate',
    'datekey',
    'reportperiod',
    'lastupdated',
    'dimension',
}

DATE_KEYS = {'calendardate', 'datekey', 'reportperiod', 'lastupdated'}


@dataclass
class Config:
    api_key: str
    db_dsn: str
    schema: str = DEFAULT_SCHEMA
    table: str = DEFAULT_TABLE
    batch_size: int = 2_000
    download_retries: int = 5
    request_timeout_connect: int = 30
    request_timeout_read: int = 600
    overwrite: bool = False
    poll_seconds: int = 5
    max_wait_seconds: int = 600
    sample_rows_for_type_detection: int = 50


def normalise_header(name: str) -> str:
    return name.strip().lower().replace(' ', '_')


def row_get(row: dict[str, str], *names: str) -> Optional[str]:
    for name in names:
        key = normalise_header(name)
        if key not in row:
            continue
        value = row[key]
        if value is None:
            return None
        value = value.strip()
        return value if value != '' else None
    return None


def parse_iso_date(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    return value


def build_export_url(table: str, api_key: str) -> str:
    return f"{BASE_URL.format(table=table)}?qopts.export=true&api_key={api_key}"


def request_with_retries(
    session: requests.Session,
    url: str,
    max_attempts: int,
    timeout: tuple[int, int],
) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, stream=True, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt == max_attempts:
                break
            sleep_s = min(2 ** attempt, 30)
            print(f'[warn] request failed ({attempt}/{max_attempts}): {exc}')
            print(f'[info] retrying in {sleep_s}s...')
            time.sleep(sleep_s)
    assert last_exc is not None
    raise last_exc


def download_zip_bytes(session: requests.Session, url: str, config: Config) -> bytes:
    deadline = time.time() + config.max_wait_seconds
    last_status: str | None = None

    while True:
        with request_with_retries(
            session,
            url,
            max_attempts=config.download_retries,
            timeout=(config.request_timeout_connect, config.request_timeout_read),
        ) as resp:
            meta_text = resp.content.decode('utf-8', errors='replace')
            reader = csv.DictReader(io.StringIO(meta_text))
            row = next(reader, None)
            if row is None:
                raise RuntimeError(f'Nasdaq bulk export metadata was empty for URL: {url}')

            file_link = (row.get('file.link') or '').strip()
            file_status = (row.get('file.status') or '').strip()
            last_status = file_status or last_status
            print(f"[debug] bulk export status={file_status!r} link_present={bool(file_link)}")

            if file_status.lower() == 'fresh':
                if not file_link:
                    raise RuntimeError('Bulk export is Fresh but file.link is empty')
                with request_with_retries(
                    session,
                    file_link,
                    max_attempts=config.download_retries,
                    timeout=(config.request_timeout_connect, config.request_timeout_read),
                ) as download_resp:
                    data = download_resp.content
                    if data[:2] != b'PK':
                        raise RuntimeError(
                            'Downloaded bulk file is not a ZIP. '
                            f'First 200 bytes: {data[:200]!r}'
                        )
                    return data

            if file_status.lower() in {'creating', 'regenerating'}:
                if time.time() >= deadline:
                    raise TimeoutError(
                        'Timed out waiting for bulk export to become Fresh. '
                        f'Last status={last_status!r}, url={url}'
                    )
                time.sleep(config.poll_seconds)
                continue

            raise RuntimeError(
                f'Unexpected Nasdaq bulk export status {file_status!r} for URL: {url}'
            )


def first_csv_from_response_bytes(data: bytes) -> io.TextIOBase:
    if data[:2] != b'PK':
        raise RuntimeError('Expected ZIP file from bulk export')
    zf = zipfile.ZipFile(io.BytesIO(data))
    csv_names = [name for name in zf.namelist() if name.lower().endswith('.csv')]
    if not csv_names:
        raise RuntimeError('Zip file did not contain a CSV')
    return io.TextIOWrapper(zf.open(csv_names[0], 'r'), encoding='utf-8', newline='')


def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
        cur.execute(
            f'''
            CREATE TABLE IF NOT EXISTS {schema}.fundamentals_sf1 (
                instrument_id BIGINT REFERENCES {schema}.instruments(instrument_id),
                ticker TEXT NOT NULL,
                dimension TEXT NOT NULL,
                calendardate DATE NOT NULL,
                datekey DATE,
                reportperiod DATE,
                lastupdated DATE,
                source_table TEXT NOT NULL DEFAULT 'SHARADAR/SF1',
                data JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (ticker, dimension, calendardate)
            )
            '''
        )


def maybe_truncate(conn: psycopg.Connection, schema: str, overwrite: bool) -> None:
    if not overwrite:
        return
    with conn.cursor() as cur:
        cur.execute(f'TRUNCATE TABLE {schema}.fundamentals_sf1')
    print(f'[info] truncated {schema}.fundamentals_sf1')


def create_staging_table(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute('DROP TABLE IF EXISTS stg_sf1')
        cur.execute(
            '''
            CREATE TEMP TABLE stg_sf1 (
                ticker TEXT,
                dimension TEXT,
                calendardate DATE,
                datekey DATE,
                reportperiod DATE,
                lastupdated DATE,
                source_table TEXT,
                data JSONB
            )
            '''
        )


def copy_rows(conn: psycopg.Connection, rows: Iterator[tuple], batch_size: int) -> int:
    total = 0
    with conn.cursor() as cur:
        with cur.copy(
            'COPY stg_sf1 (ticker, dimension, calendardate, datekey, reportperiod, lastupdated, source_table, data) FROM STDIN'
        ) as copy:
            for row in rows:
                copy.write_row(row)
                total += 1
                if total % batch_size == 0:
                    print(f'[info] staged {total:,} SF1 rows...')
    return total


def iter_sf1_rows(csv_file: io.TextIOBase, source_table: str) -> Iterator[tuple]:
    reader = csv.DictReader(csv_file)
    reader.fieldnames = [normalise_header(x) for x in (reader.fieldnames or [])]

    for row in reader:
        ticker = row_get(row, 'ticker', 'symbol')
        dimension = row_get(row, 'dimension')
        calendardate = parse_iso_date(row_get(row, 'calendardate'))
        if not ticker or not dimension or not calendardate:
            continue

        payload: dict[str, str] = {}
        for key, value in row.items():
            if value is None:
                continue
            value = value.strip()
            if value == '' or key in META_KEYS:
                continue
            payload[key] = value

        yield (
            ticker,
            dimension,
            calendardate,
            parse_iso_date(row_get(row, 'datekey')),
            parse_iso_date(row_get(row, 'reportperiod')),
            parse_iso_date(row_get(row, 'lastupdated')),
            source_table,
            Json(payload),
        )


def upsert_instruments_from_sf1(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f'''
            INSERT INTO {schema}.instruments (symbol, source_table, is_active, updated_at)
            SELECT DISTINCT s.ticker, 'SHARADAR/SF1', TRUE, NOW()
            FROM stg_sf1 s
            WHERE s.ticker IS NOT NULL
            ON CONFLICT (symbol) DO UPDATE
            SET updated_at = NOW()
            '''
        )


def upsert_sf1(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f'''
            WITH deduped AS (
                SELECT DISTINCT ON (s.ticker, s.dimension, s.calendardate)
                    s.ticker,
                    s.dimension,
                    s.calendardate,
                    s.datekey,
                    s.reportperiod,
                    s.lastupdated,
                    s.source_table,
                    s.data
                FROM stg_sf1 s
                WHERE s.ticker IS NOT NULL
                  AND s.dimension IS NOT NULL
                  AND s.calendardate IS NOT NULL
                ORDER BY
                    s.ticker,
                    s.dimension,
                    s.calendardate,
                    s.lastupdated DESC NULLS LAST
            )
            INSERT INTO {schema}.fundamentals_sf1 (
                instrument_id,
                ticker,
                dimension,
                calendardate,
                datekey,
                reportperiod,
                lastupdated,
                source_table,
                data,
                updated_at
            )
            SELECT
                i.instrument_id,
                d.ticker,
                d.dimension,
                d.calendardate,
                d.datekey,
                d.reportperiod,
                d.lastupdated,
                d.source_table,
                d.data,
                NOW()
            FROM deduped d
            LEFT JOIN {schema}.instruments i
              ON i.symbol = d.ticker
            ON CONFLICT (ticker, dimension, calendardate) DO UPDATE
            SET
                instrument_id = EXCLUDED.instrument_id,
                datekey = EXCLUDED.datekey,
                reportperiod = EXCLUDED.reportperiod,
                lastupdated = EXCLUDED.lastupdated,
                source_table = EXCLUDED.source_table,
                data = EXCLUDED.data,
                updated_at = NOW()
            '''
        )


def count_rows(conn: psycopg.Connection, schema: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM {schema}.fundamentals_sf1')
        return int(cur.fetchone()[0])


def main() -> int:
    parser = argparse.ArgumentParser(description='Download Sharadar SF1 fundamentals and load into Postgres.')
    parser.add_argument('--api-key', default=os.getenv('NDL_API_KEY') or os.getenv('NASDAQ_DATA_LINK_API_KEY') or os.getenv('NASDAQ_API_KEY'))
    parser.add_argument('--db-dsn', default=os.getenv('DATABASE_URL') or os.getenv('DB_DSN'))
    parser.add_argument('--schema', default=os.getenv('DB_SCHEMA', DEFAULT_SCHEMA))
    parser.add_argument('--table', default=DEFAULT_TABLE)
    parser.add_argument('--batch-size', type=int, default=2_000)
    parser.add_argument('--overwrite', action='store_true')
    args = parser.parse_args()

    if not args.api_key:
        raise SystemExit('Missing --api-key (or set NDL_API_KEY in .env).')
    if not args.db_dsn:
        raise SystemExit('Missing --db-dsn (or set DATABASE_URL / DB_DSN in .env).')

    config = Config(
        api_key=args.api_key,
        db_dsn=args.db_dsn,
        schema=args.schema,
        table=args.table,
        batch_size=args.batch_size,
        overwrite=args.overwrite,
    )

    session = requests.Session()
    session.headers.update({'User-Agent': 'strategy-engine/1.0'})

    export_url = build_export_url(config.table, config.api_key)
    print(f'[info] requesting bulk export for {config.table}')
    zip_bytes = download_zip_bytes(session, export_url, config)

    with psycopg.connect(config.db_dsn) as conn:
        ensure_schema(conn, config.schema)
        maybe_truncate(conn, config.schema, config.overwrite)
        create_staging_table(conn)

        csv_file = first_csv_from_response_bytes(zip_bytes)
        try:
            staged = copy_rows(
                conn,
                iter_sf1_rows(csv_file, source_table=config.table),
                batch_size=config.batch_size,
            )
        finally:
            csv_file.close()

        print(f'[info] staged {staged:,} SF1 rows')
        upsert_instruments_from_sf1(conn, config.schema)
        upsert_sf1(conn, config.schema)
        conn.commit()
        total = count_rows(conn, config.schema)

    print(f'[info] {config.schema}.fundamentals_sf1 now contains {total:,} rows')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
