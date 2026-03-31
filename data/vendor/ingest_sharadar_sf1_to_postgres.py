#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import os
import tempfile
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional
from datetime import date, timedelta
import sys

import dotenv
import psycopg
import requests
from psycopg.types.json import Json

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ENV_FILE = _PROJECT_ROOT / '.env'
if _ENV_FILE.exists():
    dotenv.load_dotenv(_ENV_FILE)

# Shared Sharadar helpers (sample validation, schema drift detection)
_VENDOR_DIR = Path(__file__).resolve().parent
if str(_VENDOR_DIR) not in sys.path:
    sys.path.insert(0, str(_VENDOR_DIR))

from sharadar_common import fetch_api_sample, validate_sample_shape

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

# Columns the SF1 JSON API must return for us to be able to ingest (rule 2c).
REQUIRED_SF1_COLS: set[str] = {'ticker', 'dimension', 'calendardate', 'lastupdated'}


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
    verify_quality: bool = True
    log_metrics: bool = True


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
        except (
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
        ) as exc:
            last_exc = exc
            if attempt == max_attempts:
                break
            sleep_s = min(2 ** attempt, 60)
            print(f'[warn] request failed ({attempt}/{max_attempts}): {type(exc).__name__}: {exc}')
            print(f'[info] retrying in {sleep_s}s...')
            time.sleep(sleep_s)
        except Exception as exc:
            # Other exceptions should fail immediately
            raise

    assert last_exc is not None
    raise last_exc





def _stream_to_file(
        session: requests.Session,
        url: str,
        tmp_path: str,
        resume_from: int,
        config: Config,
        chunk_size: int = 1024 * 1024,  # 1 MB
) -> None:
    """Stream *url* into *tmp_path*, resuming from *resume_from* bytes if > 0."""
    headers: dict[str, str] = {}
    if resume_from > 0:
        headers['Range'] = f'bytes={resume_from}-'

    mode = 'ab' if resume_from > 0 else 'wb'
    with session.get(
            url,
            stream=True,
            headers=headers,
            timeout=(config.request_timeout_connect, 900),  # 15 min for large file
    ) as resp:
        resp.raise_for_status()
        with open(tmp_path, mode) as fh:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    fh.write(chunk)


def download_zip_bytes(session: requests.Session, url: str, config: Config) -> bytes:
    deadline = time.time() + config.max_wait_seconds
    last_status: str | None = None
    max_download_attempts = 5

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

                # Stream to a temp file so partial progress survives a retry.
                # On each retry, send a Range header to resume where we left off.
                tmp_fd, tmp_path = tempfile.mkstemp(suffix='.zip')
                os.close(tmp_fd)
                try:
                    downloaded = 0
                    for attempt in range(1, max_download_attempts + 1):
                        try:
                            print(
                                f'[info] downloading bulk file '
                                f'(attempt {attempt}/{max_download_attempts}, '
                                f'resume from {downloaded:,} bytes)...'
                            )
                            _stream_to_file(session, file_link, tmp_path, downloaded, config)
                            downloaded = Path(tmp_path).stat().st_size
                            break  # success
                        except (
                                requests.exceptions.ChunkedEncodingError,
                                requests.exceptions.ConnectionError,
                                requests.exceptions.Timeout,
                        ) as exc:
                            downloaded = Path(tmp_path).stat().st_size
                            if attempt >= max_download_attempts:
                                raise RuntimeError(
                                    f'Failed to download bulk file after {max_download_attempts} attempts: {exc}'
                                ) from exc
                            sleep_s = min(2 ** attempt, 60)
                            print(
                                f'[warn] download interrupted at {downloaded:,} bytes: '
                                f'{type(exc).__name__}: {exc}'
                            )
                            print(f'[info] retrying in {sleep_s}s with Range resume...')
                            time.sleep(sleep_s)

                    # Validate the downloaded file is a ZIP.
                    with open(tmp_path, 'rb') as fh:
                        header = fh.read(2)
                    if header != b'PK':
                        raise RuntimeError(
                            f'Downloaded file is not a valid ZIP (magic={header!r}). '
                            f'Size={downloaded:,} bytes.'
                        )

                    print(f'[info] successfully downloaded {downloaded:,} bytes')
                    with open(tmp_path, 'rb') as fh:
                        return fh.read()
                finally:
                    Path(tmp_path).unlink(missing_ok=True)

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
    raise AssertionError('unreachable')  # while True always returns or raises


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


def get_csv_fieldnames_from_zip(data: bytes) -> list[str]:
    """Return normalised column names from the first CSV inside the ZIP without consuming the stream."""
    zf = zipfile.ZipFile(io.BytesIO(data))
    csv_names = [name for name in zf.namelist() if name.lower().endswith('.csv')]
    if not csv_names:
        raise RuntimeError('Zip file did not contain a CSV')
    with io.TextIOWrapper(zf.open(csv_names[0], 'r'), encoding='utf-8', newline='') as fh:
        raw_header = next(csv.reader(fh), [])
    return [normalise_header(h) for h in raw_header]


def get_existing_data_fields(conn: psycopg.Connection, schema: str) -> set[str] | None:
    """
    Return the set of JSONB keys currently stored in fundamentals_sf1.
    Returns *None* when the table is empty (no prior data to compare against).
    """
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM {schema}.fundamentals_sf1')
        count = cur.fetchone()[0]
        if count == 0:
            return None
        cur.execute(
            f'''
            SELECT DISTINCT jsonb_object_keys(data)
            FROM {schema}.fundamentals_sf1
            LIMIT 500000
            '''
        )
        return {row[0] for row in cur.fetchall()}


def get_max_lastupdated(conn: psycopg.Connection, schema: str) -> str | None:
    """Return the most-recent lastupdated date stored in the DB, or None if empty."""
    with conn.cursor() as cur:
        cur.execute(f'SELECT MAX(lastupdated) FROM {schema}.fundamentals_sf1')
        result = cur.fetchone()[0]
        return str(result) if result else None


# ── Nasdaq Data Link paginated JSON API helpers ───────────────────────────────

_TABLE_API_BASE = 'https://data.nasdaq.com/api/v3/datatables/{table}.json'


def _build_table_api_url(
    table: str,
    api_key: str,
    *,
    per_page: int = 50_000,
    filters: dict[str, str] | None = None,
    cursor_id: str | None = None,
) -> str:
    url = _TABLE_API_BASE.format(table=table) + f'?api_key={api_key}&qopts.per_page={per_page}'
    if filters:
        for k, v in filters.items():
            url += f'&{k}={v}'
    if cursor_id:
        url += f'&qopts.cursor_id={cursor_id}'
    return url


def fetch_api_page(session: requests.Session, url: str, config: Config) -> dict:
    """Fetch a single JSON page from the Nasdaq Data Link Tables API."""
    resp = request_with_retries(
        session,
        url,
        max_attempts=config.download_retries,
        timeout=(config.request_timeout_connect, config.request_timeout_read),
    )
    return resp.json()


def fetch_schema_sample(session: requests.Session, config: Config, n: int = 100) -> set[str]:
    """
    Download *n* rows from the live API (no date filter) and return the set of
    data field names (META_KEYS excluded).  Used to detect schema changes before
    committing to a full download.
    """
    url = _build_table_api_url(config.table, config.api_key, per_page=n)
    payload = fetch_api_page(session, url, config)
    all_columns = {normalise_header(c['name']) for c in payload['datatable']['columns']}
    return all_columns - META_KEYS


def download_incremental_rows(
    session: requests.Session,
    config: Config,
    since_date: str,
    until_date: str,
) -> tuple[list[str], list[dict[str, str]]]:
    """
    Download rows where ``since_date <= lastupdated <= until_date`` via the
    paginated JSON API.  Returns ``(normalised_column_names, list_of_row_dicts)``.

    ``until_date`` should normally be yesterday so that today's potentially
    incomplete data is never ingested.  ``since_date`` is set one day before
    ``max_lastupdated`` so the most-recent previously-loaded entry is always
    re-fetched (it may have been partial when first loaded).
    """
    columns: list[str] = []
    all_dicts: list[dict[str, str]] = []
    cursor_id: str | None = None
    page = 0

    while True:
        page += 1
        url = _build_table_api_url(
            config.table,
            config.api_key,
            per_page=50_000,
            filters={'lastupdated.gte': since_date, 'lastupdated.lte': until_date},
            cursor_id=cursor_id,
        )
        print(f'[info] incremental page {page} (rows so far: {len(all_dicts):,})...')
        payload = fetch_api_page(session, url, config)

        if not columns:
            columns = [normalise_header(c['name']) for c in payload['datatable']['columns']]

        for row in payload['datatable']['data']:
            all_dicts.append(
                {col: ('' if v is None else str(v)) for col, v in zip(columns, row)}
            )

        cursor_id = (payload.get('meta') or {}).get('next_cursor_id')
        if not cursor_id:
            break

    print(f'[info] downloaded {len(all_dicts):,} incremental rows across {page} page(s)')
    return columns, all_dicts


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


def _sf1_row_to_tuple(row: dict[str, str], source_table: str) -> tuple | None:
    """Convert a normalised row dict to a staging tuple, or None if required fields are missing."""
    ticker = row_get(row, 'ticker', 'symbol')
    dimension = row_get(row, 'dimension')
    calendardate = parse_iso_date(row_get(row, 'calendardate'))
    if not ticker or not dimension or not calendardate:
        return None

    payload: dict[str, str] = {}
    for key, value in row.items():
        if value is None:
            continue
        value = value.strip() if isinstance(value, str) else str(value)
        if value == '' or key in META_KEYS:
            continue
        payload[key] = value

    return (
        ticker,
        dimension,
        calendardate,
        parse_iso_date(row_get(row, 'datekey')),
        parse_iso_date(row_get(row, 'reportperiod')),
        parse_iso_date(row_get(row, 'lastupdated')),
        source_table,
        Json(payload),
    )


def iter_sf1_rows(csv_file: io.TextIOBase, source_table: str) -> Iterator[tuple]:
    reader = csv.DictReader(csv_file)
    reader.fieldnames = [normalise_header(x) for x in (reader.fieldnames or [])]
    for row in reader:
        result = _sf1_row_to_tuple(row, source_table)
        if result is not None:
            yield result


def iter_sf1_rows_from_dicts(rows: list[dict[str, str]], source_table: str) -> Iterator[tuple]:
    """Variant of iter_sf1_rows for rows already loaded as dicts (incremental JSON path)."""
    for row in rows:
        result = _sf1_row_to_tuple(row, source_table)
        if result is not None:
            yield result


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
                s.ticker,
                s.dimension,
                s.calendardate,
                s.datekey,
                s.reportperiod,
                s.lastupdated,
                s.source_table,
                s.data,
                NOW()
            FROM stg_sf1 s
            LEFT JOIN {schema}.instruments i
              ON i.symbol = s.ticker
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


def _do_load(conn: psycopg.Connection, rows_iter: Iterator[tuple], config: Config) -> int:
    """Stage *rows_iter* into stg_sf1 and upsert into fundamentals_sf1. Returns staged count."""
    create_staging_table(conn)
    staged = copy_rows(conn, rows_iter, config.batch_size)
    print(f'[info] staged {staged:,} SF1 rows')
    upsert_instruments_from_sf1(conn, config.schema)
    upsert_sf1(conn, config.schema)
    conn.commit()
    return staged


def _full_bulk_load(
    session: requests.Session,
    conn: psycopg.Connection,
    config: Config,
    truncate: bool,
) -> int:
    """Download the full bulk ZIP and load it. Returns staged row count."""
    export_url = build_export_url(config.table, config.api_key)
    print(f'[info] requesting full bulk export for {config.table}...')
    zip_bytes = download_zip_bytes(session, export_url, config)

    fieldnames = get_csv_fieldnames_from_zip(zip_bytes)
    data_fields = {f for f in fieldnames if f not in META_KEYS}
    print(f'[info] bulk CSV: {len(fieldnames)} column(s), {len(data_fields)} data field(s)')

    if truncate:
        with conn.cursor() as cur:
            cur.execute(f'TRUNCATE TABLE {config.schema}.fundamentals_sf1')
        print(f'[info] truncated {config.schema}.fundamentals_sf1')

    csv_file = first_csv_from_response_bytes(zip_bytes)
    try:
        return _do_load(conn, iter_sf1_rows(csv_file, config.table), config)
    finally:
        csv_file.close()


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

    with psycopg.connect(config.db_dsn) as conn:
        ensure_schema(conn, config.schema)

        # ── STEP 1: sample validation (rule 2c) ───────────────────────────────
        # Always download 75 rows first and verify the expected columns are
        # present before attempting any load.
        sample_cols, sample_rows = fetch_api_sample(
            session, config.table, config.api_key, n=75
        )
        validate_sample_shape(
            config.table, sample_cols, sample_rows, REQUIRED_SF1_COLS
        )

        max_lastupdated = get_max_lastupdated(conn, config.schema)

        # ── FULL BULK LOAD ────────────────────────────────────────────────────
        # Triggered when: table is empty, or --overwrite was requested.
        if max_lastupdated is None or config.overwrite:
            reason = '--overwrite requested' if config.overwrite else 'table is empty'
            print(f'[info] full bulk load ({reason})')
            _full_bulk_load(session, conn, config, truncate=config.overwrite)

        # ── INCREMENTAL LOAD ──────────────────────────────────────────────────
        # Fetch only rows updated since the last run; fall back to full reload
        # if the API field schema has changed (rule 2b).
        else:
            # Compare live API data fields (non-meta) against what is stored
            # in the DB JSONB column.
            api_fields = set(sample_cols) - META_KEYS
            existing_fields = get_existing_data_fields(conn, config.schema)

            schema_ok = True
            if existing_fields is not None:
                added = api_fields - existing_fields
                removed = existing_fields - api_fields
                if added or removed:
                    schema_ok = False
                    print(
                        f'[warn] {config.table}: field schema changed: '
                        f'+{len(added)} new, -{len(removed)} removed → '
                        f'full bulk reload (rule 2b)'
                    )
                    if added:
                        print(f'[warn]   new fields    : {sorted(added)}')
                    if removed:
                        print(f'[warn]   removed fields: {sorted(removed)}')
                else:
                    print(f'[info] schema unchanged ({len(api_fields)} data fields)')

            if not schema_ok:
                _full_bulk_load(session, conn, config, truncate=True)
            else:
                # Download rows in the window [max_lastupdated - 1 day, yesterday].
                # Lower bound re-fetches the most-recent previously-loaded entry
                # in case it was incomplete; upper bound excludes today's partial data.
                yesterday = (date.today() - timedelta(days=1)).isoformat()
                since_date = (
                    date.fromisoformat(max_lastupdated) - timedelta(days=1)
                ).isoformat()
                print(
                    f'[info] incremental load – DB max lastupdated: {max_lastupdated}, '
                    f'window: {since_date} → {yesterday}'
                )
                _columns, inc_rows = download_incremental_rows(
                    session, config, since_date, until_date=yesterday
                )
                print(f'[info] received {len(inc_rows):,} updated / new rows')
                _do_load(conn, iter_sf1_rows_from_dicts(inc_rows, config.table), config)

        total = count_rows(conn, config.schema)

    print(f'[info] {config.schema}.fundamentals_sf1 now contains {total:,} rows')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
