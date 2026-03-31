#!/usr/bin/env python3
"""
Ingest SHARADAR/SF1 (fundamentals) into Postgres.

SF1 has unique requirements vs other Sharadar datasets:
  - Streaming download with HTTP Range resume (large file)
  - JSONB storage for variable data fields
  - Custom schema-drift detection (compares JSONB keys vs API fields)

It uses common helpers from sharadar_common but keeps its own main()
orchestration to accommodate these differences.
"""
from __future__ import annotations

import csv
import io
import os
import tempfile
import time
import zipfile
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator, Optional
import sys

import psycopg
import requests
from psycopg.types.json import Json

sys.path.insert(0, str(Path(__file__).resolve().parent))

from sharadar_common import (
    IngestConfig,
    copy_rows,
    ensure_instruments_table,
    fetch_api_sample,
    fetch_incremental_rows,
    normalise_header,
    parse_ingest_args,
    parse_iso_date,
    request_with_retries,
    resolve_credentials,
    row_get,
    validate_sample_shape,
)

TABLE_CODE = "SHARADAR/SF1"
REQUIRED_SF1_COLS: set[str] = {"ticker", "dimension", "calendardate", "lastupdated"}

META_KEYS = {
    "ticker", "calendardate", "datekey", "reportperiod",
    "lastupdated", "dimension",
}

_STAGING_COLUMNS = [
    "ticker", "dimension", "calendardate", "datekey",
    "reportperiod", "lastupdated", "source_table", "data",
]


# ── Schema / DDL ─────────────────────────────────────────────────────────────

def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    ensure_instruments_table(conn, schema)
    with conn.cursor() as cur:
        cur.execute(
            f"""
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
            """
        )
    conn.commit()


def get_max_lastupdated(conn: psycopg.Connection, schema: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(lastupdated) FROM {schema}.fundamentals_sf1")
        result = cur.fetchone()[0]
        return str(result) if result else None


def get_existing_data_fields(conn: psycopg.Connection, schema: str) -> set[str] | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.fundamentals_sf1")
        if cur.fetchone()[0] == 0:
            return None
        cur.execute(
            f"SELECT DISTINCT jsonb_object_keys(data) "
            f"FROM {schema}.fundamentals_sf1 LIMIT 500000"
        )
        return {row[0] for row in cur.fetchall()}


def count_rows(conn: psycopg.Connection, schema: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.fundamentals_sf1")
        return int(cur.fetchone()[0])


# ── Row parsing ──────────────────────────────────────────────────────────────

def _sf1_row_to_tuple(row: dict[str, str], source_table: str) -> tuple | None:
    ticker = row_get(row, "ticker", "symbol")
    dimension = row_get(row, "dimension")
    calendardate = parse_iso_date(row_get(row, "calendardate"))
    if not ticker or not dimension or not calendardate:
        return None
    payload: dict[str, str] = {}
    for key, value in row.items():
        if value is None:
            continue
        value = value.strip() if isinstance(value, str) else str(value)
        if value == "" or key in META_KEYS:
            continue
        payload[key] = value
    return (
        ticker, dimension, calendardate,
        parse_iso_date(row_get(row, "datekey")),
        parse_iso_date(row_get(row, "reportperiod")),
        parse_iso_date(row_get(row, "lastupdated")),
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


def iter_sf1_rows_from_dicts(
    rows: list[dict[str, str]], source_table: str,
) -> Iterator[tuple]:
    for row in rows:
        result = _sf1_row_to_tuple(row, source_table)
        if result is not None:
            yield result


# ── Staging + upsert ─────────────────────────────────────────────────────────

def _create_staging_table(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg_sf1")
        cur.execute(
            """
            CREATE TEMP TABLE stg_sf1 (
                ticker TEXT, dimension TEXT, calendardate DATE,
                datekey DATE, reportperiod DATE, lastupdated DATE,
                source_table TEXT, data JSONB
            )
            """
        )


def _do_load(
    conn: psycopg.Connection,
    rows_iter: Iterator[tuple],
    config: IngestConfig,
) -> int:
    _create_staging_table(conn)
    total = 0
    with conn.cursor() as cur:
        with cur.copy(
            "COPY stg_sf1 ("
            "ticker, dimension, calendardate, datekey, "
            "reportperiod, lastupdated, source_table, data"
            ") FROM STDIN"
        ) as copy:
            for row in rows_iter:
                copy.write_row(row)
                total += 1
                if total % config.batch_size == 0:
                    print(f"[info] staged {total:,} SF1 rows…")
    print(f"[info] staged {total:,} SF1 rows")

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {config.schema}.instruments
                (symbol, source_table, is_active, updated_at)
            SELECT DISTINCT s.ticker, 'SHARADAR/SF1', TRUE, NOW()
            FROM stg_sf1 s WHERE s.ticker IS NOT NULL
            ON CONFLICT (symbol) DO UPDATE SET updated_at = NOW()
            """
        )
        cur.execute(
            f"""
            INSERT INTO {config.schema}.fundamentals_sf1 (
                instrument_id, ticker, dimension, calendardate,
                datekey, reportperiod, lastupdated,
                source_table, data, updated_at
            )
            SELECT i.instrument_id, s.ticker, s.dimension, s.calendardate,
                   s.datekey, s.reportperiod, s.lastupdated,
                   s.source_table, s.data, NOW()
            FROM stg_sf1 s
            LEFT JOIN {config.schema}.instruments i ON i.symbol = s.ticker
            ON CONFLICT (ticker, dimension, calendardate) DO UPDATE SET
                instrument_id = EXCLUDED.instrument_id,
                datekey       = EXCLUDED.datekey,
                reportperiod  = EXCLUDED.reportperiod,
                lastupdated   = EXCLUDED.lastupdated,
                source_table  = EXCLUDED.source_table,
                data          = EXCLUDED.data,
                updated_at    = NOW()
            """
        )
    conn.commit()
    return total


# ── Streaming bulk download with resume ──────────────────────────────────────

def _stream_to_file(
    session: requests.Session,
    url: str,
    tmp_path: str,
    resume_from: int,
    config: IngestConfig,
    chunk_size: int = 1024 * 1024,
) -> None:
    headers: dict[str, str] = {}
    if resume_from > 0:
        headers["Range"] = f"bytes={resume_from}-"
    mode = "ab" if resume_from > 0 else "wb"
    with session.get(
        url, stream=True, headers=headers,
        timeout=(config.request_timeout_connect, 900),
    ) as resp:
        resp.raise_for_status()
        with open(tmp_path, mode) as fh:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    fh.write(chunk)


def _download_bulk_zip(
    session: requests.Session,
    config: IngestConfig,
) -> bytes:
    export_url = (
        f"https://data.nasdaq.com/api/v3/datatables/{TABLE_CODE}.csv"
        f"?qopts.export=true&api_key={config.api_key}"
    )
    deadline = time.time() + config.max_wait_seconds
    max_dl_attempts = 5

    while True:
        resp = request_with_retries(
            session, export_url,
            max_attempts=config.download_retries,
            timeout=(config.request_timeout_connect, config.request_timeout_read),
        )
        meta_text = resp.content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(meta_text))
        row = next(reader, None)
        if row is None:
            raise RuntimeError("Nasdaq bulk export metadata was empty")

        file_link = (row.get("file.link") or "").strip()
        file_status = (row.get("file.status") or "").strip()
        print(f"[debug] bulk export status={file_status!r} link_present={bool(file_link)}")

        if file_status.lower() == "fresh":
            if not file_link:
                raise RuntimeError("Bulk export is Fresh but file.link is empty")
            tmp_fd, tmp_path = tempfile.mkstemp(suffix=".zip")
            os.close(tmp_fd)
            try:
                downloaded = 0
                for attempt in range(1, max_dl_attempts + 1):
                    try:
                        print(
                            f"[info] downloading bulk file "
                            f"(attempt {attempt}/{max_dl_attempts}, "
                            f"resume from {downloaded:,} bytes)…"
                        )
                        _stream_to_file(session, file_link, tmp_path, downloaded, config)
                        downloaded = Path(tmp_path).stat().st_size
                        break
                    except (
                        requests.exceptions.ChunkedEncodingError,
                        requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout,
                    ) as exc:
                        downloaded = Path(tmp_path).stat().st_size
                        if attempt >= max_dl_attempts:
                            raise RuntimeError(
                                f"Failed after {max_dl_attempts} attempts: {exc}"
                            ) from exc
                        sleep_s = min(2 ** attempt, 60)
                        print(
                            f"[warn] download interrupted at {downloaded:,} bytes: "
                            f"{type(exc).__name__}: {exc}"
                        )
                        print(f"[info] retrying in {sleep_s}s with Range resume…")
                        time.sleep(sleep_s)

                with open(tmp_path, "rb") as fh:
                    header = fh.read(2)
                if header != b"PK":
                    raise RuntimeError(
                        f"Downloaded file is not a valid ZIP (magic={header!r}). "
                        f"Size={downloaded:,} bytes."
                    )
                print(f"[info] successfully downloaded {downloaded:,} bytes")
                with open(tmp_path, "rb") as fh:
                    return fh.read()
            finally:
                Path(tmp_path).unlink(missing_ok=True)

        if file_status.lower() in {"creating", "regenerating"}:
            if time.time() >= deadline:
                raise TimeoutError("Timed out waiting for bulk export to become Fresh")
            time.sleep(config.poll_seconds)
            continue

        raise RuntimeError(
            f"Unexpected bulk export status {file_status!r}"
        )


def _first_csv_from_zip(data: bytes) -> io.TextIOBase:
    zf = zipfile.ZipFile(io.BytesIO(data))
    csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    if not csv_names:
        raise RuntimeError("Zip file did not contain a CSV")
    return io.TextIOWrapper(zf.open(csv_names[0], "r"), encoding="utf-8", newline="")


def _full_bulk_load(
    session: requests.Session,
    conn: psycopg.Connection,
    config: IngestConfig,
    truncate: bool,
) -> int:
    print(f"[info] requesting full bulk export for {TABLE_CODE}…")
    zip_bytes = _download_bulk_zip(session, config)
    if truncate:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {config.schema}.fundamentals_sf1")
        print(f"[info] truncated {config.schema}.fundamentals_sf1")
    csv_file = _first_csv_from_zip(zip_bytes)
    try:
        return _do_load(conn, iter_sf1_rows(csv_file, TABLE_CODE), config)
    finally:
        csv_file.close()


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> int:
    args = parse_ingest_args("Ingest SHARADAR/SF1 into Postgres")
    api_key, db_dsn = resolve_credentials()
    config = IngestConfig(
        api_key=api_key, db_dsn=db_dsn,
        schema=args.schema, batch_size=args.batch_size,
        overwrite=args.overwrite,
    )

    session = requests.Session()
    session.headers.update({"User-Agent": "strategy-engine/1.0"})

    with psycopg.connect(config.db_dsn) as conn:
        ensure_schema(conn, config.schema)

        # STEP 1: sample validation (rule 2c)
        sample_cols, sample_rows = fetch_api_sample(
            session, TABLE_CODE, config.api_key, n=75
        )
        validate_sample_shape(TABLE_CODE, sample_cols, sample_rows, REQUIRED_SF1_COLS)

        max_lastupdated = get_max_lastupdated(conn, config.schema)

        # FULL BULK LOAD
        if max_lastupdated is None or config.overwrite:
            reason = "--overwrite requested" if config.overwrite else "table is empty"
            print(f"[info] full bulk load ({reason})")
            _full_bulk_load(session, conn, config, truncate=config.overwrite)

        # INCREMENTAL LOAD
        else:
            # Schema-drift check: compare API data fields vs stored JSONB keys
            api_fields = set(sample_cols) - META_KEYS
            existing_fields = get_existing_data_fields(conn, config.schema)
            schema_ok = True
            if existing_fields is not None:
                added = api_fields - existing_fields
                removed = existing_fields - api_fields
                if added or removed:
                    schema_ok = False
                    print(
                        f"[warn] {TABLE_CODE}: field schema changed: "
                        f"+{len(added)} new, -{len(removed)} removed → "
                        f"full bulk reload (rule 2b)"
                    )
                else:
                    print(f"[info] schema unchanged ({len(api_fields)} data fields)")

            if not schema_ok:
                _full_bulk_load(session, conn, config, truncate=True)
            else:
                yesterday = (date.today() - timedelta(days=1)).isoformat()
                since_date = (
                    date.fromisoformat(max_lastupdated) - timedelta(days=1)
                ).isoformat()
                print(
                    f"[info] incremental load – DB max lastupdated: "
                    f"{max_lastupdated}, window: {since_date} → {yesterday}"
                )
                _cols, inc_rows = fetch_incremental_rows(
                    session, TABLE_CODE, config.api_key,
                    date_filter_col="lastupdated",
                    since_date=since_date, until_date=yesterday,
                )
                print(f"[info] received {len(inc_rows):,} updated / new rows")
                _do_load(
                    conn, iter_sf1_rows_from_dicts(inc_rows, TABLE_CODE), config,
                )

        total = count_rows(conn, config.schema)

    print(f"[info] {config.schema}.fundamentals_sf1 now contains {total:,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
