#!/usr/bin/env python3
"""
Shared infrastructure for all Sharadar data-ingestion scripts.

Protocol
--------
Every ingest script follows this sequence before loading data:

  1. ``fetch_api_sample()``      – pull 75 rows via the Nasdaq JSON API
  2. ``validate_sample_shape()`` – assert required columns are present;
                                   if not → log full structure and raise SystemExit(1)  [rule 2c]
  3. ``detect_schema_drift()``   – compare API columns to the known column set;
                                   if drift → truncate + full bulk reload              [rule 2b]
  4. If DB empty or --overwrite  → full bulk reload
  5. Otherwise                   → incremental load via ``fetch_incremental_rows()``   [rule 2a]
     (window: max_db_date - 1 day → yesterday, so latest day is always refreshed)

Shared helpers
--------------
This module provides all common infrastructure so individual ingest scripts
only need to define their dataset-specific constants, DDL, row parsing, and
upsert SQL.  The ``run_ingest()`` orchestrator executes the full protocol.
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import time
import zipfile
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, Iterable, Iterator, Optional

import dotenv
import psycopg
import requests

# ── Project paths ─────────────────────────────────────────────────────────────

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ENV_FILE = _PROJECT_ROOT / ".env"

_NASDAQ_TABLE_API = "https://data.nasdaq.com/api/v3/datatables/{table}.json"
_NASDAQ_CSV_API = "https://data.nasdaq.com/api/v3/datatables/{table}.csv"


# ── Config ────────────────────────────────────────────────────────────────────

@dataclass
class IngestConfig:
    """Shared configuration for all Sharadar ingest scripts."""

    api_key: str
    db_dsn: str
    schema: str = "strategy_engine"
    batch_size: int = 100_000
    download_retries: int = 5
    request_timeout_connect: int = 30
    request_timeout_read: int = 600
    overwrite: bool = False
    poll_seconds: int = 5
    max_wait_seconds: int = 300


# ── Environment / credentials ────────────────────────────────────────────────

def load_env() -> None:
    """Load the project .env file if it exists."""
    if _ENV_FILE.exists():
        dotenv.load_dotenv(_ENV_FILE)


def resolve_credentials() -> tuple[str, str]:
    """Return ``(api_key, db_dsn)`` from env, raising SystemExit if missing."""
    load_env()
    api_key = (
        os.getenv("NDL_API_KEY")
        or os.getenv("NASDAQ_DATA_LINK_API_KEY")
        or os.getenv("NASDAQ_API_KEY")
    )
    db_dsn = os.getenv("DATABASE_URL") or os.getenv("DB_DSN")
    if not api_key:
        raise SystemExit(
            "Missing NDL_API_KEY (or NASDAQ_DATA_LINK_API_KEY / NASDAQ_API_KEY) "
            "in .env or environment"
        )
    if not db_dsn:
        raise SystemExit("Missing DATABASE_URL (or DB_DSN) in .env or environment")
    return api_key, db_dsn


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_ingest_args(
    description: str,
    *,
    extra_args: Callable[[argparse.ArgumentParser], None] | None = None,
) -> argparse.Namespace:
    """Build a standard ingest argparse parser and return parsed args."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--schema", default=os.getenv("DB_SCHEMA", "strategy_engine"))
    parser.add_argument("--batch-size", type=int, default=100_000)
    parser.add_argument("--overwrite", action="store_true")
    if extra_args:
        extra_args(parser)
    return parser.parse_args()


# ── Parsing helpers ───────────────────────────────────────────────────────────

def normalise_header(name: str) -> str:
    """Lowercase, strip, and replace spaces with underscores."""
    return name.strip().lower().replace(" ", "_")


def row_get(row: dict[str, str], *names: str) -> Optional[str]:
    """Return the first non-empty value for *names* in *row*, or None."""
    for name in names:
        key = normalise_header(name)
        if key in row:
            value = row[key]
            if value is None:
                return None
            value = value.strip()
            return value if value != "" else None
    return None


def parse_decimal(value: Optional[str]) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError):
        return None


def parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def parse_numeric(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_bool(value: Optional[str], *, default: bool = False) -> bool:
    if value is None:
        return default
    v = value.strip().lower()
    if v in {"true", "t", "1", "y", "yes"}:
        return True
    if v in {"false", "f", "0", "n", "no"}:
        return False
    return default


def parse_iso_date(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return value if value else None


# ── Iteration helpers ─────────────────────────────────────────────────────────

def chunked(rows: Iterable, size: int) -> Iterator[list]:
    """Yield successive *size*-length chunks from *rows*."""
    buf: list = []
    for row in rows:
        buf.append(row)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


# ── DB helpers ────────────────────────────────────────────────────────────────

def copy_rows(
    conn: psycopg.Connection,
    table_name: str,
    columns: list[str],
    rows: Iterable[tuple],
) -> int:
    """COPY *rows* into *table_name* via the fast binary path.  Returns count."""
    total = 0
    with conn.cursor() as cur:
        with cur.copy(
            f"COPY {table_name} ({', '.join(columns)}) FROM STDIN"
        ) as copy:
            for row in rows:
                copy.write_row(row)
                total += 1
    return total


def ensure_instruments_table(conn: psycopg.Connection, schema: str) -> None:
    """Create the schema and shared ``instruments`` table if they don't exist."""
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
    conn.commit()


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def request_with_retries(
    session: requests.Session,
    url: str,
    *,
    max_attempts: int = 5,
    timeout: tuple[int, int] = (30, 600),
    stream: bool = False,
) -> requests.Response:
    """GET *url* with exponential back-off retry."""
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, timeout=timeout, stream=stream)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt == max_attempts:
                break
            sleep_s = min(2 ** attempt, 60)
            print(f"[warn] request failed ({attempt}/{max_attempts}): {exc}")
            print(f"[info] retrying in {sleep_s}s…")
            time.sleep(sleep_s)
    assert last_exc is not None
    raise last_exc


# ── Sample download + validation ──────────────────────────────────────────────

def fetch_api_sample(
    session: requests.Session,
    table_code: str,
    api_key: str,
    *,
    n: int = 75,
    max_attempts: int = 5,
    timeout: tuple[int, int] = (30, 120),
) -> tuple[list[str], list[dict[str, str]]]:
    """
    Fetch *n* rows from the Nasdaq Data Link Tables JSON API.

    Returns ``(normalised_column_names, list_of_row_dicts)``.
    """
    url = (
        f"{_NASDAQ_TABLE_API.format(table=table_code)}"
        f"?api_key={api_key}&qopts.per_page={n}"
    )
    print(f"[info] {table_code}: fetching {n}-row sample for validation…")
    resp = request_with_retries(session, url, max_attempts=max_attempts, timeout=timeout)
    payload = resp.json()

    columns = [normalise_header(c["name"]) for c in payload["datatable"]["columns"]]
    rows: list[dict[str, str]] = [
        {col: (str(v) if v is not None else "") for col, v in zip(columns, row)}
        for row in payload["datatable"]["data"]
    ]
    return columns, rows


def validate_sample_shape(
    table_code: str,
    columns: list[str],
    rows: list[dict[str, str]],
    required_cols: set[str],
    *,
    min_rows: int = 1,
) -> None:
    """
    Assert that every column in *required_cols* is present in *columns* and that
    at least *min_rows* sample rows were returned.

    On failure: prints a full diagnostic of the received shape and raises
    ``SystemExit(1)``  [rule 2c – wrong shape, cannot safely ingest].
    """
    missing = required_cols - set(columns)
    ok = (not missing) and len(rows) >= min_rows
    if not ok:
        print(f"[error] {table_code}: sample data has wrong shape – aborting (rule 2c).")
        print(f"[error]   rows received   : {len(rows)}")
        print(f"[error]   columns received: {sorted(columns)}")
        print(f"[error]   required columns: {sorted(required_cols)}")
        if missing:
            print(f"[error]   MISSING columns : {sorted(missing)}")
        if rows:
            # Print first row (truncated) so the caller can see actual field names / values
            sample_display = dict(list(rows[0].items())[:30])
            print(f"[error]   first sample row: {sample_display}")
        raise SystemExit(1)
    print(
        f"[info] {table_code}: sample shape OK – "
        f"{len(rows)} rows, {len(columns)} columns: {sorted(columns)}"
    )


# ── Schema-drift detection ────────────────────────────────────────────────────

def detect_schema_drift(
    table_code: str,
    api_cols: set[str],
    known_cols: set[str],
    *,
    ignore_cols: Optional[set[str]] = None,
) -> bool:
    """
    Return ``True`` if *api_cols* differs from *known_cols* (after removing
    *ignore_cols*).  Logs the diff when drift is found.

    Triggers rule 2b: caller should truncate + do a full bulk reload.
    """
    if ignore_cols:
        api_cols = api_cols - ignore_cols
        known_cols = known_cols - ignore_cols

    added = api_cols - known_cols
    removed = known_cols - api_cols
    if added or removed:
        print(
            f"[warn] {table_code}: schema drift detected → "
            f"will truncate and do a full reload (rule 2b)"
        )
        if added:
            print(f"[warn]   new columns    : {sorted(added)}")
        if removed:
            print(f"[warn]   removed columns: {sorted(removed)}")
        return True
    print(f"[info] {table_code}: schema unchanged ({len(api_cols)} data columns)")
    return False


# ── Incremental download ──────────────────────────────────────────────────────

def fetch_incremental_rows(
    session: requests.Session,
    table_code: str,
    api_key: str,
    *,
    date_filter_col: str,
    since_date: str,
    until_date: str,
    per_page: int = 50_000,
    max_attempts: int = 5,
    timeout: tuple[int, int] = (30, 600),
) -> tuple[list[str], list[dict[str, str]]]:
    """
    Download all rows where ``date_filter_col BETWEEN since_date AND until_date``
    via the paginated Nasdaq JSON API (rule 2a).

    *since_date* should be ``max_db_date - 1 day`` so the most-recent previously
    loaded entry is always re-fetched (it may have been incomplete).
    *until_date* should be ``yesterday`` so today's potentially-partial data is
    never ingested.

    Returns ``(normalised_column_names, list_of_row_dicts)``.
    """
    base = _NASDAQ_TABLE_API.format(table=table_code)
    columns: list[str] = []
    all_rows: list[dict[str, str]] = []
    cursor_id: Optional[str] = None
    page = 0

    while True:
        page += 1
        url = (
            f"{base}?api_key={api_key}"
            f"&qopts.per_page={per_page}"
            f"&{date_filter_col}.gte={since_date}"
            f"&{date_filter_col}.lte={until_date}"
        )
        if cursor_id:
            url += f"&qopts.cursor_id={cursor_id}"

        print(
            f"[info] {table_code}: incremental page {page} "
            f"(rows so far: {len(all_rows):,})…"
        )
        resp = request_with_retries(session, url, max_attempts=max_attempts, timeout=timeout)
        payload = resp.json()

        if not columns:
            columns = [
                normalise_header(c["name"]) for c in payload["datatable"]["columns"]
            ]

        for row in payload["datatable"]["data"]:
            all_rows.append(
                {col: (str(v) if v is not None else "") for col, v in zip(columns, row)}
            )

        cursor_id = (payload.get("meta") or {}).get("next_cursor_id")
        if not cursor_id:
            break

    print(
        f"[info] {table_code}: downloaded {len(all_rows):,} incremental rows "
        f"({page} page(s)) for window {since_date} → {until_date}"
    )
    return columns, all_rows


# ── Bulk download ─────────────────────────────────────────────────────────────

def download_bulk_csv(
    session: requests.Session,
    table_code: str,
    api_key: str,
    config: IngestConfig,
) -> io.TextIOBase:
    """
    Download the full bulk export for *table_code* from Nasdaq Data Link.

    Polls the export API until the file is ready, downloads the ZIP,
    and returns the first CSV inside it as a text stream.
    """
    export_url = (
        f"{_NASDAQ_CSV_API.format(table=table_code)}"
        f"?qopts.export=true&api_key={api_key}"
    )
    deadline = time.time() + config.max_wait_seconds

    while True:
        resp = session.get(
            export_url,
            timeout=(config.request_timeout_connect, 300),
        )
        resp.raise_for_status()

        reader = csv.DictReader(io.StringIO(resp.text))
        row = next(reader, None)
        if row is None:
            raise RuntimeError(
                f"No metadata row returned for bulk export {table_code}"
            )

        link = (row.get("file.link") or "").strip()
        status = (row.get("file.status") or "").strip()

        print(
            f"[debug] bulk export {table_code}: "
            f"status={status!r}, link_present={bool(link)}"
        )

        if status.lower() == "fresh":
            if not link:
                raise RuntimeError(
                    f"Bulk export {table_code} is Fresh but file.link is empty"
                )
            print(f"[info] downloading full bulk export for {table_code}…")
            dl_resp = request_with_retries(
                session,
                link,
                max_attempts=config.download_retries,
                timeout=(config.request_timeout_connect, config.request_timeout_read),
            )
            return _first_csv_from_bytes(dl_resp.content)

        if status.lower() in {"creating", "regenerating"}:
            if time.time() >= deadline:
                raise TimeoutError(
                    f"Timed out waiting for Nasdaq export {table_code} "
                    f"to become Fresh"
                )
            time.sleep(config.poll_seconds)
            continue

        raise RuntimeError(
            f"Unexpected bulk export status for {table_code}: "
            f"{status!r}; row={row}"
        )


def _first_csv_from_bytes(data: bytes) -> io.TextIOBase:
    """Extract the first CSV from a ZIP (or raw CSV) byte payload."""
    if zipfile.is_zipfile(io.BytesIO(data)):
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            csv_names = [
                n for n in zf.namelist() if n.lower().endswith(".csv")
            ]
            if not csv_names:
                raise RuntimeError(
                    f"ZIP download contained no CSV files: {zf.namelist()}"
                )
            name = csv_names[0]
            print(f"[debug] extracted CSV from ZIP: {name}")
            return io.TextIOWrapper(
                io.BytesIO(zf.read(name)),
                encoding="utf-8",
                errors="replace",
                newline="",
            )

    text = data.decode("utf-8", errors="replace")
    if text.lstrip().startswith("{"):
        raise RuntimeError(f"Nasdaq returned JSON instead of CSV: {text[:500]}")
    if "<html" in text[:500].lower():
        raise RuntimeError(f"Nasdaq returned HTML instead of CSV: {text[:500]}")
    return io.TextIOWrapper(
        io.BytesIO(data), encoding="utf-8", errors="replace", newline=""
    )


# ── Ingest orchestrator ──────────────────────────────────────────────────────

def run_ingest(
    *,
    table_code: str,
    required_cols: set[str],
    known_cols: set[str],
    date_filter_col: str | None,
    ensure_schema_fn: Callable[[psycopg.Connection, str], None],
    get_max_date_fn: Callable[[psycopg.Connection, str], str | None],
    stage_and_upsert_fn: Callable,
    iter_rows_csv_fn: Callable[[io.TextIOBase], Iterator[tuple]],
    iter_rows_dicts_fn: Callable[[list[dict[str, str]]], Iterator[tuple]],
    truncate_fn: Callable[[psycopg.Connection, str], None],
    count_fn: Callable[[psycopg.Connection, str], int],
    config: IngestConfig,
) -> int:
    """
    Execute the standard Sharadar ingest protocol.

    Individual scripts provide dataset-specific callables; this function
    handles the session, connection, validation, drift check, and
    full-vs-incremental decision.

    Set *date_filter_col* to ``None`` for reference tables that always
    do a full reload (e.g. INDICATORS).
    """
    session = requests.Session()
    session.headers.update({"User-Agent": "strategy_engine/1.0"})

    with psycopg.connect(config.db_dsn) as conn:
        ensure_schema_fn(conn, config.schema)

        # ── STEP 1: sample validation (rule 2c) ───────────────────────
        sample_cols, sample_rows = fetch_api_sample(
            session, table_code, config.api_key, n=75
        )
        validate_sample_shape(
            table_code, sample_cols, sample_rows, required_cols
        )

        # ── STEP 2: schema-drift check (rule 2b) ─────────────────────
        drift = detect_schema_drift(
            table_code, set(sample_cols), known_cols
        )

        def _do_full_load(*, truncate: bool) -> None:
            if truncate:
                truncate_fn(conn, config.schema)
            csv_file = download_bulk_csv(
                session, table_code, config.api_key, config
            )
            try:
                count = stage_and_upsert_fn(
                    conn, iter_rows_csv_fn(csv_file), config
                )
            finally:
                csv_file.close()
            if count == 0:
                raise RuntimeError(
                    f"Parsed zero rows from {table_code} export."
                )
            print(f"[ok] full bulk load complete – {count:,} rows staged")

        if date_filter_col is None:
            # Reference table: always full reload
            truncate = config.overwrite or drift
            if drift:
                print(
                    f"[info] {table_code}: schema drift detected – "
                    f"truncating before reload (rule 2b)"
                )
            elif config.overwrite:
                print(
                    f"[info] {table_code}: --overwrite requested – "
                    f"truncating before reload"
                )
            else:
                print(
                    f"[info] {table_code}: full reload "
                    f"(reference table, no incremental)"
                )
            _do_full_load(truncate=truncate)

        else:
            max_date = get_max_date_fn(conn, config.schema)

            if config.overwrite or max_date is None:
                reason = (
                    "--overwrite" if config.overwrite else "table is empty"
                )
                print(f"[info] {table_code}: full bulk load ({reason})")
                _do_full_load(truncate=config.overwrite)

            elif drift:
                print(
                    f"[info] {table_code}: full bulk reload "
                    f"due to schema drift (rule 2b)"
                )
                _do_full_load(truncate=True)

            else:
                # ── STEP 3: incremental load (rule 2a) ────────────────
                yesterday = (
                    date.today() - timedelta(days=1)
                ).isoformat()
                since = (
                    date.fromisoformat(max_date) - timedelta(days=1)
                ).isoformat()
                print(
                    f"[info] {table_code}: incremental load – "
                    f"DB max date: {max_date}, "
                    f"window: {since} → {yesterday}"
                )
                _cols, inc_rows = fetch_incremental_rows(
                    session,
                    table_code,
                    config.api_key,
                    date_filter_col=date_filter_col,
                    since_date=since,
                    until_date=yesterday,
                )
                if not inc_rows:
                    print(
                        f"[info] {table_code}: no new/updated rows "
                        f"in window {since} → {yesterday}"
                    )
                else:
                    count = stage_and_upsert_fn(
                        conn,
                        iter_rows_dicts_fn(inc_rows),
                        config,
                    )
                    print(
                        f"[info] incremental load complete – "
                        f"{count:,} rows staged"
                    )

        total = count_fn(conn, config.schema)

    print(f"[info] {table_code}: target table now contains {total:,} rows")
    return 0

