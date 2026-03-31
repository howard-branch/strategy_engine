#!/usr/bin/env python3
"""
Ingest SHARADAR/SFP (fund daily prices) into Postgres.

SHARADAR/SFP provides daily OHLCV + adjusted close for US funds (ETFs, mutual
funds, etc.).

Follows the shared Sharadar ingest protocol (see sharadar_common.py):
  1. fetch_api_sample        → pull 75 rows for validation
  2. validate_sample_shape   → assert required columns present          [rule 2c]
  3. detect_schema_drift     → compare API columns to known set         [rule 2b]
  4. Full bulk load if DB empty / --overwrite / schema drift
  5. Incremental load otherwise                                         [rule 2a]
     (window: max_trade_date - 1 day → yesterday)
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import sys
import time
import zipfile
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Iterator, Optional

import dotenv
import psycopg
import requests

# Load .env from project root
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
    fetch_incremental_rows,
)

BASE_URL = "https://data.nasdaq.com/api/v3/datatables/{table}.csv"
TABLE_CODE = "SHARADAR/SFP"
SOURCE_TABLE = "SFP"

# Columns the API must return (rule 2c) and the full known set (rule 2b).
REQUIRED_SFP_COLS: set[str] = {"ticker", "date", "close"}
KNOWN_SFP_COLS: set[str] = {
    "ticker", "date", "open", "high", "low", "close",
    "volume", "closeadj", "closeunadj", "lastupdated",
}


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


# ── helpers ───────────────────────────────────────────────────────────────────

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
            print(f"[info] retrying in {sleep_s}s…")
            time.sleep(sleep_s)
    assert last_exc is not None
    raise last_exc


# ── bulk download ─────────────────────────────────────────────────────────────

def build_export_url(table: str, api_key: str) -> str:
    return f"{BASE_URL.format(table=table)}?qopts.export=true&api_key={api_key}"


def download_zip_bytes(
    session: requests.Session,
    url: str,
    config: Config,
    poll_seconds: int = 5,
    max_wait_seconds: int = 300,
) -> bytes:
    deadline = time.time() + max_wait_seconds
    last_status: str | None = None

    while True:
        with request_with_retries(
            session,
            url,
            max_attempts=config.download_retries,
            timeout=(config.request_timeout_connect, config.request_timeout_read),
        ) as resp:
            meta_bytes = resp.content

        meta_text = meta_bytes.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(meta_text))
        row = next(reader, None)
        if row is None:
            raise RuntimeError(f"Nasdaq bulk export metadata was empty for URL: {url}")

        file_link = (row.get("file.link") or "").strip()
        file_status = (row.get("file.status") or "").strip()
        last_status = file_status or last_status

        print(f"[debug] bulk export status={file_status!r} link_present={bool(file_link)}")

        if file_status.lower() == "fresh":
            if not file_link:
                raise RuntimeError(f"Bulk export is Fresh but file.link is empty for URL: {url}")
            with request_with_retries(
                session,
                file_link,
                max_attempts=config.download_retries,
                timeout=(config.request_timeout_connect, 600),
            ) as zip_resp:
                data = zip_resp.content
                if data[:2] != b"PK":
                    raise RuntimeError(
                        "Downloaded bulk file is not a ZIP. "
                        f"First 200 bytes: {data[:200]!r}"
                    )
                return data

        if file_status.lower() in {"creating", "regenerating"}:
            if time.time() >= deadline:
                raise TimeoutError(
                    "Timed out waiting for bulk export to become Fresh. "
                    f"Last status={last_status!r}, url={url}"
                )
            time.sleep(poll_seconds)
            continue

        raise RuntimeError(
            f"Unexpected Nasdaq bulk export status {file_status!r} for URL: {url}."
        )


def first_csv_from_response_bytes(data: bytes) -> io.TextIOBase:
    if data[:2] == b"PK":
        zf = zipfile.ZipFile(io.BytesIO(data))
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError("Zip file did not contain a CSV")
        return io.TextIOWrapper(zf.open(csv_names[0], "r"), encoding="utf-8", newline="")
    raise RuntimeError("Expected ZIP payload from Nasdaq bulk export")


# ── schema / DDL ──────────────────────────────────────────────────────────────

def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        # Instruments table (shared across all ingest scripts)
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
            CREATE TABLE IF NOT EXISTS {schema}.daily_bars (
                instrument_id BIGINT NOT NULL REFERENCES {schema}.instruments(instrument_id),
                trade_date DATE NOT NULL,
                open NUMERIC(30, 6),
                high NUMERIC(30, 6),
                low NUMERIC(30, 6),
                close NUMERIC(30, 6),
                adj_close NUMERIC(30, 6),
                volume BIGINT,
                source_table TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (instrument_id, trade_date)
            )
            """
        )

        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS daily_bars_trade_date_idx
            ON {schema}.daily_bars (trade_date)
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS daily_bars_instrument_date_idx
            ON {schema}.daily_bars (instrument_id, trade_date)
            """
        )

    conn.commit()


def get_max_trade_date(conn: psycopg.Connection, schema: str) -> str | None:
    """Return the most-recent trade_date for SFP rows in daily_bars, or None if empty."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT MAX(trade_date) FROM {schema}.daily_bars WHERE source_table = %s",
            (TABLE_CODE,),
        )
        result = cur.fetchone()[0]
        return str(result) if result else None


# ── staging ───────────────────────────────────────────────────────────────────

def create_staging_table(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg_bars")
        cur.execute(
            """
            CREATE TEMP TABLE stg_bars (
                symbol TEXT,
                trade_date DATE,
                open NUMERIC(30, 6),
                high NUMERIC(30, 6),
                low NUMERIC(30, 6),
                close NUMERIC(30, 6),
                adj_close NUMERIC(30, 6),
                volume BIGINT,
                source_table TEXT
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


# ── row parsing ───────────────────────────────────────────────────────────────

def _bar_row_to_tuple(row: dict[str, str], default_source_table: str) -> tuple | None:
    """Convert a normalised row dict to a staging tuple, or None if required fields missing."""
    symbol = row_get(row, "ticker", "symbol")
    trade_date = row_get(row, "date", "trade_date")
    if not symbol or not trade_date:
        return None

    open_ = parse_decimal(row_get(row, "open"))
    high = parse_decimal(row_get(row, "high"))
    low = parse_decimal(row_get(row, "low"))
    close = parse_decimal(row_get(row, "close"))
    adj_close = parse_decimal(row_get(row, "closeadj", "adj_close", "adjusted_close", "close_adjusted"))
    volume = parse_int(row_get(row, "volume"))
    source_table = row_get(row, "table") or default_source_table

    if close is None:
        return None
    if adj_close is None:
        adj_close = close

    return (symbol, trade_date, open_, high, low, close, adj_close, volume, source_table)


def iter_bar_rows(csv_file: io.TextIOBase, default_source_table: str) -> Iterator[tuple]:
    reader = csv.DictReader(csv_file)
    reader.fieldnames = [normalise_header(x) for x in (reader.fieldnames or [])]

    debug_printed = False
    for row in reader:
        if not debug_printed:
            print(f"[debug] CSV columns: {reader.fieldnames}")
            debug_printed = True
        result = _bar_row_to_tuple(row, default_source_table)
        if result is not None:
            yield result


def iter_bar_rows_from_dicts(
    rows: list[dict[str, str]],
    default_source_table: str,
) -> Iterator[tuple]:
    """Variant for rows already loaded as dicts (incremental JSON path)."""
    for row in rows:
        result = _bar_row_to_tuple(row, default_source_table)
        if result is not None:
            yield result


# ── upsert ────────────────────────────────────────────────────────────────────

def upsert_instruments_from_bars(conn: psycopg.Connection, schema: str) -> None:
    """Ensure every symbol in stg_bars exists in instruments."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {schema}.instruments (symbol, asset_type, source_table, is_active, updated_at)
            SELECT DISTINCT s.symbol, 'FUND', '{TABLE_CODE}', TRUE, NOW()
            FROM stg_bars s
            WHERE s.symbol IS NOT NULL
            ON CONFLICT (symbol) DO UPDATE
            SET updated_at = NOW(),
                source_table = COALESCE({schema}.instruments.source_table, EXCLUDED.source_table)
            """
        )


def upsert_bars(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {schema}.daily_bars
                (instrument_id, trade_date, open, high, low, close, adj_close, volume, source_table, updated_at)
            SELECT DISTINCT ON (i.instrument_id, s.trade_date)
                i.instrument_id,
                s.trade_date,
                s.open,
                s.high,
                s.low,
                s.close,
                s.adj_close,
                s.volume,
                s.source_table,
                NOW()
            FROM stg_bars s
            JOIN {schema}.instruments i
              ON i.symbol = s.symbol
            WHERE s.symbol IS NOT NULL
              AND s.trade_date IS NOT NULL
            ORDER BY i.instrument_id, s.trade_date
            ON CONFLICT (instrument_id, trade_date) DO UPDATE
            SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                adj_close = EXCLUDED.adj_close,
                volume = EXCLUDED.volume,
                source_table = EXCLUDED.source_table,
                updated_at = NOW()
            """
        )
    conn.commit()


# ── load orchestration ────────────────────────────────────────────────────────

_STAGING_COLUMNS = [
    "symbol", "trade_date", "open", "high", "low", "close", "adj_close", "volume", "source_table",
]


def _stage_and_upsert(
    conn: psycopg.Connection,
    rows_iter: Iterator[tuple],
    config: Config,
) -> int:
    """Stage *rows_iter* into stg_bars and upsert into daily_bars."""
    create_staging_table(conn)
    total = 0
    for batch in chunked(rows_iter, config.batch_size):
        copied = copy_rows(conn, "stg_bars", _STAGING_COLUMNS, batch)
        total += copied
        print(f"[info] staged {copied:,} bar rows (running total {total:,})")

    if total == 0:
        print(
            f"[warn] zero rows staged for {TABLE_CODE} – "
            "check column aliases in iter_bar_rows"
        )
    else:
        print(f"[info] total staged: {total:,} bar rows")

    upsert_instruments_from_bars(conn, config.schema)
    upsert_bars(conn, config.schema)
    conn.commit()
    return total


def _full_bulk_load(
    session: requests.Session,
    conn: psycopg.Connection,
    config: Config,
    *,
    truncate: bool = False,
) -> None:
    """Download the full bulk ZIP and load it into daily_bars."""
    if truncate:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {config.schema}.daily_bars WHERE source_table = %s",
                (TABLE_CODE,),
            )
        conn.commit()
        print(f"[info] deleted {TABLE_CODE} rows from {config.schema}.daily_bars")

    print(f"[info] downloading full bulk export for {TABLE_CODE}…")
    data = download_zip_bytes(session, build_export_url(TABLE_CODE, config.api_key), config)
    csv_file = first_csv_from_response_bytes(data)
    try:
        count = _stage_and_upsert(conn, iter_bar_rows(csv_file, TABLE_CODE), config)
    finally:
        csv_file.close()

    if count == 0:
        raise RuntimeError(
            f"Parsed zero bar rows from {TABLE_CODE} export. "
            "Check the debug column list above and adjust iter_bar_rows aliases if needed."
        )
    print(f"[ok] full bulk load complete – {count:,} rows staged")


def _do_incremental_load(
    session: requests.Session,
    conn: psycopg.Connection,
    config: Config,
    since_date: str,
    until_date: str,
) -> None:
    """Fetch rows in the date window and upsert into daily_bars."""
    _cols, inc_rows = fetch_incremental_rows(
        session,
        TABLE_CODE,
        config.api_key,
        date_filter_col="date",
        since_date=since_date,
        until_date=until_date,
    )
    if not inc_rows:
        print(f"[info] {TABLE_CODE}: no new/updated rows in window {since_date} → {until_date}")
        return
    count = _stage_and_upsert(
        conn, iter_bar_rows_from_dicts(inc_rows, TABLE_CODE), config
    )
    print(f"[info] incremental load complete – {count:,} rows staged")


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest SHARADAR/SFP into Postgres")
    parser.add_argument("--schema", default="strategy_engine")
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    api_key = dotenv.get_key(str(_ENV_FILE), "NDL_API_KEY") if _ENV_FILE.exists() else None
    db_dsn = dotenv.get_key(str(_ENV_FILE), "DATABASE_URL") if _ENV_FILE.exists() else None

    if not api_key:
        api_key = os.getenv("NDL_API_KEY")
    if not db_dsn:
        db_dsn = os.getenv("DATABASE_URL")

    if not api_key:
        raise RuntimeError("NDL_API_KEY not found in .env or environment")
    if not db_dsn:
        raise RuntimeError("DATABASE_URL not found in .env or environment")

    config = Config(
        api_key=api_key,
        db_dsn=db_dsn,
        schema=args.schema,
        overwrite=args.overwrite,
    )

    session = requests.Session()
    session.headers.update({"User-Agent": "strategy_engine/1.0"})

    with psycopg.connect(config.db_dsn) as conn:
        ensure_schema(conn, config.schema)

        # ── STEP 1: sample validation (rule 2c) ───────────────────────────────
        sample_cols, sample_rows = fetch_api_sample(
            session, TABLE_CODE, config.api_key, n=75
        )
        validate_sample_shape(TABLE_CODE, sample_cols, sample_rows, REQUIRED_SFP_COLS)

        # ── STEP 2: schema-drift check (rule 2b) ──────────────────────────────
        drift = detect_schema_drift(TABLE_CODE, set(sample_cols), KNOWN_SFP_COLS)
        max_date = get_max_trade_date(conn, config.schema)

        if config.overwrite or max_date is None:
            reason = "--overwrite" if config.overwrite else "table is empty (no SFP rows)"
            print(f"[info] {TABLE_CODE}: full bulk load ({reason})")
            _full_bulk_load(session, conn, config, truncate=config.overwrite)

        elif drift:
            # Schema changed: truncate + full reload (rule 2b)
            print(f"[info] {TABLE_CODE}: full bulk reload due to schema drift (rule 2b)")
            _full_bulk_load(session, conn, config, truncate=True)

        else:
            # ── STEP 3: incremental load (rule 2a) ────────────────────────────
            # Window: (max_date - 1 day) → yesterday.
            # The -1 day overlap ensures the latest previously-loaded day is
            # always refreshed, as today's data is unreliable.
            yesterday = (date.today() - timedelta(days=1)).isoformat()
            since_date = (
                date.fromisoformat(max_date) - timedelta(days=1)
            ).isoformat()
            print(
                f"[info] {TABLE_CODE}: incremental load – "
                f"DB max trade_date: {max_date}, window: {since_date} → {yesterday}"
            )
            _do_incremental_load(session, conn, config, since_date, yesterday)

        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {config.schema}.daily_bars WHERE source_table = %s",
                (TABLE_CODE,),
            )
            total = cur.fetchone()[0]

    print(f"[info] {config.schema}.daily_bars ({TABLE_CODE}) now contains {total:,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

