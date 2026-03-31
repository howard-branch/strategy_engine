#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
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
TABLE_CODE = "SHARADAR/ACTIONS"

# Columns the API must return (rule 2c) and the full known set (rule 2b).
REQUIRED_ACTIONS_COLS: set[str] = {"ticker", "date", "action"}
KNOWN_ACTIONS_COLS: set[str] = {
    "ticker", "date", "action", "value", "contraticker", "contraname",
}


@dataclass
class Config:
    api_key: str
    db_dsn: str
    schema: str = "strategy_engine"
    batch_size: int = 10_000
    download_retries: int = 5
    request_timeout_connect: int = 30
    request_timeout_read: int = 300
    overwrite: bool = False


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
            print(f"[warn] request failed ({attempt}/{max_attempts}): {exc}")
            print(f"[info] retrying in {sleep_s}s...")
            time.sleep(sleep_s)
    assert last_exc is not None
    raise last_exc


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

            print(
                f"[debug] bulk export status={file_status!r} "
                f"link_present={bool(file_link)}"
            )

            if file_status.lower() == "fresh":
                if not file_link:
                    raise RuntimeError(
                        f"Bulk export is Fresh but file.link is empty for URL: {url}"
                    )
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


def chunked(rows: Iterable[tuple], size: int) -> Iterator[list[tuple]]:
    buf: list[tuple] = []
    for row in rows:
        buf.append(row)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.corporate_actions (
                instrument_id BIGINT NOT NULL
                    REFERENCES {schema}.instruments(instrument_id),
                action_date DATE NOT NULL,
                action TEXT NOT NULL,
                value NUMERIC(30, 10),
                contra_ticker TEXT NOT NULL DEFAULT '',
                contra_name TEXT,
                source_table TEXT NOT NULL DEFAULT 'SHARADAR/ACTIONS',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (instrument_id, action_date, action, contra_ticker)
            )
            """
        )

        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS corporate_actions_action_date_idx
            ON {schema}.corporate_actions (action_date)
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS corporate_actions_instrument_date_idx
            ON {schema}.corporate_actions (instrument_id, action_date)
            """
        )

    conn.commit()


def get_max_action_date(conn: psycopg.Connection, schema: str) -> str | None:
    """Return the most-recent action_date stored in the DB, or None if empty."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(action_date) FROM {schema}.corporate_actions")
        result = cur.fetchone()[0]
        return str(result) if result else None


def create_staging_table(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg_actions")
        cur.execute(
            """
            CREATE TEMP TABLE stg_actions (
                symbol TEXT,
                action_date DATE,
                action TEXT,
                value NUMERIC(30, 10),
                contra_ticker TEXT,
                contra_name TEXT,
                source_table TEXT
            ) ON COMMIT DROP
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


def _action_row_to_tuple(row: dict[str, str], default_source_table: str) -> tuple | None:
    """Convert a normalised row dict to a staging tuple, or None if required fields missing."""
    symbol = row_get(row, "ticker", "symbol")
    action_date = row_get(row, "date", "action_date")
    action = row_get(row, "action")
    if not symbol or not action_date or not action:
        return None
    value = parse_decimal(row_get(row, "value"))
    contra_ticker = row_get(row, "contraticker", "contra_ticker") or ""
    contra_name = row_get(row, "contraname", "contra_name")
    source_table = row_get(row, "table") or default_source_table
    return (symbol, action_date, action, value, contra_ticker, contra_name, source_table)


def iter_action_rows(
        csv_file: io.TextIOBase,
        default_source_table: str,
) -> Iterator[tuple]:
    reader = csv.DictReader(csv_file)
    reader.fieldnames = [normalise_header(x) for x in (reader.fieldnames or [])]

    printed = False
    for row in reader:
        if not printed:
            print(f"[debug] CSV columns: {reader.fieldnames}")
            printed = True
        result = _action_row_to_tuple(row, default_source_table)
        if result is not None:
            yield result


def iter_action_rows_from_dicts(
        rows: list[dict[str, str]],
        default_source_table: str,
) -> Iterator[tuple]:
    """Variant of iter_action_rows for rows already loaded as dicts (incremental JSON path)."""
    for row in rows:
        result = _action_row_to_tuple(row, default_source_table)
        if result is not None:
            yield result


def upsert_actions(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {schema}.corporate_actions (
                instrument_id,
                action_date,
                action,
                value,
                contra_ticker,
                contra_name,
                source_table,
                updated_at
            )
            SELECT DISTINCT ON (
                i.instrument_id,
                s.action_date,
                s.action,
                COALESCE(s.contra_ticker, '')
            )
                i.instrument_id,
                s.action_date,
                s.action,
                s.value,
                COALESCE(s.contra_ticker, ''),
                s.contra_name,
                s.source_table,
                NOW()
            FROM stg_actions s
            JOIN {schema}.instruments i
              ON i.symbol = s.symbol
            WHERE s.symbol IS NOT NULL
              AND s.action_date IS NOT NULL
              AND s.action IS NOT NULL
            ORDER BY
                i.instrument_id,
                s.action_date,
                s.action,
                COALESCE(s.contra_ticker, ''),
                s.value DESC NULLS LAST,
                s.contra_name DESC NULLS LAST
            ON CONFLICT (instrument_id, action_date, action, contra_ticker) DO UPDATE
            SET
                value = EXCLUDED.value,
                contra_name = EXCLUDED.contra_name,
                source_table = EXCLUDED.source_table,
                updated_at = NOW()
            """
        )
    conn.commit()


def _stage_and_upsert(
        conn: psycopg.Connection,
        rows_iter: Iterator[tuple],
        config: Config,
) -> int:
    """Stage *rows_iter* into stg_actions and upsert into corporate_actions."""
    create_staging_table(conn)
    count = copy_rows(
        conn,
        "stg_actions",
        ["symbol", "action_date", "action", "value", "contra_ticker", "contra_name", "source_table"],
        rows_iter,
    )
    if count == 0:
        print(
            "[warn] zero rows staged for SHARADAR/ACTIONS – "
            "check column aliases in iter_action_rows"
        )
    else:
        print(f"[info] staged {count:,} action rows")
    upsert_actions(conn, config.schema)
    return count


def _full_bulk_load(
        session: requests.Session,
        conn: psycopg.Connection,
        config: Config,
        *,
        truncate: bool = False,
) -> None:
    """Download the full bulk ZIP and load it into corporate_actions."""
    if truncate:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {config.schema}.corporate_actions")
        conn.commit()
        print(f"[info] truncated {config.schema}.corporate_actions")

    print(f"[info] downloading full bulk export for {TABLE_CODE}…")
    data = download_zip_bytes(session, build_export_url(TABLE_CODE, config.api_key), config)
    csv_file = first_csv_from_response_bytes(data)
    try:
        count = _stage_and_upsert(conn, iter_action_rows(csv_file, TABLE_CODE), config)
    finally:
        csv_file.close()

    if count == 0:
        raise RuntimeError(
            "Parsed zero corporate action rows from SHARADAR/ACTIONS export. "
            "Check the debug column list above and adjust iter_action_rows aliases if needed."
        )
    print(f"[ok] full bulk load complete – {count:,} rows staged")


def _do_incremental_load(
        session: requests.Session,
        conn: psycopg.Connection,
        config: Config,
        since_date: str,
        until_date: str,
) -> None:
    """Fetch rows in the date window and upsert into corporate_actions."""
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
        conn, iter_action_rows_from_dicts(inc_rows, TABLE_CODE), config
    )
    print(f"[info] incremental load complete – {count:,} rows staged")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest SHARADAR/ACTIONS into Postgres")
    parser.add_argument("--schema", default="strategy_engine")
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    api_key = dotenv.get_key(str(_ENV_FILE), "NDL_API_KEY") if _ENV_FILE.exists() else None
    db_dsn = dotenv.get_key(str(_ENV_FILE), "DATABASE_URL") if _ENV_FILE.exists() else None

    if not api_key:
        api_key = __import__("os").getenv("NDL_API_KEY")
    if not db_dsn:
        db_dsn = __import__("os").getenv("DATABASE_URL")

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
        validate_sample_shape(TABLE_CODE, sample_cols, sample_rows, REQUIRED_ACTIONS_COLS)

        # ── STEP 2: schema-drift check (rule 2b) ──────────────────────────────
        drift = detect_schema_drift(TABLE_CODE, set(sample_cols), KNOWN_ACTIONS_COLS)
        max_date = get_max_action_date(conn, config.schema)

        if config.overwrite or max_date is None:
            reason = "--overwrite" if config.overwrite else "table is empty"
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
                f"DB max action_date: {max_date}, window: {since_date} → {yesterday}"
            )
            _do_incremental_load(session, conn, config, since_date, yesterday)

        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {config.schema}.corporate_actions"
            )
            total = cur.fetchone()[0]

    print(f"[info] {config.schema}.corporate_actions now contains {total:,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(__import__("sys").argv[1:]))