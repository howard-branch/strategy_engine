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
    schema: str = "project_quant"
    batch_size: int = 100_000
    download_retries: int = 5
    request_timeout_connect: int = 30
    request_timeout_read: int = 600
    overwrite: bool = False


DATASET_CODE = "SHARADAR/SF2"


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

def parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def parse_numeric(value: Optional[str]):
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_bool(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    v = value.strip().lower()
    if v in {"y", "yes", "true", "t", "1"}:
        return True
    if v in {"n", "no", "false", "f", "0"}:
        return False
    return None

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
            CREATE TABLE IF NOT EXISTS {schema}.sharadar_sf2 (
                ticker TEXT NOT NULL,
                filing_date DATE NOT NULL,
                row_num INTEGER NOT NULL,
                form_type TEXT,
                issuer_name TEXT,
                owner_name TEXT,
                officer_title TEXT,
                is_director BOOLEAN,
                is_officer BOOLEAN,
                is_ten_percent_owner BOOLEAN,
                transaction_date DATE,
                security_ad_code TEXT,
                transaction_code TEXT,
                shares_owned_before_transaction NUMERIC,
                transaction_shares NUMERIC,
                shares_owned_following_transaction NUMERIC,
                transaction_price_per_share NUMERIC,
                transaction_value NUMERIC,
                security_title TEXT,
                director_indirect TEXT,
                nature_of_ownership TEXT,
                date_exercisable DATE,
                price_exercisable NUMERIC,
                expiration_date DATE,
                raw_record JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (ticker, filing_date, row_num)
            )
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS sharadar_sf2_filing_date_idx
            ON {schema}.sharadar_sf2 (filing_date)
            """
        )

        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS sharadar_sf2_ticker_idx
            ON {schema}.sharadar_sf2 (ticker)
            """
        )

        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS sharadar_sf2_transaction_date_idx
            ON {schema}.sharadar_sf2 (transaction_date)
            """
        )

        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS sharadar_sf2_owner_name_idx
            ON {schema}.sharadar_sf2 (owner_name)
            """
        )

        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS sharadar_sf2_transaction_code_idx
            ON {schema}.sharadar_sf2 (transaction_code)
            """
        )
    conn.commit()


def truncate_tables(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {schema}.sharadar_sf2")
    conn.commit()
    print("[info] truncated existing sharadar_sf2 data")


def create_staging_tables(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg_sharadar_sf2")
        cur.execute(
            """
            CREATE TEMP TABLE stg_sharadar_sf2 (
                ticker TEXT,
                filing_date DATE,
                row_num INTEGER,
                form_type TEXT,
                issuer_name TEXT,
                owner_name TEXT,
                officer_title TEXT,
                is_director BOOLEAN,
                is_officer BOOLEAN,
                is_ten_percent_owner BOOLEAN,
                transaction_date DATE,
                security_ad_code TEXT,
                transaction_code TEXT,
                shares_owned_before_transaction NUMERIC,
                transaction_shares NUMERIC,
                shares_owned_following_transaction NUMERIC,
                transaction_price_per_share NUMERIC,
                transaction_value NUMERIC,
                security_title TEXT,
                director_indirect TEXT,
                nature_of_ownership TEXT,
                date_exercisable DATE,
                price_exercisable NUMERIC,
                expiration_date DATE,
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


def iter_sf2_rows(csv_file: io.TextIOBase) -> Iterator[tuple]:
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
        filing_date = row_get(r, "filingdate", "filing_date")
        row_num = parse_int(row_get(r, "rownum"))

        if not ticker or not filing_date or row_num is None:
            continue

        yield (
            ticker,
            filing_date,
            row_num,
            row_get(r, "formtype"),
            row_get(r, "issuername"),
            row_get(r, "ownername"),
            row_get(r, "officertitle"),
            parse_bool(row_get(r, "isdirector")),
            parse_bool(row_get(r, "isofficer")),
            parse_bool(row_get(r, "istenpercentowner")),
            row_get(r, "transactiondate"),
            row_get(r, "securityadcode"),
            row_get(r, "transactioncode"),
            parse_numeric(row_get(r, "sharesownedbeforetransaction")),
            parse_numeric(row_get(r, "transactionshares")),
            parse_numeric(row_get(r, "sharesownedfollowingtransaction")),
            parse_numeric(row_get(r, "transactionpricepershare")),
            parse_numeric(row_get(r, "transactionvalue")),
            row_get(r, "securitytitle"),
            row_get(r, "directorindirect"),
            row_get(r, "natureofownership"),
            row_get(r, "dateexercisable"),
            parse_numeric(row_get(r, "priceexercisable")),
            row_get(r, "expirationdate"),
            json.dumps(r, ensure_ascii=False),
        )

def upsert_instruments_from_sf2(conn: psycopg.Connection, schema: str) -> None:
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
                'SF2',
                TRUE,
                NOW()
            FROM stg_sharadar_sf2 s
            WHERE s.ticker IS NOT NULL
            ON CONFLICT (symbol) DO UPDATE
            SET
                updated_at = NOW(),
                source_table = COALESCE({schema}.instruments.source_table, EXCLUDED.source_table)
            """
        )

def upsert_sf2(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {schema}.sharadar_sf2 (
                ticker,
                filing_date,
                row_num,
                form_type,
                issuer_name,
                owner_name,
                officer_title,
                is_director,
                is_officer,
                is_ten_percent_owner,
                transaction_date,
                security_ad_code,
                transaction_code,
                shares_owned_before_transaction,
                transaction_shares,
                shares_owned_following_transaction,
                transaction_price_per_share,
                transaction_value,
                security_title,
                director_indirect,
                nature_of_ownership,
                date_exercisable,
                price_exercisable,
                expiration_date,
                raw_record,
                updated_at
            )
            SELECT DISTINCT ON (s.ticker, s.filing_date, s.row_num)
                s.ticker,
                s.filing_date,
                s.row_num,
                s.form_type,
                s.issuer_name,
                s.owner_name,
                s.officer_title,
                s.is_director,
                s.is_officer,
                s.is_ten_percent_owner,
                s.transaction_date,
                s.security_ad_code,
                s.transaction_code,
                s.shares_owned_before_transaction,
                s.transaction_shares,
                s.shares_owned_following_transaction,
                s.transaction_price_per_share,
                s.transaction_value,
                s.security_title,
                s.director_indirect,
                s.nature_of_ownership,
                s.date_exercisable,
                s.price_exercisable,
                s.expiration_date,
                s.raw_record,
                NOW()
            FROM stg_sharadar_sf2 s
            WHERE s.ticker IS NOT NULL
              AND s.filing_date IS NOT NULL
              AND s.row_num IS NOT NULL
            ORDER BY s.ticker, s.filing_date, s.row_num
            ON CONFLICT (ticker, filing_date, row_num) DO UPDATE
            SET
                form_type = EXCLUDED.form_type,
                issuer_name = EXCLUDED.issuer_name,
                owner_name = EXCLUDED.owner_name,
                officer_title = EXCLUDED.officer_title,
                is_director = EXCLUDED.is_director,
                is_officer = EXCLUDED.is_officer,
                is_ten_percent_owner = EXCLUDED.is_ten_percent_owner,
                transaction_date = EXCLUDED.transaction_date,
                security_ad_code = EXCLUDED.security_ad_code,
                transaction_code = EXCLUDED.transaction_code,
                shares_owned_before_transaction = EXCLUDED.shares_owned_before_transaction,
                transaction_shares = EXCLUDED.transaction_shares,
                shares_owned_following_transaction = EXCLUDED.shares_owned_following_transaction,
                transaction_price_per_share = EXCLUDED.transaction_price_per_share,
                transaction_value = EXCLUDED.transaction_value,
                security_title = EXCLUDED.security_title,
                director_indirect = EXCLUDED.director_indirect,
                nature_of_ownership = EXCLUDED.nature_of_ownership,
                date_exercisable = EXCLUDED.date_exercisable,
                price_exercisable = EXCLUDED.price_exercisable,
                expiration_date = EXCLUDED.expiration_date,
                raw_record = EXCLUDED.raw_record,
                updated_at = NOW()
            """
        )

def parse_args() -> Config:
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Ingest SHARADAR/SF2 into Postgres")
    parser.add_argument("--api-key", default=os.getenv("NDL_API_KEY") or os.getenv("NASDAQ_DATA_LINK_API_KEY"))
    parser.add_argument("--db-dsn", default=os.getenv("DATABASE_URL") or os.getenv("DB_DSN"))
    parser.add_argument("--schema", default=os.getenv("DB_SCHEMA", "project_quant"))
    parser.add_argument("--batch-size", type=int, default=100_000)
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
        if config.overwrite:
            truncate_tables(conn, config.schema)

        create_staging_tables(conn)

        csv_file = download_csv_file(session, config)
        total = 0

        for batch in chunked(iter_sf2_rows(csv_file), config.batch_size):
            copied = copy_rows(
                conn,
                "stg_sharadar_sf2",
                [
                    "ticker",
                    "filing_date",
                    "row_num",
                    "form_type",
                    "issuer_name",
                    "owner_name",
                    "officer_title",
                    "is_director",
                    "is_officer",
                    "is_ten_percent_owner",
                    "transaction_date",
                    "security_ad_code",
                    "transaction_code",
                    "shares_owned_before_transaction",
                    "transaction_shares",
                    "shares_owned_following_transaction",
                    "transaction_price_per_share",
                    "transaction_value",
                    "security_title",
                    "director_indirect",
                    "nature_of_ownership",
                    "date_exercisable",
                    "price_exercisable",
                    "expiration_date",
                    "raw_record",
                ],
                batch,
            )
            total += copied
            print(f"[info] copied {copied:,} SF2 rows to staging (running total {total:,})")

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM stg_sharadar_sf2")
            staged_count = cur.fetchone()[0]
        print(f"[info] staged sharadar SF2 rows: {staged_count:,}")

        upsert_instruments_from_sf2(conn, config.schema)
        upsert_sf2(conn, config.schema)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {config.schema}.sharadar_sf2")
            final_count = cur.fetchone()[0]

        print(f"[info] sharadar_sf2 row count: {final_count:,}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
