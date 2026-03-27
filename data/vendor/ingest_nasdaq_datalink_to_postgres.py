#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import os
import sys
import time
import zipfile
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Iterator, Optional

import dotenv
import psycopg
import requests

# Load .env file from project root
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ENV_FILE = _PROJECT_ROOT / ".env"
if _ENV_FILE.exists():
    dotenv.load_dotenv(_ENV_FILE)


BASE_URL = "https://data.nasdaq.com/api/v3/datatables/{table}.csv"
DEFAULT_TABLES = ["SHARADAR/TICKERS", "SHARADAR/SEP", "SHARADAR/SFP"]


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

def get_bulk_export_link(
        session: requests.Session,
        datatable_code: str,
        api_key: str,
        poll_seconds: int = 5,
        max_wait_seconds: int = 300,
) -> str:
    """
    Call Nasdaq Data Link bulk export endpoint until file.status == Fresh,
    then return file.link.
    """
    url = (
        f"https://data.nasdaq.com/api/v3/datatables/{datatable_code}.csv"
        f"?qopts.export=true&api_key={api_key}"
    )

    deadline = time.time() + max_wait_seconds

    while True:
        resp = session.get(url, timeout=(30, 300))
        resp.raise_for_status()
        text = resp.text

        reader = csv.DictReader(io.StringIO(text))
        row = next(reader, None)
        if row is None:
            raise RuntimeError(f"No metadata row returned for bulk export {datatable_code}")

        link = (row.get("file.link") or "").strip()
        status = (row.get("file.status") or "").strip()

        print(f"[debug] bulk export {datatable_code}: status={status!r}, link_present={bool(link)}")

        # Fresh = ready to download
        if status.lower() == "fresh":
            if not link:
                raise RuntimeError(f"Bulk export {datatable_code} is Fresh but file.link is empty")
            return link

        # Creating / Regenerating = wait and poll again
        if status.lower() in {"creating", "regenerating"}:
            if time.time() >= deadline:
                raise TimeoutError(
                    f"Timed out waiting for Nasdaq export {datatable_code} to become Fresh"
                )
            time.sleep(poll_seconds)
            continue

        raise RuntimeError(
            f"Unexpected bulk export status for {datatable_code}: {status!r}; row={row}"
        )

def download_bulk_zip_bytes(
        session: requests.Session,
        datatable_code: str,
        api_key: str,
) -> bytes:
    link = get_bulk_export_link(session, datatable_code, api_key)
    resp = session.get(link, timeout=(30, 600))
    resp.raise_for_status()
    data = resp.content

    if not data.startswith(b"PK"):
        raise RuntimeError(
            f"Downloaded bulk file for {datatable_code} is not a ZIP; "
            f"first 200 bytes={data[:200]!r}"
        )

    return data

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


def first_csv_from_response_bytes(data: bytes) -> io.TextIOBase:
    """
    Handle both ZIP (bulk export) and direct CSV responses.
    """

    # Case 1: ZIP file
    if data[:2] == b"PK":  # ZIP magic header
        zf = zipfile.ZipFile(io.BytesIO(data))
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError("Zip file did not contain a CSV")
        return io.TextIOWrapper(zf.open(csv_names[0], "r"), encoding="utf-8", newline="")

    # Case 2: CSV directly
    text = data.decode("utf-8", errors="ignore")

    # Detect JSON error
    if text.strip().startswith("{"):
        raise RuntimeError(f"Nasdaq returned JSON (likely error): {text[:500]}")

    # Detect HTML error
    if "<html" in text.lower():
        raise RuntimeError(f"Nasdaq returned HTML (likely error page): {text[:500]}")

    # Otherwise assume CSV
    return io.StringIO(text)

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


def parse_bool(value: Optional[str], default: bool = True) -> bool:
    if value is None:
        return default
    v = value.strip().lower()
    if v in {"true", "t", "1", "y", "yes"}:
        return True
    if v in {"false", "f", "0", "n", "no"}:
        return False
    return default


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
        # volume sometimes arrives as "12345.0"
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


def truncate_tables(conn: psycopg.Connection, schema: str) -> None:
    """Truncate all data from instruments and daily_bars tables."""
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {schema}.daily_bars CASCADE")
        cur.execute(f"TRUNCATE TABLE {schema}.instruments CASCADE")
    conn.commit()
    print("[info] truncated existing data")


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

        cur.execute(f"ALTER TABLE {schema}.instruments ADD COLUMN IF NOT EXISTS asset_type TEXT")
        cur.execute(
            f"""
            UPDATE {schema}.instruments
            SET asset_type = COALESCE(asset_type, instrument_type, 'STOCK')
            WHERE asset_type IS NULL
            """
        )
        cur.execute(f"ALTER TABLE {schema}.instruments ALTER COLUMN asset_type SET DEFAULT 'STOCK'")

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

        cur.execute(f"ALTER TABLE {schema}.daily_bars ADD COLUMN IF NOT EXISTS source_table TEXT")
        cur.execute(f"ALTER TABLE {schema}.daily_bars ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
        cur.execute(f"ALTER TABLE {schema}.daily_bars ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
        
        # Ensure volume column allows NULL (in case table was created with old schema)
        try:
            cur.execute(f"ALTER TABLE {schema}.daily_bars ALTER COLUMN volume DROP NOT NULL")
        except Exception:
            pass
        
        # Update numeric column precision from (18,6) to (30,6) if needed for old schemas
        for col in ["open", "high", "low", "close", "adj_close"]:
            try:
                cur.execute(f"ALTER TABLE {schema}.daily_bars ALTER COLUMN {col} TYPE NUMERIC(30, 6)")
            except Exception:
                pass

    conn.commit()


def create_staging_tables(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg_tickers")
        cur.execute("DROP TABLE IF EXISTS stg_bars")

        cur.execute(
            """
            CREATE TEMP TABLE stg_tickers (
                symbol TEXT,
                asset_type TEXT,
                name TEXT,
                exchange TEXT,
                instrument_type TEXT,
                source_table TEXT,
                category TEXT,
                is_active BOOLEAN
            ) ON COMMIT DROP
            """
        )

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
        with cur.copy(
                f"COPY {table_name} ({', '.join(columns)}) FROM STDIN"
        ) as copy:
            for row in rows:
                copy.write_row(row)
                total += 1
    return total

def infer_asset_type(source_table, instrument_type, category):
    st = (source_table or "").upper()
    it = (instrument_type or "").upper()
    cat = (category or "").upper()

    if st == "SFP":
        return "FUND"
    if st in {"SEP", "DAILY"}:
        return "STOCK"
    if "ETF" in it or "ETF" in cat:
        return "ETF"
    if "FUND" in it or "FUND" in cat:
        return "FUND"
    return "STOCK"

def upsert_instruments(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
              INSERT INTO {schema}.instruments (
                symbol,
                asset_type,
                name,
                exchange,
                instrument_type,
                source_table,
                category,
                is_active,
                updated_at
            )
           
            SELECT DISTINCT ON (s.symbol)
                s.symbol,
                s.asset_type,
                s.name,
                s.exchange,
                s.instrument_type,
                s.source_table,
                s.category,
                s.is_active,
                NOW()
            FROM stg_tickers s
            WHERE s.symbol IS NOT NULL
            ORDER BY s.symbol
            ON CONFLICT (symbol) DO UPDATE
            SET
                name = EXCLUDED.name,
                asset_type = COALESCE(EXCLUDED.asset_type, {schema}.instruments.asset_type),
                exchange = EXCLUDED.exchange,
                instrument_type = EXCLUDED.instrument_type,
                source_table = EXCLUDED.source_table,
                category = EXCLUDED.category,
                is_active = EXCLUDED.is_active,
                updated_at = NOW()
            """
        )
    conn.commit()


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


def iter_ticker_rows(csv_file: io.TextIOBase) -> Iterator[tuple]:
    reader = csv.DictReader(csv_file)
    reader.fieldnames = [normalise_header(x) for x in (reader.fieldnames or [])]

    for row in reader:
        symbol = row_get(row, "ticker", "symbol")
        if not symbol:
            continue

        # Sharadar TICKERS usually has table/category/name/exchange/isdelisted-ish metadata.
        name = row_get(row, "name")
        exchange = row_get(row, "exchange")
        source_table = row_get(row, "table")
        category = row_get(row, "category", "sector")
        instrument_type = row_get(row, "type", "security_type", "asset_type", "category")
        asset_type = infer_asset_type(source_table, instrument_type, category)

        is_delisted = row_get(row, "isdelisted", "is_delisted", "delisted")
        is_active = not parse_bool(is_delisted, default=False)

        yield (
            symbol,
            name,
            exchange,
            asset_type,
            instrument_type,
            source_table,
            category,
            is_active,
        )


def iter_bar_rows(csv_file: io.TextIOBase, default_source_table: str) -> Iterator[tuple]:
    reader = csv.DictReader(csv_file)
    reader.fieldnames = [normalise_header(x) for x in (reader.fieldnames or [])]
    
    # Debug: print column names on first iteration
    _debug_printed = False

    for row in reader:
        if not _debug_printed:
            print(f"[debug] CSV columns: {reader.fieldnames}")
            _debug_printed = True
        symbol = row_get(row, "ticker", "symbol")
        trade_date = row_get(row, "date", "trade_date")
        if not symbol or not trade_date:
            continue

        open_ = parse_decimal(row_get(row, "open"))
        high = parse_decimal(row_get(row, "high"))
        low = parse_decimal(row_get(row, "low"))
        close = parse_decimal(row_get(row, "close"))
        # Sharadar commonly has closeadj; keep a few aliases.
        adj_close = parse_decimal(row_get(row, "closeadj", "adj_close", "adjusted_close", "close_adjusted"))
        volume = parse_int(row_get(row, "volume"))
        source_table = row_get(row, "table") or default_source_table

        if close is None:
            continue
        if adj_close is None:
            adj_close = close

        yield (
            symbol,
            trade_date,
            open_,
            high,
            low,
            close,
            adj_close,
            volume,
            source_table,
        )


def download_zip_bytes(
        session: requests.Session,
        url: str,
        config: Config,
        poll_seconds: int = 5,
        max_wait_seconds: int = 300,
) -> bytes:
    """
    Nasdaq bulk export flow:
    1. GET exporter metadata CSV from the datatable URL with qopts.export=true
    2. Poll until file.status == Fresh
    3. Download the actual ZIP from file.link
    """
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
            raise RuntimeError(
                f"Nasdaq bulk export metadata was empty for URL: {url}"
            )

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
            ) as resp:
                data = resp.content

            if data[:2] != b"PK":
                raise RuntimeError(
                    "Downloaded bulk file is not a ZIP. "
                    f"First 200 bytes: {data[:200]!r}"
                )

            return data

        if file_status.lower() in {"creating", "regenerating"}:
            if time.time() >= deadline:
                raise TimeoutError(
                    f"Timed out waiting for bulk export to become Fresh. "
                    f"Last status={last_status!r}, url={url}"
                )
            time.sleep(poll_seconds)
            continue

        raise RuntimeError(
            f"Unexpected Nasdaq bulk export status {file_status!r} for URL: {url}. "
            f"Metadata row={row}"
        )

def ingest_tickers(session: requests.Session, conn: psycopg.Connection, config: Config) -> None:
    print("[info] downloading SHARADAR/TICKERS...")
    data = download_zip_bytes(session, build_export_url("SHARADAR/TICKERS", config.api_key), config)
    csv_file = first_csv_from_response_bytes(data)

    create_staging_tables(conn)
    count = copy_rows(
        conn,
        "stg_tickers",
        ["symbol", "name", "exchange", "asset_type", "instrument_type", "source_table", "category", "is_active"],
        iter_ticker_rows(csv_file)
    )
    if count == 0:
        raise RuntimeError("Parsed zero ticker rows from SHARADAR/TICKERS export")
    print(f"[info] copied {count:,} ticker rows to staging")

    upsert_instruments(conn, config.schema)
    print("[ok] upserted instruments")


def ingest_bars_for_table(
        session: requests.Session,
        conn: psycopg.Connection,
        config: Config,
        table: str,
) -> None:
    # Validate that the table is supported for bar data ingestion
    supported_bar_tables = {
        "SHARADAR/SEP",
        "SHARADAR/SFP",
    }
    if table not in supported_bar_tables:
        raise ValueError(
            f"Table {table!r} does not exist or is not supported for bar data ingestion. "
            f"Valid tables: {', '.join(sorted(supported_bar_tables))}."
        )
    
    print(f"[info] downloading {table}...")
    data = download_zip_bytes(session, build_export_url(table, config.api_key), config)
    csv_file = first_csv_from_response_bytes(data)


    create_staging_tables(conn)
    count = copy_rows(
        conn,
        "stg_bars",
        ["symbol", "trade_date", "open", "high", "low", "close", "adj_close", "volume", "source_table"],
        iter_bar_rows(csv_file, default_source_table=table),
    )
    if count == 0:
        raise RuntimeError(
            f"Parsed zero bar rows from {table} export. "
            f"This may indicate the data format has changed or the table is empty. "
            f"Check that the CSV contains expected columns: ticker/symbol, date/trade_date, open, high, low, close, volume."
        )
    print(f"[info] copied {count:,} bar rows from {table} to staging")

    upsert_bars(conn, config.schema)
    print(f"[ok] upserted daily_bars from {table}")

def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Sharadar data and store directly in Postgres")
    parser.add_argument("--api-key", default=os.getenv("NDL_API_KEY") or os.getenv("NASDAQ_DATA_LINK_API_KEY"))
    parser.add_argument("--db-dsn", default=os.getenv("DATABASE_URL") or os.getenv("PG_DSN"))
    parser.add_argument("--schema", default=os.getenv("DB_SCHEMA", "strategy_engine"))
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Truncate all existing data before ingesting. WARNING: This will delete all data!",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=DEFAULT_TABLES,
        help="Subset of tables to ingest. Default: SHARADAR/TICKERS SHARADAR/SEP SHARADAR/SFP",
    )
    return parser.parse_args(list(argv))


def main(argv: Iterable[str]) -> int:
    args = parse_args(argv)

    if not args.api_key:
        print("Missing API key. Set NDL_API_KEY in .env or pass --api-key.", file=sys.stderr)
        return 2
    if not args.db_dsn:
        print("Missing DB DSN. Set DATABASE_URL or PG_DSN in .env or pass --db-dsn.", file=sys.stderr)
        return 2

    config = Config(
        api_key=args.api_key,
        db_dsn=args.db_dsn,
        schema=args.schema,
        overwrite=args.overwrite,
    )

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "strategy-engine-sharadar-ingest/1.0",
            "Accept": "*/*",
        }
    )

    with psycopg.connect(config.db_dsn, autocommit=False) as conn:
        ensure_schema(conn, config.schema)

        if config.overwrite:
            truncate_tables(conn, config.schema)

        if "SHARADAR/TICKERS" in args.tables:
            ingest_tickers(session, conn, config)

        for table in args.tables:
            if table == "SHARADAR/TICKERS":
                continue
            ingest_bars_for_table(session, conn, config, table)

    print("[summary] finished successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))