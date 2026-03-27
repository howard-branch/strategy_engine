#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import time
import zipfile
from dataclasses import dataclass
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

BASE_URL = "https://data.nasdaq.com/api/v3/datatables/{table}.csv"


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

        symbol = row_get(row, "ticker", "symbol")
        action_date = row_get(row, "date", "action_date")
        action = row_get(row, "action")
        value = parse_decimal(row_get(row, "value"))
        contra_ticker = row_get(row, "contraticker", "contra_ticker") or ""
        contra_name = row_get(row, "contraname", "contra_name")
        source_table = row_get(row, "table") or default_source_table

        if not symbol or not action_date or not action:
            continue

        yield (
            symbol,
            action_date,
            action,
            value,
            contra_ticker,
            contra_name,
            source_table,
        )


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

def ingest_actions(session: requests.Session, conn: psycopg.Connection, config: Config) -> None:
    table = "SHARADAR/ACTIONS"
    print(f"[info] downloading {table}...")

    data = download_zip_bytes(session, build_export_url(table, config.api_key), config)
    csv_file = first_csv_from_response_bytes(data)

    create_staging_table(conn)
    count = copy_rows(
        conn,
        "stg_actions",
        ["symbol", "action_date", "action", "value", "contra_ticker", "contra_name", "source_table"],
        iter_action_rows(csv_file, table),
    )

    if count == 0:
        raise RuntimeError(
            "Parsed zero corporate action rows from SHARADAR/ACTIONS export. "
            "Check the debug column list above and adjust iter_action_rows aliases if needed."
        )

    print(f"[info] copied {count:,} action rows to staging")
    upsert_actions(conn, config.schema)
    print("[ok] upserted corporate actions")


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
        raise RuntimeError("NDL_API_KEY not found in .env")
    if not db_dsn:
        raise RuntimeError("DATABASE_URL not found in .env")

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
        ingest_actions(session, conn, config)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(__import__("sys").argv[1:]))