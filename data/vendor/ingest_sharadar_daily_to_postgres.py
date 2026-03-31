#!/usr/bin/env python3
"""
Ingest SHARADAR/DAILY (daily metrics) into Postgres.

SHARADAR/DAILY provides daily-computed metrics for every US equity in the
Sharadar universe: market cap, enterprise value, price-to-earnings,
price-to-sales, price-to-book, and dozens more.

Follows the shared Sharadar ingest protocol (see sharadar_common.py):
  1. fetch_api_sample        → pull 75 rows for validation
  2. validate_sample_shape   → assert required columns present          [rule 2c]
  3. detect_schema_drift     → compare API columns to known set         [rule 2b]
  4. Full bulk load if DB empty / --overwrite / schema drift
  5. Incremental load otherwise                                         [rule 2a]
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

DATASET_CODE = "SHARADAR/DAILY"

# Columns the API must return (rule 2c) and the full known set (rule 2b).
REQUIRED_DAILY_COLS: set[str] = {"ticker", "date", "lastupdated"}
KNOWN_DAILY_COLS: set[str] = {
    "ticker", "date", "lastupdated",
    "ev", "evebit", "evebitda",
    "marketcap",
    "pb", "pe", "pe1", "ps", "ps1",
    "assetturnover", "commonequity", "debt", "dps",
    "ebitda", "ebitdamargin", "ebit",
    "eps", "epsdil", "epsusd", "epsdilgrowth1yr",
    "fcf", "fcfps", "gp", "grossmargin",
    "intangibles", "investedcapital", "inventory",
    "ncf", "ncfbus", "ncfcommon", "ncfdebt", "ncfdiv", "ncff", "ncfi", "ncfinv", "ncfo", "ncfx",
    "netinc", "netinccomstock", "netincnci", "netmargin",
    "opex", "opinc",
    "payables", "receivables", "retearn",
    "revenue", "revenueusd", "rnd",
    "ros", "roic", "roe", "roa",
    "sgna", "sharefactor", "sharesbas", "shareswa", "shareswadil",
    "sps", "tangibles", "taxassets", "taxliabilities",
    "tbvps", "workingcapital",
    "price", "volume",
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


def parse_bigint(value: Optional[str]) -> Optional[int]:
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
            sleep_s = min(2 ** attempt, 30)
            print(f"[warn] request failed ({attempt}/{max_attempts}): {exc}")
            print(f"[info] retrying in {sleep_s}s…")
            time.sleep(sleep_s)
    assert last_exc is not None
    raise last_exc


# ── bulk download ─────────────────────────────────────────────────────────────

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
                raise TimeoutError(
                    f"Timed out waiting for Nasdaq export {datatable_code} to become Fresh"
                )
            time.sleep(poll_seconds)
            continue

        raise RuntimeError(
            f"Unexpected bulk export status for {datatable_code}: {status!r}; row={row}"
        )


def first_csv_from_bytes(data: bytes) -> io.TextIOBase:
    if zipfile.is_zipfile(io.BytesIO(data)):
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                raise RuntimeError(f"ZIP download contained no CSV files: {zf.namelist()}")
            name = csv_names[0]
            print(f"[debug] extracted CSV from ZIP: {name}")
            return io.TextIOWrapper(
                io.BytesIO(zf.read(name)), encoding="utf-8", errors="replace", newline=""
            )

    text = data.decode("utf-8", errors="replace")
    if text.lstrip().startswith("{"):
        raise RuntimeError(f"Nasdaq returned JSON instead of CSV: {text[:500]}")
    if "<html" in text[:500].lower():
        raise RuntimeError(f"Nasdaq returned HTML instead of CSV: {text[:500]}")
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

        # The DAILY metrics table — one row per ticker per date
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.sharadar_daily (
                instrument_id BIGINT NOT NULL REFERENCES {schema}.instruments(instrument_id),
                trade_date DATE NOT NULL,
                last_updated TIMESTAMPTZ,
                -- valuation metrics
                ev NUMERIC,
                evebit NUMERIC,
                evebitda NUMERIC,
                marketcap NUMERIC,
                pb NUMERIC,
                pe NUMERIC,
                pe1 NUMERIC,
                ps NUMERIC,
                ps1 NUMERIC,
                -- price / volume
                price NUMERIC,
                volume BIGINT,
                -- profitability
                ebitda NUMERIC,
                ebitdamargin NUMERIC,
                ebit NUMERIC,
                grossmargin NUMERIC,
                netmargin NUMERIC,
                ros NUMERIC,
                roic NUMERIC,
                roe NUMERIC,
                roa NUMERIC,
                -- per-share
                eps NUMERIC,
                epsdil NUMERIC,
                epsusd NUMERIC,
                epsdilgrowth1yr NUMERIC,
                fcfps NUMERIC,
                sps NUMERIC,
                tbvps NUMERIC,
                dps NUMERIC,
                -- income statement
                revenue NUMERIC,
                revenueusd NUMERIC,
                gp NUMERIC,
                opex NUMERIC,
                opinc NUMERIC,
                sgna NUMERIC,
                rnd NUMERIC,
                netinc NUMERIC,
                netinccomstock NUMERIC,
                netincnci NUMERIC,
                -- balance sheet
                assetturnover NUMERIC,
                commonequity NUMERIC,
                debt NUMERIC,
                intangibles NUMERIC,
                investedcapital NUMERIC,
                inventory NUMERIC,
                payables NUMERIC,
                receivables NUMERIC,
                retearn NUMERIC,
                tangibles NUMERIC,
                taxassets NUMERIC,
                taxliabilities NUMERIC,
                workingcapital NUMERIC,
                -- cash flow
                fcf NUMERIC,
                ncf NUMERIC,
                ncfbus NUMERIC,
                ncfcommon NUMERIC,
                ncfdebt NUMERIC,
                ncfdiv NUMERIC,
                ncff NUMERIC,
                ncfi NUMERIC,
                ncfinv NUMERIC,
                ncfo NUMERIC,
                ncfx NUMERIC,
                -- share data
                sharefactor NUMERIC,
                sharesbas NUMERIC,
                shareswa NUMERIC,
                shareswadil NUMERIC,
                -- meta
                source_table TEXT NOT NULL DEFAULT 'DAILY',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (instrument_id, trade_date)
            )
            """
        )

        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS sharadar_daily_trade_date_idx
            ON {schema}.sharadar_daily (trade_date)
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS sharadar_daily_instrument_date_idx
            ON {schema}.sharadar_daily (instrument_id, trade_date)
            """
        )

    conn.commit()


def get_max_trade_date(conn: psycopg.Connection, schema: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(trade_date) FROM {schema}.sharadar_daily")
        result = cur.fetchone()[0]
        return str(result) if result else None


# ── staging ───────────────────────────────────────────────────────────────────

_STAGING_COLUMNS = [
    "symbol", "trade_date", "last_updated",
    "ev", "evebit", "evebitda", "marketcap",
    "pb", "pe", "pe1", "ps", "ps1",
    "price", "volume",
    "ebitda", "ebitdamargin", "ebit",
    "grossmargin", "netmargin", "ros", "roic", "roe", "roa",
    "eps", "epsdil", "epsusd", "epsdilgrowth1yr", "fcfps", "sps", "tbvps", "dps",
    "revenue", "revenueusd", "gp", "opex", "opinc", "sgna", "rnd",
    "netinc", "netinccomstock", "netincnci",
    "assetturnover", "commonequity", "debt", "intangibles",
    "investedcapital", "inventory", "payables", "receivables",
    "retearn", "tangibles", "taxassets", "taxliabilities", "workingcapital",
    "fcf", "ncf", "ncfbus", "ncfcommon", "ncfdebt", "ncfdiv",
    "ncff", "ncfi", "ncfinv", "ncfo", "ncfx",
    "sharefactor", "sharesbas", "shareswa", "shareswadil",
]


def create_staging_table(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg_sharadar_daily")
        cur.execute(
            """
            CREATE TEMP TABLE stg_sharadar_daily (
                symbol TEXT,
                trade_date DATE,
                last_updated TIMESTAMPTZ,
                ev NUMERIC,
                evebit NUMERIC,
                evebitda NUMERIC,
                marketcap NUMERIC,
                pb NUMERIC,
                pe NUMERIC,
                pe1 NUMERIC,
                ps NUMERIC,
                ps1 NUMERIC,
                price NUMERIC,
                volume BIGINT,
                ebitda NUMERIC,
                ebitdamargin NUMERIC,
                ebit NUMERIC,
                grossmargin NUMERIC,
                netmargin NUMERIC,
                ros NUMERIC,
                roic NUMERIC,
                roe NUMERIC,
                roa NUMERIC,
                eps NUMERIC,
                epsdil NUMERIC,
                epsusd NUMERIC,
                epsdilgrowth1yr NUMERIC,
                fcfps NUMERIC,
                sps NUMERIC,
                tbvps NUMERIC,
                dps NUMERIC,
                revenue NUMERIC,
                revenueusd NUMERIC,
                gp NUMERIC,
                opex NUMERIC,
                opinc NUMERIC,
                sgna NUMERIC,
                rnd NUMERIC,
                netinc NUMERIC,
                netinccomstock NUMERIC,
                netincnci NUMERIC,
                assetturnover NUMERIC,
                commonequity NUMERIC,
                debt NUMERIC,
                intangibles NUMERIC,
                investedcapital NUMERIC,
                inventory NUMERIC,
                payables NUMERIC,
                receivables NUMERIC,
                retearn NUMERIC,
                tangibles NUMERIC,
                taxassets NUMERIC,
                taxliabilities NUMERIC,
                workingcapital NUMERIC,
                fcf NUMERIC,
                ncf NUMERIC,
                ncfbus NUMERIC,
                ncfcommon NUMERIC,
                ncfdebt NUMERIC,
                ncfdiv NUMERIC,
                ncff NUMERIC,
                ncfi NUMERIC,
                ncfinv NUMERIC,
                ncfo NUMERIC,
                ncfx NUMERIC,
                sharefactor NUMERIC,
                sharesbas NUMERIC,
                shareswa NUMERIC,
                shareswadil NUMERIC
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

def _parse_daily_row(row: dict[str, str]) -> Optional[tuple]:
    """Return a staging tuple from a single CSV/JSON row, or None to skip."""
    symbol = row_get(row, "ticker", "symbol")
    trade_date = row_get(row, "date", "trade_date")
    if not symbol or not trade_date:
        return None

    last_updated = row_get(row, "lastupdated", "last_updated")

    return (
        symbol,
        trade_date,
        last_updated,
        # valuation
        parse_decimal(row_get(row, "ev")),
        parse_decimal(row_get(row, "evebit")),
        parse_decimal(row_get(row, "evebitda")),
        parse_decimal(row_get(row, "marketcap")),
        parse_decimal(row_get(row, "pb")),
        parse_decimal(row_get(row, "pe")),
        parse_decimal(row_get(row, "pe1")),
        parse_decimal(row_get(row, "ps")),
        parse_decimal(row_get(row, "ps1")),
        # price / volume
        parse_decimal(row_get(row, "price")),
        parse_bigint(row_get(row, "volume")),
        # profitability
        parse_decimal(row_get(row, "ebitda")),
        parse_decimal(row_get(row, "ebitdamargin")),
        parse_decimal(row_get(row, "ebit")),
        parse_decimal(row_get(row, "grossmargin")),
        parse_decimal(row_get(row, "netmargin")),
        parse_decimal(row_get(row, "ros")),
        parse_decimal(row_get(row, "roic")),
        parse_decimal(row_get(row, "roe")),
        parse_decimal(row_get(row, "roa")),
        # per-share
        parse_decimal(row_get(row, "eps")),
        parse_decimal(row_get(row, "epsdil")),
        parse_decimal(row_get(row, "epsusd")),
        parse_decimal(row_get(row, "epsdilgrowth1yr")),
        parse_decimal(row_get(row, "fcfps")),
        parse_decimal(row_get(row, "sps")),
        parse_decimal(row_get(row, "tbvps")),
        parse_decimal(row_get(row, "dps")),
        # income statement
        parse_decimal(row_get(row, "revenue")),
        parse_decimal(row_get(row, "revenueusd")),
        parse_decimal(row_get(row, "gp")),
        parse_decimal(row_get(row, "opex")),
        parse_decimal(row_get(row, "opinc")),
        parse_decimal(row_get(row, "sgna")),
        parse_decimal(row_get(row, "rnd")),
        parse_decimal(row_get(row, "netinc")),
        parse_decimal(row_get(row, "netinccomstock")),
        parse_decimal(row_get(row, "netincnci")),
        # balance sheet
        parse_decimal(row_get(row, "assetturnover")),
        parse_decimal(row_get(row, "commonequity")),
        parse_decimal(row_get(row, "debt")),
        parse_decimal(row_get(row, "intangibles")),
        parse_decimal(row_get(row, "investedcapital")),
        parse_decimal(row_get(row, "inventory")),
        parse_decimal(row_get(row, "payables")),
        parse_decimal(row_get(row, "receivables")),
        parse_decimal(row_get(row, "retearn")),
        parse_decimal(row_get(row, "tangibles")),
        parse_decimal(row_get(row, "taxassets")),
        parse_decimal(row_get(row, "taxliabilities")),
        parse_decimal(row_get(row, "workingcapital")),
        # cash flow
        parse_decimal(row_get(row, "fcf")),
        parse_decimal(row_get(row, "ncf")),
        parse_decimal(row_get(row, "ncfbus")),
        parse_decimal(row_get(row, "ncfcommon")),
        parse_decimal(row_get(row, "ncfdebt")),
        parse_decimal(row_get(row, "ncfdiv")),
        parse_decimal(row_get(row, "ncff")),
        parse_decimal(row_get(row, "ncfi")),
        parse_decimal(row_get(row, "ncfinv")),
        parse_decimal(row_get(row, "ncfo")),
        parse_decimal(row_get(row, "ncfx")),
        # shares
        parse_decimal(row_get(row, "sharefactor")),
        parse_decimal(row_get(row, "sharesbas")),
        parse_decimal(row_get(row, "shareswa")),
        parse_decimal(row_get(row, "shareswadil")),
    )


def iter_daily_rows(csv_file: io.TextIOBase) -> Iterator[tuple]:
    reader = csv.DictReader(csv_file)
    reader.fieldnames = [normalise_header(x) for x in (reader.fieldnames or [])]

    debug_printed = False
    for row in reader:
        if not debug_printed:
            print(f"[debug] CSV columns: {reader.fieldnames}")
            debug_printed = True
        parsed = _parse_daily_row(row)
        if parsed is not None:
            yield parsed


def iter_daily_rows_from_dicts(rows: list[dict[str, str]]) -> Iterator[tuple]:
    """Variant for rows already loaded as dicts (incremental JSON path)."""
    for row in rows:
        parsed = _parse_daily_row(row)
        if parsed is not None:
            yield parsed


# ── upsert ────────────────────────────────────────────────────────────────────

def upsert_instruments_from_daily(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {schema}.instruments (
                symbol, asset_type, source_table, is_active, updated_at
            )
            SELECT DISTINCT s.symbol, 'STOCK', 'DAILY', TRUE, NOW()
            FROM stg_sharadar_daily s
            WHERE s.symbol IS NOT NULL
            ON CONFLICT (symbol) DO UPDATE
            SET updated_at = NOW(),
                source_table = COALESCE({schema}.instruments.source_table, EXCLUDED.source_table)
            """
        )


def upsert_daily(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {schema}.sharadar_daily (
                instrument_id, trade_date, last_updated,
                ev, evebit, evebitda, marketcap,
                pb, pe, pe1, ps, ps1,
                price, volume,
                ebitda, ebitdamargin, ebit,
                grossmargin, netmargin, ros, roic, roe, roa,
                eps, epsdil, epsusd, epsdilgrowth1yr, fcfps, sps, tbvps, dps,
                revenue, revenueusd, gp, opex, opinc, sgna, rnd,
                netinc, netinccomstock, netincnci,
                assetturnover, commonequity, debt, intangibles,
                investedcapital, inventory, payables, receivables,
                retearn, tangibles, taxassets, taxliabilities, workingcapital,
                fcf, ncf, ncfbus, ncfcommon, ncfdebt, ncfdiv,
                ncff, ncfi, ncfinv, ncfo, ncfx,
                sharefactor, sharesbas, shareswa, shareswadil,
                updated_at
            )
            SELECT
                i.instrument_id, s.trade_date, s.last_updated,
                s.ev, s.evebit, s.evebitda, s.marketcap,
                s.pb, s.pe, s.pe1, s.ps, s.ps1,
                s.price, s.volume,
                s.ebitda, s.ebitdamargin, s.ebit,
                s.grossmargin, s.netmargin, s.ros, s.roic, s.roe, s.roa,
                s.eps, s.epsdil, s.epsusd, s.epsdilgrowth1yr, s.fcfps, s.sps, s.tbvps, s.dps,
                s.revenue, s.revenueusd, s.gp, s.opex, s.opinc, s.sgna, s.rnd,
                s.netinc, s.netinccomstock, s.netincnci,
                s.assetturnover, s.commonequity, s.debt, s.intangibles,
                s.investedcapital, s.inventory, s.payables, s.receivables,
                s.retearn, s.tangibles, s.taxassets, s.taxliabilities, s.workingcapital,
                s.fcf, s.ncf, s.ncfbus, s.ncfcommon, s.ncfdebt, s.ncfdiv,
                s.ncff, s.ncfi, s.ncfinv, s.ncfo, s.ncfx,
                s.sharefactor, s.sharesbas, s.shareswa, s.shareswadil,
                NOW()
            FROM stg_sharadar_daily s
            JOIN {schema}.instruments i ON i.symbol = s.symbol
            WHERE s.symbol IS NOT NULL
              AND s.trade_date IS NOT NULL
            ON CONFLICT (instrument_id, trade_date) DO UPDATE SET
                last_updated        = EXCLUDED.last_updated,
                ev                  = EXCLUDED.ev,
                evebit              = EXCLUDED.evebit,
                evebitda            = EXCLUDED.evebitda,
                marketcap           = EXCLUDED.marketcap,
                pb                  = EXCLUDED.pb,
                pe                  = EXCLUDED.pe,
                pe1                 = EXCLUDED.pe1,
                ps                  = EXCLUDED.ps,
                ps1                 = EXCLUDED.ps1,
                price               = EXCLUDED.price,
                volume              = EXCLUDED.volume,
                ebitda              = EXCLUDED.ebitda,
                ebitdamargin        = EXCLUDED.ebitdamargin,
                ebit                = EXCLUDED.ebit,
                grossmargin         = EXCLUDED.grossmargin,
                netmargin           = EXCLUDED.netmargin,
                ros                 = EXCLUDED.ros,
                roic                = EXCLUDED.roic,
                roe                 = EXCLUDED.roe,
                roa                 = EXCLUDED.roa,
                eps                 = EXCLUDED.eps,
                epsdil              = EXCLUDED.epsdil,
                epsusd              = EXCLUDED.epsusd,
                epsdilgrowth1yr     = EXCLUDED.epsdilgrowth1yr,
                fcfps               = EXCLUDED.fcfps,
                sps                 = EXCLUDED.sps,
                tbvps               = EXCLUDED.tbvps,
                dps                 = EXCLUDED.dps,
                revenue             = EXCLUDED.revenue,
                revenueusd          = EXCLUDED.revenueusd,
                gp                  = EXCLUDED.gp,
                opex                = EXCLUDED.opex,
                opinc               = EXCLUDED.opinc,
                sgna                = EXCLUDED.sgna,
                rnd                 = EXCLUDED.rnd,
                netinc              = EXCLUDED.netinc,
                netinccomstock      = EXCLUDED.netinccomstock,
                netincnci           = EXCLUDED.netincnci,
                assetturnover       = EXCLUDED.assetturnover,
                commonequity        = EXCLUDED.commonequity,
                debt                = EXCLUDED.debt,
                intangibles         = EXCLUDED.intangibles,
                investedcapital     = EXCLUDED.investedcapital,
                inventory           = EXCLUDED.inventory,
                payables            = EXCLUDED.payables,
                receivables         = EXCLUDED.receivables,
                retearn             = EXCLUDED.retearn,
                tangibles           = EXCLUDED.tangibles,
                taxassets           = EXCLUDED.taxassets,
                taxliabilities      = EXCLUDED.taxliabilities,
                workingcapital      = EXCLUDED.workingcapital,
                fcf                 = EXCLUDED.fcf,
                ncf                 = EXCLUDED.ncf,
                ncfbus              = EXCLUDED.ncfbus,
                ncfcommon           = EXCLUDED.ncfcommon,
                ncfdebt             = EXCLUDED.ncfdebt,
                ncfdiv              = EXCLUDED.ncfdiv,
                ncff                = EXCLUDED.ncff,
                ncfi                = EXCLUDED.ncfi,
                ncfinv              = EXCLUDED.ncfinv,
                ncfo                = EXCLUDED.ncfo,
                ncfx                = EXCLUDED.ncfx,
                sharefactor         = EXCLUDED.sharefactor,
                sharesbas           = EXCLUDED.sharesbas,
                shareswa            = EXCLUDED.shareswa,
                shareswadil         = EXCLUDED.shareswadil,
                updated_at          = NOW()
            """
        )


# ── load orchestration ────────────────────────────────────────────────────────

def _stage_and_upsert(
    conn: psycopg.Connection,
    rows_iter: Iterator[tuple],
    config: Config,
) -> int:
    create_staging_table(conn)
    total = 0
    for batch in chunked(rows_iter, config.batch_size):
        copied = copy_rows(conn, "stg_sharadar_daily", _STAGING_COLUMNS, batch)
        total += copied
        print(f"[info] staged {copied:,} daily rows (running total {total:,})")

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM stg_sharadar_daily")
        staged_count = cur.fetchone()[0]
    print(f"[info] staged sharadar daily rows: {staged_count:,}")

    upsert_instruments_from_daily(conn, config.schema)
    upsert_daily(conn, config.schema)
    conn.commit()
    return total


def _full_bulk_load(
    session: requests.Session,
    conn: psycopg.Connection,
    config: Config,
    *,
    truncate: bool = False,
) -> None:
    if truncate:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {config.schema}.sharadar_daily")
        conn.commit()
        print(f"[info] truncated {config.schema}.sharadar_daily")

    print(f"[info] downloading full bulk export for {DATASET_CODE}…")
    csv_file = download_csv_file(session, config)
    count = _stage_and_upsert(conn, iter_daily_rows(csv_file), config)
    print(f"[ok] full bulk load complete – {count:,} daily rows staged")


def _do_incremental_load(
    session: requests.Session,
    conn: psycopg.Connection,
    config: Config,
    since_date: str,
    until_date: str,
) -> None:
    _cols, inc_rows = fetch_incremental_rows(
        session,
        DATASET_CODE,
        config.api_key,
        date_filter_col="date",
        since_date=since_date,
        until_date=until_date,
    )
    if not inc_rows:
        print(f"[info] {DATASET_CODE}: no new/updated rows in window {since_date} → {until_date}")
        return
    count = _stage_and_upsert(conn, iter_daily_rows_from_dicts(inc_rows), config)
    print(f"[info] incremental load complete – {count:,} daily rows staged")


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Ingest SHARADAR/DAILY into Postgres")
    parser.add_argument(
        "--api-key",
        default=os.getenv("NDL_API_KEY") or os.getenv("NASDAQ_DATA_LINK_API_KEY") or os.getenv("NASDAQ_API_KEY"),
    )
    parser.add_argument(
        "--db-dsn",
        default=os.getenv("DATABASE_URL") or os.getenv("DB_DSN"),
    )
    parser.add_argument("--schema", default=os.getenv("DB_SCHEMA", "strategy_engine"))
    parser.add_argument("--batch-size", type=int, default=100_000)
    parser.add_argument("--overwrite", action="store_true")

    args = parser.parse_args()

    if not args.api_key:
        raise SystemExit(
            "Missing --api-key (or NDL_API_KEY / NASDAQ_DATA_LINK_API_KEY / NASDAQ_API_KEY in environment)"
        )
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

        # ── STEP 1: sample validation (rule 2c) ───────────────────────────────
        sample_cols, sample_rows = fetch_api_sample(
            session, DATASET_CODE, config.api_key, n=75
        )
        validate_sample_shape(DATASET_CODE, sample_cols, sample_rows, REQUIRED_DAILY_COLS)

        # ── STEP 2: schema-drift check (rule 2b) ──────────────────────────────
        drift = detect_schema_drift(DATASET_CODE, set(sample_cols), KNOWN_DAILY_COLS)
        max_date = get_max_trade_date(conn, config.schema)

        if config.overwrite or max_date is None:
            reason = "--overwrite" if config.overwrite else "table is empty"
            print(f"[info] {DATASET_CODE}: full bulk load ({reason})")
            _full_bulk_load(session, conn, config, truncate=config.overwrite)

        elif drift:
            print(f"[info] {DATASET_CODE}: full bulk reload due to schema drift (rule 2b)")
            _full_bulk_load(session, conn, config, truncate=True)

        else:
            # ── STEP 3: incremental load (rule 2a) ────────────────────────────
            yesterday = (date.today() - timedelta(days=1)).isoformat()
            since_date = (date.fromisoformat(max_date) - timedelta(days=1)).isoformat()
            print(
                f"[info] {DATASET_CODE}: incremental load – "
                f"DB max trade_date: {max_date}, window: {since_date} → {yesterday}"
            )
            _do_incremental_load(session, conn, config, since_date, yesterday)

        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {config.schema}.sharadar_daily")
            total = cur.fetchone()[0]

    print(f"[info] {config.schema}.sharadar_daily now contains {total:,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

