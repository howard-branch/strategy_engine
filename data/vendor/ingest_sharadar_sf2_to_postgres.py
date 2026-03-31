#!/usr/bin/env python3
"""Ingest SHARADAR/SF2 (insider transactions) into Postgres."""
from __future__ import annotations

import csv
import io
import json
import sys
from pathlib import Path
from typing import Iterator

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent))

from sharadar_common import (
    IngestConfig,
    chunked,
    copy_rows,
    ensure_instruments_table,
    normalise_header,
    parse_bool,
    parse_ingest_args,
    parse_int,
    parse_numeric,
    resolve_credentials,
    row_get,
    run_ingest,
)

TABLE_CODE = "SHARADAR/SF2"
REQUIRED_SF2_COLS: set[str] = {"ticker", "filingdate", "rownum"}
KNOWN_SF2_COLS: set[str] = {
    "ticker", "filingdate", "rownum", "formtype", "issuername", "ownername",
    "officertitle", "isdirector", "isofficer", "istenpercentowner",
    "transactiondate", "securityadcode", "transactioncode",
    "sharesownedbeforetransaction", "transactionshares",
    "sharesownedfollowingtransaction", "transactionpricepershare",
    "transactionvalue", "securitytitle", "directorindirect",
    "natureofownership", "dateexercisable", "priceexercisable", "expirationdate",
}

_STAGING_COLUMNS = [
    "ticker", "filing_date", "row_num", "form_type", "issuer_name", "owner_name",
    "officer_title", "is_director", "is_officer", "is_ten_percent_owner",
    "transaction_date", "security_ad_code", "transaction_code",
    "shares_owned_before_transaction", "transaction_shares",
    "shares_owned_following_transaction", "transaction_price_per_share",
    "transaction_value", "security_title", "director_indirect", "nature_of_ownership",
    "date_exercisable", "price_exercisable", "expiration_date", "raw_record",
]


# ── Schema / DDL ─────────────────────────────────────────────────────────────

def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    ensure_instruments_table(conn, schema)
    with conn.cursor() as cur:
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
        for idx_col in ("filing_date", "ticker", "transaction_date",
                        "owner_name", "transaction_code"):
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS sharadar_sf2_{idx_col}_idx "
                f"ON {schema}.sharadar_sf2 ({idx_col})"
            )
    conn.commit()


def get_max_filing_date(conn: psycopg.Connection, schema: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(filing_date) FROM {schema}.sharadar_sf2")
        result = cur.fetchone()[0]
        return str(result) if result else None


# ── Row parsing ──────────────────────────────────────────────────────────────

def _sf2_row_to_tuple(row: dict[str, str]) -> tuple | None:
    r = {normalise_header(k): (v.strip() if isinstance(v, str) else v)
         for k, v in row.items()}
    ticker = row_get(r, "ticker", "symbol")
    filing_date = row_get(r, "filingdate", "filing_date")
    row_num = parse_int(row_get(r, "rownum"))
    if not ticker or not filing_date or row_num is None:
        return None
    return (
        ticker, filing_date, row_num,
        row_get(r, "formtype"), row_get(r, "issuername"), row_get(r, "ownername"),
        row_get(r, "officertitle"),
        parse_bool(row_get(r, "isdirector")),
        parse_bool(row_get(r, "isofficer")),
        parse_bool(row_get(r, "istenpercentowner")),
        row_get(r, "transactiondate"), row_get(r, "securityadcode"),
        row_get(r, "transactioncode"),
        parse_numeric(row_get(r, "sharesownedbeforetransaction")),
        parse_numeric(row_get(r, "transactionshares")),
        parse_numeric(row_get(r, "sharesownedfollowingtransaction")),
        parse_numeric(row_get(r, "transactionpricepershare")),
        parse_numeric(row_get(r, "transactionvalue")),
        row_get(r, "securitytitle"), row_get(r, "directorindirect"),
        row_get(r, "natureofownership"), row_get(r, "dateexercisable"),
        parse_numeric(row_get(r, "priceexercisable")), row_get(r, "expirationdate"),
        json.dumps(r, ensure_ascii=False),
    )


def iter_rows_csv(csv_file: io.TextIOBase) -> Iterator[tuple]:
    reader = csv.DictReader(csv_file)
    reader.fieldnames = [normalise_header(x) for x in (reader.fieldnames or [])]
    debug_printed = False
    for row in reader:
        if not debug_printed:
            print(f"[debug] CSV columns: {reader.fieldnames}")
            debug_printed = True
        result = _sf2_row_to_tuple(row)
        if result is not None:
            yield result


def iter_rows_dicts(rows: list[dict[str, str]]) -> Iterator[tuple]:
    for row in rows:
        result = _sf2_row_to_tuple(row)
        if result is not None:
            yield result


# ── Staging + upsert ─────────────────────────────────────────────────────────

def stage_and_upsert(
    conn: psycopg.Connection,
    rows_iter: Iterator[tuple],
    config: IngestConfig,
) -> int:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg_sharadar_sf2")
        cur.execute(
            """
            CREATE TEMP TABLE stg_sharadar_sf2 (
                ticker TEXT, filing_date DATE, row_num INTEGER,
                form_type TEXT, issuer_name TEXT, owner_name TEXT,
                officer_title TEXT, is_director BOOLEAN, is_officer BOOLEAN,
                is_ten_percent_owner BOOLEAN, transaction_date DATE,
                security_ad_code TEXT, transaction_code TEXT,
                shares_owned_before_transaction NUMERIC,
                transaction_shares NUMERIC,
                shares_owned_following_transaction NUMERIC,
                transaction_price_per_share NUMERIC,
                transaction_value NUMERIC, security_title TEXT,
                director_indirect TEXT, nature_of_ownership TEXT,
                date_exercisable DATE, price_exercisable NUMERIC,
                expiration_date DATE, raw_record JSONB
            ) ON COMMIT PRESERVE ROWS
            """
        )
    total = 0
    for batch in chunked(rows_iter, config.batch_size):
        copied = copy_rows(conn, "stg_sharadar_sf2", _STAGING_COLUMNS, batch)
        total += copied
        print(f"[info] staged {copied:,} SF2 rows (running total {total:,})")
    print(f"[info] staged sharadar SF2 rows: {total:,}")

    with conn.cursor() as cur:
        # instruments
        cur.execute(
            f"""
            INSERT INTO {config.schema}.instruments
                (symbol, asset_type, source_table, is_active, updated_at)
            SELECT DISTINCT s.ticker, 'STOCK', 'SF2', TRUE, NOW()
            FROM stg_sharadar_sf2 s WHERE s.ticker IS NOT NULL
            ON CONFLICT (symbol) DO UPDATE
            SET updated_at = NOW(),
                source_table = COALESCE(
                    {config.schema}.instruments.source_table,
                    EXCLUDED.source_table
                )
            """
        )
        # sf2
        cur.execute(
            f"""
            INSERT INTO {config.schema}.sharadar_sf2 (
                ticker, filing_date, row_num, form_type, issuer_name,
                owner_name, officer_title, is_director, is_officer,
                is_ten_percent_owner, transaction_date, security_ad_code,
                transaction_code, shares_owned_before_transaction,
                transaction_shares, shares_owned_following_transaction,
                transaction_price_per_share, transaction_value, security_title,
                director_indirect, nature_of_ownership, date_exercisable,
                price_exercisable, expiration_date, raw_record, updated_at
            )
            SELECT DISTINCT ON (s.ticker, s.filing_date, s.row_num)
                s.ticker, s.filing_date, s.row_num, s.form_type, s.issuer_name,
                s.owner_name, s.officer_title, s.is_director, s.is_officer,
                s.is_ten_percent_owner, s.transaction_date, s.security_ad_code,
                s.transaction_code, s.shares_owned_before_transaction,
                s.transaction_shares, s.shares_owned_following_transaction,
                s.transaction_price_per_share, s.transaction_value, s.security_title,
                s.director_indirect, s.nature_of_ownership, s.date_exercisable,
                s.price_exercisable, s.expiration_date, s.raw_record, NOW()
            FROM stg_sharadar_sf2 s
            WHERE s.ticker IS NOT NULL AND s.filing_date IS NOT NULL
              AND s.row_num IS NOT NULL
            ORDER BY s.ticker, s.filing_date, s.row_num
            ON CONFLICT (ticker, filing_date, row_num) DO UPDATE SET
                form_type = EXCLUDED.form_type, issuer_name = EXCLUDED.issuer_name,
                owner_name = EXCLUDED.owner_name, officer_title = EXCLUDED.officer_title,
                is_director = EXCLUDED.is_director, is_officer = EXCLUDED.is_officer,
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
                raw_record = EXCLUDED.raw_record, updated_at = NOW()
            """
        )
    conn.commit()
    return total


def truncate_target(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {schema}.sharadar_sf2")
    conn.commit()
    print(f"[info] truncated {schema}.sharadar_sf2")


def count_target(conn: psycopg.Connection, schema: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.sharadar_sf2")
        return cur.fetchone()[0]


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> int:
    args = parse_ingest_args("Ingest SHARADAR/SF2 into Postgres")
    api_key, db_dsn = resolve_credentials()
    config = IngestConfig(
        api_key=api_key, db_dsn=db_dsn,
        schema=args.schema, batch_size=args.batch_size,
        overwrite=args.overwrite,
    )
    return run_ingest(
        table_code=TABLE_CODE,
        required_cols=REQUIRED_SF2_COLS,
        known_cols=KNOWN_SF2_COLS,
        date_filter_col="filingdate",
        ensure_schema_fn=ensure_schema,
        get_max_date_fn=get_max_filing_date,
        stage_and_upsert_fn=stage_and_upsert,
        iter_rows_csv_fn=iter_rows_csv,
        iter_rows_dicts_fn=iter_rows_dicts,
        truncate_fn=truncate_target,
        count_fn=count_target,
        config=config,
    )


if __name__ == "__main__":
    raise SystemExit(main())
