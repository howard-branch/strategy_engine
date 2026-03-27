#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

import dotenv
import psycopg

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ENV_FILE = _PROJECT_ROOT / '.env'
if _ENV_FILE.exists():
    dotenv.load_dotenv(_ENV_FILE)

DEFAULT_SCHEMA = 'strategy_engine'


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

        cur.execute(
            f'''
            CREATE INDEX IF NOT EXISTS fundamentals_sf1_instrument_idx
            ON {schema}.fundamentals_sf1 (instrument_id)
            '''
        )
        cur.execute(
            f'''
            CREATE INDEX IF NOT EXISTS fundamentals_sf1_datekey_idx
            ON {schema}.fundamentals_sf1 (datekey)
            '''
        )
        cur.execute(
            f'''
            CREATE INDEX IF NOT EXISTS fundamentals_sf1_dimension_date_idx
            ON {schema}.fundamentals_sf1 (dimension, calendardate)
            '''
        )
        cur.execute(
            f'''
            CREATE INDEX IF NOT EXISTS fundamentals_sf1_lastupdated_idx
            ON {schema}.fundamentals_sf1 (lastupdated)
            '''
        )
        cur.execute(
            f'''
            CREATE INDEX IF NOT EXISTS fundamentals_sf1_data_gin_idx
            ON {schema}.fundamentals_sf1 USING GIN (data)
            '''
        )

        cur.execute(
            f'''
            CREATE OR REPLACE VIEW {schema}.fundamentals_sf1_common AS
            SELECT
                f.instrument_id,
                f.ticker,
                f.dimension,
                f.calendardate,
                f.datekey,
                f.reportperiod,
                f.lastupdated,
                NULLIF(f.data ->> 'revenue', '')::NUMERIC(30, 6) AS revenue,
                NULLIF(f.data ->> 'netinc', '')::NUMERIC(30, 6) AS netinc,
                NULLIF(f.data ->> 'ebitda', '')::NUMERIC(30, 6) AS ebitda,
                NULLIF(f.data ->> 'fcf', '')::NUMERIC(30, 6) AS fcf,
                NULLIF(f.data ->> 'assets', '')::NUMERIC(30, 6) AS assets,
                NULLIF(f.data ->> 'liabilities', '')::NUMERIC(30, 6) AS liabilities,
                NULLIF(f.data ->> 'equity', '')::NUMERIC(30, 6) AS equity,
                NULLIF(f.data ->> 'cashneq', '')::NUMERIC(30, 6) AS cashneq,
                NULLIF(f.data ->> 'debt', '')::NUMERIC(30, 6) AS debt,
                NULLIF(f.data ->> 'workingcapital', '')::NUMERIC(30, 6) AS workingcapital,
                NULLIF(f.data ->> 'marketcap', '')::NUMERIC(30, 6) AS marketcap,
                NULLIF(f.data ->> 'ev', '')::NUMERIC(30, 6) AS enterprise_value,
                NULLIF(f.data ->> 'pe', '')::NUMERIC(30, 6) AS pe,
                NULLIF(f.data ->> 'pb', '')::NUMERIC(30, 6) AS pb,
                NULLIF(f.data ->> 'ps', '')::NUMERIC(30, 6) AS ps,
                NULLIF(f.data ->> 'grossmargin', '')::NUMERIC(30, 6) AS grossmargin,
                NULLIF(f.data ->> 'opmargin', '')::NUMERIC(30, 6) AS operating_margin,
                NULLIF(f.data ->> 'netmargin', '')::NUMERIC(30, 6) AS netmargin,
                NULLIF(f.data ->> 'roa', '')::NUMERIC(30, 6) AS roa,
                NULLIF(f.data ->> 'roe', '')::NUMERIC(30, 6) AS roe,
                NULLIF(f.data ->> 'roic', '')::NUMERIC(30, 6) AS roic,
                NULLIF(f.data ->> 'currentratio', '')::NUMERIC(30, 6) AS currentratio,
                NULLIF(f.data ->> 'eps', '')::NUMERIC(30, 6) AS eps,
                NULLIF(f.data ->> 'epsdil', '')::NUMERIC(30, 6) AS epsdil,
                NULLIF(f.data ->> 'bvps', '')::NUMERIC(30, 6) AS bvps,
                NULLIF(f.data ->> 'shareswa', '')::NUMERIC(30, 6) AS shareswa,
                NULLIF(f.data ->> 'shareswadil', '')::NUMERIC(30, 6) AS shareswadil,
                NULLIF(f.data ->> 'ncfo', '')::NUMERIC(30, 6) AS ncfo,
                NULLIF(f.data ->> 'capex', '')::NUMERIC(30, 6) AS capex,
                f.data,
                f.source_table,
                f.created_at,
                f.updated_at
            FROM {schema}.fundamentals_sf1 f
            '''
        )

    conn.commit()


def main() -> int:
    parser = argparse.ArgumentParser(description='Add Sharadar SF1 fundamentals table and view.')
    parser.add_argument('--db-dsn', default=os.getenv('DATABASE_URL') or os.getenv('DB_DSN'))
    parser.add_argument('--schema', default=os.getenv('DB_SCHEMA', DEFAULT_SCHEMA))
    args = parser.parse_args()

    if not args.db_dsn:
        raise SystemExit('Missing --db-dsn (or set DATABASE_URL / DB_DSN in .env).')

    with psycopg.connect(args.db_dsn) as conn:
        ensure_schema(conn, args.schema)

    print(f'[info] ensured {args.schema}.fundamentals_sf1 and {args.schema}.fundamentals_sf1_common')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
