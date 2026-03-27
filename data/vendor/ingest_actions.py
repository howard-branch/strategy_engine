import csv
import io
import requests
import psycopg
from typing import Iterator
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
NDL_API_KEY = os.getenv("NDL_API_KEY")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

if not NDL_API_KEY:
    raise RuntimeError("NDL_API_KEY not set")

def normalise_header(x):
    return x.strip().lower() if x else x


def row_get(row, *keys):
    for k in keys:
        if k in row and row[k]:
            return row[k]
    return None


def download_zip_bytes(url: str):
    r = requests.get(url)
    r.raise_for_status()
    return r.content


def first_csv_from_zip_bytes(data: bytes):
    import zipfile
    zf = zipfile.ZipFile(io.BytesIO(data))
    name = zf.namelist()[0]
    return io.TextIOWrapper(zf.open(name), encoding="utf-8")


# -------------------------
# SCHEMA
# -------------------------

def ensure_actions_schema(conn, schema):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.corporate_actions (
                instrument_id BIGINT NOT NULL REFERENCES {schema}.instruments(instrument_id),
                action_date DATE NOT NULL,
                action TEXT NOT NULL,
                value NUMERIC,
                contra_ticker TEXT NOT NULL DEFAULT '',
                contra_name TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (instrument_id, action_date, action, contra_ticker)
            )
            """
        )
    conn.commit()


# -------------------------
# PARSE
# -------------------------

def iter_actions(csv_file: io.TextIOBase) -> Iterator[tuple]:
    reader = csv.DictReader(csv_file)
    reader.fieldnames = [normalise_header(x) for x in reader.fieldnames]

    for row in reader:
        symbol = row_get(row, "ticker", "symbol")
        date = row_get(row, "date")
        action = row_get(row, "action")

        if not symbol or not date or not action:
            continue

        yield (
            symbol,
            date,
            action,
            row_get(row, "value"),
            row_get(row, "contraticker") or "",
            row_get(row, "contraname"),
        )


# -------------------------
# LOAD
# -------------------------

def ingest_actions(conn, api_key, schema="strategy_engine"):
    print("[info] downloading SHARADAR/ACTIONS")
    url = f"https://data.nasdaq.com/api/v3/datatables/SHARADAR/ACTIONS.csv?api_key={api_key}"
    data = download_zip_bytes(url)
    csv_file = first_csv_from_zip_bytes(data)

    ensure_actions_schema(conn, schema)

    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg_actions")
        cur.execute(
            """
            CREATE TEMP TABLE stg_actions (
                symbol TEXT,
                action_date DATE,
                action TEXT,
                value NUMERIC,
                contra_ticker TEXT,
                contra_name TEXT
            )
            """
        )

        rows = list(iter_actions(csv_file))

        cur.executemany(
            "INSERT INTO stg_actions VALUES (%s,%s,%s,%s,%s,%s)",
            rows,
        )

        cur.execute(
            f"""
            INSERT INTO {schema}.corporate_actions (
                instrument_id,
                action_date,
                action,
                value,
                contra_ticker,
                contra_name
            )
            SELECT
                i.instrument_id,
                s.action_date,
                s.action,
                s.value,
                COALESCE(s.contra_ticker, ''),
                s.contra_name
            FROM stg_actions s
            JOIN {schema}.instruments i
              ON i.symbol = s.symbol
            ON CONFLICT (instrument_id, action_date, action, contra_ticker)
            DO UPDATE SET
                value = EXCLUDED.value,
                contra_name = EXCLUDED.contra_name,
                updated_at = NOW()
            """
        )

    conn.commit()
    print("[info] corporate actions loaded")

def main():
    import psycopg

    conn = psycopg.connect(DATABASE_URL)

    ingest_actions(
        conn=conn,
        api_key=NDL_API_KEY,
        schema="strategy_engine",
    )

    conn.close()


if __name__ == "__main__":
    main()