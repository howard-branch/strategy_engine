#!/usr/bin/env python3
"""
Shared helpers for all Sharadar data-ingestion scripts.

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
"""
from __future__ import annotations

import time
from typing import Optional

import requests

_NASDAQ_TABLE_API = "https://data.nasdaq.com/api/v3/datatables/{table}.json"


def normalise_header(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


# ── HTTP helper ───────────────────────────────────────────────────────────────

def request_with_retries(
    session: requests.Session,
    url: str,
    *,
    max_attempts: int = 5,
    timeout: tuple[int, int] = (30, 600),
) -> requests.Response:
    """GET *url* with exponential back-off retry."""
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

