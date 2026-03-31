#!/usr/bin/env python3
"""
Download all Sharadar datasets into Postgres.

Runs every individual ingest script in dependency order (reference tables
first, then time-series data).  Exits with a non-zero status code if ANY
individual script fails.

Usage:
    python data/vendor/ingest_all_sharadar.py
    python data/vendor/ingest_all_sharadar.py --overwrite
    python data/vendor/ingest_all_sharadar.py --schema my_schema --batch-size 50000
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

# Directory containing all individual ingest scripts
_VENDOR_DIR = Path(__file__).resolve().parent

# Scripts in dependency order:
#   1. Reference / metadata tables (no date filter, always full reload)
#   2. Time-series data tables
_SCRIPTS: list[tuple[str, list[str]]] = [
    # ── reference tables ──────────────────────────────────────────
    ("SHARADAR/TICKERS",    ["ingest_sharadar_tickers_to_postgres.py"]),
    ("SHARADAR/INDICATORS", ["ingest_sharadar_indicators_to_postgres.py"]),
    # ── event / action tables ─────────────────────────────────────
    ("SHARADAR/ACTIONS",    ["ingest_sharadar_actions_to_postgres.py"]),
    ("SHARADAR/EVENTS",     ["ingest_sharadar_events_to_postgres.py"]),
    ("SHARADAR/SP500",      ["ingest_sharadar_sp500_to_postgres.py"]),
    # ── price tables ──────────────────────────────────────────────
    ("SHARADAR/DAILY",      ["ingest_sharadar_daily_to_postgres.py"]),
    ("SHARADAR/SEP",        ["ingest_sharadar_prices_to_postgres.py", "--table", "SHARADAR/SEP"]),
    ("SHARADAR/SFP",        ["ingest_sharadar_prices_to_postgres.py", "--table", "SHARADAR/SFP"]),
    # ── fundamental tables ────────────────────────────────────────
    ("SHARADAR/SF1",        ["ingest_sharadar_sf1_to_postgres.py"]),
    ("SHARADAR/SF2",        ["ingest_sharadar_sf2_to_postgres.py"]),
    ("SHARADAR/SF3",        ["ingest_sharadar_sf3_to_postgres.py"]),
]


def main() -> int:
    # Collect any extra CLI args (e.g. --overwrite, --schema, --batch-size)
    # and forward them to every child script.
    extra_args = sys.argv[1:]

    failed: list[tuple[str, int]] = []
    total = len(_SCRIPTS)

    print(f"{'=' * 60}")
    print(f"  Sharadar full ingest – {total} datasets")
    print(f"  Extra args: {extra_args or '(none)'}")
    print(f"{'=' * 60}\n")

    for idx, (label, cmd_parts) in enumerate(_SCRIPTS, 1):
        script_path = str(_VENDOR_DIR / cmd_parts[0])
        cmd = [sys.executable, script_path] + cmd_parts[1:] + extra_args

        print(f"\n{'─' * 60}")
        print(f"  [{idx}/{total}] {label}")
        print(f"  cmd: {' '.join(cmd)}")
        print(f"{'─' * 60}")

        t0 = time.monotonic()
        result = subprocess.run(cmd)
        elapsed = time.monotonic() - t0

        if result.returncode != 0:
            print(f"\n[FAIL] {label} exited with code {result.returncode} "
                  f"after {elapsed:.1f}s")
            failed.append((label, result.returncode))
            # Fail fast: stop on first error
            break
        else:
            print(f"\n[OK]   {label} completed in {elapsed:.1f}s")

    # ── Summary ───────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    if failed:
        print(f"  FAILED – {len(failed)} dataset(s) had errors:")
        for label, rc in failed:
            print(f"    • {label}  (exit code {rc})")
        print(f"{'=' * 60}")
        return 1

    print(f"  SUCCESS – all {total} datasets ingested")
    print(f"{'=' * 60}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

