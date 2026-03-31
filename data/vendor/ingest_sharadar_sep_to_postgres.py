#!/usr/bin/env python3
"""Backward-compatible entry point for SHARADAR/SEP.

See ingest_sharadar_prices_to_postgres.py for the implementation.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ingest_sharadar_prices_to_postgres import main  # noqa: E402

if __name__ == "__main__":
    # Force --table SHARADAR/SEP when invoked via this legacy entry point
    if "--table" not in sys.argv:
        sys.argv.extend(["--table", "SHARADAR/SEP"])
    raise SystemExit(main())
