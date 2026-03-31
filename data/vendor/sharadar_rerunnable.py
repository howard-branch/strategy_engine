"""Base utilities for all Sharadar ingest scripts."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import psycopg
from datetime import datetime


@dataclass
class IngestMetrics:
    """Metrics from a single ingest run."""
    table_name: str
    rows_staged: int
    rows_inserted: int
    rows_updated: int
    total_rows_after: int
    run_started_at: str
    run_completed_at: str

    def duration_seconds(self) -> float:
        start = datetime.fromisoformat(self.run_started_at)
        end = datetime.fromisoformat(self.run_completed_at)
        return (end - start).total_seconds()


def log_ingest_metrics(conn: psycopg.Connection, schema: str, metrics: IngestMetrics) -> None:
    """Log ingest metrics to a tracking table (optional, for auditing)."""
    with conn.cursor() as cur:
        cur.execute(
            f'''
            CREATE TABLE IF NOT EXISTS {schema}._ingest_logs (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                rows_staged INTEGER,
                rows_inserted INTEGER,
                rows_updated INTEGER,
                total_rows_after INTEGER,
                run_started_at TIMESTAMPTZ,
                run_completed_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            '''
        )
        cur.execute(
            f'''
            INSERT INTO {schema}._ingest_logs (
                table_name,
                rows_staged,
                rows_inserted,
                rows_updated,
                total_rows_after,
                run_started_at,
                run_completed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''',
            (
                metrics.table_name,
                metrics.rows_staged,
                metrics.rows_inserted,
                metrics.rows_updated,
                metrics.total_rows_after,
                metrics.run_started_at,
                metrics.run_completed_at,
            ),
        )
        conn.commit()