"""Operational metadata: run log, ingestion manifest and data-quality results,
all kept in a `meta` schema inside the DuckDB warehouse."""
from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

import duckdb

from .config import Settings

DDL = """
CREATE SCHEMA IF NOT EXISTS meta;
CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
    run_id      VARCHAR PRIMARY KEY,
    command     VARCHAR,
    started_at  TIMESTAMP,
    finished_at TIMESTAMP,
    status      VARCHAR,
    error       VARCHAR
);
CREATE TABLE IF NOT EXISTS meta.ingestion_log (
    source       VARCHAR,
    partition_key VARCHAR,
    path         VARCHAR,
    sha256       VARCHAR,
    bytes        BIGINT,
    ingested_at  TIMESTAMP,
    run_id       VARCHAR,
    PRIMARY KEY (source, partition_key)
);
CREATE TABLE IF NOT EXISTS meta.dq_results (
    run_id     VARCHAR,
    layer      VARCHAR,
    check_name VARCHAR,
    severity   VARCHAR,
    passed     BOOLEAN,
    observed   VARCHAR,
    expected   VARCHAR,
    checked_at TIMESTAMP
);
"""


def now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def connect(settings: Settings, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    settings.ensure_dirs()
    con = duckdb.connect(str(settings.warehouse), read_only=read_only)
    if not read_only:
        con.execute(DDL)
    return con


@contextmanager
def tracked_run(con: duckdb.DuckDBPyConnection, command: str):
    """Record start/finish/failure of a pipeline run so every load is auditable."""
    run_id = uuid.uuid4().hex[:12]
    con.execute(
        "INSERT INTO meta.pipeline_runs VALUES (?, ?, ?, NULL, 'running', NULL)",
        [run_id, command, now()],
    )
    try:
        yield run_id
    except Exception as exc:
        con.execute(
            "UPDATE meta.pipeline_runs SET finished_at=?, status='failed', error=? WHERE run_id=?",
            [now(), str(exc)[:500], run_id],
        )
        raise
    con.execute(
        "UPDATE meta.pipeline_runs SET finished_at=?, status='success' WHERE run_id=?",
        [now(), run_id],
    )


def already_ingested(con, source: str, partition_key: str) -> bool:
    return con.execute(
        "SELECT 1 FROM meta.ingestion_log WHERE source=? AND partition_key=?",
        [source, partition_key],
    ).fetchone() is not None


def log_ingestion(con, source, partition_key, path, sha256, nbytes, run_id) -> None:
    con.execute(
        "INSERT OR REPLACE INTO meta.ingestion_log VALUES (?, ?, ?, ?, ?, ?, ?)",
        [source, partition_key, str(path), sha256, nbytes, now(), run_id],
    )
