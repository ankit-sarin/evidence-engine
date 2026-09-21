"""Migration 014: bring the cloud extraction tables into the migration set.

MIGRATIONS-01. `cloud_extractions` and `cloud_evidence_spans` were the only
objects in the live schema that mapped to **no migration**: they are created by
`engine.cloud.schema.init_cloud_tables`, which is called from the cloud
extractor, the cloud package's `__init__` and `scripts/run_cloud_extraction.py`
— never from `ReviewDatabase`. A fresh database therefore had twenty tables
where the live one has twenty-four, and a second review could not reach the same
schema without running a cloud extraction first.

This migration does not re-declare the DDL. It calls `init_cloud_tables`, so the
tables have exactly one definition and this file cannot drift from it. That
function is already idempotent — `CREATE TABLE IF NOT EXISTS` plus guarded
`ALTER TABLE`s for databases that predate later columns — so running it twice
changes nothing.

The cloud call sites keep their own `init_cloud_tables()` calls: they are no-ops
once the tables exist, and removing them would make a cloud run depend on the
migration having been run, which is a coupling this migration does not need.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}


def run_migration(db_path: str | None = None) -> dict:
    """Create the cloud extraction tables. Idempotent."""
    if db_path is None:
        raise ValueError("014 requires an explicit db_path")

    from engine.cloud.schema import init_cloud_tables

    conn = sqlite3.connect(str(db_path))
    before = _tables(conn)
    conn.close()

    init_cloud_tables(str(db_path))

    conn = sqlite3.connect(str(db_path))
    try:
        after = _tables(conn)
        missing = {"cloud_extractions", "cloud_evidence_spans"} - after
        if missing:
            raise RuntimeError(
                f"014 ran but {sorted(missing)} are still absent from {db_path}"
            )
        return {
            "created": sorted({"cloud_extractions", "cloud_evidence_spans"} - before),
            "already_present": sorted(
                {"cloud_extractions", "cloud_evidence_spans"} & before),
        }
    finally:
        conn.close()
