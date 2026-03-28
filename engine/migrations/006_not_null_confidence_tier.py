"""Migration 006: Add tier column to evidence_spans with NOT NULL constraint.

The evidence_spans table has confidence REAL NOT NULL already, but is missing
the tier column entirely (tier was stored on the Pydantic EvidenceSpan model
but never persisted to the DB).

This migration:
  1. Adds tier INTEGER NOT NULL DEFAULT 1 to evidence_spans via the
     rename-create-copy-drop pattern (SQLite cannot ALTER COLUMN).
  2. cloud_evidence_spans already has NOT NULL on both columns via
     engine/cloud/schema.py — no changes needed there.

Idempotent: checks whether the tier column exists before running.
"""

import sqlite3
import logging

logger = logging.getLogger(__name__)


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """Check whether a column exists in a table."""
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    return column in cols


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def run_migration(db_path: str | None = None) -> dict:
    """Add tier column to evidence_spans with NOT NULL DEFAULT 1.

    Returns summary dict with counts.
    """
    if db_path is None:
        raise ValueError("db_path is required")

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = OFF")

    summary = {"evidence_spans_rebuilt": False, "rows_migrated": 0}

    if not _table_exists(conn, "evidence_spans"):
        logger.info("evidence_spans table does not exist — skipping migration")
        conn.close()
        return summary

    if _has_column(conn, "evidence_spans", "tier"):
        logger.info("evidence_spans already has tier column — skipping migration")
        conn.close()
        return summary

    # Count existing rows for reporting
    row_count = conn.execute("SELECT COUNT(*) FROM evidence_spans").fetchone()[0]
    logger.info("Rebuilding evidence_spans: adding tier column (%d rows)", row_count)

    conn.executescript("""
        ALTER TABLE evidence_spans RENAME TO _evidence_spans_old;

        CREATE TABLE evidence_spans (
            id              INTEGER PRIMARY KEY,
            extraction_id   INTEGER NOT NULL REFERENCES extractions(id),
            field_name      TEXT NOT NULL,
            value           TEXT NOT NULL,
            source_snippet  TEXT,
            confidence      REAL NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
            tier            INTEGER NOT NULL DEFAULT 1 CHECK (tier >= 1 AND tier <= 4),
            audit_status    TEXT NOT NULL DEFAULT 'pending'
                            CHECK (audit_status IN (
                                'pending', 'verified', 'contested',
                                'flagged', 'invalid_snippet'
                            )),
            auditor_model   TEXT,
            audit_rationale TEXT,
            audited_at      TEXT
        );

        INSERT INTO evidence_spans
            (id, extraction_id, field_name, value, source_snippet,
             confidence, tier, audit_status, auditor_model,
             audit_rationale, audited_at)
            SELECT id, extraction_id, field_name, value, source_snippet,
                   confidence, 1, audit_status, auditor_model,
                   audit_rationale, audited_at
            FROM _evidence_spans_old;

        DROP TABLE _evidence_spans_old;

        CREATE INDEX IF NOT EXISTS idx_spans_extraction
            ON evidence_spans(extraction_id);
    """)

    conn.execute("PRAGMA foreign_keys = ON")
    conn.commit()

    summary["evidence_spans_rebuilt"] = True
    summary["rows_migrated"] = row_count
    logger.info("Migration 006 complete: %d rows migrated", row_count)

    conn.close()
    return summary


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m engine.migrations.006_not_null_confidence_tier <db_path>")
        sys.exit(1)
    logging.basicConfig(level=logging.INFO)
    result = run_migration(sys.argv[1])
    print(result)
