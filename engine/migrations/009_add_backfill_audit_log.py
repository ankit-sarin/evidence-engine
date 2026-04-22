"""Migration 009: add judge_run_audit table.

Generic audit log for post-hoc corrections or annotations on judge_runs
rows. First user: the post-hoc digest backfill for Pass 2 run
`surgical_autonomy_pass2_full_20260421T174729Z`, whose `judge_model_digest`
was initially stored as the model-name string `gemma3:27b` because the
canonical fetcher fell back silently when Ollama's /api/show returned no
`digest` field. A subsequent code patch moved judge-run digest capture to
`/api/tags models[].digest` with strict validation; this audit table
records the first (and any future) retroactive correction.

Schema notes:
  - ``event_type`` is an open-vocabulary string so future audit shapes
    don't need migrations (e.g., 'backfill_judge_model_digest',
    'manual_digest_correction', 'run_annotation').
  - ``before_value``/``after_value`` are TEXT rather than structured
    columns because different event types will log different value
    shapes. Queryability sacrificed, flexibility gained — defensible
    for a low-volume audit table.
  - ``rationale`` is NOT NULL to enforce that every audit row carries
    its justification.
  - ON DELETE CASCADE mirrors ``fabrication_verifications``: if a
    ``judge_run`` is ever purged, its audit trail goes with it.

Spec corrections applied (recorded here because the spec prose carried
two typos that would not have compiled as written):
  - ``judge_run_id`` is ``TEXT NOT NULL`` (not ``INTEGER NOT NULL``)
    because ``judge_runs.run_id`` is TEXT.
  - FK target is ``judge_runs(run_id)`` (not ``judge_runs(judge_run_id)``)
    — that column does not exist. Matches migration 008's FK.

Idempotent (IF NOT EXISTS). Single-transaction DDL.
``rollback()`` drops the indexes and the table for test-harness and
disaster-recovery use; it is NOT wired into the auto-migration path.
"""

from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger(__name__)


_DDL = """
BEGIN;

CREATE TABLE IF NOT EXISTS judge_run_audit (
    audit_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    judge_run_id    TEXT    NOT NULL
                    REFERENCES judge_runs(run_id) ON DELETE CASCADE,
    event_type      TEXT    NOT NULL,
    target_table    TEXT    NOT NULL,
    target_column   TEXT,
    before_value    TEXT,
    after_value     TEXT,
    rationale       TEXT    NOT NULL
                    CHECK (TRIM(rationale) != ''),
    performed_at    TEXT    NOT NULL,
    performed_by    TEXT    NOT NULL DEFAULT 'system'
);

CREATE INDEX IF NOT EXISTS idx_judge_run_audit_run
    ON judge_run_audit(judge_run_id);

CREATE INDEX IF NOT EXISTS idx_judge_run_audit_event
    ON judge_run_audit(event_type);

COMMIT;
"""


_ROLLBACK_DDL = """
BEGIN;
DROP INDEX IF EXISTS idx_judge_run_audit_event;
DROP INDEX IF EXISTS idx_judge_run_audit_run;
DROP TABLE IF EXISTS judge_run_audit;
COMMIT;
"""


_EXPECTED_TABLES = ("judge_run_audit",)
_EXPECTED_INDEXES = (
    "idx_judge_run_audit_run",
    "idx_judge_run_audit_event",
)


def _existing_tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    return {r[0] for r in rows}


def _existing_indexes(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'"
    ).fetchall()
    return {r[0] for r in rows}


def run_migration(db_path: str | None = None) -> dict:
    """Create ``judge_run_audit`` table + indexes. Idempotent."""
    if db_path is None:
        raise ValueError("db_path is required")

    conn = sqlite3.connect(str(db_path))
    try:
        pre_tables = _existing_tables(conn)
        pre_indexes = _existing_indexes(conn)

        conn.executescript(_DDL)

        post_tables = _existing_tables(conn)
        post_indexes = _existing_indexes(conn)

        missing_tables = [t for t in _EXPECTED_TABLES if t not in post_tables]
        if missing_tables:
            raise RuntimeError(
                f"Migration 009 failed: tables missing after DDL: {missing_tables}"
            )
        missing_indexes = [i for i in _EXPECTED_INDEXES if i not in post_indexes]
        if missing_indexes:
            raise RuntimeError(
                f"Migration 009 failed: indexes missing after DDL: {missing_indexes}"
            )

        summary = {
            "tables_created": len(post_tables - pre_tables),
            "indexes_created": len(post_indexes - pre_indexes),
        }
        logger.info(
            "Migration 009 complete: %d tables created, %d indexes created",
            summary["tables_created"], summary["indexes_created"],
        )
        return summary
    finally:
        conn.close()


def rollback(db_path: str | None = None) -> dict:
    """Drop the audit indexes and table. Idempotent. Not auto-run."""
    if db_path is None:
        raise ValueError("db_path is required")

    conn = sqlite3.connect(str(db_path))
    try:
        pre_tables = _existing_tables(conn)
        pre_indexes = _existing_indexes(conn)
        conn.executescript(_ROLLBACK_DDL)
        post_tables = _existing_tables(conn)
        post_indexes = _existing_indexes(conn)
        summary = {
            "tables_dropped": len(pre_tables - post_tables),
            "indexes_dropped": len(pre_indexes - post_indexes),
        }
        logger.info(
            "Migration 009 rollback complete: %d tables dropped, %d indexes dropped",
            summary["tables_dropped"], summary["indexes_dropped"],
        )
        return summary
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print(
            "Usage: python -m engine.migrations.009_add_backfill_audit_log "
            "<db_path> [rollback]"
        )
        sys.exit(1)
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) >= 3 and sys.argv[2] == "rollback":
        result = rollback(sys.argv[1])
    else:
        result = run_migration(sys.argv[1])
    print(result)
