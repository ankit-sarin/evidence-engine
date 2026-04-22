"""Migration 008: add fabrication_verifications table.

Stores Pass 2 (per-arm fabrication verification) output for Paper 1.
One row per (judge_run_id, paper_id, field_name, arm_name).

Schema notes:
  - judge_run_id → judge_runs(run_id) ON DELETE CASCADE so dropping
    a run removes its verdicts in lockstep with its ratings.
  - verdict is an enum: SUPPORTED | PARTIALLY_SUPPORTED | UNSUPPORTED.
  - CHECK: UNSUPPORTED verdicts must carry BOTH reasoning and
    fabrication_hypothesis (non-null, non-empty). A SUPPORTED verdict
    may leave both NULL; a PARTIALLY_SUPPORTED verdict is left
    unconstrained at the DB layer (Pydantic enforces reasoning).
  - pre_check_short_circuit is 0/1 (SQLite has no bool).

Idempotent (IF NOT EXISTS). Executes in a single transaction.
"""

from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger(__name__)

_DDL = """
BEGIN;

CREATE TABLE IF NOT EXISTS fabrication_verifications (
    verification_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    judge_run_id               TEXT    NOT NULL
                               REFERENCES judge_runs(run_id) ON DELETE CASCADE,
    paper_id                   TEXT    NOT NULL,
    field_name                 TEXT    NOT NULL,
    arm_name                   TEXT    NOT NULL,
    pre_check_short_circuit    INTEGER NOT NULL
                               CHECK (pre_check_short_circuit IN (0, 1)),
    verdict                    TEXT    NOT NULL
                               CHECK (verdict IN (
                                   'SUPPORTED',
                                   'PARTIALLY_SUPPORTED',
                                   'UNSUPPORTED'
                               )),
    verification_span          TEXT,
    reasoning                  TEXT,
    fabrication_hypothesis     TEXT,
    verified_at                TEXT    NOT NULL,
    CHECK (
        verdict != 'UNSUPPORTED'
        OR (
            reasoning IS NOT NULL
            AND TRIM(reasoning) != ''
            AND fabrication_hypothesis IS NOT NULL
            AND TRIM(fabrication_hypothesis) != ''
        )
    ),
    UNIQUE (judge_run_id, paper_id, field_name, arm_name)
);

CREATE INDEX IF NOT EXISTS idx_fab_verif_run
    ON fabrication_verifications (judge_run_id);

CREATE INDEX IF NOT EXISTS idx_fab_verif_run_paper_field
    ON fabrication_verifications (judge_run_id, paper_id, field_name);

CREATE INDEX IF NOT EXISTS idx_fab_verif_verdict
    ON fabrication_verifications (verdict);

COMMIT;
"""

_EXPECTED_TABLES = ("fabrication_verifications",)
_EXPECTED_INDEXES = (
    "idx_fab_verif_run",
    "idx_fab_verif_run_paper_field",
    "idx_fab_verif_verdict",
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
    """Create fabrication_verifications table + indexes.

    Idempotent — second run is a no-op.
    """
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
                f"Migration 008 failed: tables missing after DDL: {missing_tables}"
            )
        missing_indexes = [i for i in _EXPECTED_INDEXES if i not in post_indexes]
        if missing_indexes:
            raise RuntimeError(
                f"Migration 008 failed: indexes missing after DDL: {missing_indexes}"
            )

        summary = {
            "tables_created": len(post_tables - pre_tables),
            "indexes_created": len(post_indexes - pre_indexes),
        }
        logger.info(
            "Migration 008 complete: %d tables created, %d indexes created",
            summary["tables_created"], summary["indexes_created"],
        )
        return summary
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m engine.migrations.008_add_fabrication_verifications <db_path>")
        sys.exit(1)
    logging.basicConfig(level=logging.INFO)
    result = run_migration(sys.argv[1])
    print(result)
