"""Migration 007: Add judge_runs, judge_ratings, judge_pair_ratings tables.

Persists LLM-as-judge Pass 1 output for Paper 1 concordance analysis.

Schema:
  - judge_runs             — one row per judge invocation batch
  - judge_ratings          — one row per (run, paper, field) triple
  - judge_pair_ratings     — one row per arm pair within a triple

Reserves pass_number = 2 for the Pass 2 fabrication verification
pass (no separate migration needed for that later).

paper_id is stored as TEXT to match the pattern established by
analysis/paper1/human_extractions (paper_id = "EE-NNN" strings).
No FK to papers(id) — Paper 1 IDs are ee_identifier strings, not
the integer primary key on papers.

Idempotent: uses CREATE TABLE / CREATE INDEX IF NOT EXISTS.
All DDL runs in a single transaction via executescript.
"""

from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger(__name__)

_DDL = """
BEGIN;

CREATE TABLE IF NOT EXISTS judge_runs (
    run_id               TEXT    PRIMARY KEY,
    judge_model_name     TEXT    NOT NULL,
    judge_model_digest   TEXT    NOT NULL,
    codebook_sha256      TEXT    NOT NULL,
    pass_number          INTEGER NOT NULL CHECK (pass_number IN (1, 2)),
    input_scope          TEXT    NOT NULL,
    started_at           TEXT    NOT NULL,
    completed_at         TEXT,
    n_triples_attempted  INTEGER NOT NULL DEFAULT 0,
    n_triples_succeeded  INTEGER NOT NULL DEFAULT 0,
    n_triples_failed     INTEGER NOT NULL DEFAULT 0,
    run_config_json      TEXT    NOT NULL,
    notes                TEXT
);

CREATE TABLE IF NOT EXISTS judge_ratings (
    rating_id                INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                   TEXT    NOT NULL REFERENCES judge_runs(run_id),
    paper_id                 TEXT    NOT NULL,
    field_name               TEXT    NOT NULL,
    field_type               TEXT    NOT NULL
                             CHECK (field_type IN ('categorical', 'numeric', 'free_text')),
    seed                     INTEGER NOT NULL,
    arm_permutation_json     TEXT    NOT NULL,
    prompt_hash              TEXT    NOT NULL,
    raw_response             TEXT    NOT NULL,
    pass1_fabrication_risk   TEXT    NOT NULL
                             CHECK (pass1_fabrication_risk IN ('low', 'medium', 'high')),
    pass1_proposed_consensus TEXT,
    pass1_overall_rationale  TEXT    NOT NULL,
    created_at               TEXT    NOT NULL,
    UNIQUE (run_id, paper_id, field_name)
);

CREATE TABLE IF NOT EXISTS judge_pair_ratings (
    pair_rating_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    rating_id        INTEGER NOT NULL
                     REFERENCES judge_ratings(rating_id) ON DELETE CASCADE,
    arm_a            TEXT    NOT NULL,
    arm_b            TEXT    NOT NULL,
    level1_rating    TEXT    NOT NULL
                     CHECK (level1_rating IN ('EQUIVALENT', 'PARTIAL', 'DIVERGENT')),
    level2_type      TEXT    CHECK (
                         level2_type IS NULL OR level2_type IN (
                             'GRANULARITY', 'SELECTION', 'OMISSION',
                             'CONTRADICTION', 'FABRICATION'
                         )
                     ),
    rationale        TEXT    NOT NULL,
    CHECK (
        (level1_rating = 'EQUIVALENT' AND level2_type IS NULL)
        OR
        (level1_rating != 'EQUIVALENT' AND level2_type IS NOT NULL)
    ),
    CHECK (arm_a < arm_b),
    UNIQUE (rating_id, arm_a, arm_b)
);

CREATE INDEX IF NOT EXISTS idx_judge_ratings_run_paper_field
    ON judge_ratings (run_id, paper_id, field_name);

CREATE INDEX IF NOT EXISTS idx_judge_ratings_run
    ON judge_ratings (run_id);

CREATE INDEX IF NOT EXISTS idx_judge_ratings_field
    ON judge_ratings (field_name);

CREATE INDEX IF NOT EXISTS idx_judge_pair_ratings_rating
    ON judge_pair_ratings (rating_id);

CREATE INDEX IF NOT EXISTS idx_judge_pair_ratings_aggregation
    ON judge_pair_ratings (arm_a, arm_b, level1_rating, level2_type);

COMMIT;
"""

_EXPECTED_TABLES = ("judge_runs", "judge_ratings", "judge_pair_ratings")
_EXPECTED_INDEXES = (
    "idx_judge_ratings_run_paper_field",
    "idx_judge_ratings_run",
    "idx_judge_ratings_field",
    "idx_judge_pair_ratings_rating",
    "idx_judge_pair_ratings_aggregation",
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
    """Create judge_runs, judge_ratings, judge_pair_ratings + indexes.

    Idempotent: all DDL uses IF NOT EXISTS, so a second run is a no-op
    and returns tables_created=0, indexes_created=0.

    Returns summary dict with creation counts.
    """
    if db_path is None:
        raise ValueError("db_path is required")

    conn = sqlite3.connect(str(db_path))
    try:
        pre_tables = _existing_tables(conn)
        pre_indexes = _existing_indexes(conn)

        # executescript runs statements in autocommit mode; the leading
        # BEGIN / trailing COMMIT in _DDL make the whole script atomic.
        conn.executescript(_DDL)

        post_tables = _existing_tables(conn)
        post_indexes = _existing_indexes(conn)

        missing_tables = [t for t in _EXPECTED_TABLES if t not in post_tables]
        if missing_tables:
            raise RuntimeError(
                f"Migration 007 failed: tables missing after DDL: {missing_tables}"
            )
        missing_indexes = [i for i in _EXPECTED_INDEXES if i not in post_indexes]
        if missing_indexes:
            raise RuntimeError(
                f"Migration 007 failed: indexes missing after DDL: {missing_indexes}"
            )

        summary = {
            "tables_created": len(post_tables - pre_tables),
            "indexes_created": len(post_indexes - pre_indexes),
        }
        logger.info(
            "Migration 007 complete: %d tables created, %d indexes created",
            summary["tables_created"], summary["indexes_created"],
        )
        return summary
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m engine.migrations.007_add_judge_tables <db_path>")
        sys.exit(1)
    logging.basicConfig(level=logging.INFO)
    result = run_migration(sys.argv[1])
    print(result)
