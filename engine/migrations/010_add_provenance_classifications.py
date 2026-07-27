"""Migration 010: add provenance census tables.

Stores per-span evidence-provenance classifications for Paper 1/1b under the
taxonomy pinned in analysis/provenance/DEFINITIONS.md.

Schema notes:
  - Two tables. provenance_census_runs is the run header (one row per census);
    provenance_classifications is one row per classified span.
  - census_run_id → provenance_census_runs(census_run_id) ON DELETE CASCADE, so
    dropping a census removes its rows in lockstep.
  - taxonomy_class is a closed enum: the five taxonomy classes plus the three
    non-taxonomy classes that let a census cover 100% of spans.
  - field_class is the PROPOSED extractive/interpretive split; '' is permitted
    so an unratified or unknown field never blocks a census write.
  - sentence_ratios is a JSON array of per-sentence best ratios. Storing the
    raw ratios (not just the verdict) is what makes the threshold sensitivity
    analysis re-derivable without re-running the matcher.
  - definitions_sha256 pins the DEFINITIONS.md content a run was produced under;
    a changed definitions file therefore yields a visibly different run.
  - Existing audit tables (evidence_spans, cloud_evidence_spans, judge_*,
    fabrication_verifications) are NOT touched by this migration.

Idempotent (IF NOT EXISTS). Executes in a single transaction.
"""

from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger(__name__)

_DDL = """
BEGIN;

CREATE TABLE IF NOT EXISTS provenance_census_runs (
    census_run_id          TEXT    PRIMARY KEY,
    review_name            TEXT    NOT NULL,
    created_at             TEXT    NOT NULL,
    definitions_sha256     TEXT    NOT NULL,
    normalization_version  TEXT    NOT NULL,
    tokenizer              TEXT    NOT NULL,
    tokenizer_version      TEXT    NOT NULL,
    threshold_primary      REAL    NOT NULL,
    threshold_band         TEXT    NOT NULL,
    min_sentence_tokens    INTEGER NOT NULL,
    ratio_ceiling          REAL    NOT NULL,
    spans_total            INTEGER NOT NULL,
    notes                  TEXT
);

CREATE TABLE IF NOT EXISTS provenance_classifications (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    census_run_id          TEXT    NOT NULL
                           REFERENCES provenance_census_runs(census_run_id)
                           ON DELETE CASCADE,
    arm                    TEXT    NOT NULL,
    paper_id               INTEGER NOT NULL,
    field_name             TEXT    NOT NULL,
    span_table             TEXT    NOT NULL
                           CHECK (span_table IN ('evidence_spans',
                                                 'cloud_evidence_spans')),
    span_row_id            INTEGER NOT NULL,
    value                  TEXT,
    snippet_chars          INTEGER NOT NULL,
    taxonomy_class         TEXT    NOT NULL
                           CHECK (taxonomy_class IN (
                               'ANCHORED',
                               'STITCHED',
                               'DRIFTED',
                               'UNTRACEABLE_PARTIAL',
                               'UNTRACEABLE_NO_BASIS',
                               'ABSENCE_DECLARED',
                               'MISSING_SNIPPET',
                               'UNCLASSIFIABLE_SHORT'
                           )),
    field_class            TEXT    NOT NULL DEFAULT ''
                           CHECK (field_class IN ('extractive',
                                                  'interpretive', '')),
    n_sentences            INTEGER NOT NULL,
    n_evaluated            INTEGER NOT NULL,
    n_exact                INTEGER NOT NULL,
    sentence_ratios        TEXT    NOT NULL,
    min_ratio              REAL,
    strict_variant_class   TEXT,
    has_ellipsis           INTEGER NOT NULL CHECK (has_ellipsis IN (0, 1)),
    parser_tier            TEXT,
    classified_at          TEXT    NOT NULL,
    UNIQUE (census_run_id, arm, paper_id, field_name)
);

CREATE INDEX IF NOT EXISTS idx_prov_class_run
    ON provenance_classifications (census_run_id);

CREATE INDEX IF NOT EXISTS idx_prov_class_run_arm_class
    ON provenance_classifications (census_run_id, arm, taxonomy_class);

CREATE INDEX IF NOT EXISTS idx_prov_class_run_field
    ON provenance_classifications (census_run_id, field_name);

COMMIT;
"""

_EXPECTED_TABLES = ("provenance_census_runs", "provenance_classifications")
_EXPECTED_INDEXES = (
    "idx_prov_class_run",
    "idx_prov_class_run_arm_class",
    "idx_prov_class_run_field",
)


def _existing_tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {r[0] for r in rows}


def _existing_indexes(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
    return {r[0] for r in rows}


def run_migration(db_path: str | None = None) -> dict:
    """Create the provenance census tables + indexes. Idempotent."""
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
                f"Migration 010 failed: tables missing after DDL: {missing_tables}"
            )
        missing_indexes = [i for i in _EXPECTED_INDEXES if i not in post_indexes]
        if missing_indexes:
            raise RuntimeError(
                f"Migration 010 failed: indexes missing after DDL: {missing_indexes}"
            )

        summary = {
            "tables_created": len(post_tables - pre_tables),
            "indexes_created": len(post_indexes - pre_indexes),
        }
        logger.info(
            "Migration 010 complete: %d tables created, %d indexes created",
            summary["tables_created"], summary["indexes_created"],
        )
        return summary
    finally:
        conn.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python engine/migrations/010_add_provenance_classifications.py <db_path>")
        sys.exit(1)
    logging.basicConfig(level=logging.INFO)
    print(run_migration(sys.argv[1]))
