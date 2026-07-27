"""Migration 011: admit ABSENCE_CLAIM to the provenance taxonomy vocabulary.

Extends the census tables created by migration 010 for DEFINITIONS.md v1.1:

  1. provenance_classifications.absence_pattern  — new nullable TEXT column
     recording which pinned pattern (P1..P6) fired, so every ABSENCE_CLAIM
     verdict is traceable to the rule that produced it. Plain ALTER TABLE ADD
     COLUMN; strictly additive.
  2. provenance_census_runs.absence_pattern_version — new TEXT column, default
     '' for pre-v1.1 runs so existing rows keep an honest "no detector ran"
     value rather than being retro-labelled. Strictly additive.
  3. provenance_classifications.taxonomy_class CHECK — widened to admit
     'ABSENCE_CLAIM'.

Note on (3): SQLite cannot alter a CHECK constraint in place, so the table is
rebuilt via the standard create-copy-drop-rename procedure. **Every existing row
is preserved** — the migration counts rows before and after and aborts the
transaction if the counts differ, so it cannot silently lose census history. No
column is dropped and no value is rewritten; the only schema change is the wider
enum plus the added column. This is additive in data terms; it is a rebuild only
because SQLite offers no narrower instrument.

Idempotent: re-running detects the already-widened CHECK and the already-present
columns and returns without touching anything.
"""

from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger(__name__)

_NEW_TABLE_DDL = """
CREATE TABLE provenance_classifications_new (
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
                               'ABSENCE_CLAIM',
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
    absence_pattern        TEXT,
    classified_at          TEXT    NOT NULL,
    UNIQUE (census_run_id, arm, paper_id, field_name)
);
"""

_COPY_SQL = """
INSERT INTO provenance_classifications_new
    (id, census_run_id, arm, paper_id, field_name, span_table, span_row_id,
     value, snippet_chars, taxonomy_class, field_class, n_sentences,
     n_evaluated, n_exact, sentence_ratios, min_ratio, strict_variant_class,
     has_ellipsis, parser_tier, absence_pattern, classified_at)
SELECT
     id, census_run_id, arm, paper_id, field_name, span_table, span_row_id,
     value, snippet_chars, taxonomy_class, field_class, n_sentences,
     n_evaluated, n_exact, sentence_ratios, min_ratio, strict_variant_class,
     has_ellipsis, parser_tier, NULL, classified_at
FROM provenance_classifications;
"""

_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS idx_prov_class_run
    ON provenance_classifications (census_run_id);
CREATE INDEX IF NOT EXISTS idx_prov_class_run_arm_class
    ON provenance_classifications (census_run_id, arm, taxonomy_class);
CREATE INDEX IF NOT EXISTS idx_prov_class_run_field
    ON provenance_classifications (census_run_id, field_name);
CREATE INDEX IF NOT EXISTS idx_prov_class_absence_pattern
    ON provenance_classifications (census_run_id, absence_pattern);
"""


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}


def _check_admits_absence_claim(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' "
        "AND name='provenance_classifications'"
    ).fetchone()
    return bool(row) and "ABSENCE_CLAIM" in (row[0] or "")


def run_migration(db_path: str | None = None) -> dict:
    """Widen the taxonomy enum and add the absence-pattern columns. Idempotent."""
    if db_path is None:
        raise ValueError("db_path is required")

    conn = sqlite3.connect(str(db_path))
    try:
        if "provenance_classifications" not in {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }:
            raise RuntimeError("Migration 011 requires migration 010 to have run first")

        already = _check_admits_absence_claim(conn)
        if already and "absence_pattern" in _columns(conn, "provenance_classifications"):
            logger.info("Migration 011: already applied — no-op")
            return {"rebuilt": False, "columns_added": 0, "rows_preserved": 0}

        before = conn.execute(
            "SELECT COUNT(*) FROM provenance_classifications"
        ).fetchone()[0]

        columns_added = 0
        if "absence_pattern_version" not in _columns(conn, "provenance_census_runs"):
            conn.execute(
                "ALTER TABLE provenance_census_runs "
                "ADD COLUMN absence_pattern_version TEXT NOT NULL DEFAULT ''"
            )
            columns_added += 1

        # Rebuild only what SQLite forces us to rebuild.
        #
        # isolation_level=None puts sqlite3 in autocommit so that the explicit
        # BEGIN/COMMIT below is the only transaction in play. executescript() is
        # avoided inside the transaction: it issues an implicit COMMIT before
        # running, which would silently end the transaction and defeat the
        # row-count guard.
        conn.isolation_level = None
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("BEGIN")
        try:
            conn.execute(_NEW_TABLE_DDL)
            conn.execute(_COPY_SQL)
            conn.execute("DROP TABLE provenance_classifications")
            conn.execute(
                "ALTER TABLE provenance_classifications_new "
                "RENAME TO provenance_classifications"
            )
            for stmt in filter(None, (s.strip() for s in _INDEX_DDL.split(";"))):
                conn.execute(stmt)

            after = conn.execute(
                "SELECT COUNT(*) FROM provenance_classifications"
            ).fetchone()[0]
            if after != before:
                raise RuntimeError(
                    f"Migration 011 aborted: row count changed {before} -> {after}"
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        columns_added += 1  # absence_pattern

        if not _check_admits_absence_claim(conn):
            raise RuntimeError("Migration 011 failed: CHECK does not admit ABSENCE_CLAIM")

        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise RuntimeError(f"Migration 011 failed: FK violations after rebuild: {violations[:3]}")
        conn.execute("PRAGMA foreign_keys = ON")

        logger.info(
            "Migration 011 complete: %d columns added, %d rows preserved",
            columns_added, after,
        )
        return {"rebuilt": True, "columns_added": columns_added, "rows_preserved": after}
    finally:
        conn.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python engine/migrations/011_add_absence_claim_class.py <db_path>")
        sys.exit(1)
    logging.basicConfig(level=logging.INFO)
    print(run_migration(sys.argv[1]))
