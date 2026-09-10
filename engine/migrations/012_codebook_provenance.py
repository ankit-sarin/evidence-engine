"""Migration 012: codebook provenance on extractions, cloud_extractions, review_runs.

CODEBOOK-AUTH-01 R2. Every extraction records `extraction_schema_hash`, which
is `spec.extraction_hash()` — a hash of the SPEC. The prompt the model actually
answered is built from the CODEBOOK. So the codebook could be edited — a
definition, an instruction, a valid value, a field_class — and every stored
extraction's provenance stayed byte-identical: staleness detection did not
fire, and `check_schema_parity` (which compares spec-derived hashes arm to arm)
stayed silent. A `codebook_sha256` already existed and would have caught it,
but it was wired only into `judge_runs`, never into the extraction lane.

Two columns per table, because they answer different questions:

  ``codebook_hash``    canonical sorted-key JSON of the projection that reaches
                       a prompt — fields[*] plus the three token/sentinel keys.
                       Moves when what the model saw moved, and not otherwise:
                       a comment, a reformat, or an edit to version/date/review
                       leaves it alone. This is the one to compare across runs.

  ``codebook_sha256``  the file's bytes, same function `judge_runs` uses. Moves
                       on any edit at all. This is the one to cite when saying
                       exactly which file was on disk.

Existing rows get NULL, which is correct and must be read as "nobody recorded
it" — never as "unchanged". Nothing is backfilled: the codebook's content at
the time of those extractions is not recoverable from the database, and
inventing a value would be worse than the gap.

Idempotent: ADD COLUMN is guarded by a PRAGMA check, so a re-run is a no-op.
``rollback()`` is not provided — SQLite cannot drop a column without a table
rebuild, and rebuilding `extractions` to remove a nullable column is a larger
risk than the column.
"""

from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger(__name__)

TARGETS = ("extractions", "cloud_extractions", "review_runs")
COLUMNS = ("codebook_hash", "codebook_sha256")


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }


def run_migration(db_path: str | None = None) -> dict:
    """Add the two codebook-provenance columns to the three tables. Idempotent.

    A target table that does not exist is skipped, not created: `review_runs`
    and `cloud_extractions` are created elsewhere, and a migration that
    invented an empty one would hide the fact that the lane was never set up.
    """
    if db_path is None:
        raise ValueError("db_path is required")

    conn = sqlite3.connect(str(db_path))
    added: list[str] = []
    skipped: list[str] = []
    absent: list[str] = []
    try:
        present = _tables(conn)
        for table in TARGETS:
            if table not in present:
                absent.append(table)
                continue
            existing = _columns(conn, table)
            for column in COLUMNS:
                if column in existing:
                    skipped.append(f"{table}.{column}")
                    continue
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} TEXT")
                added.append(f"{table}.{column}")
        conn.commit()
    finally:
        conn.close()

    if added:
        logger.info("Migration 012 added: %s", ", ".join(added))
    return {"added": added, "already_present": skipped, "table_absent": absent}


if __name__ == "__main__":  # pragma: no cover - hand-run diagnostics
    import sys

    logging.basicConfig(level=logging.INFO)
    print(run_migration(sys.argv[1]))
