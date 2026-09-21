"""Migration 015: drop the three pre-rename adjudication indices.

MIGRATIONS-01. Migration 002 renamed `idx_adjudication_*` to
`idx_abstract_adjudication_*` on `abstract_screening_adjudication`. But
`engine.adjudication.schema.ensure_adjudication_table` still carried
`CREATE INDEX IF NOT EXISTS idx_adjudication_*`, and `ReviewDatabase.__init__`
calls it on **every** construction — so the pre-rename names came straight back,
and the live database has carried **six** indices on three columns ever since.

Duplicated indices are not merely waste: every write to the table maintains both
copies, and a reader looking for "the index on paper_id" finds two with
different names and no way to tell which one a query planner chose.

This is the runner's own bug, so the runner fixes it: `schema.py` now creates
only the post-002 names, and this migration removes the duplicates from a
database that already has them.

**Checked before writing this:** the only places naming `idx_adjudication_*` are
`engine/adjudication/schema.py` (the creator, now corrected) and migration 002
itself. No query, no `INDEXED BY`, nothing under `analysis/`. Dropping them
changes no query plan that anything selected on purpose.
"""

from __future__ import annotations

import sqlite3

_PRE_RENAME = (
    "idx_adjudication_paper",
    "idx_adjudication_ext_key",
    "idx_adjudication_decision",
)

#: The names 002 renamed them to. The migration refuses to drop a duplicate
#: unless its replacement is present: dropping the only index on a column
#: because a rename half-ran would be worse than leaving two.
_POST_RENAME = (
    "idx_abstract_adjudication_paper",
    "idx_abstract_adjudication_ext_key",
    "idx_abstract_adjudication_decision",
)


def _indices(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'")}


def run_migration(db_path: str | None = None) -> dict:
    """Drop the three pre-rename duplicates. Idempotent."""
    if db_path is None:
        raise ValueError("015 requires an explicit db_path")

    conn = sqlite3.connect(str(db_path))
    try:
        present = _indices(conn)
        dropped, kept = [], []

        for old, new in zip(_PRE_RENAME, _POST_RENAME):
            if old not in present:
                continue
            if new not in present:
                kept.append(old)
                continue
            conn.execute(f'DROP INDEX "{old}"')
            dropped.append(old)

        conn.commit()

        if kept:
            raise RuntimeError(
                f"refusing to drop {kept}: the post-rename replacement is absent, "
                "so these are the only indices on their columns. Run migration "
                "002 first, or investigate."
            )
        return {"dropped": dropped, "already_absent": sorted(set(_PRE_RENAME) - present)}
    finally:
        conn.close()
