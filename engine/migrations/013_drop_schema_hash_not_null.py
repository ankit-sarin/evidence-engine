"""Migration 013: extraction_schema_hash / extraction_hash become nullable.

SCHEMA-DERIVE-01. `extraction_schema_hash` was `spec.extraction_hash()` — a
hash of a spec section that no longer exists. Staleness, cleanup and parity now
read `codebook_hash`, which is what the prompt was actually built from, and
nothing writes the old column any more. The columns STAY: they are the
historical record for the 190 local and 379 cloud extractions produced while
they were the authority, and dropping them would destroy the only evidence of
which spec version those runs used. What goes is the NOT NULL, which would make
every future insert carry a value nothing computes.

SQLite cannot alter a constraint in place, so the constrained tables are
rebuilt. The new schema is derived from `sqlite_master` — the CREATE TABLE
text as it stands, with the NOT NULL removed from the one column — rather than
written out here, so every column added by 004, 005 and 012 survives a rebuild
that does not know about them. Indexes are captured and recreated the same way.

**Only tables that are actually constrained are rebuilt.** Measured on the live
database: `extractions.extraction_schema_hash` and `review_runs.extraction_hash`
are NOT NULL; `cloud_extractions.extraction_schema_hash` is already nullable and
is therefore left alone. Rebuilding a table to remove a constraint it does not
have is risk with no benefit.

Idempotent: a table whose column is already nullable is skipped, so a re-run is
a no-op. No rollback is provided — the reverse would re-impose a constraint on
rows that legitimately have no value.
"""

from __future__ import annotations

import logging
import re
import sqlite3

logger = logging.getLogger(__name__)

#: (table, column) pairs whose NOT NULL this migration lifts.
TARGETS = (
    ("extractions", "extraction_schema_hash"),
    ("cloud_extractions", "extraction_schema_hash"),
    ("review_runs", "extraction_hash"),
)


def _table_sql(conn: sqlite3.Connection, table: str) -> str | None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row[0] if row else None


def _indexes(conn: sqlite3.Connection, table: str) -> list[str]:
    return [
        r[0] for r in conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? "
            "AND sql IS NOT NULL", (table,)
        ).fetchall()
    ]


def _is_not_null(conn: sqlite3.Connection, table: str, column: str) -> bool:
    for r in conn.execute(f"PRAGMA table_info({table})").fetchall():
        if r[1] == column:
            return bool(r[3])
    return False


def _columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _relax(sql: str, table: str, column: str) -> str:
    """Remove NOT NULL from one column of a CREATE TABLE statement."""
    pattern = re.compile(
        r"(\b" + re.escape(column) + r"\b\s+[A-Za-z]+)(\s+NOT\s+NULL)",
        re.IGNORECASE,
    )
    relaxed, n = pattern.subn(r"\1", sql, count=1)
    if n != 1:
        raise RuntimeError(
            f"could not relax NOT NULL on {table}.{column}; the CREATE TABLE "
            f"text did not match the expected shape:\n{sql}"
        )
    return relaxed.replace(f"CREATE TABLE IF NOT EXISTS {table}",
                           f"CREATE TABLE {table}__013", 1).replace(
        f"CREATE TABLE {table}", f"CREATE TABLE {table}__013", 1)


def run_migration(db_path: str | None = None) -> dict:
    """Lift NOT NULL where it is present. Idempotent."""
    if db_path is None:
        raise ValueError("db_path is required")

    conn = sqlite3.connect(str(db_path))
    rebuilt: list[str] = []
    already: list[str] = []
    absent: list[str] = []
    try:
        present = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        for table, column in TARGETS:
            if table not in present:
                absent.append(table)
                continue
            if column not in _columns(conn, table):
                absent.append(f"{table}.{column}")
                continue
            if not _is_not_null(conn, table, column):
                already.append(f"{table}.{column}")
                continue

            sql = _table_sql(conn, table)
            new_sql = _relax(sql, table, column)
            cols = ", ".join(f'"{c}"' for c in _columns(conn, table))
            index_sql = _indexes(conn, table)

            conn.execute("PRAGMA foreign_keys=OFF")
            conn.execute("BEGIN")
            try:
                conn.execute(new_sql)
                conn.execute(
                    f"INSERT INTO {table}__013 ({cols}) SELECT {cols} FROM {table}")
                conn.execute(f"DROP TABLE {table}")
                conn.execute(f"ALTER TABLE {table}__013 RENAME TO {table}")
                for stmt in index_sql:
                    conn.execute(stmt)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.execute("PRAGMA foreign_keys=ON")
            rebuilt.append(f"{table}.{column}")
    finally:
        conn.close()

    if rebuilt:
        logger.info("Migration 013 lifted NOT NULL on: %s", ", ".join(rebuilt))
    return {"rebuilt": rebuilt, "already_nullable": already, "absent": absent}


if __name__ == "__main__":  # pragma: no cover - hand-run diagnostics
    import sys

    logging.basicConfig(level=logging.INFO)
    print(run_migration(sys.argv[1]))
