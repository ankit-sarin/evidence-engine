"""Migration 018: the cloud tables reach the fresh shape; `audit_adjudication` goes.

Three structural changes, one transaction, no row content altered.

**C10** — `cloud_evidence_spans.confidence` and `.tier` are `NOT NULL` on a fresh
database and nullable on live, because `init_cloud_tables`'s rebuild branch never
ran there. 014 cannot close it: its receipt exists, so the runner skips it, and
editing 014 would make the runner refuse to start for *every* migration
(`MigrationDrift`). R27 rules that one new migration does it instead.

**R16** — `UNIQUE(paper_id, arm)` is dropped from `cloud_extractions`, so an arm
can hold more than one claim on a paper and supersession becomes representable.
The constraint is dropped **here and in `engine/cloud/schema.py::_CLOUD_SCHEMA`
in the same commit** (R27 as amended): live and fresh are structurally identical
on this table today, so dropping it on one side only would *create* the
divergence this migration exists to remove.

**R32 / A11** — `audit_adjudication` is dropped. Its `span_id` references the
phantom `_evidence_spans_old` left by the `evidence_spans` rebuild, which is why
it holds 0 rows and why `PRAGMA foreign_keys=ON` makes every INSERT fail. The
census run for R32 found **no production reader**, three INSERT sites all
unreachable behind an unconditional `raise AuditAdjudicationDeprecated`, and one
DDL creator — `ensure_adjudication_table` — which is why the same commit removes
`_AUDIT_ADJUDICATION_TABLE` from it. **Dropping the table alone would not have
worked**: `ensure_adjudication_table` runs on every `ReviewDatabase`
construction, so the table came straight back (measured: present 1 -> 0 -> 1).
R32 reverses the *sequencing* half of R18's A11 Option B; the route half —
human audit decisions become `field_events`, importer in session 12 — stands.

**Why rebuild-then-rename and never rename-then-drop.** A11 exists because
`ALTER TABLE evidence_spans RENAME TO _evidence_spans_old` rewrote the
*referencing* table's `REFERENCES` clause, and the subsequent `DROP` left it
pointing at nothing. This module never renames a table out of its own name: it
builds the replacement under a temporary name, copies, drops the original, then
renames the replacement in. Nothing ever references the temporary name, so
nothing can be rewritten to it.

**`execute`, never `executescript`.** `executescript` issues an implicit COMMIT
before it runs, which silently ends a module-owned transaction and would leave a
failed rebuild half-applied. Migration 019's forced-exception rehearsal found
that; it is corrected here by construction.

**Self-contained (R35).** Every DDL string is declared in this module and
imported from nowhere, so editing `engine/cloud/schema.py` later cannot change
what this migration meant when it ran.

**Idempotent by postcondition**, not by a marker row: the migration reads the
schema back and recognises its own work, so a marker can never disagree with the
table it describes.
"""

from __future__ import annotations

import sqlite3

#: Column order is preserved exactly. `db_fingerprint` hashes a row as
#: `SELECT <colnames in PRAGMA order> ... ORDER BY rowid`, so a rebuild that
#: reorders columns moves every per-table content hash without changing a value.
CLOUD_EXTRACTIONS_COLUMNS = (
    "id", "paper_id", "arm", "model_string", "extracted_data", "reasoning_trace",
    "prompt_text", "input_tokens", "output_tokens", "reasoning_tokens",
    "cost_usd", "extraction_schema_hash", "extracted_at", "codebook_hash",
    "codebook_sha256",
)

CLOUD_EVIDENCE_SPANS_COLUMNS = (
    "id", "cloud_extraction_id", "field_name", "value", "source_snippet",
    "confidence", "tier", "notes",
)


def cloud_extractions_sql(name: str = "cloud_extractions") -> str:
    """R16: no `UNIQUE(paper_id, arm)`. Everything else as it stands."""
    return f"""
    CREATE TABLE {name} (
        id                      INTEGER PRIMARY KEY,
        paper_id                INTEGER NOT NULL REFERENCES papers(id),
        arm                     TEXT NOT NULL,
        model_string            TEXT NOT NULL,
        extracted_data          TEXT,
        reasoning_trace         TEXT,
        prompt_text             TEXT,
        input_tokens            INTEGER,
        output_tokens           INTEGER,
        reasoning_tokens        INTEGER,
        cost_usd                REAL,
        extraction_schema_hash  TEXT,
        extracted_at            TEXT NOT NULL,
        codebook_hash           TEXT,
        codebook_sha256         TEXT
    )
    """


def cloud_evidence_spans_sql(name: str = "cloud_evidence_spans") -> str:
    """C10: `confidence` and `tier` NOT NULL, as on a fresh database."""
    return f"""
    CREATE TABLE {name} (
        id                      INTEGER PRIMARY KEY,
        cloud_extraction_id     INTEGER NOT NULL REFERENCES cloud_extractions(id),
        field_name              TEXT NOT NULL,
        value                   TEXT,
        source_snippet          TEXT,
        confidence              REAL NOT NULL,
        tier                    INTEGER NOT NULL,
        notes                   TEXT,
        UNIQUE(cloud_extraction_id, field_name)
    )
    """


def _table_ddl(conn: sqlite3.Connection, name: str) -> str | None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row[0] if row else None


def _notnull(conn: sqlite3.Connection, table: str, column: str) -> bool:
    for r in conn.execute(f"PRAGMA table_info({table})"):
        if r[1] == column:
            return bool(r[3])
    raise RuntimeError(f"{table}.{column} does not exist")


def _has_unique_paper_arm(conn: sqlite3.Connection) -> bool:
    for idx in conn.execute("PRAGMA index_list(cloud_extractions)"):
        if not idx[2]:                        # not unique
            continue
        cols = [r[2] for r in conn.execute(f'PRAGMA index_info("{idx[1]}")')]
        if cols == ["paper_id", "arm"]:
            return True
    return False


def already_applied(conn: sqlite3.Connection) -> bool:
    """All three postconditions hold."""
    if _table_ddl(conn, "audit_adjudication") is not None:
        return False
    if _has_unique_paper_arm(conn):
        return False
    return (_notnull(conn, "cloud_evidence_spans", "confidence")
            and _notnull(conn, "cloud_evidence_spans", "tier"))


def _refuse_nulls(conn: sqlite3.Connection) -> None:
    """A NULL in a column about to become NOT NULL is not silently backfilled.

    `init_cloud_tables`'s rebuild branch would have set `confidence = 0.0` and
    `tier = 1` on such rows — inventing a confidence and a parser tier nobody
    measured. R27's contract is "no row content changed", so this migration
    refuses instead and names the rows. Measured 2026-09-22: zero on live, over
    7,257 rows.
    """
    bad = conn.execute(
        "SELECT COUNT(*) FROM cloud_evidence_spans "
        "WHERE confidence IS NULL OR tier IS NULL"
    ).fetchone()[0]
    if bad:
        raise RuntimeError(
            f"018 refuses: {bad} cloud_evidence_spans rows have a NULL "
            "confidence or tier, which cannot be carried into a NOT NULL column. "
            "A value for them is not derivable from the record, and this "
            "migration will not invent one (R27: no row content changed). Rule "
            "on a backfill first, as its own migration."
        )


def _rebuild(conn, table, new_sql_fn, columns) -> int:
    """Build the replacement under a temp name, copy, drop, rename in.

    Never renames `table` out of its own name — that is the A11 mechanism.
    """
    tmp = f"{table}__018"
    before = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    conn.execute(new_sql_fn(tmp))
    cols = ", ".join(f'"{c}"' for c in columns)
    conn.execute(f"INSERT INTO {tmp} ({cols}) SELECT {cols} FROM {table} ORDER BY rowid")
    conn.execute(f"DROP TABLE {table}")
    conn.execute(f"ALTER TABLE {tmp} RENAME TO {table}")
    after = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if after != before:
        raise RuntimeError(f"018 copied {after} rows of {table} but found {before}")
    return after


def run_migration(db_path: str | None = None, *, _fail_after_copy: bool = False) -> dict:
    """Apply the three structural changes. Idempotent by postcondition."""
    if db_path is None:
        raise ValueError("018 requires an explicit db_path")

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        if already_applied(conn):
            return {"status": "already_applied", "spans": 0, "extractions": 0,
                    "audit_adjudication_dropped": False}

        _refuse_nulls(conn)

        conn.execute("BEGIN")

        spans = _rebuild(conn, "cloud_evidence_spans", cloud_evidence_spans_sql,
                         CLOUD_EVIDENCE_SPANS_COLUMNS)
        extractions = _rebuild(conn, "cloud_extractions", cloud_extractions_sql,
                               CLOUD_EXTRACTIONS_COLUMNS)

        if _fail_after_copy:
            raise RuntimeError("forced mid-rebuild failure (rehearsal)")

        had_audit = _table_ddl(conn, "audit_adjudication") is not None
        # R32. IF EXISTS because a fresh database no longer creates it at all,
        # `_AUDIT_ADJUDICATION_TABLE` having been removed from
        # `ensure_adjudication_table` in the same commit.
        conn.execute("DROP TABLE IF EXISTS audit_adjudication")

        dangling = conn.execute("PRAGMA foreign_key_check").fetchall()
        if dangling:
            raise RuntimeError(
                "018 would leave dangling foreign keys and refuses to commit: "
                f"{dangling[:10]}"
            )

        conn.commit()
        return {"status": "executed", "spans": spans, "extractions": extractions,
                "audit_adjudication_dropped": had_audit}
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
