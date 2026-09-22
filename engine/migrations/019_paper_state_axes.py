"""Migration 019: the paper-state vocabulary splits into two axes (R29, R39).

016 gave `paper_events.to_state` a CHECK over five tokens and gave
`PAPER_EVENT_TYPES` fourteen event types, with no relation between them. READERS-01
Phase 1 measured the consequence: `extraction_failed`, `parsed` and `extracted`
are event types with **no `to_state` token they can write**, and `to_state` is
`NOT NULL` — so recording an extraction failure was impossible without lying
about the paper's state. A9 ("eligibility conflated with processing success") was
therefore not latent in the corpus predicate; it was unwritable in the store.

This migration rebuilds `paper_events` with:

* **two disjoint closed token sets** — eligibility (`eligible`, `abstract_out`,
  `full_text_out`) and processing (`parsed`, `extracted`, `extraction_failed`,
  `full_text_not_obtainable`, `parse_failed`, `input_exceeds_context`,
  `audited_ai`);
* **an event_type -> axis CHECK**, so a `screened` event cannot carry a
  processing token and an `extracted` event cannot carry an eligibility one;
* **a reason CHECK** — `reason_code` is NOT NULL for exactly the four failure
  tokens and NULL for every other, so S3h's "failure states carry a reason" is
  an invariant of the record rather than a convention in a writer.

**No `axis` column is stored.** The sets are disjoint, so the axis is a function
of `to_state`; a column carrying a value derivable from its own row is the second
source of truth session 5 deleted `field_state` to avoid.

**Why a rebuild and not an ALTER.** SQLite cannot alter a CHECK. The table must
be recreated, which means its two append-only triggers are dropped with it and
restored afterwards **inside the same transaction** — so a failure anywhere
leaves the guard exactly as it was rather than leaving the table unguarded.

**Transaction discipline (the 017 pattern).** The runner does not wrap a
migration in a transaction; it closes its connection and calls
`run_migration(db_path)`, so each module owns its own. This one opens `BEGIN`,
does the whole rebuild, restores the triggers, writes its marker **last**, and
rolls back on any exception.

**Self-contained (R35).** Every token list and every DDL string below is declared
here and imported from nowhere: editing `engine/core/paper_state.py` must never
change what an already-applied migration meant.
`tests/test_paper_state_vocabulary.py` asserts the two agree.

**No history is reconstructed (R25).** Existing rows are copied verbatim,
`event_id` preserved. A row whose token became a failure token but carries no
`reason_code` would fail the new CHECK; rather than invent a reason, this
migration **refuses and names the rows**. Measured 2026-09-22: zero such rows on
the live database (all 190 are `to_state='eligible'`, `reason_code` NULL) and
zero on a fresh one.
"""

from __future__ import annotations

import sqlite3

# ── R35: re-declared here, never imported ────────────────────────────
ELIGIBILITY_STATES = ("eligible", "abstract_out", "full_text_out")

PROCESSING_STATES = (
    "parsed", "extracted", "extraction_failed", "full_text_not_obtainable",
    "parse_failed", "input_exceeds_context", "audited_ai",
)

FAILURE_STATES = (
    "extraction_failed", "full_text_not_obtainable", "parse_failed",
    "input_exceeds_context",
)

COMPLETED_PROCESSING_STATES = ("extracted", "audited_ai")

PAPER_EVENT_TYPES = (
    "identified", "duplicate_of", "screened", "verified", "adjudicated",
    "acquired", "not_obtainable", "parsed", "extracted", "extraction_failed",
    "audited", "manual_advance", "bypass", "state_at_migration",
)

EVENT_TYPE_AXIS = {
    "identified": "eligibility", "duplicate_of": "eligibility",
    "screened": "eligibility", "verified": "eligibility",
    "adjudicated": "eligibility",
    "acquired": "processing", "not_obtainable": "processing",
    "parsed": "processing", "extracted": "processing",
    "extraction_failed": "processing", "audited": "processing",
    "manual_advance": "both", "bypass": "both", "state_at_migration": "both",
}

#: A token that exists only in the post-019 vocabulary. Its presence in the
#: table's own CHECK is how this migration recognises its own work.
POST_019_SENTINEL_TOKEN = "parse_failed"

#: Column order is the shape `db_fingerprint` hashes a row in, so it is preserved
#: exactly: the spine, then the four paper columns. A rebuild that reorders
#: columns changes every per-table content hash without changing a single value.
_COLUMNS = (
    "event_id", "event_uid", "event_type", "occurred_at", "recorded_at",
    "actor_kind", "actor_role", "actor_name", "actor_digest", "run_id",
    "run_marker", "prior_event_id", "presented_context_sha256", "reason",
    "payload_json", "paper_id", "to_state", "from_state", "reason_code",
    "stage_name",
)


def _in(values) -> str:
    return ", ".join(f"'{v}'" for v in values)


def _elig_types() -> tuple[str, ...]:
    return tuple(t for t, a in EVENT_TYPE_AXIS.items() if a == "eligibility")


def _proc_types() -> tuple[str, ...]:
    return tuple(t for t, a in EVENT_TYPE_AXIS.items() if a == "processing")


def _both_types() -> tuple[str, ...]:
    return tuple(t for t, a in EVENT_TYPE_AXIS.items() if a == "both")


def table_sql(name: str = "paper_events") -> str:
    """The rebuilt table. `name` lets the rebuild build it under a temp name."""
    all_states = ELIGIBILITY_STATES + PROCESSING_STATES
    return f"""
    CREATE TABLE {name} (
        event_id    INTEGER PRIMARY KEY AUTOINCREMENT,
        event_uid   TEXT    NOT NULL UNIQUE,
        event_type  TEXT    NOT NULL,
        occurred_at TEXT    NOT NULL,
        recorded_at TEXT    NOT NULL,
        actor_kind  TEXT    NOT NULL CHECK (actor_kind IN ('model', 'human', 'engine')),
        actor_role  TEXT    NOT NULL CHECK (actor_role IN ('reviewer', 'extractor', 'system')),
        actor_name  TEXT    NOT NULL,
        actor_digest TEXT,
        run_id      INTEGER,
        run_marker  TEXT,
        prior_event_id INTEGER REFERENCES {name}(event_id),
        presented_context_sha256 TEXT,
        reason      TEXT,
        payload_json TEXT   NOT NULL DEFAULT '{{}}',
        paper_id    INTEGER NOT NULL REFERENCES papers(id),
        to_state    TEXT    NOT NULL CHECK (to_state IN ({_in(all_states)})),
        from_state  TEXT,
        reason_code TEXT,
        stage_name  TEXT,
        CHECK (actor_role <> 'system' OR actor_kind = 'engine'),
        CHECK (event_type IN ({_in(PAPER_EVENT_TYPES)})),
        -- R39: an event's type and its to_state must name the same axis.
        CHECK (
            (event_type IN ({_in(_elig_types())}) AND to_state IN ({_in(ELIGIBILITY_STATES)}))
         OR (event_type IN ({_in(_proc_types())}) AND to_state IN ({_in(PROCESSING_STATES)}))
         OR (event_type IN ({_in(_both_types())}))
        ),
        -- R39 / S3h: a reason for exactly the failure tokens, and for no other.
        CHECK (
            (to_state IN ({_in(FAILURE_STATES)}) AND reason_code IS NOT NULL)
         OR (to_state NOT IN ({_in(FAILURE_STATES)}) AND reason_code IS NULL)
        )
    )
    """


def index_sql(name: str = "paper_events") -> str:
    return f"CREATE INDEX IF NOT EXISTS idx_paper_events_paper ON {name}(paper_id, event_id)"


def trigger_statements(name: str = "paper_events") -> tuple[str, ...]:
    """The two append-only triggers, as SEPARATE statements.

    They are returned one per string, and every statement in this module is run
    with `Connection.execute`, never `executescript`: **`executescript` issues an
    implicit COMMIT before it runs**, which silently ends the module-owned
    transaction and would leave a failed rebuild half-applied with the guard off.
    The forced-exception rehearsal (I11) is what found that, on this migration.
    """
    msg = f"{name} is append-only: correct by appending an event"
    return (
        f"CREATE TRIGGER IF NOT EXISTS {name}_no_update BEFORE UPDATE ON {name} "
        f"BEGIN SELECT RAISE(ABORT, '{msg}'); END",
        f"CREATE TRIGGER IF NOT EXISTS {name}_no_delete BEFORE DELETE ON {name} "
        f"BEGIN SELECT RAISE(ABORT, '{msg}'); END",
    )


def _already_applied(conn: sqlite3.Connection) -> bool:
    """Idempotent by POSTCONDITION, not by a marker row.

    017 recorded itself in `review_identities` because 017 is a *data* migration
    whose work is rows; that table is the review's identity store — which spec,
    which codebook — and `tests/test_migration_016_017.py` pins it empty on a
    fresh database, correctly. A schema migration's marker belongs in
    `schema_migrations`, which the runner already writes, or in the schema
    itself. This reads the schema: a `to_state` CHECK naming a token that exists
    only after 019 is 019's work, and it cannot disagree with the table the way a
    separate marker row can (verify the postcondition, never the receipt).
    """
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='paper_events'"
    ).fetchone()
    return bool(row) and f"'{POST_019_SENTINEL_TOKEN}'" in (row[0] or "")


def _refuse_unreasoned_failures(conn: sqlite3.Connection) -> None:
    """A failure token with no reason cannot be copied, and is not invented."""
    placeholders = ", ".join("?" * len(FAILURE_STATES))
    rows = conn.execute(
        f"SELECT event_id, paper_id, to_state FROM paper_events "
        f"WHERE to_state IN ({placeholders}) AND reason_code IS NULL",
        FAILURE_STATES,
    ).fetchall()
    if rows:
        raise RuntimeError(
            "019 refuses: these paper_events carry a failure token with no "
            "reason_code, and R39 makes the reason mandatory for them — "
            + "; ".join(f"event_id={r[0]} paper_id={r[1]} to_state={r[2]}" for r in rows[:20])
            + (f" (and {len(rows) - 20} more)" if len(rows) > 20 else "")
            + ". A reason is not reconstructable from the record (R25), so this "
            "migration will not invent one. Append a correcting event naming the "
            "reason, or rule on a default, before running 019."
        )


def run_migration(db_path: str | None = None, *, _fail_after_copy: bool = False) -> dict:
    """Rebuild `paper_events` with the two-axis vocabulary. Idempotent by marker.

    `_fail_after_copy` exists for the forced-exception rehearsal (I11): it raises
    after the rows are copied and the old table dropped, which is the moment at
    which a non-transactional rebuild would have destroyed the table and its
    guard. It is not reachable from the runner.
    """
    if db_path is None:
        raise ValueError("019 requires an explicit db_path")

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        if _already_applied(conn):
            return {"status": "already_applied", "rows_preserved": 0}

        _refuse_unreasoned_failures(conn)

        before = conn.execute("SELECT COUNT(*) FROM paper_events").fetchone()[0]
        seq_before = conn.execute(
            "SELECT seq FROM sqlite_sequence WHERE name = 'paper_events'"
        ).fetchone()
        seq_before = seq_before[0] if seq_before else 0

        conn.execute("BEGIN")

        # The guard comes off and goes back on inside this transaction. A
        # rollback restores both, because DDL is transactional in SQLite.
        conn.execute("DROP TRIGGER IF EXISTS paper_events_no_update")
        conn.execute("DROP TRIGGER IF EXISTS paper_events_no_delete")

        conn.execute(table_sql("paper_events_new_019"))
        cols = ", ".join(_COLUMNS)
        conn.execute(
            f"INSERT INTO paper_events_new_019 ({cols}) "
            f"SELECT {cols} FROM paper_events ORDER BY event_id"
        )

        if _fail_after_copy:
            raise RuntimeError("forced mid-rebuild failure (rehearsal)")

        conn.execute("DROP TABLE paper_events")
        conn.execute("ALTER TABLE paper_events_new_019 RENAME TO paper_events")
        conn.execute(index_sql())
        for stmt in trigger_statements():
            conn.execute(stmt)

        # AUTOINCREMENT: the rebuild resets the sequence to max(event_id), which
        # would let a later insert REUSE an id the store has already issued if
        # the sequence had run ahead of the rows. Restore the high-water mark.
        if seq_before:
            # `sqlite_sequence` carries no PRIMARY KEY or UNIQUE, so there is no
            # upsert to use: update the row the INSERT..SELECT created, and only
            # insert one if the rebuild left the table with no rows at all.
            updated = conn.execute(
                "UPDATE sqlite_sequence SET seq = ? "
                "WHERE name = 'paper_events' AND seq < ?",
                (seq_before, seq_before),
            ).rowcount
            if not updated and not conn.execute(
                "SELECT 1 FROM sqlite_sequence WHERE name = 'paper_events'"
            ).fetchone():
                conn.execute(
                    "INSERT INTO sqlite_sequence (name, seq) VALUES ('paper_events', ?)",
                    (seq_before,),
                )

        after = conn.execute("SELECT COUNT(*) FROM paper_events").fetchone()[0]
        if after != before:
            raise RuntimeError(
                f"019 copied {after} rows but found {before} — refusing to commit"
            )

        # The RENAME is the last structural act, and the postcondition this
        # migration recognises itself by. Either the new table is in place with
        # its guard, or the rollback has left the old one untouched — there is no
        # third state and no separate marker that could disagree with either.
        conn.commit()
        return {"status": "executed", "rows_preserved": after}
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
