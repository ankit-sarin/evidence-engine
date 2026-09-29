"""Migration 022: session-10 rulings R212-R218 (10a-R1, R222a).

One module, one transaction, owning every schema change the session-10
migration set names:

* **`paper_events` rebuilt** (R213): `identified` and `duplicate_of` leave both
  the `event_type` CHECK and the eligibility arm of the R39 axis CHECK. No
  other CHECK on this table changes — the actor CHECKs, the reason CHECK and
  the R68 run-link CHECK are re-declared verbatim (R202/R208).
* **`run_manifests` rebuilt** (R214, R215): `run_kind` CHECK gains `'import'`
  (RUN_KINDS gains the same string in `engine/core/run_manifest.py`, same
  commit — the constant and the CHECK never disagree); `end_status` CHECK
  gains `'aborted'`; a new nullable `end_reason TEXT` column, with
  `end_status = 'completed' => end_reason IS NULL` and
  `end_status = 'aborted' => end_reason IS NOT NULL`. The one-shot
  `run_manifests_end_once` trigger is re-declared from the SAME immutable
  column list (`_MANIFEST_BODY`, unchanged) that generates it in 020 — adding
  `end_reason` to the table without adding it to that list is what lets it
  change alongside `end_status`/`ended_at` on the one permitted close, and
  still refuses a second one via the existing `OLD.ended_at IS NOT NULL` guard.
* **`run_stage_configs` untouched** (10a-C2 Phase A, B1(c)): a table rebuilt
  under a temporary name and then renamed INTO the vacated original name never
  disturbs another table's `REFERENCES` text or live FK enforcement — only
  renaming the referenced table AWAY does that (A11's cause). Measured before
  drafting this module: `run_stage_configs`' schema text is byte-identical
  before and after a `run_manifests` rebuild of this shape, and
  `PRAGMA foreign_key_check` is clean. No file needed touching.
* **`run_calls` rebuilt** (R216): the existing 8 columns, unchanged in order,
  plus `outcome TEXT NOT NULL CHECK (...)` over the 6 tokens the
  input-fit guard and the completion path can produce, and
  `outcome_detail TEXT` (nullable, unconstrained). No default on `outcome` —
  every future INSERT names it. `response_digest` keeps no outcome-linked
  CHECK; the cloud path's `NULL` on every completed call (10a-P0/P1 M4) is
  F15's to change, not this migration's.
* **`claim_inputs`** (R217, new): one row per extraction call, keyed by
  `extraction_uid` — the natural key, because `extraction_uid` is minted once
  per call and repeats across every field of that call in `field_events`
  (10a-P0/P1 M6), never per field-event row.
* **`audit_verdicts`** (R218, new): the eleven `engine.core.audit_telemetry`
  FIELDS as columns, `verdict` CHECKed to `{'verified', 'flagged'}` — the
  closed set `AuditVerdict.status: Literal["verified", "flagged"]` in
  `engine/agents/auditor.py` permits (10a-C2 Phase A A1) and the ONLY set the
  live event-side auditor (`engine/agents/audit_events.py`) can ever write;
  the four-state `{'verified','contested','flagged','invalid_snippet'}`
  vocabulary belongs to `audit_span`, retired at the 9b cut-over (R111) and not
  on this table's write path.

**No behaviour changes beyond what the new DDL forces.** This commit does not
wire refusal rows, aborted closes, the `claim_inputs` writer, the
`audit_verdicts` writer or the jsonl retirement — those are C3-C5. The two
writer edits this commit DOES make: `record_call`'s INSERT names `outcome`
('completed' on its only path today) and `close_run` gains a `reason`
parameter nothing calls yet, both in `engine/core/run_manifest.py`.

**Self-contained (R35).** Every DDL string, token list and marker literal
below is declared here and imported from nowhere — including a full copy of
020's `_MANIFEST_BODY` and its trigger-generation shape, because a later edit
to 020 (which cannot happen; it is checksummed) must never change what this
migration meant, and vice versa.

**Transaction discipline (the 019/020 pattern).** One `BEGIN`, every statement
through `Connection.execute` (never `executescript`), every rebuilt table
built under a temporary name and the ORIGINAL never renamed away (only
dropped, then a temp table renamed into the vacated name — 10a-C2's own
measurement, not just 020's docstring claim), `ROLLBACK` on any exception.

**Idempotent by postcondition (R44).** `_already_applied` reads the schema for
both new tables; a schema migration writes no marker row.

**No history is reconstructed (R25).** `paper_events` rows are copied
verbatim, `event_id` preserved; a row whose `event_type` is `identified` or
`duplicate_of` cannot be copied under the new CHECK and is not silently
dropped — this migration refuses and names the rows (019's pattern). Measured
at 10a-P0/P1 (Q3): zero such rows on the live database. `run_manifests` and
`run_calls` are copied the same way; any pre-existing `run_calls` row (none on
live) is assigned `outcome = 'completed'`, the only outcome any writer could
have produced before this migration existed.
"""

from __future__ import annotations

import sqlite3

# ── R35: re-declared here, never imported ────────────────────────────

# -- paper_events (R213: 'identified' and 'duplicate_of' removed) -----
ELIGIBILITY_STATES = ("eligible", "abstract_out", "full_text_out")
PROCESSING_STATES = (
    "parsed", "extracted", "extraction_failed", "full_text_not_obtainable",
    "parse_failed", "input_exceeds_context", "audited_ai",
)
FAILURE_STATES = (
    "extraction_failed", "full_text_not_obtainable", "parse_failed",
    "input_exceeds_context",
)
PAPER_EVENT_TYPES = (
    "screened", "verified", "adjudicated",
    "acquired", "not_obtainable", "parsed", "extracted", "extraction_failed",
    "audited", "manual_advance", "bypass", "state_at_migration",
)
EVENT_TYPE_AXIS = {
    "screened": "eligibility", "verified": "eligibility",
    "adjudicated": "eligibility",
    "acquired": "processing", "not_obtainable": "processing",
    "parsed": "processing", "extracted": "processing",
    "extraction_failed": "processing", "audited": "processing",
    "manual_advance": "both", "bypass": "both", "state_at_migration": "both",
}
#: The two tokens 022 removes — refused if copying would require them (R25).
REMOVED_PAPER_EVENT_TYPES = ("identified", "duplicate_of")

PRE_MANIFEST_MARKER = "pre-manifest"

RUN_LINK_CHECK = (
    "CHECK (\n"
    "            (run_id IS NOT NULL AND run_marker IS NULL)\n"
    "            OR\n"
    f"            (run_id IS NULL AND run_marker IS '{PRE_MANIFEST_MARKER}')\n"
    "        )"
)

#: Column order is what `db_fingerprint` hashes a row in (the 019 lesson).
_SPINE_COLUMNS = (
    "event_id", "event_uid", "event_type", "occurred_at", "recorded_at",
    "actor_kind", "actor_role", "actor_name", "actor_digest", "run_id",
    "run_marker", "prior_event_id", "presented_context_sha256", "reason",
    "payload_json",
)
PAPER_COLUMNS = _SPINE_COLUMNS + (
    "paper_id", "to_state", "from_state", "reason_code", "stage_name",
)

# -- run_manifests / run_stage_configs / run_calls (R214-R216) --------
RUN_KINDS = ("extraction", "screening", "judge", "review_session", "import")
END_STATUSES = ("completed", "failed", "interrupted", "aborted")
RUN_CALL_OUTCOMES = (
    "completed", "refused_input_overflow", "refused_ceiling_unavailable",
    "refused_input_truncated", "refused_input_dropped", "error",
)

#: The immutable body of a manifest — UNCHANGED from 020. `end_status`,
#: `ended_at` and (new) `end_reason` are deliberately absent: that absence is
#: what lets all three change together on the one permitted close.
_MANIFEST_BODY = (
    "run_uid", "review_id", "run_kind", "git_commit", "git_dirty", "git_tag",
    "engine_state", "spec_hash", "codebook_hash", "codebook_sha256",
    "library_versions_json", "host", "started_at", "cloud_arms_json",
    "payload_description", "manifest_json", "manifest_sha256",
)
RUN_MANIFESTS_OLD_COLUMNS = (
    "run_id", "run_uid", "review_id", "run_kind", "git_commit", "git_dirty",
    "git_tag", "engine_state", "spec_hash", "codebook_hash", "codebook_sha256",
    "library_versions_json", "host", "started_at", "ended_at", "end_status",
    "cloud_arms_json", "payload_description", "manifest_json", "manifest_sha256",
)
RUN_MANIFESTS_NEW_COLUMNS = (
    "run_id", "run_uid", "review_id", "run_kind", "git_commit", "git_dirty",
    "git_tag", "engine_state", "spec_hash", "codebook_hash", "codebook_sha256",
    "library_versions_json", "host", "started_at", "ended_at", "end_status",
    "end_reason", "cloud_arms_json", "payload_description", "manifest_json",
    "manifest_sha256",
)
RUN_CALLS_OLD_COLUMNS = (
    "call_id", "run_id", "stage", "paper_id", "request_hash",
    "response_digest", "started_at", "ended_at",
)
RUN_CALLS_NEW_COLUMNS = RUN_CALLS_OLD_COLUMNS + ("outcome", "outcome_detail")

# -- audit_verdicts (R218) ---------------------------------------------
#: 10a-C2 Phase A A1: the closed set `AuditVerdict.status` permits
#: (`engine/agents/auditor.py`) — the only set the live auditor can write.
AUDIT_VERDICTS = ("verified", "flagged")

#: `engine.core.audit_telemetry.FIELDS`, re-declared (R35).
AUDIT_TELEMETRY_FIELDS = (
    "schema", "run_id", "paper_id", "claim_id", "field_name", "arm",
    "auditor_model", "auditor_digest", "verdict", "rationale", "occurred_at",
)


def _in(values) -> str:
    return ", ".join(f"'{v}'" for v in values)


def _axis(kind: str) -> tuple[str, ...]:
    return tuple(t for t, a in EVENT_TYPE_AXIS.items() if a == kind)


# ── DDL: paper_events (rebuild) ────────────────────────────────────────
def _spine(table: str) -> str:
    return f"""
    event_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    event_uid   TEXT    NOT NULL UNIQUE,
    event_type  TEXT    NOT NULL,
    occurred_at TEXT    NOT NULL,
    recorded_at TEXT    NOT NULL,
    actor_kind  TEXT    NOT NULL CHECK (actor_kind IN ('model', 'human', 'engine')),
    actor_role  TEXT    NOT NULL CHECK (actor_role IN ('reviewer', 'extractor', 'system')),
    actor_name  TEXT    NOT NULL,
    actor_digest TEXT,
    run_id      INTEGER REFERENCES run_manifests(run_id),
    run_marker  TEXT,
    prior_event_id INTEGER REFERENCES {table}(event_id),
    presented_context_sha256 TEXT,
    reason      TEXT,
    payload_json TEXT   NOT NULL DEFAULT '{{}}'"""


def paper_events_sql(name: str = "paper_events") -> str:
    all_states = ELIGIBILITY_STATES + PROCESSING_STATES
    return f"""
    CREATE TABLE {name} ({_spine(name)},
        paper_id    INTEGER NOT NULL REFERENCES papers(id),
        to_state    TEXT    NOT NULL CHECK (to_state IN ({_in(all_states)})),
        from_state  TEXT,
        reason_code TEXT,
        stage_name  TEXT,
        CHECK (actor_role <> 'system' OR actor_kind = 'engine'),
        CHECK (event_type IN ({_in(PAPER_EVENT_TYPES)})),
        -- R39: an event's type and its to_state must name the same axis.
        CHECK (
            (event_type IN ({_in(_axis('eligibility'))}) AND to_state IN ({_in(ELIGIBILITY_STATES)}))
         OR (event_type IN ({_in(_axis('processing'))}) AND to_state IN ({_in(PROCESSING_STATES)}))
         OR (event_type IN ({_in(_axis('both'))}))
        ),
        -- R39 / S3h: a reason for exactly the failure tokens, and for no other.
        CHECK (
            (to_state IN ({_in(FAILURE_STATES)}) AND reason_code IS NOT NULL)
         OR (to_state NOT IN ({_in(FAILURE_STATES)}) AND reason_code IS NULL)
        ),
        -- R68: a row with no run is a seeded, pre-manifest row and nothing else.
        {RUN_LINK_CHECK}
    )
    """


PAPER_EVENTS_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_paper_events_paper ON paper_events(paper_id, event_id)",
)


def _append_only_triggers(table: str) -> tuple[str, ...]:
    msg = f"{table} is append-only: correct by appending an event"
    return (
        f"CREATE TRIGGER IF NOT EXISTS {table}_no_update BEFORE UPDATE ON {table} "
        f"BEGIN SELECT RAISE(ABORT, '{msg}'); END",
        f"CREATE TRIGGER IF NOT EXISTS {table}_no_delete BEFORE DELETE ON {table} "
        f"BEGIN SELECT RAISE(ABORT, '{msg}'); END",
    )


def _record_only_triggers(table: str, what: str) -> tuple[str, ...]:
    msg = f"{table} is a record: {what}"
    return (
        f"CREATE TRIGGER IF NOT EXISTS {table}_no_update BEFORE UPDATE ON {table} "
        f"BEGIN SELECT RAISE(ABORT, '{msg}'); END",
        f"CREATE TRIGGER IF NOT EXISTS {table}_no_delete BEFORE DELETE ON {table} "
        f"BEGIN SELECT RAISE(ABORT, '{msg}'); END",
    )


# ── DDL: run_manifests (rebuild) ───────────────────────────────────────
def run_manifests_sql(name: str = "run_manifests") -> str:
    return f"""
    CREATE TABLE {name} (
        run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
        run_uid         TEXT    NOT NULL UNIQUE,
        review_id       TEXT    NOT NULL,
        run_kind        TEXT    NOT NULL CHECK (run_kind IN ({_in(RUN_KINDS)})),
        git_commit      TEXT    NOT NULL CHECK (length(git_commit) = 40),
        git_dirty       INTEGER NOT NULL CHECK (git_dirty = 0),
        git_tag         TEXT,
        engine_state    TEXT,
        spec_hash       TEXT    NOT NULL,
        codebook_hash   TEXT    NOT NULL,
        codebook_sha256 TEXT    NOT NULL,
        library_versions_json TEXT NOT NULL,
        host            TEXT    NOT NULL,
        started_at      TEXT    NOT NULL,
        ended_at        TEXT,
        end_status      TEXT    CHECK (end_status IS NULL OR end_status IN ({_in(END_STATUSES)})),
        end_reason      TEXT,
        cloud_arms_json TEXT    NOT NULL DEFAULT '[]',
        payload_description TEXT,
        manifest_json   TEXT    NOT NULL,
        manifest_sha256 TEXT    NOT NULL,
        CHECK ((ended_at IS NULL) = (end_status IS NULL)),
        CHECK (cloud_arms_json = '[]' OR payload_description IS NOT NULL),
        -- R215/C26: a completed run carries no reason; an aborted one must.
        CHECK (end_status IS NOT 'completed' OR end_reason IS NULL),
        CHECK (end_status IS NOT 'aborted' OR end_reason IS NOT NULL)
    )
    """


def manifest_triggers(name: str = "run_manifests") -> tuple[str, ...]:
    """The body is immutable; the run's end (status, timestamp, reason) is
    written once. Re-declared verbatim from 020's shape over the SAME
    `_MANIFEST_BODY` — `end_reason` is not in that tuple, exactly like
    `end_status`/`ended_at` are not, which is what permits it to change
    alongside them on the one permitted close."""
    changed = " OR ".join(f"NEW.{c} IS NOT OLD.{c}" for c in _MANIFEST_BODY)
    return (
        f"CREATE TRIGGER IF NOT EXISTS {name}_end_once BEFORE UPDATE ON {name} "
        f"WHEN OLD.ended_at IS NOT NULL OR NEW.run_id IS NOT OLD.run_id OR {changed} "
        f"BEGIN SELECT RAISE(ABORT, '{name}: a manifest is written before the "
        "first call and never edited; only its end is recorded, once'); END",
        f"CREATE TRIGGER IF NOT EXISTS {name}_no_delete BEFORE DELETE ON {name} "
        f"BEGIN SELECT RAISE(ABORT, '{name}: a run record is never deleted'); END",
    )


# ── DDL: run_calls (rebuild) ───────────────────────────────────────────
def run_calls_sql(name: str = "run_calls") -> str:
    return f"""
    CREATE TABLE {name} (
        call_id         INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id          INTEGER NOT NULL REFERENCES run_manifests(run_id),
        stage           TEXT    NOT NULL,
        paper_id        INTEGER REFERENCES papers(id),
        request_hash    TEXT    NOT NULL,
        response_digest TEXT,
        started_at      TEXT    NOT NULL,
        ended_at        TEXT    NOT NULL,
        outcome         TEXT    NOT NULL CHECK (outcome IN ({_in(RUN_CALL_OUTCOMES)})),
        outcome_detail  TEXT,
        FOREIGN KEY (run_id, stage) REFERENCES run_stage_configs(run_id, stage)
    )
    """


RUN_CALLS_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_run_calls_run ON run_calls(run_id, stage)",
)


# ── DDL: claim_inputs (new, R217) ──────────────────────────────────────
CLAIM_INPUTS_SQL = """
CREATE TABLE claim_inputs (
    extraction_uid     TEXT    PRIMARY KEY,
    arm                TEXT    NOT NULL REFERENCES arms(arm_name),
    paper_id           INTEGER NOT NULL REFERENCES papers(id),
    reuse_key          TEXT    NOT NULL,
    parsed_text_sha256 TEXT    NOT NULL,
    parsed_text_uid    TEXT    NOT NULL,
    run_id             INTEGER NOT NULL REFERENCES run_manifests(run_id),
    recorded_at        TEXT    NOT NULL
)
"""
CLAIM_INPUTS_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_claim_inputs_reuse "
    "ON claim_inputs(arm, paper_id, reuse_key)",
)


# ── DDL: audit_verdicts (new, R218) ────────────────────────────────────
def audit_verdicts_sql() -> str:
    return f"""
    CREATE TABLE audit_verdicts (
        verdict_id      INTEGER PRIMARY KEY AUTOINCREMENT,
        schema          TEXT    NOT NULL,
        run_id          INTEGER NOT NULL REFERENCES run_manifests(run_id),
        paper_id        INTEGER NOT NULL REFERENCES papers(id),
        claim_id        TEXT    NOT NULL,
        field_name      TEXT    NOT NULL,
        arm             TEXT    NOT NULL,
        auditor_model   TEXT    NOT NULL,
        auditor_digest  TEXT    NOT NULL,
        verdict         TEXT    NOT NULL CHECK (verdict IN ({_in(AUDIT_VERDICTS)})),
        rationale       TEXT,
        occurred_at     TEXT    NOT NULL
    )
    """


AUDIT_VERDICTS_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_audit_verdicts_run_paper "
    "ON audit_verdicts(run_id, paper_id)",
)


# ── Postcondition ────────────────────────────────────────────────────
def _already_applied(conn: sqlite3.Connection) -> bool:
    """022's work, read from the schema itself (R44): both new tables exist."""
    n = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
        "AND name IN ('claim_inputs', 'audit_verdicts')"
    ).fetchone()[0]
    return n == 2


def _columns(conn, table) -> tuple[str, ...]:
    return tuple(r[1] for r in conn.execute(f"PRAGMA table_info({table})"))


def _seq(conn, table) -> int:
    row = conn.execute("SELECT seq FROM sqlite_sequence WHERE name = ?", (table,)).fetchone()
    return row[0] if row else 0


def _restore_seq(conn, table, seq_before) -> None:
    if not seq_before:
        return
    updated = conn.execute(
        "UPDATE sqlite_sequence SET seq = ? WHERE name = ? AND seq < ?",
        (seq_before, table, seq_before)).rowcount
    if not updated and not conn.execute(
            "SELECT 1 FROM sqlite_sequence WHERE name = ?", (table,)).fetchone():
        conn.execute("INSERT INTO sqlite_sequence (name, seq) VALUES (?, ?)",
                     (table, seq_before))


def _refuse_removed_event_types(conn: sqlite3.Connection) -> None:
    """A row whose event_type is being retired cannot be copied under the new
    CHECK, and is not silently dropped (R25) — refuse and name it, 019's
    pattern."""
    placeholders = ", ".join("?" * len(REMOVED_PAPER_EVENT_TYPES))
    rows = conn.execute(
        f"SELECT event_id, paper_id, event_type FROM paper_events "
        f"WHERE event_type IN ({placeholders})",
        REMOVED_PAPER_EVENT_TYPES,
    ).fetchall()
    if rows:
        raise RuntimeError(
            "022 refuses: these paper_events carry an event_type R207/R213 "
            "retires — " + "; ".join(
                f"event_id={r[0]} paper_id={r[1]} event_type={r[2]}" for r in rows[:20])
            + (f" (and {len(rows) - 20} more)" if len(rows) > 20 else "")
            + ". A retired event_type is not reconstructable into a surviving "
            "one (R25); rule on a mapping before running 022 against a "
            "database that has any."
        )


def _rebuild_paper_events(conn: sqlite3.Connection) -> int:
    live_cols = _columns(conn, "paper_events")
    if live_cols != PAPER_COLUMNS:
        raise RuntimeError(
            f"022 refuses: paper_events columns are {live_cols}, expected "
            f"{PAPER_COLUMNS} (a rebuild copies by the pinned list and must "
            "not reorder or drop one)")
    before = conn.execute("SELECT COUNT(*) FROM paper_events").fetchone()[0]
    seq_before = _seq(conn, "paper_events")
    tmp = "paper_events_new_022"
    for stmt in ("DROP TRIGGER IF EXISTS paper_events_no_update",
                 "DROP TRIGGER IF EXISTS paper_events_no_delete"):
        conn.execute(stmt)
    conn.execute(paper_events_sql(tmp))
    cols = ", ".join(PAPER_COLUMNS)
    conn.execute(
        f"INSERT INTO {tmp} ({cols}) SELECT {cols} FROM paper_events ORDER BY event_id")
    conn.execute("DROP TABLE paper_events")
    conn.execute(f"ALTER TABLE {tmp} RENAME TO paper_events")
    for stmt in PAPER_EVENTS_INDEX:
        conn.execute(stmt)
    for stmt in _append_only_triggers("paper_events"):
        conn.execute(stmt)
    _restore_seq(conn, "paper_events", seq_before)
    after = conn.execute("SELECT COUNT(*) FROM paper_events").fetchone()[0]
    if after != before:
        raise RuntimeError(f"022 copied {after} paper_events rows but found {before} — refusing")
    return after


def _rebuild_run_manifests(conn: sqlite3.Connection) -> int:
    live_cols = _columns(conn, "run_manifests")
    if live_cols != RUN_MANIFESTS_OLD_COLUMNS:
        raise RuntimeError(
            f"022 refuses: run_manifests columns are {live_cols}, expected "
            f"{RUN_MANIFESTS_OLD_COLUMNS}")
    before = conn.execute("SELECT COUNT(*) FROM run_manifests").fetchone()[0]
    seq_before = _seq(conn, "run_manifests")
    tmp = "run_manifests_new_022"
    for stmt in ("DROP TRIGGER IF EXISTS run_manifests_end_once",
                 "DROP TRIGGER IF EXISTS run_manifests_no_delete"):
        conn.execute(stmt)
    conn.execute(run_manifests_sql(tmp))
    old_cols = ", ".join(RUN_MANIFESTS_OLD_COLUMNS)
    # end_reason is not selected: existing rows get its column default (NULL),
    # which is exactly right — no manifest closed before this migration
    # existed ever recorded a reason.
    conn.execute(
        f"INSERT INTO {tmp} ({old_cols}) SELECT {old_cols} FROM run_manifests "
        "ORDER BY run_id")
    conn.execute("DROP TABLE run_manifests")
    conn.execute(f"ALTER TABLE {tmp} RENAME TO run_manifests")
    for stmt in manifest_triggers():
        conn.execute(stmt)
    _restore_seq(conn, "run_manifests", seq_before)
    after = conn.execute("SELECT COUNT(*) FROM run_manifests").fetchone()[0]
    if after != before:
        raise RuntimeError(f"022 copied {after} run_manifests rows but found {before} — refusing")
    return after


def _rebuild_run_calls(conn: sqlite3.Connection) -> int:
    live_cols = _columns(conn, "run_calls")
    if live_cols != RUN_CALLS_OLD_COLUMNS:
        raise RuntimeError(
            f"022 refuses: run_calls columns are {live_cols}, expected "
            f"{RUN_CALLS_OLD_COLUMNS}")
    before = conn.execute("SELECT COUNT(*) FROM run_calls").fetchone()[0]
    seq_before = _seq(conn, "run_calls")
    tmp = "run_calls_new_022"
    for stmt in ("DROP TRIGGER IF EXISTS run_calls_no_update",
                 "DROP TRIGGER IF EXISTS run_calls_no_delete"):
        conn.execute(stmt)
    conn.execute(run_calls_sql(tmp))
    old_cols = ", ".join(RUN_CALLS_OLD_COLUMNS)
    # Every pre-022 row was written by the one completion path that existed
    # (10a-P0/P1 M4) — 'completed' is not a guess, it is the only outcome any
    # writer could have produced. outcome_detail gets its column default
    # (NULL): no writer has ever populated a detail column that did not exist.
    conn.execute(
        f"INSERT INTO {tmp} ({old_cols}, outcome) "
        f"SELECT {old_cols}, 'completed' FROM run_calls ORDER BY call_id")
    conn.execute("DROP TABLE run_calls")
    conn.execute(f"ALTER TABLE {tmp} RENAME TO run_calls")
    for stmt in RUN_CALLS_INDEX:
        conn.execute(stmt)
    for stmt in _record_only_triggers("run_calls", "one row per call, never edited"):
        conn.execute(stmt)
    _restore_seq(conn, "run_calls", seq_before)
    after = conn.execute("SELECT COUNT(*) FROM run_calls").fetchone()[0]
    if after != before:
        raise RuntimeError(f"022 copied {after} run_calls rows but found {before} — refusing")
    return after


def run_migration(db_path: str | None = None) -> dict:
    """Apply R212-R218: rebuild paper_events, run_manifests and run_calls;
    create claim_inputs and audit_verdicts. Idempotent by postcondition."""
    if db_path is None:
        raise ValueError("022 requires an explicit db_path")

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        if _already_applied(conn):
            return {"status": "already_applied", "rows_preserved": {}}
        _refuse_removed_event_types(conn)

        conn.execute("BEGIN")
        rows = {
            "paper_events": _rebuild_paper_events(conn),
            "run_manifests": _rebuild_run_manifests(conn),
            "run_calls": _rebuild_run_calls(conn),
        }
        conn.execute(CLAIM_INPUTS_SQL)
        for stmt in CLAIM_INPUTS_INDEX:
            conn.execute(stmt)
        for stmt in _record_only_triggers(
                "claim_inputs", "one row per extraction call, never edited"):
            conn.execute(stmt)

        conn.execute(audit_verdicts_sql())
        for stmt in AUDIT_VERDICTS_INDEX:
            conn.execute(stmt)
        for stmt in _record_only_triggers(
                "audit_verdicts", "one row per verdict, never edited"):
            conn.execute(stmt)

        # Scoped to what 022 touched (plus the two it deliberately left alone,
        # to prove B1(c)'s reasoning rather than merely assert it): a legacy
        # table's own FK debt is not 022's to discover, and must not block it.
        problems = [
            r for t in ("paper_events", "run_manifests", "run_calls",
                       "run_stage_configs", "claim_inputs", "audit_verdicts")
            for r in conn.execute(f"PRAGMA foreign_key_check({t})").fetchall()
        ]
        if problems:
            raise RuntimeError(f"022 refuses: foreign_key_check after rebuild: {problems[:10]}")

        conn.execute("COMMIT")
        return {"status": "executed", "rows_preserved": rows}
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
