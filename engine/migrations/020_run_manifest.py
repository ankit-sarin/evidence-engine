"""Migration 020: the run manifest, run linkage and arm pinning (S3a, S3b, R59, R68).

MANIFEST-01 Phase 2a. Session 7's schema, in one module-owned transaction:

* **`run_manifests`** — one row per run, written before its first model call.
  `git_dirty` is CHECKed to 0, so a dirty-tree run cannot be recorded even by a
  writer that forgot to refuse. The body is immutable; the single permitted
  UPDATE is the run's end (`ended_at` + `end_status`, once).
* **`run_stage_configs`** — one row per (run, stage): the resolver's
  `EffectiveConfig` — model, digest, options and their hash, sent keys, sources,
  keep_alive, format-schema hash, prompt hash. Append-only.
* **`run_calls`** — one row per model call, local and cloud: the hash of the
  request as sent, a digest of the response, timestamps. Its `(run_id, stage)`
  must name a declared stage row. Append-only.
* **`field_events` and `paper_events` rebuilt** under the 019 template with
  `run_id REFERENCES run_manifests(run_id)` and R77's CHECK enumerating the two
  permitted states — a run and no marker, or no run and `'pre-manifest'`. Rows are
  copied verbatim, `event_id` preserved, the AUTOINCREMENT high-water mark
  restored; the append-only triggers are dropped and restored inside the
  transaction.
* **`arms` gains `pinned_run_id` and `pinned_sha256`** (ALTER, so a fresh and a
  live database agree on column order — the C14 lesson), and the freeze trigger
  widens from "holds a claim" to "holds a claim OR is pinned OR is
  pre-manifest" (R59). The seeded rows' values are untouched.

**Self-contained (R35).** Every DDL string, token list and marker literal below
is declared here and imported from nowhere. `tests/test_run_manifest_migration.py`
asserts the re-declared lists agree with `engine/core/effective_config.py`,
`engine/core/run_manifest.py` and migrations 016/019.

**Idempotent by postcondition (R44).** The last structural act is the event
tables' rename; `_already_applied` recognises 020's work by `paper_events`'
foreign keys naming `run_manifests` together with the widened arms trigger.

**Transaction discipline (the 017/019 pattern).** One `BEGIN`, every statement
through `Connection.execute` (never `executescript`, which issues an implicit
COMMIT), the rebuilt tables built under temporary names (renaming a referenced
table away rewrites its referrers — A11's cause), `ROLLBACK` on any exception.
"""

from __future__ import annotations

import sqlite3

# ── R35: re-declared here, never imported ────────────────────────────
PRE_MANIFEST_MARKER = "pre-manifest"          # event rows seeded before manifests
ARM_PRE_MANIFEST = "not recorded (pre-manifest)"  # arms registered before manifests
ARM_PINNED = "pinned"

RUN_KINDS = ("extraction", "screening", "judge", "review_session")
END_STATUSES = ("completed", "failed", "interrupted")
PROVIDERS = ("ollama", "openai", "anthropic")

OLLAMA_STAGES = (
    "abstract_screen_primary", "abstract_screen_verifier",
    "ft_screen_primary", "ft_screen_verifier",
    "audit",
    "extract_pass1", "extract_pass2", "extract_retry_snippet",
    "elicitation_pass1",
    "vision_parse", "pdf_quality",
    "preflight",
)
STAGE_KINDS = OLLAMA_STAGES + ("cloud",)

FIELD_EVENT_TYPES = (
    "asserted", "declined", "contract_unmet", "superseded",
    "human_accepted", "human_corrected", "human_withdrew",
    "duplicate_detected", "citation_located", "state_at_migration",
)

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

#: R68's invariant as R77 states it: the two permitted states, enumerated. Written
#: with IS, never `=`: SQLite passes a CHECK that evaluates to NULL, and the first
#: draft of this constraint (`run_id IS NOT NULL OR run_marker = 'pre-manifest'`)
#: admitted a row with both columns NULL (R78, R79). The 2b postcondition
#: read-back expects exactly this text on both tables.
RUN_LINK_CHECK = (
    "CHECK (\n"
    "            (run_id IS NOT NULL AND run_marker IS NULL)\n"
    "            OR\n"
    f"            (run_id IS NULL AND run_marker IS '{PRE_MANIFEST_MARKER}')\n"
    "        )"
)

#: The widened arms trigger's name. Its presence is half of 020's postcondition.
ARMS_TRIGGER = "arms_configuration_frozen"
OLD_ARMS_TRIGGER = "arms_configuration_frozen_once_claimed"

#: Column order is what `db_fingerprint` hashes a row in, so both rebuilds keep
#: it exactly (the 019 lesson).
SPINE_COLUMNS = (
    "event_id", "event_uid", "event_type", "occurred_at", "recorded_at",
    "actor_kind", "actor_role", "actor_name", "actor_digest", "run_id",
    "run_marker", "prior_event_id", "presented_context_sha256", "reason",
    "payload_json",
)
FIELD_COLUMNS = SPINE_COLUMNS + (
    "claim_id", "extraction_uid", "paper_id", "field_name", "arm", "value",
    "source_snippet",
)
PAPER_COLUMNS = SPINE_COLUMNS + (
    "paper_id", "to_state", "from_state", "reason_code", "stage_name",
)


def _in(values) -> str:
    return ", ".join(f"'{v}'" for v in values)


def _axis(kind: str) -> tuple[str, ...]:
    return tuple(t for t, a in EVENT_TYPE_AXIS.items() if a == kind)


# ── DDL ──────────────────────────────────────────────────────────────
RUN_MANIFESTS_SQL = f"""
CREATE TABLE run_manifests (
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
    cloud_arms_json TEXT    NOT NULL DEFAULT '[]',
    payload_description TEXT,
    manifest_json   TEXT    NOT NULL,
    manifest_sha256 TEXT    NOT NULL,
    CHECK ((ended_at IS NULL) = (end_status IS NULL)),
    CHECK (cloud_arms_json = '[]' OR payload_description IS NOT NULL)
)
"""

RUN_STAGE_CONFIGS_SQL = f"""
CREATE TABLE run_stage_configs (
    run_id          INTEGER NOT NULL REFERENCES run_manifests(run_id),
    stage           TEXT    NOT NULL,
    stage_kind      TEXT    NOT NULL CHECK (stage_kind IN ({_in(STAGE_KINDS)})),
    arm_name        TEXT    REFERENCES arms(arm_name),
    provider        TEXT    NOT NULL CHECK (provider IN ({_in(PROVIDERS)})),
    model_name      TEXT    NOT NULL,
    model_digest    TEXT,
    options_json    TEXT    NOT NULL,
    options_hash    TEXT    NOT NULL,
    sent_keys_json  TEXT    NOT NULL,
    sources_json    TEXT    NOT NULL,
    keep_alive      TEXT    NOT NULL,
    format_schema_hash TEXT NOT NULL,
    prompt_hash     TEXT    NOT NULL,
    PRIMARY KEY (run_id, stage),
    -- R78: NULL-safe. `length(NULL) = 64` is NULL, which a CHECK passes, so the
    -- digest's presence is tested with IS NOT NULL before its length.
    CHECK (provider IS NOT 'ollama' OR (model_digest IS NOT NULL AND length(model_digest) = 64))
)
"""

RUN_CALLS_SQL = """
CREATE TABLE run_calls (
    call_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          INTEGER NOT NULL REFERENCES run_manifests(run_id),
    stage           TEXT    NOT NULL,
    paper_id        INTEGER REFERENCES papers(id),
    request_hash    TEXT    NOT NULL,
    response_digest TEXT,
    started_at      TEXT    NOT NULL,
    ended_at        TEXT    NOT NULL,
    FOREIGN KEY (run_id, stage) REFERENCES run_stage_configs(run_id, stage)
)
"""

INDEX_STATEMENTS = (
    "CREATE INDEX IF NOT EXISTS idx_run_calls_run ON run_calls(run_id, stage)",
)


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


def field_events_sql(name: str = "field_events") -> str:
    return f"""
    CREATE TABLE {name} ({_spine(name)},
        claim_id       TEXT    NOT NULL,
        extraction_uid TEXT,
        paper_id       INTEGER NOT NULL REFERENCES papers(id),
        field_name     TEXT    NOT NULL,
        arm            TEXT    NOT NULL REFERENCES arms(arm_name),
        value          TEXT,
        source_snippet TEXT,
        CHECK (actor_role <> 'system' OR actor_kind = 'engine'),
        CHECK (event_type IN ({_in(FIELD_EVENT_TYPES)})),
        CHECK (event_type <> 'citation_located'
               OR json_extract(payload_json, '$.located') IS NOT NULL),
        -- R68: a row with no run is a seeded, pre-manifest row and nothing else.
        {RUN_LINK_CHECK}
    )
    """


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


EVENT_INDEXES = {
    "field_events": (
        "CREATE INDEX IF NOT EXISTS idx_field_events_cell "
        "ON field_events(paper_id, field_name, arm, event_id)",
        "CREATE INDEX IF NOT EXISTS idx_field_events_claim ON field_events(claim_id)",
        "CREATE INDEX IF NOT EXISTS idx_field_events_extraction_uid "
        "ON field_events(extraction_uid)",
    ),
    "paper_events": (
        "CREATE INDEX IF NOT EXISTS idx_paper_events_paper ON paper_events(paper_id, event_id)",
    ),
}


def append_only_triggers(table: str) -> tuple[str, ...]:
    """Two triggers, as separate statements (never `executescript`)."""
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


_MANIFEST_BODY = (
    "run_uid", "review_id", "run_kind", "git_commit", "git_dirty", "git_tag",
    "engine_state", "spec_hash", "codebook_hash", "codebook_sha256",
    "library_versions_json", "host", "started_at", "cloud_arms_json",
    "payload_description", "manifest_json", "manifest_sha256",
)


def manifest_triggers() -> tuple[str, ...]:
    """The body is immutable; the run's end is written once; nothing is deleted."""
    changed = " OR ".join(f"NEW.{c} IS NOT OLD.{c}" for c in _MANIFEST_BODY)
    return (
        "CREATE TRIGGER IF NOT EXISTS run_manifests_end_once BEFORE UPDATE ON run_manifests "
        f"WHEN OLD.ended_at IS NOT NULL OR NEW.run_id IS NOT OLD.run_id OR {changed} "
        "BEGIN SELECT RAISE(ABORT, 'run_manifests: a manifest is written before the "
        "first call and never edited; only its end is recorded, once'); END",
        "CREATE TRIGGER IF NOT EXISTS run_manifests_no_delete BEFORE DELETE ON run_manifests "
        "BEGIN SELECT RAISE(ABORT, 'run_manifests: a run record is never deleted'); END",
    )


def arms_trigger() -> str:
    """R59: frozen once claimed, once pinned, or when registered pre-manifest.

    `retired_at` stays outside the column list — retiring means "accepts no new
    claims" while its claims stand (R21), so it must remain possible.
    """
    return (
        f"CREATE TRIGGER IF NOT EXISTS {ARMS_TRIGGER} "
        "BEFORE UPDATE OF arm_kind, configuration_json, configuration_marker, "
        "pinned_run_id, pinned_sha256 ON arms "
        "WHEN EXISTS (SELECT 1 FROM field_events WHERE arm = OLD.arm_name) "
        f"OR OLD.configuration_marker IN ('{ARM_PINNED}', '{ARM_PRE_MANIFEST}') "
        "BEGIN SELECT RAISE(ABORT, 'arms: configuration is frozen once the arm holds a "
        "claim or is pinned by a manifest, and a pre-manifest arm never pins (R21, R59) "
        "— a changed configuration is a new arm (R10)'); END"
    )


# ── Postcondition ────────────────────────────────────────────────────
def _already_applied(conn: sqlite3.Connection) -> bool:
    """020's work, read from the schema itself (R44)."""
    fks = {r[2] for r in conn.execute("PRAGMA foreign_key_list(paper_events)")}
    trig = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?", (ARMS_TRIGGER,)
    ).fetchone()
    return "run_manifests" in fks and bool(trig)


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


def _refuse_unlinked(conn) -> None:
    """A row with no run that is not marked pre-manifest cannot be copied under
    R68's CHECK, and a run is never invented for it (R25)."""
    bad = []
    for table in ("field_events", "paper_events"):
        n = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE run_id IS NULL "
            f"AND (run_marker IS NULL OR run_marker <> ?)", (PRE_MANIFEST_MARKER,)
        ).fetchone()[0]
        if n:
            bad.append(f"{table}: {n} row(s)")
        n = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id IS NOT NULL").fetchone()[0]
        if n:
            bad.append(f"{table}: {n} row(s) name a run_id no manifest can exist for yet")
    if bad:
        raise RuntimeError(
            "020 refuses: " + "; ".join(bad) + ". R68 permits a NULL run_id only on a "
            "seeded row marked 'pre-manifest', and a run is not reconstructable from the "
            "record (R25). Nothing was changed.")


def _rebuild(conn, table: str, columns: tuple[str, ...], build_sql) -> int:
    live_cols = _columns(conn, table)
    if live_cols != columns:
        raise RuntimeError(
            f"020 refuses: {table} columns are {live_cols}, expected {columns} "
            "(a rebuild copies by the pinned list and must not reorder or drop one)")
    before = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    seq_before = _seq(conn, table)
    tmp = f"{table}_new_020"
    for stmt in (f"DROP TRIGGER IF EXISTS {table}_no_update",
                 f"DROP TRIGGER IF EXISTS {table}_no_delete"):
        conn.execute(stmt)
    conn.execute(build_sql(tmp))
    cols = ", ".join(columns)
    conn.execute(f"INSERT INTO {tmp} ({cols}) SELECT {cols} FROM {table} ORDER BY event_id")
    conn.execute(f"DROP TABLE {table}")
    conn.execute(f"ALTER TABLE {tmp} RENAME TO {table}")
    for stmt in EVENT_INDEXES[table]:
        conn.execute(stmt)
    for stmt in append_only_triggers(table):
        conn.execute(stmt)
    _restore_seq(conn, table, seq_before)
    after = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if after != before:
        raise RuntimeError(f"020 copied {after} {table} rows but found {before} — refusing")
    return after


def run_migration(db_path: str | None = None, *, _fail_after_copy: bool = False) -> dict:
    """Create the manifest tables, pin columns and widened trigger; rebuild both
    event tables with the run link. Idempotent by postcondition.

    `_fail_after_copy` is for the forced-exception test: it raises after both
    rebuilds, which is the moment a non-transactional migration would have left
    the event tables replaced and their guards partly restored. Not reachable
    from the runner.
    """
    if db_path is None:
        raise ValueError("020 requires an explicit db_path")

    conn = sqlite3.connect(str(db_path), isolation_level=None)
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        if _already_applied(conn):
            return {"status": "already_applied", "rows_preserved": {}}
        _refuse_unlinked(conn)

        conn.execute("BEGIN")
        conn.execute(RUN_MANIFESTS_SQL)
        conn.execute(RUN_STAGE_CONFIGS_SQL)
        conn.execute(RUN_CALLS_SQL)
        for stmt in INDEX_STATEMENTS:
            conn.execute(stmt)
        for stmt in manifest_triggers():
            conn.execute(stmt)
        for stmt in _record_only_triggers(
                "run_stage_configs", "the configuration of a stage is written with its manifest"):
            conn.execute(stmt)
        for stmt in _record_only_triggers("run_calls", "one row per call, never edited"):
            conn.execute(stmt)

        arm_cols = _columns(conn, "arms")
        if "pinned_run_id" not in arm_cols:
            conn.execute("ALTER TABLE arms ADD COLUMN pinned_run_id INTEGER "
                         "REFERENCES run_manifests(run_id)")
        if "pinned_sha256" not in arm_cols:
            conn.execute("ALTER TABLE arms ADD COLUMN pinned_sha256 TEXT")
        # The arms trigger names field_events, so it comes off BEFORE the
        # rebuild and the widened one goes on AFTER it: SQLite validates every
        # trigger in the schema at RENAME, and a trigger naming a table that is
        # mid-rebuild fails that check. Measured on a fresh database.
        conn.execute(f"DROP TRIGGER IF EXISTS {OLD_ARMS_TRIGGER}")

        rows = {
            "field_events": _rebuild(conn, "field_events", FIELD_COLUMNS, field_events_sql),
            "paper_events": _rebuild(conn, "paper_events", PAPER_COLUMNS, paper_events_sql),
        }
        conn.execute(arms_trigger())
        if _fail_after_copy:
            raise RuntimeError("forced mid-rebuild failure (test)")

        # Scoped to what 020 touched: a legacy table's own FK debt is not 020's
        # to discover, and must not block it.
        problems = [r for t in ("field_events", "paper_events", "arms",
                                "field_event_against", "field_event_against_decisions")
                    for r in conn.execute(f"PRAGMA foreign_key_check({t})").fetchall()]
        if problems:
            raise RuntimeError(f"020 refuses: foreign_key_check after rebuild: {problems[:10]}")

        conn.execute("COMMIT")
        return {"status": "executed", "rows_preserved": rows}
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
