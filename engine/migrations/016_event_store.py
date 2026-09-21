"""Migration 016: the event store, the arm registry, and the two identity tables.

EFFECTIVE-RESULT-02 (session 5, S2 core). This is the structure half of S2. It
creates seven tables and touches none that already exist: under R25 the legacy
stores — `extractions`, `evidence_spans`, `cloud_extractions`,
`cloud_evidence_spans`, the audit, screening and adjudication tables, and
`workflow_state` — stay exactly as they are, read-only, as regression fixture
and telemetry. Nothing is imported from them and nothing is rewritten in them.

**Append-only by trigger, in the database, not by convention in the writer**
(read-out §3.4). The database already has five delete sites and two importers
that bypass each other; a rule that lives only in the writer is a rule the next
writer does not inherit. A trigger refuses `scripts/`, `analysis/`, an ad-hoc
`sqlite3` session and a future app equally. The consequence, stated plainly: a
mistake is corrected by **appending a correcting event**, never by editing.

**The against-reference is set-valued** (R20). Rows 6 and 7 of resolution rule
v2.1 can only be exited by a decision naming *every* competing claim, and row 2
by one naming *every* competing decision. Addendum 3 §G: "read-out §3.2's
`against_claim_id TEXT` is superseded by a set-valued reference, and session 5
must not implement the singular column." It is two junction tables rather than a
canonical JSON array because the reader's whole test is set equality and proper
subset, which the junction makes indexable SQL and the array makes a
parse-every-candidate loop — and a JSON copy beside the junction would be a
second source of truth that drifts.

**No UNIQUE on `extraction_uid` or on `claim_id`.** v2.1 row 6 requires **two
`asserted` events sharing one `claim_id`** (duplicate values within one claim),
and `claim_id` embeds `extraction_uid`, so a UNIQUE on either makes row 6
unreachable and its fixture unconstructible. `event_uid TEXT NOT NULL UNIQUE` is
the per-event identity, which is what §2.2b actually needs. `extraction_uid`'s
uniqueness is a property of **minting** (uuid4) and would be enforced against an
extraction registry, which does not exist this session and is not in scope.

**`field_state` is not a column.** The reader derives every state and never
reads a stored one (read-out §5.5). The writer records
`{"state_at_write": …, "rule_version": "v2.1"}` in `payload_json`, which is
telemetry a test asserts the derivation against — not a second source of truth
the reader consults.

**No S4 reservations.** Read-out §3.3 proposed reserving `criterion_id`, two
offsets and three verifier columns on `paper_events`; addendum 2 §C.3 withdrew
them under R15, which makes S4 screening evidence a separate table keyed to the
event, added with S4.

**`to_state`'s vocabulary is introduced here.** Read-out §3.3 declared
`to_state … from the S2 paper-level vocabulary, CHECK-constrained` and never
enumerated it anywhere; §5.4 gives four prose labels, not tokens. The five below
are those four tokenised plus the unqualified `eligible`, which is what the seed
writes: R4 makes corpus membership *eligibility*, and R25 seeds eligibility only
with processing empty, so `audited_ai` would be the processing fact the seed is
forbidden to carry.
"""

from __future__ import annotations

import sqlite3

#: Paper-level states. See the module docstring for why this list begins here.
TO_STATES = (
    "eligible",
    "abstract_out",
    "full_text_out",
    "full_text_not_obtainable",
    "audited_ai",
)

FIELD_EVENT_TYPES = (
    "asserted", "declined", "contract_unmet", "superseded",
    "human_accepted", "human_corrected", "human_withdrew",
    "duplicate_detected", "citation_located", "state_at_migration",
)

PAPER_EVENT_TYPES = (
    "identified", "duplicate_of", "screened", "verified", "adjudicated",
    "acquired", "not_obtainable", "parsed", "extracted", "extraction_failed",
    "audited", "manual_advance", "bypass", "state_at_migration",
)

#: The marker R10 requires on an arm registered before manifests existed. It is
#: what makes v2.1 row 7 — indistinguishable pre-manifest claims — reachable
#: (addendum 2 §C.4). Imported, never re-spelled: the reader compares against it.
from engine.core.effective import PRE_MANIFEST  # noqa: E402  (re-exported)


def _in(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{v}'" for v in values)


def _spine(table: str) -> str:
    """The provenance spine both event tables carry (read-out §3.1).

    `actor_role` is a first-class column per addendum 2 §C.2 (R11), and is a
    different axis from `actor_kind`: a *human* can be a reviewer or an
    extractor, and v2.1's override rows gate on **role**. `'system'` extends the
    pair for engine-written events — seeded `state_at_migration` rows and
    `citation_located` rows — and never triggers an override row.
    """
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
    run_id      INTEGER,
    run_marker  TEXT,
    prior_event_id INTEGER REFERENCES {table}(event_id),
    presented_context_sha256 TEXT,
    reason      TEXT,
    payload_json TEXT   NOT NULL DEFAULT '{{}}'
    """


def _append_only(table: str) -> str:
    return f"""
    CREATE TRIGGER IF NOT EXISTS {table}_no_update BEFORE UPDATE ON {table}
      BEGIN SELECT RAISE(ABORT, '{table} is append-only: correct by appending an event'); END;
    CREATE TRIGGER IF NOT EXISTS {table}_no_delete BEFORE DELETE ON {table}
      BEGIN SELECT RAISE(ABORT, '{table} is append-only: correct by appending an event'); END;
    """


def schema_sql() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS arms (
        arm_name             TEXT PRIMARY KEY,
        arm_kind             TEXT NOT NULL CHECK (arm_kind IN ('model', 'human_extractor')),
        configuration_json   TEXT NOT NULL DEFAULT '{{}}',
        configuration_marker TEXT,
        registered_at        TEXT NOT NULL,
        retired_at           TEXT
    );

    CREATE TABLE IF NOT EXISTS field_events (
        {_spine('field_events')},
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
               OR json_extract(payload_json, '$.located') IS NOT NULL)
    );

    CREATE TABLE IF NOT EXISTS paper_events (
        {_spine('paper_events')},
        paper_id    INTEGER NOT NULL REFERENCES papers(id),
        to_state    TEXT    NOT NULL CHECK (to_state IN ({_in(TO_STATES)})),
        from_state  TEXT,
        reason_code TEXT,
        stage_name  TEXT,
        CHECK (actor_role <> 'system' OR actor_kind = 'engine'),
        CHECK (event_type IN ({_in(PAPER_EVENT_TYPES)}))
    );

    -- R20: a reviewer event's against-reference is a SET of claim ids, and for
    -- the conflict case a set of decision (event) ids. Written in the same
    -- transaction as the event row, so a crash leaves neither.
    CREATE TABLE IF NOT EXISTS field_event_against (
        event_id         INTEGER NOT NULL REFERENCES field_events(event_id),
        against_claim_id TEXT    NOT NULL,
        PRIMARY KEY (event_id, against_claim_id)
    );
    CREATE TABLE IF NOT EXISTS field_event_against_decisions (
        event_id         INTEGER NOT NULL REFERENCES field_events(event_id),
        against_event_id INTEGER NOT NULL REFERENCES field_events(event_id),
        PRIMARY KEY (event_id, against_event_id)
    );

    -- Parsed-text references mint their own uid: full_text_assets.id is a plain
    -- INTEGER PRIMARY KEY and is therefore reusable after a delete, for exactly
    -- the reason read-out §2.2b gives for evidence_spans.id. The source id is
    -- recorded as provenance and is never an identity.
    CREATE TABLE IF NOT EXISTS parsed_text_refs (
        parsed_text_uid   TEXT PRIMARY KEY,
        paper_id          INTEGER NOT NULL REFERENCES papers(id),
        parsed_text_path  TEXT    NOT NULL,
        parsed_text_version INTEGER NOT NULL,
        source_full_text_assets_id INTEGER,
        recorded_at       TEXT    NOT NULL,
        UNIQUE (paper_id, parsed_text_version, parsed_text_path)
    );

    -- The seed of the S3a run manifest (session 7). Nothing more is built here:
    -- this table records which spec and which codebook the store was seeded
    -- against, so a later manifest has an identity to point back to.
    CREATE TABLE IF NOT EXISTS review_identities (
        kind        TEXT NOT NULL,
        key         TEXT NOT NULL,
        value_json  TEXT NOT NULL,
        recorded_at TEXT NOT NULL,
        PRIMARY KEY (kind, key)
    );

    CREATE INDEX IF NOT EXISTS idx_field_events_cell
        ON field_events(paper_id, field_name, arm, event_id);
    CREATE INDEX IF NOT EXISTS idx_field_events_claim
        ON field_events(claim_id);
    CREATE INDEX IF NOT EXISTS idx_field_events_extraction_uid
        ON field_events(extraction_uid);
    CREATE INDEX IF NOT EXISTS idx_paper_events_paper
        ON paper_events(paper_id, event_id);

    {_append_only('field_events')}
    {_append_only('paper_events')}
    {_append_only('field_event_against')}
    {_append_only('field_event_against_decisions')}
    {_append_only('parsed_text_refs')}
    {_append_only('review_identities')}

    -- R21: an arm's configuration cannot be re-pinned once the arm holds a
    -- claim; a new configuration is a NEW ARM (R10). Retirement stays possible,
    -- because retiring means "accepts no new claims" while its claims stand —
    -- so retired_at is deliberately outside the frozen column list.
    CREATE TRIGGER IF NOT EXISTS arms_configuration_frozen_once_claimed
      BEFORE UPDATE OF arm_kind, configuration_json, configuration_marker ON arms
      WHEN EXISTS (SELECT 1 FROM field_events WHERE arm = OLD.arm_name)
      BEGIN
        SELECT RAISE(ABORT,
          'arms: configuration cannot be re-pinned once the arm holds a claim (R21) — a changed configuration is a new arm (R10)');
      END;

    -- A rename is refused unconditionally: claim ids embed the arm name, so a
    -- rename orphans every claim that ever named it.
    CREATE TRIGGER IF NOT EXISTS arms_name_frozen
      BEFORE UPDATE OF arm_name ON arms
      BEGIN
        SELECT RAISE(ABORT,
          'arms: arm_name is immutable — claim ids embed it, so a rename orphans every claim');
      END;
    """


def create_schema(conn: sqlite3.Connection) -> None:
    """Create the event store on an open connection. Idempotent."""
    conn.executescript(schema_sql())


def run_migration(db_path: str | None = None) -> dict:
    if db_path is None:
        raise ValueError("016 requires an explicit db_path")
    conn = sqlite3.connect(str(db_path))
    try:
        before = _tables(conn)
        create_schema(conn)
        conn.commit()
        after = _tables(conn)
        return {"created": sorted(after - before), "tables": len(after)}
    finally:
        conn.close()


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
