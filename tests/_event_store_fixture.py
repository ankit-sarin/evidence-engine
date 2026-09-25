"""Build an S2 event store for tests whose fixtures predate it.

READERS-01 Phase 2a. Concordance, the evidence-table exporter, the judge loader
and the distribution monitor read through `engine/core/effective.py` now, and
their direct-table paths are gone (R30). Every test fixture that declared values
by INSERTing into `evidence_spans` / `cloud_evidence_spans` / `human_extractions`
therefore declares them somewhere no reader looks.

The tests themselves are not wrong — they pin distributions, exports and
alignments that are still the contract. Only the STORE moved. So the store moves
here, in one helper, rather than thirty-one test bodies being rewritten by hand
(B5: a test that pinned a behaviour is rewritten to the corrected behaviour).

Underscore-prefixed so pytest does not collect it.
"""

from __future__ import annotations

import importlib
import sqlite3
from pathlib import Path

from engine.core import events

m016 = importlib.import_module("engine.migrations.016_event_store")
m019 = importlib.import_module("engine.migrations.019_paper_state_axes")
m020 = importlib.import_module("engine.migrations.020_run_manifest")
m021 = importlib.import_module("engine.migrations.021_parsed_text_sha256")

_PAPERS_DDL = """
CREATE TABLE IF NOT EXISTS papers (
    id INTEGER PRIMARY KEY, title TEXT NOT NULL DEFAULT 't',
    source TEXT NOT NULL DEFAULT 'fixture', status TEXT NOT NULL DEFAULT 'EXTRACTED',
    created_at TEXT NOT NULL DEFAULT 'x', updated_at TEXT NOT NULL DEFAULT 'x',
    pmid TEXT, doi TEXT, authors TEXT, year INTEGER, journal TEXT
);
"""


_DONE: set[str] = set()


def ensure_event_store(db_path: str | Path) -> None:
    """Create `papers` (if absent) and the event store at the post-019 shape.

    Memoised per path: `add_values` is called repeatedly by a single fixture and
    running 016 + 019 on each call made the suite markedly slower for no effect
    (both are idempotent).
    """
    db_path = str(db_path)
    if db_path in _DONE:
        return
    conn = sqlite3.connect(db_path)
    conn.executescript(_PAPERS_DDL)
    if not conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE name='evidence_spans'"
    ).fetchone()[0]:
        conn.execute("CREATE TABLE evidence_spans (id INTEGER PRIMARY KEY)")
    m016.create_schema(conn)
    conn.commit()
    conn.close()
    m019.run_migration(db_path)
    m020.run_migration(db_path)
    # INPUT-IDENTITY-01: parsed_text_refs at 021's shape, so the resolver can
    # read references a fixture records. The table is empty here, so 021 reads
    # no file and needs no baseline.
    m021.run_migration(db_path)
    _DONE.add(db_path)


# ── Runs and seeds for fixtures (MANIFEST-01 Phase 2a, R68) ──────────
#: The one run a fixture database's events are written under. A real run's
#: manifest is written by `engine.core.run_manifest.open_run`, which resolves a
#: spec, fetches digests and checks the tree; a fixture that only needs somewhere
#: for `run_id` to point writes the rows directly, as below. It records nothing a
#: reader of these fixtures depends on.
FIXTURE_RUN_UID = "fixture-run"


def fixture_run(conn, arm: str | None = None, *, arm_kind: str = "model") -> int:
    """The fixture database's run id; with `arm`, that run also pins `arm`."""
    row = conn.execute("SELECT run_id FROM run_manifests WHERE run_uid = ?",
                       (FIXTURE_RUN_UID,)).fetchone()
    if row:
        run_id = row[0]
    else:
        run_id = conn.execute(
            "INSERT INTO run_manifests (run_uid, review_id, run_kind, git_commit, "
            "git_dirty, spec_hash, codebook_hash, codebook_sha256, library_versions_json, "
            "host, started_at, manifest_json, manifest_sha256) "
            "VALUES (?, 'fixture', 'extraction', ?, 0, 'fixture', 'fixture', 'fixture', "
            "'{}', 'fixture', '2026-01-01T00:00:00+00:00', '{}', 'fixture')",
            (FIXTURE_RUN_UID, "0" * 40)).lastrowid
    if arm is not None:
        if not conn.execute("SELECT 1 FROM arms WHERE arm_name = ?", (arm,)).fetchone():
            events.register_arm(conn, arm, arm_kind)
        if not conn.execute("SELECT 1 FROM run_stage_configs WHERE run_id = ? AND arm_name = ?",
                            (run_id, arm)).fetchone():
            conn.execute(
                "INSERT INTO run_stage_configs (run_id, stage, stage_kind, arm_name, provider, "
                "model_name, model_digest, options_json, options_hash, sent_keys_json, "
                "sources_json, keep_alive, format_schema_hash, prompt_hash) "
                "VALUES (?, ?, 'extract_pass2', ?, 'ollama', 'fixture', ?, '{}', 'fixture', "
                "'[]', '{}', '-1', 'none', 'fixture')",
                (run_id, f"fixture:{arm}", arm, "0" * 64))
    return run_id


def seed_pre_manifest_paper_event(conn, paper_id: int, *, to_state: str = "eligible",
                                  actor_name: str = "fixture",
                                  payload: dict | None = None) -> None:
    """One `state_at_migration` row exactly as migration 017 wrote them.

    Below the writer on purpose: R68 lets only a migration write a pre-manifest
    row, and a fixture standing in for 017's seed is standing in for a migration.
    """
    conn.execute(
        "INSERT INTO paper_events (event_uid, event_type, occurred_at, recorded_at, "
        "actor_kind, actor_role, actor_name, run_id, run_marker, payload_json, "
        "paper_id, to_state) VALUES (?, 'state_at_migration', ?, ?, 'engine', 'system', "
        "?, NULL, 'pre-manifest', ?, ?, ?)",
        (events.mint_extraction_uid(), "2026-01-01T00:00:00+00:00",
         "2026-01-01T00:00:00+00:00", actor_name, __import__("json").dumps(payload or {}),
         paper_id, to_state))


def add_values(db_path: str | Path, arm: str, field_name: str,
               values, *, arm_kind: str = "model", start_paper: int = 1,
               located: bool = True) -> None:
    """Declare `values` for `field_name` on `arm`, one paper each.

    `values[i]` belongs to paper `start_paper + i`. A `None` declares that the
    arm recorded nothing for that paper — which the reader returns as `missing`,
    the same fact the old fixtures expressed by omitting the row.
    """
    ensure_event_store(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        run_id = fixture_run(conn, arm, arm_kind=arm_kind)

        for i, value in enumerate(values):
            pid = start_paper + i
            conn.execute(
                "INSERT OR IGNORE INTO papers (id) VALUES (?)", (pid,))
            if not conn.execute(
                "SELECT COUNT(*) FROM paper_events WHERE paper_id = ?", (pid,)
            ).fetchone()[0]:
                seed_pre_manifest_paper_event(conn, pid)
            if value is None:
                continue
            uid = events.mint_extraction_uid()
            events.write_field_event(
                conn, event_type="asserted", paper_id=pid, field_name=field_name,
                arm=arm, value=value, extraction_uid=uid, source_snippet=value,
                actor_kind="model", actor_role="extractor", actor_name="fixture",
                run_id=run_id)
            if located:
                events.write_field_event(
                    conn, event_type="citation_located", paper_id=pid,
                    field_name=field_name, arm=arm, extraction_uid=uid,
                    actor_kind="engine", actor_role="system", actor_name="locator",
                    payload={"located": True, "snippet": value}, run_id=run_id)
        conn.commit()
    finally:
        conn.close()


def mirror_legacy_into_events(db, *, arm: str = "local") -> int:
    """Mirror a legacy-shaped fixture's rows into the event store. TEST ONLY.

    Some fixtures declare their world through `ReviewDatabase.add_extraction` /
    `add_evidence_span` / `update_status` — the engine's own write path before
    the event store existed. Those calls are still the clearest way to say what
    the fixture contains, and rewriting them by hand into `write_field_event`
    calls would change what the test READS without changing what it MEANS.

    So this translates instead: the latest extraction's spans become `asserted`
    (+ `citation_located`) field events, and a corpus status becomes one
    `eligible` paper event. It is the shape migration 017 seeded on live, with
    field values added — which 017 deliberately did not do (R25: seed, do not
    migrate). **That is why this lives in `tests/` and not in `engine/`:** it is
    a fixture convenience, not a supported import path, and nothing in the
    engine may grow a dependency on it.

    Returns the number of field events written.
    """
    conn = db._conn
    ensure_event_store(db.db_path)

    run_id = fixture_run(conn, arm)

    corpus = ("FT_ELIGIBLE", "EXTRACTED", "AI_AUDIT_COMPLETE",
              "HUMAN_AUDIT_COMPLETE", "EXTRACT_FAILED")
    for (pid,) in conn.execute(
        f"SELECT id FROM papers WHERE status IN "
        f"({', '.join('?' * len(corpus))}) ORDER BY id", corpus
    ).fetchall():
        if not conn.execute(
            "SELECT COUNT(*) FROM paper_events WHERE paper_id = ?", (pid,)
        ).fetchone()[0]:
            seed_pre_manifest_paper_event(conn, pid)

    rows = conn.execute(
        """SELECT e.paper_id, es.field_name, es.value, es.source_snippet
           FROM evidence_spans es
           JOIN extractions e ON e.id = es.extraction_id
           WHERE e.id = (SELECT MAX(e2.id) FROM extractions e2
                         WHERE e2.paper_id = e.paper_id)
           ORDER BY e.paper_id, es.id"""
    ).fetchall()

    n = 0
    for paper_id, field_name, value, snippet in rows:
        if value is None:
            continue
        uid = events.mint_extraction_uid()
        events.write_field_event(
            conn, event_type="asserted", paper_id=paper_id,
            field_name=field_name, arm=arm, value=value,
            extraction_uid=uid, source_snippet=snippet,
            actor_kind="model", actor_role="extractor", actor_name="fixture",
            run_id=run_id)
        events.write_field_event(
            conn, event_type="citation_located", paper_id=paper_id,
            field_name=field_name, arm=arm, extraction_uid=uid,
            actor_kind="engine", actor_role="system", actor_name="locator",
            payload={"located": bool(snippet), "snippet": snippet or ""}, run_id=run_id)
        n += 1
    conn.commit()
    return n


def upgrade_event_store(db_path) -> None:
    """Bring a fixture database that already holds `papers` (and, if it had them,
    016's tables) to the post-020 shape: 016 → 019 → 020, each idempotent."""
    conn = sqlite3.connect(str(db_path))
    try:
        if not conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE name='evidence_spans'").fetchone()[0]:
            conn.execute("CREATE TABLE evidence_spans (id INTEGER PRIMARY KEY)")
        m016.create_schema(conn)
        conn.commit()
    finally:
        conn.close()
    m019.run_migration(str(db_path))
    m020.run_migration(str(db_path))


def seed_claim(conn, *, arm: str, paper_id: int, field_name: str, value=None,
               claim_id: str | None = None, extraction_uid: str | None = None,
               source_snippet: str | None = None, event_type: str = "asserted",
               actor_name: str = "seed", payload: dict | None = None) -> str:
    """A claim as a migration would seed it on a pre-manifest arm: no run,
    `run_marker = 'pre-manifest'`. Below the writer on purpose — R59 refuses a
    NEW claim on such an arm, and v2.1 row 7 is exactly two such seeded claims."""
    import json
    extraction_uid = extraction_uid or events.mint_extraction_uid()
    claim_id = claim_id or events.make_claim_id(arm, extraction_uid, field_name)
    conn.execute(
        "INSERT INTO field_events (event_uid, event_type, occurred_at, recorded_at, "
        "actor_kind, actor_role, actor_name, run_id, run_marker, payload_json, claim_id, "
        "extraction_uid, paper_id, field_name, arm, value, source_snippet) "
        "VALUES (?, ?, ?, ?, 'model', 'extractor', ?, NULL, 'pre-manifest', ?, ?, ?, ?, ?, ?, ?, ?)",
        (events.mint_extraction_uid(), event_type, "2026-01-01T00:00:00+00:00",
         "2026-01-01T00:00:00+00:00", actor_name,
         json.dumps(payload or {"state_at_write": "seeded"}), claim_id, extraction_uid,
         paper_id, field_name, arm, value, source_snippet))
    return claim_id


def run_for(conn) -> int:
    """The fixture run, pinning every registered model arm that can take a claim
    (not pre-manifest, not retired). For tests whose subject is a READER: their
    events need a run (R68) and their claims a pinned arm (R10), and neither is
    what they test. Tests of the writer's refusals build their runs explicitly."""
    run_id = fixture_run(conn)
    for (arm,) in conn.execute(
            "SELECT arm_name FROM arms WHERE arm_kind = 'model' AND retired_at IS NULL "
            "AND (configuration_marker IS NULL OR configuration_marker <> ?)",
            (events.PRE_MANIFEST,)).fetchall():
        fixture_run(conn, arm)
    return run_id


def seed_eligibility(conn, paper_id: int, *, to_state: str = "eligible") -> int:
    """One eligibility-axis paper event, through the engine's writer, under the
    fixture run (R68). WRITE-PATH-01 9b-2a R5: extraction selects on the
    eligibility axis now, so a fixture that seeded only `papers.status` selects
    nothing; this is the one place such fixtures get their corpus membership."""
    return events.write_paper_event(
        conn, event_type="screened", paper_id=paper_id, to_state=to_state,
        actor_kind="engine", actor_role="system", actor_name="fixture",
        run_id=fixture_run(conn))


#: The digest every fixture extraction run resolves (9b-2b R117 fixtures).
FIXTURE_DIGEST = "a" * 64


def open_extraction_run(db, spec, *, digest: str = FIXTURE_DIGEST) -> int:
    """A real run manifest on a scratch database, through `run_manifest.open_run`
    (WRITE-PATH-01 9b-2b): the spec's extraction stages plus `audit`, every
    digest `digest`, a clean tree. It pins the spec's arm exactly as a
    production run would, so `extraction_digest` agrees. Returns the run_id.

    Open it BEFORE writing any claim on the arm: once the arm holds a claim the
    freeze trigger refuses the pin (R21)."""
    from engine.agents.extractor import extraction_stages
    from engine.core import run_manifest as rm
    from engine.core.codebook import load_codebook_beside

    handle = rm.open_run(
        db._conn, spec, kind="extraction",
        stages=[*extraction_stages(spec), "audit"],
        codebook=load_codebook_beside(db.db_path), digest_fn=lambda m: digest,
        git=rm.GitState(commit="0" * 40, dirty=False, tag=None), host="fixture")
    return handle.run_id
