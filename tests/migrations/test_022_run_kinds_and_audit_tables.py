"""Migration 022 — R212-R218 (10a-R1, 10a-C2).

What is asserted here is the EFFECT on a database, never that a file mentions
a name: a fresh database carries 022 with an executed receipt; paper_events
retires 'identified'/'duplicate_of' while preserving every other row exactly;
run_manifests accepts 'import' and 'aborted'+end_reason; run_calls requires an
outcome; claim_inputs and audit_verdicts exist with their CHECKs and triggers;
run_stage_configs is provably untouched.
"""

from __future__ import annotations

import hashlib
import importlib
import sqlite3
import uuid
from pathlib import Path

import pytest

from engine.core.database import ReviewDatabase
from engine.core import run_manifest as rm
from engine.migrations import runner
from engine.tools.db_fingerprint import structure, structure_differences

m022 = importlib.import_module("engine.migrations.022_run_kinds_and_audit_tables")


@pytest.fixture
def fresh(tmp_path) -> Path:
    db = ReviewDatabase("m022", data_root=tmp_path)
    path = Path(db.db_path)
    db.close()
    return path


def _ro(path):
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def _table_sql(conn, table) -> str:
    return conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()[0]


def _row_hash(conn, table, pk) -> dict:
    """paper_id-keyed content hash of every column, for a row-preservation
    postcondition independent of insertion order."""
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
    out = {}
    for row in conn.execute(f"SELECT {', '.join(cols)} FROM {table} ORDER BY {pk}"):
        out[row[cols.index(pk)]] = hashlib.sha256(
            "|".join("" if v is None else str(v) for v in row).encode()
        ).hexdigest()
    return out


# ── T1 ──────────────────────────────────────────────────────────────


def test_T1_fresh_database_carries_022_executed(fresh):
    """R212: 022 applies on a fresh database with an executed receipt."""
    conn = _ro(fresh)
    row = conn.execute(
        "SELECT mode FROM schema_migrations WHERE migration_id = ?",
        ("022_run_kinds_and_audit_tables",),
    ).fetchone()
    assert row is not None and row[0] == "executed"

    paper_sql = _table_sql(conn, "paper_events")
    assert "'identified'" not in paper_sql
    assert "'duplicate_of'" not in paper_sql

    manifests_sql = _table_sql(conn, "run_manifests")
    assert "'import'" in manifests_sql
    assert "'aborted'" in manifests_sql
    assert "end_reason" in manifests_sql

    calls_sql = _table_sql(conn, "run_calls")
    for token in m022.RUN_CALL_OUTCOMES:
        assert f"'{token}'" in calls_sql
    assert "outcome_detail" in calls_sql

    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"claim_inputs", "audit_verdicts"} <= tables
    conn.close()


def test_T1_a_second_pass_executes_nothing(fresh):
    result = runner.run(fresh)
    assert result["executed"] == []
    assert "022_run_kinds_and_audit_tables" in result["already"]


# ── T2 ──────────────────────────────────────────────────────────────


def _seed_paper_event(conn, *, paper_id, event_type, to_state, run_marker="pre-manifest"):
    conn.execute(
        "INSERT INTO paper_events (event_uid, event_type, occurred_at, recorded_at, "
        "actor_kind, actor_role, actor_name, run_id, run_marker, payload_json, "
        "paper_id, to_state) VALUES (?, ?, 't', 't', 'engine', 'system', 'seed', "
        "NULL, ?, '{}', ?, ?)",
        (str(uuid.uuid4()), event_type, run_marker, paper_id, to_state),
    )


def test_T2_022_preserves_paper_events_rows_and_triggers(tmp_path):
    """R213/R25: a receipt-bearing (021-applied, 022 genuinely NOT yet applied
    — I3's monkeypatched-discovery pattern, not a receipt-row deletion, which
    would leave 022's schema in place while lying about its receipt) fixture
    with distinct-state seeded rows keeps its row count and per-row content
    through 022; its append-only triggers fire after the rebuild."""
    real_discover = runner.discover
    runner.discover = lambda: [
        (m, p) for m, p in real_discover()
        if m != "022_run_kinds_and_audit_tables"
    ]
    try:
        db = ReviewDatabase("m022_t2", data_root=tmp_path)
    finally:
        runner.discover = real_discover
    path = Path(db.db_path)
    conn = db._conn
    conn.execute("INSERT INTO papers (id, title, source, created_at, updated_at) VALUES (9001, 't1', 's', 't', 't')")
    conn.execute("INSERT INTO papers (id, title, source, created_at, updated_at) VALUES (9002, 't2', 's', 't', 't')")
    conn.execute("INSERT INTO papers (id, title, source, created_at, updated_at) VALUES (9003, 't3', 's', 't', 't')")
    _seed_paper_event(conn, paper_id=9001, event_type="state_at_migration", to_state="eligible")
    _seed_paper_event(conn, paper_id=9002, event_type="acquired", to_state="parsed")
    _seed_paper_event(conn, paper_id=9003, event_type="manual_advance", to_state="eligible")
    conn.commit()
    db.close()

    before_sql = _table_sql(_ro(path), "paper_events")
    assert "'identified'" in before_sql, (
        "the fixture must genuinely predate 022, or this test proves nothing")
    before = _row_hash(_ro(path), "paper_events", "event_id")
    assert len(before) == 3, "seeding must have landed, or this test proves nothing"

    # Receipt-bearing (021 applied) + something pending: the 10a-C1 guard
    # requires apply_pending=True, exactly as a real 10b rehearsal would pass.
    result = runner.run(path, apply_pending=True)
    assert result["executed"] == ["022_run_kinds_and_audit_tables"]

    after_conn = _ro(path)
    after = _row_hash(after_conn, "paper_events", "event_id")
    assert after == before, "022 must preserve every row's content exactly"

    with pytest.raises(sqlite3.OperationalError):
        after_conn.execute("UPDATE paper_events SET reason = 'x' WHERE event_id = 1")
    with pytest.raises(sqlite3.OperationalError):
        after_conn.execute("DELETE FROM paper_events WHERE event_id = 1")
    after_conn.close()


# ── T3 ──────────────────────────────────────────────────────────────


def test_T3_paper_events_refuses_identified_and_duplicate_of(fresh):
    conn = sqlite3.connect(str(fresh))
    conn.execute("INSERT INTO papers (id, title, source, created_at, updated_at) VALUES (1, 't', 's', 't', 't')")
    for event_type in ("identified", "duplicate_of"):
        with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint"):
            conn.execute(
                "INSERT INTO paper_events (event_uid, event_type, occurred_at, "
                "recorded_at, actor_kind, actor_role, actor_name, run_id, "
                "run_marker, payload_json, paper_id, to_state) VALUES "
                "(?, ?, 't', 't', 'engine', 'system', 'x', NULL, "
                "'pre-manifest', '{}', 1, 'eligible')",
                (str(uuid.uuid4()), event_type),
            )
    conn.close()


def test_T3_paper_events_accepts_screened_to_eligible_under_a_run(fresh):
    from engine.core.events import write_paper_event

    conn = sqlite3.connect(str(fresh))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("INSERT INTO papers (id, title, source, created_at, updated_at) VALUES (1, 't', 's', 't', 't')")
    run_id = _open_test_run(conn, tmp_dir=fresh.parent)

    write_paper_event(
        conn, event_type="screened", paper_id=1, to_state="eligible", from_state=None,
        actor_kind="model", actor_role="extractor", actor_name="qwen3:8b",
        run_id=run_id,
    )
    row = conn.execute(
        "SELECT event_type, to_state FROM paper_events WHERE paper_id = 1"
    ).fetchone()
    assert tuple(row) == ("screened", "eligible")
    conn.close()


def _open_test_run(conn, *, tmp_dir):
    """A raw-SQL 'import'-kind run for tests that only need a valid run_id to
    hang a claim/verdict/call off of — not exercising open_run() itself."""
    conn.execute(
        "INSERT INTO run_manifests (run_uid, review_id, run_kind, git_commit, "
        "git_dirty, spec_hash, codebook_hash, codebook_sha256, "
        "library_versions_json, host, started_at, manifest_json, manifest_sha256) "
        "VALUES (?, 'r', 'import', ?, 0, 'h', 'h', 'h', '{}', 'h', 't', '{}', 'h')",
        (str(uuid.uuid4()), "0" * 40),
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


# ── T4 ──────────────────────────────────────────────────────────────


def test_T4_run_manifests_accepts_import_kind(fresh):
    conn = sqlite3.connect(str(fresh))
    run_id = _open_test_run(conn, tmp_dir=fresh.parent)
    row = conn.execute(
        "SELECT run_kind FROM run_manifests WHERE run_id = ?", (run_id,)
    ).fetchone()
    assert row[0] == "import"
    conn.close()


LIVE_CODEBOOK = Path("data/surgical_autonomy/extraction_codebook.yaml")
CLEAN_GIT = rm.GitState(commit="b" * 40, dirty=False, tag=None)


def test_T4_open_run_with_kind_import_succeeds(tmp_path):
    """R214: open_run(kind='import', stages=()) — the review_session precedent,
    no stage rows required. Uses the live spec/codebook, the established
    fixture pattern (tests/test_run_manifest.py)."""
    import shutil
    from engine.core.codebook import load_codebook
    from engine.core.review_paths import load_spec_for

    db = ReviewDatabase("m022_t4", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    codebook = load_codebook(Path(db.db_path).parent / "extraction_codebook.yaml")
    spec = load_spec_for("surgical_autonomy")

    handle = rm.open_run(db._conn, spec, kind="import", stages=(), codebook=codebook,
                         git=CLEAN_GIT, digest_fn=lambda m: "a" * 64)
    row = db._conn.execute(
        "SELECT run_kind FROM run_manifests WHERE run_id = ?", (handle.run_id,)
    ).fetchone()
    assert row[0] == "import"
    n_stages = db._conn.execute(
        "SELECT COUNT(*) FROM run_stage_configs WHERE run_id = ?", (handle.run_id,)
    ).fetchone()[0]
    assert n_stages == 0, "an import run declares no stages, like review_session"
    db.close()


# ── T5 ──────────────────────────────────────────────────────────────


def test_T5_aborted_with_reason_accepted(fresh):
    conn = sqlite3.connect(str(fresh))
    run_id = _open_test_run(conn, tmp_dir=fresh.parent)
    conn.execute(
        "UPDATE run_manifests SET ended_at = 't', end_status = 'aborted', "
        "end_reason = 'three consecutive extraction_failed papers' WHERE run_id = ?",
        (run_id,))
    conn.close()


def test_T5_aborted_without_reason_refused(fresh):
    conn = sqlite3.connect(str(fresh))
    run_id = _open_test_run(conn, tmp_dir=fresh.parent)
    with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint"):
        conn.execute(
            "UPDATE run_manifests SET ended_at = 't', end_status = 'aborted' "
            "WHERE run_id = ?", (run_id,))
    conn.close()


def test_T5_completed_with_reason_refused(fresh):
    conn = sqlite3.connect(str(fresh))
    run_id = _open_test_run(conn, tmp_dir=fresh.parent)
    with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint"):
        conn.execute(
            "UPDATE run_manifests SET ended_at = 't', end_status = 'completed', "
            "end_reason = 'should not be allowed' WHERE run_id = ?", (run_id,))
    conn.close()


def test_T5_failed_with_and_without_reason_both_accepted(fresh):
    conn = sqlite3.connect(str(fresh))
    r1 = _open_test_run(conn, tmp_dir=fresh.parent)
    conn.execute(
        "UPDATE run_manifests SET ended_at = 't', end_status = 'failed' "
        "WHERE run_id = ?", (r1,))
    r2 = _open_test_run(conn, tmp_dir=fresh.parent)
    conn.execute(
        "UPDATE run_manifests SET ended_at = 't', end_status = 'failed', "
        "end_reason = 'ollama timeout' WHERE run_id = ?", (r2,))
    conn.close()


def test_T5_the_one_shot_trigger_permits_the_triple_together_and_refuses_a_second_close(fresh):
    conn = sqlite3.connect(str(fresh))
    run_id = _open_test_run(conn, tmp_dir=fresh.parent)
    conn.execute(
        "UPDATE run_manifests SET ended_at = 't1', end_status = 'aborted', "
        "end_reason = 'r' WHERE run_id = ?", (run_id,))
    with pytest.raises(sqlite3.IntegrityError, match="never edited"):
        conn.execute(
            "UPDATE run_manifests SET ended_at = 't2', end_status = 'failed', "
            "end_reason = 'r2' WHERE run_id = ?", (run_id,))
    conn.close()


# ── T5b (10b-C1, R79) ───────────────────────────────────────────────


def test_T5b_open_run_with_a_reason_refused(fresh):
    """The NULL reproducer (Step 4 rule 11; 10b-P3i finding 3): with end_status
    NULL, `end_status IS NOT 'completed' OR …` was TRUE, so an OPEN run could
    carry an end_reason. The permitted-states CHECK refuses it."""
    conn = sqlite3.connect(str(fresh))
    run_id = _open_test_run(conn, tmp_dir=fresh.parent)
    with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint"):
        conn.execute(
            "UPDATE run_manifests SET end_reason = 'x' WHERE run_id = ?", (run_id,))
    conn.close()


@pytest.mark.parametrize("end_status, end_reason", [
    ("completed", None),
    ("aborted", "r"),
    ("failed", None),
    ("failed", "r"),
    ("interrupted", None),
    ("interrupted", "r"),
])
def test_T5b_permitted_close_states_accepted(fresh, end_status, end_reason):
    conn = sqlite3.connect(str(fresh))
    run_id = _open_test_run(conn, tmp_dir=fresh.parent)
    conn.execute(
        "UPDATE run_manifests SET ended_at = 't', end_status = ?, end_reason = ? "
        "WHERE run_id = ?", (end_status, end_reason, run_id))
    row = conn.execute(
        "SELECT end_status, end_reason FROM run_manifests WHERE run_id = ?",
        (run_id,)).fetchone()
    assert row == (end_status, end_reason)
    conn.close()


def test_T5b_open_state_accepted(fresh):
    conn = sqlite3.connect(str(fresh))
    run_id = _open_test_run(conn, tmp_dir=fresh.parent)
    row = conn.execute(
        "SELECT end_status, ended_at, end_reason FROM run_manifests WHERE run_id = ?",
        (run_id,)).fetchone()
    assert row == (None, None, None)
    conn.close()


@pytest.mark.parametrize("end_status, end_reason", [
    ("completed", "r"),
    ("aborted", None),
])
def test_T5b_excluded_close_states_refused(fresh, end_status, end_reason):
    conn = sqlite3.connect(str(fresh))
    run_id = _open_test_run(conn, tmp_dir=fresh.parent)
    with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint"):
        conn.execute(
            "UPDATE run_manifests SET ended_at = 't', end_status = ?, end_reason = ? "
            "WHERE run_id = ?", (end_status, end_reason, run_id))
    conn.close()


# ── T6 ──────────────────────────────────────────────────────────────


def _linked_run_and_stage(conn):
    run_id = _open_test_run(conn, tmp_dir=None)
    digest = "a" * 64
    conn.execute(
        "INSERT INTO run_stage_configs (run_id, stage, stage_kind, provider, "
        "model_name, model_digest, options_json, options_hash, sent_keys_json, "
        "sources_json, keep_alive, format_schema_hash, prompt_hash) VALUES "
        "(?, 'audit', 'audit', 'ollama', 'm', ?, '{}', 'h', '[]', '{}', '-1', 'h', 'h')",
        (run_id, digest),
    )
    return run_id


def test_T6_run_calls_insert_without_outcome_refused(fresh):
    conn = sqlite3.connect(str(fresh))
    run_id = _linked_run_and_stage(conn)
    with pytest.raises(sqlite3.IntegrityError, match="NOT NULL"):
        conn.execute(
            "INSERT INTO run_calls (run_id, stage, request_hash, started_at, ended_at) "
            "VALUES (?, 'audit', 'h', 't', 't')", (run_id,))
    conn.close()


def test_T6_every_declared_outcome_token_is_accepted(fresh):
    conn = sqlite3.connect(str(fresh))
    run_id = _linked_run_and_stage(conn)
    for token in m022.RUN_CALL_OUTCOMES:
        conn.execute(
            "INSERT INTO run_calls (run_id, stage, request_hash, started_at, ended_at, "
            "outcome) VALUES (?, 'audit', 'h', 't', 't', ?)", (run_id, token))
    conn.close()


def test_T6_an_unlisted_outcome_token_is_refused(fresh):
    conn = sqlite3.connect(str(fresh))
    run_id = _linked_run_and_stage(conn)
    with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint"):
        conn.execute(
            "INSERT INTO run_calls (run_id, stage, request_hash, started_at, ended_at, "
            "outcome) VALUES (?, 'audit', 'h', 't', 't', 'not_a_real_outcome')",
            (run_id,))
    conn.close()


def test_T6_record_call_on_a_fixture_writes_outcome_completed(fresh):
    """10a-C3 note: `outcome` is now required (no default) on `record_call`;
    this still writes 'completed' — the caller names it explicitly, same as
    every other caller (`ollama_chat`'s recorder, cloud's `send()`)."""
    conn = sqlite3.connect(str(fresh))
    run_id = _linked_run_and_stage(conn)
    call_id = rm.record_call(
        conn, run_id, "audit", None, {"model": "m"}, "digest", "t0", "t1",
        outcome="completed")
    row = conn.execute(
        "SELECT outcome, outcome_detail FROM run_calls WHERE call_id = ?", (call_id,)
    ).fetchone()
    assert row == ("completed", None)
    conn.close()


# ── T7 ──────────────────────────────────────────────────────────────


def test_T7_claim_inputs_duplicate_extraction_uid_refused(fresh):
    conn = sqlite3.connect(str(fresh))
    conn.execute("INSERT INTO papers (id, title, source, created_at, updated_at) VALUES (1, 't', 's', 't', 't')")
    conn.execute(
        "INSERT INTO arms (arm_name, arm_kind, registered_at) "
        "VALUES ('a1', 'model', 't')")
    run_id = _open_test_run(conn, tmp_dir=None)
    row = ("euid-1", "a1", 1, "rk", "h" * 64, "puid-1", run_id, "t")
    conn.execute(
        "INSERT INTO claim_inputs (extraction_uid, arm, paper_id, reuse_key, "
        "parsed_text_sha256, parsed_text_uid, run_id, recorded_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)", row)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO claim_inputs (extraction_uid, arm, paper_id, reuse_key, "
            "parsed_text_sha256, parsed_text_uid, run_id, recorded_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)", row)
    conn.close()


def test_T7_claim_inputs_append_only_triggers_fire(fresh):
    conn = sqlite3.connect(str(fresh))
    conn.execute("INSERT INTO papers (id, title, source, created_at, updated_at) VALUES (1, 't', 's', 't', 't')")
    conn.execute(
        "INSERT INTO arms (arm_name, arm_kind, registered_at) "
        "VALUES ('a1', 'model', 't')")
    run_id = _open_test_run(conn, tmp_dir=None)
    conn.execute(
        "INSERT INTO claim_inputs (extraction_uid, arm, paper_id, reuse_key, "
        "parsed_text_sha256, parsed_text_uid, run_id, recorded_at) "
        "VALUES ('euid-1', 'a1', 1, 'rk', ?, 'puid-1', ?, 't')", ("h" * 64, run_id))
    with pytest.raises(sqlite3.IntegrityError, match="never edited"):
        conn.execute("UPDATE claim_inputs SET reuse_key = 'x' WHERE extraction_uid = 'euid-1'")
    with pytest.raises(sqlite3.IntegrityError, match="never edited"):
        conn.execute("DELETE FROM claim_inputs WHERE extraction_uid = 'euid-1'")
    conn.close()


def test_T7_claim_inputs_fk_to_papers_enforced(fresh):
    """`PRAGMA foreign_keys` is OFF on the migration's own connection (needed
    for the rebuilds) and ON on every `ReviewDatabase` connection
    (`database.py`'s `__init__`) — this test opens the latter, which is what
    a real writer would use."""
    db = ReviewDatabase("m022_t7fk", data_root=fresh.parent.parent / "t7fk")
    conn = db._conn
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    conn.execute(
        "INSERT INTO arms (arm_name, arm_kind, registered_at) "
        "VALUES ('a1', 'model', 't')")
    run_id = _open_test_run(conn, tmp_dir=None)
    with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
        conn.execute(
            "INSERT INTO claim_inputs (extraction_uid, arm, paper_id, reuse_key, "
            "parsed_text_sha256, parsed_text_uid, run_id, recorded_at) "
            "VALUES ('euid-1', 'a1', 999999, 'rk', ?, 'puid-1', ?, 't')",
            ("h" * 64, run_id))
    db.close()


# ── T8 ──────────────────────────────────────────────────────────────


def _seed_audit_verdict_deps(conn):
    conn.execute("INSERT INTO papers (id, title, source, created_at, updated_at) VALUES (1, 't', 's', 't', 't')")
    run_id = _open_test_run(conn, tmp_dir=None)
    return run_id


def test_T8_verdict_outside_the_closed_set_is_refused(fresh):
    conn = sqlite3.connect(str(fresh))
    run_id = _seed_audit_verdict_deps(conn)
    with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint"):
        conn.execute(
            "INSERT INTO audit_verdicts (schema, run_id, paper_id, claim_id, "
            "field_name, arm, auditor_model, auditor_digest, verdict, occurred_at) "
            "VALUES ('audit-telemetry-1', ?, 1, 'c1', 'f1', 'a1', 'gemma3:27b', "
            "'d', 'contested', 't')", (run_id,))
    conn.close()


def test_T8_verified_and_flagged_are_accepted(fresh):
    conn = sqlite3.connect(str(fresh))
    run_id = _seed_audit_verdict_deps(conn)
    for verdict in ("verified", "flagged"):
        conn.execute(
            "INSERT INTO audit_verdicts (schema, run_id, paper_id, claim_id, "
            "field_name, arm, auditor_model, auditor_digest, verdict, occurred_at) "
            "VALUES ('audit-telemetry-1', ?, 1, ?, 'f1', 'a1', 'gemma3:27b', "
            "'d', ?, 't')", (run_id, f"c-{verdict}", verdict))
    conn.close()


def test_T8_append_only_triggers_fire(fresh):
    conn = sqlite3.connect(str(fresh))
    run_id = _seed_audit_verdict_deps(conn)
    conn.execute(
        "INSERT INTO audit_verdicts (schema, run_id, paper_id, claim_id, "
        "field_name, arm, auditor_model, auditor_digest, verdict, occurred_at) "
        "VALUES ('audit-telemetry-1', ?, 1, 'c1', 'f1', 'a1', 'gemma3:27b', "
        "'d', 'verified', 't')", (run_id,))
    with pytest.raises(sqlite3.IntegrityError, match="never edited"):
        conn.execute("UPDATE audit_verdicts SET verdict = 'flagged' WHERE claim_id = 'c1'")
    with pytest.raises(sqlite3.IntegrityError, match="never edited"):
        conn.execute("DELETE FROM audit_verdicts WHERE claim_id = 'c1'")
    conn.close()


# ── T9 ──────────────────────────────────────────────────────────────


def test_T9_structure_differences_name_exactly_the_touched_tables(tmp_path):
    """The 10b G3 rehearsal's fresh half: fresh-before-022 vs fresh-after-022
    differ in exactly the tables 022 touches, nothing else."""
    real_discover = runner.discover
    runner.discover = lambda: [
        (m, p) for m, p in real_discover()
        if m != "022_run_kinds_and_audit_tables"
    ]
    try:
        before_db = ReviewDatabase("m022_before", data_root=tmp_path / "before")
        before_conn = before_db._conn
        before_struct = structure(before_conn)
        before_db.close()
    finally:
        runner.discover = real_discover

    after_db = ReviewDatabase("m022_after", data_root=tmp_path / "after")
    after_conn = after_db._conn
    after_struct = structure(after_conn)
    after_db.close()

    diffs = structure_differences(
        before_struct, after_struct, left_label="before", right_label="after")
    # New-table lines read: table 'claim_inputs': absent in before
    named = set()
    for d in diffs:
        if d.startswith("table '"):
            named.add(d.split("'")[1])
        else:
            named.add(d.split(".")[0])
    # paper_events is DELIBERATELY absent: 022's only change to it is CHECK
    # text (identified/duplicate_of removed), and `structure()` captures only
    # columns, indices and foreign keys — never CHECK constraints (the same
    # property the 6b closeout recorded for 019's to_state CHECK: "the
    # structure hash cannot see a CHECK"). run_manifests and run_calls DO
    # appear because they gain real columns (end_reason; outcome,
    # outcome_detail); claim_inputs and audit_verdicts are new tables.
    assert named == {"run_manifests", "run_calls",
                     "claim_inputs", "audit_verdicts"}, sorted(named)
