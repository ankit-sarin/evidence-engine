"""Migration 020 — the run manifest, run linkage and arm pinning (T3, G2).

MANIFEST-01 Phase 2a. What is asserted here is the EFFECT on a database, never
that a file mentions a name:

* a fresh `ReviewDatabase` carries 020 with an `executed` receipt, and a second
  runner pass executes nothing (`already`);
* the rebuilt event tables keep their rows, rowids, values, column order and
  AUTOINCREMENT high-water mark, and their append-only triggers;
* a forced failure after both rebuilds rolls everything back;
* **every CHECK 020 declares refuses a row carrying NULL in each column it names**
  (R78, R79 — SQLite passes a CHECK that evaluates to NULL), and R77's run-link
  CHECK rejects its four excluded states on both event tables;
* the token lists 020 re-declares agree with their owners (R35).
"""

from __future__ import annotations

import importlib
import sqlite3
from pathlib import Path

import pytest

from engine.core import effective_config, events, run_manifest
from engine.core.database import ReviewDatabase
from engine.migrations import runner

m016 = importlib.import_module("engine.migrations.016_event_store")
m019 = importlib.import_module("engine.migrations.019_paper_state_axes")
m020 = importlib.import_module("engine.migrations.020_run_manifest")

R77_CHECK = (
    "CHECK (\n"
    "            (run_id IS NOT NULL AND run_marker IS NULL)\n"
    "            OR\n"
    "            (run_id IS NULL AND run_marker IS 'pre-manifest')\n"
    "        )"
)


@pytest.fixture
def fresh(tmp_path) -> Path:
    db = ReviewDatabase("m020", data_root=tmp_path)
    path = Path(db.db_path)
    db.close()
    return path


def _ro(path):
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def _rw(path):
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _run_row(conn, **over):
    row = dict(run_uid="u1", review_id="r", run_kind="extraction", git_commit="a" * 40,
               git_dirty=0, spec_hash="h", codebook_hash="h", codebook_sha256="h",
               library_versions_json="{}", host="h", started_at="t",
               manifest_json="{}", manifest_sha256="h")
    row.update(over)
    cols = ", ".join(row)
    return conn.execute(f"INSERT INTO run_manifests ({cols}) VALUES "
                        f"({', '.join('?' * len(row))})", tuple(row.values())).lastrowid


def _stage_row(conn, run_id, **over):
    row = dict(run_id=run_id, stage="extract_pass1", stage_kind="extract_pass1",
               provider="ollama", model_name="m", model_digest="d" * 64,
               options_json="{}", options_hash="h", sent_keys_json="[]",
               sources_json="{}", keep_alive="-1", format_schema_hash="none",
               prompt_hash="h")
    row.update(over)
    cols = ", ".join(row)
    conn.execute(f"INSERT INTO run_stage_configs ({cols}) VALUES "
                 f"({', '.join('?' * len(row))})", tuple(row.values()))


# ── G2: fresh database, receipt, idempotence ─────────────────────────
def test_a_fresh_database_carries_020_with_an_executed_receipt(fresh):
    conn = _ro(fresh)
    mode, sha = conn.execute(
        "SELECT mode, file_sha256 FROM schema_migrations WHERE migration_id = "
        "'020_run_manifest'").fetchone()
    assert mode == "executed"
    assert sha == runner.file_sha256(Path(m020.__file__))
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"run_manifests", "run_stage_configs", "run_calls"} <= tables
    fks = {r[2] for r in conn.execute("PRAGMA foreign_key_list(paper_events)")}
    assert "run_manifests" in fks
    assert "run_manifests" in {r[2] for r in conn.execute("PRAGMA foreign_key_list(field_events)")}
    arm_cols = [r[1] for r in conn.execute("PRAGMA table_info(arms)")]
    assert arm_cols[-2:] == ["pinned_run_id", "pinned_sha256"]


def test_a_second_pass_executes_nothing(fresh):
    result = runner.run(fresh)
    assert result["executed"] == []
    assert "020_run_manifest" in result["already"]
    assert m020.run_migration(str(fresh))["status"] == "already_applied"


def test_the_r77_check_text_is_on_both_event_tables_verbatim(fresh):
    """The 2b postcondition read-back, run here first on a fresh database."""
    conn = _ro(fresh)
    for table in ("field_events", "paper_events"):
        sql = conn.execute("SELECT sql FROM sqlite_master WHERE name = ?", (table,)).fetchone()[0]
        assert R77_CHECK in sql, table


@pytest.mark.parametrize("table", ["field_events", "paper_events", "run_manifests",
                                   "run_stage_configs", "run_calls"])
def test_append_only_or_record_only_triggers_are_present(fresh, table):
    names = {r[0] for r in _ro(fresh).execute(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name = ?", (table,))}
    assert names, table
    assert any(n.endswith("_no_delete") for n in names)


def test_the_arms_trigger_is_widened_and_the_old_one_gone(fresh):
    conn = _ro(fresh)
    names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'")}
    assert m020.ARMS_TRIGGER in names and m020.OLD_ARMS_TRIGGER not in names
    sql = conn.execute("SELECT sql FROM sqlite_master WHERE name = ?",
                       (m020.ARMS_TRIGGER,)).fetchone()[0]
    assert "'pinned'" in sql and "'not recorded (pre-manifest)'" in sql


# ── Rows preserved through the rebuild ───────────────────────────────
def _pre_020(tmp_path) -> Path:
    """A database at the post-019 shape holding seeded rows, as live does."""
    path = tmp_path / "pre020.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE papers (id INTEGER PRIMARY KEY)")
    conn.execute("CREATE TABLE evidence_spans (id INTEGER PRIMARY KEY)")
    conn.executemany("INSERT INTO papers VALUES (?)", [(i,) for i in range(1, 6)])
    m016.create_schema(conn)
    conn.commit()
    conn.close()
    m019.run_migration(str(path))
    conn = sqlite3.connect(path)
    events.register_arm(conn, "local", "model", configuration_marker=events.PRE_MANIFEST)
    for pid in (1, 2, 4):
        conn.execute(
            "INSERT INTO paper_events (event_uid, event_type, occurred_at, recorded_at, "
            "actor_kind, actor_role, actor_name, run_id, run_marker, payload_json, paper_id, "
            "to_state) VALUES (?, 'state_at_migration', 'o', 'r', 'engine', 'system', "
            "'017_seed_event_store', NULL, 'pre-manifest', '{\"n\": 1}', ?, 'eligible')",
            (f"uid-{pid}", pid))
    conn.execute("UPDATE sqlite_sequence SET seq = 40 WHERE name = 'paper_events'")
    conn.commit()
    conn.close()
    return path


def test_the_rebuild_preserves_rowids_values_marker_and_high_water_mark(tmp_path):
    path = _pre_020(tmp_path)
    before = _ro(path).execute("SELECT * FROM paper_events ORDER BY event_id").fetchall()
    cols_before = [r[1] for r in _ro(path).execute("PRAGMA table_info(paper_events)")]
    assert m020.run_migration(str(path))["rows_preserved"] == {"field_events": 0,
                                                                "paper_events": 3}
    conn = _ro(path)
    assert conn.execute("SELECT * FROM paper_events ORDER BY event_id").fetchall() == before
    assert [r[1] for r in conn.execute("PRAGMA table_info(paper_events)")] == cols_before
    assert conn.execute("SELECT seq FROM sqlite_sequence WHERE name='paper_events'"
                        ).fetchone()[0] == 40
    assert {r[0] for r in conn.execute("SELECT run_marker FROM paper_events")} == {"pre-manifest"}
    assert conn.execute("SELECT configuration_marker, configuration_json FROM arms"
                        ).fetchone() == (events.PRE_MANIFEST, "{}")


def test_a_forced_failure_after_both_rebuilds_rolls_everything_back(tmp_path):
    path = _pre_020(tmp_path)
    schema_before = _ro(path).execute(
        "SELECT type, name, sql FROM sqlite_master ORDER BY type, name").fetchall()
    with pytest.raises(RuntimeError, match="forced"):
        m020.run_migration(str(path), _fail_after_copy=True)
    assert _ro(path).execute(
        "SELECT type, name, sql FROM sqlite_master ORDER BY type, name").fetchall() == schema_before
    assert m020.run_migration(str(path))["status"] == "executed"


def test_020_refuses_a_runless_row_that_is_not_marked_pre_manifest(tmp_path):
    path = _pre_020(tmp_path)
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO paper_events (event_uid, event_type, occurred_at, recorded_at, "
        "actor_kind, actor_role, actor_name, paper_id, to_state) VALUES "
        "('x', 'state_at_migration', 'o', 'r', 'engine', 'system', 'n', 5, 'eligible')")
    conn.commit()
    conn.close()
    with pytest.raises(RuntimeError, match="020 refuses"):
        m020.run_migration(str(path))
    assert "run_manifests" not in {r[0] for r in _ro(path).execute(
        "SELECT name FROM sqlite_master")}


# ── R77: the four rejected states, on both event tables ──────────────
def _event_insert(table, run_id, marker):
    if table == "paper_events":
        return ("INSERT INTO paper_events (event_uid, event_type, occurred_at, recorded_at, "
                "actor_kind, actor_role, actor_name, run_id, run_marker, paper_id, to_state) "
                "VALUES (?, 'extracted', 'o', 'r', 'engine', 'system', 'n', ?, ?, 1, "
                "'extracted')", (f"e-{run_id}-{marker}", run_id, marker))
    return ("INSERT INTO field_events (event_uid, event_type, occurred_at, recorded_at, "
            "actor_kind, actor_role, actor_name, run_id, run_marker, claim_id, paper_id, "
            "field_name, arm) VALUES (?, 'asserted', 'o', 'r', 'model', 'extractor', 'n', "
            "?, ?, 'c', 1, 'f', 'a')", (f"e-{run_id}-{marker}", run_id, marker))


@pytest.fixture
def linked(fresh):
    conn = _rw(fresh)
    conn.execute("INSERT INTO papers (id, title, source, status, created_at, updated_at) "
                 "VALUES (1, 't', 's', 'EXTRACTED', 'x', 'x')")
    events.register_arm(conn, "a", "model")
    run_id = _run_row(conn)
    conn.commit()
    return conn, run_id


@pytest.mark.parametrize("table", ["field_events", "paper_events"])
@pytest.mark.parametrize("case", [
    "null_and_null", "run_with_pre_manifest_marker", "run_with_other_marker",
    "no_run_other_marker"])
def test_r77_rejects_each_excluded_state(linked, table, case):
    conn, run_id = linked
    rid, marker = {
        "null_and_null": (None, None),
        "run_with_pre_manifest_marker": (run_id, "pre-manifest"),
        "run_with_other_marker": (run_id, "legacy"),
        "no_run_other_marker": (None, "legacy"),
    }[case]
    sql, params = _event_insert(table, rid, marker)
    with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
        conn.execute(sql, params)


@pytest.mark.parametrize("table", ["field_events", "paper_events"])
def test_r77_admits_exactly_its_two_states(linked, table):
    conn, run_id = linked
    for rid, marker in ((run_id, None), (None, "pre-manifest")):
        sql, params = _event_insert(table, rid, marker)
        conn.execute(sql, params)


# ── R78: every 020 CHECK, with a NULL in every column it names ──────
# (table, column set to NULL, extra overrides) — each row must be REFUSED.
NULL_CASES = [
    ("run_manifests", "run_kind", {}),
    ("run_manifests", "git_commit", {}),
    ("run_manifests", "git_dirty", {}),
    ("run_manifests", "cloud_arms_json", {}),
    # the end CHECK names ended_at and end_status: one NULL, the other set
    ("run_manifests", "ended_at", {"end_status": "completed"}),
    ("run_manifests", "end_status", {"ended_at": "t"}),
    # the cloud CHECK names cloud_arms_json and payload_description
    ("run_manifests", "payload_description", {"cloud_arms_json": '["x"]'}),
    ("run_stage_configs", "stage_kind", {}),
    ("run_stage_configs", "provider", {}),
    ("run_stage_configs", "model_digest", {}),       # an ollama stage with no digest
]


@pytest.mark.parametrize("table, column, extra", NULL_CASES,
                         ids=[f"{t}.{c}" for t, c, _ in NULL_CASES])
def test_r78_every_check_refuses_a_null(linked, table, column, extra):
    conn, run_id = linked
    with pytest.raises(sqlite3.IntegrityError):
        if table == "run_manifests":
            _run_row(conn, run_uid=f"n-{column}", **{column: None, **extra})
        else:
            _stage_row(conn, run_id, stage=f"s-{column}", **{column: None, **extra})


def test_r78_an_end_status_outside_the_vocabulary_is_refused(linked):
    conn, _ = linked
    with pytest.raises(sqlite3.IntegrityError):
        _run_row(conn, run_uid="bad-end", ended_at="t", end_status="blocked")


def test_r78_a_dirty_tree_row_is_refused_by_the_database_itself(linked):
    conn, _ = linked
    with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
        _run_row(conn, run_uid="dirty", git_dirty=1)


def test_the_manifest_body_is_immutable_and_its_end_is_written_once(linked):
    conn, run_id = linked
    with pytest.raises(sqlite3.IntegrityError, match="never edited"):
        conn.execute("UPDATE run_manifests SET spec_hash = 'other' WHERE run_id = ?", (run_id,))
    conn.execute("UPDATE run_manifests SET ended_at = 't', end_status = 'completed' "
                 "WHERE run_id = ?", (run_id,))
    with pytest.raises(sqlite3.IntegrityError, match="never edited"):
        conn.execute("UPDATE run_manifests SET end_status = 'failed' WHERE run_id = ?",
                     (run_id,))
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("DELETE FROM run_manifests WHERE run_id = ?", (run_id,))


def test_a_call_row_must_name_a_declared_stage(linked):
    conn, run_id = linked
    with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
        conn.execute("INSERT INTO run_calls (run_id, stage, request_hash, started_at, "
                     "ended_at) VALUES (?, 'undeclared', 'h', 't', 't')", (run_id,))


# ── R35: re-declared lists agree with their owners ───────────────────
def test_the_stage_vocabulary_agrees_with_the_resolver():
    assert m020.OLLAMA_STAGES == effective_config.OLLAMA_STAGES


def test_the_run_kinds_and_end_statuses_agree_with_run_manifest():
    assert m020.RUN_KINDS == run_manifest.RUN_KINDS
    assert m020.END_STATUSES == run_manifest.END_STATUSES
    assert m020.ARM_PINNED == run_manifest.ARM_PINNED


def test_the_markers_agree_with_the_writer_and_the_reader():
    assert m020.PRE_MANIFEST_MARKER == events.PRE_MANIFEST_MARKER
    assert m020.ARM_PRE_MANIFEST == events.PRE_MANIFEST


def test_the_event_vocabularies_agree_with_016_and_019():
    assert m020.FIELD_EVENT_TYPES == m016.FIELD_EVENT_TYPES
    for name in ("ELIGIBILITY_STATES", "PROCESSING_STATES", "FAILURE_STATES",
                 "PAPER_EVENT_TYPES", "EVENT_TYPE_AXIS"):
        assert getattr(m020, name) == getattr(m019, name), name


def test_020_imports_nothing_from_engine():
    src = Path(m020.__file__).read_text()
    assert "from engine" not in src and "import engine" not in src
