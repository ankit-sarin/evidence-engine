"""The migration runner and its receipts (MIGRATIONS-01, R2).

The live database's schema hash is deliberately NOT pinned here. It is meant to
change at the next migration, and a test asserting it would have to be edited
every time the database changed by design. The live comparison is a measured
step in the session report; the standing check is the fingerprint CLI's
`--compare` at session open and close.
"""

import sqlite3
import tempfile
from pathlib import Path

import pytest

from engine.core.database import ReviewDatabase
from engine.migrations import runner
from engine.tools.db_fingerprint import fingerprint, structure_differences


@pytest.fixture
def fresh(tmp_path):
    """A database built the way production builds one."""
    db = ReviewDatabase("scratch_review", data_root=tmp_path)
    path = db.db_path
    db._conn.close()
    return path


def _receipts(path):
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return [dict(zip(("id", "sha", "at", "mode", "ver", "note"), r))
                for r in conn.execute(
                    "SELECT migration_id, file_sha256, applied_at, mode, "
                    "runner_version, note FROM schema_migrations "
                    "ORDER BY migration_id")]
    finally:
        conn.close()


# ── The point: two fresh databases are the same database ────────────


def test_two_fresh_databases_have_identical_schema(tmp_path):
    a = ReviewDatabase("scratch_review", data_root=tmp_path / "a")
    b = ReviewDatabase("scratch_review", data_root=tmp_path / "b")
    pa, pb = a.db_path, b.db_path
    a._conn.close(); b._conn.close()

    fa, fb = fingerprint(pa), fingerprint(pb)
    assert structure_differences(fa["structure"], fb["structure"]) == []
    assert fa["schema_structure_hash"] == fb["schema_structure_hash"]
    assert fa["schema_hash_sha256"] == fb["schema_hash_sha256"], (
        "two fresh databases should not even differ textually"
    )


# ── Receipts ────────────────────────────────────────────────────────


def test_receipts_list_every_executed_migration_in_order(fresh):
    recs = _receipts(fresh)
    ids = [r["id"] for r in recs]

    schema_ids = [m for m, _ in runner.discover() if runner.kind_of(m) == "schema"]
    assert ids == sorted(schema_ids), "every schema migration, in numeric order"
    assert all(r["mode"] == "executed" for r in recs)
    assert all(len(r["sha"]) == 64 for r in recs)
    assert all(r["ver"] == runner.RUNNER_VERSION for r in recs)


def test_a_data_migration_is_not_executed_on_a_fresh_database(fresh):
    ids = {r["id"] for r in _receipts(fresh)}
    data_ids = [m for m, _ in runner.discover() if runner.kind_of(m) == "data"]

    assert data_ids, "the fixture is worthless if no migration is classified data"
    for m in data_ids:
        assert m not in ids, (
            f"{m} is a data migration: 003 would import one review's corpus into "
            f"another review's database, and 002 renames rows a fresh database "
            f"does not have"
        )

    result = runner.run(fresh)
    assert set(result["skipped"]) == set(data_ids)


def test_every_numbered_migration_has_a_declared_kind():
    for migration_id, _ in runner.discover():
        assert runner.kind_of(migration_id) in ("schema", "data")


def test_an_undeclared_migration_refuses(monkeypatch):
    monkeypatch.setitem(runner.KINDS, "004", runner.KINDS["004"])
    kinds = dict(runner.KINDS)
    kinds.pop("004")
    monkeypatch.setattr(runner, "KINDS", kinds)
    with pytest.raises(runner.MigrationError, match="no declared kind"):
        runner.discover()


# ── Idempotence ─────────────────────────────────────────────────────


def test_re_running_is_a_no_op_with_no_new_receipts(fresh):
    before = _receipts(fresh)
    result = runner.run(fresh)
    after = _receipts(fresh)

    assert result["executed"] == [], "nothing should run a second time"
    assert before == after, "not one receipt may change on a re-run"

    fp_a = fingerprint(fresh)
    runner.run(fresh)
    fp_b = fingerprint(fresh)
    assert fp_a["schema_structure_hash"] == fp_b["schema_structure_hash"]


def test_constructing_the_same_database_twice_runs_nothing_new(tmp_path):
    first = ReviewDatabase("scratch_review", data_root=tmp_path)
    path = first.db_path
    before = _receipts(path)
    first._conn.close()

    second = ReviewDatabase("scratch_review", data_root=tmp_path)
    second._conn.close()
    assert _receipts(path) == before


# ── Drift ───────────────────────────────────────────────────────────


def test_a_migration_edited_after_its_receipt_makes_the_runner_refuse(fresh):
    conn = sqlite3.connect(str(fresh))
    conn.execute(
        "UPDATE schema_migrations SET file_sha256 = ? WHERE migration_id = ?",
        ("0" * 64, "007_add_judge_tables"),
    )
    conn.commit()
    conn.close()

    assert runner.check_drift(sqlite3.connect(str(fresh))) == ["007_add_judge_tables"]

    with pytest.raises(runner.MigrationDrift) as exc:
        runner.run(fresh)
    assert "007_add_judge_tables" in str(exc.value)
    assert "different migration" in str(exc.value)


def test_drift_names_every_drifted_migration_not_just_the_first(fresh):
    conn = sqlite3.connect(str(fresh))
    conn.execute("UPDATE schema_migrations SET file_sha256 = ?", ("0" * 64,))
    conn.commit()
    conn.close()

    with pytest.raises(runner.MigrationDrift) as exc:
        runner.run(fresh)
    message = str(exc.value)
    for migration_id, _ in runner.discover():
        if runner.kind_of(migration_id) == "schema":
            assert migration_id in message


# ── Pre-applied registration ────────────────────────────────────────


def test_register_preapplied_writes_receipts_without_executing(tmp_path):
    path = tmp_path / "bare.db"
    sqlite3.connect(str(path)).close()

    registered = runner.register_preapplied(path, note="test")
    assert set(registered) == {m for m, _ in runner.discover()}

    recs = _receipts(path)
    assert all(r["mode"] == "registered_preapplied" for r in recs)
    assert all(r["note"] == "test" for r in recs)

    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    conn.close()
    assert tables == {"schema_migrations"}, (
        "registration asserts; it must not create anything"
    )

    assert runner.register_preapplied(path) == [], "idempotent"


def test_registering_an_unknown_migration_refuses(tmp_path):
    path = tmp_path / "bare.db"
    sqlite3.connect(str(path)).close()
    with pytest.raises(runner.MigrationError, match="no such migration"):
        runner.register_preapplied(path, ["999_not_a_migration"])


# ── The two migrations this session added ───────────────────────────


def test_cloud_tables_are_in_the_schema_a_fresh_database_builds(fresh):
    conn = sqlite3.connect(f"file:{fresh}?mode=ro", uri=True)
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    conn.close()
    assert {"cloud_extractions", "cloud_evidence_spans"} <= tables, (
        "before 014 these existed only after a cloud run, so a fresh database "
        "had 20 tables where the live one had 24"
    )


def test_only_the_post_rename_adjudication_indices_exist(fresh):
    conn = sqlite3.connect(f"file:{fresh}?mode=ro", uri=True)
    idx = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' "
        "AND tbl_name='abstract_screening_adjudication'")}
    conn.close()
    assert "idx_abstract_adjudication_paper" in idx
    for pre_rename in ("idx_adjudication_paper", "idx_adjudication_ext_key",
                       "idx_adjudication_decision"):
        assert pre_rename not in idx, (
            "ensure_adjudication_table recreated the pre-002 names on every "
            "construction, so the live database carried six indices on three "
            "columns"
        )


def test_015_refuses_to_drop_the_only_index_on_a_column(tmp_path):
    """Half a rename must not become no index at all."""
    import importlib
    mod = importlib.import_module(
        "engine.migrations.015_drop_prerename_adjudication_indices")

    path = tmp_path / "half.db"
    conn = sqlite3.connect(str(path))
    conn.executescript("""
        CREATE TABLE abstract_screening_adjudication (
            id INTEGER PRIMARY KEY, paper_id INTEGER,
            external_key TEXT, adjudication_decision TEXT);
        CREATE INDEX idx_adjudication_paper
            ON abstract_screening_adjudication(paper_id);
    """)
    conn.commit(); conn.close()

    with pytest.raises(RuntimeError, match="only indices on their columns"):
        mod.run_migration(str(path))

    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    still = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'")}
    conn.close()
    assert "idx_adjudication_paper" in still, "the index must survive the refusal"


def test_014_is_idempotent(fresh):
    import importlib
    mod = importlib.import_module("engine.migrations.014_cloud_tables")
    before = fingerprint(fresh)["schema_structure_hash"]
    result = mod.run_migration(str(fresh))
    assert result["created"] == []
    assert fingerprint(fresh)["schema_structure_hash"] == before
