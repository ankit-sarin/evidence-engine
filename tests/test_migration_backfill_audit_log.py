"""Tests for engine/migrations/009_add_backfill_audit_log.py."""

from __future__ import annotations

import importlib
import sqlite3

import pytest

migration_007 = importlib.import_module(
    "engine.migrations.007_add_judge_tables"
)
migration_009 = importlib.import_module(
    "engine.migrations.009_add_backfill_audit_log"
)


@pytest.fixture()
def db(tmp_path):
    """Fresh SQLite with migration 007 (judge_runs) + 009, FK ON."""
    db_path = tmp_path / "m009.db"
    migration_007.run_migration(str(db_path))
    migration_009.run_migration(str(db_path))
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    yield conn
    conn.close()


def _insert_run(conn, run_id="r1"):
    conn.execute(
        """INSERT INTO judge_runs
           (run_id, judge_model_name, judge_model_digest, codebook_sha256,
            pass_number, input_scope, started_at, completed_at,
            n_triples_attempted, n_triples_succeeded, n_triples_failed,
            run_config_json, notes)
           VALUES (?, 'gemma3:27b', 'sha256:abc', 'sha256:cb',
                   2, 'AI_TRIPLES', '2026-04-21T12:00:00Z', NULL,
                   0, 0, 0, '{}', NULL)""",
        (run_id,),
    )
    conn.commit()


def _insert_audit(conn, **overrides):
    row = dict(
        judge_run_id="r1",
        event_type="backfill_judge_model_digest",
        target_table="judge_runs",
        target_column="judge_model_digest",
        before_value="gemma3:27b",
        after_value="a" * 64,
        rationale="post-hoc correction",
        performed_at="2026-04-22T16:00:00Z",
        performed_by="system",
    )
    row.update(overrides)
    conn.execute(
        """INSERT INTO judge_run_audit
           (judge_run_id, event_type, target_table, target_column,
            before_value, after_value, rationale, performed_at, performed_by)
           VALUES (:judge_run_id, :event_type, :target_table, :target_column,
                   :before_value, :after_value, :rationale, :performed_at,
                   :performed_by)""",
        row,
    )
    conn.commit()


# ── Schema presence ─────────────────────────────────────────────────


def test_migration_applies_cleanly_on_fresh_db(tmp_path):
    db_path = tmp_path / "fresh.db"
    migration_007.run_migration(str(db_path))
    result = migration_009.run_migration(str(db_path))
    assert result["tables_created"] == 1
    assert result["indexes_created"] == 2

    conn = sqlite3.connect(str(db_path))
    try:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        indexes = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()}
    finally:
        conn.close()

    assert "judge_run_audit" in tables
    assert {"idx_judge_run_audit_run",
            "idx_judge_run_audit_event"} <= indexes


def test_migration_is_idempotent(tmp_path):
    db_path = tmp_path / "idem.db"
    migration_007.run_migration(str(db_path))
    first = migration_009.run_migration(str(db_path))
    second = migration_009.run_migration(str(db_path))
    assert first["tables_created"] == 1
    assert second["tables_created"] == 0
    assert second["indexes_created"] == 0


def test_rollback_drops_table_and_indexes(tmp_path):
    db_path = tmp_path / "rb.db"
    migration_007.run_migration(str(db_path))
    migration_009.run_migration(str(db_path))
    rb = migration_009.rollback(str(db_path))
    assert rb["tables_dropped"] == 1
    assert rb["indexes_dropped"] == 2

    conn = sqlite3.connect(str(db_path))
    try:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        indexes = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()}
    finally:
        conn.close()

    assert "judge_run_audit" not in tables
    assert "idx_judge_run_audit_run" not in indexes
    assert "idx_judge_run_audit_event" not in indexes


# ── Constraints ─────────────────────────────────────────────────────


def test_null_rationale_rejected(db):
    _insert_run(db)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_audit(db, rationale=None)


def test_empty_rationale_rejected(db):
    _insert_run(db)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_audit(db, rationale="   ")


def test_fk_to_nonexistent_judge_run_fails(db):
    # No judge_runs row created.
    with pytest.raises(sqlite3.IntegrityError):
        _insert_audit(db, judge_run_id="ghost")


def test_fk_cascade_delete(db):
    _insert_run(db)
    _insert_audit(db)
    _insert_audit(db, event_type="run_annotation",
                  before_value=None, after_value=None,
                  target_column=None,
                  rationale="note the thing")
    db.execute("DELETE FROM judge_runs WHERE run_id = 'r1'")
    db.commit()
    n = db.execute("SELECT COUNT(*) FROM judge_run_audit").fetchone()[0]
    assert n == 0


def test_defaults(db):
    _insert_run(db)
    db.execute(
        """INSERT INTO judge_run_audit
           (judge_run_id, event_type, target_table, rationale, performed_at)
           VALUES ('r1', 'run_annotation', 'judge_runs', 'note',
                   '2026-04-22T16:00:00Z')""",
    )
    db.commit()
    row = db.execute(
        "SELECT performed_by FROM judge_run_audit WHERE judge_run_id='r1'"
    ).fetchone()
    assert row[0] == "system"
