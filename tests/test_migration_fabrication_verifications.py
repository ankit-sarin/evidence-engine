"""Tests for engine/migrations/008_add_fabrication_verifications.py."""

from __future__ import annotations

import importlib
import sqlite3

import pytest

migration_007 = importlib.import_module(
    "engine.migrations.007_add_judge_tables"
)
migration_008 = importlib.import_module(
    "engine.migrations.008_add_fabrication_verifications"
)


@pytest.fixture()
def db(tmp_path):
    """Fresh SQLite with migrations 007 + 008 applied, FK enforcement ON."""
    db_path = tmp_path / "m008.db"
    migration_007.run_migration(str(db_path))
    migration_008.run_migration(str(db_path))
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    yield conn
    conn.close()


def _insert_run(conn, run_id="r1", pass_number=2):
    conn.execute(
        """INSERT INTO judge_runs
           (run_id, judge_model_name, judge_model_digest, codebook_sha256,
            pass_number, input_scope, started_at, completed_at,
            n_triples_attempted, n_triples_succeeded, n_triples_failed,
            run_config_json, notes)
           VALUES (?, 'gemma3:27b', 'sha256:abc', 'sha256:cb',
                   ?, 'AI_TRIPLES', '2026-04-21T12:00:00Z', NULL,
                   0, 0, 0, '{}', NULL)""",
        (run_id, pass_number),
    )
    conn.commit()


def _insert_verdict(conn, **overrides):
    row = dict(
        judge_run_id="r1",
        paper_id="1",
        field_name="study_design",
        arm_name="local",
        pre_check_short_circuit=0,
        verdict="SUPPORTED",
        verification_span=None,
        reasoning=None,
        fabrication_hypothesis=None,
        verified_at="2026-04-21T12:00:01Z",
    )
    row.update(overrides)
    conn.execute(
        """INSERT INTO fabrication_verifications
           (judge_run_id, paper_id, field_name, arm_name,
            pre_check_short_circuit, verdict,
            verification_span, reasoning, fabrication_hypothesis,
            verified_at)
           VALUES (:judge_run_id, :paper_id, :field_name, :arm_name,
                   :pre_check_short_circuit, :verdict,
                   :verification_span, :reasoning,
                   :fabrication_hypothesis, :verified_at)""",
        row,
    )
    conn.commit()


# ── schema presence ─────────────────────────────────────────────────


def test_migration_creates_table(db):
    names = {r[0] for r in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert "fabrication_verifications" in names


def test_migration_creates_indexes(db):
    names = {r[0] for r in db.execute(
        "SELECT name FROM sqlite_master WHERE type='index'"
    ).fetchall()}
    assert {"idx_fab_verif_run", "idx_fab_verif_run_paper_field",
            "idx_fab_verif_verdict"} <= names


def test_migration_is_idempotent(tmp_path):
    db_path = tmp_path / "idem.db"
    migration_007.run_migration(str(db_path))
    first = migration_008.run_migration(str(db_path))
    second = migration_008.run_migration(str(db_path))
    assert first["tables_created"] >= 1
    assert second["tables_created"] == 0
    assert second["indexes_created"] == 0


# ── CHECK constraints ──────────────────────────────────────────────


def test_verdict_rejects_unknown_label(db):
    _insert_run(db)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_verdict(db, verdict="MAYBE")


def test_short_circuit_rejects_non_binary(db):
    _insert_run(db)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_verdict(db, pre_check_short_circuit=2)


def test_unsupported_requires_reasoning(db):
    _insert_run(db)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_verdict(db, verdict="UNSUPPORTED",
                        reasoning=None,
                        fabrication_hypothesis="h")


def test_unsupported_requires_fabrication_hypothesis(db):
    _insert_run(db)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_verdict(db, verdict="UNSUPPORTED",
                        reasoning="r", fabrication_hypothesis=None)


def test_unsupported_rejects_empty_reasoning(db):
    _insert_run(db)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_verdict(db, verdict="UNSUPPORTED",
                        reasoning="   ", fabrication_hypothesis="h")


def test_unsupported_accepts_full_payload(db):
    _insert_run(db)
    _insert_verdict(db, verdict="UNSUPPORTED",
                    reasoning="invalid span",
                    fabrication_hypothesis="hallucinated default")


def test_supported_allows_null_reasoning(db):
    _insert_run(db)
    _insert_verdict(db, verdict="SUPPORTED", reasoning=None,
                    fabrication_hypothesis=None)


def test_partially_supported_allows_null_hypothesis(db):
    _insert_run(db)
    _insert_verdict(db, verdict="PARTIALLY_SUPPORTED", reasoning="r",
                    fabrication_hypothesis=None)


# ── FK + uniqueness ────────────────────────────────────────────────


def test_fk_to_judge_runs_enforced(db):
    with pytest.raises(sqlite3.IntegrityError):
        _insert_verdict(db, judge_run_id="ghost")


def test_fk_cascade_delete(db):
    _insert_run(db)
    _insert_verdict(db, arm_name="local")
    _insert_verdict(db, arm_name="openai_o4_mini_high")
    _insert_verdict(db, arm_name="anthropic_sonnet_4_6")
    db.execute("DELETE FROM judge_runs WHERE run_id = 'r1'")
    db.commit()
    n = db.execute(
        "SELECT COUNT(*) FROM fabrication_verifications"
    ).fetchone()[0]
    assert n == 0


def test_unique_per_run_paper_field_arm(db):
    _insert_run(db)
    _insert_verdict(db, arm_name="local")
    with pytest.raises(sqlite3.IntegrityError):
        _insert_verdict(db, arm_name="local")


def test_same_arm_allowed_across_different_runs(db):
    _insert_run(db, run_id="r1")
    _insert_run(db, run_id="r2")
    _insert_verdict(db, judge_run_id="r1", arm_name="local")
    _insert_verdict(db, judge_run_id="r2", arm_name="local")


def test_expected_row_count_for_one_triple(db):
    _insert_run(db)
    for arm in ("local", "openai_o4_mini_high", "anthropic_sonnet_4_6"):
        _insert_verdict(db, arm_name=arm)
    n = db.execute(
        "SELECT COUNT(*) FROM fabrication_verifications WHERE judge_run_id='r1'"
    ).fetchone()[0]
    assert n == 3
