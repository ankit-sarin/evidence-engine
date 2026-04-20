"""Tests for engine/migrations/007_add_judge_tables.py."""

from __future__ import annotations

import importlib
import sqlite3

import pytest

migration_007 = importlib.import_module(
    "engine.migrations.007_add_judge_tables"
)
run_migration = migration_007.run_migration


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def fresh_db(tmp_path):
    """Fresh SQLite file with no prior tables."""
    db_path = tmp_path / "judge.db"
    run_migration(str(db_path))
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    yield conn
    conn.close()


def _table_names(conn):
    return {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }


def _index_names(conn):
    return {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
    }


def _insert_run(conn, run_id="r1", **overrides):
    defaults = dict(
        run_id=run_id,
        judge_model_name="gemma3:27b",
        judge_model_digest="sha256:abc",
        codebook_sha256="sha256:codebook",
        pass_number=1,
        input_scope="AI_TRIPLES",
        started_at="2026-04-20T12:00:00+00:00",
        completed_at=None,
        n_triples_attempted=0,
        n_triples_succeeded=0,
        n_triples_failed=0,
        run_config_json='{"arms":["a","b","c"]}',
        notes=None,
    )
    defaults.update(overrides)
    conn.execute(
        """INSERT INTO judge_runs
           (run_id, judge_model_name, judge_model_digest, codebook_sha256,
            pass_number, input_scope, started_at, completed_at,
            n_triples_attempted, n_triples_succeeded, n_triples_failed,
            run_config_json, notes)
           VALUES (:run_id, :judge_model_name, :judge_model_digest,
                   :codebook_sha256, :pass_number, :input_scope,
                   :started_at, :completed_at,
                   :n_triples_attempted, :n_triples_succeeded,
                   :n_triples_failed, :run_config_json, :notes)""",
        defaults,
    )
    conn.commit()


def _insert_rating(conn, run_id="r1", paper_id="EE-001",
                   field_name="study_design", **overrides):
    defaults = dict(
        run_id=run_id,
        paper_id=paper_id,
        field_name=field_name,
        field_type="categorical",
        seed=123456,
        arm_permutation_json='["a","b","c"]',
        prompt_hash="p" * 64,
        raw_response="{}",
        pass1_fabrication_risk="low",
        pass1_proposed_consensus="RCT",
        pass1_overall_rationale="match",
        created_at="2026-04-20T12:00:01+00:00",
    )
    defaults.update(overrides)
    cur = conn.execute(
        """INSERT INTO judge_ratings
           (run_id, paper_id, field_name, field_type, seed,
            arm_permutation_json, prompt_hash, raw_response,
            pass1_fabrication_risk, pass1_proposed_consensus,
            pass1_overall_rationale, created_at)
           VALUES (:run_id, :paper_id, :field_name, :field_type, :seed,
                   :arm_permutation_json, :prompt_hash, :raw_response,
                   :pass1_fabrication_risk, :pass1_proposed_consensus,
                   :pass1_overall_rationale, :created_at)""",
        defaults,
    )
    conn.commit()
    return cur.lastrowid


def _insert_pair(conn, rating_id, arm_a, arm_b, level1="EQUIVALENT",
                 level2=None, rationale="x"):
    conn.execute(
        """INSERT INTO judge_pair_ratings
           (rating_id, arm_a, arm_b, level1_rating, level2_type, rationale)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (rating_id, arm_a, arm_b, level1, level2, rationale),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------


def test_migration_creates_all_three_tables(fresh_db):
    tables = _table_names(fresh_db)
    assert {"judge_runs", "judge_ratings", "judge_pair_ratings"} <= tables


def test_migration_creates_all_five_indexes(fresh_db):
    idx = _index_names(fresh_db)
    assert {
        "idx_judge_ratings_run_paper_field",
        "idx_judge_ratings_run",
        "idx_judge_ratings_field",
        "idx_judge_pair_ratings_rating",
        "idx_judge_pair_ratings_aggregation",
    } <= idx


def test_migration_is_idempotent(tmp_path):
    db_path = tmp_path / "j.db"
    # First run creates the 3 judge tables + 5 named indexes (plus SQLite's
    # own sqlite_sequence table and sqlite_autoindex_* entries for UNIQUE /
    # PK constraints — hence we assert >=, not ==).
    first = run_migration(str(db_path))
    assert first["tables_created"] >= 3
    assert first["indexes_created"] >= 5
    # Second run is a no-op.
    second = run_migration(str(db_path))
    assert second["tables_created"] == 0
    assert second["indexes_created"] == 0


# ---------------------------------------------------------------------------
# judge_runs constraints
# ---------------------------------------------------------------------------


def test_pass_number_rejects_zero(fresh_db):
    with pytest.raises(sqlite3.IntegrityError):
        _insert_run(fresh_db, pass_number=0)


def test_pass_number_rejects_three(fresh_db):
    with pytest.raises(sqlite3.IntegrityError):
        _insert_run(fresh_db, pass_number=3)


def test_pass_number_accepts_one_and_two(fresh_db):
    _insert_run(fresh_db, run_id="r_pass1", pass_number=1)
    _insert_run(fresh_db, run_id="r_pass2", pass_number=2)


def test_run_id_primary_key_rejects_duplicates(fresh_db):
    _insert_run(fresh_db, run_id="dup")
    with pytest.raises(sqlite3.IntegrityError):
        _insert_run(fresh_db, run_id="dup")


# ---------------------------------------------------------------------------
# judge_ratings constraints
# ---------------------------------------------------------------------------


def test_field_type_rejects_other(fresh_db):
    _insert_run(fresh_db)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_rating(fresh_db, field_type="other")


def test_fabrication_risk_rejects_critical(fresh_db):
    _insert_run(fresh_db)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_rating(fresh_db, pass1_fabrication_risk="critical")


def test_unique_run_paper_field_blocks_duplicate(fresh_db):
    _insert_run(fresh_db)
    _insert_rating(fresh_db, paper_id="EE-001", field_name="study_design")
    with pytest.raises(sqlite3.IntegrityError):
        _insert_rating(fresh_db, paper_id="EE-001", field_name="study_design")


def test_different_paper_field_allowed_within_run(fresh_db):
    _insert_run(fresh_db)
    _insert_rating(fresh_db, paper_id="EE-001", field_name="study_design")
    _insert_rating(fresh_db, paper_id="EE-002", field_name="study_design")
    _insert_rating(fresh_db, paper_id="EE-001", field_name="sample_size",
                   field_type="numeric")


def test_not_null_prompt_hash_enforced(fresh_db):
    _insert_run(fresh_db)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_rating(fresh_db, prompt_hash=None)


def test_fk_to_judge_runs_enforced(fresh_db):
    with pytest.raises(sqlite3.IntegrityError):
        _insert_rating(fresh_db, run_id="does_not_exist")


# ---------------------------------------------------------------------------
# judge_pair_ratings constraints
# ---------------------------------------------------------------------------


def test_pair_check_arm_a_less_than_arm_b_equal_rejected(fresh_db):
    _insert_run(fresh_db)
    rid = _insert_rating(fresh_db)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_pair(fresh_db, rid, "a", "a")


def test_pair_check_arm_a_less_than_arm_b_reversed_rejected(fresh_db):
    _insert_run(fresh_db)
    rid = _insert_rating(fresh_db)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_pair(fresh_db, rid, "b", "a")


def test_pair_check_equivalent_with_type_rejected(fresh_db):
    _insert_run(fresh_db)
    rid = _insert_rating(fresh_db)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_pair(fresh_db, rid, "a", "b",
                     level1="EQUIVALENT", level2="GRANULARITY")


def test_pair_check_partial_without_type_rejected(fresh_db):
    _insert_run(fresh_db)
    rid = _insert_rating(fresh_db)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_pair(fresh_db, rid, "a", "b",
                     level1="PARTIAL", level2=None)


def test_pair_check_divergent_without_type_rejected(fresh_db):
    _insert_run(fresh_db)
    rid = _insert_rating(fresh_db)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_pair(fresh_db, rid, "a", "b",
                     level1="DIVERGENT", level2=None)


def test_pair_unique_rejects_duplicate_pair(fresh_db):
    _insert_run(fresh_db)
    rid = _insert_rating(fresh_db)
    _insert_pair(fresh_db, rid, "a", "b")
    with pytest.raises(sqlite3.IntegrityError):
        _insert_pair(fresh_db, rid, "a", "b")


def test_pair_on_delete_cascade_from_judge_ratings(fresh_db):
    _insert_run(fresh_db)
    rid = _insert_rating(fresh_db)
    _insert_pair(fresh_db, rid, "a", "b")
    _insert_pair(fresh_db, rid, "a", "c")
    _insert_pair(fresh_db, rid, "b", "c")
    fresh_db.execute("DELETE FROM judge_ratings WHERE rating_id = ?", (rid,))
    fresh_db.commit()
    remaining = fresh_db.execute(
        "SELECT COUNT(*) FROM judge_pair_ratings WHERE rating_id = ?",
        (rid,),
    ).fetchone()[0]
    assert remaining == 0


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------


def test_round_trip_3_arm(fresh_db):
    _insert_run(fresh_db)
    rid = _insert_rating(fresh_db)
    _insert_pair(fresh_db, rid, "a", "b")
    _insert_pair(fresh_db, rid, "a", "c")
    _insert_pair(fresh_db, rid, "b", "c",
                 level1="PARTIAL", level2="GRANULARITY")
    rows = fresh_db.execute(
        "SELECT arm_a, arm_b, level1_rating, level2_type FROM judge_pair_ratings "
        "WHERE rating_id = ? ORDER BY arm_a, arm_b",
        (rid,),
    ).fetchall()
    assert rows == [
        ("a", "b", "EQUIVALENT", None),
        ("a", "c", "EQUIVALENT", None),
        ("b", "c", "PARTIAL", "GRANULARITY"),
    ]


def test_round_trip_4_arm_has_6_pairs(fresh_db):
    _insert_run(fresh_db)
    rid = _insert_rating(fresh_db)
    arms = ("a", "b", "c", "d")
    for i, arm_a in enumerate(arms):
        for arm_b in arms[i + 1:]:
            _insert_pair(fresh_db, rid, arm_a, arm_b)
    count = fresh_db.execute(
        "SELECT COUNT(*) FROM judge_pair_ratings WHERE rating_id = ?",
        (rid,),
    ).fetchone()[0]
    assert count == 6


# ---------------------------------------------------------------------------
# Aggregation sanity
# ---------------------------------------------------------------------------


def test_aggregation_groupby_over_10_ratings(fresh_db):
    _insert_run(fresh_db)
    expected_counts: dict[tuple[str, str, str], int] = {}
    for i in range(10):
        rid = _insert_rating(
            fresh_db, paper_id=f"EE-{i:03d}", field_name="study_design"
        )
        # Alternate pattern so groupby has variety.
        if i % 2 == 0:
            combos = [
                ("a", "b", "EQUIVALENT", None),
                ("a", "c", "EQUIVALENT", None),
                ("b", "c", "PARTIAL", "GRANULARITY"),
            ]
        else:
            combos = [
                ("a", "b", "DIVERGENT", "CONTRADICTION"),
                ("a", "c", "PARTIAL", "OMISSION"),
                ("b", "c", "EQUIVALENT", None),
            ]
        for arm_a, arm_b, lv1, lv2 in combos:
            _insert_pair(fresh_db, rid, arm_a, arm_b, level1=lv1, level2=lv2)
            key = (arm_a, arm_b, lv1)
            expected_counts[key] = expected_counts.get(key, 0) + 1

    rows = fresh_db.execute(
        """SELECT arm_a, arm_b, level1_rating, COUNT(*) AS n
           FROM judge_pair_ratings
           GROUP BY arm_a, arm_b, level1_rating
           ORDER BY arm_a, arm_b, level1_rating"""
    ).fetchall()
    actual = {(r[0], r[1], r[2]): r[3] for r in rows}
    assert actual == expected_counts
    assert sum(actual.values()) == 30
