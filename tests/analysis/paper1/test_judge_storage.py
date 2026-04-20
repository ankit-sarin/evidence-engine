"""Tests for analysis/paper1/judge_storage.py."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from itertools import combinations

import pytest

from analysis.paper1.judge_schema import (
    ArmOutput,
    DisagreementPair,
    EquivalentPair,
    JudgeResult,
    Pass1Output,
)
from analysis.paper1.judge_storage import (
    JudgeStorageError,
    complete_judge_run,
    create_judge_run,
    insert_judge_result,
)
from analysis.paper1.precheck import PreCheckFlags
from engine.core.database import ReviewDatabase


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db(tmp_path):
    rdb = ReviewDatabase("judge_storage_test", data_root=tmp_path)
    yield rdb
    rdb.close()


def _seeded_run(db, run_id="r1", **overrides):
    params = dict(
        judge_model_name="gemma3:27b",
        judge_model_digest="sha256:aaa",
        codebook_sha256="b" * 64,
        pass_number=1,
        input_scope="AI_TRIPLES",
        run_config={"arms": ["local", "o4mini", "sonnet"]},
        notes=None,
    )
    params.update(overrides)
    create_judge_run(db, run_id, **params)


def _flags():
    return PreCheckFlags(
        span_present=True, span_in_source=True, value_in_span=True,
        span_length=40, span_match_method="exact_substring",
        value_match_method="categorical_exact",
    )


def make_result(
    paper_id="EE-024",
    field_name="study_type",
    arms=("local", "o4mini", "sonnet"),
    equivalences=None,
    types=None,
    fabrication_risk="low",
    proposed_consensus="RCT",
    raw_response='{"ok":true}',
    prompt_hash="c" * 64,
    seed=987654,
    timestamp_iso="2026-04-20T12:00:00+00:00",
    judge_model_name="gemma3:27b",
    judge_model_digest="sha256:aaa",
):
    n = len(arms)
    slot_pairs = list(combinations(range(1, n + 1), 2))
    if equivalences is None:
        equivalences = ["EQUIVALENT"] * len(slot_pairs)
    if len(equivalences) != len(slot_pairs):
        raise AssertionError("equivalences length must match pair count")
    if types is None:
        types = [None if r == "EQUIVALENT" else "GRANULARITY"
                 for r in equivalences]

    ratings = []
    for (a, b), rating, dtype in zip(slot_pairs, equivalences, types):
        if rating == "EQUIVALENT":
            ratings.append(
                EquivalentPair(slot_a=a, slot_b=b, rating=rating,
                               rationale=f"pair {a}-{b}")
            )
        else:
            ratings.append(
                DisagreementPair(slot_a=a, slot_b=b, rating=rating,
                                 disagreement_type=dtype,
                                 rationale=f"pair {a}-{b}")
            )
    pass1 = Pass1Output(
        pairwise_ratings=ratings,
        fabrication_risk=fabrication_risk,
        proposed_consensus=proposed_consensus,
        overall_rationale="overall",
    )
    return JudgeResult(
        paper_id=paper_id,
        field_name=field_name,
        arm_permutation=list(arms),
        pass1=pass1,
        prompt_hash=prompt_hash,
        judge_model_digest=judge_model_digest,
        judge_model_name=judge_model_name,
        raw_response=raw_response,
        seed=seed,
        timestamp_iso=timestamp_iso,
    )


# ---------------------------------------------------------------------------
# create_judge_run
# ---------------------------------------------------------------------------


class TestCreateJudgeRun:
    def test_happy_path(self, db):
        _seeded_run(db, run_id="r1")
        row = db._conn.execute(
            "SELECT * FROM judge_runs WHERE run_id = 'r1'"
        ).fetchone()
        assert row["completed_at"] is None
        assert row["n_triples_attempted"] == 0
        assert row["n_triples_succeeded"] == 0
        assert row["n_triples_failed"] == 0
        parsed = datetime.fromisoformat(row["started_at"])
        assert parsed.tzinfo is not None
        assert row["input_scope"] == "AI_TRIPLES"
        assert row["judge_model_name"] == "gemma3:27b"

    def test_duplicate_run_id_rejected(self, db):
        _seeded_run(db, run_id="dup")
        with pytest.raises(JudgeStorageError):
            _seeded_run(db, run_id="dup")

    def test_invalid_pass_number_zero(self, db):
        with pytest.raises(JudgeStorageError):
            _seeded_run(db, run_id="r_bad0", pass_number=0)

    def test_invalid_pass_number_three(self, db):
        with pytest.raises(JudgeStorageError):
            _seeded_run(db, run_id="r_bad3", pass_number=3)

    def test_notes_none_stored_as_null(self, db):
        _seeded_run(db, run_id="r_nn", notes=None)
        row = db._conn.execute(
            "SELECT notes FROM judge_runs WHERE run_id = 'r_nn'"
        ).fetchone()
        assert row["notes"] is None

    def test_run_config_serialized_as_json(self, db):
        cfg = {"arms": ["a", "b"], "filter": {"field": "x", "min": 3}}
        _seeded_run(db, run_id="r_cfg", run_config=cfg)
        row = db._conn.execute(
            "SELECT run_config_json FROM judge_runs WHERE run_id = 'r_cfg'"
        ).fetchone()
        assert json.loads(row["run_config_json"]) == cfg

    def test_codebook_sha_stored_verbatim(self, db):
        sha = "a" * 64
        _seeded_run(db, run_id="r_sha", codebook_sha256=sha)
        row = db._conn.execute(
            "SELECT codebook_sha256 FROM judge_runs WHERE run_id = 'r_sha'"
        ).fetchone()
        assert row["codebook_sha256"] == sha

    def test_unserializable_run_config_rejected(self, db):
        class Blob:
            pass
        with pytest.raises(JudgeStorageError):
            create_judge_run(
                db, run_id="r_unser",
                judge_model_name="m", judge_model_digest="d",
                codebook_sha256="x", pass_number=1,
                input_scope="AI_TRIPLES",
                run_config={"blob": Blob()},
            )


# ---------------------------------------------------------------------------
# complete_judge_run
# ---------------------------------------------------------------------------


class TestCompleteJudgeRun:
    def test_happy_path(self, db):
        _seeded_run(db, run_id="r_ok")
        started = db._conn.execute(
            "SELECT started_at FROM judge_runs WHERE run_id = 'r_ok'"
        ).fetchone()["started_at"]
        complete_judge_run(db, "r_ok",
                           n_triples_attempted=10,
                           n_triples_succeeded=9,
                           n_triples_failed=1)
        row = db._conn.execute(
            "SELECT completed_at, n_triples_attempted, n_triples_succeeded, "
            "n_triples_failed FROM judge_runs WHERE run_id = 'r_ok'"
        ).fetchone()
        assert row["completed_at"] is not None
        assert row["n_triples_attempted"] == 10
        assert row["n_triples_succeeded"] == 9
        assert row["n_triples_failed"] == 1
        assert datetime.fromisoformat(row["completed_at"]) >= \
               datetime.fromisoformat(started)

    def test_unknown_run_id(self, db):
        with pytest.raises(JudgeStorageError):
            complete_judge_run(db, "missing", 0, 0, 0)

    def test_already_completed_blocks(self, db):
        _seeded_run(db, run_id="r_c")
        complete_judge_run(db, "r_c", 0, 0, 0)
        with pytest.raises(JudgeStorageError):
            complete_judge_run(db, "r_c", 0, 0, 0)

    def test_counter_mismatch_rejected(self, db):
        _seeded_run(db, run_id="r_cm")
        with pytest.raises(JudgeStorageError):
            complete_judge_run(db, "r_cm",
                               n_triples_attempted=10,
                               n_triples_succeeded=5, n_triples_failed=3)

    def test_counters_zero_zero_zero_allowed(self, db):
        _seeded_run(db, run_id="r_z")
        complete_judge_run(db, "r_z", 0, 0, 0)
        row = db._conn.execute(
            "SELECT completed_at FROM judge_runs WHERE run_id = 'r_z'"
        ).fetchone()
        assert row["completed_at"] is not None


# ---------------------------------------------------------------------------
# insert_judge_result — happy paths and counts
# ---------------------------------------------------------------------------


class TestInsertHappy:
    def test_3_arm_inserts_1_plus_3_rows(self, db):
        _seeded_run(db)
        result = make_result(paper_id="EE-001",
                             arms=("local", "o4mini", "sonnet"))
        rating_id = insert_judge_result(db, "r1", result,
                                        field_type="categorical")
        assert isinstance(rating_id, int)
        n_ratings = db._conn.execute(
            "SELECT COUNT(*) FROM judge_ratings"
        ).fetchone()[0]
        n_pairs = db._conn.execute(
            "SELECT COUNT(*) FROM judge_pair_ratings WHERE rating_id = ?",
            (rating_id,),
        ).fetchone()[0]
        assert (n_ratings, n_pairs) == (1, 3)

    def test_4_arm_inserts_1_plus_6_rows(self, db):
        _seeded_run(db)
        result = make_result(paper_id="EE-002",
                             arms=("a", "b", "c", "d"))
        rid = insert_judge_result(db, "r1", result, field_type="categorical")
        n_pairs = db._conn.execute(
            "SELECT COUNT(*) FROM judge_pair_ratings WHERE rating_id = ?",
            (rid,),
        ).fetchone()[0]
        assert n_pairs == 6

    def test_5_arm_inserts_1_plus_10_rows(self, db):
        _seeded_run(db)
        result = make_result(paper_id="EE-003",
                             arms=("a", "b", "c", "d", "e"))
        rid = insert_judge_result(db, "r1", result, field_type="categorical")
        n_pairs = db._conn.execute(
            "SELECT COUNT(*) FROM judge_pair_ratings WHERE rating_id = ?",
            (rid,),
        ).fetchone()[0]
        assert n_pairs == 10


# ---------------------------------------------------------------------------
# insert_judge_result — errors
# ---------------------------------------------------------------------------


class TestInsertErrors:
    def test_unknown_run_id_rejected(self, db):
        _seeded_run(db)
        result = make_result()
        with pytest.raises(JudgeStorageError):
            insert_judge_result(db, "no_such_run", result,
                                field_type="categorical")

    def test_duplicate_run_paper_field_rejected(self, db):
        _seeded_run(db)
        r1 = make_result(paper_id="EE-100", field_name="study_design")
        r2 = make_result(paper_id="EE-100", field_name="study_design")
        insert_judge_result(db, "r1", r1, field_type="categorical")
        with pytest.raises(JudgeStorageError):
            insert_judge_result(db, "r1", r2, field_type="categorical")

    def test_arm_permutation_length_1_rejected(self, db):
        _seeded_run(db)
        pass1 = Pass1Output(
            pairwise_ratings=[
                EquivalentPair(slot_a=1, slot_b=2, rating="EQUIVALENT",
                               rationale="x"),
            ],
            fabrication_risk="low",
            proposed_consensus=None,
            overall_rationale="x",
        )
        result = JudgeResult(
            paper_id="EE-1", field_name="f",
            arm_permutation=["only"],
            pass1=pass1, prompt_hash="x" * 64,
            judge_model_digest="d", judge_model_name="m",
            raw_response="{}", seed=1, timestamp_iso="2026-04-20T00:00:00+00:00",
        )
        with pytest.raises(JudgeStorageError):
            insert_judge_result(db, "r1", result, field_type="categorical")

    def test_pair_count_mismatch_rejected(self, db):
        _seeded_run(db)
        # 3 arms → expected 3 pairs; provide only 2.
        ratings = [
            EquivalentPair(slot_a=1, slot_b=2, rating="EQUIVALENT",
                           rationale="x"),
            EquivalentPair(slot_a=1, slot_b=3, rating="EQUIVALENT",
                           rationale="x"),
        ]
        pass1 = Pass1Output(
            pairwise_ratings=ratings, fabrication_risk="low",
            proposed_consensus=None, overall_rationale="x",
        )
        result = JudgeResult(
            paper_id="EE-2", field_name="f",
            arm_permutation=["a", "b", "c"],
            pass1=pass1, prompt_hash="x" * 64,
            judge_model_digest="d", judge_model_name="m",
            raw_response="{}", seed=1, timestamp_iso="t",
        )
        with pytest.raises(JudgeStorageError):
            insert_judge_result(db, "r1", result, field_type="categorical")

    def test_slot_a_out_of_range(self, db):
        _seeded_run(db)
        ratings = [
            EquivalentPair(slot_a=1, slot_b=2, rating="EQUIVALENT",
                           rationale="x"),
            EquivalentPair(slot_a=5, slot_b=6, rating="EQUIVALENT",
                           rationale="x"),  # out of range
            EquivalentPair(slot_a=2, slot_b=3, rating="EQUIVALENT",
                           rationale="x"),
        ]
        pass1 = Pass1Output(
            pairwise_ratings=ratings, fabrication_risk="low",
            proposed_consensus=None, overall_rationale="x",
        )
        result = JudgeResult(
            paper_id="EE-3", field_name="f",
            arm_permutation=["a", "b", "c"],
            pass1=pass1, prompt_hash="x" * 64,
            judge_model_digest="d", judge_model_name="m",
            raw_response="{}", seed=1, timestamp_iso="t",
        )
        with pytest.raises(JudgeStorageError):
            insert_judge_result(db, "r1", result, field_type="categorical")

    def test_slot_b_out_of_range(self, db):
        _seeded_run(db)
        # 3 arms → slot_b=4 is invalid. Keep slot_a < slot_b to pass Pydantic.
        ratings = [
            EquivalentPair(slot_a=1, slot_b=2, rating="EQUIVALENT",
                           rationale="x"),
            EquivalentPair(slot_a=1, slot_b=3, rating="EQUIVALENT",
                           rationale="x"),
            EquivalentPair(slot_a=3, slot_b=4, rating="EQUIVALENT",
                           rationale="x"),
        ]
        pass1 = Pass1Output(
            pairwise_ratings=ratings, fabrication_risk="low",
            proposed_consensus=None, overall_rationale="x",
        )
        result = JudgeResult(
            paper_id="EE-4", field_name="f",
            arm_permutation=["a", "b", "c"],
            pass1=pass1, prompt_hash="x" * 64,
            judge_model_digest="d", judge_model_name="m",
            raw_response="{}", seed=1, timestamp_iso="t",
        )
        with pytest.raises(JudgeStorageError):
            insert_judge_result(db, "r1", result, field_type="categorical")


# ---------------------------------------------------------------------------
# De-randomization and round-trip
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_de_randomization_swaps_to_lex_order(self, db):
        _seeded_run(db)
        # arm_permutation: slot1="sonnet", slot2="local", slot3="o4mini"
        # → pair (1,2) decodes to ("sonnet","local") → must store
        # ("local","sonnet").
        arms = ("sonnet", "local", "o4mini")
        result = make_result(paper_id="EE-swap", arms=arms)
        rid = insert_judge_result(db, "r1", result,
                                  field_type="categorical")
        rows = db._conn.execute(
            "SELECT arm_a, arm_b FROM judge_pair_ratings WHERE rating_id = ? "
            "ORDER BY arm_a, arm_b",
            (rid,),
        ).fetchall()
        stored = [(r["arm_a"], r["arm_b"]) for r in rows]
        for a, b in stored:
            assert a < b
        assert ("local", "sonnet") in stored
        assert ("local", "o4mini") in stored
        assert ("o4mini", "sonnet") in stored

    def test_raw_response_verbatim(self, db):
        _seeded_run(db)
        raw = 'a\n"b"\nunicode: résumé ✓\n{"json": true}'
        result = make_result(paper_id="EE-raw", raw_response=raw)
        rid = insert_judge_result(db, "r1", result, field_type="categorical")
        row = db._conn.execute(
            "SELECT raw_response FROM judge_ratings WHERE rating_id = ?",
            (rid,),
        ).fetchone()
        assert row["raw_response"] == raw

    def test_arm_permutation_json_round_trip(self, db):
        _seeded_run(db)
        arms = ("one", "two", "three", "four")
        result = make_result(paper_id="EE-perm", arms=arms)
        rid = insert_judge_result(db, "r1", result, field_type="categorical")
        row = db._conn.execute(
            "SELECT arm_permutation_json FROM judge_ratings "
            "WHERE rating_id = ?",
            (rid,),
        ).fetchone()
        assert json.loads(row["arm_permutation_json"]) == list(arms)

    def test_full_column_round_trip(self, db):
        _seeded_run(db)
        result = make_result(
            paper_id="EE-full",
            field_name="sample_size",
            fabrication_risk="medium",
            proposed_consensus=None,
            raw_response="verbatim",
            prompt_hash="d" * 64,
            seed=424242,
        )
        rid = insert_judge_result(db, "r1", result, field_type="numeric")
        row = db._conn.execute(
            "SELECT run_id, paper_id, field_name, field_type, seed, "
            "prompt_hash, raw_response, pass1_fabrication_risk, "
            "pass1_proposed_consensus, pass1_overall_rationale "
            "FROM judge_ratings WHERE rating_id = ?",
            (rid,),
        ).fetchone()
        assert row["run_id"] == "r1"
        assert row["paper_id"] == "EE-full"
        assert row["field_name"] == "sample_size"
        assert row["field_type"] == "numeric"
        assert row["seed"] == 424242
        assert row["prompt_hash"] == "d" * 64
        assert row["raw_response"] == "verbatim"
        assert row["pass1_fabrication_risk"] == "medium"
        assert row["pass1_proposed_consensus"] is None
        assert row["pass1_overall_rationale"] == "overall"


# ---------------------------------------------------------------------------
# Atomicity
# ---------------------------------------------------------------------------


class TestAtomicity:
    def test_rollback_leaves_no_rows_after_mid_txn_failure(self, db):
        """Trigger a UNIQUE violation on the second pair insert — after
        the judge_ratings row and first pair row have already been written
        inside the transaction. Confirms the judge_ratings row is rolled
        back (observing aborted state, not never-started state).

        We exploit the lack of pair-uniqueness checking in _validate_insert
        invariants: two PairwiseRatings resolving to the same de-randomized
        pair (arm_a, arm_b) pass invariant checks, but the second triggers
        the UNIQUE (rating_id, arm_a, arm_b) constraint mid-transaction.
        """
        _seeded_run(db)
        assert db._conn.execute(
            "SELECT COUNT(*) FROM judge_ratings"
        ).fetchone()[0] == 0

        # 3-arm case → C(3,2)=3 pairs expected. Provide 3 PairwiseRatings
        # but make the first two resolve to the same (arm_a, arm_b):
        # both (1,2) pairs decode to ("a","b").
        ratings = [
            EquivalentPair(slot_a=1, slot_b=2, rating="EQUIVALENT",
                           rationale="first"),
            EquivalentPair(slot_a=1, slot_b=2, rating="EQUIVALENT",
                           rationale="duplicate"),
            EquivalentPair(slot_a=2, slot_b=3, rating="EQUIVALENT",
                           rationale="third"),
        ]
        pass1 = Pass1Output(
            pairwise_ratings=ratings, fabrication_risk="low",
            proposed_consensus="x", overall_rationale="x",
        )
        result = JudgeResult(
            paper_id="EE-atom", field_name="f",
            arm_permutation=["a", "b", "c"],
            pass1=pass1, prompt_hash="x" * 64,
            judge_model_digest="d", judge_model_name="m",
            raw_response="{}", seed=1, timestamp_iso="t",
        )
        with pytest.raises(JudgeStorageError) as exc_info:
            insert_judge_result(db, "r1", result, field_type="categorical")
        # Underlying sqlite3 error chained in __cause__.
        assert isinstance(exc_info.value.__cause__, sqlite3.Error)

        # Both tables must be empty — judge_ratings row was written inside
        # the aborted txn, and ROLLBACK reverted it.
        assert db._conn.execute(
            "SELECT COUNT(*) FROM judge_ratings"
        ).fetchone()[0] == 0
        assert db._conn.execute(
            "SELECT COUNT(*) FROM judge_pair_ratings"
        ).fetchone()[0] == 0
        # Connection must be out of the aborted txn so subsequent writes
        # succeed. Prove it by inserting a different result cleanly.
        clean_result = make_result(paper_id="EE-after",
                                   arms=("a", "b", "c"))
        rid = insert_judge_result(db, "r1", clean_result,
                                  field_type="categorical")
        assert rid is not None

    def test_sqlite_error_on_fk_rollback(self, db):
        """FK violation on judge_ratings insert → no row lingers."""
        # No run created → FK violation on run_id.
        result = make_result(paper_id="EE-fk", arms=("a", "b", "c"))
        with pytest.raises(JudgeStorageError):
            insert_judge_result(db, "ghost_run", result,
                                field_type="categorical")
        assert db._conn.execute(
            "SELECT COUNT(*) FROM judge_ratings"
        ).fetchone()[0] == 0
        assert db._conn.execute(
            "SELECT COUNT(*) FROM judge_pair_ratings"
        ).fetchone()[0] == 0
