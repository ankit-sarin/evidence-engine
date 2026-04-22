"""Tests for insert_pass2_verifications in analysis/paper1/judge_storage.py."""

from __future__ import annotations

import sqlite3

import pytest

from analysis.paper1.judge_schema import (
    PartiallySupportedVerdict,
    Pass2Output,
    Pass2Result,
    SupportedVerdict,
    UnsupportedVerdict,
)
from analysis.paper1.judge_storage import (
    JudgeStorageError,
    create_judge_run,
    insert_pass2_verifications,
)
from engine.core.database import ReviewDatabase


@pytest.fixture()
def db(tmp_path):
    rdb = ReviewDatabase("pass2_storage_test", data_root=tmp_path)
    create_judge_run(
        rdb, "r2",
        judge_model_name="gemma3:27b", judge_model_digest="sha256:abc",
        codebook_sha256="c" * 64, pass_number=2, input_scope="AI_TRIPLES",
        run_config={}, notes=None,
    )
    yield rdb
    rdb.close()


def _result(verdicts, arms=("local", "o4mini", "sonnet"),
            short_circuit_by_arm=None):
    if short_circuit_by_arm is None:
        short_circuit_by_arm = {a: False for a in arms}
    return Pass2Result(
        paper_id="EE-1", field_name="study_design",
        arm_permutation=list(arms),
        pass2=Pass2Output(
            paper_id="EE-1", field_name="study_design",
            arm_verdicts=verdicts,
            overall_fabrication_detected=any(
                v.verdict == "UNSUPPORTED" for v in verdicts
            ),
        ),
        pre_check_short_circuit_by_arm=short_circuit_by_arm,
        prompt_hash="p" * 64,
        judge_model_digest="sha256:abc",
        judge_model_name="gemma3:27b",
        raw_response="{}",
        seed=123,
        timestamp_iso="2026-04-21T12:00:00+00:00",
        source_text_windowed=False,
        source_text_tokens=5000,
    )


class TestInsertPass2Verifications:
    def test_inserts_three_rows_one_per_arm(self, db):
        result = _result(
            [
                SupportedVerdict(arm_slot=1, verdict="SUPPORTED"),
                SupportedVerdict(arm_slot=2, verdict="SUPPORTED"),
                SupportedVerdict(arm_slot=3, verdict="SUPPORTED"),
            ],
            short_circuit_by_arm={"local": True, "o4mini": False, "sonnet": True},
        )
        n = insert_pass2_verifications(db, "r2", result)
        assert n == 3
        rows = db._conn.execute(
            "SELECT arm_name, verdict, pre_check_short_circuit "
            "FROM fabrication_verifications ORDER BY arm_name"
        ).fetchall()
        assert [r["arm_name"] for r in rows] == ["local", "o4mini", "sonnet"]
        assert all(r["verdict"] == "SUPPORTED" for r in rows)
        # short_circuit persisted per arm.
        sc = {r["arm_name"]: r["pre_check_short_circuit"] for r in rows}
        assert sc == {"local": 1, "o4mini": 0, "sonnet": 1}

    def test_de_randomizes_slots_to_arm_names(self, db):
        # arm_permutation order differs from alphabetic.
        verdicts = [
            SupportedVerdict(arm_slot=1, verdict="SUPPORTED"),
            UnsupportedVerdict(arm_slot=2, verdict="UNSUPPORTED",
                               reasoning="r", fabrication_hypothesis="h"),
            PartiallySupportedVerdict(arm_slot=3, verdict="PARTIALLY_SUPPORTED",
                                      reasoning="r"),
        ]
        result = _result(
            verdicts,
            arms=("sonnet", "o4mini", "local"),
        )
        insert_pass2_verifications(db, "r2", result)
        rows = db._conn.execute(
            "SELECT arm_name, verdict FROM fabrication_verifications"
        ).fetchall()
        mapping = {r["arm_name"]: r["verdict"] for r in rows}
        assert mapping["sonnet"] == "SUPPORTED"
        assert mapping["o4mini"] == "UNSUPPORTED"
        assert mapping["local"] == "PARTIALLY_SUPPORTED"

    def test_unsupported_writes_reasoning_and_hypothesis(self, db):
        verdicts = [
            SupportedVerdict(arm_slot=1, verdict="SUPPORTED"),
            SupportedVerdict(arm_slot=2, verdict="SUPPORTED"),
            UnsupportedVerdict(arm_slot=3, verdict="UNSUPPORTED",
                               reasoning="no match", fabrication_hypothesis="guess"),
        ]
        result = _result(verdicts)
        insert_pass2_verifications(db, "r2", result)
        row = db._conn.execute(
            "SELECT reasoning, fabrication_hypothesis FROM fabrication_verifications "
            "WHERE verdict='UNSUPPORTED'"
        ).fetchone()
        assert row["reasoning"] == "no match"
        assert row["fabrication_hypothesis"] == "guess"

    def test_supported_leaves_reasoning_and_hypothesis_null(self, db):
        result = _result([
            SupportedVerdict(arm_slot=1, verdict="SUPPORTED"),
            SupportedVerdict(arm_slot=2, verdict="SUPPORTED"),
            SupportedVerdict(arm_slot=3, verdict="SUPPORTED"),
        ])
        insert_pass2_verifications(db, "r2", result)
        rows = db._conn.execute(
            "SELECT reasoning, fabrication_hypothesis FROM fabrication_verifications"
        ).fetchall()
        for r in rows:
            assert r["reasoning"] is None
            assert r["fabrication_hypothesis"] is None

    def test_partial_writes_reasoning_only(self, db):
        result = _result([
            SupportedVerdict(arm_slot=1, verdict="SUPPORTED"),
            SupportedVerdict(arm_slot=2, verdict="SUPPORTED"),
            PartiallySupportedVerdict(arm_slot=3, verdict="PARTIALLY_SUPPORTED",
                                      reasoning="middle"),
        ])
        insert_pass2_verifications(db, "r2", result)
        row = db._conn.execute(
            "SELECT reasoning, fabrication_hypothesis FROM fabrication_verifications "
            "WHERE verdict='PARTIALLY_SUPPORTED'"
        ).fetchone()
        assert row["reasoning"] == "middle"
        assert row["fabrication_hypothesis"] is None

    def test_rollback_on_duplicate_arm_in_same_run(self, db):
        result = _result([
            SupportedVerdict(arm_slot=1, verdict="SUPPORTED"),
            SupportedVerdict(arm_slot=2, verdict="SUPPORTED"),
            SupportedVerdict(arm_slot=3, verdict="SUPPORTED"),
        ])
        insert_pass2_verifications(db, "r2", result)
        # Second call with same (run, paper, field) violates UNIQUE on arm_name.
        with pytest.raises(JudgeStorageError):
            insert_pass2_verifications(db, "r2", result)
        # First insert remained (no partial rollback of the good batch).
        n = db._conn.execute(
            "SELECT COUNT(*) FROM fabrication_verifications"
        ).fetchone()[0]
        assert n == 3

    def test_rejects_mismatched_verdict_count(self, db):
        # Only two verdicts but 3 arms.
        result = _result([
            SupportedVerdict(arm_slot=1, verdict="SUPPORTED"),
            SupportedVerdict(arm_slot=2, verdict="SUPPORTED"),
        ])
        with pytest.raises(JudgeStorageError):
            insert_pass2_verifications(db, "r2", result)

    def test_rejects_out_of_range_slot(self, db):
        result = _result([
            SupportedVerdict(arm_slot=1, verdict="SUPPORTED"),
            SupportedVerdict(arm_slot=2, verdict="SUPPORTED"),
            SupportedVerdict(arm_slot=99, verdict="SUPPORTED"),
        ])
        with pytest.raises(JudgeStorageError):
            insert_pass2_verifications(db, "r2", result)

    def test_rejects_unknown_run_id(self, db):
        result = _result([
            SupportedVerdict(arm_slot=1, verdict="SUPPORTED"),
            SupportedVerdict(arm_slot=2, verdict="SUPPORTED"),
            SupportedVerdict(arm_slot=3, verdict="SUPPORTED"),
        ])
        with pytest.raises(JudgeStorageError):
            insert_pass2_verifications(db, "ghost_run", result)
