"""Smoke tests for analysis/paper1/pass1_inspection.py.

Minimal coverage per task brief: verify that every query function executes
cleanly against a seeded DB and produces a DataFrame (or finding object)
with the expected shape. Not a full-coverage suite.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from analysis.paper1 import pass1_inspection as mod
from analysis.paper1.judge_loader import load_codebook
from analysis.paper1.judge_schema import (
    DisagreementPair,
    EquivalentPair,
    JudgeResult,
    Pass1Output,
)
from analysis.paper1.judge_storage import (
    complete_judge_run,
    create_judge_run,
    insert_judge_result,
)
from engine.core.database import ReviewDatabase


CODEBOOK_YAML = """
fields:
  - name: study_type
    tier: 1
    type: categorical
    definition: Type of study.
    valid_values:
      - "RCT"
      - "Cohort"
  - name: sample_size
    tier: 1
    type: numeric
    definition: N subjects.
    tolerance: 2
  - name: robot_platform
    tier: 1
    type: free_text
    definition: Robot name.
"""


def _result(paper_id, field_name, ratings, types, fab_risk="medium"):
    pairs = []
    for (a, b), rating, dtype in zip([(1, 2), (1, 3), (2, 3)], ratings, types):
        if rating == "EQUIVALENT":
            pairs.append(EquivalentPair(slot_a=a, slot_b=b, rating=rating,
                                        rationale="x"))
        else:
            pairs.append(DisagreementPair(slot_a=a, slot_b=b, rating=rating,
                                          disagreement_type=dtype,
                                          rationale="x"))
    pass1 = Pass1Output(
        pairwise_ratings=pairs,
        fabrication_risk=fab_risk,
        proposed_consensus="RCT",
        overall_rationale="x",
    )
    return JudgeResult(
        paper_id=paper_id, field_name=field_name,
        arm_permutation=["local", "openai_o4_mini_high", "anthropic_sonnet_4_6"],
        pass1=pass1, prompt_hash="d" * 64, judge_model_name="gemma3:27b",
        judge_model_digest="sha256:ab", raw_response=json.dumps({}),
        seed=1, timestamp_iso="2026-04-20T12:00:00+00:00",
    )


@pytest.fixture()
def seeded_db(tmp_path):
    db = ReviewDatabase("pass1_inspection_test", data_root=tmp_path)
    create_judge_run(
        db, "run-x",
        judge_model_name="gemma3:27b", judge_model_digest="sha256:ab",
        codebook_sha256="c" * 64, pass_number=1, input_scope="AI_TRIPLES",
        run_config={}, notes=None,
    )
    # Triple A: all EQ, low risk, categorical
    insert_judge_result(
        db, "run-x",
        _result("1", "study_type",
                ["EQUIVALENT", "EQUIVALENT", "EQUIVALENT"],
                [None, None, None], fab_risk="low"),
        field_type="categorical",
    )
    # Triple B: all PARTIAL/GRANULARITY, medium risk, free_text
    insert_judge_result(
        db, "run-x",
        _result("2", "robot_platform",
                ["PARTIAL", "PARTIAL", "PARTIAL"],
                ["GRANULARITY", "GRANULARITY", "GRANULARITY"],
                fab_risk="medium"),
        field_type="free_text",
    )
    # Triple C: mixed, high risk, numeric
    insert_judge_result(
        db, "run-x",
        _result("3", "sample_size",
                ["DIVERGENT", "PARTIAL", "EQUIVALENT"],
                ["FABRICATION", "SELECTION", None], fab_risk="high"),
        field_type="numeric",
    )
    complete_judge_run(db, "run-x", n_triples_attempted=3,
                       n_triples_succeeded=3, n_triples_failed=0)
    yield db
    db.close()


@pytest.fixture()
def codebook(tmp_path):
    p = tmp_path / "codebook.yaml"
    p.write_text(CODEBOOK_YAML)
    return load_codebook(p)


def _pair_df(db):
    return mod._load_pair_ratings(db, "run-x")


def _ratings_df(db):
    return mod._load_ratings(db, "run-x")


def test_load_helpers_return_dataframes(seeded_db):
    ratings = _ratings_df(seeded_db)
    pairs = _pair_df(seeded_db)
    assert len(ratings) == 3
    assert len(pairs) == 9  # 3 triples × C(3,2)
    assert set(ratings["fab_risk"]) == {"low", "medium", "high"}


def test_q1_field_level1(seeded_db):
    pairs = _pair_df(seeded_db)
    df = mod.q1_field_level1(pairs)
    assert set(df.columns) >= {"field_name", "EQUIVALENT", "PARTIAL",
                               "DIVERGENT", "total", "pct_eq", "flag"}
    assert len(df) == 3
    # study_type — all EQ (3/3) → HIGH_EQ flag
    study = df[df["field_name"] == "study_type"].iloc[0]
    assert study["EQUIVALENT"] == 3
    assert study["flag"] == "HIGH_EQ"
    # robot_platform — all PARTIAL (0/3 EQ) → LOW_EQ
    robot = df[df["field_name"] == "robot_platform"].iloc[0]
    assert robot["EQUIVALENT"] == 0
    assert robot["flag"] == "LOW_EQ"


def test_q2_field_level2_flags_saturation(seeded_db):
    pairs = _pair_df(seeded_db)
    df = mod.q2_field_level2(pairs)
    robot = df[df["field_name"] == "robot_platform"].iloc[0]
    assert robot["GRANULARITY"] == 3
    assert robot["flag"] == "GRANULARITY_SATURATED"
    # sample_size: 1 FABRICATION + 1 SELECTION → not saturated
    if "sample_size" in df["field_name"].values:
        ss = df[df["field_name"] == "sample_size"].iloc[0]
        assert ss["flag"] == ""


def test_q3_fieldtype_level2(seeded_db, codebook):
    pairs = _pair_df(seeded_db)
    df = mod.q3_fieldtype_level2(pairs, codebook)
    assert set(df["field_type"]) == {"free_text", "categorical", "numeric"}
    ft = df[df["field_type"] == "free_text"].iloc[0]
    assert ft["GRANULARITY"] == 3


def test_q4_armpair_level2(seeded_db):
    pairs = _pair_df(seeded_db)
    df = mod.q4_armpair_level2(pairs)
    assert len(df) == 3  # exactly three arm pairs
    assert "noneq_total" in df.columns


def test_q5_attribution_audit_is_false_for_current_schema():
    finding = mod.q5_fabricator_attribution()
    assert finding.has_attribution_field is False
    assert "Pass 2" in finding.note


def test_q7_field_1_root_cause_absent(tmp_path):
    csv_path = tmp_path / "pairs.csv"
    csv_path.write_text(
        "paper_id,field_name,local_value,o4mini_value,sonnet_value\n"
        "1,study_type,RCT,RCT,RCT\n"
    )
    out = mod.q7_field_1_root_cause(csv_path)
    assert out["count"] == 0
    assert out["classification"] == "not_present"


def test_q7_field_1_classifies_local_artifact(tmp_path):
    csv_path = tmp_path / "pairs.csv"
    csv_path.write_text(
        "paper_id,paper_label,field_name,field_tier,field_type,"
        "local_value,o4mini_value,sonnet_value\n"
        "719,Bauzano,field_1,0,free_text,"
        "\"summary text here\",,\n"
    )
    out = mod.q7_field_1_root_cause(csv_path)
    assert out["count"] == 1
    assert out["classification"] == "local_arm_extractor_artifact"
    assert out["example"]["paper_id"] == "719"
