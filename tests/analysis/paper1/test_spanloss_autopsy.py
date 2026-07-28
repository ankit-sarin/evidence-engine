"""Tests for the single-span collapse autopsy.

The load-bearing logic is `response_shape` + `fork_verdict`: they decide whether
a paper's spans were never produced or were produced and lost, and the whole
report turns on that call.
"""

import json

import pytest

from analysis.paper1 import spanloss_autopsy as S

SPAN = {"field_name": "study_type", "value": "Original Research",
        "source_snippet": "…", "confidence": 0.9, "tier": 1}


# ── response shape ───────────────────────────────────────────────────────


def test_wrapped_list_is_the_healthy_shape():
    for key in ("fields", "extractions", "data", "results"):
        shape, n = S.response_shape(json.dumps({key: [SPAN] * 20}))
        assert shape == S.SHAPE_WRAPPED_LIST
        assert n == 20


def test_bare_single_span_dict_is_the_openai_collapse_shape():
    """What the 17 openai extractions actually stored: a complete, unwrapped span."""
    shape, n = S.response_shape(json.dumps(SPAN))
    assert shape == S.SHAPE_SINGLE_SPAN_DICT
    assert n == 1


def test_single_element_list_is_the_local_collapse_shape():
    """What the 2 local extractions stored: a one-element array."""
    shape, n = S.response_shape(json.dumps([{**SPAN, "field_name": "Title"}]))
    assert shape == S.SHAPE_SINGLE_ELEMENT_LIST
    assert n == 1


def test_bare_list_of_many():
    shape, n = S.response_shape(json.dumps([SPAN] * 19))
    assert shape == S.SHAPE_BARE_LIST
    assert n == 19


def test_raw_fallback_is_recognized_as_a_parse_failure():
    shape, n = S.response_shape(json.dumps({"fields": [], "raw": "not json at all"}))
    assert shape == S.SHAPE_RAW_FALLBACK
    assert n == 0


def test_unparseable_and_empty_are_unknown():
    assert S.response_shape("{not json") == (S.SHAPE_UNKNOWN, 0)
    assert S.response_shape("") == (S.SHAPE_UNKNOWN, 0)
    assert S.response_shape(None) == (S.SHAPE_UNKNOWN, 0)


def test_bare_list_counts_only_span_objects():
    shape, n = S.response_shape(json.dumps([SPAN, {"junk": 1}, SPAN]))
    assert shape == S.SHAPE_BARE_LIST
    assert n == 2


# ── the fork ─────────────────────────────────────────────────────────────


def test_response_carrying_one_span_and_storing_one_is_an_extraction_defect():
    assert S.fork_verdict(S.SHAPE_SINGLE_SPAN_DICT, 1, 1) == "EXTRACTION"
    assert S.fork_verdict(S.SHAPE_SINGLE_ELEMENT_LIST, 1, 1) == "EXTRACTION"


def test_response_carrying_twenty_but_storing_one_is_a_storage_defect():
    assert S.fork_verdict(S.SHAPE_WRAPPED_LIST, 20, 1) == "STORAGE"


def test_unparseable_response_is_undetermined_not_assumed():
    assert S.fork_verdict(S.SHAPE_UNKNOWN, 0, 1) == "UNDETERMINED"


def test_healthy_extraction_is_not_flagged_as_storage_loss():
    assert S.fork_verdict(S.SHAPE_WRAPPED_LIST, 20, 20) == "EXTRACTION"


# ── affected-set selection ───────────────────────────────────────────────


def test_affected_selects_below_threshold_and_sorts():
    rows = [
        {"arm": "openai_o4_mini_high", "paper_id": 5, "spans_stored": 20},
        {"arm": "openai_o4_mini_high", "paper_id": 3, "spans_stored": 1},
        {"arm": "local_deepseek_r1_32b", "paper_id": 9, "spans_stored": 19},
        {"arm": "local_deepseek_r1_32b", "paper_id": 7, "spans_stored": 1},
    ]
    aff = S.affected(rows)
    assert [(r["arm"], r["paper_id"]) for r in aff] == [
        ("local_deepseek_r1_32b", 7), ("local_deepseek_r1_32b", 9),
        ("openai_o4_mini_high", 3),
    ]


def test_affected_threshold_is_configurable():
    rows = [{"arm": "a", "paper_id": 1, "spans_stored": 19}]
    assert S.affected(rows) != []
    assert S.affected(rows, threshold=19) == []


# ── pairs-csv exposure ───────────────────────────────────────────────────


def test_pairs_csv_exposure_attributes_mismatches_to_empty_cells(tmp_path):
    p = tmp_path / "pairs.csv"
    p.write_text(
        "paper_id,field_name,o4mini_value,local_vs_o4mini_score\n"
        "277,robot_platform,,MISMATCH\n"          # affected, empty -> attributable
        "277,task_performed,,MISMATCH\n"          # affected, empty -> attributable
        "277,study_type,Original Research,MATCH\n"  # affected, populated
        "999,robot_platform,dVRK,MISMATCH\n"      # unaffected genuine mismatch
    )
    out = S.pairs_csv_exposure(p, {277})
    assert out["rows_from_affected_papers"] == 3
    assert out["rows_with_empty_value"] == 2
    imp = out["score_impact"]["local_vs_o4mini_score"]
    assert imp["mismatch_total"] == 3
    assert imp["mismatch_from_empty_cells"] == 2
    assert imp["pct_of_all_mismatches"] == pytest.approx(66.7, abs=0.1)


def test_pairs_csv_exposure_missing_file_is_empty_not_an_error(tmp_path):
    assert S.pairs_csv_exposure(tmp_path / "nope.csv", {1}) == {}
