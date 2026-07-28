"""Tests for the judge-verdict x provenance restatement.

The join is the part that can silently lie — a dropped arm alias or an
unhandled type would quietly shrink the denominator and flatter every rate — so
it gets the most coverage here, including the requirement that unjoined rows are
returned rather than discarded.
"""

import pytest

from analysis.paper1 import judge_provenance as JP
from analysis.provenance import classifier as C


def _v(paper, field, arm, verdict):
    return {
        "paper_id": paper, "field_name": field, "arm": JP.canonical_arm(arm),
        "arm_raw": arm, "verdict": verdict, "pre_check_short_circuit": 0,
    }


def _span(arm, paper, field, taxonomy, absence_pattern=None, snippet_chars=60):
    return (arm, paper, field), {
        "arm": arm, "paper_id": paper, "field_name": field,
        "taxonomy_class": taxonomy, "absence_pattern": absence_pattern,
        "snippet_chars": snippet_chars, "value": "x",
    }


LOCAL = "local_deepseek_r1_32b"


@pytest.fixture
def joined():
    verdicts = [
        _v(1, "country", "local", "SUPPORTED"),                      # no-basis  -> TRUE FAILURE
        _v(1, "robot_platform", "local", "SUPPORTED"),               # anchored
        _v(2, "clinical_readiness_assessment", "local", "SUPPORTED"),# no-basis  -> TRUE FAILURE
        _v(2, "task_select", "anthropic_sonnet_4_6", "SUPPORTED"),   # stitched  -> divergence
        _v(3, "study_type", "anthropic_sonnet_4_6", "SUPPORTED"),    # drifted   -> divergence
        _v(3, "sample_size", "openai_o4_mini_high", "UNSUPPORTED"),  # anchored  -> symmetry
        _v(4, "comparison_to_human", "local", "SUPPORTED"),          # absence claim
        _v(9, "country", "local", "SUPPORTED"),                      # no span at all
    ]
    spans = dict([
        _span(LOCAL, 1, "country", C.UNTRACEABLE_NO_BASIS),
        _span(LOCAL, 1, "robot_platform", C.ANCHORED),
        _span(LOCAL, 2, "clinical_readiness_assessment", C.UNTRACEABLE_NO_BASIS),
        _span("anthropic_sonnet_4_6", 2, "task_select", C.STITCHED),
        _span("anthropic_sonnet_4_6", 3, "study_type", C.DRIFTED),
        _span("openai_o4_mini_high", 3, "sample_size", C.ANCHORED),
        _span(LOCAL, 4, "comparison_to_human", C.ABSENCE_CLAIM, "P1_referent_negation"),
    ])
    return JP.join(verdicts, spans)


# ── join ─────────────────────────────────────────────────────────────────


def test_arm_alias_is_applied():
    assert JP.canonical_arm("local") == LOCAL
    assert JP.canonical_arm("openai_o4_mini_high") == "openai_o4_mini_high"


def test_join_splits_and_loses_nothing(joined):
    j, u = joined
    assert len(j) == 7
    assert len(u) == 1
    assert len(j) + len(u) == 8, "every judged row must be accounted for"


def test_unjoined_rows_are_returned_not_dropped(joined):
    _, u = joined
    assert u[0]["paper_id"] == 9
    assert u[0]["verdict"] == "SUPPORTED"


def test_joined_rows_carry_both_instruments_and_the_field_class(joined):
    j, _ = joined
    row = next(r for r in j if r["field_name"] == "country")
    assert row["verdict"] == "SUPPORTED"
    assert row["taxonomy_class"] == C.UNTRACEABLE_NO_BASIS
    assert row["field_class3"] == "inferable"


# ── headline ─────────────────────────────────────────────────────────────


def test_supported_breakdown_counts(joined):
    j, _ = joined
    b = JP.supported_breakdown(j)
    assert b["supported_total"] == 6
    assert b["on_no_basis"]["n"] == 2
    assert b["on_anchored"]["n"] == 1
    assert b["on_stitched_or_drifted"]["n"] == 2
    assert b["on_absence_claim"]["n"] == 1
    assert b["on_any_non_anchored"]["n"] == 5
    assert b["on_no_basis"]["pct"] == pytest.approx(33.33, abs=0.01)


def test_supported_breakdown_per_arm(joined):
    j, _ = joined
    b = JP.supported_breakdown(j, arm=LOCAL)
    assert b["supported_total"] == 4
    assert b["on_no_basis"]["n"] == 2


def test_breakdown_of_empty_set_is_none_not_zero_division():
    b = JP.supported_breakdown([])
    assert b["supported_total"] == 0
    assert b["on_no_basis"]["pct"] is None


# ── true failures ────────────────────────────────────────────────────────


def test_true_failure_is_supported_on_no_basis_only(joined):
    j, _ = joined
    fails = JP.true_failures(j)
    assert len(fails) == 2
    assert {f["field_name"] for f in fails} == {"country", "clinical_readiness_assessment"}


def test_stitched_and_drifted_are_not_failures(joined):
    j, _ = joined
    fails = JP.true_failures(j)
    assert all(f["taxonomy_class"] == C.UNTRACEABLE_NO_BASIS for f in fails)
    assert not any(f["taxonomy_class"] in JP.REAL_BUT_NONCONFORMING for f in fails)


def test_true_failure_crosstab_splits_by_field_class(joined):
    j, _ = joined
    x = JP.true_failure_crosstab(j)
    assert x["POOLED"]["inferable"]["failures"] == 1       # country
    assert x["POOLED"]["judgment"]["failures"] == 1        # clinical_readiness
    assert x["POOLED"]["stated"]["failures"] == 0
    assert x[LOCAL]["ALL"]["failures"] == 2


# ── symmetry ─────────────────────────────────────────────────────────────


def test_symmetry_counts_unsupported_on_anchored(joined):
    j, _ = joined
    s = JP.symmetry_check(j)
    assert s["openai_o4_mini_high"]["anchored_rows"] == 1
    assert s["openai_o4_mini_high"]["unsupported_on_anchored"] == 1
    assert s["POOLED"]["anchored_rows"] == 2
    assert s["POOLED"]["unsupported_on_anchored"] == 1


# ── absence interaction ──────────────────────────────────────────────────


def test_absence_interaction_reports_verdicts_and_patterns(joined):
    j, _ = joined
    a = JP.absence_interaction(j)
    assert a["absence_claim_rows_judged"] == 1
    assert a["verdicts"] == {"SUPPORTED": 1}
    assert a["by_pattern"] == {"P1_referent_negation": 1}


# ── legacy disclosure ────────────────────────────────────────────────────


def test_legacy_restatement_uses_only_rows_with_a_legacy_verdict(joined):
    j, _ = joined
    legacy = {
        (LOCAL, 1, "country"): False,
        (LOCAL, 1, "robot_platform"): True,
        # remaining rows deliberately absent from the legacy map
    }
    out = JP.legacy_restatement(j, legacy)
    assert out["supported_with_legacy_verdict"] == 2
    assert out["legacy_unanchored"] == 1
    assert out["pct"] == 50.0


def test_legacy_restatement_is_none_without_data(joined):
    j, _ = joined
    assert JP.legacy_restatement(j, {}) is None


# ── verdict x class table ────────────────────────────────────────────────


def test_verdict_by_class_covers_every_row(joined):
    j, _ = joined
    t = JP.verdict_by_class(j)
    assert sum(sum(v.values()) for v in t.values()) == len(j)
    assert t["SUPPORTED"][C.UNTRACEABLE_NO_BASIS] == 2
    assert t["UNSUPPORTED"][C.ANCHORED] == 1
