"""Tests for the three-way field classification and the census recount.

Same treatment as TAXONOMY-CENSUS-02's classifier tests: the classification
table is data, so it is tested for completeness and for agreement with its
pre-registration document; the tabulation code is tested on synthetic rows where
the expected counts are obvious by hand.
"""

import re
from pathlib import Path

import pytest

from analysis.provenance import classifier as C
from analysis.provenance import recount as R
from analysis.provenance.field_class import FIELD_CLASS as FIELD_CLASS_BINARY
from analysis.provenance.field_class3 import (
    CLASSES,
    FIELD_CLASS3,
    INFERABLE,
    JUDGMENT,
    PAPER_VARIABLE,
    STATED,
    basis,
    field_class3,
    fields_by_class,
)

DOC = Path(__file__).resolve().parents[3] / "analysis" / "provenance" / "FIELD_CLASSES.md"


# ── the classification table ─────────────────────────────────────────────


def test_covers_exactly_the_twenty_codebook_fields():
    assert len(FIELD_CLASS3) == 20
    assert set(FIELD_CLASS3) == set(FIELD_CLASS_BINARY), (
        "the three-way axis must cover the same fields as the binary one it supersedes"
    )


def test_every_field_has_a_valid_class_basis_and_justification():
    for name, (cls, bas, why) in FIELD_CLASS3.items():
        assert cls in CLASSES, name
        assert bas in ("measured", "reasoned"), name
        assert len(why.strip()) > 40, f"{name} lacks a real justification"


def test_class_totals_match_the_pinned_document():
    assert len(fields_by_class(STATED)) == 9
    assert len(fields_by_class(INFERABLE)) == 6
    assert len(fields_by_class(JUDGMENT)) == 5


def test_axes_are_not_nested():
    """The whole point of the reclassification: INFERABLE is drawn from both
    binary classes, so the two cross-tabs are not a relabelling of each other."""
    inferable = fields_by_class(INFERABLE)
    binaries = {FIELD_CLASS_BINARY[f][0] for f in inferable}
    assert binaries == {"extractive", "interpretive"}
    assert FIELD_CLASS_BINARY["country"][0] == "extractive"
    assert field_class3("country") == INFERABLE


def test_judgment_and_stated_map_cleanly_onto_the_old_axis():
    for f in fields_by_class(JUDGMENT):
        assert FIELD_CLASS_BINARY[f][0] == "interpretive", f
    for f in fields_by_class(STATED):
        assert FIELD_CLASS_BINARY[f][0] == "extractive", f


def test_measured_fields_are_the_ones_with_a_declared_sample():
    assert {f for f in FIELD_CLASS3 if basis(f) == "measured"} == {
        "country", "primary_outcome_value", "sample_size",
    }


def test_paper_variable_fields_are_flagged_in_the_document():
    assert PAPER_VARIABLE == {"sample_size", "surgical_domain"}
    text = DOC.read_text()
    for f in PAPER_VARIABLE:
        assert f in text


def test_module_agrees_with_its_pre_registration_document():
    """FIELD_CLASSES.md §2 is the authority; this module must mirror it."""
    text = DOC.read_text()
    for name, (cls, _, _) in FIELD_CLASS3.items():
        row = next((ln for ln in text.splitlines()
                    if re.search(rf"\|\s*{re.escape(name)}\s*\|", ln)), None)
        assert row is not None, f"{name} missing from FIELD_CLASSES.md"
        assert cls.upper() in row, f"{name}: doc and module disagree on class"


def test_unknown_field_returns_empty_not_an_exception():
    """Run 6 contains two spans whose field_name is not a codebook field."""
    assert field_class3("Title") == ""
    assert field_class3("field_1") == ""


# ── tabulation ───────────────────────────────────────────────────────────


def _row(arm, field, taxonomy, snippet_chars=50):
    return {
        "arm": arm,
        "paper_id": 1,
        "field_name": field,
        "taxonomy_class": taxonomy,
        "snippet_chars": snippet_chars,
        "field_class_binary": FIELD_CLASS_BINARY.get(field, ("",))[0],
    }


@pytest.fixture
def rows():
    data = [
        _row("local", "country", C.UNTRACEABLE_NO_BASIS),
        _row("local", "country", C.ANCHORED),
        _row("local", "task_select", C.UNTRACEABLE_NO_BASIS),
        _row("local", "clinical_readiness_assessment", C.UNTRACEABLE_NO_BASIS),
        _row("local", "clinical_readiness_assessment", C.ANCHORED),
        _row("local", "primary_outcome_value", C.ANCHORED),
        # an absence claim: must leave the no-basis denominator entirely
        _row("local", "country", C.ABSENCE_CLAIM),
        # an empty-snippet span: excluded from the denominator too
        _row("local", "country", C.ABSENCE_DECLARED, snippet_chars=0),
        _row("cloud", "country", C.ANCHORED),
        _row("cloud", "task_select", C.STITCHED),
        _row("cloud", "Title", C.UNTRACEABLE_NO_BASIS),   # unknown field
    ]
    return R.annotate(data)


def test_annotate_adds_the_axis_without_touching_taxonomy(rows):
    assert all("field_class3" in r for r in rows)
    assert rows[0]["field_class3"] == INFERABLE
    assert rows[0]["taxonomy_class"] == C.UNTRACEABLE_NO_BASIS


def test_distribution_counts_by_group(rows):
    d = R.distribution(rows, "field_class3")
    assert d[INFERABLE][C.UNTRACEABLE_NO_BASIS] == 2      # country + task_select
    assert d[JUDGMENT][C.UNTRACEABLE_NO_BASIS] == 1
    assert d[STATED][C.ANCHORED] == 1
    assert d[""][C.UNTRACEABLE_NO_BASIS] == 1             # the unknown field


def test_distribution_covers_every_span(rows):
    d = R.distribution(rows, "field_class3")
    assert sum(sum(v.values()) for v in d.values()) == len(rows)


def test_absence_claims_and_empty_snippets_leave_the_denominator(rows):
    res = R.no_basis_rate(rows, "field_class3", INFERABLE, arm="local")
    # local INFERABLE spans: country(no_basis), country(anchored), task_select(no_basis),
    # country(ABSENCE_CLAIM -> excluded), country(empty snippet -> excluded)
    assert res["denominator"] == 3
    assert res["no_basis"] == 2
    assert res["rate_pct"] == pytest.approx(66.67, abs=0.01)


def test_no_basis_rate_handles_empty_group():
    res = R.no_basis_rate([], "field_class3", STATED)
    assert res == {"no_basis": 0, "denominator": 0, "rate_pct": None}


def test_cross_tab_splits_by_arm(rows):
    x = R.cross_tab(rows, "field_class3")
    assert set(x) == {"local", "cloud"}
    assert x["cloud"][INFERABLE][C.STITCHED] == 1
    assert x["local"][INFERABLE][C.UNTRACEABLE_NO_BASIS] == 2


def test_per_field_table_is_sorted_and_carries_both_axes(rows):
    t = R.per_field_table(rows)
    fields = [e["field"] for e in t]
    assert "country" in fields
    entry = next(e for e in t if e["field"] == "country")
    assert entry["class3"] == INFERABLE
    assert entry["class_binary"] == "extractive"
    assert entry["absence_claims"] == 1
    rates = [e["rate_pct"] for e in t if e["rate_pct"] is not None]
    assert rates == sorted(rates, reverse=True)


def test_coverage_check_reports_unknown_fields(rows):
    cov = R.coverage_check(rows)
    assert cov["spans"] == len(rows)
    assert cov["unclassified_three_way"] == {"Title": 1}
    assert cov["axes_agree_on_coverage"] is True


def test_build_report_shape(rows):
    rep = R.build_report("run-x", rows)
    assert rep["census_run_id"] == "run-x"
    assert set(rep["three_way"]["no_basis"]) == set(CLASSES)
    assert set(rep["binary"]["no_basis"]) == {"extractive", "interpretive"}
    assert rep["coverage"]["spans"] == len(rows)
    assert len(rep["field_assignments"]) == 20
