"""Tests for LOW_YIELD extraction threshold detection.

Covers: field counting, threshold flagging, configurable threshold,
audit queue integration, PRISMA reporting.
"""

import json
from pathlib import Path

import pytest

from engine.agents.auditor import count_populated_fields
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.search.models import Citation
from engine.core.codebook import load_codebook


SPEC_PATH = Path(__file__).resolve().parent.parent / "review_specs" / "surgical_autonomy.yaml"
LIVE_CODEBOOK = SPEC_PATH.parent.parent / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
#: LOW_YIELD's absence set is the codebook's (R136), passed in by check_low_yield.
ABSENCE = load_codebook(LIVE_CODEBOOK).absence_sentinel_set


@pytest.fixture
def spec():
    return load_review_spec(SPEC_PATH)


@pytest.fixture
def tmp_db(tmp_path):
    db = ReviewDatabase("test_review", data_root=tmp_path)
    yield db
    db.close()


def _add_paper(db, title="Test Paper", pmid=None):
    cit = Citation(
        title=title, abstract="Abstract text",
        pmid=pmid, doi=None, source="pubmed",
        authors=["A"], journal="J Test", year=2024,
    )
    db.add_papers([cit])
    row = db._conn.execute(
        "SELECT id FROM papers WHERE title = ?", (title,)
    ).fetchone()
    return row["id"]


# ── count_populated_fields Tests ─────────────────────────────────


def _rows(d: dict) -> list[dict]:
    """The reader-row shape `audit_events.low_yield` passes (R166, 9c-C6)."""
    return [{"field_name": k, "value": v} for k, v in d.items()]


class TestCountPopulatedFields:
    """9c-C6 (R166, B5): the cases are unchanged; they are passed as reader rows
    because the v1 dict shape retired."""

    def test_all_populated(self):
        data = _rows({
            "study_type": "Original Research",
            "robot_platform": "STAR",
            "task_performed": "suturing",
            "sample_size": "20 trials",
            "country": "USA",
        })
        assert count_populated_fields(data, absence_sentinels=ABSENCE) == 5

    def test_with_absence_values(self):
        data = _rows({
            "study_type": "Original Research",
            "robot_platform": "STAR",
            "fda_status": "NR",
            "comparison_to_human": "NR",  # R128: the codebook now directs NR here
            "key_limitation": "NOT_FOUND",
        })
        assert count_populated_fields(data, absence_sentinels=ABSENCE) == 2  # only study_type and robot_platform

    def test_with_null_and_empty(self):
        data = _rows({
            "study_type": "Original Research",
            "robot_platform": None,
            "task_performed": "",
            "sample_size": "   ",
        })
        assert count_populated_fields(data, absence_sentinels=ABSENCE) == 1  # only study_type

    def test_empty_list(self):
        assert count_populated_fields([], absence_sentinels=ABSENCE) == 0

    def test_the_absence_set_is_the_codebooks(self):
        """R136 (rewritten under B5 from test_all_absence): every codebook
        sentinel is absence, in any case; nothing else is."""
        sentinels = _rows({f"f{i}": v for i, v in enumerate(
            ["NR", "N/A", "NA", "NOT_FOUND", "NOT FOUND", "NOT REPORTED", " nr "])})
        assert count_populated_fields(sentinels, absence_sentinels=ABSENCE) == 0

    def test_a_declared_value_and_an_undeclared_legacy_form_both_count(self):
        """R136: "Not assessable" is a declared ordinal value of
        clinical_readiness_assessment, and "Not discussed" is declared nowhere.
        Neither is an absence sentinel, so both count as populated."""
        data = _rows({"clinical_readiness_assessment": "Not assessable",
                      "f": "Not discussed"})
        assert count_populated_fields(data, absence_sentinels=ABSENCE) == 2


# ── check_low_yield Tests ────────────────────────────────────────


class TestCheckLowYield:

    # test_paper_below_threshold_flagged, test_paper_above_threshold_not_flagged and
    # test_threshold_configurable retired 2026-09-25 with check_low_yield (9b-FLIP,
    # R111; R47). LOW_YIELD is computed on read now: tests/test_audit_events.py T10.

    def test_threshold_from_review_spec(self, spec):
        """Verify spec loads the threshold correctly."""
        assert spec.low_yield_threshold == 4


# ── Audit Queue Integration Tests ────────────────────────────────
# TestLowYieldInAuditQueue (2 tests) retired 2026-09-25 with the audit
# adjudicator it drove (9c-C3, R162; R47).


# ── PRISMA Tests ─────────────────────────────────────────────────


# TestPrismaLowYield (2 ids) retired under R187 (R47): papers_rejected,
# rejection_reasons and low_yield_rejected are no longer PRISMA quantities —
# no writer reaches REJECTED and low yield is a per-arm reader function.


# ── Database Schema Tests ────────────────────────────────────────
# TestLowYieldSchema (2 ids) retired under R200 (R47, B17): they pinned the
# legacy `extractions.low_yield` column, whose only writer (add_extraction)
# retired with R160b. The column stays on disk under R25.
