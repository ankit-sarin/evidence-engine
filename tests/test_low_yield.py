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
from engine.exporters.prisma import generate_prisma_flow
from engine.search.models import Citation
from engine.core.codebook import load_codebook, load_codebook_beside


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


def _advance_to_ai_audit(db, pid, extracted_data, spec):
    """Move paper through to AI_AUDIT_COMPLETE with given extracted_data."""
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    db.update_status(pid, "PARSED")
    db.update_status(pid, "EXTRACTED")

    ext_id = db.add_extraction(
        pid, load_codebook_beside(db.db_path).semantic_hash, extracted_data,
        "reasoning trace", "deepseek-r1:32b",
    )

    # Add evidence spans for populated fields
    for fname, value in extracted_data.items():
        if value and not load_codebook_beside(db.db_path).is_absence_sentinel(value):
            db.add_evidence_span(ext_id, fname, value, "Source text here.", 0.9)

    # Audit all spans as verified
    spans = db._conn.execute(
        "SELECT id FROM evidence_spans WHERE extraction_id = ?", (ext_id,)
    ).fetchall()
    for s in spans:
        db.update_audit(s["id"], "verified", "gemma3:27b", "OK")

    db.update_status(pid, "AI_AUDIT_COMPLETE")
    return ext_id


# ── count_populated_fields Tests ─────────────────────────────────


class TestCountPopulatedFields:

    def test_all_populated(self):
        data = {
            "study_type": "Original Research",
            "robot_platform": "STAR",
            "task_performed": "suturing",
            "sample_size": "20 trials",
            "country": "USA",
        }
        assert count_populated_fields(data, absence_sentinels=ABSENCE) == 5

    def test_with_absence_values(self):
        data = {
            "study_type": "Original Research",
            "robot_platform": "STAR",
            "fda_status": "NR",
            "comparison_to_human": "NR",  # R128: the codebook now directs NR here
            "key_limitation": "NOT_FOUND",
        }
        assert count_populated_fields(data, absence_sentinels=ABSENCE) == 2  # only study_type and robot_platform

    def test_with_null_and_empty(self):
        data = {
            "study_type": "Original Research",
            "robot_platform": None,
            "task_performed": "",
            "sample_size": "   ",
        }
        assert count_populated_fields(data, absence_sentinels=ABSENCE) == 1  # only study_type

    def test_empty_dict(self):
        assert count_populated_fields({}, absence_sentinels=ABSENCE) == 0

    def test_the_absence_set_is_the_codebooks(self):
        """R136 (rewritten under B5 from test_all_absence): every codebook
        sentinel is absence, in any case; nothing else is."""
        sentinels = {f"f{i}": v for i, v in enumerate(
            ["NR", "N/A", "NA", "NOT_FOUND", "NOT FOUND", "NOT REPORTED", " nr "])}
        assert count_populated_fields(sentinels, absence_sentinels=ABSENCE) == 0

    def test_a_declared_value_and_an_undeclared_legacy_form_both_count(self):
        """R136: "Not assessable" is a declared ordinal value of
        clinical_readiness_assessment, and "Not discussed" is declared nowhere.
        Neither is an absence sentinel, so both count as populated."""
        data = {"clinical_readiness_assessment": "Not assessable",
                "f": "Not discussed"}
        assert count_populated_fields(data, absence_sentinels=ABSENCE) == 2


# ── check_low_yield Tests ────────────────────────────────────────


def _seed_low_yield(db, pid):
    """The legacy low_yield flag readers 10–14 still read, set directly: its writer
    (check_low_yield) retired at the cut-over, and these readers move in slice 3."""
    db._conn.execute("UPDATE extractions SET low_yield = 1 WHERE id = (SELECT MAX(id) "
                     "FROM extractions WHERE paper_id = ?)", (pid,))
    db._conn.commit()


class TestCheckLowYield:

    # test_paper_below_threshold_flagged, test_paper_above_threshold_not_flagged and
    # test_threshold_configurable retired 2026-09-25 with check_low_yield (9b-FLIP,
    # R111; R47). LOW_YIELD is computed on read now: tests/test_audit_events.py T10.

    def test_threshold_from_review_spec(self, spec):
        """Verify spec loads the threshold correctly."""
        assert spec.low_yield_threshold == 4


# ── Audit Queue Integration Tests ────────────────────────────────


class TestLowYieldInAuditQueue:

    def test_low_yield_papers_in_audit_export(self, tmp_db, tmp_path, spec):
        """LOW_YIELD papers should appear in the exported audit queue."""
        from engine.adjudication.audit_adjudicator import (
            _collect_papers_for_review,
            export_audit_review_queue,
        )

        pid = _add_paper(tmp_db, title="Sparse Export Paper", pmid="60001")
        sparse_data = {
            "study_type": "Original Research",
            "robot_platform": "STAR",
            "task_performed": "NR",
            "sample_size": "NR",
        }
        _advance_to_ai_audit(tmp_db, pid, sparse_data, spec)

        # Flag as low_yield
        _seed_low_yield(tmp_db, pid)

        # Collect papers for review
        papers = _collect_papers_for_review(tmp_db, spot_check_pct=0)
        assert len(papers) == 1
        assert papers[0]["paper_id"] == pid
        assert papers[0]["review_reason"] == "low_yield"
        assert papers[0]["low_yield"] is True

    def test_export_includes_low_yield_spans(self, tmp_db, tmp_path, spec):
        """Exported XLSX should show LOW_YIELD audit state for low-yield paper spans."""
        from engine.adjudication.audit_adjudicator import export_audit_review_queue

        pid = _add_paper(tmp_db, title="LY XLSX Paper", pmid="60002")
        sparse_data = {
            "study_type": "Original Research",
            "robot_platform": "STAR",
            "task_performed": "NR",
        }
        _advance_to_ai_audit(tmp_db, pid, sparse_data, spec)
        _seed_low_yield(tmp_db, pid)

        out = tmp_path / "audit_queue.xlsx"
        result = export_audit_review_queue(tmp_db, out, spot_check_pct=0)
        assert result["low_yield"] == 1

        from openpyxl import load_workbook
        wb = load_workbook(out)
        ws = wb["Review Queue"]
        headers = [cell.value for cell in ws[1]]
        assert "Audit State" in headers

        # LOW_YIELD papers have verified spans shown as "LOW_YIELD" audit state
        audit_col = headers.index("Audit State")
        audit_values = []
        for row in ws.iter_rows(min_row=2, values_only=False):
            if row[0].value is not None:
                audit_values.append(row[audit_col].value)
        assert any(v == "LOW_YIELD" for v in audit_values)


# ── PRISMA Tests ─────────────────────────────────────────────────


class TestPrismaLowYield:

    def test_prisma_includes_low_yield_rejected(self, tmp_db, spec):
        """PRISMA flow should report LOW_YIELD rejections as a distinct category."""
        pid = _add_paper(tmp_db, title="Rejected LY Paper", pmid="70001")
        sparse_data = {
            "study_type": "Original Research",
            "robot_platform": "NR",
        }
        _advance_to_ai_audit(tmp_db, pid, sparse_data, spec)
        _seed_low_yield(tmp_db, pid)

        # Reject the paper with low_yield reason
        tmp_db.reject_paper(pid, "low_yield_excluded: too few populated fields")

        flow = generate_prisma_flow(tmp_db)
        assert flow["papers_rejected"] == 1
        assert flow["low_yield_rejected"] == 1
        assert "low_yield_excluded" in str(flow["rejection_reasons"])

    def test_prisma_no_low_yield_when_none_rejected(self, tmp_db):
        """PRISMA low_yield_rejected should be 0 when no such rejections exist."""
        flow = generate_prisma_flow(tmp_db)
        assert flow["low_yield_rejected"] == 0


# ── Database Schema Tests ────────────────────────────────────────


class TestLowYieldSchema:

    def test_extractions_has_low_yield_column(self, tmp_db):
        """The extractions table should have a low_yield column."""
        row = tmp_db._conn.execute(
            "PRAGMA table_info(extractions)"
        ).fetchall()
        col_names = [r["name"] for r in row]
        assert "low_yield" in col_names

    def test_low_yield_defaults_to_zero(self, tmp_db, spec):
        """New extractions should have low_yield=0 by default."""
        pid = _add_paper(tmp_db, title="Default Test", pmid="80001")
        tmp_db.update_status(pid, "ABSTRACT_SCREENED_IN")
        tmp_db.update_status(pid, "PDF_ACQUIRED")
        tmp_db.update_status(pid, "PARSED")
        tmp_db.update_status(pid, "EXTRACTED")

        ext_id = tmp_db.add_extraction(
            pid, None, {"study_type": "RCT"}, "trace", "model",
            codebook_hash=load_codebook_beside(tmp_db.db_path).semantic_hash,
        )
        row = tmp_db._conn.execute(
            "SELECT low_yield FROM extractions WHERE id = ?", (ext_id,)
        ).fetchone()
        assert row["low_yield"] == 0
