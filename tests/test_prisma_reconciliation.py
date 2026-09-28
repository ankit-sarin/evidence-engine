"""Tests for PRISMA count reconciliation on the freshman count model (R183–R196).

The screening side is declared on `papers.status` and the extraction side on the
event store, both through `seed_prisma_world` (tests/_event_store_fixture.py).
"""

import re
from pathlib import Path

import pytest

from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.exporters.prisma import (
    SCREENING_TOKENS, export_prisma_csv, generate_prisma_flow, validate_prisma_counts,
)
from engine.search.models import Citation
from tests._event_store_fixture import add_values, seed_eligibility, seed_prisma_world


SPEC_PATH = "review_specs/surgical_autonomy.yaml"
PRISMA_SRC = Path(__file__).resolve().parent.parent / "engine" / "exporters" / "prisma.py"


@pytest.fixture
def spec():
    return load_review_spec(SPEC_PATH)


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("test_prisma", data_root=tmp_path)
    yield rdb
    rdb.close()


def _add_papers(db, n, source="pubmed"):
    cits = [
        Citation(title=f"Paper {i}", source=source, pmid=str(i + 1000))
        for i in range(n)
    ]
    db.add_papers(cits)
    return [p["id"] for p in db.get_papers_by_status("INGESTED")]


class TestReconciliation:

    def test_reconciliation_passes_clean_db(self, db, spec):
        """Both identities hold on a fully categorized DB."""
        _add_papers(db, 10)
        seed_prisma_world(
            db,
            screening=[("ABSTRACT_SCREENED_OUT", None)] * 4
            + [("PDF_EXCLUDED", "NON_ENGLISH")] * 2
            + [("FT_SCREENED_OUT", None)],
            eligible=[("audited_ai", None)] * 3,
        )

        result = validate_prisma_counts(db)
        assert result["valid"] is True
        assert result["total_db"] == 10
        assert result["discrepancy"] == 0

    def test_reconciliation_catches_mismatch(self, tmp_path):
        """B5 rewrite (R193): the seam is compared as SETS, and each paper on the
        wrong side is named with its status token.

        (i) eligible on events, INGESTED on status (9d route C);
        (ii) a paper at REJECTED — no writer, no box.

        Mutation note: with the two seam `details.append` blocks in
        `validate_prisma_counts` removed, (i) passes reconciliation (the status
        partition alone still sums) and this test fails.
        """
        # (i)
        db1 = ReviewDatabase("mismatch_seam", data_root=tmp_path)
        _add_papers(db1, 3)
        world = seed_prisma_world(db1, screening=[("ABSTRACT_SCREENED_OUT", None)],
                                  eligible=[("audited_ai", None)])
        ghost = max(world["screening"] + world["eligible"]) + 1
        assert db1._conn.execute("SELECT status FROM papers WHERE id = ?",
                                 (ghost,)).fetchone()[0] == "INGESTED"
        seed_eligibility(db1._conn, ghost)
        with pytest.raises(ValueError, match="PRISMA reconciliation failed") as exc:
            validate_prisma_counts(db1)
        assert f"{ghost} (INGESTED)" in str(exc.value)
        assert "eligible on the eligibility axis but at a screening token" in str(exc.value)
        db1.close()

        # (ii)
        db2 = ReviewDatabase("mismatch_rejected", data_root=tmp_path)
        _add_papers(db2, 3)
        world = seed_prisma_world(db2, screening=[("ABSTRACT_SCREENED_OUT", None)],
                                  eligible=[("audited_ai", None)])
        rejected = max(world["screening"] + world["eligible"]) + 1
        db2._conn.execute("UPDATE papers SET status = 'REJECTED' WHERE id = ?", (rejected,))
        db2._conn.commit()
        with pytest.raises(ValueError, match="PRISMA reconciliation failed") as exc:
            validate_prisma_counts(db2)
        assert f"{rejected} (REJECTED)" in str(exc.value)
        assert "not eligible on the eligibility axis" in str(exc.value)
        db2.close()

    def test_in_progress_papers_counted(self, db):
        """Papers mid-pipeline land in one of the two in-progress boxes, not lost."""
        _add_papers(db, 10)
        seed_prisma_world(
            db,
            screening=[("ABSTRACT_SCREENED_IN", None)] * 2
            + [("PARSED", None), ("FT_FLAGGED", None)]
            + [("ABSTRACT_SCREENED_OUT", None)] * 2,
            eligible=[(None, None), ("parsed", None), ("extracted", None),
                      ("audited_ai", None)],
        )

        flow = generate_prisma_flow(db)
        assert flow["screening_in_progress"] == 4  # SCREENED_IN(2) + PARSED(1) + FT_FLAGGED(1)
        assert flow["extraction_in_progress"] == 3  # none, parsed, extracted
        assert flow["studies_included"] == 1
        assert flow["records_excluded"] == 2
        assert flow["n_eligible"] == 4

        result = validate_prisma_counts(db)
        assert result["valid"] is True


class TestPDFExcludedSubcounts:

    def test_pdf_excluded_subcounts_sum(self, db):
        """PDF_EXCLUDED sub-counts by reason sum to total."""
        pids = _add_papers(db, 5)

        reasons = ["NON_ENGLISH", "NOT_MANUSCRIPT", "NOT_MANUSCRIPT", "INACCESSIBLE", "NON_ENGLISH"]
        for pid, reason in zip(pids, reasons):
            db.update_status(pid, "ABSTRACT_SCREENED_IN")
            db.update_status(pid, "PDF_ACQUIRED")
            db._conn.execute(
                "UPDATE papers SET pdf_exclusion_reason = ? WHERE id = ?",
                (reason, pid),
            )
            db.update_status(pid, "PDF_EXCLUDED")

        flow = generate_prisma_flow(db)
        assert flow["pdf_excluded"] == 5
        assert sum(flow["pdf_exclusion_reasons"].values()) == 5
        assert flow["pdf_exclusion_reasons"]["NON_ENGLISH"] == 2
        assert flow["pdf_exclusion_reasons"]["NOT_MANUSCRIPT"] == 2
        assert flow["pdf_exclusion_reasons"]["INACCESSIBLE"] == 1


class TestNoDoubleCount:

    def test_no_paper_in_multiple_terminal_boxes(self, db):
        """Each paper appears in exactly one box; included comes from events."""
        _add_papers(db, 4)
        seed_prisma_world(
            db,
            screening=[("ABSTRACT_SCREENED_OUT", None), ("PDF_EXCLUDED", "NON_ENGLISH"),
                       ("FT_SCREENED_OUT", None)],
            eligible=[("audited_ai", None)],
        )

        flow = generate_prisma_flow(db)
        assert flow["records_excluded"] == 1
        assert flow["pdf_excluded"] == 1
        assert flow["ft_screened_out"] == 1
        assert flow["studies_included"] == 1
        assert flow["screening_in_progress"] == 0
        assert flow["extraction_in_progress"] == 0

        result = validate_prisma_counts(db)
        assert result["valid"] is True
        assert result["total_db"] == 4

    def test_ai_audit_complete_not_double_counted_with_ft(self, db):
        """An audited paper is counted once in studies_included; an eligible paper
        with no processing record is extraction-in-progress, and both are counted
        once in full_text_assessed."""
        _add_papers(db, 2)
        seed_prisma_world(db, eligible=[(None, None), ("audited_ai", None)])

        flow = generate_prisma_flow(db)
        assert flow["studies_included"] == 1
        assert flow["extraction_in_progress"] == 1
        assert flow["full_text_assessed"] == 2

        result = validate_prisma_counts(db)
        assert result["valid"] is True
        assert result["total_db"] == 2


class TestExtractFailed:

    def test_extract_failed_appears_in_flow_and_csv(self, db, tmp_path):
        """Failed extractions are counted per processing token and rendered one
        CSV line per reason code."""
        _add_papers(db, 6)
        seed_prisma_world(
            db,
            screening=[("ABSTRACT_SCREENED_OUT", None)] * 2,
            eligible=[("extraction_failed", "model_call_failed")] * 2
            + [("input_exceeds_context", "input_overflow_estimated"),
               ("audited_ai", None)],
        )

        flow = generate_prisma_flow(db)
        assert flow["extraction_failed"] == 2
        assert flow["failure_reasons"]["extraction_failed"] == {"model_call_failed": 2}
        assert flow["input_exceeds_context"] == 1
        assert flow["studies_included"] == 1  # only the audited paper

        result = validate_prisma_counts(db)
        assert result["valid"] is True

        csv_path = str(tmp_path / "prisma.csv")
        export_prisma_csv(db, csv_path)
        content = open(csv_path).read()
        assert "Extraction failed,2,model_call_failed" in content
        assert "Input exceeds context,1,input_overflow_estimated" in content

    def test_extract_failed_zero_omitted_from_csv(self, db, tmp_path):
        """With no failure events, no failure line is rendered."""
        _add_papers(db, 3)
        seed_prisma_world(db, screening=[("ABSTRACT_SCREENED_OUT", None)] * 2,
                          eligible=[("audited_ai", None)])

        flow = generate_prisma_flow(db)
        assert flow["extraction_failed"] == 0

        csv_path = str(tmp_path / "prisma.csv")
        export_prisma_csv(db, csv_path)
        content = open(csv_path).read()
        assert "Extraction failed" not in content


class TestCountModel:

    def test_prisma_names_only_screening_tokens(self):
        """R184(e) as amended: prisma.py names no extraction-stage status token,
        takes no complement over status counts, and does not reach the frozen
        corpus module."""
        text = PRISMA_SRC.read_text()
        for banned in ("EXTRACTED", "EXTRACT_FAILED", "AI_AUDIT_COMPLETE",
                       "HUMAN_AUDIT_COMPLETE", "REJECTED", "status_counts.items()",
                       "engine.core.corpus"):
            assert banned not in text, banned
        assert len(SCREENING_TOKENS) == 9
        named = set(re.findall(r'"([A-Z][A-Z_]+)"', text))
        assert named - {"INACCESSIBLE"} <= SCREENING_TOKENS

    def test_bare_add_values_paper_passes_identity_1(self, tmp_path):
        """R191: a bare `add_values` paper is FT_ELIGIBLE on status (the toolkit
        DDL default) and eligible on events, so the reconciler passes with no
        further declaration.

        `ReviewDatabase` cannot open a toolkit-built file (9d route B), so the
        three screening-side reads the flow makes are given empty shapes here by
        raw SQL; they retire at the screeners' cut-over (R163 precedent).
        """
        import sqlite3
        from types import SimpleNamespace

        path = tmp_path / "bare.db"
        add_values(path, "local", "study_design", ["RCT", "Cohort"], start_paper=1)
        conn = sqlite3.connect(str(path))
        conn.row_factory = sqlite3.Row
        conn.executescript(
            "ALTER TABLE papers ADD COLUMN pdf_exclusion_reason TEXT;"
            "CREATE TABLE abstract_screening_decisions "
            "(paper_id INTEGER, decision TEXT, rationale TEXT);"
            "CREATE TABLE ft_screening_adjudication "
            "(paper_id INTEGER, adjudication_decision TEXT);")
        try:
            statuses = {r[0] for r in conn.execute("SELECT status FROM papers")}
            assert statuses == {"FT_ELIGIBLE"}
            result = validate_prisma_counts(SimpleNamespace(_conn=conn))
            assert result["valid"] is True
            assert result["total_db"] == 2
        finally:
            conn.close()
