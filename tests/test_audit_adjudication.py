"""Tests for audit adjudication: export/import round-trip, spot-check, reject cascade, min_status."""

import json
from pathlib import Path

import pytest

from engine.adjudication.audit_adjudicator import (
    _collect_papers_for_review,
    check_audit_review_gate,
    export_audit_review_queue,
    import_audit_review_decisions,
)
from engine.adjudication.workflow import (
    complete_stage,
    is_stage_done,
)
from engine.core.database import ReviewDatabase, _STATUS_ORDER
from engine.search.models import Citation


# ── Fixtures ──────────────────────────────────────────────────────────


def _make_citation(pmid: str, title: str = "Test Paper") -> Citation:
    return Citation(
        title=title, abstract="test abstract", pmid=pmid,
        source="pubmed", authors=["Author A"], journal="J Test", year=2024,
    )


def _add_paper_with_extraction(db, pmid, *, spans, status="AI_AUDIT_COMPLETE"):
    """Add a paper, extraction, and evidence spans. Returns paper_id."""
    db.add_papers([_make_citation(pmid, title=f"Paper {pmid}")])
    paper = db._conn.execute(
        "SELECT id FROM papers WHERE pmid = ?", (pmid,)
    ).fetchone()
    pid = paper["id"]

    # Walk through status transitions to reach AI_AUDIT_COMPLETE
    _transition_to(db, pid, status)

    # Insert extraction
    db._conn.execute(
        "INSERT INTO extractions (paper_id, extraction_schema_hash, extracted_data, model, extracted_at) "
        "VALUES (?, 'testhash', '{}', 'test', datetime('now'))",
        (pid,),
    )
    ext = db._conn.execute(
        "SELECT id FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1",
        (pid,),
    ).fetchone()
    ext_id = ext["id"]

    # Insert spans
    for s in spans:
        db._conn.execute(
            """INSERT INTO evidence_spans
               (extraction_id, field_name, value, source_snippet, confidence,
                audit_status, auditor_model, audit_rationale, audited_at)
               VALUES (?, ?, ?, ?, ?, ?, 'test_model', ?, datetime('now'))""",
            (ext_id, s["field_name"], s["value"], s.get("snippet", "some text"),
             s.get("confidence", 0.8), s["audit_status"],
             s.get("rationale", "")),
        )
    db._conn.commit()
    return pid


def _transition_to(db, pid, target):
    """Walk paper through valid transitions to reach target status."""
    transitions = {
        "INGESTED": [],
        "SCREENED_IN": ["SCREENED_IN"],
        "PDF_ACQUIRED": ["SCREENED_IN", "PDF_ACQUIRED"],
        "PARSED": ["SCREENED_IN", "PDF_ACQUIRED", "PARSED"],
        "EXTRACTED": ["SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED"],
        "AI_AUDIT_COMPLETE": ["SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED", "AI_AUDIT_COMPLETE"],
        "HUMAN_AUDIT_COMPLETE": ["SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED",
                                 "AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE"],
    }
    for step in transitions.get(target, []):
        try:
            db.update_status(pid, step)
        except ValueError:
            pass  # already at or past this status


@pytest.fixture
def db(tmp_path):
    d = ReviewDatabase("test_audit_adj", data_root=tmp_path)
    yield d
    d.close()


# ── Collection Tests ──────────────────────────────────────────────────


def test_collect_flagged_paper(db):
    """Papers with flagged spans should be collected for review."""
    _add_paper_with_extraction(db, "10001", spans=[
        {"field_name": "study_design", "value": "RCT", "audit_status": "verified"},
        {"field_name": "sample_size", "value": "50", "audit_status": "flagged"},
    ])

    papers = _collect_papers_for_review(db, spot_check_pct=0)
    assert len(papers) == 1
    assert papers[0]["worst_state"] == "flagged"
    assert papers[0]["review_reason"] == "audit_issues"
    assert len(papers[0]["problem_spans"]) == 1


def test_collect_contested_paper(db):
    """Papers with contested spans should be collected."""
    _add_paper_with_extraction(db, "10002", spans=[
        {"field_name": "study_design", "value": "RCT", "audit_status": "verified"},
        {"field_name": "sample_size", "value": "50", "audit_status": "contested"},
    ])

    papers = _collect_papers_for_review(db, spot_check_pct=0)
    assert len(papers) == 1
    assert papers[0]["worst_state"] == "contested"


def test_collect_spot_check(db):
    """All-verified papers should be spot-checked at configured rate."""
    # Create 10 all-verified papers
    for i in range(10):
        _add_paper_with_extraction(db, str(20000 + i), spans=[
            {"field_name": "study_design", "value": "RCT", "audit_status": "verified"},
        ])

    papers = _collect_papers_for_review(db, spot_check_pct=0.50)
    spot = [p for p in papers if p["review_reason"] == "spot_check"]
    assert len(spot) == 5  # 50% of 10


def test_collect_minimum_one_spot_check(db):
    """Even at spot_check_pct=0, at least 1 all-verified paper is sampled."""
    for i in range(5):
        _add_paper_with_extraction(db, str(30000 + i), spans=[
            {"field_name": "study_design", "value": "RCT", "audit_status": "verified"},
        ])

    papers = _collect_papers_for_review(db, spot_check_pct=0)
    # min 1 spot-check when there are all-verified papers (max(1, int(0)) = 1)
    assert len(papers) == 1
    assert papers[0]["review_reason"] == "spot_check"


# ── Export Tests ──────────────────────────────────────────────────────


def test_export_creates_xlsx(db, tmp_path):
    """Export should create an Excel file with expected sheets."""
    _add_paper_with_extraction(db, "40001", spans=[
        {"field_name": "study_design", "value": "RCT", "audit_status": "flagged"},
        {"field_name": "sample_size", "value": "100", "audit_status": "verified"},
    ])

    # Complete prerequisite stages for AUDIT_QUEUE_EXPORTED
    for stage in ("SCREENING_COMPLETE", "DIAGNOSTIC_SAMPLE_COMPLETE",
                   "CATEGORIES_CONFIGURED", "QUEUE_EXPORTED",
                   "ADJUDICATION_COMPLETE", "EXTRACTION_COMPLETE",
                   "AI_AUDIT_COMPLETE_STAGE"):
        complete_stage(db._conn, stage)

    out = tmp_path / "audit_queue.xlsx"
    stats = export_audit_review_queue(db, out, spot_check_pct=0)

    assert out.exists()
    assert stats["total"] == 1
    assert stats["flagged"] == 1

    from openpyxl import load_workbook
    wb = load_workbook(out)
    assert "Audit Review" in wb.sheetnames
    assert "Paper Summary" in wb.sheetnames
    assert "Instructions" in wb.sheetnames

    ws = wb["Audit Review"]
    headers = [cell.value for cell in ws[1]]
    assert "paper_id" in headers
    assert "accept_as_is" in headers
    assert "reject_paper" in headers
    assert "study_design_value" in headers
    assert "study_design_correction" in headers


def test_export_sets_workflow_stage(db, tmp_path):
    """Export should auto-advance AUDIT_QUEUE_EXPORTED."""
    _add_paper_with_extraction(db, "40002", spans=[
        {"field_name": "study_design", "value": "RCT", "audit_status": "contested"},
    ])

    for stage in ("SCREENING_COMPLETE", "DIAGNOSTIC_SAMPLE_COMPLETE",
                   "CATEGORIES_CONFIGURED", "QUEUE_EXPORTED",
                   "ADJUDICATION_COMPLETE", "EXTRACTION_COMPLETE",
                   "AI_AUDIT_COMPLETE_STAGE"):
        complete_stage(db._conn, stage)

    out = tmp_path / "audit_queue.xlsx"
    export_audit_review_queue(db, out, spot_check_pct=0)

    assert is_stage_done(db._conn, "AUDIT_QUEUE_EXPORTED")


# ── Import / Round-Trip Tests ─────────────────────────────────────────


def test_import_accept_as_is(db, tmp_path):
    """accept_as_is=TRUE should mark contested/flagged spans as verified."""
    pid = _add_paper_with_extraction(db, "50001", spans=[
        {"field_name": "study_design", "value": "RCT", "audit_status": "flagged"},
        {"field_name": "sample_size", "value": "100", "audit_status": "contested"},
    ])

    for stage in ("SCREENING_COMPLETE", "DIAGNOSTIC_SAMPLE_COMPLETE",
                   "CATEGORIES_CONFIGURED", "QUEUE_EXPORTED",
                   "ADJUDICATION_COMPLETE", "EXTRACTION_COMPLETE",
                   "AI_AUDIT_COMPLETE_STAGE"):
        complete_stage(db._conn, stage)

    out = tmp_path / "queue.xlsx"
    export_audit_review_queue(db, out, spot_check_pct=0)

    # Fill in accept_as_is
    from openpyxl import load_workbook
    wb = load_workbook(out)
    ws = wb["Audit Review"]
    headers = [cell.value for cell in ws[1]]
    accept_col = headers.index("accept_as_is") + 1
    ws.cell(row=2, column=accept_col, value="TRUE")
    wb.save(out)

    result = import_audit_review_decisions(db, out)
    assert result["stats"]["accepted"] == 1

    # Verify spans are now "verified"
    ext = db._conn.execute(
        "SELECT id FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1",
        (pid,),
    ).fetchone()
    spans = db._conn.execute(
        "SELECT audit_status FROM evidence_spans WHERE extraction_id = ?",
        (ext["id"],),
    ).fetchall()
    assert all(s["audit_status"] == "verified" for s in spans)


def test_import_correction_records_original(db, tmp_path):
    """Corrections should update span value and record original in audit_adjudication."""
    pid = _add_paper_with_extraction(db, "50002", spans=[
        {"field_name": "study_design", "value": "RCT", "audit_status": "flagged"},
        {"field_name": "sample_size", "value": "100", "audit_status": "verified"},
    ])

    for stage in ("SCREENING_COMPLETE", "DIAGNOSTIC_SAMPLE_COMPLETE",
                   "CATEGORIES_CONFIGURED", "QUEUE_EXPORTED",
                   "ADJUDICATION_COMPLETE", "EXTRACTION_COMPLETE",
                   "AI_AUDIT_COMPLETE_STAGE"):
        complete_stage(db._conn, stage)

    out = tmp_path / "queue.xlsx"
    export_audit_review_queue(db, out, spot_check_pct=0)

    from openpyxl import load_workbook
    wb = load_workbook(out)
    ws = wb["Audit Review"]
    headers = [cell.value for cell in ws[1]]

    # Set accept + correction
    accept_col = headers.index("accept_as_is") + 1
    correction_col = headers.index("study_design_correction") + 1
    ws.cell(row=2, column=accept_col, value="TRUE")
    ws.cell(row=2, column=correction_col, value="Prospective cohort")
    wb.save(out)

    result = import_audit_review_decisions(db, out)
    assert result["stats"]["corrected_fields"] == 1

    # Check span value was updated
    ext = db._conn.execute(
        "SELECT id FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1",
        (pid,),
    ).fetchone()
    span = db._conn.execute(
        "SELECT value FROM evidence_spans WHERE extraction_id = ? AND field_name = 'study_design'",
        (ext["id"],),
    ).fetchone()
    assert span["value"] == "Prospective cohort"

    # Check audit_adjudication table recorded the original
    adj = db._conn.execute(
        "SELECT * FROM audit_adjudication WHERE paper_id = ? AND field_name = 'study_design'",
        (pid,),
    ).fetchone()
    assert adj["original_value"] == "RCT"
    assert adj["override_value"] == "Prospective cohort"
    assert adj["human_decision"] == "override"


def test_import_transitions_to_human_audit_complete(db, tmp_path):
    """Paper should transition to HUMAN_AUDIT_COMPLETE when all spans resolved."""
    pid = _add_paper_with_extraction(db, "50003", spans=[
        {"field_name": "study_design", "value": "RCT", "audit_status": "flagged"},
    ])

    for stage in ("SCREENING_COMPLETE", "DIAGNOSTIC_SAMPLE_COMPLETE",
                   "CATEGORIES_CONFIGURED", "QUEUE_EXPORTED",
                   "ADJUDICATION_COMPLETE", "EXTRACTION_COMPLETE",
                   "AI_AUDIT_COMPLETE_STAGE"):
        complete_stage(db._conn, stage)

    out = tmp_path / "queue.xlsx"
    export_audit_review_queue(db, out, spot_check_pct=0)

    from openpyxl import load_workbook
    wb = load_workbook(out)
    ws = wb["Audit Review"]
    headers = [cell.value for cell in ws[1]]
    accept_col = headers.index("accept_as_is") + 1
    ws.cell(row=2, column=accept_col, value="TRUE")
    wb.save(out)

    import_audit_review_decisions(db, out)

    paper = db._conn.execute("SELECT status FROM papers WHERE id = ?", (pid,)).fetchone()
    assert paper["status"] == "HUMAN_AUDIT_COMPLETE"


def test_import_sets_audit_review_complete(db, tmp_path):
    """When all papers resolved, AUDIT_REVIEW_COMPLETE should be set."""
    _add_paper_with_extraction(db, "50004", spans=[
        {"field_name": "study_design", "value": "RCT", "audit_status": "contested"},
    ])

    for stage in ("SCREENING_COMPLETE", "DIAGNOSTIC_SAMPLE_COMPLETE",
                   "CATEGORIES_CONFIGURED", "QUEUE_EXPORTED",
                   "ADJUDICATION_COMPLETE", "EXTRACTION_COMPLETE",
                   "AI_AUDIT_COMPLETE_STAGE"):
        complete_stage(db._conn, stage)

    out = tmp_path / "queue.xlsx"
    export_audit_review_queue(db, out, spot_check_pct=0)

    from openpyxl import load_workbook
    wb = load_workbook(out)
    ws = wb["Audit Review"]
    headers = [cell.value for cell in ws[1]]
    accept_col = headers.index("accept_as_is") + 1
    ws.cell(row=2, column=accept_col, value="TRUE")
    wb.save(out)

    import_audit_review_decisions(db, out)

    assert is_stage_done(db._conn, "AUDIT_REVIEW_COMPLETE")


def test_import_missing_decision_blocks_completion(db, tmp_path):
    """Papers with no decision should block AUDIT_REVIEW_COMPLETE."""
    _add_paper_with_extraction(db, "50005", spans=[
        {"field_name": "study_design", "value": "RCT", "audit_status": "flagged"},
    ])

    for stage in ("SCREENING_COMPLETE", "DIAGNOSTIC_SAMPLE_COMPLETE",
                   "CATEGORIES_CONFIGURED", "QUEUE_EXPORTED",
                   "ADJUDICATION_COMPLETE", "EXTRACTION_COMPLETE",
                   "AI_AUDIT_COMPLETE_STAGE"):
        complete_stage(db._conn, stage)

    out = tmp_path / "queue.xlsx"
    export_audit_review_queue(db, out, spot_check_pct=0)

    # Don't fill in any decisions
    result = import_audit_review_decisions(db, out)

    assert result["stats"]["missing"] == 1
    assert not is_stage_done(db._conn, "AUDIT_REVIEW_COMPLETE")


# ── Reject Cascade ────────────────────────────────────────────────────


def test_reject_paper_cascade(db, tmp_path):
    """reject_paper=TRUE should transition paper to REJECTED."""
    pid = _add_paper_with_extraction(db, "60001", spans=[
        {"field_name": "study_design", "value": "RCT", "audit_status": "flagged"},
    ])

    for stage in ("SCREENING_COMPLETE", "DIAGNOSTIC_SAMPLE_COMPLETE",
                   "CATEGORIES_CONFIGURED", "QUEUE_EXPORTED",
                   "ADJUDICATION_COMPLETE", "EXTRACTION_COMPLETE",
                   "AI_AUDIT_COMPLETE_STAGE"):
        complete_stage(db._conn, stage)

    out = tmp_path / "queue.xlsx"
    export_audit_review_queue(db, out, spot_check_pct=0)

    from openpyxl import load_workbook
    wb = load_workbook(out)
    ws = wb["Audit Review"]
    headers = [cell.value for cell in ws[1]]
    reject_col = headers.index("reject_paper") + 1
    ws.cell(row=2, column=reject_col, value="TRUE")
    wb.save(out)

    result = import_audit_review_decisions(db, out)
    assert result["stats"]["rejected"] == 1

    paper = db._conn.execute("SELECT status FROM papers WHERE id = ?", (pid,)).fetchone()
    assert paper["status"] == "REJECTED"


# ── min_status Filtering ──────────────────────────────────────────────


def test_min_status_filtering(db):
    """_STATUS_ORDER filtering: AI_AUDIT includes both, HUMAN_AUDIT only human-verified."""
    from engine.core.database import _STATUS_ORDER

    # Add an AI_AUDIT_COMPLETE paper
    _add_paper_with_extraction(db, "70001", spans=[
        {"field_name": "study_design", "value": "RCT", "audit_status": "verified"},
    ], status="AI_AUDIT_COMPLETE")

    # Add a HUMAN_AUDIT_COMPLETE paper
    _add_paper_with_extraction(db, "70002", spans=[
        {"field_name": "study_design", "value": "cohort", "audit_status": "verified"},
    ], status="HUMAN_AUDIT_COMPLETE")

    # AI_AUDIT_COMPLETE level should include both
    min_level = _STATUS_ORDER["AI_AUDIT_COMPLETE"]
    qualifying = [s for s, lvl in _STATUS_ORDER.items() if lvl >= min_level]
    placeholders = ", ".join("?" for _ in qualifying)
    rows = db._conn.execute(
        f"SELECT id FROM papers WHERE status IN ({placeholders})",
        qualifying,
    ).fetchall()
    assert len(rows) == 2

    # HUMAN_AUDIT_COMPLETE level should include only the human-verified paper
    min_level = _STATUS_ORDER["HUMAN_AUDIT_COMPLETE"]
    qualifying = [s for s, lvl in _STATUS_ORDER.items() if lvl >= min_level]
    placeholders = ", ".join("?" for _ in qualifying)
    rows = db._conn.execute(
        f"SELECT id FROM papers WHERE status IN ({placeholders})",
        qualifying,
    ).fetchall()
    assert len(rows) == 1


# ── Gate Check ────────────────────────────────────────────────────────


def test_check_audit_review_gate(db):
    """Gate should count papers with unresolved spans."""
    _add_paper_with_extraction(db, "80001", spans=[
        {"field_name": "study_design", "value": "RCT", "audit_status": "flagged"},
    ])
    _add_paper_with_extraction(db, "80002", spans=[
        {"field_name": "study_design", "value": "RCT", "audit_status": "verified"},
    ])

    count = check_audit_review_gate(db)
    assert count == 1


def test_check_audit_review_gate_zero_when_clean(db):
    """Gate should return 0 when no papers have issues."""
    _add_paper_with_extraction(db, "80003", spans=[
        {"field_name": "study_design", "value": "RCT", "audit_status": "verified"},
    ])

    count = check_audit_review_gate(db)
    assert count == 0
