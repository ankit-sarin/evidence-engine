"""Tests for audit adjudication: export/import round-trip, spot-check, reject, min_status.

Tests cover the per-span export format with PI_decision (ACCEPT/REJECT/CORRECT)
and the hardened two-pass validation importer.
"""

import json
from pathlib import Path

import pytest

from engine.adjudication.audit_adjudicator import (
    _collect_papers_for_review,
    _flatten_to_span_rows,
    check_audit_review_gate,
    export_audit_review_queue,
    AuditAdjudicationDeprecated,
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
        "ABSTRACT_SCREENED_IN": ["ABSTRACT_SCREENED_IN"],
        "PDF_ACQUIRED": ["ABSTRACT_SCREENED_IN", "PDF_ACQUIRED"],
        "PARSED": ["ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED"],
        "EXTRACTED": ["ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED"],
        "AI_AUDIT_COMPLETE": ["ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED", "AI_AUDIT_COMPLETE"],
        "HUMAN_AUDIT_COMPLETE": ["ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED",
                                 "AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE"],
    }
    for step in transitions.get(target, []):
        try:
            db.update_status(pid, step)
        except ValueError:
            pass  # already at or past this status


def _complete_prereq_stages(db):
    """Complete all workflow stages up to AUDIT_QUEUE_EXPORTED prerequisite."""
    for stage in ("ABSTRACT_SCREENING_COMPLETE", "ABSTRACT_DIAGNOSTIC_COMPLETE",
                   "ABSTRACT_CATEGORIES_CONFIGURED", "ABSTRACT_QUEUE_EXPORTED",
                   "ABSTRACT_ADJUDICATION_COMPLETE",
                   "FULL_TEXT_SCREENING_COMPLETE", "FULL_TEXT_ADJUDICATION_COMPLETE",
                   "EXTRACTION_COMPLETE",
                   "AI_AUDIT_COMPLETE_STAGE"):
        complete_stage(db._conn, stage)


def _find_header_col(ws, name):
    """Find 0-indexed column by partial header name match."""
    for cell in ws[1]:
        if cell.value and name.lower() in str(cell.value).lower():
            return cell.column - 1
    raise ValueError(f"Column '{name}' not found in headers")


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
    assert len(papers) == 1
    assert papers[0]["review_reason"] == "spot_check"


# ── Flatten Tests ─────────────────────────────────────────────────────


def test_flatten_exports_problem_spans_only(db):
    """For audit_issues papers, only problem spans are exported."""
    _add_paper_with_extraction(db, "11001", spans=[
        {"field_name": "study_design", "value": "RCT", "audit_status": "verified"},
        {"field_name": "sample_size", "value": "50", "audit_status": "flagged"},
    ])

    papers = _collect_papers_for_review(db, spot_check_pct=0)
    rows = _flatten_to_span_rows(papers)
    assert len(rows) == 1
    assert rows[0]["field_name"] == "sample_size"
    assert rows[0]["audit_state"] == "flagged"


def test_flatten_spot_check_exports_all_spans(db):
    """For spot-check papers, all spans are exported."""
    _add_paper_with_extraction(db, "11002", spans=[
        {"field_name": "study_design", "value": "RCT", "audit_status": "verified"},
        {"field_name": "sample_size", "value": "50", "audit_status": "verified"},
    ])

    papers = _collect_papers_for_review(db, spot_check_pct=1.0)
    rows = _flatten_to_span_rows(papers)
    assert len(rows) == 2


# ── Export Tests ──────────────────────────────────────────────────────


def test_export_creates_xlsx(db, tmp_path):
    """Export should create an Excel file with per-span rows and expected sheets."""
    _add_paper_with_extraction(db, "40001", spans=[
        {"field_name": "study_design", "value": "RCT", "audit_status": "flagged"},
        {"field_name": "sample_size", "value": "100", "audit_status": "verified"},
    ])

    _complete_prereq_stages(db)

    out = tmp_path / "audit_queue.xlsx"
    stats = export_audit_review_queue(db, out, spot_check_pct=0)

    assert out.exists()
    assert stats["total"] == 1
    assert stats["flagged"] == 1

    from openpyxl import load_workbook
    wb = load_workbook(out)
    assert "Instructions" in wb.sheetnames
    assert "Review Queue" in wb.sheetnames
    assert "Audit Reference" in wb.sheetnames

    ws = wb["Review Queue"]
    headers = [cell.value for cell in ws[1]]
    assert "paper_id" in headers
    assert "Field Name" in headers
    assert "Extracted Value" in headers
    assert "Audit State" in headers
    # Decision column with valid values in header
    assert any("PI_decision" in str(h) for h in headers if h)
    # Free text columns
    assert any("corrected_value" in str(h) for h in headers if h)
    assert any("PI_notes" in str(h) for h in headers if h)

    # Only the flagged span should appear (not verified)
    data_rows = list(ws.iter_rows(min_row=2, values_only=True))
    non_empty = [r for r in data_rows if r[0] is not None]
    assert len(non_empty) == 1


def test_export_sets_workflow_stage(db, tmp_path):
    """Export should auto-advance AUDIT_QUEUE_EXPORTED."""
    _add_paper_with_extraction(db, "40002", spans=[
        {"field_name": "study_design", "value": "RCT", "audit_status": "contested"},
    ])

    _complete_prereq_stages(db)

    out = tmp_path / "audit_queue.xlsx"
    export_audit_review_queue(db, out, spot_check_pct=0)

    assert is_stage_done(db._conn, "AUDIT_QUEUE_EXPORTED")


# ── Import / Round-Trip Tests ─────────────────────────────────────────
















# ── Reject Span ──────────────────────────────────────────────────────




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


# ── H9: Missing span stats accuracy ─────────────────────────────────


def test_missing_span_not_counted_as_success(db, tmp_path):
    """Adjudication for a nonexistent span is rejected — not counted as success."""
    pid = _add_paper_with_extraction(db, "90001", spans=[
        {"field_name": "study_design", "value": "RCT", "audit_status": "contested"},
    ])

    # Create a decision referencing a span that doesn't exist
    decisions = [{
        "span_id": 999999,  # nonexistent
        "paper_id": pid,
        "field_name": "nonexistent_field",
        "decision": "ACCEPT",
    }]

    json_path = tmp_path / "missing_span_decisions.json"
    json_path.write_text(json.dumps(decisions))

    # R18, A11 Option B: the path refuses before it validates anything, so the
    # "nothing applied" guarantee this test pinned now holds by construction
    # rather than by the validator getting it right.
    with pytest.raises(AuditAdjudicationDeprecated):
        import_audit_review_decisions(db, str(json_path))

    # Original span unchanged — the property this test exists to protect
    span = db._conn.execute(
        "SELECT audit_status FROM evidence_spans WHERE field_name = 'study_design'"
    ).fetchone()
    assert span["audit_status"] == "contested"


# ── R18, A11 Option B: this path no longer writes ─────────────────────
#
# EFFECTIVE-RESULT-02 (session 5) stopped writing `audit_adjudication`; session
# 12 drops it. The eight tests that used to drive `import_audit_review_decisions`
# — ACCEPT, CORRECT, REJECT, the stage transitions and the three validation
# refusals — are rewritten here as one pinned refusal rather than deleted: a test
# that pinned a behaviour is rewritten to the corrected behaviour (B5), and the
# behaviour they pinned is exactly what R18 retired.
#
# They were never a guarantee in any case. `audit_adjudication.span_id`
# references the phantom `_evidence_spans_old`, so under the
# `PRAGMA foreign_keys=ON` that `ReviewDatabase.__init__` sets, the INSERT this
# path performs fails on the live database — which is why the table holds 0 rows
# there (A11).

def test_the_audit_import_path_refuses_and_names_its_successor(db, tmp_path):
    out = tmp_path / "decisions.json"
    out.write_text("[]")
    with pytest.raises(AuditAdjudicationDeprecated) as exc:
        import_audit_review_decisions(db, out)
    msg = str(exc.value)
    assert "R18, A11 Option B" in msg
    assert "session 12" in msg
    assert "field_events" in msg


def test_the_audit_import_path_refuses_before_it_reads_anything(db):
    """No file, no arguments beyond the database: it still refuses."""
    with pytest.raises(AuditAdjudicationDeprecated):
        import_audit_review_decisions(db)


def test_audit_adjudication_is_left_on_disk_untouched_session_12_drops_it(db):
    """R18 stops the writer; it does not drop the table."""
    assert db._conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
        "AND name='audit_adjudication'").fetchone()[0] == 1
    assert db._conn.execute(
        "SELECT COUNT(*) FROM audit_adjudication").fetchone()[0] == 0
