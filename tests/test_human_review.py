"""Tests for human review infrastructure."""

import csv
from pathlib import Path

import pytest

from engine.core.database import ReviewDatabase
from engine.review.human_review import (
    export_review_queue,
    import_review_decisions,
    bulk_accept,
    REVIEW_COLUMNS,
)
from engine.search.models import Citation


def _cit(**kw):
    defaults = dict(title="Study A", source="pubmed", pmid="111", doi="10.1/a",
                    abstract="This is an abstract about surgical robotics.")
    defaults.update(kw)
    return Citation(**defaults)


def _make_audited_paper(db, tmp_path, pmid, span_statuses):
    """Create a paper at AI_AUDIT_COMPLETE with given span statuses.

    span_statuses: list of (field_name, value, snippet, status) tuples
    """
    db.add_papers([_cit(pmid=pmid, title=f"Paper {pmid}")])
    pid = db.get_papers_by_status("INGESTED")[-1]["id"]
    for s in ("ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED"):
        db.update_status(pid, s)

    # Write parsed text
    parsed_dir = Path(db.db_path).parent / "parsed_text"
    parsed_dir.mkdir(exist_ok=True)
    (parsed_dir / f"{pid}_v1.md").write_text(
        "This is a randomized controlled trial with 20 patients. "
        "The STAR robot was used for autonomous suturing. "
        "Results showed 95% accuracy."
    )

    ext_id = db.add_extraction(pid, "hash", {}, "trace", "model")
    for fname, value, snippet, status in span_statuses:
        sid = db.add_evidence_span(ext_id, fname, value, snippet, 0.9)
        if status != "pending":
            db.update_audit(sid, status, "qwen3:32b", f"Audit: {status}")

    db.update_status(pid, "AI_AUDIT_COMPLETE")
    return pid


@pytest.fixture()
def review_db(tmp_path):
    db = ReviewDatabase("test_review", data_root=tmp_path)
    yield db, tmp_path
    db.close()


def test_export_review_queue_columns(review_db):
    db, tmp_path = review_db
    pid = _make_audited_paper(db, tmp_path, "EX1", [
        ("study_design", "RCT", "randomized controlled trial", "contested"),
        ("sample_size", "20", "20 patients", "verified"),
        ("robot_platform", "STAR", "The STAR robot", "flagged"),
    ])

    out = str(tmp_path / "queue.csv")
    count = export_review_queue(db, out)

    assert count == 2  # only contested + flagged, not verified

    with open(out) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == 2
    assert set(rows[0].keys()) == set(REVIEW_COLUMNS)
    fields = {r["field_name"] for r in rows}
    assert "study_design" in fields
    assert "robot_platform" in fields
    assert "sample_size" not in fields  # verified, not exported


def test_import_accept(review_db):
    db, tmp_path = review_db
    pid = _make_audited_paper(db, tmp_path, "IM1", [
        ("study_design", "RCT", "randomized controlled trial", "contested"),
    ])

    # Create decision CSV
    csv_path = str(tmp_path / "decisions.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=REVIEW_COLUMNS)
        writer.writeheader()
        writer.writerow({
            "paper_id": pid, "ee_identifier": "EE-001",
            "field_name": "study_design", "tier": "1", "field_type": "text",
            "extracted_value": "RCT", "source_snippet": "randomized controlled trial",
            "audit_status": "contested", "audit_rationale": "", "confidence": "0.9",
            "parsed_text_context": "", "reviewer_decision": "ACCEPT",
            "corrected_snippet": "", "reviewer_note": "",
        })

    result = import_review_decisions(db, csv_path)
    assert result["applied"] == 1
    assert not result["errors"]

    # Span should now be verified
    span = db._conn.execute(
        "SELECT audit_status FROM evidence_spans ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert span["audit_status"] == "verified"

    # Paper should transition to HUMAN_AUDIT_COMPLETE (no remaining contested/flagged)
    assert len(db.get_papers_by_status("HUMAN_AUDIT_COMPLETE")) == 1


def test_import_rejects_bad_corrected_snippet(review_db):
    db, tmp_path = review_db
    pid = _make_audited_paper(db, tmp_path, "IM2", [
        ("study_design", "RCT", "randomized controlled trial", "contested"),
    ])

    csv_path = str(tmp_path / "decisions.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=REVIEW_COLUMNS)
        writer.writeheader()
        writer.writerow({
            "paper_id": pid, "ee_identifier": "EE-001",
            "field_name": "study_design", "tier": "1", "field_type": "text",
            "extracted_value": "RCT", "source_snippet": "randomized controlled trial",
            "audit_status": "contested", "audit_rationale": "", "confidence": "0.9",
            "parsed_text_context": "",
            "reviewer_decision": "ACCEPT_CORRECTED",
            "corrected_snippet": "This text is completely fabricated and not in the paper",
            "reviewer_note": "",
        })

    result = import_review_decisions(db, csv_path)
    assert len(result["errors"]) == 1
    assert "not found verbatim" in result["errors"][0]


def test_import_reject_value(review_db):
    db, tmp_path = review_db
    pid = _make_audited_paper(db, tmp_path, "IM3", [
        ("sample_size", "200", "20 patients", "flagged"),
    ])

    csv_path = str(tmp_path / "decisions.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=REVIEW_COLUMNS)
        writer.writeheader()
        writer.writerow({
            "paper_id": pid, "ee_identifier": "EE-001",
            "field_name": "sample_size", "tier": "3", "field_type": "text",
            "extracted_value": "200", "source_snippet": "20 patients",
            "audit_status": "flagged", "audit_rationale": "", "confidence": "0.9",
            "parsed_text_context": "",
            "reviewer_decision": "REJECT_VALUE",
            "corrected_snippet": "", "reviewer_note": "Wrong value",
        })

    result = import_review_decisions(db, csv_path)
    assert result["applied"] == 1

    span = db._conn.execute(
        "SELECT value, audit_status FROM evidence_spans ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert span["value"] == "NR"
    assert span["audit_status"] == "verified"


def test_bulk_accept_does_not_transition_to_human(review_db):
    """bulk_accept keeps papers at AI_AUDIT_COMPLETE, not HUMAN_AUDIT_COMPLETE."""
    db, tmp_path = review_db
    pid = _make_audited_paper(db, tmp_path, "BA1", [
        ("study_design", "RCT", "randomized controlled trial", "contested"),
        ("sample_size", "20", "20 patients", "flagged"),
    ])

    result = bulk_accept(db)
    assert result["spans_accepted"] == 2

    # Paper stays at AI_AUDIT_COMPLETE
    assert len(db.get_papers_by_status("AI_AUDIT_COMPLETE")) == 1
    assert len(db.get_papers_by_status("HUMAN_AUDIT_COMPLETE")) == 0

    # Spans are now verified
    pending = db._conn.execute(
        "SELECT COUNT(*) FROM evidence_spans WHERE audit_status != 'verified'"
    ).fetchone()[0]
    assert pending == 0


def test_paper_transitions_only_when_all_resolved(review_db):
    """Paper stays at AI_AUDIT_COMPLETE if some spans are still contested."""
    db, tmp_path = review_db
    pid = _make_audited_paper(db, tmp_path, "TR1", [
        ("study_design", "RCT", "randomized controlled trial", "contested"),
        ("sample_size", "20", "20 patients", "contested"),
    ])

    # Only resolve one span
    csv_path = str(tmp_path / "decisions.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=REVIEW_COLUMNS)
        writer.writeheader()
        writer.writerow({
            "paper_id": pid, "ee_identifier": "EE-001",
            "field_name": "study_design", "tier": "1", "field_type": "text",
            "extracted_value": "RCT", "source_snippet": "randomized controlled trial",
            "audit_status": "contested", "audit_rationale": "", "confidence": "0.9",
            "parsed_text_context": "",
            "reviewer_decision": "ACCEPT",
            "corrected_snippet": "", "reviewer_note": "",
        })

    result = import_review_decisions(db, csv_path)
    assert result["applied"] == 1

    # Paper stays at AI_AUDIT_COMPLETE (one span still contested)
    assert len(db.get_papers_by_status("AI_AUDIT_COMPLETE")) == 1
    assert len(db.get_papers_by_status("HUMAN_AUDIT_COMPLETE")) == 0
