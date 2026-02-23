"""Tests for the SQLite review database."""

import json

import pytest

from engine.core.database import ReviewDatabase, STATUSES, ALLOWED_TRANSITIONS
from engine.search.models import Citation


@pytest.fixture()
def db(tmp_path):
    """Create a fresh ReviewDatabase in a temp directory."""
    rdb = ReviewDatabase("test_review", data_root=tmp_path)
    yield rdb
    rdb.close()


def _cit(**kw):
    defaults = dict(title="Study A", source="pubmed", pmid="111", doi="10.1/a")
    defaults.update(kw)
    return Citation(**defaults)


# ── Table Creation ───────────────────────────────────────────────────


def test_tables_exist(db):
    tables = {
        r[0]
        for r in db._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    expected = {
        "papers",
        "screening_decisions",
        "full_text_assets",
        "extractions",
        "evidence_spans",
        "review_runs",
    }
    assert expected.issubset(tables)


def test_directories_created(db, tmp_path):
    base = tmp_path / "test_review"
    assert (base / "pdfs").is_dir()
    assert (base / "parsed_text").is_dir()
    assert (base / "vector_store").is_dir()


def test_wal_mode(db):
    mode = db._conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode == "wal"


# ── Add Papers & Dedup ───────────────────────────────────────────────


def test_add_papers(db):
    cits = [_cit(pmid=str(i), title=f"Study {i}") for i in range(5)]
    added = db.add_papers(cits)
    assert added == 5


def test_no_duplicates_on_readd(db):
    cits = [_cit(pmid="99", title="Dup Study")]
    db.add_papers(cits)
    added_again = db.add_papers(cits)
    assert added_again == 0

    rows = db.get_papers_by_status("INGESTED")
    pmid_99 = [r for r in rows if r["pmid"] == "99"]
    assert len(pmid_99) == 1


# ── Full Lifecycle Walk ──────────────────────────────────────────────


def test_full_lifecycle(db):
    db.add_papers([_cit(pmid="LC1", title="Lifecycle Paper")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    # INGESTED → SCREENED_IN
    db.update_status(pid, "SCREENED_IN")
    assert db.get_papers_by_status("SCREENED_IN")[0]["id"] == pid

    # SCREENED_IN → PDF_ACQUIRED
    db.update_status(pid, "PDF_ACQUIRED")

    # PDF_ACQUIRED → PARSED
    db.update_status(pid, "PARSED")

    # PARSED → EXTRACTED
    db.update_status(pid, "EXTRACTED")

    # EXTRACTED → AUDITED
    db.update_status(pid, "AUDITED")
    assert db.get_papers_by_status("AUDITED")[0]["id"] == pid


def test_screened_out_lifecycle(db):
    db.add_papers([_cit(pmid="SO1", title="Screened Out")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    db.update_status(pid, "SCREENED_OUT")
    assert db.get_papers_by_status("SCREENED_OUT")[0]["id"] == pid


def test_flagged_then_resolved(db):
    db.add_papers([_cit(pmid="FL1", title="Flagged")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    db.update_status(pid, "SCREEN_FLAGGED")
    db.update_status(pid, "SCREENED_IN")
    assert db.get_papers_by_status("SCREENED_IN")[0]["id"] == pid


# ── Invalid Transitions ─────────────────────────────────────────────


def test_invalid_transition_raises(db):
    db.add_papers([_cit(pmid="IT1", title="Bad Transition")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    with pytest.raises(ValueError, match="Invalid transition"):
        db.update_status(pid, "EXTRACTED")


def test_invalid_status_raises(db):
    db.add_papers([_cit(pmid="IS1", title="Invalid Status")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    with pytest.raises(ValueError, match="Invalid status"):
        db.update_status(pid, "NONEXISTENT")


def test_screened_out_is_terminal(db):
    db.add_papers([_cit(pmid="T1", title="Terminal")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    db.update_status(pid, "SCREENED_OUT")
    with pytest.raises(ValueError, match="Invalid transition"):
        db.update_status(pid, "SCREENED_IN")


# ── Screening Decisions ─────────────────────────────────────────────


def test_screening_decisions(db):
    db.add_papers([_cit(pmid="SD1", title="Screen Me")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    d1 = db.add_screening_decision(pid, 1, "include", "Relevant study", "qwen3:8b")
    d2 = db.add_screening_decision(pid, 2, "include", "Confirmed relevant", "qwen3:8b")
    assert d1 > 0
    assert d2 > d1

    rows = db._conn.execute(
        "SELECT * FROM screening_decisions WHERE paper_id = ?", (pid,)
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["pass_number"] == 1
    assert rows[1]["pass_number"] == 2


# ── Staleness Detection ─────────────────────────────────────────────


def test_staleness_detection(db):
    db.add_papers([_cit(pmid="ST1", title="Stale")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    # Walk to EXTRACTED
    db.update_status(pid, "SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    db.update_status(pid, "PARSED")
    db.update_status(pid, "EXTRACTED")

    old_hash = "abc123"
    db.add_extraction(pid, old_hash, {"study_design": "RCT"}, "thinking...", "deepseek-r1:32b")

    # Same hash → not stale
    assert len(db.get_stale_extractions(old_hash)) == 0

    # Different hash → stale
    stale = db.get_stale_extractions("new_hash_456")
    assert len(stale) == 1
    assert stale[0]["id"] == pid


# ── Evidence Spans & Audit ───────────────────────────────────────────


def test_evidence_spans_and_audit(db):
    db.add_papers([_cit(pmid="ES1", title="Spans")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    db.update_status(pid, "SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    db.update_status(pid, "PARSED")
    db.update_status(pid, "EXTRACTED")

    ext_id = db.add_extraction(pid, "hash1", {"design": "RCT"}, "trace", "deepseek-r1:32b")
    span_id = db.add_evidence_span(ext_id, "study_design", "RCT", "This was an RCT...", 0.95)

    # Verify pending
    span = db._conn.execute(
        "SELECT * FROM evidence_spans WHERE id = ?", (span_id,)
    ).fetchone()
    assert span["audit_status"] == "pending"

    # Audit it
    db.update_audit(span_id, "verified", "qwen3:32b", "Confirmed RCT design")
    span = db._conn.execute(
        "SELECT * FROM evidence_spans WHERE id = ?", (span_id,)
    ).fetchone()
    assert span["audit_status"] == "verified"
    assert span["auditor_model"] == "qwen3:32b"


# ── Pipeline Stats ───────────────────────────────────────────────────


def test_pipeline_stats(db):
    cits = [_cit(pmid=str(i), title=f"Stat {i}") for i in range(10)]
    db.add_papers(cits)

    # Screen 3 in, 2 out
    papers = db.get_papers_by_status("INGESTED")
    for p in papers[:3]:
        db.update_status(p["id"], "SCREENED_IN")
    for p in papers[3:5]:
        db.update_status(p["id"], "SCREENED_OUT")

    stats = db.get_pipeline_stats()
    assert stats["total_papers"] == 10
    assert stats["SCREENED_IN"] == 3
    assert stats["SCREENED_OUT"] == 2
    assert stats["INGESTED"] == 5
    assert stats["total_extractions"] == 0
    assert stats["total_evidence_spans"] == 0
